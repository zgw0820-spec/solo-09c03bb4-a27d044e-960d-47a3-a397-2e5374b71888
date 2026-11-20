"""补跑业务编排：查计划、匹配 Job、按模板补建、超时回查。

轮次模型：
- 一轮巡检由 (CronJob UID, 计划时刻) 唯一标识；
- 调用方可把“查计划”返回的 cronjob_uid / scheduled_at_utc 回传给
  /catchup、/reverify，锁定要处理的轮次；两项须一起提供，省略时沿用最近一轮；
- 指定轮次后先查台账与集群：找到匹配 Job 即返回，即使已过期、服务重启或
  CronJob 已改排程；确认该轮没有 Job 后才校验补跑条件（原 CronJob 仍暂停、
  所选时刻仍符合当前排程、已到点、到点未超 2 小时）；
- 查不清是否已有 Job 时报“待核实”，绝不盲目补建。
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from .kube import (
    KubeClient,
    KubeError,
    KubeNotFound,
    build_catchup_job,
    cronjob_summary,
    find_matching_job,
    is_catchup_job,
    scheduled_at_iso,
)
from .schedule import CronError, latest_scheduled_time, parse_cron
from .store import (
    STATE_CREATED,
    STATE_CREATING,
    STATE_FAILED,
    STATE_PENDING,
    CatchupStore,
)

CATCHUP_WINDOW = timedelta(hours=2)


class ServiceError(Exception):
    def __init__(self, message: str, status_code: int = 409):
        super().__init__(message)
        self.message = message
        self.status_code = status_code


def _normalize_round_time(value: datetime) -> datetime:
    """调用方指定的计划时刻：无时区按 UTC 处理，归一到 UTC 秒级。"""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).replace(microsecond=0)


class CatchupService:
    def __init__(self, kube: KubeClient, store: CatchupStore):
        self.kube = kube
        self.store = store

    # ---------- 查计划 ----------

    async def inspect(
        self, namespace: str, name: str, now_utc: datetime
    ) -> dict[str, Any]:
        cronjob = await self._load_valid_cronjob(namespace, name)
        info = cronjob_summary(cronjob)
        scheduled = latest_scheduled_time(
            info["schedule"], info["time_zone"], now_utc
        )
        jobs = await self.kube.list_jobs(namespace)
        job, source = find_matching_job(jobs, info["uid"], scheduled)
        within = timedelta(0) <= (now_utc - scheduled) <= CATCHUP_WINDOW
        return {
            "namespace": namespace,
            "cronjob": name,
            "cronjob_uid": info["uid"],
            "time_zone": info["time_zone"],
            "schedule": info["schedule"],
            "suspended": info["suspended"],
            "scheduled_at_utc": scheduled,
            "is_daily_fixed_time": parse_cron(info["schedule"]).is_daily_fixed_time,
            "existing_job": self._job_ref(job) if job else None,
            "existing_job_source": source,
            "within_catchup_window": within,
        }

    # ---------- 轮次解析 ----------

    async def _resolve_round(
        self,
        namespace: str,
        name: str,
        now_utc: datetime,
        cronjob_uid: str | None,
        scheduled_at: datetime | None,
    ) -> tuple[str, datetime, bool, dict | None]:
        """确定本次操作的轮次 (cronjob_uid, scheduled)。

        两项参数必须一起提供；都省略时沿用最近一轮（需读取 CronJob 现算）。
        返回 (uid, scheduled, pinned, cronjob|None)；指定轮次时 cronjob 延迟到
        确认需要补建时再读取，保证“查回已有 Job”不受 CronJob 改排程影响。
        """
        if (cronjob_uid is None) != (scheduled_at is None):
            raise ServiceError(
                "cronjob_uid 与 scheduled_at 必须一起提供"
                "（取查计划接口返回的 cronjob_uid 与 scheduled_at_utc）；"
                "都省略时按最近一轮处理",
                status_code=422,
            )
        if cronjob_uid is None:
            cronjob = await self._load_valid_cronjob(namespace, name)
            info = cronjob_summary(cronjob)
            scheduled = latest_scheduled_time(
                info["schedule"], info["time_zone"], now_utc
            )
            return info["uid"], scheduled, False, cronjob
        return cronjob_uid, _normalize_round_time(scheduled_at), True, None

    # ---------- 补跑 ----------

    async def catchup(
        self,
        namespace: str,
        name: str,
        now_utc: datetime,
        cronjob_uid: str | None = None,
        scheduled_at: datetime | None = None,
    ) -> dict[str, Any]:
        uid, scheduled, pinned, cronjob = await self._resolve_round(
            namespace, name, now_utc, cronjob_uid, scheduled_at
        )
        scheduled_iso = scheduled_at_iso(scheduled)

        async with self.store.async_lock_for(f"{uid}/{scheduled_iso}"):
            info: dict[str, Any] = {
                "namespace": namespace, "name": name, "uid": uid,
            }

            # 1. 台账查回：重启后记录仍在；确认已建出即返回，
            #    不受补跑窗口、进程重启、CronJob 改排程影响。
            record = self.store.get_by_round(uid, scheduled_iso)
            if record and record["state"] == STATE_CREATED:
                job = await self._safe_get_job(namespace, record["job_name"])
                if job is not None:
                    return self._existing_response(
                        info, scheduled, job,
                        "catchup" if is_catchup_job(job) else "scheduler",
                        record_id=record["id"],
                    )
                # 按名字查不到：继续走下面的 (UID, 计划时刻) 列查再下结论
            if record and record["state"] == STATE_PENDING:
                return await self._reverify(namespace, info, scheduled, record)

            # 2. 集群列查：按 (UID, 计划时刻) 匹配已有 Job（原调度器创建的也算）
            jobs, list_error = await self._try_list_jobs(namespace)
            if list_error is not None:
                # 查不清是否已有 Job -> 待核实，绝不盲目补建。
                # 尚未发起任何创建，不落台账；集群恢复后按原轮次重试即可。
                if record and record["state"] == STATE_CREATED:
                    self.store.update_state(
                        record["id"], STATE_PENDING,
                        detail=f"台账显示已创建，但集群回查失败: {list_error}",
                    )
                    record = self.store.get_by_round(uid, scheduled_iso)
                    return self._pending_response(
                        info, scheduled, record,
                        "台账显示已创建，但集群回查失败，无法确认 Job 状态",
                    )
                return self._pending_response(
                    info, scheduled, None,
                    f"无法确认该轮是否已有 Job（集群查询失败: {list_error}），"
                    "请人工核实后重试",
                )
            existing, source = find_matching_job(jobs, uid, scheduled)
            if existing is not None:
                if record and record["state"] == STATE_CREATED:
                    # 台账里的 Job 名已查不到，但同轮另有 Job：修正台账指向
                    ref = self._job_ref(existing)
                    self.store.update_state(
                        record["id"], STATE_CREATED,
                        job_name=ref["metadata"]["name"],
                        job_uid=ref["metadata"].get("uid"),
                        detail="台账记录的 Job 名未查到，按 UID+计划时刻命中其他 Job",
                    )
                return self._existing_response(
                    info, scheduled, existing, source,
                    record_id=record["id"] if record else None,
                )
            if record and record["state"] == STATE_CREATED:
                # 集群可达但按名字/按轮次都查不到：台账与集群不一致，转待核实
                self.store.update_state(
                    record["id"], STATE_PENDING,
                    detail="台账已记录但集群查不到该 Job",
                )
                record = self.store.get_by_round(uid, scheduled_iso)
                return self._pending_response(
                    info, scheduled, record,
                    "记录显示已创建，但集群中查不到该 Job",
                )

            # 3. 已确认该轮没有 Job：校验补跑前置条件
            if cronjob is None:
                cronjob = await self._load_valid_cronjob(namespace, name)
            info = cronjob_summary(cronjob)
            if info["uid"] != uid:
                raise ServiceError(
                    "所传 cronjob_uid 与当前 CronJob UID 不一致"
                    f"（当前为 {info['uid']}），CronJob 可能已删除重建，拒绝补建"
                )
            self._assert_catchup_allowed(
                info, scheduled, now_utc, verify_schedule=pinned
            )

            # 4. 构造 Job 并落台账（同轮唯一约束兜底）
            job_name = self._compose_job_name(name, scheduled)
            manifest = build_catchup_job(cronjob, scheduled, job_name)
            record, inserted = self.store.insert_creating(
                namespace, name, uid, scheduled_iso, job_name
            )
            if not inserted:
                # 并发/多实例下同轮已有记录：按记录状态查回，绝不重复创建
                if record["state"] in (STATE_CREATED, STATE_PENDING):
                    return await self._reverify(namespace, info, scheduled, record)
                if record["state"] == STATE_CREATING:
                    return self._pending_response(
                        info, scheduled, record,
                        "同轮已有创建请求在进行中，结果待核实",
                    )
                # STATE_FAILED：上次创建明确失败，复用记录再次尝试

            # 5. 调 Kubernetes 创建；回包超时按原轮次回查
            return await self._create_with_verify(
                namespace, info, scheduled, manifest, record
            )

    # ---------- 待核实记录的手动/再次回查 ----------

    async def reverify(
        self,
        namespace: str,
        name: str,
        now_utc: datetime,
        cronjob_uid: str | None = None,
        scheduled_at: datetime | None = None,
    ) -> dict[str, Any]:
        uid, scheduled, _pinned, _cronjob = await self._resolve_round(
            namespace, name, now_utc, cronjob_uid, scheduled_at
        )
        info: dict[str, Any] = {
            "namespace": namespace, "name": name, "uid": uid,
        }
        record = self.store.get_by_round(uid, scheduled_at_iso(scheduled))
        if record:
            return await self._reverify(namespace, info, scheduled, record)
        # 该轮没有台账记录：仍按轮次查一遍集群，找到匹配 Job 即返回
        jobs, list_error = await self._try_list_jobs(namespace)
        if list_error is not None:
            raise ServiceError(
                f"该轮没有补跑台账记录，且回查集群失败: {list_error}",
                status_code=502,
            )
        job, source = find_matching_job(jobs, uid, scheduled)
        if job is None:
            raise ServiceError(
                "该轮没有补跑台账记录，集群也未找到匹配 Job，无需核实",
                status_code=404,
            )
        return self._existing_response(info, scheduled, job, source)

    # ---------- 内部方法 ----------

    async def _create_with_verify(
        self, namespace, info, scheduled, manifest, record
    ) -> dict[str, Any]:
        try:
            job = await self.kube.create_job(namespace, manifest)
        except KubeError as exc:
            if "超时" not in str(exc) and "timeout" not in str(exc).lower():
                self.store.update_state(record["id"], STATE_FAILED, detail=str(exc))
                raise ServiceError(f"创建 Job 失败: {exc}", status_code=502) from exc
            # 创建回包超时：按原轮次回查集群
            return await self._verify_after_timeout(
                namespace, info, scheduled, record, str(exc)
            )

        job_ref = self._job_ref(job)
        self.store.update_state(
            record["id"], STATE_CREATED,
            job_name=job_ref["metadata"]["name"],
            job_uid=job_ref["metadata"].get("uid"),
            detail="创建成功",
        )
        return {
            "status": "created",
            **self._base(info, scheduled, record["id"]),
            "job": job_ref,
            "job_source": "catchup",
            "reason": None,
        }

    async def _verify_after_timeout(
        self, namespace, info, scheduled, record, timeout_detail
    ) -> dict[str, Any]:
        job = await self._safe_get_job(namespace, record["job_name"])
        if job is not None:
            ref = self._job_ref(job)
            self.store.update_state(
                record["id"], STATE_CREATED,
                job_name=ref["metadata"]["name"],
                job_uid=ref["metadata"].get("uid"),
                detail=f"创建回包超时，回查确认已存在: {timeout_detail}",
            )
            return {
                "status": "created",
                **self._base(info, scheduled, record["id"]),
                "job": ref,
                "job_source": "catchup",
                "reason": "创建请求回包超时，回查集群确认 Job 已存在",
            }
        # 再按 (UID, 计划时刻) 列查一遍，覆盖“名字不同但已建出”的小概率情况
        jobs = await self._safe_list_jobs(namespace)
        matched, source = find_matching_job(jobs, info["uid"], scheduled)
        if matched is not None:
            ref = self._job_ref(matched)
            self.store.update_state(
                record["id"], STATE_CREATED,
                job_name=ref["metadata"]["name"],
                job_uid=ref["metadata"].get("uid"),
                detail="超时后按 UID+计划时刻回查命中",
            )
            return {
                "status": "created",
                **self._base(info, scheduled, record["id"]),
                "job": ref,
                "job_source": source or "catchup",
                "reason": "创建请求回包超时，按 UID+计划时刻回查命中已有 Job",
            }
        self.store.update_state(record["id"], STATE_PENDING, detail=timeout_detail)
        return self._pending_response(
            info, scheduled, record,
            f"创建请求回包超时且回查集群未能确认: {timeout_detail}",
        )

    async def _reverify(self, namespace, info, scheduled, record) -> dict[str, Any]:
        job = await self._safe_get_job(namespace, record["job_name"])
        if job is None:
            jobs, list_error = await self._try_list_jobs(namespace)
            if list_error is not None:
                # 查不清是否已有 Job：保持/回到待核实，绝不下“没有”的结论
                self.store.update_state(
                    record["id"], STATE_PENDING,
                    detail=f"回查集群失败: {list_error}",
                )
                return self._pending_response(
                    info, scheduled, record,
                    f"回查集群失败，仍待核实: {list_error}",
                )
            job, _ = find_matching_job(jobs, info["uid"], scheduled)
        if job is None:
            self.store.update_state(record["id"], STATE_PENDING,
                                    detail="再次回查仍未找到 Job")
            return self._pending_response(
                info, scheduled, record, "回查集群未找到对应 Job，需人工核实"
            )
        ref = self._job_ref(job)
        self.store.update_state(
            record["id"], STATE_CREATED,
            job_name=ref["metadata"]["name"],
            job_uid=ref["metadata"].get("uid"),
            detail="回查确认 Job 存在",
        )
        return {
            "status": "created",
            **self._base(info, scheduled, record["id"]),
            "job": ref,
            "job_source": "catchup" if is_catchup_job(job) else "scheduler",
            "reason": "回查确认 Job 已存在",
        }

    async def _safe_get_job(self, namespace: str, name: str) -> dict | None:
        try:
            return await self.kube.get_job(namespace, name)
        except KubeNotFound:
            return None
        except KubeError:
            return None

    async def _safe_list_jobs(self, namespace: str) -> list[dict]:
        jobs, _ = await self._try_list_jobs(namespace)
        return jobs

    async def _try_list_jobs(
        self, namespace: str
    ) -> tuple[list[dict], KubeError | None]:
        """列查 Job；失败时返回 ([], error)，由调用方决定如何处置。"""
        try:
            return await self.kube.list_jobs(namespace), None
        except KubeError as exc:
            return [], exc

    def _assert_catchup_allowed(
        self,
        info,
        scheduled: datetime,
        now_utc: datetime,
        verify_schedule: bool = False,
    ) -> None:
        if not info["suspended"]:
            raise ServiceError(
                f"CronJob {info['name']} 当前未暂停（suspend=false），不应由本服务补建 Job"
            )
        if verify_schedule:
            # 指定轮次补建前，确认所选时刻仍是当前排程会到点的时刻
            try:
                still = latest_scheduled_time(
                    info["schedule"], info["time_zone"], scheduled
                )
            except CronError as exc:
                raise ServiceError(
                    f"所选计划时刻无法用当前排程核验: {exc}"
                ) from exc
            if still != scheduled:
                raise ServiceError(
                    f"所选计划时刻 {scheduled_at_iso(scheduled)} 已不符合当前排程"
                    f"（{info['schedule']}，{info['time_zone']}），"
                    "CronJob 可能已改排程，拒绝补建"
                )
        if scheduled > now_utc:
            raise ServiceError(
                f"计划时刻 {scheduled_at_iso(scheduled)} 尚未到点，不能补建"
            )
        if now_utc - scheduled > CATCHUP_WINDOW:
            raise ServiceError(
                f"已超过补跑期限：计划时刻 {scheduled_at_iso(scheduled)}，"
                f"补跑窗口为到点后 2 小时（UTC）"
            )

    async def _load_valid_cronjob(self, namespace: str, name: str) -> dict[str, Any]:
        try:
            cronjob = await self.kube.get_cronjob(namespace, name)
        except KubeNotFound as exc:
            raise ServiceError(f"CronJob {namespace}/{name} 不存在",
                               status_code=404) from exc
        except KubeError as exc:
            raise ServiceError(f"读取 CronJob 失败: {exc}", status_code=502) from exc

        info = cronjob_summary(cronjob)
        if not info["time_zone"]:
            raise ServiceError(
                "该 CronJob 未设置 spec.timeZone，本服务只支持显式时区的 CronJob"
            )
        try:
            spec = parse_cron(info["schedule"])
        except CronError as exc:
            raise ServiceError(f"CronJob schedule 非法: {exc}") from exc
        if not spec.is_daily_fixed_time:
            raise ServiceError(
                f"本服务只处理每天固定时刻的 CronJob，当前 schedule={info['schedule']} "
                "含星期/日期/月份限制"
            )
        return cronjob

    @staticmethod
    def _compose_job_name(cronjob_name: str, scheduled: datetime) -> str:
        stamp = scheduled.astimezone(timezone.utc).strftime("%Y%m%d%H%M")
        suffix = f"-catchup-{stamp}"
        # Job 名最长 63：超长时截断 CronJob 名前缀，完整保留后缀
        max_prefix = 63 - len(suffix)
        prefix = cronjob_name[:max_prefix].rstrip("-")
        return f"{prefix}{suffix}"

    @staticmethod
    def _job_ref(job: dict) -> dict:
        """裁剪返回体，保留关键字段，避免 Swagger 输出过大。"""
        meta = job.get("metadata", {})
        ref = {
            "apiVersion": job.get("apiVersion", "batch/v1"),
            "kind": "Job",
            "metadata": {
                "name": meta.get("name"),
                "namespace": meta.get("namespace"),
                "uid": meta.get("uid"),
                "labels": meta.get("labels", {}),
                "annotations": meta.get("annotations", {}),
                "creationTimestamp": meta.get("creationTimestamp"),
            },
        }
        if "spec" in job:
            # 保留 Pod 模板片段，便于核对补跑 Job 是否沿用原 jobTemplate
            ref["spec"] = {"template": job["spec"].get("template", {})}
        return ref

    @staticmethod
    def _base(info, scheduled, record_id: int | None) -> dict:
        return {
            "namespace": info["namespace"],
            "cronjob": info["name"],
            "cronjob_uid": info["uid"],
            "scheduled_at_utc": scheduled,
            "record_id": record_id,
        }

    def _existing_response(self, info, scheduled, job, source, record_id=None) -> dict:
        return {
            "status": "returned_existing",
            **self._base(info, scheduled, record_id),
            "job": self._job_ref(job),
            "job_source": source or (
                "catchup" if is_catchup_job(job) else "scheduler"
            ),
            "reason": "已存在匹配 Job（按 CronJob UID + 计划时刻匹配）",
        }

    def _pending_response(self, info, scheduled, record, reason) -> dict:
        return {
            "status": "pending_verification",
            **self._base(info, scheduled, record["id"] if record else None),
            "job": None,
            "job_source": None,
            "reason": reason,
        }
