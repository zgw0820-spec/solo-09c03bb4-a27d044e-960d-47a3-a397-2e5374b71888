"""轮次锁定：调用方回传查计划得到的 (cronjob_uid, scheduled_at)。

覆盖事故场景：隔天提交不再落到当天轮次——
- 指定旧轮次：有 Job 直接返回（即使已过期/改排程/重启），没有则按条件补建；
- 两项参数必须一起提供，省略时沿用最近一轮；
- 过期/未到点/排程不符/UID 不符一律拒绝；
- 查不清是否已有 Job 报待核实且不落台账、不发起创建。
"""
from datetime import datetime, timedelta, timezone

import pytest

from app.kube import KubeError, build_catchup_job

UTC = timezone.utc
DAILY = "inspection-daily"      # 0 9 * * * Asia/Shanghai；预置 2026-11-20T01:00:00Z 调度器 Job
UTC_CJ = "inspection-utc"       # 30 0 * * * UTC；无预置 Job
NY_CJ = "inspection-ny"

# 种子 Job 所属轮次（2026-11-20 09:00 上海）
SEEDED_ROUND = "2026-11-20T01:00:00Z"
# 隔天
NEXT_DAY = datetime(2026, 11, 21, 2, 0, tzinfo=UTC)


async def _plan(api, cj: str, at: datetime) -> dict:
    resp = await api.get(
        f"/namespaces/default/cronjobs/{cj}/latest-schedule",
        params={"at": at.isoformat()},
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


def _patch_verify_failure(kube):
    """创建超时 + 创建后回查全失败（创建前列查正常）。返回恢复函数。"""
    state = {"create_attempted": False}
    real_list = kube.list_jobs

    async def boom_create(namespace, manifest):
        state["create_attempted"] = True
        raise KubeError("Kubernetes API 请求超时: 模拟")

    async def boom_get_job(namespace, name):
        raise KubeError("模拟回查网络故障")

    async def flaky_list(namespace):
        if state["create_attempted"]:
            raise KubeError("模拟回查网络故障")
        return await real_list(namespace)

    kube.create_job = boom_create
    kube.get_job = boom_get_job
    kube.list_jobs = flaky_list

    def restore():
        del kube.create_job
        del kube.get_job
        del kube.list_jobs

    return restore


# ---------- 参数规则 ----------

async def test_round_params_must_come_together(harness):
    api = harness["api"]
    plan = await _plan(api, DAILY, datetime(2026, 11, 20, 1, 30, tzinfo=UTC))

    for endpoint in ("catchup", "reverify"):
        only_uid = await api.post(
            f"/namespaces/default/cronjobs/{DAILY}/{endpoint}",
            params={"cronjob_uid": plan["cronjob_uid"]},
        )
        assert only_uid.status_code == 422, (endpoint, only_uid.text)
        assert "一起提供" in only_uid.json()["detail"]

        only_ts = await api.post(
            f"/namespaces/default/cronjobs/{DAILY}/{endpoint}",
            params={"scheduled_at": plan["scheduled_at_utc"]},
        )
        assert only_ts.status_code == 422, (endpoint, only_ts.text)
        assert "一起提供" in only_ts.json()["detail"]


# ---------- 跨日提交：指定旧轮 ----------

async def test_pinned_old_round_returns_existing_job_cross_day(harness):
    """隔天提交旧轮次：返回旧轮的 Job，而不是建出当天的 Job。"""
    api = harness["api"]
    plan = await _plan(api, DAILY, datetime(2026, 11, 20, 1, 30, tzinfo=UTC))
    jobs_before = len(harness["state"]["jobs"])

    resp = await api.post(
        f"/namespaces/default/cronjobs/{DAILY}/catchup",
        params={
            "at": NEXT_DAY.isoformat(),
            "cronjob_uid": plan["cronjob_uid"],
            "scheduled_at": plan["scheduled_at_utc"],  # 2026-11-20T01:00:00Z
        },
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "returned_existing"
    assert body["job_source"] == "scheduler"
    # 回包指向旧轮，而非当天（2026-11-21T01:00:00Z）
    assert body["scheduled_at_utc"].replace("+00:00", "Z") == SEEDED_ROUND
    assert body["job"]["metadata"]["annotations"][
        "batch.kubernetes.io/job-name"
    ] == SEEDED_ROUND
    # 没有建出新 Job，也没有落台账
    assert len(harness["state"]["jobs"]) == jobs_before
    assert harness["store"].list_records() == []


async def test_omitted_params_fall_back_to_latest_round(harness):
    """省略两个参数时沿用最近一轮（事故中的旧行为，作为兼容保留）。"""
    api = harness["api"]
    resp = await api.post(
        f"/namespaces/default/cronjobs/{DAILY}/catchup",
        params={"at": NEXT_DAY.isoformat()},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "created"
    # 最近一轮是 2026-11-21 09:00 上海 = 01:00Z
    assert body["scheduled_at_utc"].endswith("2026-11-21T01:00:00Z")


async def test_pinned_expired_round_without_job_rejected(harness):
    """指定轮次无 Job 且已过 2 小时窗口：拒绝补建，不留台账。"""
    api = harness["api"]
    plan = await _plan(api, UTC_CJ, datetime(2026, 11, 20, 1, 0, tzinfo=UTC))
    jobs_before = len(harness["state"]["jobs"])

    resp = await api.post(
        f"/namespaces/default/cronjobs/{UTC_CJ}/catchup",
        params={
            "at": datetime(2026, 11, 20, 2, 0, tzinfo=UTC).isoformat(),
            "cronjob_uid": plan["cronjob_uid"],
            "scheduled_at": "2026-11-19T00:30:00Z",  # 前一天 00:30Z，已超 24h
        },
    )
    assert resp.status_code == 409, resp.text
    assert "补跑期限" in resp.json()["detail"]
    assert len(harness["state"]["jobs"]) == jobs_before
    assert harness["store"].list_records() == []


async def test_pinned_round_within_window_creates(harness):
    """指定轮次在窗口内：正常补建，回包与台账都落在指定轮次。"""
    api = harness["api"]
    plan = await _plan(api, UTC_CJ, datetime(2026, 11, 20, 1, 0, tzinfo=UTC))

    resp = await api.post(
        f"/namespaces/default/cronjobs/{UTC_CJ}/catchup",
        params={
            "at": datetime(2026, 11, 20, 1, 0, tzinfo=UTC).isoformat(),
            "cronjob_uid": plan["cronjob_uid"],
            "scheduled_at": "2026-11-20T00:30:00Z",
        },
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "created"
    assert body["scheduled_at_utc"].replace("+00:00", "Z") == "2026-11-20T00:30:00Z"
    assert body["job"]["metadata"]["annotations"][
        "inspection.example.com/scheduled-at"
    ] == "2026-11-20T00:30:00Z"

    # 再调一次：返回已建 Job，不重复创建
    resp2 = await api.post(
        f"/namespaces/default/cronjobs/{UTC_CJ}/catchup",
        params={
            "at": datetime(2026, 11, 20, 1, 30, tzinfo=UTC).isoformat(),
            "cronjob_uid": plan["cronjob_uid"],
            "scheduled_at": "2026-11-20T00:30:00Z",
        },
    )
    assert resp2.json()["status"] == "returned_existing"
    assert resp2.json()["job"]["metadata"]["name"] == body["job"]["metadata"]["name"]

    records = harness["store"].list_records()
    assert len(records) == 1
    assert records[0]["scheduled_at"] == "2026-11-20T00:30:00Z"
    assert records[0]["state"] == "created"


async def test_pinned_future_round_rejected(harness):
    api = harness["api"]
    plan = await _plan(api, UTC_CJ, datetime(2026, 11, 20, 1, 0, tzinfo=UTC))
    resp = await api.post(
        f"/namespaces/default/cronjobs/{UTC_CJ}/catchup",
        params={
            "at": datetime(2026, 11, 20, 1, 0, tzinfo=UTC).isoformat(),
            "cronjob_uid": plan["cronjob_uid"],
            "scheduled_at": "2026-11-21T00:30:00Z",  # 还没到点
        },
    )
    assert resp.status_code == 409
    assert "尚未到点" in resp.json()["detail"]


async def test_pinned_time_off_schedule_rejected(harness):
    """指定时刻不符合当前排程：拒绝补建。"""
    api = harness["api"]
    plan = await _plan(api, UTC_CJ, datetime(2026, 11, 20, 1, 0, tzinfo=UTC))
    resp = await api.post(
        f"/namespaces/default/cronjobs/{UTC_CJ}/catchup",
        params={
            "at": datetime(2026, 11, 20, 1, 0, tzinfo=UTC).isoformat(),
            "cronjob_uid": plan["cronjob_uid"],
            "scheduled_at": "2026-11-20T00:45:00Z",  # 排程是 00:30
        },
    )
    assert resp.status_code == 409
    assert "排程" in resp.json()["detail"]


async def test_pinned_round_after_reschedule(harness):
    """CronJob 改排程后：旧轮次不再符合排程 -> 拒绝补建；但已有 Job 仍可查回。"""
    api = harness["api"]
    plan = await _plan(api, UTC_CJ, datetime(2026, 11, 20, 1, 0, tzinfo=UTC))
    uid = plan["cronjob_uid"]
    # 改排程：00:30 -> 00:45
    harness["state"]["cronjobs"][("default", UTC_CJ)]["spec"]["schedule"] = "45 0 * * *"

    resp = await api.post(
        f"/namespaces/default/cronjobs/{UTC_CJ}/catchup",
        params={
            "at": datetime(2026, 11, 20, 1, 0, tzinfo=UTC).isoformat(),
            "cronjob_uid": uid,
            "scheduled_at": "2026-11-20T00:30:00Z",
        },
    )
    assert resp.status_code == 409
    assert "排程" in resp.json()["detail"]

    # 旧轮次其实已有调度器 Job：即使改了排程也应查回
    cj = harness["state"]["cronjobs"][("default", UTC_CJ)]
    planted = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": "inspection-utc-202611200030",
            "namespace": "default",
            "uid": "job-planted-old-round",
            "labels": {"batch.kubernetes.io/cronjob-name": UTC_CJ},
            "annotations": {
                "batch.kubernetes.io/cronjob-uid": uid,
                "batch.kubernetes.io/job-name": "2026-11-20T00:30:00Z",
            },
        },
        "spec": cj["spec"]["jobTemplate"]["spec"],
    }
    harness["state"]["jobs"][("default", planted["metadata"]["name"])] = planted

    resp2 = await api.post(
        f"/namespaces/default/cronjobs/{UTC_CJ}/catchup",
        params={
            "at": datetime(2026, 11, 20, 1, 0, tzinfo=UTC).isoformat(),
            "cronjob_uid": uid,
            "scheduled_at": "2026-11-20T00:30:00Z",
        },
    )
    assert resp2.status_code == 200
    body2 = resp2.json()
    assert body2["status"] == "returned_existing"
    assert body2["job"]["metadata"]["uid"] == "job-planted-old-round"


async def test_pinned_uid_mismatch_rejected(harness):
    """所传 UID 与当前 CronJob 不一致（对象已重建）：拒绝补建。"""
    api = harness["api"]
    resp = await api.post(
        f"/namespaces/default/cronjobs/{UTC_CJ}/catchup",
        params={
            "at": datetime(2026, 11, 20, 1, 0, tzinfo=UTC).isoformat(),
            "cronjob_uid": "uid-from-a-deleted-cronjob",
            "scheduled_at": "2026-11-20T00:30:00Z",
        },
    )
    assert resp.status_code == 409
    assert "不一致" in resp.json()["detail"]
    assert harness["store"].list_records() == []


# ---------- 并发与超时 ----------

async def test_pinned_concurrent_catchup_creates_single_job(harness):
    import asyncio

    api = harness["api"]
    plan = await _plan(api, UTC_CJ, datetime(2026, 11, 20, 1, 0, tzinfo=UTC))
    params = {
        "at": datetime(2026, 11, 20, 1, 0, tzinfo=UTC).isoformat(),
        "cronjob_uid": plan["cronjob_uid"],
        "scheduled_at": "2026-11-20T00:30:00Z",
    }

    async def call():
        return await api.post(
            f"/namespaces/default/cronjobs/{UTC_CJ}/catchup", params=params
        )

    results = await asyncio.gather(*(call() for _ in range(5)))
    statuses = [r.json()["status"] for r in results]
    assert statuses.count("created") == 1, statuses
    assert statuses.count("returned_existing") == 4, statuses
    job_names = {r.json()["job"]["metadata"]["name"] for r in results}
    assert len(job_names) == 1
    assert len(harness["store"].list_records()) == 1


@pytest.mark.mock_fault("create-timeout")
@pytest.mark.mock_slow(1.5)
async def test_pinned_create_timeout_verifies_original_round(harness):
    """创建回包超时：按指定轮次（而非当前最近一轮）回查确认。"""
    api = harness["api"]
    probe = datetime(2026, 11, 20, 12, 0, tzinfo=UTC)
    plan = await _plan(api, NY_CJ, probe)
    sched = datetime.fromisoformat(
        plan["scheduled_at_utc"].replace("Z", "+00:00")
    )
    at = sched + timedelta(minutes=20)

    resp = await api.post(
        f"/namespaces/default/cronjobs/{NY_CJ}/catchup",
        params={
            "at": at.isoformat(),
            "cronjob_uid": plan["cronjob_uid"],
            "scheduled_at": plan["scheduled_at_utc"],
        },
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "created"
    assert "超时" in body["reason"]
    assert body["scheduled_at_utc"].replace("+00:00", "Z") == plan[
        "scheduled_at_utc"
    ].replace("+00:00", "Z")
    record = harness["store"].list_records()[0]
    assert record["scheduled_at"] == body["scheduled_at_utc"].replace("+00:00", "Z")
    assert record["state"] == "created"


# ---------- 查不清 -> 待核实 ----------

async def test_list_failure_reports_pending_without_creating(harness):
    """创建前查不清是否已有 Job：报待核实，不落台账、不发起创建；恢复后可重试。"""
    api = harness["api"]
    kube = harness["kube"]
    called = {"create": False}
    real_create = kube.create_job

    async def spy_create(namespace, manifest):
        called["create"] = True
        return await real_create(namespace, manifest)

    async def boom_list(namespace):
        raise KubeError("模拟集群列表故障")

    kube.create_job = spy_create
    kube.list_jobs = boom_list
    try:
        resp = await api.post(
            f"/namespaces/default/cronjobs/{UTC_CJ}/catchup",
            params={"at": datetime(2026, 11, 20, 1, 0, tzinfo=UTC).isoformat()},
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["status"] == "pending_verification"
        assert "无法确认" in body["reason"]
        assert called["create"] is False  # 未发起创建
        assert harness["store"].list_records() == []  # 未落台账
    finally:
        del kube.create_job
        del kube.list_jobs

    # 集群恢复后重试：正常补建
    resp2 = await api.post(
        f"/namespaces/default/cronjobs/{UTC_CJ}/catchup",
        params={"at": datetime(2026, 11, 20, 1, 0, tzinfo=UTC).isoformat()},
    )
    assert resp2.json()["status"] == "created"


# ---------- 指定轮次回查 ----------

async def test_reverify_pinned_round_confirms_pending_record(harness):
    """待核实记录按指定轮次回查确认（即使该轮已过期）。"""
    api = harness["api"]
    plan = await _plan(api, UTC_CJ, datetime(2026, 11, 20, 1, 0, tzinfo=UTC))
    uid = plan["cronjob_uid"]
    at = datetime(2026, 11, 20, 1, 0, tzinfo=UTC)

    restore = _patch_verify_failure(harness["kube"])
    resp = await api.post(
        f"/namespaces/default/cronjobs/{UTC_CJ}/catchup",
        params={
            "at": at.isoformat(),
            "cronjob_uid": uid,
            "scheduled_at": "2026-11-20T00:30:00Z",
        },
    )
    assert resp.json()["status"] == "pending_verification"
    restore()

    # 运维确认：控制器其实建成功了，把 Job 补写进集群
    record = harness["store"].list_records()[0]
    cj = harness["state"]["cronjobs"][("default", UTC_CJ)]
    sched = datetime.fromisoformat(record["scheduled_at"].replace("Z", "+00:00"))
    manifest = build_catchup_job(cj, sched, record["job_name"])
    manifest["metadata"]["uid"] = "uid-confirmed"
    harness["state"]["jobs"][("default", record["job_name"])] = manifest

    # 隔天按原轮次回查（已远超 2 小时窗口，仍应查回）
    resp2 = await api.post(
        f"/namespaces/default/cronjobs/{UTC_CJ}/reverify",
        params={
            "at": NEXT_DAY.isoformat(),
            "cronjob_uid": uid,
            "scheduled_at": "2026-11-20T00:30:00Z",
        },
    )
    assert resp2.status_code == 200, resp2.text
    body2 = resp2.json()
    assert body2["status"] == "created"
    assert body2["job"]["metadata"]["uid"] == "uid-confirmed"
    assert body2["scheduled_at_utc"].replace("+00:00", "Z") == "2026-11-20T00:30:00Z"
    assert harness["store"].list_records()[0]["state"] == "created"


async def test_reverify_pinned_without_record_checks_cluster(harness):
    """无台账记录时，reverify 仍按指定轮次查集群：有 Job 返回，没有报 404。"""
    api = harness["api"]
    plan = await _plan(api, DAILY, datetime(2026, 11, 20, 1, 30, tzinfo=UTC))

    # 种子 Job 所在轮次：直接查回（不需要台账）
    resp = await api.post(
        f"/namespaces/default/cronjobs/{DAILY}/reverify",
        params={
            "cronjob_uid": plan["cronjob_uid"],
            "scheduled_at": SEEDED_ROUND,
        },
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "returned_existing"
    assert body["job_source"] == "scheduler"
    assert body["scheduled_at_utc"].replace("+00:00", "Z") == SEEDED_ROUND

    # 没有 Job 的轮次：404
    resp2 = await api.post(
        f"/namespaces/default/cronjobs/{DAILY}/reverify",
        params={
            "cronjob_uid": plan["cronjob_uid"],
            "scheduled_at": "2026-11-19T01:00:00Z",
        },
    )
    assert resp2.status_code == 404


async def test_pinned_reverify_after_restart_recovery(harness):
    """服务重启后 creating 记录转待核实，仍可按指定轮次查回。"""
    api = harness["api"]
    plan = await _plan(api, UTC_CJ, datetime(2026, 11, 20, 1, 0, tzinfo=UTC))
    params = {
        "at": datetime(2026, 11, 20, 1, 0, tzinfo=UTC).isoformat(),
        "cronjob_uid": plan["cronjob_uid"],
        "scheduled_at": "2026-11-20T00:30:00Z",
    }
    resp = await api.post(
        f"/namespaces/default/cronjobs/{UTC_CJ}/catchup", params=params
    )
    assert resp.json()["status"] == "created"
    job_name = resp.json()["job"]["metadata"]["name"]

    # 模拟进程在创建中途崩溃：记录打回 creating，重启恢复
    record = harness["store"].list_records()[0]
    with harness["store"]._lock:
        harness["store"]._conn.execute(
            "UPDATE catchup_records SET state='creating' WHERE id=?",
            (record["id"],),
        )
        harness["store"]._conn.commit()
    recovered = harness["store"].recover_creating()
    assert recovered[0]["state"] == "pending_verification"

    # 隔天按原轮次回查
    resp2 = await api.post(
        f"/namespaces/default/cronjobs/{UTC_CJ}/reverify",
        params={
            "at": NEXT_DAY.isoformat(),
            "cronjob_uid": plan["cronjob_uid"],
            "scheduled_at": "2026-11-20T00:30:00Z",
        },
    )
    assert resp2.status_code == 200
    assert resp2.json()["status"] == "created"
    assert resp2.json()["job"]["metadata"]["name"] == job_name
