# CronJob 巡检补跑服务（FastAPI + Kubernetes 1.32+）

检修期间 CronJob 被暂停（`spec.suspend: true`），值班员需要核实“最近一轮到点的巡检”有没有漏跑，
漏了就按原模板补一个 Job。本服务把这套流程做成 HTTP API，并附带一个本地 Kubernetes API 模拟服务，
可复现 **Job 已创建但创建回包超时** 的场景。

## 功能范围

1. **查最近一次计划**
   - 只接受显式设置 `spec.timeZone`、且为**每天固定时刻**（日/月/星期均为通配）的 CronJob。
   - 自后向前扫描最近一次应到点的计划时刻（UTC 返回）。
   - 夏令时处理：
     - 回拨产生的**重复时刻取第一次**（`fold=0`）；
     - 春进产生的**不存在时刻直接跳过**，不取平移后的时间。
2. **按 (CronJob UID, 计划时刻) 匹配已有 Job**
   - 兼容 Kubernetes 1.32 的新注解 `batch.kubernetes.io/cronjob-uid` / `job-name`，
     也兼容旧的 `kubernetes.io/controller-uid` / `scheduled-at`；
   - 原调度器创建的 Job 与本服务补建的 Job 一视同仁。
3. **轮次锁定（`/catchup`、`/reverify`）**
   - 调用方可把查计划返回的 `cronjob_uid` 与 `scheduled_at_utc` 作为
     `cronjob_uid` / `scheduled_at` 参数回传，锁定要处理的轮次；
     **两项须一起提供**，省略时沿用最近一轮（`at` 仍表示演练当前时间）；
   - 指定轮次后先查台账与集群：**找到匹配 Job 即返回**，即使已过期、
     服务重启或 CronJob 已改排程；
   - 确认该轮没有 Job 后，仅当 **原 CronJob 仍暂停**、**所选时刻仍符合当前排程**、
     **已到点** 且 **到点后 2 小时内（UTC）** 才按 `jobTemplate` 补建 Job；
   - **查不清是否已有 Job 时报 `pending_verification`**，绝不盲目补建；
   - 所传 UID 与当前 CronJob 不一致（对象已删除重建）时拒绝补建。
4. **补跑可靠性**
   - 同轮（同 CronJob UID + 同计划时刻）并发只允许建一个（进程内异步锁 + SQLite 唯一约束双保险）；
   - 补建 Job 带 `inspection.example.com/catchup=true` 标签和 UID/计划时刻注解。
5. **超时与可靠性**
   - 创建请求回包超时：先按 Job 名回查，再按 (UID, 计划时刻) 列查，**始终按原轮次核实**；
   - 回查能确认 → 正常返回创建结果；查不清 → 返回 `pending_verification`（HTTP 200），提示人工核实；
   - 补跑台账持久化在 SQLite，**进程重启后**创建到一半的记录自动转为待核实，可通过 `/reverify` 查回；
   - 超过 2 小时补跑期限后，计划与历史 Job/台账仍可查询，只是不能再新建。
6. **演练支持**：所有接口支持 `at` 查询参数（ISO 8601，可带时区），在 Swagger 上即可指定演练时间。

## 目录结构

```
app/
  schedule.py   # cron 解析、时区/DST 计算
  kube.py       # Kubernetes batch/v1 客户端、Job 匹配与清单构造
  store.py      # SQLite 台账 + 同轮并发锁
  service.py    # 补跑编排（校验、创建、超时回查、状态机）
  models.py     # Swagger 响应模型
  main.py       # FastAPI 入口
mock_k8s/
  server.py     # 本地 Kubernetes API 模拟（含故障注入）
tests/          # pytest：调度/DST、接口、并发、超时重试、重启恢复
docker-compose.yml
```

## 用 Docker Compose 启动

```bash
docker compose up --build
```

- 模拟 K8s API：<http://localhost:8001>（文档内路径 `/docs`）
- 巡检补跑 API：<http://localhost:8080/docs>
- Compose 默认注入 `KUBE_MOCK_FAULT=create-timeout` 且客户端创建超时 3s、模拟端滞留 8s，
  因此每次补跑都会稳定复现“**创建成功后回包超时 → 回查确认**”。
  置空该环境变量即恢复正常路径。

### 模拟服务故障注入（请求头 `X-Mock-Fault`）

| 值 | 行为 |
|---|---|
| `create-timeout` | Job 先落库，再滞留到客户端超时后返回 504 |
| `create-timeout-lose` | Job 先落库，然后直接断开连接（无任何回包） |
| `create-conflict-once` | 首次创建返回 409，第二次正常 |

## 不用容器的本地运行方式

```bash
pip install -r requirements.txt
# 终端 1：模拟 K8s
uvicorn mock_k8s.server:app --port 8001
# 终端 2：巡检服务
KUBE_API_URL=http://127.0.0.1:8001 CATCHUP_DB=./data/catchup.db \
  uvicorn app.main:app --port 8080
```

## 典型调用（Swagger 也可点选）

种子 CronJob `inspection-daily`：`0 9 * * *`，时区 `Asia/Shanghai`，暂停；
模拟端已预置一轮原调度器 Job（计划时刻 `2026-11-20T01:00:00Z`）。

```bash
# 查最近一轮计划与已有 Job
curl "http://localhost:8080/namespaces/default/cronjobs/inspection-daily/latest-schedule?at=2026-11-20T01:30:00Z"

# 补跑（有 Job 则返回原 Job）
curl -X POST "http://localhost:8080/namespaces/default/cronjobs/inspection-ny/catchup?at=2026-11-20T06:00:00Z"

# 对待核实记录重新回查
curl -X POST "http://localhost:8080/namespaces/default/cronjobs/inspection-ny/reverify?at=2026-11-20T06:00:00Z"

# 台账
curl "http://localhost:8080/records"
```

## 跨日提交与回查演练（Swagger / curl 均可）

事故场景：值班员发现 **11-20 那轮**漏跑，**11-21 才提交**。修复前服务只按 `at`
算“最近一轮”，会建出 11-21（当天）的 Job；现在把查计划返回的
`cronjob_uid` + `scheduled_at_utc` 回传即可锁定旧轮。

```bash
# 1) 查 11-20 当天的计划，拿到 cronjob_uid 与 scheduled_at_utc
curl "http://localhost:8080/namespaces/default/cronjobs/inspection-daily/latest-schedule?at=2026-11-20T01:30:00Z"
#   -> cronjob_uid=cronjob-…  scheduled_at_utc=2026-11-20T01:00:00Z（该轮已有调度器 Job）

# 2) 隔天提交：带上两个返回值 + 当前演练时间 at=2026-11-21T02:00:00Z
curl -X POST "http://localhost:8080/namespaces/default/cronjobs/inspection-daily/catchup" \
  --data-urlencode "at=2026-11-21T02:00:00Z" \
  --data-urlencode "cronjob_uid=<第1步的 cronjob_uid>" \
  --data-urlencode "scheduled_at=2026-11-20T01:00:00Z" -G
#   -> returned_existing：返回 11-20 那轮的 Job（即使已过期也不重复创建）
#   对比：省略这两个参数 -> 按最近一轮处理，会建出 11-21（当天）的 Job，即修复前的事故行为

# 3) 过期拒绝：inspection-utc（UTC 00:30，无预置 Job）指定前天轮次
curl -X POST "http://localhost:8080/namespaces/default/cronjobs/inspection-utc/catchup" \
  --data-urlencode "at=2026-11-20T02:00:00Z" \
  --data-urlencode "cronjob_uid=<inspection-utc 的 uid>" \
  --data-urlencode "scheduled_at=2026-11-19T00:30:00Z" -G
#   -> 409 已超过补跑期限

# 4) 按指定轮次回查（无台账记录时也会直接查集群）
curl -X POST "http://localhost:8080/namespaces/default/cronjobs/inspection-daily/reverify" \
  --data-urlencode "cronjob_uid=<第1步的 cronjob_uid>" \
  --data-urlencode "scheduled_at=2026-11-20T01:00:00Z" -G
#   -> returned_existing：回查指向 11-20 那轮，而不是当天

# 5)（可选）模拟 CronJob 改排程：旧轮次不再符合排程 -> 拒绝补建，但已有 Job 仍可查回
curl -X POST "http://localhost:8001/_mock/cronjobs/default/inspection-utc/reschedule?schedule=45%200%20*%20*%20*"
```

在 Swagger（<http://localhost:8080/docs>）上操作：`POST …/catchup` /
`POST …/reverify` 的 `cronjob_uid`、`scheduled_at` 两个查询参数留空即沿用最近一轮。

## 测试

```bash
pytest
```

覆盖：cron 解析与 DST（纽约/柏林/上海/东京）、已有 Job 直接返回、补建成功、
暂停/窗口/时区/非每日表达式拦截、5 路并发只建一个、创建超时回查、查不清待核实、重启恢复查回、
轮次锁定（跨日查回旧轮、过期/未到点/改排程/UID 不符拒绝、参数必须成对、指定轮次并发与超时回查）。

## 已知边界 / 未完成项

- 鉴权仅支持 Bearer Token（`KUBE_TOKEN`），未做 mTLS 与 in-cluster ServiceAccount 自动挂载；
- 并发锁是单进程内的 asyncio 锁，多副本部署时只有 SQLite 唯一约束兜底
  （多副本需换共享数据库或带 CAS 的外部锁）；
- 模拟服务只实现了本项目用到的 batch/v1 接口子集，不是完整 apiserver；
- 未实现真实集群的集成测试（环境内无 Docker/集群），集成路径以模拟服务验证。
