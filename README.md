# sample-fragment-service

保留触发前后样本的 HTTP 服务（Python + FastAPI，状态仅存于单进程内存）。

- 创建会话时指定历史长度 `H` 与样本容量 `C`（`1 ≤ H ≤ C ≤ 1000`）。
- 逐条写入整数样本，序号从 1 开始连续编号。
- 触发以**当前末条**为中心生成片段：前 `before` 条 + 中心 + 后 `after` 条
  （`before`、`after` 为非负整数，`before < H`，且 `before + 1 + after ≤ C`）。
  历史不足返回 `409`；`after > 0` 时片段处于 `pending`，后续样本到齐后变为 `completed`。
- 片段可重叠。查询片段返回 `pending` / `completed` 状态，以及按序号升序排列的序号和值。
- **容量核算**：最近 `H` 条历史与所有未删除片段引用的样本共同占用容量 `C`，
  同一序号只计一次（并集）；仅回收两者都不再需要的样本。
  新样本无处容纳时写入返回 `409`，**不消耗序号、不改变历史或片段**。
- 删除片段只解除该片段的引用，回收后写入可重试；已完成片段在删除前内容不变。
- **运行中调整 H/C**：`PUT /sessions/{sid}/limits` 不停采样地同时调整 `history` 与 `capacity`
  （必须是整数，`1 ≤ H ≤ C ≤ 1000`；布尔值、字符串、浮点等非整数返回 `422`）。
  新历史窗口要求的最近 `min(H, 已写入条数)` 条必须全部仍存活，且所有未删除片段的总长、
  新历史与片段已写入部分的序号并集均不得超过新 `C`，否则返回 `409` 且**不改变任何状态**。
  成功则立即按新参数回收不再需要的样本；片段及其内容、样本与片段编号、模式配置、
  匹配进度与命中记录均不变，**扩容不会恢复已回收的样本**。旧片段的 `before` 即使
  不小于新 `H` 也原样保留，仅后续触发按新参数校验。
- 会话内所有操作按会话级锁串行执行，等效于某个串行顺序。
- **整数模式识别（可选）**：会话可在**首条样本写入前**配置一次长度 `1～32` 的整数
  pattern，此后按成功写入的连续样本值精确匹配，**允许重叠**（pattern 可以长于 `H` 和 `C`）。
  识别只保存流式自动机的标量状态，**不保存原始样本、不占用样本容量**；片段触发、片段删除、
  样本回收都不改变识别进度，命中记录在回收后依然保留。容量拒绝不推进匹配状态，重试成功只识别一次。
- **命中留样（可选）**：已配置 pattern 且尚未写入样本的会话可经
  `POST /sessions/{sid}/hit-fragments/enable` 启用一次（未配置 pattern、已有样本、重复启用、
  模式长度超过 `C` 均返回 `409`，未知会话 `404`）。启用后**每次命中自动用现有片段编号生成
  `completed` 片段**（`center`=命中末条序号、`before`=模式长度减 1、`after`=0，允许重叠，
  不受手动触发 `before < H` 限制），**即使模式长于 `H` 也可查询原值**；流尾匹配 pattern
  真前缀的最长后缀（**临时前缀**，即自动机标量状态对应的流尾区间）也须保留。
  **历史、未删除片段与临时前缀按序号并集占用容量**；写入按本条匹配后的状态核算与回收，
  超容返回 `409` 且**样本、样本与片段编号、匹配进度、命中记录与片段均不变**；
  删除片段同样保护临时前缀；调参还须保证新 `C` 容得下完整模式及上述并集，否则 `409`
  且不改变任何状态。未启用时原有行为完全不变。
- **会话分叉**：`POST /sessions/{sid}/fork` 把当前采样会话复制为一条可独立续采的新会话。
  复制当时的 `H`、`C`、存活样本、全部未删除片段及其编号、下一样本与片段编号，以及模式配置、
  自动机匹配进度、累计命中数、最近 100 次命中记录和命中留样启用状态（临时前缀随自动机状态
  一并复制）；**已回收样本不恢复，也不重放样本重新识别**（直接复制自动机标量状态）。分叉在来源会话锁内取号并快照，等效于与来源已有操作串行执行的某个
  时刻，**不会混合两个时刻的状态**；来源本身保持不变。每次分叉都产生不同的、且与普通创建不冲突
  的新 `session_id`（共用同一个递增 ID 计数器），来源不存在返回 `404`，成功返回含新 `session_id`
  的**原会话查询结果**。分叉后两边的写入、触发、删除、参数调整和模式操作**互不影响**，各自继续
  遵守原容量与校验规则；待完成片段在各分支分别等待各自的后续样本。不提供合并，也不落盘。
- **批量写入（成组提交）**：`POST /sessions/{sid}/samples/batch` 一次提交 `values` 数组
  （1～100 个整数；布尔值、字符串、浮点数等非整数返回 `422`，未知会话 `404`，非法输入不改变任何状态）。
  **整批接收或整批拒绝，不留下部分写入**：批内按输入顺序逐条应用与单条写入完全相同的
  容量、匹配、留样与回收规则，**每一步都按当时状态检查容量**（而非只核算批末）。
  全部成功返回 `201` 与每项的 `seq`/`value`（按输入顺序排列，**批内已被回收的样本也返回**）；
  任一步超容则整批 `409`，响应体带首个失败项从 1 起的 `index`，且样本（含本批回收的旧样本）、
  片段、样本与片段编号、匹配进度与命中记录**均恢复批前状态**。整批在会话锁内一次完成，
  等效于与会话内其他操作的一次串行执行：其他操作不能插入批内，查询与分叉只能看到批前或批后状态。
- **撤销最近一次写入**：`POST /sessions/{sid}/undo` 撤销会话最近一次成功的单条或整批采样写入，
  用于纠正已接收的误采样。每会话仅保留**最近一次成功写入前**的状态（整批以**首项写入前**为准）：
  撤销恢复存活样本（**含该次写入时回收的旧样本**）、全部片段、样本与片段两类编号、匹配进度与
  命中记录；**不重放样本**，撤销后记录清空（再次撤销返回 `409`），后续写入从恢复的序号继续。
  **成功触发、删除片段、调参、配置模式或启用留样都会使记录失效**；失败请求（含容量拒绝与
  整批回滚）、查询与分叉不影响记录，**新分支不继承记录**。记录仅存内存且不计入样本容量；
  撤销与同会话其他操作串行，查询不会看到部分恢复。未知会话 `404`，无可撤销记录 `409`，
  成功返回 `200` 与原会话查询格式。
- **SSE 会话快照订阅**：`GET /sessions/{sid}/stream` 以 `text/event-stream` 持续推送会话状态，
  无需轮询（未知会话在建流前返回 `404`，不提供历史重放）。每帧为 `event: snapshot`，
  `data` 是含 `revision` 与 `state` 的 JSON：`state` 含**原会话查询字段**（同 `GET /sessions/{sid}`）、
  **按序号排列的存活样本**（`samples`）、**全部片段详情**（`fragment_details`，含各片段当前可查的
  样本内容），以及**模式与命中留样配置和命中结果**（`pattern`：`configured`/`pattern`/
  `hit_fragments_enabled`/`hit_count`/`recent_hits`）。首帧取订阅时刻的完整快照，此后每次成功
  修改各发一帧完整快照，衔接处不漏更新。`revision` 从 `0` 开始：成功写入（**整批只加 1**）、
  撤销、触发、删除、调参、配置模式及启用留样各加 `1`；查询、失败请求与来源分叉不增加，
  **新分支从 0 开始**；`revision` **不随撤销回退**，在后续帧中严格递增，帧内不会含批内或
  回滚前状态。每个订阅最多缓存一条待发快照（新快照合并掉未发出的旧快照），停止修改后继续
  读取必能收到最新状态；慢订阅者不阻塞采样，发送不占用会话锁，断连即释放订阅。

## 启动

单条命令（Docker Compose）：

```bash
docker compose up --build
```

- 访问地址：http://localhost:8000
- 交互式接口文档（Swagger UI）：http://localhost:8000/docs
- 端口：`8000`（宿主机 `8000` → 容器 `8000`，可在 `docker-compose.yml` 中调整）

也可以只用 Docker：

```bash
docker build -t sample-fragment-service .
docker run --rm -p 8000:8000 sample-fragment-service
```

## 接口一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/sessions` | 创建会话，指定 `history`(H)、`capacity`(C) |
| GET | `/sessions/{sid}` | 查看会话状态（已用容量、存活序号、片段列表） |
| GET | `/sessions/{sid}/stream` | SSE 订阅会话快照：每帧 `event: snapshot`，`data` 含 `revision` 与完整 `state`（未知会话 `404`） |
| POST | `/sessions/{sid}/fork` | 分叉出一条独立续采的新会话，返回含新 `session_id` 的会话状态（来源不存在返回 `404`） |
| PUT | `/sessions/{sid}/limits` | 不停采样地调整 `history`(H) 与 `capacity`(C)，返回会话状态 |
| POST | `/sessions/{sid}/samples` | 写入一条整数样本 |
| POST | `/sessions/{sid}/samples/batch` | 成组批量写入 1～100 个整数样本：整批接收或整批拒绝，超容返回 `409` 与首个失败项的 `index` |
| POST | `/sessions/{sid}/undo` | 撤销最近一次成功的单条或整批写入，返回会话状态（无可撤销记录返回 `409`） |
| PUT | `/sessions/{sid}/pattern` | 在首条样本写入前配置识别 pattern（1～32 个整数，仅一次） |
| GET | `/sessions/{sid}/pattern/hits` | 查询累计命中数与最近 100 次命中的 `start_seq`/`end_seq` |
| POST | `/sessions/{sid}/hit-fragments/enable` | 启用命中留样：每次模式命中自动生成 `completed` 留样片段（每会话一次） |
| POST | `/sessions/{sid}/triggers` | 以当前末条为中心触发片段 |
| GET | `/sessions/{sid}/fragments` | 列出所有未删除片段 |
| GET | `/sessions/{sid}/fragments/{fid}` | 查询片段状态与样本内容 |
| DELETE | `/sessions/{sid}/fragments/{fid}` | 删除片段（解除引用并回收样本） |

错误码：`404` 会话/片段不存在；`409` 历史不足、容量不足、pattern 重复配置或已有样本后配置、
查询未配置 pattern 的会话、调整参数与存活样本或片段冲突（新历史窗口含已回收样本、
未删除片段总长或所需序号并集超过新 `C`）、命中留样启用条件不满足（未配置 pattern、已有样本、
重复启用、模式长度超过 `C`）、启用命中留样后写入或调参超容（含新 `C` 容不下完整模式）、
批量写入任一步超容（响应体带首个失败项从 1 起的 `index`，整批回滚）、
撤销时无可撤销记录（最近一次成功写入的记录不存在或已失效）；
`400` 触发参数违反 `before < H` 或 `before+1+after ≤ C`；`422` 请求体不合法
（含 pattern 长度非法、元素为布尔值或字符串，
调整参数为布尔值/非整数或不满足 `1 ≤ H ≤ C ≤ 1000`，
以及批量 `values` 非 1～100 个整数或含布尔值/字符串/浮点数）。

## 调用示例

### 1. 创建会话（H=2，C=3）

```bash
curl -s -X POST http://localhost:8000/sessions \
  -H 'Content-Type: application/json' \
  -d '{"history": 2, "capacity": 3}'
# {"session_id":1,"history":2,"capacity":3}
```

### 2. 逐条写入样本

```bash
for v in 10 20 30; do
  curl -s -X POST http://localhost:8000/sessions/1/samples \
    -H 'Content-Type: application/json' -d "{\"value\": $v}"; echo
done
# {"seq":1,"value":10}
# {"seq":2,"value":20}
# {"seq":3,"value":30}
```

历史窗口只保留最近 H=2 条，序号 1 已被回收：

```bash
curl -s http://localhost:8000/sessions/1
# {"session_id":1,"history":2,"capacity":3,"next_seq":4,"used":2,"stored_seqs":[2,3],"fragments":[]}
```

### 3. 触发片段（中心=当前末条 3，前 1 条 + 中心 + 后 1 条）

```bash
curl -s -X POST http://localhost:8000/sessions/1/triggers \
  -H 'Content-Type: application/json' \
  -d '{"before": 1, "after": 1}'
# {"fragment_id":1,"center":3,"before":1,"after":1,"start":2,"end":4,"status":"pending"}
```

### 4. 查询片段：pending → completed

```bash
curl -s http://localhost:8000/sessions/1/fragments/1
# {"fragment_id":1,...,"status":"pending","samples":[{"seq":2,"value":20},{"seq":3,"value":30}]}

curl -s -X POST http://localhost:8000/sessions/1/samples \
  -H 'Content-Type: application/json' -d '{"value": 40}'
# {"seq":4,"value":40}   —— 片段所需的后续样本到齐

curl -s http://localhost:8000/sessions/1/fragments/1
# {"fragment_id":1,...,"status":"completed",
#  "samples":[{"seq":2,"value":20},{"seq":3,"value":30},{"seq":4,"value":40}]}
```

### 5. 容量不足 → 409，删除后重试成功

片段 1 仍引用序号 2–4，历史窗口需要 3–4，并集 {2,3,4} 已占满 C=3，
再写一条需要 {2,3,4,5} 共 4 个位置，超出容量：

```bash
curl -s -o /dev/null -w '%{http_code}\n' -X POST http://localhost:8000/sessions/1/samples \
  -H 'Content-Type: application/json' -d '{"value": 50}'
# 409   —— 序号未消耗，next_seq 仍为 5

curl -s -X DELETE http://localhost:8000/sessions/1/fragments/1 -o /dev/null -w '%{http_code}\n'
# 204   —— 解除引用，序号 2 被回收

curl -s -X POST http://localhost:8000/sessions/1/samples \
  -H 'Content-Type: application/json' -d '{"value": 50}'
# {"seq":5,"value":50}   —— 回收后重试成功，序号连续
```

### 6. 历史不足 → 409

```bash
curl -s -X POST http://localhost:8000/sessions \
  -H 'Content-Type: application/json' -d '{"history": 2, "capacity": 3}'
# {"session_id":2,...}

curl -s -X POST http://localhost:8000/sessions/2/triggers \
  -H 'Content-Type: application/json' -d '{"before": 1, "after": 0}'
# 409 {"detail":"历史不足：已有样本不足以覆盖中心及其前 before 条"}
```

### 7. 配置识别 pattern（首条样本写入前，仅一次）

```bash
curl -s -X POST http://localhost:8000/sessions \
  -H 'Content-Type: application/json' -d '{"history": 2, "capacity": 3}'
# {"session_id":3,"history":2,"capacity":3}

curl -s -X PUT http://localhost:8000/sessions/3/pattern \
  -H 'Content-Type: application/json' -d '{"pattern": [1, 1]}'
# {"pattern":[1,1]}
```

非法配置返回 `422`（长度须为 1～32，元素必须是整数，布尔值/字符串均拒绝）；
重复配置、或已有样本后配置返回 `409`；未知会话返回 `404`：

```bash
curl -s -o /dev/null -w '%{http_code}\n' -X PUT http://localhost:8000/sessions/3/pattern \
  -H 'Content-Type: application/json' -d '{"pattern": []}'
# 422

curl -s -o /dev/null -w '%{http_code}\n' -X PUT http://localhost:8000/sessions/3/pattern \
  -H 'Content-Type: application/json' -d '{"pattern": [1, true, "x"]}'
# 422

curl -s -o /dev/null -w '%{http_code}\n' -X PUT http://localhost:8000/sessions/3/pattern \
  -H 'Content-Type: application/json' -d '{"pattern": [1, 1]}'
# 409   —— 重复配置
```

### 8. 重叠命中与查询

连续写入三个 `1`，pattern `[1,1]` 允许重叠，命中两次：`(1,2)` 与 `(2,3)`。
识别不依赖存活样本——序号 1 虽已被历史窗口回收，命中记录仍保留：

```bash
for v in 1 1 1; do
  curl -s -X POST http://localhost:8000/sessions/3/samples \
    -H 'Content-Type: application/json' -d "{\"value\": $v}" > /dev/null
done

curl -s http://localhost:8000/sessions/3/pattern/hits
# {"pattern":[1,1],"hit_count":2,
#  "recent_hits":[{"start_seq":1,"end_seq":2},{"start_seq":2,"end_seq":3}]}
```

返回 `hit_count`（累计命中总数，初始为 0）和 `recent_hits`（最近 100 次命中，
按 `start_seq` 升序，初始为空列表）。未配置 pattern 的会话查询返回 `409`：

```bash
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8000/sessions/2/pattern/hits
# 409
```

### 9. 容量拒绝后重试：只识别一次

容量不足的写入返回 `409` 时匹配状态不推进、不产生命中；解除引用、回收后用相同值
重试成功，该样本只参与一次识别：

```bash
# 会话 3：片段引用序号 2–4，历史窗口需要 3–4，并集占满 C=3
curl -s -X POST http://localhost:8000/sessions/3/triggers \
  -H 'Content-Type: application/json' -d '{"before": 1, "after": 1}' > /dev/null
curl -s -X POST http://localhost:8000/sessions/3/samples \
  -H 'Content-Type: application/json' -d '{"value": 2}' > /dev/null   # seq=4

# seq=5 需要 4 个位置 -> 409；被拒绝的值不会进入识别自动机
curl -s -o /dev/null -w '%{http_code}\n' -X POST http://localhost:8000/sessions/3/samples \
  -H 'Content-Type: application/json' -d '{"value": 1}'
# 409

curl -s -X DELETE http://localhost:8000/sessions/3/fragments/1 -o /dev/null -w '%{http_code}\n'
# 204   —— 回收后重试成功，pattern [1,1] 会在后续样本流中继续匹配，且不会补算被拒绝的写入
curl -s -X POST http://localhost:8000/sessions/3/samples \
  -H 'Content-Type: application/json' -d '{"value": 1}'
# {"seq":5,"value":1}
```

### 10. 运行中调整 H/C：成功收缩

会话 4 以 `H=3, C=5` 创建并写入 5 条样本，历史窗口只保留最近 3 条：

```bash
curl -s -X POST http://localhost:8000/sessions \
  -H 'Content-Type: application/json' -d '{"history": 3, "capacity": 5}'
# {"session_id":4,"history":3,"capacity":5}

for v in 10 20 30 40 50; do
  curl -s -X POST http://localhost:8000/sessions/4/samples \
    -H 'Content-Type: application/json' -d "{\"value\": $v}" > /dev/null
done

curl -s http://localhost:8000/sessions/4
# {"session_id":4,"history":3,"capacity":5,"next_seq":6,"used":3,"stored_seqs":[3,4,5],"fragments":[]}
```

收缩为 `H=2, C=3`：新窗口 `{4,5}` 全部存活、无片段、并集大小 2 ≤ 3，校验通过，
立即按新参数回收序号 3，返回会话查询格式：

```bash
curl -s -X PUT http://localhost:8000/sessions/4/limits \
  -H 'Content-Type: application/json' -d '{"history": 2, "capacity": 3}'
# {"session_id":4,"history":2,"capacity":3,"next_seq":6,"used":2,"stored_seqs":[4,5],"fragments":[]}
```

### 11. 扩大历史失败 → 409，状态不变

会话 4 的序号 1–3 已被回收。把 `H` 扩大为 4 需要序号 2–5 全部存活，
其中 2、3 已不存在，返回 `409` 且会话参数与样本保持原样：

```bash
curl -s -o /dev/null -w '%{http_code}\n' -X PUT http://localhost:8000/sessions/4/limits \
  -H 'Content-Type: application/json' -d '{"history": 4, "capacity": 5}'
# 409   —— 新历史窗口包含已回收样本，拒绝且不改变任何状态

curl -s http://localhost:8000/sessions/4
# {"session_id":4,"history":2,"capacity":3,"next_seq":6,"used":2,"stored_seqs":[4,5],"fragments":[]}
```

布尔值、字符串等非整数参数返回 `422`，不存在的会话返回 `404`：

```bash
curl -s -o /dev/null -w '%{http_code}\n' -X PUT http://localhost:8000/sessions/4/limits \
  -H 'Content-Type: application/json' -d '{"history": true, "capacity": 3}'
# 422

curl -s -o /dev/null -w '%{http_code}\n' -X PUT http://localhost:8000/sessions/99/limits \
  -H 'Content-Type: application/json' -d '{"history": 1, "capacity": 1}'
# 404
```

### 12. 扩容后重试写入

会话 5（`H=2, C=3`）的片段 1 引用序号 2–4，与历史窗口的并集占满 `C=3`，
写入第 5 条被 `409` 拒绝（序号未消耗）。把 `C` 扩容为 4 后重试成功；
扩容只放宽上限，不会恢复已回收的序号 1：

```bash
curl -s -X POST http://localhost:8000/sessions \
  -H 'Content-Type: application/json' -d '{"history": 2, "capacity": 3}'
# {"session_id":5,"history":2,"capacity":3}

for v in 10 20 30; do
  curl -s -X POST http://localhost:8000/sessions/5/samples \
    -H 'Content-Type: application/json' -d "{\"value\": $v}" > /dev/null
done
curl -s -X POST http://localhost:8000/sessions/5/triggers \
  -H 'Content-Type: application/json' -d '{"before": 1, "after": 1}' > /dev/null
curl -s -X POST http://localhost:8000/sessions/5/samples \
  -H 'Content-Type: application/json' -d '{"value": 40}' > /dev/null   # seq=4，片段 completed

curl -s -o /dev/null -w '%{http_code}\n' -X POST http://localhost:8000/sessions/5/samples \
  -H 'Content-Type: application/json' -d '{"value": 50}'
# 409   —— 并集 {2,3,4,5} 超出 C=3，next_seq 仍为 5

curl -s -X PUT http://localhost:8000/sessions/5/limits \
  -H 'Content-Type: application/json' -d '{"history": 2, "capacity": 4}'
# {"session_id":5,"history":2,"capacity":4,"next_seq":5,"used":3,"stored_seqs":[2,3,4],"fragments":[1]}

curl -s -X POST http://localhost:8000/sessions/5/samples \
  -H 'Content-Type: application/json' -d '{"value": 50}'
# {"seq":5,"value":50}   —— 扩容后重试成功，序号连续，片段内容与编号不变
```

### 13. 分叉会话，两支分别独立续采

把会话 5 在当前时刻（`next_seq=6`，片段 1 已完成，引用序号 2–4）分叉为新会话。
新 ID 与普通创建共用同一递增计数器，故每次分叉得到不同且不冲突的 ID；返回值是
**新会话**的原会话查询结果（除 `session_id` 外与来源一致）：

```bash
curl -s -X POST http://localhost:8000/sessions/5/fork
# {"session_id":6,"history":2,"capacity":4,"next_seq":6,"used":4,
#  "stored_seqs":[2,3,4,5],"fragments":[1]}

curl -s -X POST http://localhost:8000/sessions/5/fork
# {"session_id":7,"history":2,"capacity":4,"next_seq":6,"used":4,
#  "stored_seqs":[2,3,4,5],"fragments":[1]}
```

分叉只复制当时仍存活的内容：片段编号、下一样本序号都原样延续（两支的下一条都是 `seq=6`），
已回收的序号 1 **不会恢复**，也不会重放样本重新识别——自动机进度、累计命中与最近 100 次
命中记录按当时快照直接复制。来源会话 5 本身保持不变：

```bash
curl -s http://localhost:8000/sessions/5
# {"session_id":5,"history":2,"capacity":4,"next_seq":6,...,"fragments":[1]}

```bash
curl -s -X POST http://localhost:8000/sessions/404/fork -o /dev/null -w '%{http_code}\n'
# 404   —— 来源会话不存在
```

**在分支 6 续采**：分支 6 与来源同样处于 `C=4` 已占满状态，先只在该分支把容量扩到 5
（参数调整互不影响来源与另一支），再写入 `seq=6` 并触发新片段，片段编号从复制来的
`next_fragment_id=2` 继续：

```bash
curl -s -X PUT http://localhost:8000/sessions/6/limits \
  -H 'Content-Type: application/json' -d '{"history": 2, "capacity": 5}'
# {"session_id":6,"history":2,"capacity":5,"next_seq":6,...,"stored_seqs":[2,3,4,5],"fragments":[1]}

curl -s -X POST http://localhost:8000/sessions/6/samples \
  -H 'Content-Type: application/json' -d '{"value": 60}'
# {"seq":6,"value":60}

curl -s -X POST http://localhost:8000/sessions/6/triggers \
  -H 'Content-Type: application/json' -d '{"before": 1, "after": 0}'
# {"fragment_id":2,"center":6,"before":1,"after":0,"start":5,"end":6,"status":"completed"}
```

**在分支 7 走另一条路**：删除复制来的片段 1（解除引用并回收），再触发编号同样为 2、
但中心不同的片段——两支编号空间各自独立，写入与删除互不影响：

```bash
curl -s -X DELETE http://localhost:8000/sessions/7/fragments/1 -o /dev/null -w '%{http_code}\n'
# 204

curl -s -X POST http://localhost:8000/sessions/7/samples \
  -H 'Content-Type: application/json' -d '{"value": 61}'
# {"seq":6,"value":61}

curl -s -X POST http://localhost:8000/sessions/7/triggers \
  -H 'Content-Type: application/json' -d '{"before": 0, "after": 1}'
# {"fragment_id":2,"center":6,"before":0,"after":1,"start":6,"end":7,"status":"pending"}
```

查询对比：分支 6 的片段 1 仍在、片段 2 中心为 6 且已完成；分支 7 的片段 1 已删除、
片段 2 仍 `pending`（等待各自的 `seq=7`）；来源 5 完全不受两支影响：

```bash
curl -s http://localhost:8000/sessions/6
# {"session_id":6,...,"next_seq":7,"stored_seqs":[2,3,4,5,6],"fragments":[1,2]}

curl -s http://localhost:8000/sessions/7
# {"session_id":7,...,"next_seq":7,"stored_seqs":[5,6],"fragments":[2]}

curl -s http://localhost:8000/sessions/5
# {"session_id":5,...,"next_seq":6,"stored_seqs":[2,3,4,5],"fragments":[1]}
```

分支 7 的待完成片段只等待分支 7 自己的后续样本，与分支 6 的 `seq=7` 无关；
两支后续的容量拒绝、参数调整、模式配置等都各自按原规则独立校验。分叉不提供合并，
服务仍为单进程内存态，重启后所有会话（含分支）都不保留。

### 14. 启用命中留样（每会话一次）

会话 8 以 `H=2, C=6` 创建，配置长度 5 的 pattern `[7,8,9,7,8]`（长于 `H`），然后启用命中留样：

```bash
curl -s -X POST http://localhost:8000/sessions \
  -H 'Content-Type: application/json' -d '{"history": 2, "capacity": 6}'
# {"session_id":8,"history":2,"capacity":6}

curl -s -X PUT http://localhost:8000/sessions/8/pattern \
  -H 'Content-Type: application/json' -d '{"pattern": [7, 8, 9, 7, 8]}'
# {"pattern":[7,8,9,7,8]}

curl -s -X POST http://localhost:8000/sessions/8/hit-fragments/enable
# {"session_id":8,"hit_fragments_enabled":true,"pattern":[7,8,9,7,8]}
```

启用仅限**已配置 pattern 且尚未写入样本**的会话，每会话一次；其余不满足条件或
模式长度超过 `C` 时返回 `409`，未知会话返回 `404`（拒绝均不改变任何状态）：

```bash
curl -s -o /dev/null -w '%{http_code}\n' -X POST http://localhost:8000/sessions/8/hit-fragments/enable
# 409   —— 重复启用

curl -s -X POST http://localhost:8000/sessions \
  -H 'Content-Type: application/json' -d '{"history": 2, "capacity": 3}' > /dev/null   # 会话 9
curl -s -o /dev/null -w '%{http_code}\n' -X POST http://localhost:8000/sessions/9/hit-fragments/enable
# 409   —— 尚未配置 pattern

curl -s -X PUT http://localhost:8000/sessions/9/pattern \
  -H 'Content-Type: application/json' -d '{"pattern": [1, 2, 3, 4]}' > /dev/null
curl -s -o /dev/null -w '%{http_code}\n' -X POST http://localhost:8000/sessions/9/hit-fragments/enable
# 409   —— 模式长度 4 超过 C=3

curl -s -X POST http://localhost:8000/sessions \
  -H 'Content-Type: application/json' -d '{"history": 2, "capacity": 5}' > /dev/null   # 会话 10
curl -s -X PUT http://localhost:8000/sessions/10/pattern \
  -H 'Content-Type: application/json' -d '{"pattern": [1, 1]}' > /dev/null
curl -s -X POST http://localhost:8000/sessions/10/samples \
  -H 'Content-Type: application/json' -d '{"value": 1}' > /dev/null
curl -s -o /dev/null -w '%{http_code}\n' -X POST http://localhost:8000/sessions/10/hit-fragments/enable
# 409   —— 已有样本写入

curl -s -o /dev/null -w '%{http_code}\n' -X POST http://localhost:8000/sessions/99/hit-fragments/enable
# 404   —— 会话不存在
```

### 15. 长模式重叠命中自动留样

会话 8 的 pattern 长度 5 > `H=2`。写入 `7 8 9 7 8` 后 `seq=5` 命中，自动用现有片段编号
生成 `completed` 片段 1（`center=5`、`before=4`、`after=0`，即命中区间 `[1,5]`）：

```bash
for v in 7 8 9 7 8; do
  curl -s -X POST http://localhost:8000/sessions/8/samples \
    -H 'Content-Type: application/json' -d "{\"value\": $v}"; echo
done
# {"seq":1,"value":7}
# {"seq":2,"value":8}
# {"seq":3,"value":9}
# {"seq":4,"value":7}
# {"seq":5,"value":8}   —— 命中 (1,5)，片段 1 自动完成

curl -s http://localhost:8000/sessions/8/fragments
# [{"fragment_id":1,"center":5,"before":4,"after":0,"start":1,"end":5,"status":"completed"}]
```

命中片段引用的样本不受历史窗口回收影响，**模式长于 `H` 也可查询原值**：

```bash
curl -s http://localhost:8000/sessions/8/fragments/1
# {"fragment_id":1,"center":5,"before":4,"after":0,"start":1,"end":5,"status":"completed",
#  "samples":[{"seq":1,"value":7},{"seq":2,"value":8},{"seq":3,"value":9},
#             {"seq":4,"value":7},{"seq":5,"value":8}]}
```

再写入 `9`（`seq=6`）。此时历史窗口 `{5,6}`、片段 1 `{1..5}` 与临时前缀 `{4,5,6}`
（流尾 `7,8,9` 恰为 pattern 前 3 项）的并集 `{1..6}` 恰好占满 `C=6`：

```bash
curl -s -X POST http://localhost:8000/sessions/8/samples \
  -H 'Content-Type: application/json' -d '{"value": 9}'
# {"seq":6,"value":9}

curl -s http://localhost:8000/sessions/8
# {"session_id":8,"history":2,"capacity":6,"next_seq":7,"used":6,
#  "stored_seqs":[1,2,3,4,5,6],"fragments":[1]}
```

### 16. 命中留样的容量拒绝、回滚与重试

继续写 `7`：本条匹配后临时前缀将扩为 `{4..7}`，并集 `{1..7}` 共 7 个序号超出 `C=6`，
返回 `409`——**样本、样本与片段编号、匹配进度、命中记录与片段均不变**（序号未消耗）：

```bash
curl -s -o /dev/null -w '%{http_code}\n' -X POST http://localhost:8000/sessions/8/samples \
  -H 'Content-Type: application/json' -d '{"value": 7}'
# 409

curl -s http://localhost:8000/sessions/8
# {"session_id":8,...,"next_seq":7,"used":6,"stored_seqs":[1,2,3,4,5,6],"fragments":[1]}

curl -s http://localhost:8000/sessions/8/pattern/hits
# {"pattern":[7,8,9,7,8],"hit_count":1,"recent_hits":[{"start_seq":1,"end_seq":5}]}
```

删除片段 1 释放容量。**删除同样保护临时前缀**：序号 `4,5,6` 是流尾匹配 pattern 真前缀的
最长后缀，不随片段删除被回收。随后用相同值重试——被拒绝的写入从未进入自动机，重试只识别一次：

```bash
curl -s -X DELETE http://localhost:8000/sessions/8/fragments/1 -o /dev/null -w '%{http_code}\n'
# 204

curl -s http://localhost:8000/sessions/8
# {"session_id":8,...,"next_seq":7,"used":3,"stored_seqs":[4,5,6],"fragments":[]}

curl -s -X POST http://localhost:8000/sessions/8/samples \
  -H 'Content-Type: application/json' -d '{"value": 7}'
# {"seq":7,"value":7}

curl -s -X POST http://localhost:8000/sessions/8/samples \
  -H 'Content-Type: application/json' -d '{"value": 8}'
# {"seq":8,"value":8}   —— 与首次命中重叠的 (4,8) 再次命中，自动生成片段 2
```

第二次命中 `(4,8)` 与第一次 `(1,5)` **重叠**，片段 2 与手动触发共用同一片段编号计数器，
`before=4` 不受手动触发 `before < H` 限制：

```bash
curl -s http://localhost:8000/sessions/8/pattern/hits
# {"pattern":[7,8,9,7,8],"hit_count":2,
#  "recent_hits":[{"start_seq":1,"end_seq":5},{"start_seq":4,"end_seq":8}]}

curl -s http://localhost:8000/sessions/8/fragments/2
# {"fragment_id":2,"center":8,"before":4,"after":0,"start":4,"end":8,"status":"completed",
#  "samples":[{"seq":4,"value":7},{"seq":5,"value":8},{"seq":6,"value":9},
#             {"seq":7,"value":7},{"seq":8,"value":8}]}
```

启用命中留样后，调参还须保证新 `C` 容得下完整模式及历史、未删除片段与临时前缀的并集，
否则 `409` 且不改变任何状态；分叉会复制启用状态与临时前缀，分支续采的命中继续自动留样，
各支独立：

```bash
curl -s -o /dev/null -w '%{http_code}\n' -X PUT http://localhost:8000/sessions/8/limits \
  -H 'Content-Type: application/json' -d '{"history": 2, "capacity": 4}'
# 409   —— 新 C 容不下长度 5 的完整模式

curl -s -X POST http://localhost:8000/sessions/8/fork
# {"session_id":11,"history":2,"capacity":6,"next_seq":9,"used":5,
#  "stored_seqs":[4,5,6,7,8],"fragments":[2]}
```

### 17. 批量写入：批内命中，批内回收的样本也返回

会话 12 以 `H=2, C=4` 创建并配置 pattern `[1, 1]`，一次批量写入三个 `1`：

```bash
curl -s -X POST http://localhost:8000/sessions \
  -H 'Content-Type: application/json' -d '{"history": 2, "capacity": 4}'
# {"session_id":12,"history":2,"capacity":4}

curl -s -X PUT http://localhost:8000/sessions/12/pattern \
  -H 'Content-Type: application/json' -d '{"pattern": [1, 1]}'
# {"pattern":[1,1]}

curl -s -X POST http://localhost:8000/sessions/12/samples/batch \
  -H 'Content-Type: application/json' -d '{"values": [1, 1, 1]}'
# {"items":[{"seq":1,"value":1},{"seq":2,"value":1},{"seq":3,"value":1}]}
```

批内逐条应用现有匹配规则：两次**重叠命中** `(1,2)`、`(2,3)` 在批内即时记录；
序号 1 在批内已被历史窗口回收，响应仍按输入顺序返回全部三项的 `seq`/`value`：

```bash
curl -s http://localhost:8000/sessions/12/pattern/hits
# {"pattern":[1,1],"hit_count":2,
#  "recent_hits":[{"start_seq":1,"end_seq":2},{"start_seq":2,"end_seq":3}]}

curl -s http://localhost:8000/sessions/12
# {"session_id":12,"history":2,"capacity":4,"next_seq":4,"used":2,"stored_seqs":[2,3],"fragments":[]}
```

`values` 非 1～100 个整数（布尔值、字符串、浮点数均拒绝）返回 `422` 且不改变任何状态；
未知会话返回 `404`：

```bash
curl -s -o /dev/null -w '%{http_code}\n' -X POST http://localhost:8000/sessions/12/samples/batch \
  -H 'Content-Type: application/json' -d '{"values": [1, true, "x", 1.5]}'
# 422

curl -s -o /dev/null -w '%{http_code}\n' -X POST http://localhost:8000/sessions/12/samples/batch \
  -H 'Content-Type: application/json' -d '{"values": []}'
# 422   —— 长度须为 1～100

curl -s -o /dev/null -w '%{http_code}\n' -X POST http://localhost:8000/sessions/99/samples/batch \
  -H 'Content-Type: application/json' -d '{"values": [1]}'
# 404
```

### 18. 批量写入：整批拒绝、回滚与重试

会话 13（`H=2, C=4`）写入 3 条后触发片段 1（引用序号 2–4），再写 1 条使其完成，
此时并集 `{2,3,4}` 占 3 个位置：

```bash
curl -s -X POST http://localhost:8000/sessions \
  -H 'Content-Type: application/json' -d '{"history": 2, "capacity": 4}'
# {"session_id":13,"history":2,"capacity":4}

for v in 10 20 30; do
  curl -s -X POST http://localhost:8000/sessions/13/samples \
    -H 'Content-Type: application/json' -d "{\"value\": $v}" > /dev/null
done
curl -s -X POST http://localhost:8000/sessions/13/triggers \
  -H 'Content-Type: application/json' -d '{"before": 1, "after": 1}' > /dev/null
curl -s -X POST http://localhost:8000/sessions/13/samples \
  -H 'Content-Type: application/json' -d '{"value": 40}' > /dev/null   # seq=4，片段 1 completed
```

批量写入 `[50, 60, 70]`：**每一步都按当时状态检查容量**——第 1 项（`seq=5`）需要
`{2,3,4,5}` 恰好占满 `C=4` 可以通过；第 2 项（`seq=6`）需要 `{2,3,4,5,6}` 共 5 个位置
超容，整批 `409` 并返回首个失败项从 1 起的 `index`：

```bash
curl -s -w '\n%{http_code}\n' -X POST http://localhost:8000/sessions/13/samples/batch \
  -H 'Content-Type: application/json' -d '{"values": [50, 60, 70]}'
# {"detail":"容量不足：新样本无处容纳，序号未消耗，状态未改变","index":2}
# 409
```

第 1 项虽曾暂时写入，也随整批回滚：`next_seq` 仍为 5，样本、片段、两类编号、
匹配进度与命中记录全部保持批前状态：

```bash
curl -s http://localhost:8000/sessions/13
# {"session_id":13,"history":2,"capacity":4,"next_seq":5,"used":3,"stored_seqs":[2,3,4],"fragments":[1]}
```

删除片段 1 释放容量后**原批重试**，序号从 5 连续编起（拒绝未消耗序号）：

```bash
curl -s -X DELETE http://localhost:8000/sessions/13/fragments/1 -o /dev/null -w '%{http_code}\n'
# 204

curl -s -X POST http://localhost:8000/sessions/13/samples/batch \
  -H 'Content-Type: application/json' -d '{"values": [50, 60, 70]}'
# {"items":[{"seq":5,"value":50},{"seq":6,"value":60},{"seq":7,"value":70}]}
```

启用命中留样的会话同样适用：批内命中即时生成留样片段并参与后续步骤的容量核算；
若整批失败，批内生成的留样片段、片段编号与命中记录也一并回滚。

### 19. 命中留样后撤销：恢复样本、片段、两类编号与命中记录

会话 14 以 `H=2, C=6` 创建，配置长度 5 的 pattern `[7,8,9,7,8]` 并启用命中留样，
写入 `7 8 9 7 8` 后 `seq=5` 命中，自动生成留样片段 1：

```bash
curl -s -X POST http://localhost:8000/sessions \
  -H 'Content-Type: application/json' -d '{"history": 2, "capacity": 6}'
# {"session_id":14,"history":2,"capacity":6}

curl -s -X PUT http://localhost:8000/sessions/14/pattern \
  -H 'Content-Type: application/json' -d '{"pattern": [7, 8, 9, 7, 8]}' > /dev/null
curl -s -X POST http://localhost:8000/sessions/14/hit-fragments/enable > /dev/null

for v in 7 8 9 7 8; do
  curl -s -X POST http://localhost:8000/sessions/14/samples \
    -H 'Content-Type: application/json' -d "{\"value\": $v}" > /dev/null
done

curl -s http://localhost:8000/sessions/14
# {"session_id":14,"history":2,"capacity":6,"next_seq":6,"used":5,
#  "stored_seqs":[1,2,3,4,5],"fragments":[1]}

curl -s http://localhost:8000/sessions/14/pattern/hits
# {"pattern":[7,8,9,7,8],"hit_count":1,"recent_hits":[{"start_seq":1,"end_seq":5}]}
```

发现 `seq=5` 是误采样，撤销最近一次成功写入（即写入 `seq=5` 的那条）：留样片段 1 消失、
片段编号回退、命中记录清空、匹配进度与存活样本恢复到写入前（含临时前缀保留的序号 1–4）：

```bash
curl -s -X POST http://localhost:8000/sessions/14/undo
# {"session_id":14,"history":2,"capacity":6,"next_seq":5,"used":4,
#  "stored_seqs":[1,2,3,4],"fragments":[]}

curl -s http://localhost:8000/sessions/14/pattern/hits
# {"pattern":[7,8,9,7,8],"hit_count":0,"recent_hits":[]}

curl -s http://localhost:8000/sessions/14/fragments
# []
```

记录已清空，再次撤销返回 `409`；重新写入后序号从恢复的 5 继续，命中与留样片段
随新写入重新产生（片段编号也从恢复的 1 继续，撤销本身不重放样本）：

```bash
curl -s -o /dev/null -w '%{http_code}\n' -X POST http://localhost:8000/sessions/14/undo
# 409   —— 没有可撤销的写入记录

curl -s -X POST http://localhost:8000/sessions/14/samples \
  -H 'Content-Type: application/json' -d '{"value": 8}'
# {"seq":5,"value":8}   —— 重新命中 (1,5)，再次自动生成片段 1

curl -s http://localhost:8000/sessions/14/fragments
# [{"fragment_id":1,"center":5,"before":4,"after":0,"start":1,"end":5,"status":"completed"}]
```

### 20. 失败批次后撤销上次成功写入

会话 15（`H=2, C=4`）写入 3 条后触发片段 1（引用序号 2–4），再写 1 条使其完成；
随后批量写入 `[50, 60, 70]` 在第 2 项超容，整批 `409` 回滚。**失败请求不影响撤销记录**，
记录仍是最近一次成功写入（`seq=4`）之前的状态：

```bash
curl -s -X POST http://localhost:8000/sessions \
  -H 'Content-Type: application/json' -d '{"history": 2, "capacity": 4}'
# {"session_id":15,"history":2,"capacity":4}

for v in 10 20 30; do
  curl -s -X POST http://localhost:8000/sessions/15/samples \
    -H 'Content-Type: application/json' -d "{\"value\": $v}" > /dev/null
done
curl -s -X POST http://localhost:8000/sessions/15/triggers \
  -H 'Content-Type: application/json' -d '{"before": 1, "after": 1}' > /dev/null
curl -s -X POST http://localhost:8000/sessions/15/samples \
  -H 'Content-Type: application/json' -d '{"value": 40}' > /dev/null   # seq=4，片段 1 completed

curl -s -w '\n%{http_code}\n' -X POST http://localhost:8000/sessions/15/samples/batch \
  -H 'Content-Type: application/json' -d '{"values": [50, 60, 70]}'
# {"detail":"容量不足：新样本无处容纳，序号未消耗，状态未改变","index":2}
# 409   —— 整批回滚，next_seq 仍为 5；撤销记录不受影响
```

撤销回退的是**最近一次成功写入**（`seq=4`），而非失败的批次：序号 4 被收回，
片段 1 从 `completed` 回到 `pending`（重新等待 `seq=4`）：

```bash
curl -s -X POST http://localhost:8000/sessions/15/undo
# {"session_id":15,"history":2,"capacity":4,"next_seq":4,"used":2,
#  "stored_seqs":[2,3],"fragments":[1]}

curl -s http://localhost:8000/sessions/15/fragments/1
# {"fragment_id":1,"center":3,"before":1,"after":1,"start":2,"end":4,"status":"pending",
#  "samples":[{"seq":2,"value":20},{"seq":3,"value":30}]}
```

后续写入从恢复的序号继续；而**成功触发会使记录失效**——触发片段 2 后再撤销返回 `409`：

```bash
curl -s -X POST http://localhost:8000/sessions/15/samples \
  -H 'Content-Type: application/json' -d '{"value": 40}'
# {"seq":4,"value":40}   —— 序号连续，片段 1 重新 completed

curl -s -X POST http://localhost:8000/sessions/15/triggers \
  -H 'Content-Type: application/json' -d '{"before": 1, "after": 0}'
# {"fragment_id":2,"center":4,"before":1,"after":0,"start":3,"end":4,"status":"completed"}

curl -s -o /dev/null -w '%{http_code}\n' -X POST http://localhost:8000/sessions/15/undo
# 409   —— 成功触发已使撤销记录失效
```

### 21. SSE 订阅会话快照（curl -N）

会话 16 以 `H=2, C=3` 创建。在一个终端用 `curl -N` 订阅快照流（`-N` 关闭缓冲，
帧即时显示；未知会话返回 `404`）：

```bash
curl -s -X POST http://localhost:8000/sessions \
  -H 'Content-Type: application/json' -d '{"history": 2, "capacity": 3}'
# {"session_id":16,"history":2,"capacity":3}

# 终端 1：持续观察，无需轮询
curl -N http://localhost:8000/sessions/16/stream
```

订阅后立即收到首帧——订阅时刻的完整快照，`revision` 从 0 开始：

```
event: snapshot
data: {"revision": 0, "state": {"session_id": 16, "history": 2, "capacity": 3, "next_seq": 1, "used": 0, "stored_seqs": [], "fragments": [], "samples": [], "fragment_details": [], "pattern": {"configured": false, "pattern": null, "hit_fragments_enabled": false, "hit_count": 0, "recent_hits": []}}}
```

在**另一个终端**写入两条样本、再撤销一次：

```bash
# 终端 2
curl -s -X POST http://localhost:8000/sessions/16/samples \
  -H 'Content-Type: application/json' -d '{"value": 10}'
# {"seq":1,"value":10}

curl -s -X POST http://localhost:8000/sessions/16/samples \
  -H 'Content-Type: application/json' -d '{"value": 20}'
# {"seq":2,"value":20}

curl -s -X POST http://localhost:8000/sessions/16/undo
# {"session_id":16,"history":2,"capacity":3,"next_seq":2,"used":1,"stored_seqs":[1],"fragments":[]}
```

终端 1 随之逐帧收到每次成功修改后的完整快照，`revision` 严格递增；
撤销本身也是一次修改（`revision` 加 1、**不回退**），帧内状态恢复到写入前：

```
event: snapshot
data: {"revision": 1, "state": {"session_id": 16, ..., "next_seq": 2, "used": 1, "stored_seqs": [1], "samples": [{"seq": 1, "value": 10}], ...}}

event: snapshot
data: {"revision": 2, "state": {"session_id": 16, ..., "next_seq": 3, "used": 2, "stored_seqs": [1, 2], "samples": [{"seq": 1, "value": 10}, {"seq": 2, "value": 20}], ...}}

event: snapshot
data: {"revision": 3, "state": {"session_id": 16, ..., "next_seq": 2, "used": 1, "stored_seqs": [1], "samples": [{"seq": 1, "value": 10}], ...}}
```

每帧 `data` 的 JSON 都含 `revision` 与 `state`：`state` 除原会话查询字段外，还有按序号
排列的存活样本 `samples`、全部片段详情 `fragment_details`（含各片段当前可查的样本内容）、
模式与命中留样配置及命中结果 `pattern`。查询、失败请求与来源分叉不会产生新帧；整批写入
只产生一帧。`Ctrl+C` 断开即释放订阅；慢消费者不会阻塞采样，服务只为其缓存最新一条待发快照。

## 本地开发（可选）

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```
