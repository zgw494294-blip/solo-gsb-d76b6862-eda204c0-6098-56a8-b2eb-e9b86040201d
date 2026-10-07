"""样本片段服务。

单进程内存态的 HTTP 服务：按会话维护连续编号的整数样本，
支持以当前末条为中心触发片段（前 before 条 + 中心 + 后 after 条），
片段等后续样本到齐后完成；最近 H 条历史与所有未删除片段共同占用容量 C。
会话运行中可不停采样地调整 H 与 C：校验通过才生效并按新参数回收，
任一校验失败则拒绝且不改变任何状态。

可选的整数模式识别：会话可在首条样本写入前配置一次长度 1～32 的整数
pattern，此后每条成功写入的样本经流式 KMP 自动机精确匹配（允许重叠）。
自动机只保存标量状态，不保存原始样本、不占用样本容量，故样本回收与
片段增删均不影响识别进度；命中记录在样本回收后仍然保留。

可选的命中留样：已配置 pattern 且尚未写入样本的会话可经 HTTP 启用一次
（模式长度超过 C 时拒绝）。启用后每次命中自动用现有片段编号生成一个
completed 片段（center=命中末条序号、before=模式长度减 1、after=0，
允许重叠，不受手动触发 before<H 限制），即使模式长于 H 也可查询原值；
流尾匹配 pattern 真前缀的最长后缀（临时前缀，即自动机标量状态对应的
流尾区间）也须保留。历史、未删除片段与临时前缀按序号并集占用容量；
写入按本条匹配后的状态核算与回收，超容则 409 且样本、两类编号、匹配
进度、命中记录与片段均不变；删除片段与调整参数同样保护临时前缀，
调参还须保证新 C 容得下完整模式。未启用时上述行为完全不变。

会话可在任意时刻分叉为一条独立续采的新会话：复制当时的 H、C、存活样本、
全部未删除片段及其编号、下一样本与片段编号，以及模式配置、自动机进度、
累计命中、最近 100 次命中记录与命中留样启用状态（临时前缀随自动机状态
一并复制）。分叉只复制存活状态：已回收样本不恢复，也不重放样本重新识别
（直接复制自动机标量状态）。分叉在来源会话锁内完成取号与快照，等效于与
来源已有操作串行执行的某个时刻；来源保持不变，两支此后的写入、触发、
删除、调参与模式操作互不影响。不提供合并与落盘。

成组提交连续样本：会话可经 HTTP 一次提交 1～100 个整数值（布尔值、字符串、
浮点数均拒绝），整批接收或整批拒绝，不留下部分写入。批内按输入顺序逐条
应用现有写入、匹配、留样与回收规则，每一步均按当时状态检查容量，而非只
核算批末；任一步超容则整批 409 并返回首个失败项从 1 起的 index，样本、
片段、样本与片段编号、匹配进度与命中记录均恢复批前状态，本批回收的旧
样本也一并还原。整批在会话锁内一次完成，等效于与会话内其他操作的一次
串行执行：其他操作不能插入批内，查询与分叉只能看到批前或批后状态。

撤销最近一次成功写入：每会话仅保留最近一次成功单条或整批写入前的状态
（整批以首项写入前为准），可经 HTTP 撤销一次以纠正已接收的误采样——恢复
存活样本（含该次写入时回收的旧样本）、全部片段、样本与片段两类编号、
匹配进度与命中记录；不重放样本，撤销后记录清空，后续写入从恢复的序号
继续。成功触发、删除片段、调参、配置模式或启用留样使记录失效；失败请求、
查询与分叉不影响记录，新分支不继承记录。记录仅存内存且不计入样本容量；
撤销在会话锁内一次完成，与同会话其他操作串行，查询不会看到部分恢复。

SSE 会话快照订阅：GET /sessions/{sid}/stream 以 text/event-stream 持续推送
会话状态，无需轮询。每帧为 snapshot 事件，data 是含 revision 与 state 的
JSON；state 含原会话查询字段、按序号排列的存活样本、全部片段详情，以及
模式与命中留样配置和命中结果。首帧取订阅时刻（同一会话锁临界区内）的完整
快照，此后每次成功修改各发一帧完整快照，衔接处不漏更新。revision 从 0 开始，
成功写入（整批只加 1）、撤销、触发、删除、调参、配置模式及启用留样各加 1；
查询、失败请求与来源分叉不增加，新分支从 0 开始；revision 不随撤销回退，
在后续帧中严格递增，帧内不会含批内或回滚前状态。每个订阅最多缓存一条待发
快照（新快照合并掉未发出的旧快照），停止修改后继续读取必能收到最新状态；
发布只入队不等待，慢订阅者不阻塞采样，网络发送不占用会话锁，断连即释放
订阅。未知会话在建流前返回 404；不提供历史重放。
"""
from __future__ import annotations

import asyncio
import json
from collections import deque

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field, model_validator

app = FastAPI(title="sample-fragment-service", version="1.0.0")


# ---------------- 请求模型 ----------------

class CreateSessionRequest(BaseModel):
    history: int = Field(ge=1, le=1000, description="历史长度 H")
    capacity: int = Field(ge=1, le=1000, description="样本容量 C")

    @model_validator(mode="after")
    def _check_h_le_c(self) -> "CreateSessionRequest":
        if self.history > self.capacity:
            raise ValueError("必须满足 1 <= H <= C <= 1000")
        return self


class UpdateLimitsRequest(BaseModel):
    history: int = Field(description="新的历史长度 H")
    capacity: int = Field(description="新的样本容量 C")

    @model_validator(mode="before")
    @classmethod
    def _check_types(cls, data: object) -> object:
        # 显式校验，确保布尔值、字符串、浮点等不会被宽松转换为整数（返回 422）
        if not isinstance(data, dict):
            raise ValueError("请求体必须是包含 history 和 capacity 的对象")
        for key in ("history", "capacity"):
            if key not in data or type(data[key]) is not int:
                raise ValueError("history 和 capacity 必须是整数，不能为布尔值或字符串")
        return data

    @model_validator(mode="after")
    def _check_range(self) -> "UpdateLimitsRequest":
        if not (1 <= self.history <= self.capacity <= 1000):
            raise ValueError("必须满足 1 <= H <= C <= 1000")
        return self


class WriteSampleRequest(BaseModel):
    value: int = Field(description="整数样本值")


class WriteBatchRequest(BaseModel):
    values: list[int] = Field(description="一批整数样本值，1～100 个；元素必须为整数")

    @model_validator(mode="before")
    @classmethod
    def _check_values(cls, data: object) -> object:
        # 显式校验，确保布尔值、字符串、浮点等不会被宽松转换为整数（返回 422）
        if not isinstance(data, dict) or not isinstance(data.get("values"), list):
            raise ValueError("values 必须是整数数组，长度 1～100")
        values = data["values"]
        if not (1 <= len(values) <= 100):
            raise ValueError("values 长度必须在 1～100 之间")
        if any(type(x) is not int for x in values):
            raise ValueError("values 元素必须是整数，不能为布尔值、字符串或浮点数")
        return data


class TriggerRequest(BaseModel):
    before: int = Field(ge=0, description="中心之前的条数（非负整数）")
    after: int = Field(ge=0, description="中心之后的条数（非负整数）")


class ConfigurePatternRequest(BaseModel):
    pattern: list[int] = Field(description="待识别的整数模式，长度 1～32；元素必须为整数")

    @model_validator(mode="before")
    @classmethod
    def _check_pattern(cls, data: object) -> object:
        # 显式校验，确保布尔值、字符串、浮点等不会被宽松转换为整数（返回 422）
        if not isinstance(data, dict) or not isinstance(data.get("pattern"), list):
            raise ValueError("pattern 必须是整数数组，长度 1～32")
        pattern = data["pattern"]
        if not (1 <= len(pattern) <= 32):
            raise ValueError("pattern 长度必须在 1～32 之间")
        if any(type(x) is not int for x in pattern):
            raise ValueError("pattern 元素必须是整数，不能为布尔值或字符串")
        return data


# ---------------- 流式模式匹配（KMP 自动机） ----------------

class PatternMatcher:
    """只保存标量状态的流式 KMP：逐样本喂入，不保存原始样本。

    state 为当前匹配到的 pattern 前缀长度；feed 在恰好完整匹配时返回
    True，否则返回 False。失败回退允许重叠命中。pattern 可以长于 H 和 C：
    进度只依赖自动机状态，与样本是否被回收无关。
    """

    __slots__ = ("pattern", "failure", "state")

    def __init__(self, pattern: list[int]):
        self.pattern = pattern
        m = len(pattern)
        failure = [0] * m
        k = 0
        for i in range(1, m):
            while k > 0 and pattern[i] != pattern[k]:
                k = failure[k - 1]
            if pattern[i] == pattern[k]:
                k += 1
            failure[i] = k
        self.failure = failure
        self.state = 0

    def feed(self, value: int) -> bool:
        m = len(self.pattern)
        k = self.state
        while k > 0 and value != self.pattern[k]:
            k = self.failure[k - 1]
        if value == self.pattern[k]:
            k += 1
        if k == m:
            self.state = self.failure[k - 1]  # 回退以支持重叠命中
            return True
        self.state = k
        return False


# ---------------- 会话状态 ----------------

class Fragment:
    __slots__ = ("fragment_id", "center", "before", "after")

    def __init__(self, fragment_id: int, center: int, before: int, after: int):
        self.fragment_id = fragment_id
        self.center = center
        self.before = before
        self.after = after

    @property
    def start(self) -> int:
        return self.center - self.before

    @property
    def end(self) -> int:
        return self.center + self.after


def _conflict(detail: str) -> HTTPException:
    return HTTPException(status_code=409, detail=detail)


class BatchConflictError(Exception):
    """批量写入中途超容：携带首个失败项从 1 起的序号；抛出前状态已整体回滚。"""

    def __init__(self, index: int, detail: str):
        super().__init__(detail)
        self.index = index
        self.detail = detail


# ---------------- SSE 快照订阅 ----------------

class Subscription:
    """单个 SSE 订阅的待发快照缓存：最多一条，满则合并为最新。

    每帧快照都是完整状态，新快照天然包含此前全部修改的效果，故缓存满时
    丢弃旧快照、只留最新即可——中间更新被合并，但停止修改后继续读取必能
    收到最新状态。offer 只调用 put_nowait，从不等待：慢订阅者不阻塞采样，
    网络发送由订阅方协程在会话锁外完成。
    """

    __slots__ = ("queue",)

    def __init__(self) -> None:
        self.queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=1)

    def offer(self, frame: dict) -> None:
        try:
            self.queue.put_nowait(frame)
        except asyncio.QueueFull:
            # 合并中间更新：丢弃未发出的旧快照，替换为最新完整快照
            self.queue.get_nowait()
            self.queue.put_nowait(frame)


class Session:
    """单个会话的全部内存状态；所有操作在 self.lock 下串行执行。"""

    def __init__(self, session_id: int, history: int, capacity: int):
        self.session_id = session_id
        self.history = history            # H
        self.capacity = capacity          # C
        self.next_seq = 1                 # 下一条样本的序号（从 1 连续编号）
        self.samples: dict[int, int] = {}  # 当前存活的样本：序号 -> 值
        self.fragments: dict[int, Fragment] = {}  # 未删除的片段
        self.next_fragment_id = 1
        # 模式识别：仅首条样本写入前可配置一次；状态不占用样本容量
        self.pattern: list[int] | None = None
        self.matcher: PatternMatcher | None = None
        self.pattern_configured = False
        self.hit_count = 0
        self.recent_hits: deque[dict] = deque(maxlen=100)  # 最近 100 次命中
        # 命中留样：已配置 pattern 且尚未写入样本时可经 HTTP 启用一次；
        # 启用后命中自动生成 completed 片段，临时前缀参与容量核算
        self.preserve_hits = False
        # 撤销记录：最近一次成功写入（整批为首项写入）前的状态快照；
        # 仅存内存、不计入样本容量；成功触发/删除片段/调参/配置模式/
        # 启用留样后失效，分叉不继承
        self.undo_snapshot: dict | None = None
        # SSE 订阅：revision 从 0 开始，每次成功修改加 1（整批只加 1；
        # 查询、失败请求与来源分叉不加，新分支从 0 开始；不随撤销回退）
        self.revision = 0
        self.subscribers: set[Subscription] = set()
        self.lock = asyncio.Lock()

    # ---- 修改发布 ----

    def _commit_mutation(self) -> None:
        """一次成功修改的收尾：revision 加 1 并向全部订阅投递新快照。

        在会话锁内调用：只在锁内构造完整快照并放入各订阅的缓存队列
        （put_nowait，队列满则合并），不做任何网络发送，故慢订阅者或
        断连都不会占用会话锁、不会阻塞采样。无论是否有订阅者，revision
        都随成功修改严格递增。
        """
        self.revision += 1
        if not self.subscribers:
            return
        frame = {"revision": self.revision, "state": _session_state(self)}
        for sub in list(self.subscribers):
            sub.offer(frame)

    # ---- 容量核算 ----

    def needed_seqs(self, upto: int, history: int | None = None) -> set[int]:
        """假设已写入 upto 条时，历史窗口、未删除片段与临时前缀需要的序号集合。

        history 缺省为当前 H；显式传入时按该长度核算（用于调整参数的预检）。
        同序号只计一次（集合语义）；片段只统计已写入的部分，
        未写入的后续样本在将来写入时再参与核算。
        临时前缀是流尾匹配 pattern 真前缀的最长后缀对应的序号区间，
        仅启用命中留样后参与核算；其长度即自动机标量状态，故随匹配进度
        自动伸缩，无需额外保存样本。
        """
        h = self.history if history is None else history
        lo = max(1, upto - h + 1)
        needed = set(range(lo, upto + 1))
        for frag in self.fragments.values():
            hi = min(frag.end, upto)
            if frag.start <= hi:
                needed.update(range(frag.start, hi + 1))
        if self.preserve_hits and self.matcher is not None and self.matcher.state > 0:
            needed.update(range(upto - self.matcher.state + 1, upto + 1))
        return needed

    def reclaim(self) -> None:
        """回收历史窗口、未删除片段与临时前缀都不再需要的样本。"""
        live = self.needed_seqs(self.next_seq - 1)
        for seq in list(self.samples):
            if seq not in live:
                del self.samples[seq]

    # ---- 运行中调整 H / C ----

    def adjust_limits(self, history: int, capacity: int) -> None:
        """不停采样地调整 H 与 C；任一校验失败抛 409 且不改变任何状态。

        全部通过后按新参数回收不再需要的样本。片段及其内容、样本与片段
        编号、模式配置与命中记录均不受影响；扩容不会恢复已回收的样本，
        旧片段的 before 即使不小于新 H 也原样保留（仅约束后续触发）。
        """
        upto = self.next_seq - 1
        # 新历史窗口要求的最近 min(H, 已写入条数) 条必须全部仍存活
        lo = max(1, upto - history + 1)
        if any(seq not in self.samples for seq in range(lo, upto + 1)):
            raise _conflict("新历史窗口包含已回收的样本，无法调整")
        # 未删除片段的总长不能超过新容量
        if any(f.before + 1 + f.after > capacity for f in self.fragments.values()):
            raise _conflict("存在总长超过新容量的未删除片段，无法调整")
        # 启用命中留样时，新容量必须容得下完整模式（命中片段的长度）
        if self.preserve_hits and self.pattern is not None and len(self.pattern) > capacity:
            raise _conflict("新容量容不下完整模式，无法调整")
        # 新历史窗口、片段已写入部分与临时前缀的序号并集不能超过新容量
        if len(self.needed_seqs(upto, history)) > capacity:
            raise _conflict("新历史与未删除片段所需序号的并集超过新容量，无法调整")
        self.history = history
        self.capacity = capacity
        self.reclaim()
        # 调参成功使撤销记录失效（校验失败已在上面抛 409，记录不变）
        self.undo_snapshot = None
        self._commit_mutation()

    # ---- 模式识别 ----

    def configure_pattern(self, pattern: list[int]) -> None:
        """配置识别模式；重复配置或首条样本已写入后配置均抛 409。"""
        if self.pattern_configured:
            raise _conflict("该会话已配置过 pattern，不可重复配置")
        if self.next_seq > 1:
            raise _conflict("首条样本已写入，pattern 仅能在首条样本写入前配置一次")
        self.pattern = list(pattern)
        self.matcher = PatternMatcher(self.pattern)
        self.pattern_configured = True
        self.undo_snapshot = None  # 配置模式使撤销记录失效
        self._commit_mutation()

    def enable_hit_fragments(self) -> None:
        """启用命中留样；每会话一次，条件不满足均抛 409 且不改变任何状态。

        仅限已配置 pattern 且尚未写入样本的会话；模式长度超过 C 时同样拒绝，
        以保证启用后任何命中片段（长度=模式长度）都容得下。
        """
        if not self.pattern_configured:
            raise _conflict("该会话尚未配置 pattern，无法启用命中留样")
        if self.next_seq > 1:
            raise _conflict("首条样本已写入，命中留样仅能在尚无样本时启用")
        if self.preserve_hits:
            raise _conflict("该会话已启用命中留样，不可重复启用")
        assert self.pattern is not None
        if len(self.pattern) > self.capacity:
            raise _conflict("模式长度超过样本容量 C，无法启用命中留样")
        self.preserve_hits = True
        self.undo_snapshot = None  # 启用留样使撤销记录失效
        self._commit_mutation()

    def observe(self, seq: int, value: int) -> None:
        """样本成功提交后喂入自动机；命中则追加记录（与写样本同一串行临界区）。"""
        if self.matcher is not None and self.matcher.feed(value):
            assert self.pattern is not None
            m = len(self.pattern)
            self.hit_count += 1
            self.recent_hits.append({"start_seq": seq - m + 1, "end_seq": seq})

    # ---- 操作 ----

    def write(self, value: int, record_undo: bool = True) -> int:
        """写入一条样本；容量不足时抛 409，不消耗序号、不改变历史或片段。

        容量拒绝时不推进匹配状态、不产生命中记录；只有成功提交才喂入自动机，
        因此同一值被拒绝后重试成功只会被识别一次。
        record_undo 为真（HTTP 单条写入）时，先把撤销记录更新为本次写入前
        的状态；写入失败则恢复原记录——失败请求不影响记录。批内逐条写入
        传 False，整批记录由 write_batch 以首项写入前的状态统一掌管。
        """
        if not record_undo:
            return self._write(value)
        saved_record = self.undo_snapshot
        self.undo_snapshot = self._snapshot_state()
        try:
            seq = self._write(value)
        except HTTPException:
            self.undo_snapshot = saved_record
            raise
        self._commit_mutation()
        return seq

    def _write(self, value: int) -> int:
        """单条写入的核心逻辑：容量核算、提交与识别（不含撤销记录管理）。"""
        n = self.next_seq
        if self.preserve_hits:
            return self._write_preserving(n, value)
        needed = self.needed_seqs(n)
        if len(needed) > self.capacity:
            raise _conflict("容量不足：新样本无处容纳，序号未消耗，状态未改变")
        self._commit(n, needed, value)
        self.observe(n, value)
        return n

    def _write_preserving(self, n: int, value: int) -> int:
        """启用命中留样后的写入：按本条匹配后的状态核算容量并回收。

        命中则先用现有片段编号试建 completed 片段（center=命中末条、
        before=模式长度减 1、after=0，允许重叠，不受手动触发 before<H
        限制），再连同临时前缀一起核算并集；超容抛 409 并整体回滚——
        样本、样本与片段编号、匹配进度、命中记录与片段均不变。
        """
        assert self.matcher is not None and self.pattern is not None
        saved_state = self.matcher.state
        hit = self.matcher.feed(value)
        frag = None
        if hit:
            frag = Fragment(self.next_fragment_id, n, len(self.pattern) - 1, 0)
            self.fragments[frag.fragment_id] = frag
        needed = self.needed_seqs(n)
        if len(needed) > self.capacity:
            self.matcher.state = saved_state
            if frag is not None:
                del self.fragments[frag.fragment_id]
            raise _conflict("容量不足：新样本及其命中留样无处容纳，序号未消耗，状态未改变")
        if frag is not None:
            self.next_fragment_id += 1
            self.hit_count += 1
            self.recent_hits.append({"start_seq": frag.start, "end_seq": n})
        self._commit(n, needed, value)
        return n

    def _commit(self, n: int, needed: set[int], value: int) -> None:
        """按已核算的序号集合提交第 n 条样本并就地回收。"""
        # 不变式：needed ⊆ 现有 samples ∪ {n}，故此处不会 KeyError
        kept = {seq: self.samples[seq] for seq in needed if seq != n}
        kept[n] = value
        self.samples = kept
        self.next_seq = n + 1

    # ---- 批量写入 ----

    def write_batch(self, values: list[int]) -> list[int]:
        """成组写入一批样本：全部成功才提交，任一步超容则整体回滚。

        按输入顺序逐条调用 write，故写入、匹配、留样与回收规则及每步的
        容量检查与逐条写入完全一致（非只核算批末）。任一条抛 409 时恢复
        批前状态——样本（含本批回收的旧样本）、片段、样本与片段编号、
        匹配进度与命中记录——再抛 BatchConflictError（携带首个失败项
        从 1 起的序号）。调用方在会话锁内执行本方法，故整批等效于与会话
        内其他操作的一次串行执行，查询与分叉只能看到批前或批后状态。
        整批成功时撤销记录更新为首项写入前的状态；整批失败属失败请求，
        撤销记录保持批前原样。
        """
        saved_record = self.undo_snapshot
        snapshot = self._snapshot_state()
        seqs: list[int] = []
        try:
            for value in values:
                seqs.append(self.write(value, record_undo=False))
        except HTTPException as exc:
            self._restore_state(snapshot)
            self.undo_snapshot = saved_record
            raise BatchConflictError(len(seqs) + 1, str(exc.detail)) from exc
        self.undo_snapshot = snapshot
        # 整批只加 1：批内不发布，订阅者只会看到批前或批后的完整状态
        self._commit_mutation()
        return seqs

    # ---- 撤销记录 ----

    def _snapshot_state(self) -> dict:
        """捕获写入会改变的全部状态，作为撤销记录（仅存内存，不占样本容量）。"""
        return {
            "next_seq": self.next_seq,
            "samples": dict(self.samples),
            "fragments": dict(self.fragments),
            "next_fragment_id": self.next_fragment_id,
            "matcher_state": self.matcher.state if self.matcher is not None else None,
            "hit_count": self.hit_count,
            "recent_hits": deque(self.recent_hits, maxlen=100),
        }

    def _restore_state(self, snap: dict) -> None:
        """把快照内容写回会话状态（撤销与整批回滚共用）。"""
        self.next_seq = snap["next_seq"]
        self.samples = dict(snap["samples"])
        self.fragments = dict(snap["fragments"])
        self.next_fragment_id = snap["next_fragment_id"]
        if self.matcher is not None:
            self.matcher.state = snap["matcher_state"]
        self.hit_count = snap["hit_count"]
        self.recent_hits = deque(snap["recent_hits"], maxlen=100)

    def undo(self) -> None:
        """撤销最近一次成功的单条或整批写入，恢复其写入前的状态。

        恢复存活样本（含该次写入时回收的旧样本）、全部片段、样本与片段
        编号、匹配进度与命中记录；不重放样本。撤销后记录清空（再次撤销
        抛 409），后续写入从恢复的序号继续并生成新的撤销记录。无可撤销
        记录时抛 409 且不改变任何状态。
        """
        snap = self.undo_snapshot
        if snap is None:
            raise _conflict("没有可撤销的写入记录")
        self._restore_state(snap)
        self.undo_snapshot = None
        # 撤销本身也是一次成功修改：revision 加 1，不随状态回退
        self._commit_mutation()

    def trigger(self, before: int, after: int) -> Fragment:
        """以当前末条为中心创建片段；历史不足时抛 409。"""
        if before >= self.history:
            raise HTTPException(
                status_code=400,
                detail=f"before 必须小于历史长度 H={self.history}",
            )
        if before + 1 + after > self.capacity:
            raise HTTPException(
                status_code=400,
                detail=f"before+1+after 不能超过样本容量 C={self.capacity}",
            )
        center = self.next_seq - 1
        if center - before < 1:
            raise _conflict("历史不足：已有样本不足以覆盖中心及其前 before 条")
        frag = Fragment(self.next_fragment_id, center, before, after)
        self.fragments[frag.fragment_id] = frag
        self.next_fragment_id += 1
        self.undo_snapshot = None  # 成功触发使撤销记录失效
        self._commit_mutation()
        return frag

    def status_of(self, frag: Fragment) -> str:
        return "completed" if frag.end < self.next_seq else "pending"

    def delete_fragment(self, fragment_id: int) -> None:
        """删除片段：解除引用并回收不再被任何一方需要的样本；不存在则抛 404。"""
        if self.fragments.pop(fragment_id, None) is None:
            raise HTTPException(status_code=404, detail="片段不存在")
        # 解除引用后立刻回收不再被任何一方需要的样本
        self.reclaim()
        self.undo_snapshot = None  # 删除片段使撤销记录失效
        self._commit_mutation()

    # ---- 分叉 ----

    def fork(self, new_id: int) -> "Session":
        """以当前快照派生出一条独立续采的新会话。

        复制 H、C、存活样本、全部未删除片段及其编号、下一样本与片段编号，
        以及模式配置、自动机标量进度、累计命中、最近 100 次命中记录与
        命中留样启用状态（临时前缀即自动机 state 对应的流尾，随之复制）。
        只复制存活状态：已回收样本不恢复，也不重放样本重新识别——自动机的
        state/failure 与 hit_count、recent_hits 原样拷贝，两支后续识别各自独立。
        撤销记录不随分叉复制，新分支从空记录开始；来源的撤销记录也不受影响。
        调用方保证在来源锁内取号并调用本方法，故快照对应与来源操作串行的
        某个确定时刻；来源自身不做任何修改。
        """
        child = Session(new_id, self.history, self.capacity)
        child.next_seq = self.next_seq
        child.samples = dict(self.samples)
        child.fragments = {
            fid: Fragment(f.fragment_id, f.center, f.before, f.after)
            for fid, f in self.fragments.items()
        }
        child.next_fragment_id = self.next_fragment_id
        child.pattern_configured = self.pattern_configured
        if self.pattern is not None:
            child.pattern = list(self.pattern)
            child.matcher = PatternMatcher(child.pattern)
            child.matcher.failure = list(self.matcher.failure)
            child.matcher.state = self.matcher.state
        child.hit_count = self.hit_count
        child.recent_hits = deque(self.recent_hits, maxlen=100)
        # 命中留样启用状态一并复制；临时前缀随自动机 state 已拷贝，各支此后独立
        child.preserve_hits = self.preserve_hits
        return child


# ---------------- 会话注册表（单进程内存） ----------------

class Registry:
    def __init__(self) -> None:
        self.sessions: dict[int, Session] = {}
        self.next_id = 1


registry = Registry()


def _get_session(session_id: int) -> Session:
    sess = registry.sessions.get(session_id)
    if sess is None:
        raise HTTPException(status_code=404, detail="会话不存在")
    return sess


def _session_summary(sess: Session) -> dict:
    return {
        "session_id": sess.session_id,
        "history": sess.history,
        "capacity": sess.capacity,
        "next_seq": sess.next_seq,
        "used": len(sess.samples),
        "stored_seqs": sorted(sess.samples),
        "fragments": sorted(sess.fragments),
    }


def _fragment_summary(sess: Session, frag: Fragment) -> dict:
    return {
        "fragment_id": frag.fragment_id,
        "center": frag.center,
        "before": frag.before,
        "after": frag.after,
        "start": frag.start,
        "end": frag.end,
        "status": sess.status_of(frag),
    }


def _fragment_detail(sess: Session, frag: Fragment) -> dict:
    # 片段引用且已写入的样本一定仍被保留（未被回收），按序号升序返回
    hi = min(frag.end, sess.next_seq - 1)
    return {
        **_fragment_summary(sess, frag),
        "samples": [
            {"seq": seq, "value": sess.samples[seq]}
            for seq in range(frag.start, hi + 1)
        ],
    }


def _session_state(sess: Session) -> dict:
    """SSE 快照帧的 state：在会话锁内构造的完整状态。

    含原会话查询字段（与 GET /sessions/{sid} 一致）、按序号排列的存活样本、
    全部未删除片段的详情，以及模式与命中留样配置和命中结果。
    """
    state = _session_summary(sess)
    # 按序号升序排列的存活样本
    state["samples"] = [
        {"seq": seq, "value": sess.samples[seq]} for seq in sorted(sess.samples)
    ]
    # 全部未删除片段的详情（含各片段当前可查的样本内容），按编号升序
    state["fragment_details"] = [
        _fragment_detail(sess, sess.fragments[fid]) for fid in sorted(sess.fragments)
    ]
    # 模式识别与命中留样配置及命中结果
    state["pattern"] = {
        "configured": sess.pattern_configured,
        "pattern": list(sess.pattern) if sess.pattern is not None else None,
        "hit_fragments_enabled": sess.preserve_hits,
        "hit_count": sess.hit_count,
        "recent_hits": list(sess.recent_hits),
    }
    return state


# ---------------- HTTP 接口 ----------------

@app.get("/")
async def root() -> dict:
    return {"service": "sample-fragment-service", "docs": "/docs"}


@app.post("/sessions", status_code=201)
async def create_session(req: CreateSessionRequest) -> dict:
    # 单事件循环内无 await，创建过程天然原子
    sid = registry.next_id
    registry.next_id += 1
    registry.sessions[sid] = Session(sid, req.history, req.capacity)
    return {"session_id": sid, "history": req.history, "capacity": req.capacity}


@app.get("/sessions/{session_id}")
async def get_session(session_id: int) -> dict:
    sess = _get_session(session_id)
    async with sess.lock:
        return _session_summary(sess)


@app.post("/sessions/{session_id}/fork", status_code=201)
async def fork_session(session_id: int) -> dict:
    """把来源会话分叉为可独立续采的新会话。

    取号与快照复制都在来源会话锁内完成：分叉等效于排在来源某个已完成操作
    之后执行，不会混合两个时刻的状态，来源保持不变。新 ID 与普通创建共用
    同一个递增计数器，故每次分叉都得到不同且不冲突的 ID。来源不存在返回 404。
    返回值是新会话的原会话查询结果（其中 session_id 为新 ID）。
    """
    sess = _get_session(session_id)
    async with sess.lock:
        new_id = registry.next_id
        registry.next_id += 1
        child = sess.fork(new_id)
        registry.sessions[new_id] = child
        return _session_summary(child)


@app.put("/sessions/{session_id}/limits")
async def update_limits(session_id: int, req: UpdateLimitsRequest) -> dict:
    sess = _get_session(session_id)
    async with sess.lock:
        sess.adjust_limits(req.history, req.capacity)
        return _session_summary(sess)


@app.post("/sessions/{session_id}/samples", status_code=201)
async def write_sample(session_id: int, req: WriteSampleRequest) -> dict:
    sess = _get_session(session_id)
    async with sess.lock:
        seq = sess.write(req.value)
        return {"seq": seq, "value": req.value}


@app.post("/sessions/{session_id}/samples/batch", status_code=201)
async def write_samples_batch(session_id: int, req: WriteBatchRequest) -> dict:
    """成组写入一批连续样本：整批接收或整批拒绝，不留下部分写入。

    按输入顺序逐条应用与单条写入完全相同的容量、匹配、留样与回收规则，
    每一步都按当时状态检查容量。全部成功返回 201 与每项的 seq/value
    （按输入顺序，批内已被回收的样本也返回）；任一步超容则整批 409，
    响应体带首个失败项从 1 起的 index，样本、片段、两类编号、匹配进度
    与命中记录均恢复批前状态。整个批次在会话锁内一次完成，等效于与会话
    内其他操作的一次串行执行，查询与分叉只能看到批前或批后状态。
    会话不存在返回 404；values 非 1～100 个整数（布尔值、字符串、浮点数
    均拒绝）返回 422 且不改变任何状态。
    """
    sess = _get_session(session_id)
    async with sess.lock:
        try:
            seqs = sess.write_batch(req.values)
        except BatchConflictError as exc:
            return JSONResponse(
                status_code=409,
                content={"detail": exc.detail, "index": exc.index},
            )
        return {
            "items": [
                {"seq": seq, "value": value}
                for seq, value in zip(seqs, req.values)
            ]
        }


@app.post("/sessions/{session_id}/undo")
async def undo_last_write(session_id: int) -> dict:
    """撤销最近一次成功的单条或整批采样写入，纠正已接收的误采样。

    恢复该次写入前的存活样本（含写入时回收的旧样本）、全部片段、样本与
    片段两类编号、匹配进度与命中记录；不重放样本，撤销后记录清空，后续
    写入从恢复的序号继续。成功触发、删除片段、调参、配置模式或启用留样
    都会使记录失效；失败请求、查询与分叉不影响记录，新分支不继承记录。
    撤销在会话锁内一次完成，查询不会看到部分恢复。会话不存在返回 404；
    无可撤销记录返回 409；成功返回 200 与原会话查询格式。
    """
    sess = _get_session(session_id)
    async with sess.lock:
        sess.undo()
        return _session_summary(sess)


@app.put("/sessions/{session_id}/pattern")
async def configure_pattern(session_id: int, req: ConfigurePatternRequest) -> dict:
    sess = _get_session(session_id)
    async with sess.lock:
        sess.configure_pattern(req.pattern)
        return {"pattern": sess.pattern}


@app.get("/sessions/{session_id}/pattern/hits")
async def get_pattern_hits(session_id: int) -> dict:
    sess = _get_session(session_id)
    async with sess.lock:
        if not sess.pattern_configured:
            raise _conflict("该会话尚未配置 pattern")
        return {
            "pattern": sess.pattern,
            "hit_count": sess.hit_count,
            # 按命中产生顺序保存，即按 start_seq 升序；只保留最近 100 次
            "recent_hits": list(sess.recent_hits),
        }


@app.post("/sessions/{session_id}/hit-fragments/enable")
async def enable_hit_fragments(session_id: int) -> dict:
    """启用命中留样：此后每次模式命中自动生成留样片段。

    仅限已配置 pattern 且尚未写入样本的会话，每会话一次；会话不存在返回
    404，未配置 pattern、已有样本、重复启用或模式长度超过 C 均返回 409
    且不改变任何状态。未启用的会话行为与原来完全一致。
    """
    sess = _get_session(session_id)
    async with sess.lock:
        sess.enable_hit_fragments()
        return {
            "session_id": sess.session_id,
            "hit_fragments_enabled": True,
            "pattern": sess.pattern,
        }


@app.post("/sessions/{session_id}/triggers", status_code=201)
async def trigger(session_id: int, req: TriggerRequest) -> dict:
    sess = _get_session(session_id)
    async with sess.lock:
        frag = sess.trigger(req.before, req.after)
        return _fragment_summary(sess, frag)


@app.get("/sessions/{session_id}/fragments")
async def list_fragments(session_id: int) -> list:
    sess = _get_session(session_id)
    async with sess.lock:
        return [
            _fragment_summary(sess, sess.fragments[fid])
            for fid in sorted(sess.fragments)
        ]


@app.get("/sessions/{session_id}/fragments/{fragment_id}")
async def get_fragment(session_id: int, fragment_id: int) -> dict:
    sess = _get_session(session_id)
    async with sess.lock:
        frag = sess.fragments.get(fragment_id)
        if frag is None:
            raise HTTPException(status_code=404, detail="片段不存在")
        return _fragment_detail(sess, frag)


@app.delete("/sessions/{session_id}/fragments/{fragment_id}", status_code=204)
async def delete_fragment(session_id: int, fragment_id: int) -> None:
    sess = _get_session(session_id)
    async with sess.lock:
        sess.delete_fragment(fragment_id)


# ---------------- SSE 会话快照订阅 ----------------

@app.get("/sessions/{session_id}/stream")
async def stream_session(session_id: int):
    """SSE 订阅会话快照：持续推送完整状态，无需轮询。

    每帧为 `event: snapshot`，data 是含 `revision` 与 `state` 的 JSON；state 含
    原会话查询字段、按序号排列的存活样本、全部片段详情，以及模式与命中留样
    配置和命中结果。首帧为订阅时刻（与注册同一临界区）的完整快照，此后每次
    成功修改各发一帧完整快照，衔接处不漏更新。revision 从 0 开始，成功写入
    （整批只加 1）、撤销、触发、删除、调参、配置模式及启用留样各加 1；查询、
    失败请求与来源分叉不加，新分支从 0 开始；revision 不随撤销回退，帧间严格
    递增，帧内不会含批内或回滚前状态。每个订阅最多缓存一条待发快照（合并
    中间更新），慢订阅者不阻塞采样，发送不占用会话锁，断连自动释放订阅。
    会话不存在返回 404；不提供历史重放。
    """
    sess = _get_session(session_id)  # 未知会话在建流前返回 404
    sub = Subscription()
    async with sess.lock:
        # 注册与首帧快照在同一会话锁临界区内完成：首帧对应一个确定时刻，
        # 其后的每次成功修改都会向本订阅投递新快照，衔接处不会漏掉更新
        sess.subscribers.add(sub)
        sub.offer({"revision": sess.revision, "state": _session_state(sess)})

    async def events():
        try:
            while True:
                try:
                    frame = await asyncio.wait_for(sub.queue.get(), timeout=15)
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"  # SSE 注释行：保活并探测断连
                    continue
                # 网络发送在会话锁外进行，慢订阅者不会阻塞采样
                yield (
                    "event: snapshot\n"
                    f"data: {json.dumps(frame, ensure_ascii=False)}\n\n"
                )
        finally:
            # 断连（响应任务被取消）或流结束时释放订阅
            sess.subscribers.discard(sub)

    return StreamingResponse(
        events(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # 禁用反向代理缓冲，帧即时到达
        },
    )
