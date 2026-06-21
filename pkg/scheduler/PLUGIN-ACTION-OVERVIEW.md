# Volcano 调度器：Plugin 注册 × Action 调用 全览

## 目录

1. [机制简述](#1-机制简述)
2. [插件注册总表](#2-插件注册总表)
3. [Action 调用总表](#3-action-调用总表)
4. [连接矩阵](#4-连接矩阵)
5. [配置开关速查](#5-配置开关速查)

---

## 1. 机制简述

### 交互模型

```
┌──────────┐   OnSessionOpen() 注册    ┌──────────────────┐
│  Plugin  │ ──────────────────────▶  │  Session 函数表    │
│          │                          │  jobOrderFns       │
│  binpack │  ssn.AddNodeOrderFn()    │  nodeOrderFns      │
│  gang    │  ssn.AddJobReadyFn()     │  jobReadyFns       │
│  ...     │  ssn.AddPreemptableFn()  │  preemptableFns    │
└──────────┘                          │  ...               │
                                      └────────┬───────────┘
                                               │ 调用
                                               ▼
                                      ┌──────────────────┐
                                      │     Action        │
                                      │                   │
                                      │  allocate  ──────▶│ ssn.JobReady()
                                      │  preempt   ──────▶│ ssn.Preemptable()
                                      │  enqueue   ──────▶│ ssn.JobEnqueueable()
                                      │  ...              │
                                      └──────────────────┘
```

**三步走**：
1. **Plugin.OnSessionOpen()** → 调用 `ssn.AddXxxFn()` 把回调函数注册到 Session
2. **Action.Execute()** → 调用 `ssn.XxxFn()` 触发所有已注册的插件回调
3. **开关控制** → YAML 中的 `enableXxx: true/false` 决定某个插件的某个回调是否生效

### 排序 vs 过滤 vs 打分

插件注册的函数按语义分为三类：

| 类型 | 接口签名特征 | 作用 | 例子 |
|------|------------|------|------|
| **排序** | `CompareFn`: `func(l,r) int` | 决定队列中谁优先 | `JobOrderFn`, `QueueOrderFn` |
| **过滤/判定** | `ValidateFn`: `func(obj) bool` | 决定是否通过 | `JobReadyFn`, `JobValidFn`, `PredicateFn` |
| **打分** | `NodeOrderFn`: `func(task,node) float64` | 节点优先级 | `NodeOrderFn`, `BatchNodeOrderFn` |
| **投票** | `VoteFn`: `func(obj) int` (1/0/-1) | 多插件投票 | `JobPipelinedFn`, `JobEnqueueableFn` |
| **选择** | `EvictableFn`: `func(preemptor, victims) victims` | 选择 victim | `PreemptableFn`, `ReclaimableFn` |

---

## 2. 插件注册总表

每个插件在 `OnSessionOpen` 中注册的函数一览。

| 插件 | 注册的函数 | 类型 |
|------|-----------|------|
| **binpack** | `NodeOrderFn` | 打分 |
| **capacity** | `ReclaimableFn`, `UnifiedEvictableFn`, `PreemptiveFn`, `AllocatableFn`, `JobEnqueueableFn`, `PrePredicateFn`, `SimulateAddTaskFn`, `SimulateRemoveTaskFn`, `SimulateAllocatableFn`, `EventHandler`, `QueueOrderFn`, `VictimQueueOrderFn` | 判定+排序+模拟 |
| **cdp** | `PreemptableFn`, `ReclaimableFn` | 选择 victim |
| **conformance** | `PreemptableFn`, `ReclaimableFn`, `UnifiedEvictableFn` | 选择 victim |
| **deviceshare** | `PredicateFn`, `NodeOrderFn`, `BatchNodeOrderFn` | 过滤+打分 |
| **drf** | `PreemptableFn`, `QueueOrderFn`, `ReclaimableFn`, `JobOrderFn`, `EventHandler` | 排序+选择 |
| **extender** | `PredicateFn`, `BatchNodeOrderFn`, `PreemptableFn`, `ReclaimableFn`, `JobEnqueueableFn`, `OverusedFn`, `JobReadyFn`, `EventHandler` | 过滤+打分+选择+判定 |
| **gang** | `JobValidFn`, `ReclaimableFn`, `PreemptableFn`, `UnifiedEvictableFn`, `JobOrderFn`, `SubJobOrderFn`, `JobReadyFn`, `SubJobReadyFn`, `JobPipelinedFn`, `SubJobPipelinedFn`, `JobStarvingFns` | **全类型**（排序+判定+选择） |
| **model-locality** | `PredicateFn`, `NodeOrderFn`, `EventHandler` | 过滤+打分 |
| **network-topology-aware** | `HyperNodeOrderFn`, `BatchNodeOrderFn`, `HyperNodeGradientForJobFn`, `HyperNodeGradientForSubJobFn`, `EventHandler` | 打分+分组 |
| **nodegroup** | `NodeOrderFn`, `PredicateFn` | 过滤+打分 |
| **nodeorder** | `NodeOrderFn`, `BatchNodeOrderFn` | 打分 |
| **numaaware** | `EventHandler`, `PredicateFn`, `BatchNodeOrderFn` | 过滤+打分 |
| **overcommit** | `JobEnqueueableFn`, `JobEnqueuedFn` | 投票+通知 |
| **pdb** | `VictimTasksFns`, `ReclaimableFn`, `PreemptableFn` | 选择 victim |
| **predicates** | `EventHandler`, `PrePredicateFn`, `PredicateFn`, `BatchNodeOrderFn`, `SimulateAddTaskFn`, `SimulateRemoveTaskFn`, `SimulatePredicateFn` | 过滤+打分+模拟 |
| **priority** | `TaskOrderFn`, `JobOrderFn`, `SubJobOrderFn`, `PreemptableFn`, `UnifiedEvictableFn`, `JobStarvingFns` | 排序+选择 |
| **proportion** | `QueueOrderFn`, `ReclaimableFn`, `OverusedFn`, `AllocatableFn`, `SimulateAllocatableFn`, `PreemptiveFn`, `PrePredicateFn`, `JobEnqueueableFn`, `SimulateAddTaskFn`, `SimulateRemoveTaskFn`, `EventHandler` | 排序+判定+模拟 |
| **rescheduling** | `VictimTasksFns` | 选择 victim |
| **resource-strategy-fit** | `PredicateFn`, `NodeOrderFn` | 过滤+打分 |
| **resourcequota** | `JobEnqueueableFn` | 投票 |
| **sla** | `JobOrderFn`, `JobEnqueueableFn`, `JobPipelinedFn` | 排序+判定 |
| **task-topology** | `TaskOrderFn`, `NodeOrderFn`, `EventHandler` | 排序+打分 |
| **tdm** | `PredicateFn`, `NodeOrderFn`, `PreemptableFn`, `VictimTasksFns`, `JobOrderFn`, `JobPipelinedFn`, `JobStarvingFns` | 过滤+打分+选择+判定 |
| **usage** | `PredicateFn`, `NodeOrderFn` | 过滤+打分 |

---

## 3. Action 调用总表

每个 Action 在执行过程中会调用哪些 Session 函数。

### 3.1 enqueue — 入队

**职责**：过滤不可调度的 Job，将符合条件的 Job 放入调度队列。

| 调用的函数 | 用途 |
|-----------|------|
| `JobEnqueueable` | 多插件投票：Job 是否可以入队 |
| `JobEnqueued` | 通知各插件：该 Job 已入队（插件做后续处理） |

### 3.2 allocate — 分配

**职责**：为核心 action：为 Job 的 Pending Task 分配节点资源。

| 调用的函数 | 用途 |
|-----------|------|
| `JobValid` | Job 合法性校验（gang: Pod 数量够不够） |
| `Overused` | Queue 是否超用（proportion: 资源配额检查） |
| `HyperNodeGradientForJobFn` | 为 Job 分组 HyperNode（network-topology-aware） |
| `HyperNodeGradientForSubJobFn` | 为 SubJob 分组 HyperNode |
| `PrePredicateFn` | 预选前处理（predicates） |
| `Allocatable` | Queue 是否还能接收新的分配 |
| `SubJobOrderFn` | SubJob 优先级排序 |
| `JobReady` | **Gang 核心**：是否达到 MinAvailable 可以 Commit |
| `JobPipelined` | **Gang 核心**：是否达到 Pipelined 状态 |
| `SubJobReady` | SubJob 级别 Ready 判定 |
| `SubJobPipelined` | SubJob 级别 Pipelined 判定 |
| `BestNodeFn` | 从打分结果中选最佳节点 |
| `SchGateManager` | 移除 QueueAllocationGate |
| `MarkJobDirty` | 标记 Job 状态已变更 |

### 3.3 backfill — 回填

**职责**：为非 Gang 的零散 Task 补充调度。

| 调用的函数 | 用途 |
|-----------|------|
| `JobValid` | Job 合法性校验 |
| `PrePredicateFn` | 预选过滤 |
| `BestNodeFn` | 选最佳节点 |
| `Allocate` | 直接分配（不走 stmt.Commit） |

### 3.4 preempt — 抢占

**职责**：为 Starving Job 抢占低优先级 Task 的资源。

| 调用的函数 | 用途 |
|-----------|------|
| `JobValid` | 抢占者 Job 是否合法 |
| `JobStarving` | 抢占者是否真的需要抢占 |
| `JobPipelined` | 备份：是否可 Pipelined |
| `PrePredicateFn` | 预选过滤 |
| `PredicateFn` | 对候选节点执行完整过滤 |
| `Preemptable` | **Gang 保护核心**：选择可安全抢占的 victim |
| `Allocatable` | 被抢占后 Queue 是否仍满足配额 |
| `SimulateAddTaskFn` | 模拟：把 victim 移除后 Task 能否放入 |
| `SimulateRemoveTaskFn` | 模拟：移除 victim |
| `SimulateAllocatableFn` | 模拟：移除后 Queue 是否合规 |
| `SimulatePredicateFn` | 模拟：在新状态下 Predicate 是否通过 |
| `FilterOutUnschedulableAndUnresolvableNodesForTask` | 过滤不可调度节点 |
| `BuildVictimsPriorityQueue` | 构建 victim 优先级队列 |
| `GetCycleState` | 获取 K8s CycleState（用于模拟） |

### 3.5 reclaim — 回收

**职责**：回收超用 Queue 的资源，重新分配给等待中的 Job。

| 调用的函数 | 用途 |
|-----------|------|
| `JobValid` | Job 合法性 |
| `JobStarving` | 是否需要回收 |
| `JobPipelined` | 是否可 Pipelined |
| `Overused` | Queue 是否超用 |
| `Preemptive` | 是否允许抢占式回收 |
| `PrePredicateFn` | 预选 |
| `Reclaimable` | **Gang 保护核心**：选择可安全回收的 victim |
| `FilterOutUnschedulableAndUnresolvableNodesForTask` | 过滤不可调度节点 |
| `BuildVictimsPriorityQueue` | victim 优先级队列 |

### 3.6 gangpreempt — Gang 级抢占

**职责**：按 Gang 语义整体抢占（bundle 模型）。

| 调用的函数 | 用途 |
|-----------|------|
| `JobValid` | Job 合法性 |
| `JobOrderFn` | Job 排序 |
| `JobStarving` | 是否 Starving |
| `JobPipelined` | 是否可 Pipelined |
| `UnifiedEvictable` | 统一驱逐接口（bundle 模型） |

### 3.7 gangreclaim — Gang 级回收

**职责**：按 Gang 语义整体回收。

| 调用的函数 | 用途 |
|-----------|------|
| `JobValid` | Job 合法性 |
| `JobStarving` | 是否 Starving |
| `JobPipelined` | 是否可 Pipelined |
| `Overused` | Queue 是否超用 |
| `Preemptive` | 是否允许抢占式回收 |
| `QueueOrderFn` | Queue 排序 |
| `VictimQueueOrderFn` | Victim Queue 排序 |
| `UnifiedEvictable` | 统一驱逐接口 |

### 3.8 shuffle — 碎片整理

**职责**：驱逐并重新调度 Task 以减少碎片。

| 调用的函数 | 用途 |
|-----------|------|
| `VictimTasks` | 选择要驱逐的 Task |
| `Evict` | 执行驱逐 |

---

## 4. 连接矩阵

Action（列）× 扩展点（行），标注哪些 Action 调用了哪些扩展点。

| 扩展点 | enqueue | allocate | backfill | preempt | reclaim | gang-preempt | gang-reclaim | shuffle |
|--------|:-------:|:--------:|:--------:|:-------:|:-------:|:------------:|:------------:|:-------:|
| `JobEnqueueable` | ✓ | | | | | | | |
| `JobEnqueued` | ✓ | | | | | | | |
| `JobValid` | | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | |
| `JobReady` | | ✓ | | | | | | |
| `JobPipelined` | | ✓ | | ✓ | ✓ | ✓ | ✓ | |
| `SubJobReady` | | ✓ | | | | | | |
| `SubJobPipelined` | | ✓ | | | | | | |
| `SubJobOrderFn` | | ✓ | | | | | | |
| `JobStarving` | | | | ✓ | ✓ | ✓ | ✓ | |
| `JobOrderFn` | | | | | | ✓ | | |
| `QueueOrderFn` | | | | | | | ✓ | |
| `VictimQueueOrderFn` | | | | | | | ✓ | |
| `TaskOrderFn` | | | | | | | | |
| `Overused` | | ✓ | | | ✓ | | ✓ | |
| `Preemptive` | | | | | ✓ | | ✓ | |
| `Allocatable` | | ✓ | | ✓ | | | | |
| `PrePredicateFn` | | ✓ | ✓ | ✓ | ✓ | | | |
| `PredicateFn` | | | | ✓ | | | | |
| `NodeOrderFn` | | * | | | | | | |
| `BatchNodeOrderFn` | | * | | | | | | |
| `BestNodeFn` | | ✓ | ✓ | | | | | |
| `Preemptable` | | | | ✓ | | | | |
| `Reclaimable` | | | | | ✓ | | | |
| `UnifiedEvictable` | | | | | | ✓ | ✓ | |
| `VictimTasks` | | | | | | | | ✓ |
| `BuildVictimsPriorityQueue` | | | | ✓ | ✓ | | | |
| `SimulateAddTaskFn` | | | | ✓ | | | | |
| `SimulateRemoveTaskFn` | | | | ✓ | | | | |
| `SimulateAllocatableFn` | | | | ✓ | | | | |
| `SimulatePredicateFn` | | | | ✓ | | | | |
| `HyperNodeGradientForXxx` | | ✓ | | | | | | |
| `HyperNodeOrderFn` | | * | | | | | | |
| `Evict` | | | | | | | | ✓ |

> \* `NodeOrderFn` / `BatchNodeOrderFn` / `HyperNodeOrderFn` 是 allocate 内部通过 `util.PrioritizeNodes` 间接调用的。

### 核心 Action 流程示意

```
enqueue                          allocate                          preempt
─────────                        ────────                          ───────
JobEnqueueable (投票)    ──▶     JobValid (合法性)         ──▶     JobValid
JobEnqueued (通知)               Overused (配额)                   JobStarving (真需要?)
                                 PrePredicateFn (预过滤)           PrePredicateFn
                                 Allocatable (可分配?)             PredicateFn (过滤节点)
                                 NodeOrderFn (节点打分)            Preemptable (选victim)
                                 JobReady? → Commit                SimulateAdd/Remove (模拟)
                                 JobPipelined? → 等待              确定可行 → Evict → Allocate
```

---

## 5. 配置开关速查

在 YAML 配置中通过 `enableXxx: true` 启用某个插件在某个扩展点上的回调。`nil` 等同于 `false`。

| YAML 字段 | 对应扩展点 | 主要使用者 |
|-----------|-----------|-----------|
| `enableJobOrder` | `JobOrderFn` | gang, priority, drf, sla, tdm |
| `enableJobReady` | `JobReadyFn` | **gang**, extender |
| `enableJobPipelined` | `JobPipelinedFn` | **gang**, sla, tdm |
| `enableTaskOrder` | `TaskOrderFn` | priority, task-topology |
| `enablePreemptable` | `PreemptableFn` | **gang**, priority, conformance, cdp, pdb, tdm |
| `enableReclaimable` | `ReclaimableFn` | **gang**, capacity, conformance, drf, pdb, proportion |
| `enablePreemptive` | `PreemptiveFn` | capacity, proportion |
| `enableQueueOrder` | `QueueOrderFn` | capacity, drf, proportion |
| `enablePredicate` | `PredicateFn` | **predicates**, deviceshare, numaaware, usage 等 |
| `enableBestNode` | `BestNodeFn` | (预留) |
| `enableNodeOrder` | `NodeOrderFn` | **binpack**, **nodeorder**, deviceshare, model-locality 等 |
| `enableTargetJob` | `TargetJobFn` | (预留) |
| `enableJobEnqueued` | `JobEnqueueableFn` / `JobEnqueuedFn` | overcommit, proportion, resourcequota, sla |
| `enabledVictim` | `VictimTasksFns` | pdb, rescheduling, tdm |
| `enableJobStarving` | `JobStarvingFns` | **gang**, priority, tdm |
| `enabledOverused` | `OverusedFn` | proportion, extender |
| `enabledAllocatable` | `AllocatableFn` | capacity, proportion |
| `enabledHyperNodeOrder` | `HyperNodeOrderFn` | network-topology-aware |
| `enabledSubJobReady` | `SubJobReadyFn` | gang |
| `enabledSubJobPipelined` | `SubJobPipelinedFn` | gang |
| `enabledSubJobOrder` | `SubJobOrderFn` | gang |
| `enabledHyperNodeGradient` | `HyperNodeGradientFn` | network-topology-aware |

---

*文档生成日期：2026-06-20*
