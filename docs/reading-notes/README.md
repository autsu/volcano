# 源码阅读笔记

个人学习 Volcano 调度器源码过程中产出的分析文档，所有文件均为原始路径的软链接。

## 目录

| 文件 | 内容 |
|------|------|
| [binpack-analysis.md](binpack-analysis.md) | Binpack 插件 — Best-Fit 紧凑装箱算法 |
| [nodeorder-analysis.md](nodeorder-analysis.md) | NodeOrder 插件 — K8s 原生 8 种策略适配层 |
| [gang-analysis.md](gang-analysis.md) | Gang 插件 — All-or-Nothing 成组调度 |
| [plugin-action-overview.md](plugin-action-overview.md) | Plugin × Action 全览 — 注册表、调用表、连接矩阵 |
| [allocate-analysis.md](allocate-analysis.md) | Allocate Action — 核心分配流程与 Gang 协作 |
| [concepts-job-task-pod.md](concepts-job-task-pod.md) | Job / Task / Pod 概念关系 |
| [priority-analysis.md](priority-analysis.md) | Priority 优先级体系 — Job/Task 两层优先级与抢占保护 |
| [minavailable-dynamic.md](minavailable-dynamic.md) | Gang MinAvailable 动态修改 — 上调后已调度 Pod 不被驱逐 |

## 原始路径

- `pkg/scheduler/plugins/binpack/ANALYSIS.md`
- `pkg/scheduler/plugins/nodeorder/ANALYSIS.md`
- `pkg/scheduler/plugins/gang/ANALYSIS.md`
- `pkg/scheduler/plugins/gang/MINAVAILABLE-DYNAMIC.md`
- `pkg/scheduler/PLUGIN-ACTION-OVERVIEW.md`
- `pkg/scheduler/actions/allocate/ANALYSIS.md`
