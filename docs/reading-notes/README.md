# 源码阅读笔记

个人学习 Volcano 调度器源码过程中产出的分析文档，所有文件均为原始路径的软链接。

## 目录

| 文件 | 内容 |
|------|------|
| [binpack-analysis.md](binpack-analysis.md) | Binpack 插件 — Best-Fit 紧凑装箱算法 |
| [nodeorder-analysis.md](nodeorder-analysis.md) | NodeOrder 插件 — K8s 原生 8 种策略适配层 |
| [gang-analysis.md](gang-analysis.md) | Gang 插件 — All-or-Nothing 成组调度 |
| [plugin-action-overview.md](plugin-action-overview.md) | Plugin × Action 全览 — 注册表、调用表、连接矩阵 |

## 原始路径

- `pkg/scheduler/plugins/binpack/ANALYSIS.md`
- `pkg/scheduler/plugins/nodeorder/ANALYSIS.md`
- `pkg/scheduler/plugins/gang/ANALYSIS.md`
- `pkg/scheduler/PLUGIN-ACTION-OVERVIEW.md`
