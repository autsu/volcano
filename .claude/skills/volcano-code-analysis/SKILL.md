---
name: volcano-code-analysis
description: Analyze Volcano scheduler source code — read code, add Chinese comments, write ANALYSIS.md with Mermaid diagrams and examples, symlink to docs/reading-notes/. Use when asked to analyze any Volcano plugin, action, scheduling mechanism, or core concept.
---

# Volcano 源码分析

当用户要求分析 Volcano 项目中的任何内容（插件、Action、调度机制、核心概念、数据结构关系等），按此规范执行。

## 硬性要求：必须产出文档

**每次调用本 skill 都必须产出至少一个 markdown 文档（ANALYSIS.md 或 CONCEPTS-*.md），并建立软链接到 `docs/reading-notes/`。不得仅以对话形式作答。**

不满足此要求等同于 skill 未生效。

## 核心原则

- **一切结论必须有源码依据**：每条分析结论都要引用具体源码文件和行号作为证据（markdown 链接格式：`[file.go:42](path/to/file.go#L42)`），不得凭经验推测或编造
- 如果某个说法在源码中找不到直接支撑，标注为"推测"或"待验证"

## 分析流程

1. **先完整阅读目标文件**，再读依赖的接口/类型定义
2. **不只读插件代码**，还要追踪 Action 中如何调用这些函数——从调用方理解被调用方
3. **从源码层面验证**，不盲从用户猜测，每条结论都要有源码引用支撑
4. **交叉验证接口签名**：检查 `session_plugins.go` 中的 `AddXxxFn` 方法和对应 Action 中的调用方式，确认调用链完整

## 产出内容

### 源码注释

为目标 .go 文件添加完整中文注释，覆盖设计意图、算法逻辑、边界条件，确保编译通过。

**注释规范**：
- **只能增量添加，不得删除或修改任何已有注释**（包括英文注释、TODO、代码原作者注记）
- 新增的中文注释放在已有代码/注释的**上方**或**右侧**，不与原有内容混在同一行
- 注释内容要点：类型设计意图、函数入参/出参/核心逻辑、关键决策的理由、边界条件处理
- 注释完后必须验证 `go build` 通过

### 分析文档

命名为 `ANALYSIS.md`，放在目标目录下。10 个标准章节：

1. 概述 — 是什么、系统定位、核心文件列表
2. 核心概念 — 关键数据结构、参数含义
3. 架构设计 — Mermaid 组件关系图
4. 详细拆解 — 每个扩展点/子策略的源码级分析
5. 流程/状态机 — Mermaid 状态图或序列图
6. 配置体系 — 参数表、配置示例、调优建议
7. 测试用例分析 — 关键场景和覆盖点
8. 设计要点 — 值得学习的设计模式
9. 与其他组件的协作 — 在完整调度周期中的位置
10. 总结 — 适用场景、局限性

**文档规范**：
- **所有流程描述必须用 Mermaid 图**，禁止 ASCII art（`┌─┐` 等）
- **Mermaid 图用 TD（上到下）方向**，不用 LR（左到右）——扁平窄幅，方便渲染和阅读
- **图的宽度控制在合理范围**：多层级时纵向展开而非横向并排，避免 subgraph 并排过多
- **关键概念必须举具体例子**：假设场景 → 代码如何处理 → 结果
- **状态/决策用表格对照**
- **源码引用用 markdown 链接**：`[file.go:42](path/to/file.go#L42)`
- 首次出现的术语要解释清楚

### 软链接

所有分析文档软链到 `docs/reading-notes/`，更新 README 索引。
