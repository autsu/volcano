# Volcano 项目指引

## 源码分析规范

当用户要求分析 Volcano 调度器中的任何内容（插件、Action、调度机制、核心概念等）时，**必须产出文档**（ANALYSIS.md 或 CONCEPTS-*.md），不得仅以对话形式作答。详见 `/volcano-code-analysis` skill。

按以下流程产出：

### 分析流程

1. **先完整阅读目标文件**，再读依赖的接口/类型定义
2. **不只读插件代码**，还要追踪 Action 中如何调用这些函数，从调用方理解被调用方
3. **从源码层面验证**，不盲从用户猜测，每条结论都要有源码引用支撑

### 产出内容

**源码注释**：目标 .go 文件添加完整中文注释，覆盖设计意图、算法逻辑、边界条件，编译通过。

**分析文档**（`ANALYSIS.md`，放插件目录下）：

10 个标准章节：概述 → 核心概念 → 架构设计 → 详细拆解 → 流程/状态机 → 配置 → 测试 → 设计要点 → 与其他组件协作 → 总结。

文档规范：
- **流程描述必须用 Mermaid 图**，禁止 ASCII art（`┌─┐` 等）
- **关键概念必须举具体例子**：场景 → 代码处理 → 结果
- **状态/决策用表格对照**
- **源码引用用 markdown 链接**：`[file.go:42](path/to/file.go#L42)`
- 首次出现的术语要解释清楚

**软链接**：完成后链到 `docs/reading-notes/`，更新索引 README。

```
docs/reading-notes/
├── README.md
├── binpack-analysis.md          → pkg/scheduler/plugins/binpack/ANALYSIS.md
├── nodeorder-analysis.md        → pkg/scheduler/plugins/nodeorder/ANALYSIS.md
├── gang-analysis.md             → pkg/scheduler/plugins/gang/ANALYSIS.md
└── plugin-action-overview.md    → pkg/scheduler/PLUGIN-ACTION-OVERVIEW.md
```
