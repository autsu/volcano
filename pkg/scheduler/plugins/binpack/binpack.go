/*
Copyright 2019 The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package binpack

import (
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
)

const (
	// PluginName 是 Volcano 调度器中 binpack 插件的唯一名称。
	// 在调度配置的 tiers.plugins.name 字段中引用此名称即可启用该插件。
	PluginName = "binpack"
)

const (
	// BinpackWeight 是全局 Binpack 优先级的权重键名。
	// 用于控制 binpack 打分在整个节点优先级中的总体影响力。
	// 取值范围建议 [0, 10]，设为 0 会跳过打分注册。
	BinpackWeight = "binpack.weight"

	// BinpackCPU 是 CPU 资源维度的权重键名。
	// 值越大，CPU 维度的装箱紧凑度对最终得分影响越大。
	BinpackCPU = "binpack.cpu"

	// BinpackMemory 是内存资源维度的权重键名。
	// 值越大，内存维度的装箱紧凑度对最终得分影响越大。
	BinpackMemory = "binpack.memory"

	// BinpackResources 是扩展资源列表的键名。
	// 值为逗号分隔的资源名称字符串（如 "nvidia.com/gpu, example.com/foo"）。
	// 每个列出的资源会自动创建对应的 binpack.resources.<name> 权重键。
	BinpackResources = "binpack.resources"

	// BinpackResourcesPrefix 是扩展资源权重键的前缀。
	// 例如 GPU 权重键为 "binpack.resources.nvidia.com/gpu"。
	BinpackResourcesPrefix = BinpackResources + "."

	// resourceFmt 是权重信息输出的格式化字符串模板。
	// 生成形如 "binpack.cpu[5]" 的可读输出。
	resourceFmt = "%s[%d]"
)

// priorityWeight 封装了 binpack 插件的所有权重配置。
//
// 每个资源维度（CPU、内存、GPU 等扩展资源）都有独立的权重，
// BinPackingWeight 作为全局缩放因子控制整体优先级得分的影响程度。
//
// 权重设计理念：
//   - 权重 > 1: 该维度在装箱决策中更重要，优先填满该资源
//   - 权重 = 1: 默认权重，与其他维度平等参与计算
//   - 权重 = 0: 该维度不参与打分（但在 BinPackingScore 中通过 continue 跳过）
//   - 权重 < 0: 会自动被纠正为 1
type priorityWeight struct {
	// BinPackingWeight 是全局装箱权重，作为最终得分的缩放因子。
	// 设为 0 时，插件不注册节点排序函数，完全跳过打分环节。
	BinPackingWeight int

	// BinPackingCPU 是 CPU 资源维度的权重。
	// CPU 在内部以 MilliCPU（毫核）为单位计算。
	BinPackingCPU int

	// BinPackingMemory 是内存资源维度的权重。
	// 内存以字节为单位计算。
	BinPackingMemory int

	// BinPackingResources 存储所有资源维度（含 CPU/内存及扩展资源）到其权重的映射。
	// 这是运行时实际使用的查找表，CPU 和 Memory 在 calculateWeight 中也会被写入此 map。
	BinPackingResources map[v1.ResourceName]int
}

// String 返回 priorityWeight 的可读字符串表示，用于日志和调试。
//
// 输出示例:
//
//	"binpack.weight[10], binpack.cpu[5], binpack.memory[1], no extend resources."
//	"binpack.weight[10], binpack.cpu[5], binpack.memory[1], nvidia.com/gpu[2], example.com/foo[3]"
func (w *priorityWeight) String() string {
	// 预分配切片容量：3 个基础字段 + 扩展资源数（或 1 个 "no extend resources"）
	length := 3
	if extendLength := len(w.BinPackingResources); extendLength == 0 {
		length++
	} else {
		length += extendLength
	}
	msg := make([]string, 0, length)
	msg = append(msg,
		fmt.Sprintf(resourceFmt, BinpackWeight, w.BinPackingWeight),
		fmt.Sprintf(resourceFmt, BinpackCPU, w.BinPackingCPU),
		fmt.Sprintf(resourceFmt, BinpackMemory, w.BinPackingMemory),
	)

	if len(w.BinPackingResources) == 0 {
		msg = append(msg, "no extend resources.")
	} else {
		for name, weight := range w.BinPackingResources {
			msg = append(msg, fmt.Sprintf(resourceFmt, name, weight))
		}
	}
	return strings.Join(msg, ", ")
}

// binpackPlugin 是 Binpack 插件的核心结构体，实现了 framework.Plugin 接口。
//
// 插件生命周期：
//  1. New() 创建实例并解析配置参数
//  2. OnSessionOpen() 在每个调度会话开始时被调用，注册节点排序函数
//  3. OnSessionClose() 在会话结束时被调用（当前为空实现）
//
// Binpack 属于 NodeOrder 类型的插件，仅参与节点优先级打分，
// 不参与 predicate（预选）、preempt（抢占）、reclaim（回收）等环节。
type binpackPlugin struct {
	// weight 保存从调度配置中解析出的所有权重参数。
	// 在整个调度会话期间保持不变。
	weight priorityWeight
}

// New 是 binpack 插件的工厂函数，由 framework.PluginBuilder 调用。
//
// 调度配置示例：
//
//	actions: "enqueue, reclaim, allocate, backfill, preempt"
//	tiers:
//	- plugins:
//	  - name: binpack
//	    arguments:
//	      binpack.weight: 10
//	      binpack.cpu: 5
//	      binpack.memory: 1
//	      binpack.resources: nvidia.com/gpu, example.com/foo
//	      binpack.resources.nvidia.com/gpu: 2
//	      binpack.resources.example.com/foo: 3
func New(arguments framework.Arguments) framework.Plugin {
	weight := calculateWeight(arguments)
	return &binpackPlugin{weight: weight}
}

// calculateWeight 从调度配置参数中解析并构建 priorityWeight 结构体。
//
// 参数解析规则：
//  1. binpack.weight  → 全局缩放权重（默认 1）
//  2. binpack.cpu     → CPU 维度权重（默认 1，负数自动纠正为 1）
//  3. binpack.memory  → 内存维度权重（默认 1，负数自动纠正为 1）
//  4. binpack.resources → 逗号分隔的扩展资源名列表
//     每个资源自动对应 binpack.resources.<name> 权重键（默认 1）
//
// CPU 和内存的权重在解析完成后会被合并到 BinPackingResources map 中，
// 使得运行时打分可以统一通过该 map 查找任意资源的权重。
//
// 返回值一定包含合法的权重值——所有负数会被安全纠正为 1。
func calculateWeight(args framework.Arguments) priorityWeight {
	/*
	   User Should give priorityWeight in this format(binpack.weight, binpack.cpu, binpack.memory).
	   Support change the weight about cpu, memory and additional resource by arguments.

	   actions: "enqueue, reclaim, allocate, backfill, preempt"
	   tiers:
	   - plugins:
	     - name: binpack
	       arguments:
	         binpack.weight: 10
	         binpack.cpu: 5
	         binpack.memory: 1
	         binpack.resources: nvidia.com/gpu, example.com/foo
	         binpack.resources.nvidia.com/gpu: 2
	         binpack.resources.example.com/foo: 3
	*/

	// 初始化权重结构体，所有值默认为 1（安全的默认值）
	weight := priorityWeight{
		BinPackingWeight:    1,
		BinPackingCPU:       1,
		BinPackingMemory:    1,
		BinPackingResources: make(map[v1.ResourceName]int),
	}

	// 解析全局装箱权重，若未配置则保持默认值 1
	args.GetInt(&weight.BinPackingWeight, BinpackWeight)

	// 解析 CPU 权重，若为负数则纠正为 1（防御性编程）
	args.GetInt(&weight.BinPackingCPU, BinpackCPU)
	if weight.BinPackingCPU < 0 {
		weight.BinPackingCPU = 1
	}

	// 解析内存权重，若为负数则纠正为 1（防御性编程）
	args.GetInt(&weight.BinPackingMemory, BinpackMemory)
	if weight.BinPackingMemory < 0 {
		weight.BinPackingMemory = 1
	}

	// 解析扩展资源列表字符串，格式如 "nvidia.com/gpu, example.com/foo"
	resourcesStr, ok := args[BinpackResources].(string)
	if !ok {
		resourcesStr = ""
	}

	// 按逗号拆分并逐个处理
	resources := strings.Split(resourcesStr, ",")
	for _, resource := range resources {
		resource = strings.TrimSpace(resource)
		if resource == "" {
			continue
		}

		// 拼接完整键名：binpack.resources.<ResourceName>
		resourceKey := BinpackResourcesPrefix + resource
		resourceWeight := 1
		args.GetInt(&resourceWeight, resourceKey)
		if resourceWeight < 0 {
			resourceWeight = 1
		}
		// 将扩展资源及其权重存入查找表
		weight.BinPackingResources[v1.ResourceName(resource)] = resourceWeight
	}

	// 将 CPU 和内存也写入 BinPackingResources 统一查找表。
	// 这样打分时对所有资源（含 CPU/内存/扩展资源）使用统一的查表逻辑，
	// 无需为 CPU/内存写特殊的 case 分支。
	weight.BinPackingResources[v1.ResourceCPU] = weight.BinPackingCPU
	weight.BinPackingResources[v1.ResourceMemory] = weight.BinPackingMemory

	return weight
}

// Name 返回插件名称，供调度框架识别和引用。
func (bp *binpackPlugin) Name() string {
	return PluginName
}

// OnSessionOpen 在每次调度会话开始时被框架调用。
//
// 其主要职责是：
//  1. 诊断日志——检查配置的权重资源是否在集群节点上存在，不存在则警告
//  2. 注册 nodeOrderFn——创建闭包并注册到 Session，供 allocate/preempt 等 action 打分使用
//
// 关键设计决策：
//   - 当 BinPackingWeight == 0 时，跳过 nodeOrderFn 注册，完全禁用 binpack 打分。
//     这比注册一个永远返回 0 的函数更高效——调度框架不会调用不存在的函数。
//   - nodeOrderFn 是闭包，捕获了 bp.weight，每个会话使用一致的权重配置。
func (bp *binpackPlugin) OnSessionOpen(ssn *framework.Session) {
	klog.V(5).Infof("Enter binpack plugin ...")
	defer func() {
		klog.V(5).Infof("Leaving binpack plugin. %s ...", bp.weight.String())
	}()

	// 诊断：检查配置了权重但集群所有节点都不具备的资源。
	// 例如配置了 nvidia.com/gpu 的权重，但集群中没有 GPU 节点——这种情况
	// 可能是配置错误，值得在 V(4) 日志中提醒运维人员。
	if klog.V(4).Enabled() {
		notFoundResource := []string{}
		for resource := range bp.weight.BinPackingResources {
			found := false
			for _, nodeInfo := range ssn.Nodes {
				// 检查节点是否具备该资源
				if nodeInfo.Allocatable.Get(resource) > 0 {
					found = true
					break
				}
			}
			if !found {
				notFoundResource = append(notFoundResource, string(resource))
			}
		}
		klog.V(4).Infof("resources [%s] record in weight but not found on any node",
			strings.Join(notFoundResource, ", "))
	}

	// 构建节点排序闭包。
	// 该闭包的签名必须匹配 api.NodeOrderFn: func(*TaskInfo, *NodeInfo) (float64, error)
	// 被 allocate action 等调用，为每个 task-node 对计算装箱得分。
	nodeOrderFn := func(task *api.TaskInfo, node *api.NodeInfo) (float64, error) {
		binPackingScore := BinPackingScore(task, node, bp.weight)
		klog.V(4).Infof("Binpack score for Task %s/%s on node %s is: %v",
			task.Namespace, task.Name, node.Name, binPackingScore)
		return binPackingScore, nil
	}

	// 仅在全局权重非零时注册 nodeOrderFn。
	// BinPackingWeight == 0 意味着管理员想要禁用 binpack 打分。
	if bp.weight.BinPackingWeight != 0 {
		ssn.AddNodeOrderFn(bp.Name(), nodeOrderFn)
	} else {
		klog.Infof("binpack weight is zero, skip node order function")
	}
}

// OnSessionClose 在调度会话结束时被调用。
// binpack 插件不需要在会话结束时做清理，因此为空实现。
func (bp *binpackPlugin) OnSessionClose(ssn *framework.Session) {
}

// BinPackingScore 计算一个 Task 在指定 Node 上的装箱得分。
//
// 核心算法——加权 Best-Fit：
//
//  1. 遍历 Task 请求的每个资源维度（CPU、内存、GPU 等）
//  2. 对每个资源维度调用 ResourceBinPackingScore 计算单维度得分
//  3. 按权重加权求和：score = Σ(resourceScore_i)
//  4. 归一化到 [0, 1] 区间：score /= weightSum
//  5. 缩放到最终分数区间：[0, MaxNodeScore * BinPackingWeight]
//
// 分数语义：
//   - 更高的分数 = 节点该资源利用率更高 = 更紧凑的装箱
//   - 得分为 0 = Task 无法调度到该节点（至少一个资源维度容量不足）
//
// 这实现了 Best-Fit 装箱策略：
// 优先选择已使用资源占比最高的节点，使得新 Task 恰好"填满"剩余空间，
// 从而减少资源碎片、提高集群整体资源利用率。
//
// 参数:
//   - task: 待调度的任务，其 Resreq 字段描述了所需的各维度资源量
//   - node: 候选节点，包含 Allocatable（总可分配量）和 Used（已用量）
//   - weight: 权重配置，决定各资源维度在打分中的重要程度
//
// 返回值:
//   - 正数: 装箱得分，分数越高越优先
//   - 0: 节点无法满足该 Task 的资源需求
func BinPackingScore(task *api.TaskInfo, node *api.NodeInfo, weight priorityWeight) float64 {
	score := 0.0                    // 各维度得分累加器
	weightSum := 0                  // 实际参与计算的权重之和（用于归一化）
	requested := task.Resreq        // Task 请求的资源量
	allocatable := node.Allocatable // 节点可分配资源总量
	used := node.Used               // 节点已使用资源量

	// 遍历 Task 需要的每个资源维度（自动排除请求量为 0 的维度）
	for _, resource := range requested.ResourceNames() {
		request := requested.Get(resource)
		if request == 0 {
			continue // 跳过零请求的资源维度
		}
		allocate := allocatable.Get(resource)
		nodeUsed := used.Get(resource)

		// 查找该资源维度的权重；若未配置则跳过该维度
		resourceWeight, found := weight.BinPackingResources[resource]
		if !found {
			continue
		}

		// 计算单维度装箱得分
		resourceScore, err := ResourceBinPackingScore(request, allocate, nodeUsed, resourceWeight)
		if err != nil {
			// 资源容量不足，该节点不适合此 Task
			klog.V(4).Infof("task %s/%s cannot binpack node %s: resource: %s is %s, need %f, used %f, allocatable %f",
				task.Namespace, task.Name, node.Name, resource, err.Error(), request, nodeUsed, allocate)
			return 0
		}
		klog.V(5).Infof("task %s/%s on node %s resource %s, need %f, used %f, allocatable %f, weight %d, score %f",
			task.Namespace, task.Name, node.Name, resource, request, nodeUsed, allocate, resourceWeight, resourceScore)

		score += resourceScore
		weightSum += resourceWeight
	}

	// 归一化：将 [0, weightSum] 映射到 [0, 1]。
	// 避免因资源维度数量差异导致分数不可比较。
	if weightSum > 0 {
		score /= float64(weightSum)
	}

	// 缩放到最终得分区间：[0, MaxNodeScore * BinPackingWeight]
	// MaxNodeScore 是 Kubernetes 调度框架定义的最大节点分数（通常为 100），
	// 乘以 BinPackingWeight 后可以在多插件环境下调节 binpack 的相对影响力。
	score *= float64(fwk.MaxNodeScore * int64(weight.BinPackingWeight))

	return score
}

// ResourceBinPackingScore 计算单个资源维度的装箱得分。
//
// 算法公式：
//
//	score = (used + requested) * weight / capacity
//
// 直观理解：
//   - 分子 (used + requested) 表示调度后该资源的预期使用量
//   - 分母 capacity 是节点该资源的总容量
//   - 整个公式计算的是"调度后的资源利用率"，乘以权重后作为得分
//
// 为什么这个公式能实现 Best-Fit？
//   - 假设两个节点都有足够容量，已用量更高的节点会得到更高分
//   - 这引导调度器将 Task 放到"已经比较满"的节点上
//   - 结果是尽量填满已有节点，减少节点碎片化
//
// 边界条件：
//   - capacity == 0 或 weight == 0 → 返回 0（该维度不参与投票）
//   - used + requested > capacity → 返回错误（节点容量不够）
//
// 参数:
//   - requested: Task 对该资源的需求量
//   - capacity: 节点该资源的总容量
//   - used: 节点该资源的已使用量
//   - weight: 该资源维度的权重
//
// 返回值:
//   - 正数: 装箱得分
//   - 0, nil: 权重或容量为 0，该维度不参与打分
//   - 0, error: 容量不足
func ResourceBinPackingScore(requested, capacity, used float64, weight int) (float64, error) {
	// 容量为 0 表示节点不具备该资源；权重为 0 表示不关心该维度
	if capacity == 0 || weight == 0 {
		return 0, nil
	}

	// 计算调度后的预期使用量
	usedFinally := requested + used
	if usedFinally > capacity {
		// 节点剩余容量不足以容纳该 Task
		return 0, fmt.Errorf("not enough")
	}

	// Best-Fit 核心公式：调度后利用率 × 权重
	score := usedFinally * float64(weight) / capacity
	return score, nil
}
