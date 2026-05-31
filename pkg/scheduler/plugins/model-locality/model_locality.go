/*
Copyright 2026 The Volcano Authors.

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

// Package modellocality 演示如何为模型推理/模型服务场景编写 Volcano 调度插件。
//
// 这个插件刻意只做两件事，方便初学者理解 Volcano 插件开发的基本套路：
//   - Predicate：做“必须满足”的硬约束，比如同一个实例的多个 Pod 必须在同一台机器，
//     或者必须在同一个 NVLink domain。Predicate 返回错误后，该节点会被过滤掉。
//   - NodeOrder：做“优先选择”的软约束，比如模型权重已经在某个节点缓存，就给这个节点
//     更高分。NodeOrder 不应该过滤节点，因为没有缓存时 Pod 仍然应该能调度到冷节点上。
//
// 为了便于本地学习和模拟，这里没有依赖真实 NVLink 拓扑发现组件，也没有依赖真实的
// JuiceFS/Alluxio/P2P 缓存控制器，而是用 Kubernetes Node label/annotation 表达：
//   - label：节点属于哪个 NVLink domain。
//   - annotation：节点已经缓存了哪些模型权重。
package modellocality

import (
	"fmt"
	"sort"
	"strings"

	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
)

const (
	// PluginName 是 volcano-scheduler.conf 里启用插件时使用的名字。
	PluginName = "model-locality"

	// DefaultInstanceAnnotation 标记“逻辑实例”。
	//
	// 举例：一个 llama-serving-a 实例可能由多个 Pod 组成，这些 Pod 都设置同一个
	// instance 值。插件就能知道这些 Pod 属于同一组，需要按同一个拓扑约束放置。
	DefaultInstanceAnnotation = "model-locality.volcano.sh/instance"

	// DefaultTopologyAnnotation 标记实例内 Pod 的硬拓扑约束。
	//
	// 支持值：
	//   - same-node：同实例 Pod 必须放在同一台 Node。
	//   - same-nvlink：同实例 Pod 必须放在同一个 NVLink domain。
	DefaultTopologyAnnotation = "model-locality.volcano.sh/topology"

	// DefaultModelAnnotation 标记当前 Pod 需要加载哪个模型。
	//
	// 这个字段不参与硬过滤，只用于 NodeOrder 打分：节点 annotation 里声明已经缓存
	// 该模型时，插件给该节点加分。
	DefaultModelAnnotation = "model-locality.volcano.sh/model"

	// DefaultNVLinkLabel 是模拟 NVLink 拓扑域的 Node label。
	//
	// 真实生产环境里，这个 label 可以由节点发现组件写入；学习时可以手工 label node。
	DefaultNVLinkLabel = "model-locality.volcano.sh/nvlink-domain"

	// DefaultModelCacheAnnotation 是模拟模型缓存状态的 Node annotation。
	//
	// 真实生产环境里，这个 annotation 可以由 P2P 分发 agent、JuiceFS/Alluxio 缓存
	// 控制器或自研 DaemonSet 写入。这里用逗号分隔模型名，便于手工测试。
	DefaultModelCacheAnnotation = "model-locality.volcano.sh/cached-models"
	DefaultModelCacheDelimiter  = ","

	// DefaultCacheHitWeight 控制“模型缓存命中”的加分权重。
	//
	// Volcano 最终会把多个 NodeOrder 插件的分数相加，所以这个权重要和 binpack、
	// nodeorder 等插件一起调。如果设得太小，缓存命中可能被资源打分淹没；如果设得
	// 太大，则可能过度偏向缓存节点。
	DefaultCacheHitWeight = 10

	// DefaultSameTopologyBonusWeight 控制“靠近已有同实例 Pod”的额外加分权重。
	//
	// 注意这只是软偏好；真正的硬约束仍然在 Predicate 里完成。
	DefaultSameTopologyBonusWeight = 2
)

const (
	// SameNode 表示同一个逻辑实例的所有 Pod 必须运行在同一台 Kubernetes Node 上。
	//
	// 这个模式最严格，适合多个 Pod 必须共享本机资源的场景。一个 Pod 内的多个
	// Container 天然就在同一台 Node 上，不需要调度器额外处理；这里主要处理“多个 Pod”。
	SameNode = "same-node"

	// SameNVLink 表示同一个逻辑实例的所有 Pod 必须运行在同一个 NVLink domain 内。
	//
	// 这里的 NVLink domain 是通过 Node label 模拟出来的。两个节点 label 值相同，
	// 就认为它们在同一个高速互联拓扑网络内。
	SameNVLink = "same-nvlink"
)

const (
	// 以下参数允许用户在 volcano-scheduler.conf 中替换默认 annotation/label key。
	// 这样插件逻辑不用改代码，也能接入已有集群里不同命名规范的元数据。
	instanceAnnotationArg      = "instance.annotation"
	topologyAnnotationArg      = "topology.annotation"
	modelAnnotationArg         = "model.annotation"
	nvlinkLabelArg             = "nvlink.label"
	modelCacheAnnotationArg    = "model-cache.annotation"
	modelCacheDelimiterArg     = "model-cache.delimiter"
	cacheHitWeightArg          = "cache-hit.weight"
	sameTopologyBonusWeightArg = "same-topology-bonus.weight"
)

type modelLocalityPlugin struct {
	// 下面这些字段都是从 scheduler config arguments 解析出的运行时配置。
	// Volcano 插件通常在 New() 里读取配置，在 OnSessionOpen() 里注册调度扩展点。
	instanceAnnotation      string
	topologyAnnotation      string
	modelAnnotation         string
	nvlinkLabel             string
	modelCacheAnnotation    string
	modelCacheDelimiter     string
	cacheHitWeight          int
	sameTopologyBonusWeight int
}

// New 创建插件对象，并读取 volcano-scheduler.conf 中的可选 arguments。
//
// Volcano 加载插件的大致流程是：
//  1. pkg/scheduler/plugins/factory.go 里通过 RegisterPluginBuilder 注册 New。
//  2. 每次打开一个调度 Session 时，framework.OpenSession 调用 New(arguments)。
//  3. 然后调用插件的 OnSessionOpen(ssn)，插件在里面注册 Predicate/NodeOrder 等函数。
//
// 配置示例：
//
//   - name: model-locality
//     arguments:
//     cache-hit.weight: 20
//     same-topology-bonus.weight: 3
//
// 默认值是可直接使用的，这样初学时只要启用插件并给 Node/Pod 打 label、annotation，
// 就能观察调度行为。
func New(arguments framework.Arguments) framework.Plugin {
	plugin := &modelLocalityPlugin{
		instanceAnnotation:      DefaultInstanceAnnotation,
		topologyAnnotation:      DefaultTopologyAnnotation,
		modelAnnotation:         DefaultModelAnnotation,
		nvlinkLabel:             DefaultNVLinkLabel,
		modelCacheAnnotation:    DefaultModelCacheAnnotation,
		modelCacheDelimiter:     DefaultModelCacheDelimiter,
		cacheHitWeight:          DefaultCacheHitWeight,
		sameTopologyBonusWeight: DefaultSameTopologyBonusWeight,
	}

	arguments.GetString(&plugin.instanceAnnotation, instanceAnnotationArg)
	arguments.GetString(&plugin.topologyAnnotation, topologyAnnotationArg)
	arguments.GetString(&plugin.modelAnnotation, modelAnnotationArg)
	arguments.GetString(&plugin.nvlinkLabel, nvlinkLabelArg)
	arguments.GetString(&plugin.modelCacheAnnotation, modelCacheAnnotationArg)
	arguments.GetString(&plugin.modelCacheDelimiter, modelCacheDelimiterArg)
	arguments.GetInt(&plugin.cacheHitWeight, cacheHitWeightArg)
	arguments.GetInt(&plugin.sameTopologyBonusWeight, sameTopologyBonusWeightArg)

	// 对配置做最小防御：
	//   - 分隔符为空会导致缓存列表无法正确切分，所以退回逗号。
	//   - 权重为负没有明确语义，退回默认值。
	// 如果这里不处理，用户写错配置时可能得到很难解释的调度分数。
	if plugin.modelCacheDelimiter == "" {
		plugin.modelCacheDelimiter = DefaultModelCacheDelimiter
	}
	if plugin.cacheHitWeight < 0 {
		plugin.cacheHitWeight = DefaultCacheHitWeight
	}
	if plugin.sameTopologyBonusWeight < 0 {
		plugin.sameTopologyBonusWeight = DefaultSameTopologyBonusWeight
	}

	return plugin
}

func (ml *modelLocalityPlugin) Name() string {
	return PluginName
}

func (ml *modelLocalityPlugin) OnSessionOpen(ssn *framework.Session) {
	klog.V(5).InfoS("Enter model-locality plugin",
		"instanceAnnotation", ml.instanceAnnotation,
		"topologyAnnotation", ml.topologyAnnotation,
		"modelAnnotation", ml.modelAnnotation,
		"nvlinkLabel", ml.nvlinkLabel,
		"modelCacheAnnotation", ml.modelCacheAnnotation)
	defer klog.V(5).InfoS("Leaving model-locality plugin")

	// 先为当前调度 Session 建一个“实例 -> 已放置位置”的索引。
	//
	// 为什么需要这个索引：
	//   - Predicate 被调用时只拿到“当前待调度 task”和“候选 node”。
	//   - 但我们的策略需要知道“同一实例的其它 Pod 已经放在哪”。
	//   - 因此要在 Session 开始时扫描 ssn.Jobs，把已有 placement 缓存起来。
	//
	// 如果每次 Predicate 都全量扫描 ssn.Jobs，逻辑更难读，复杂度也更高。
	instancePlacements := ml.buildInstancePlacements(ssn)

	// Predicate 是硬过滤扩展点。
	//
	// 返回 nil：当前 node 可以继续参与后续打分。
	// 返回 error：当前 node 被过滤，Volcano 会尝试其它 node。
	//
	// 所以 same-node / same-nvlink 这类“必须满足”的需求应该放在 Predicate，
	// 而不是 NodeOrder。否则分数再低也可能被调度过去，违反硬约束。
	ssn.AddPredicateFn(ml.Name(), func(task *api.TaskInfo, node *api.NodeInfo) error {
		return ml.predicate(task, node, instancePlacements)
	})

	// NodeOrder 是软打分扩展点。
	//
	// 这里用它表达“优先调度到已有模型缓存的节点”。缓存命中不是硬条件：
	// 如果所有缓存节点资源不足，Pod 仍然应该能调度到冷节点，只是启动会慢一些。
	// 如果把缓存命中写成 Predicate，扩容时没有缓存的节点会被全部过滤，Pod 可能一直 Pending。
	ssn.AddNodeOrderFn(ml.Name(), func(task *api.TaskInfo, node *api.NodeInfo) (float64, error) {
		return ml.nodeOrder(task, node, instancePlacements), nil
	})

	// 调度 Session 内可能连续分配多个 Pod。
	//
	// 例子：同一个实例有 pod-0、pod-1 都是 Pending。Session 开始时二者都还没有
	// NodeName。如果只在 buildInstancePlacements 里扫描一次，那么 pod-0 分配到
	// node-a 后，pod-1 的 Predicate 仍然不知道 pod-0 已经去了 node-a。
	//
	// 因此这里注册 AllocateFunc：每当 Volcano 在本轮 Session 里分配一个 task，
	// 立即把它写入 instancePlacements，让后续 task 能看到最新 placement。
	ssn.AddEventHandler(&framework.EventHandler{
		AllocateFunc: func(event *framework.Event) {
			ml.rememberPlacement(event.Task, ssn.Nodes, instancePlacements)
		},
	})
}

func (ml *modelLocalityPlugin) OnSessionClose(ssn *framework.Session) {}

type instancePlacement struct {
	// nodes 记录某个 instance 已经使用过哪些 Node。
	// same-node 模式会要求后续 Pod 只能落在这些 Node 上。
	nodes map[string]struct{}

	// nvlinkDomain 记录某个 instance 第一次放置时所在的 NVLink domain。
	// same-nvlink 模式会要求后续 Pod 的候选 Node 具有相同 domain label。
	nvlinkDomain string
}

// buildInstancePlacements 从当前 Session 快照里恢复已有实例 placement。
//
// Session 是 Volcano 一轮调度看到的集群快照，ssn.Jobs 包含当前已知的 Pod/Task。
// 已经绑定或已经在本轮早些时候处理过的 task 会有 NodeName，可以作为同实例后续
// Pod 的拓扑锚点。
func (ml *modelLocalityPlugin) buildInstancePlacements(ssn *framework.Session) map[string]*instancePlacement {
	placements := map[string]*instancePlacement{}
	for _, job := range ssn.Jobs {
		for _, task := range job.Tasks {
			ml.rememberPlacement(task, ssn.Nodes, placements)
		}
	}
	return placements
}

// rememberPlacement 把一个已经有 NodeName 的 task 记录到 instance placement 索引。
//
// 这个函数会被两个地方调用：
//   - Session 打开时，用于恢复历史上已经放好的 Pod。
//   - AllocateFunc 回调里，用于记录本轮刚分配的 Pod。
//
// 这样可以避免同一个逻辑实例的第二个 Pod 在同一轮调度里“看不到”第一个 Pod。
func (ml *modelLocalityPlugin) rememberPlacement(task *api.TaskInfo, nodes map[string]*api.NodeInfo, placements map[string]*instancePlacement) {
	instance := ml.instanceName(task)
	if instance == "" || task.NodeName == "" {
		return
	}
	node, found := nodes[task.NodeName]
	if !found {
		return
	}
	placement := placements[instance]
	if placement == nil {
		placement = &instancePlacement{nodes: map[string]struct{}{}}
		placements[instance] = placement
	}
	placement.nodes[task.NodeName] = struct{}{}

	// nvlinkDomain 只在第一次拿到非空 placement 时设置。
	// 对 same-nvlink 来说，第一个被放置的 Pod 决定了该实例后续 Pod 必须跟随的 domain。
	if placement.nvlinkDomain == "" {
		placement.nvlinkDomain = ml.nvlinkDomain(node)
	}
}

// predicate 实现硬拓扑约束。
//
// 注意：这里不会处理模型缓存。模型缓存只是加速启动的偏好，不是“必须满足”的条件。
// 把缓存写进 Predicate 的常见后果是：如果没有任何节点缓存该模型，Pod 会一直无法调度。
func (ml *modelLocalityPlugin) predicate(task *api.TaskInfo, node *api.NodeInfo, placements map[string]*instancePlacement) error {
	instance := ml.instanceName(task)
	if instance == "" {
		// 没有 instance annotation 的 Pod 不参与本插件的实例拓扑约束。
		// 这样启用插件后，不会影响普通 workload。
		return nil
	}

	mode := ml.topologyMode(task)
	if mode == "" {
		// 没有声明 topology 的 Pod 也不做硬约束。
		// 用户可以只使用 model annotation 获得缓存加分。
		return nil
	}

	placement := placements[instance]
	if placement == nil {
		// 这是该 instance 的第一个 Pod，尚无拓扑锚点，任何候选节点都可以。
		// 一旦该 Pod 被分配，AllocateFunc 会记录它的位置。
		return nil
	}

	switch mode {
	case SameNode:
		// same-node：后续 Pod 只能调度到该 instance 已经使用过的 Node。
		// 如果不在 Predicate 里过滤，NodeOrder 的低分节点仍可能被选中，破坏“必须同机”。
		if _, ok := placement.nodes[node.Name]; ok {
			return nil
		}
		return ml.fitError(task, node, fmt.Sprintf("instance %q must stay on node(s) %s", instance, sortedKeys(placement.nodes)))

	case SameNVLink:
		// same-nvlink：后续 Pod 只能调度到同一个 NVLink domain。
		// 这里的 domain 来自 Node label，模拟真实 NVLink 拓扑网络。
		want := placement.nvlinkDomain
		got := ml.nvlinkDomain(node)
		if want == "" {
			// 已有 Pod 所在节点没有 NVLink label，插件无法判断“同一个 domain”。
			// 这里选择拒绝后续节点，避免静默地把同实例 Pod 放散。
			return ml.fitError(task, node, fmt.Sprintf("instance %q already has pods on nodes without %q label", instance, ml.nvlinkLabel))
		}
		if got == want {
			return nil
		}
		return ml.fitError(task, node, fmt.Sprintf("instance %q must stay in NVLink domain %q, node domain is %q", instance, want, got))

	default:
		// 未知 topology 值先忽略，不阻塞集群调度。
		//
		// 生产环境如果希望更严格，可以补一个 validating webhook，在 Pod 创建时就拒绝
		// 拼写错误的 annotation。调度插件里直接拒绝未知值也可以，但新手调试时容易
		// 因为 annotation 写错导致 Pod 全部 Pending。
		klog.V(4).InfoS("Ignoring unsupported model locality topology mode",
			"namespace", task.Namespace, "pod", task.Name, "mode", mode)
		return nil
	}
}

// nodeOrder 实现软打分。
//
// Volcano 会把所有启用的 NodeOrder 插件分数累加，然后选择综合分更高的节点。
// 因此这里返回的分数不是“最终分数”，而是本插件对候选节点的加分。
func (ml *modelLocalityPlugin) nodeOrder(task *api.TaskInfo, node *api.NodeInfo, placements map[string]*instancePlacement) float64 {
	score := 0.0

	// 模型缓存命中加分：
	//   - Pod annotation 说明需要哪个模型。
	//   - Node annotation 说明节点本地已经缓存哪些模型。
	//   - 命中时加分，扩容 Pod 更倾向于落到可秒级拉起的节点。
	//
	// 这对应用户提到的 P2P 分发、JuiceFS/Alluxio 本地缓存场景。
	if model := ml.modelName(task); model != "" && ml.nodeHasCachedModel(node, model) {
		score += float64(fwk.MaxNodeScore * int64(ml.cacheHitWeight))
	}

	// 同实例靠近已有 placement 的轻量加分：
	//   - 如果 node 正好是已有 Pod 所在 Node，加分。
	//   - 如果 node 与已有 Pod 在同一 NVLink domain，也加分。
	//
	// 这不是硬约束，只是让未声明 topology 的 Pod 也具备一点 locality 倾向。
	// 已声明 same-node/same-nvlink 的 Pod 会先经过 Predicate 硬过滤。
	if instance := ml.instanceName(task); instance != "" {
		if placement := placements[instance]; placement != nil {
			if _, ok := placement.nodes[node.Name]; ok {
				score += float64(fwk.MaxNodeScore * int64(ml.sameTopologyBonusWeight))
			} else if placement.nvlinkDomain != "" && ml.nvlinkDomain(node) == placement.nvlinkDomain {
				score += float64(fwk.MaxNodeScore * int64(ml.sameTopologyBonusWeight))
			}
		}
	}

	klog.V(4).InfoS("Model locality score calculated", "namespace", task.Namespace, "pod", task.Name, "node", node.Name, "score", score)
	return score
}

// fitError 把本插件的拒绝原因包装成 Volcano 能识别的 FitError。
//
// Code 使用 UnschedulableAndUnresolvable，是因为节点的 label/topology 不会因为抢占
// 其它 Pod 而改变。即使抢占释放资源，一个不在同一 NVLink domain 的节点仍然不合格。
func (ml *modelLocalityPlugin) fitError(task *api.TaskInfo, node *api.NodeInfo, reason string) error {
	return api.NewFitErrWithStatus(task, node, &api.Status{
		Plugin: PluginName,
		Code:   api.UnschedulableAndUnresolvable,
		Reason: reason,
	})
}

// instanceName 读取 Pod 上的逻辑实例名。
//
// 只有 instance 相同的 Pod 才会互相影响；不同实例即使使用同一个模型，也不会被
// same-node/same-nvlink 绑在一起。
func (ml *modelLocalityPlugin) instanceName(task *api.TaskInfo) string {
	return taskAnnotation(task, ml.instanceAnnotation)
}

// topologyMode 读取 Pod 声明的硬拓扑模式。
func (ml *modelLocalityPlugin) topologyMode(task *api.TaskInfo) string {
	return strings.TrimSpace(taskAnnotation(task, ml.topologyAnnotation))
}

// modelName 读取 Pod 需要的模型名，用于模型缓存打分。
func (ml *modelLocalityPlugin) modelName(task *api.TaskInfo) string {
	return strings.TrimSpace(taskAnnotation(task, ml.modelAnnotation))
}

// taskAnnotation 安全读取 task.Pod.Annotations。
//
// 调度器内部测试或异常对象里 task.Pod 可能为空，所以辅助函数统一做 nil 判断，
// 避免插件因为一个异常 task panic，影响整个调度循环。
func taskAnnotation(task *api.TaskInfo, key string) string {
	if task == nil || task.Pod == nil || key == "" {
		return ""
	}
	return strings.TrimSpace(task.Pod.Annotations[key])
}

// nvlinkDomain 读取候选节点所属的模拟 NVLink domain。
//
// 生产中可以把这个 label 替换成真实拓扑发现组件写入的 label；插件只关心 label 值
// 是否相同，不关心这个值如何产生。
func (ml *modelLocalityPlugin) nvlinkDomain(node *api.NodeInfo) string {
	if node == nil || node.Node == nil || ml.nvlinkLabel == "" {
		return ""
	}
	return strings.TrimSpace(node.Node.Labels[ml.nvlinkLabel])
}

// nodeHasCachedModel 判断节点是否声明已经缓存了指定模型。
//
// 这里用 annotation 的逗号分隔字符串是为了演示简单；生产环境可以改成更结构化的
// 数据来源，例如 CRD、ConfigMap、节点 agent 上报的状态，或者直接在 NodeInfo.Others
// 里挂自定义缓存信息。
func (ml *modelLocalityPlugin) nodeHasCachedModel(node *api.NodeInfo, model string) bool {
	if node == nil || node.Node == nil || model == "" {
		return false
	}
	raw := node.Node.Annotations[ml.modelCacheAnnotation]
	for _, cachedModel := range strings.Split(raw, ml.modelCacheDelimiter) {
		if strings.TrimSpace(cachedModel) == model {
			return true
		}
	}
	return false
}

// sortedKeys 只用于生成稳定、可读的错误信息。
//
// map 遍历顺序是随机的；如果不排序，测试和日志里同一组节点可能每次顺序不同。
func sortedKeys(values map[string]struct{}) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
