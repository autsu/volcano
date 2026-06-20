/*
Copyright 2019 The Kubernetes Authors.
Copyright 2019-2025 The Volcano Authors.

Modifications made by Volcano authors:
- Refactored to use Kubernetes native scheduling plugins for improved compatibility

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

// Package nodeorder 是 Volcano 调度器的节点优先级打分插件。
//
// 与 binpack 插件（纯 Volcano 自研算法）不同，nodeorder 的核心设计理念是
// **复用 Kubernetes 原生调度插件的打分能力**，将它们包装成 Volcano 框架可用的
// NodeOrder 插件。
//
// 内部集成了 8 种 K8s 原生 ScorePlugin，分为两类：
//
//	NodeOrderScorePlugins（逐节点打分）
//	  ├── NodeResourcesFit.LeastAllocated    ← 分散策略（与 binpack 相反）
//	  ├── NodeResourcesFit.MostAllocated     ← 紧凑策略（与 binpack 相同）
//	  ├── NodeResourcesBalancedAllocation    ← 资源均衡策略
//	  ├── NodeAffinity                       ← 节点亲和性偏好
//	  └── ImageLocality                      ← 镜像本地性偏好
//
//	ScorePlugins（批量打分，需要全局视角）
//	  ├── InterPodAffinity                   ← Pod 间亲和性
//	  ├── TaintToleration                    ← 污点容忍度
//	  └── PodTopologySpread                  ← Pod 拓扑分散
//
// 配置示例：
//
//	actions: "enqueue, reclaim, allocate, backfill, preempt"
//	tiers:
//	- plugins:
//	  - name: nodeorder
//	    arguments:
//	      leastrequested.weight: 1       # 分散策略权重
//	      mostrequested.weight: 0        # 紧凑策略权重（默认关闭）
//	      balancedresource.weight: 1     # 均衡策略权重
//	      nodeaffinity.weight: 2         # 节点亲和性权重
//	      podaffinity.weight: 2          # Pod 亲和性权重
//	      tainttoleration.weight: 3      # 污点容忍权重
//	      imagelocality.weight: 1        # 镜像本地性权重
//	      podtopologyspread.weight: 2    # 拓扑分散权重
package nodeorder

import (
	"context"

	v1 "k8s.io/api/core/v1"
	utilFeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	k8sframework "k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/imagelocality"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/interpodaffinity"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodeaffinity"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/noderesources"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/podtopologyspread"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/tainttoleration"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins/util/k8s"
	"volcano.sh/volcano/pkg/scheduler/plugins/util/nodescore"
)

const (
	// PluginName 是 Volcano 调度器中 nodeorder 插件的唯一名称。
	PluginName = "nodeorder"

	// NodeAffinityWeight 是节点亲和性优先级的权重键名。
	// 控制 PreferredDuringScheduling 亲和性规则在打分中的影响力。
	NodeAffinityWeight = "nodeaffinity.weight"

	// PodAffinityWeight 是 Pod 间亲和性优先级的权重键名。
	// 控制 Pod 间 PreferredDuringScheduling 规则在打分中的影响力。
	PodAffinityWeight = "podaffinity.weight"

	// LeastRequestedWeight 是最少请求优先级的权重键名。
	// **这是与 binpack 相反的策略**——优先将 Pod 调度到资源利用率最低的节点，
	// 实现工作负载在集群中的均匀分散（Spread）。
	LeastRequestedWeight = "leastrequested.weight"

	// BalancedResourceWeight 是资源均衡优先级的权重键名。
	// 优先选择 CPU 和内存使用比例最接近的节点（均衡利用各维度资源）。
	BalancedResourceWeight = "balancedresource.weight"

	// MostRequestedWeight 是最多请求优先级的权重键名。
	// **这是与 binpack 相同的策略**——优先将 Pod 调度到资源利用率最高的节点，
	// 实现紧凑装箱（Binpacking / Best-Fit）。默认权重为 0（关闭）。
	MostRequestedWeight = "mostrequested.weight"

	// TaintTolerationWeight 是污点容忍优先级的权重键名。
	// Pod 对节点污点的容忍度越高，该节点得分越高。
	TaintTolerationWeight = "tainttoleration.weight"

	// ImageLocalityWeight 是镜像本地性优先级的权重键名。
	// 节点上已有 Pod 所需镜像则得分更高，减少镜像拉取时间。
	ImageLocalityWeight = "imagelocality.weight"

	// PodTopologySpreadWeight 是 Pod 拓扑分散优先级的权重键名。
	// 基于 PodTopologySpread 约束减少跨拓扑域的倾斜度（skew）。
	PodTopologySpreadWeight = "podtopologyspread.weight"
)

// NodeOrderPlugin 是 nodeorder 插件的核心结构体。
//
// 它内部持有两类 K8s 原生插件：
//
//	NodeOrderScorePlugins — 可逐节点独立打分的插件。
//	  适用于 NodeOrderFn（单节点打分），也参与 BatchNodeOrderFn。
//	  内部通过 ScorePluginWithWeight 同时保存插件实例和权重。
//
//	ScorePlugins — 需要全局视角的批量打分插件。
//	  仅用于 BatchNodeOrderFn（批量节点打分）。
//	  如 InterPodAffinity 需要看所有节点上已运行的 Pod 才能计算。
//
// 这种两分类设计是出于性能考量：单节点打分路径更轻量，批量打分只在特定场景使用。
type NodeOrderPlugin struct {
	// pluginArguments 保存原始的调度配置参数（当前仅存储，未运行时使用）
	pluginArguments framework.Arguments

	// weight 保存所有 8 种子优先级的权重配置
	weight priorityWeight

	// Handle 是 K8s 调度框架句柄，提供 ClientSet、Informer、SharedLister 等能力
	Handle fwk.Handle

	// ScorePlugins 存储批量打分插件（InterPodAffinity, TaintToleration, PodTopologySpread）
	ScorePlugins map[string]nodescore.BaseScorePlugin

	// NodeOrderScorePlugins 存储逐节点打分插件及其权重
	NodeOrderScorePlugins map[string]ScorePluginWithWeight
}

// ScorePluginWithWeight 将 K8s 原生 ScorePlugin 与其在 Volcano 中的权重绑定。
//
// 权重的作用：
//   - 原始 K8s Score 范围是 [0, 100]
//   - 乘以 weight 后得到加权分数
//   - 最终所有子插件的加权分数累加为 nodeorder 的总分
type ScorePluginWithWeight struct {
	plugin fwk.ScorePlugin // K8s 原生 ScorePlugin 实例
	weight int             // Volcano 侧配置的权重
}

// New 是 nodeorder 插件的工厂函数。
// 注意：此时只解析权重参数，K8s 原生插件的初始化延迟到 OnSessionOpen 阶段。
func New(arguments framework.Arguments) framework.Plugin {
	weight := calculateWeight(arguments)
	return &NodeOrderPlugin{pluginArguments: arguments, weight: weight}
}

func (pp *NodeOrderPlugin) Name() string {
	return PluginName
}

// priorityWeight 封装了 nodeorder 插件下所有 8 种子优先级的权重配置。
//
// 默认权重设计原则：
//   - leastReqWeight=1, mostReqWeight=0  → 默认倾向分散，关闭紧凑装箱
//   - nodeAffinityWeight=2               → 尊重用户配置的亲和性偏好
//   - taintTolerationWeight=3            → 污点容忍最重要，避免调度到不合适的节点
//   - podTopologySpreadWeight=2          → 与 K8s 默认保持一致
//   - 所有 weight=0 的子插件在 InitPlugin 中不会被创建，节省资源
type priorityWeight struct {
	leastReqWeight          int // 最少请求（分散策略）权重
	mostReqWeight           int // 最多请求（紧凑策略）权重
	nodeAffinityWeight      int // 节点亲和性权重
	podAffinityWeight       int // Pod 间亲和性权重
	balancedResourceWeight  int // 资源均衡分配权重
	taintTolerationWeight   int // 污点容忍权重
	imageLocalityWeight     int // 镜像本地性权重
	podTopologySpreadWeight int // Pod 拓扑分散权重
}

// calculateWeight 从配置参数中解析各子优先级权重。
//
// 默认值设计（向后兼容）：
//   - LeastRequested 默认开启（权重 1），遵循 K8s 默认调度行为
//   - MostRequested 默认关闭（权重 0），需要显式配置才会启用
//
// 参数示例：
//
//	arguments:
//	  leastrequested.weight: 1
//	  mostrequested.weight: 0
//	  nodeaffinity.weight: 2
//	  balancedresource.weight: 1
func calculateWeight(args framework.Arguments) priorityWeight {
	// 初始化默认权重。
	// LeastRequested 默认启用（与 K8s 原生调度行为一致），
	// MostRequested 默认关闭（需要用户显式开启紧凑装箱策略）。
	weight := priorityWeight{
		leastReqWeight:          1, // 默认：分散策略
		mostReqWeight:           0, // 默认：关闭紧凑策略
		nodeAffinityWeight:      2,
		podAffinityWeight:       2,
		balancedResourceWeight:  1,
		taintTolerationWeight:   3,
		imageLocalityWeight:     1,
		podTopologySpreadWeight: 2, // 与 K8s 默认值保持一致
	}

	// 逐个检查配置参数，若提供则覆盖默认值
	args.GetInt(&weight.nodeAffinityWeight, NodeAffinityWeight)
	args.GetInt(&weight.podAffinityWeight, PodAffinityWeight)
	args.GetInt(&weight.leastReqWeight, LeastRequestedWeight)
	args.GetInt(&weight.mostReqWeight, MostRequestedWeight)
	args.GetInt(&weight.balancedResourceWeight, BalancedResourceWeight)
	args.GetInt(&weight.taintTolerationWeight, TaintTolerationWeight)
	args.GetInt(&weight.imageLocalityWeight, ImageLocalityWeight)
	args.GetInt(&weight.podTopologySpreadWeight, PodTopologySpreadWeight)

	return weight
}

// OnSessionOpen 在每个调度会话开始时被调用。
//
// 主要职责：
//  1. 创建 K8s Framework Handle（桥接 Volcano 节点信息到 K8s API）
//  2. 调用 InitPlugin 初始化所有启用的 K8s 原生 ScorePlugin
//  3. 注册 nodeOrderFn — 单节点打分闭包
//  4. 注册 batchNodeOrderFn — 批量节点打分闭包（用于亲和性/拓扑分散等全局视角打分）
//
// 为什么需要两条打分路径？
//   - NodeOrderFn（单节点）：遍历节点时为每个 task-node pair 调用，
//     适合 NodeResourcesFit、NodeAffinity 等逐节点独立的打分。
//   - BatchNodeOrderFn（批量）：一次性处理所有候选节点，
//     适合 InterPodAffinity、PodTopologySpread 等需要全局视角的打分。
//     批量路径通过 PreScore → Score → NormalizeScore 的标准 K8s 打分流水线。
func (pp *NodeOrderPlugin) OnSessionOpen(ssn *framework.Session) {
	// 从 Session 获取节点映射，用于构造 K8s NodeInfo
	nodeMap := ssn.NodeMap

	// 创建 K8s Framework Handle，它是调用 K8s 原生 ScorePlugin 的必要桥梁。
	// 注：这里只注入 ClientSet 和 InformerFactory，不注入完整的 K8s scheduling framework，
	// 因为 Volcano 有自己的调度框架，只需要 K8s 插件所需的 API 访问能力。
	handle := k8s.NewFramework(nodeMap,
		k8s.WithClientSet(ssn.KubeClient()),
		k8s.WithInformerFactory(ssn.InformerFactory()),
	)
	pp.Handle = handle

	// 初始化所有启用的 K8s 原生 ScorePlugin。
	// 仅 weight != 0 的策略会被实例化，weight == 0 则跳过以节省资源。
	pp.InitPlugin()

	// 注册单节点打分函数。
	// allocate/preempt 等 action 为每个 task-node pair 调用此函数。
	ssn.AddNodeOrderFn(pp.Name(), func(task *api.TaskInfo, node *api.NodeInfo) (float64, error) {
		nodeInfo := nodeMap[node.Name]
		state := k8sframework.NewCycleState()
		return pp.NodeOrderFn(task, node, nodeInfo, state)
	})

	// 注册批量节点打分函数。
	// 用于需要全局视角的子插件（InterPodAffinity, TaintToleration, PodTopologySpread）。
	ssn.AddBatchNodeOrderFn(pp.Name(), func(task *api.TaskInfo, nodeInfo []*api.NodeInfo) (map[string]float64, error) {
		state := k8sframework.NewCycleState()
		return pp.BatchNodeOrderFn(task, nodeInfo, state)
	})
}

// InitPlugin 初始化所有启用的 K8s 原生 ScorePlugin。
//
// 按权重门控：
//   - weight != 0 → 创建对应的 K8s ScorePlugin 实例并存入对应 map
//   - weight == 0 → 跳过创建，节省内存和初始化开销
//
// 分类逻辑：
//   - NodeOrderScorePlugins（逐节点）：NodeResourcesFit, BalancedAllocation, NodeAffinity, ImageLocality
//   - ScorePlugins（批量）：InterPodAffinity, TaintToleration, PodTopologySpread
//
// 为什么 InterPodAffinity / TaintToleration / PodTopologySpread 放在 ScorePlugins 而非 NodeOrderScorePlugins？
// 因为这三个插件的打分需要 PreScore 阶段来预处理全局信息（例如扫描所有节点的已有 Pod），
// 不能简单地逐节点独立打分——因此它们走批量路径 BatchNodeOrderFn。
func (pp *NodeOrderPlugin) InitPlugin() {
	scorePlugins := map[string]nodescore.BaseScorePlugin{}
	nodeOrderScorePlugins := map[string]ScorePluginWithWeight{}

	// 读取 K8s FeatureGate 状态，初始化 PodTopologySpread 需要
	fts := feature.Features{
		EnableNodeInclusionPolicyInPodTopologySpread: utilFeature.DefaultFeatureGate.Enabled(features.NodeInclusionPolicyInPodTopologySpread),
		EnableMatchLabelKeysInPodTopologySpread:      utilFeature.DefaultFeatureGate.Enabled(features.MatchLabelKeysInPodTopologySpread),
	}

	// =====================================================================
	// 逐节点打分插件初始化
	// =====================================================================

	// 1. NodeResourcesLeastAllocated —— 分散策略（与 binpack 相反！）
	//    公式：score = (capacity - (used + requested)) / capacity × weight
	//    节点空闲资源越多，得分越高。效果是将负载均匀分散到所有节点。
	if pp.weight.leastReqWeight != 0 {
		leastAllocatedArgs := &config.NodeResourcesFitArgs{
			ScoringStrategy: &config.ScoringStrategy{
				Type:      config.LeastAllocated,
				Resources: []config.ResourceSpec{{Name: "cpu", Weight: 50}, {Name: "memory", Weight: 50}},
			},
		}
		if p, err := noderesources.NewFit(context.TODO(), leastAllocatedArgs, pp.Handle, fts); err == nil {
			leastAllocated := p.(*noderesources.Fit)
			nodeOrderScorePlugins[leastAllocated.Name()+"_LeastAllocated"] = ScorePluginWithWeight{leastAllocated, pp.weight.leastReqWeight}
		} else {
			klog.Errorf("Failed to init Least Allocated plugin %v", err)
		}
	}

	// 2. NodeResourcesMostAllocated —— 紧凑策略（与 binpack 相同！）
	//    公式：score = (used + requested) / capacity × weight
	//    节点已用资源越多，得分越高。效果是将 Pod 塞到最满的节点上，减少碎片。
	if pp.weight.mostReqWeight != 0 {
		mostAllocatedArgs := &config.NodeResourcesFitArgs{
			ScoringStrategy: &config.ScoringStrategy{
				Type:      config.MostAllocated,
				Resources: []config.ResourceSpec{{Name: "cpu", Weight: 1}, {Name: "memory", Weight: 1}},
			},
		}
		if p, err := noderesources.NewFit(context.TODO(), mostAllocatedArgs, pp.Handle, fts); err == nil {
			mostAllocation := p.(*noderesources.Fit)
			nodeOrderScorePlugins[mostAllocation.Name()+"_MostAllocated"] = ScorePluginWithWeight{mostAllocation, pp.weight.mostReqWeight}
		} else {
			klog.Errorf("Failed to init Most Allocated plugin %v", err)
		}
	}

	// 3. NodeResourcesBalancedAllocation —— 资源均衡策略
	//    同时考虑 CPU、内存、GPU 的使用比例，优先选择各维度使用率最接近的节点。
	//    例如节点 A（CPU 80%, Mem 20%）vs 节点 B（CPU 50%, Mem 50%）→ B 得分更高。
	if pp.weight.balancedResourceWeight != 0 {
		blArgs := &config.NodeResourcesBalancedAllocationArgs{
			Resources: []config.ResourceSpec{
				{Name: string(v1.ResourceCPU), Weight: 1},
				{Name: string(v1.ResourceMemory), Weight: 1},
				{Name: "nvidia.com/gpu", Weight: 1},
			},
		}
		if p, err := noderesources.NewBalancedAllocation(context.TODO(), blArgs, pp.Handle, fts); err == nil {
			balancedAllocation := p.(*noderesources.BalancedAllocation)
			nodeOrderScorePlugins[balancedAllocation.Name()] = ScorePluginWithWeight{balancedAllocation, pp.weight.balancedResourceWeight}
		} else {
			klog.Errorf("Failed to init Balanced Resource Allocation plugin %v", err)
		}
	}

	// 4. NodeAffinity —— 节点亲和性偏好
	//    根据 Pod 的 PreferredDuringScheduling 规则为节点打分。
	if pp.weight.nodeAffinityWeight != 0 {
		naArgs := &config.NodeAffinityArgs{
			AddedAffinity: &v1.NodeAffinity{},
		}
		if p, err := nodeaffinity.New(context.TODO(), naArgs, pp.Handle, fts); err == nil {
			nodeAffinity := p.(*nodeaffinity.NodeAffinity)
			nodeOrderScorePlugins[nodeAffinity.Name()] = ScorePluginWithWeight{nodeAffinity, pp.weight.nodeAffinityWeight}
		} else {
			klog.Errorf("Failed to init Node Affinity plugin %v", err)
		}
	}

	// 5. ImageLocality —— 镜像本地性
	//    节点上已有 Pod 所需镜像 → 得分更高 → 减少镜像拉取时间和网络开销。
	if pp.weight.imageLocalityWeight != 0 {
		if p, err := imagelocality.New(context.TODO(), nil, pp.Handle); err == nil {
			imageLocality := p.(*imagelocality.ImageLocality)
			nodeOrderScorePlugins[imageLocality.Name()] = ScorePluginWithWeight{imageLocality, pp.weight.imageLocalityWeight}
		} else {
			klog.Errorf("Failed to init Image Locality plugin %v", err)
		}
	}

	// =====================================================================
	// 批量打分插件初始化（需要 PreScore 预处理）
	// =====================================================================

	// 6. InterPodAffinity —— Pod 间亲和性
	//    需要扫描所有节点上已运行的 Pod 来计算匹配度，因此走批量路径。
	if pp.weight.podAffinityWeight != 0 {
		plArgs := &config.InterPodAffinityArgs{}
		if p, err := interpodaffinity.New(context.TODO(), plArgs, pp.Handle, fts); err == nil {
			interPodAffinity := p.(*interpodaffinity.InterPodAffinity)
			scorePlugins[interpodaffinity.Name] = interPodAffinity
		} else {
			klog.Errorf("Failed to init InterPodAffinity plugin %v", err)
		}
	}

	// 7. TaintToleration —— 污点容忍
	//    需要全局视角来比较 Pod 对每个节点污点的容忍度，因此走批量路径。
	if pp.weight.taintTolerationWeight != 0 {
		if p, err := tainttoleration.New(context.TODO(), nil, pp.Handle, fts); err == nil {
			taintToleration := p.(*tainttoleration.TaintToleration)
			scorePlugins[tainttoleration.Name] = taintToleration
		} else {
			klog.Errorf("Failed to init TaintToleration plugin %v", err)
		}
	}

	// 8. PodTopologySpread —— Pod 拓扑分散
	//    需要全局扫描所有拓扑域内的 Pod 分布来计算 skew，因此走批量路径。
	if pp.weight.podTopologySpreadWeight != 0 {
		ptsArgs := &config.PodTopologySpreadArgs{
			DefaultingType: config.SystemDefaulting,
		}
		if p, err := podtopologyspread.New(context.TODO(), ptsArgs, pp.Handle, fts); err == nil {
			podTopologySpread := p.(*podtopologyspread.PodTopologySpread)
			scorePlugins[podtopologyspread.Name] = podTopologySpread
		} else {
			klog.Errorf("Failed to init PodTopologySpread plugin %v", err)
		}
	}

	pp.NodeOrderScorePlugins = nodeOrderScorePlugins
	pp.ScorePlugins = scorePlugins
}

// NodeOrderFn 执行单节点打分。
//
// 仅使用 NodeOrderScorePlugins 中的逐节点打分插件（LeastAllocated, MostAllocated,
// BalancedAllocation, NodeAffinity, ImageLocality）。
//
// 打分流程：
//
//	for each plugin in NodeOrderScorePlugins:
//	    score = plugin.Score(task.Pod, nodeInfo)  // K8s 原生 Score 方法
//	    nodeScore += score × plugin.weight         // 加权累加
//
// 注意：这里调用的是 K8s 原生的 Score() 方法，而非 Volcano 自己的打分公式。
// 这与 binpack 完全自研的打分逻辑形成对比。
func (pp *NodeOrderPlugin) NodeOrderFn(task *api.TaskInfo, node *api.NodeInfo, k8sNodeInfo fwk.NodeInfo, state *k8sframework.CycleState) (float64, error) {
	var nodeScore = 0.0

	for name, p := range pp.NodeOrderScorePlugins {
		klog.V(5).Infof("Score node through %s plugin", name)

		// 调用 K8s 原生 Score 方法，传入 Pod 和 K8s NodeInfo
		score, status := p.plugin.Score(context.TODO(), state, task.Pod, k8sNodeInfo)
		if !status.IsSuccess() {
			klog.Warningf("Node: %s, <%s> Priority Failed because of Error: %v",
				node.Name, name, status.AsError())
			return 0, status.AsError()
		}

		// K8s 原生插件返回的 score 范围是 [0, MaxNodeScore(100)]
		// 乘以 Volcano 侧配置的 weight 后累加
		nodeScore += float64(score) * float64(p.weight)
		klog.V(5).Infof("Node: %s, task<%s/%s> %s weight %d, score: %f",
			node.Name, task.Namespace, task.Name, name, p.weight, float64(score)*float64(p.weight))
	}

	klog.V(4).Infof("Nodeorder Total Score for task<%s/%s> on node %s is: %f",
		task.Namespace, task.Name, node.Name, nodeScore)
	return nodeScore, nil
}

// BatchNodeOrderFn 执行批量节点打分。
//
// 用于需要全局视角的子插件：InterPodAffinity, TaintToleration, PodTopologySpread。
// 这三个插件必须看到"所有候选节点"以及"各节点上已运行的 Pod"才能计算得分。
//
// 批量打分流程（标准 K8s 打分流水线）：
//
//  1. PreScore  — 预处理全局信息（如扫描所有节点上符合亲和性条件的 Pod）
//  2. Score     — 并行对所有候选节点打分（内部使用 ParallelizeUntil 16 路并发）
//  3. NormalizeScore — 将分数归一化到 [0, 100] 范围
//  4. 加权      — 乘以 Volcano 侧配置的 weight
//
// 与 binpack 的对比：
//   - binpack: 纯 Volcano 算法，O(R×N) 复杂度，自己管理归一化
//   - nodeorder batch: 委托给 K8s 原生插件，各自处理归一化，最终累加
func (pp *NodeOrderPlugin) BatchNodeOrderFn(task *api.TaskInfo, nodeInfo []*api.NodeInfo, state *k8sframework.CycleState) (map[string]float64, error) {
	// 将 Volcano NodeInfo 转换为 K8s NodeInfo（两种类型不同，需要转换）
	nodeInfos := make([]fwk.NodeInfo, 0, len(nodeInfo))
	nodes := make([]*v1.Node, 0, len(nodeInfo))
	for _, node := range nodeInfo {
		newNodeInfo := &k8sframework.NodeInfo{}
		newNodeInfo.SetNode(node.Node)
		nodeInfos = append(nodeInfos, newNodeInfo)
		nodes = append(nodes, node.Node)
	}
	nodeScores := make(map[string]float64, len(nodes))

	// --- InterPodAffinity 打分 ---
	// 扫描所有节点找到符合 PodAffinity/PodAntiAffinity 规则的已有 Pod
	var podAffinityScores map[string]float64
	var err error
	if pp.ScorePlugins[interpodaffinity.Name] != nil {
		podAffinityScores, err = interPodAffinityScore(
			pp.ScorePlugins[interpodaffinity.Name].(*interpodaffinity.InterPodAffinity),
			state, task.Pod, nodeInfos, pp.weight.podAffinityWeight,
		)
		if err != nil {
			return nil, err
		}
	}

	// --- TaintToleration 打分 ---
	// 基于 Pod 的 Tolerations 比较各节点的污点
	var nodeTolerationScores map[string]float64
	if pp.ScorePlugins[tainttoleration.Name] != nil {
		nodeTolerationScores, err = taintTolerationScore(
			pp.ScorePlugins[tainttoleration.Name].(*tainttoleration.TaintToleration),
			state, task.Pod, nodeInfos, pp.weight.taintTolerationWeight,
		)
		if err != nil {
			return nil, err
		}
	}

	// --- PodTopologySpread 打分 ---
	// 基于 PodTopologySpread 约束，计算每个节点的拓扑域 skew
	var podTopologySpreadScores map[string]float64
	if pp.ScorePlugins[podtopologyspread.Name] != nil {
		podTopologySpreadScores, err = podTopologySpreadScore(
			pp.ScorePlugins[podtopologyspread.Name].(*podtopologyspread.PodTopologySpread),
			state, task.Pod, nodeInfos, pp.weight.podTopologySpreadWeight,
		)
		if err != nil {
			return nil, err
		}
	}

	// 合并三个批量子插件的得分
	for _, node := range nodes {
		score := 0.0
		if podAffinityScores != nil {
			score += podAffinityScores[node.Name]
		}
		if nodeTolerationScores != nil {
			score += nodeTolerationScores[node.Name]
		}
		if podTopologySpreadScores != nil {
			score += podTopologySpreadScores[node.Name]
		}
		nodeScores[node.Name] = score
	}

	klog.V(4).Infof("Batch Total Score for task %s/%s is: %v",
		task.Namespace, task.Name, nodeScores)
	return nodeScores, nil
}

// interPodAffinityScore 计算 InterPodAffinity 的批量打分。
//
// 通过 nodescore.CalculatePluginScore 执行标准的 PreScore → Score → NormalizeScore 流水线。
func interPodAffinityScore(
	interPodAffinity *interpodaffinity.InterPodAffinity,
	state fwk.CycleState,
	pod *v1.Pod,
	nodeInfos []fwk.NodeInfo,
	podAffinityWeight int,
) (map[string]float64, error) {
	return nodescore.CalculatePluginScore(interPodAffinity.Name(), interPodAffinity, interPodAffinity,
		state, pod, nodeInfos, podAffinityWeight)
}

// taintTolerationScore 计算 TaintToleration 的批量打分。
func taintTolerationScore(
	taintToleration *tainttoleration.TaintToleration,
	cycleState fwk.CycleState,
	pod *v1.Pod,
	nodeInfos []fwk.NodeInfo,
	taintTolerationWeight int,
) (map[string]float64, error) {
	return nodescore.CalculatePluginScore(taintToleration.Name(), taintToleration, taintToleration,
		cycleState, pod, nodeInfos, taintTolerationWeight)
}

// podTopologySpreadScore 计算 PodTopologySpread 的批量打分。
func podTopologySpreadScore(
	podTopologySpread *podtopologyspread.PodTopologySpread,
	cycleState fwk.CycleState,
	pod *v1.Pod,
	nodeInfos []fwk.NodeInfo,
	podTopologySpreadWeight int,
) (map[string]float64, error) {
	return nodescore.CalculatePluginScore(podTopologySpread.Name(), podTopologySpread, podTopologySpread,
		cycleState, pod, nodeInfos, podTopologySpreadWeight)
}

// OnSessionClose 在调度会话结束时调用，当前为空实现。
func (pp *NodeOrderPlugin) OnSessionClose(ssn *framework.Session) {
}
