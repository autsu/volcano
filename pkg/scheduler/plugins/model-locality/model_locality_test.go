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

package modellocality

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"

	schedulingv1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/actions/allocate"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins/gang"
	"volcano.sh/volcano/pkg/scheduler/plugins/predicates"
	"volcano.sh/volcano/pkg/scheduler/plugins/priority"
	"volcano.sh/volcano/pkg/scheduler/plugins/proportion"
	"volcano.sh/volcano/pkg/scheduler/uthelper"
	"volcano.sh/volcano/pkg/scheduler/util"
)

// =============================================================================
// 集成测试 — 使用 uthelper 框架测试完整 allocate 调度流程
// =============================================================================
//
// 这些测试不走单个函数的 mock，而是真实创建 Session，注册插件，运行 allocate action，
// 然后通过 uthelper 的 CheckAll 验证 Pod 是否被绑定到预期节点。
//
// 对比单元测试（只测 predicate/nodeOrder 函数）：
//   单元测试：给定 placement map，验证返回值
//   集成测试：给定 Node/Pod/PodGroup，运行完整调度 → 验证最终 Bind 结果
//   → 集成测试覆盖了 Plugin → Session → Action 的完整链路

// TestFullFlowModelCache 测试：模型缓存命中节点优先被选择。
//
// 场景：
//   - node-a 缓存了 llama-7b，node-b 没有
//   - 只有一个 Pod，请求 llama-7b 模型
//   - 期望 Pod 因为 NodeOrder 加分而调度到 node-a
func TestFullFlowModelCache(t *testing.T) {
	plugins := map[string]framework.PluginBuilder{
		PluginName:              New,
		gang.PluginName:         gang.New,
		predicates.PluginName:   predicates.New,
		priority.PluginName:     priority.New,
		proportion.PluginName:   proportion.New,
	}

	n1 := buildNodeWithCache("node-a", "", "llama-7b,qwen-7b")
	n2 := buildNodeWithCache("node-b", "", "qwen-7b")
	p0 := podWithAnnos("p0", "", "", "", "llama-7b")

	test := uthelper.TestCommonStruct{
		Name:    "model-cache-hits-cached-node",
		Plugins: plugins,
		Pods:    []*corev1.Pod{p0},
		Nodes:   []*corev1.Node{n1, n2},
		PodGroups: []*schedulingv1.PodGroup{
			util.BuildPodGroup("pg1", "c1", "c1", 1, nil, schedulingv1.PodGroupInqueue),
		},
		Queues:         []*schedulingv1.Queue{util.BuildQueue("c1", 1, nil)},
		ExpectBindsNum: 1,
		ExpectBindMap: map[string]string{
			"c1/p0": "node-a", // Pod 应该落到有模型缓存的节点
		},
	}

	test.RegisterSession(fullFlowTiers(), nil)
	defer test.Close()
	test.Run([]framework.Action{allocate.New()})
	if err := test.CheckAll(0); err != nil {
		t.Fatal(err)
	}
}

// TestFullFlowColdStart 测试：即使没有模型缓存，Pod 仍能调度。
//
// 场景：
//   - 集群没有任何节点缓存 llama-7b
//   - Pod 请求 llama-7b，但仍应调度成功
//   - 验证模型缓存是软约束（NodeOrder），不是硬约束（Predicate）
func TestFullFlowColdStart(t *testing.T) {
	plugins := map[string]framework.PluginBuilder{
		PluginName:              New,
		gang.PluginName:         gang.New,
		predicates.PluginName:   predicates.New,
		priority.PluginName:     priority.New,
		proportion.PluginName:   proportion.New,
	}

	n1 := buildNodeWithCache("node-a", "", "other-model")
	n2 := buildNodeWithCache("node-b", "", "other-model")
	p0 := podWithAnnos("p0", "", "", "", "llama-7b")

	test := uthelper.TestCommonStruct{
		Name:    "cold-start-no-cache-still-works",
		Plugins: plugins,
		Pods:    []*corev1.Pod{p0},
		Nodes:   []*corev1.Node{n1, n2},
		PodGroups: []*schedulingv1.PodGroup{
			util.BuildPodGroup("pg1", "c1", "c1", 1, nil, schedulingv1.PodGroupInqueue),
		},
		Queues:           []*schedulingv1.Queue{util.BuildQueue("c1", 1, nil)},
		ExpectBindsNum:   1,
		MinimalBindCheck: true, // 不关心具体去哪个节点，只验证能调度成功
	}

	test.RegisterSession(fullFlowTiers(), nil)
	defer test.Close()
	test.Run([]framework.Action{allocate.New()})
	if err := test.CheckAll(0); err != nil {
		t.Fatal(err)
	}
	t.Log("cold-start PASS: pod scheduled even without model cache")
}

// TestFullFlowSameNVLink 测试：same-nvlink 硬约束在完整流程中生效。
//
// 场景：
//   - node-a(nvlink-a), node-b(nvlink-a), node-c(nvlink-b)，三个节点资源相同
//   - instance=llama-a 的两个 Pod，topology=same-nvlink
//   - 第一个 Pod 会落到 nvlink-a 域的某个节点
//   - 第二个 Pod 必须也落在 nvlink-a 域，即使 node-c 资源充足也不能用
//
// 验证方式：两个 Pod 都成功绑定（ExpectBindsNum=2），然后用 GetBinds 检查
// 是否都落在 nvlink-a 域。
func TestFullFlowSameNVLink(t *testing.T) {
	plugins := map[string]framework.PluginBuilder{
		PluginName:              New,
		gang.PluginName:         gang.New,
		predicates.PluginName:   predicates.New,
		priority.PluginName:     priority.New,
		proportion.PluginName:   proportion.New,
	}

	n1 := buildNodeWithNVLink("node-a", "nvlink-a")
	n2 := buildNodeWithNVLink("node-b", "nvlink-a")
	n3 := buildNodeWithNVLink("node-c", "nvlink-b")
	p0 := podWithAnnos("p0", "", "llama-a", SameNVLink, "")
	p1 := podWithAnnos("p1", "", "llama-a", SameNVLink, "")

	test := uthelper.TestCommonStruct{
		Name:    "same-nvlink-constraint-full-flow",
		Plugins: plugins,
		Pods:    []*corev1.Pod{p0, p1},
		Nodes:   []*corev1.Node{n1, n2, n3},
		PodGroups: []*schedulingv1.PodGroup{
			util.BuildPodGroup("pg1", "c1", "c1", 2, nil, schedulingv1.PodGroupInqueue),
		},
		Queues:         []*schedulingv1.Queue{util.BuildQueue("c1", 1, nil)},
		ExpectBindsNum: 2,
	}

	test.RegisterSession(fullFlowTiers(), nil)
	defer test.Close()
	test.Run([]framework.Action{allocate.New()})

	// 三个节点资源相同。p0 先调度，因为没有 placement 锚点，三节点都过 Predicate，
	// NodeOrder 无缓存命中无加分，节点遍历稳定排在第一个的 node-a 被选中。
	// p0 分配后 AllocateFunc 记录 placement={node-a, nvlink-a}。
	// p1 调度时：node-a/node-b 过 Predicate（同 nvlink-a），node-c 被过滤（nvlink-b）。
	// node-a 有 same-topology bonus（同节点），得分高于 node-b → p1 也去 node-a。
	test.ExpectBindMap = map[string]string{
		"c1/p0": "node-a",
		"c1/p1": "node-a",
	}
	if err := test.CheckAll(0); err != nil {
		t.Fatal(err)
	}
}

// TestFullFlowSameNode 测试：same-node 硬约束在完整流程中生效。
//
// 场景：
//   - node-a 和 node-b 资源相同，node-a 多声明了模型缓存（引导第一个 Pod 过去）
//   - instance=llama-a 的两个 Pod，topology=same-node
//   - 第一个 Pod 因缓存命中调度到 node-a
//   - 第二个 Pod 必须也落在 node-a，即使 node-b 资源充足也不能用
func TestFullFlowSameNode(t *testing.T) {
	plugins := map[string]framework.PluginBuilder{
		PluginName:              New,
		gang.PluginName:         gang.New,
		predicates.PluginName:   predicates.New,
		priority.PluginName:     priority.New,
		proportion.PluginName:   proportion.New,
	}

	// node-a 有模型缓存（引导 p0），node-b 没有
	n1 := buildNodeWithCache("node-a", "", "llama-7b")
	n2 := buildNodeWithCache("node-b", "", "other")
	p0 := podWithAnnos("p0", "", "llama-a", SameNode, "llama-7b")
	p1 := podWithAnnos("p1", "", "llama-a", SameNode, "")

	test := uthelper.TestCommonStruct{
		Name:    "same-node-constraint-full-flow",
		Plugins: plugins,
		Pods:    []*corev1.Pod{p0, p1},
		Nodes:   []*corev1.Node{n1, n2},
		PodGroups: []*schedulingv1.PodGroup{
			util.BuildPodGroup("pg1", "c1", "c1", 2, nil, schedulingv1.PodGroupInqueue),
		},
		Queues:         []*schedulingv1.Queue{util.BuildQueue("c1", 1, nil)},
		ExpectBindsNum: 2,
		ExpectBindMap: map[string]string{
			"c1/p0": "node-a", // p0 因模型缓存命中调度到 node-a
			"c1/p1": "node-a", // p1 因 same-node 硬约束也必须到 node-a
		},
	}

	test.RegisterSession(fullFlowTiers(), nil)
	defer test.Close()
	test.Run([]framework.Action{allocate.New()})
	if err := test.CheckAll(0); err != nil {
		t.Fatal(err)
	}
}

// TestFullFlowNoInstanceAnnotation 测试：没有 instance annotation 的 Pod 不受本插件影响。
//
// 场景：
//   - 一个普通 Pod（无 model-locality annotations）
//   - 应该正常调度，不被本插件的 Predicate 过滤掉
func TestFullFlowNoInstanceAnnotation(t *testing.T) {
	plugins := map[string]framework.PluginBuilder{
		PluginName:              New,
		gang.PluginName:         gang.New,
		predicates.PluginName:   predicates.New,
		priority.PluginName:     priority.New,
		proportion.PluginName:   proportion.New,
	}

	n1 := util.BuildNode("node-a", api.BuildResourceList("4", "8Gi",
		[]api.ScalarResource{{Name: "pods", Value: "10"}}...), make(map[string]string))
	p0 := util.BuildPod("c1", "p0", "", corev1.PodPending,
		api.BuildResourceList("1", "1Gi"), "pg1",
		make(map[string]string), make(map[string]string))

	test := uthelper.TestCommonStruct{
		Name:    "no-annotation-passes-through",
		Plugins: plugins,
		Pods:    []*corev1.Pod{p0},
		Nodes:   []*corev1.Node{n1},
		PodGroups: []*schedulingv1.PodGroup{
			util.BuildPodGroup("pg1", "c1", "c1", 1, nil, schedulingv1.PodGroupInqueue),
		},
		Queues:         []*schedulingv1.Queue{util.BuildQueue("c1", 1, nil)},
		ExpectBindsNum: 1,
		ExpectBindMap: map[string]string{
			"c1/p0": "node-a",
		},
	}

	test.RegisterSession(fullFlowTiers(), nil)
	defer test.Close()
	test.Run([]framework.Action{allocate.New()})
	if err := test.CheckAll(0); err != nil {
		t.Fatal(err)
	}
}

// =============================================================================
// 单元测试 — 测试单个函数的正确性
// =============================================================================

func TestPredicateSameNode(t *testing.T) {
	plugin := New(framework.Arguments{}).(*modelLocalityPlugin)
	placements := map[string]*instancePlacement{
		"llama-a": {nodes: map[string]struct{}{"node-a": {}}},
	}
	task := taskInfo("pending", "llama-a", SameNode, "llama-7b", "")

	if err := plugin.predicate(task, nodeInfo("node-a", "", ""), placements); err != nil {
		t.Fatalf("same node should pass: %v", err)
	}
	if err := plugin.predicate(task, nodeInfo("node-b", "", ""), placements); err == nil {
		t.Fatalf("different node should be rejected")
	}
}

func TestPredicateSameNVLink(t *testing.T) {
	plugin := New(framework.Arguments{}).(*modelLocalityPlugin)
	placements := map[string]*instancePlacement{
		"llama-a": {
			nodes:        map[string]struct{}{"node-a": {}},
			nvlinkDomain: "nvlink-a",
		},
	}
	task := taskInfo("pending", "llama-a", SameNVLink, "llama-7b", "")

	if err := plugin.predicate(task, nodeInfo("node-b", "nvlink-a", ""), placements); err != nil {
		t.Fatalf("same NVLink domain should pass: %v", err)
	}
	if err := plugin.predicate(task, nodeInfo("node-c", "nvlink-b", ""), placements); err == nil {
		t.Fatalf("different NVLink domain should be rejected")
	}
}

func TestNodeOrderPrefersCachedModel(t *testing.T) {
	plugin := New(framework.Arguments{
		cacheHitWeightArg: 2, sameTopologyBonusWeightArg: 0,
	}).(*modelLocalityPlugin)
	task := taskInfo("pending", "llama-a", SameNVLink, "llama-7b", "")

	cachedScore := plugin.nodeOrder(task, nodeInfo("node-a", "nvlink-a", "llama-7b,qwen-7b"), nil)
	coldScore := plugin.nodeOrder(task, nodeInfo("node-b", "nvlink-a", "qwen-7b"), nil)

	if cachedScore <= coldScore {
		t.Fatalf("cached node should score higher, cached=%v cold=%v", cachedScore, coldScore)
	}
}

func TestNodeOrderSameTopologyBonus(t *testing.T) {
	plugin := New(framework.Arguments{
		cacheHitWeightArg: 0, sameTopologyBonusWeightArg: 3,
	}).(*modelLocalityPlugin)

	placements := map[string]*instancePlacement{
		"llama-a": {
			nodes: map[string]struct{}{"node-a": {}}, nvlinkDomain: "nvlink-a",
		},
	}
	task := taskInfo("pending", "llama-a", "", "", "")

	sameScore := plugin.nodeOrder(task, nodeInfo("node-b", "nvlink-a", ""), placements)
	diffScore := plugin.nodeOrder(task, nodeInfo("node-c", "nvlink-b", ""), placements)

	if sameScore <= diffScore {
		t.Fatalf("same NVLink domain should score higher, same=%v diff=%v", sameScore, diffScore)
	}
}

func TestPredicateFirstPodAlwaysPasses(t *testing.T) {
	plugin := New(framework.Arguments{}).(*modelLocalityPlugin)
	// placement 为空 → 第一个 Pod 总是通过
	placements := map[string]*instancePlacement{}
	task := taskInfo("pending", "llama-a", SameNode, "llama-7b", "")

	if err := plugin.predicate(task, nodeInfo("node-any", "", ""), placements); err != nil {
		t.Fatalf("first pod of an instance should always pass: %v", err)
	}
}

// =============================================================================
// 测试辅助函数
// =============================================================================

func taskInfo(name, instance, topology, model, nodeName string) *api.TaskInfo {
	annos := make(map[string]string)
	if instance != "" {
		annos[DefaultInstanceAnnotation] = instance
	}
	if topology != "" {
		annos[DefaultTopologyAnnotation] = topology
	}
	if model != "" {
		annos[DefaultModelAnnotation] = model
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID: uuid.NewUUID(), Name: name, Namespace: "default",
			Annotations: annos,
		},
	}
	task := api.NewTaskInfo(pod)
	task.NodeName = nodeName
	return task
}

func nodeInfo(name, nvlinkDomain, cachedModels string) *api.NodeInfo {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels:      map[string]string{DefaultNVLinkLabel: nvlinkDomain},
			Annotations: map[string]string{DefaultModelCacheAnnotation: cachedModels},
		},
	}
	return api.NewNodeInfo(node)
}

func podWithAnnos(name, nodeName, instance, topology, model string) *corev1.Pod {
	pod := util.BuildPod("c1", name, nodeName, corev1.PodPending,
		api.BuildResourceList("1", "1Gi"), "pg1",
		map[string]string{}, map[string]string{})
	// BuildPod 把 "labels" 和 "selector" 作为参数，不会把 annotations 写入 Pod.Annotations。
	// 模型本地性插件从 task.Pod.Annotations 读配置，所以需要构建后追加。
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	if instance != "" {
		pod.Annotations[DefaultInstanceAnnotation] = instance
	}
	if topology != "" {
		pod.Annotations[DefaultTopologyAnnotation] = topology
	}
	if model != "" {
		pod.Annotations[DefaultModelAnnotation] = model
	}
	return pod
}

func buildNodeWithNVLink(name, nvlinkDomain string) *corev1.Node {
	return util.BuildNode(name, api.BuildResourceList("4", "8Gi",
		[]api.ScalarResource{{Name: "pods", Value: "10"}}...),
		map[string]string{DefaultNVLinkLabel: nvlinkDomain})
}

func buildNodeWithCache(name, nvlinkDomain, cachedModels string) *corev1.Node {
	node := util.BuildNode(name, api.BuildResourceList("4", "8Gi",
		[]api.ScalarResource{{Name: "pods", Value: "10"}}...), make(map[string]string))
	if nvlinkDomain != "" {
		node.Labels[DefaultNVLinkLabel] = nvlinkDomain
	}
	if cachedModels != "" {
		if node.Annotations == nil {
			node.Annotations = make(map[string]string)
		}
		node.Annotations[DefaultModelCacheAnnotation] = cachedModels
	}
	return node
}

func fullFlowTiers() []conf.Tier {
	trueValue := true
	return []conf.Tier{{
		Plugins: []conf.PluginOption{
			{
				Name:                gang.PluginName,
				EnabledJobReady:     &trueValue,
				EnabledJobOrder:     &trueValue,
				EnabledJobPipelined: &trueValue,
				EnabledJobStarving:  &trueValue,
			},
			{
				Name:             predicates.PluginName,
				EnabledPredicate: &trueValue,
			},
			{
				Name:             PluginName,
				EnabledPredicate: &trueValue,
				EnabledNodeOrder: &trueValue,
			},
			{
				Name:             priority.PluginName,
				EnabledJobOrder:  &trueValue,
				EnabledTaskOrder: &trueValue,
			},
			{
				Name:               proportion.PluginName,
				EnabledQueueOrder:  &trueValue,
				EnabledAllocatable: &trueValue,
			},
		},
	}}
}

