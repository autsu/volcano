/*
Copyright 2025 The Kubernetes Authors.

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

package nodeorder

import (
	"os"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8sframework "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/imagelocality"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/interpodaffinity"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodeaffinity"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/noderesources"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/podtopologyspread"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/tainttoleration"
	k8smetrics "k8s.io/kubernetes/pkg/scheduler/metrics"

	schedulingv1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/cmd/scheduler/app/options"
	"volcano.sh/volcano/pkg/scheduler/actions/allocate"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins/gang"
	"volcano.sh/volcano/pkg/scheduler/plugins/util/k8s"
	"volcano.sh/volcano/pkg/scheduler/uthelper"
	"volcano.sh/volcano/pkg/scheduler/util"
)

// TestMain 是测试入口，初始化调度器选项和 K8s 指标注册。
func TestMain(m *testing.M) {
	options.Default()
	k8smetrics.Register()
	os.Exit(m.Run())
}

// nodeOrderTestCase 封装了 nodeorder 插件的集成测试用例。
// 每个测试用例通过设置不同的权重来验证特定子策略的行为。
type nodeOrderTestCase struct {
	uthelper.TestCommonStruct                                          // 嵌入通用测试结构（Nodes, Pods, Queues 等）
	LeastRequestedWeight          int                                   // 分散策略权重
	MostRequestedWeight           int                                   // 紧凑策略权重
	BalancedResourceWeight        int                                   // 均衡策略权重
	NodeAffinityWeight            int                                   // 节点亲和性权重
	TaintTolerationWeight         int                                   // 污点容忍权重
	PodAffinityWeight             int                                   // Pod 亲和性权重
	PodTopologySpreadWeight       int                                   // 拓扑分散权重
	ImageLocalityWeight           int                                   // 镜像本地性权重
}

// TestNodeOrderPlugin 对 nodeorder 的 7 种核心子策略进行端到端集成测试。
//
// 测试方法：每种策略只启用一个高权重，验证 Pod 被调度到预期的节点。
//
// 测试覆盖的子策略：
//  1. LeastAllocated（分散）：n1(2c/4G) vs n2(4c/8G)，Pod(1c/1G) → n2（更空闲）
//  2. MostAllocated（紧凑）：n1(2c/4G) vs n2(4c/8G)，Pod(1c/1G) → n1（更紧凑）
//  3. BalancedAllocation（均衡）：n1(2c/2G 均衡) vs n2(4c/2G 不均衡) → n1
//  4. NodeAffinity（亲和性）：n1(label zone=zone1) vs n2(zone=zone2) → n1
//  5. TaintToleration（污点）：n1(无污点) vs n2(有 PreferNoSchedule) → n1
//  6. InterPodAffinity（Pod 亲和）：p1 在 n1 上，p2 亲和 p1 → p2 调 n1
//  7. PodTopologySpread（拓扑分散）：zone 维度 2:1:0 → 优先 zone3
func TestNodeOrderPlugin(t *testing.T) {
	// 所有测试用例都需要 gang 和 nodeorder 两个插件
	plugins := map[string]framework.PluginBuilder{
		PluginName:      New,
		gang.PluginName: gang.New,
	}

	tests := []nodeOrderTestCase{
		// ================================================================
		// 测试 1：LeastAllocated 分散策略（与 binpack 相反）
		// ================================================================
		// 场景：两个节点容量不同，n2 更空闲
		//   n1: 2 CPU, 4Gi 内存
		//   n2: 4 CPU, 8Gi 内存
		//   Pod 请求: 1 CPU, 1Gi 内存
		// 期望：n2 — 因为 n2 有更多空闲资源，分散策略给 n2 更高分
		{
			TestCommonStruct: uthelper.TestCommonStruct{
				Name: "leastAllocated strategy",
				PodGroups: []*schedulingv1.PodGroup{
					util.BuildPodGroup("pg1", "c1", "c1", 0, nil, schedulingv1.PodGroupInqueue),
				},
				Pods: []*v1.Pod{
					util.BuildPod("c1", "p1", "", v1.PodPending,
						api.BuildResourceList("1", "1G"), "pg1",
						make(map[string]string), make(map[string]string)),
				},
				Nodes: []*v1.Node{
					util.BuildNode("n1", api.BuildResourceList("2", "4Gi",
						[]api.ScalarResource{{Name: "pods", Value: "10"}}...), make(map[string]string)),
					util.BuildNode("n2", api.BuildResourceList("4", "8Gi",
						[]api.ScalarResource{{Name: "pods", Value: "10"}}...), make(map[string]string)),
				},
				Queues: []*schedulingv1.Queue{
					util.BuildQueue("c1", 1, nil),
				},
				ExpectBindsNum: 1,
				ExpectBindMap: map[string]string{
					"c1/p1": "n2", // 分散策略：空闲更多的节点得分更高
				},
			},
			LeastRequestedWeight: 1,
			MostRequestedWeight:  0,
		},
		// ================================================================
		// 测试 2：MostAllocated 紧凑策略（与 binpack 相同）
		// ================================================================
		// 场景：与测试 1 相同的节点和 Pod
		// 期望：n1 — 因为 n1 资源更紧凑（利用率更高），紧凑策略给 n1 更高分
		{
			TestCommonStruct: uthelper.TestCommonStruct{
				Name: "mostAllocated strategy",
				PodGroups: []*schedulingv1.PodGroup{
					util.BuildPodGroup("pg1", "c1", "c1", 0, nil, schedulingv1.PodGroupInqueue),
				},
				Pods: []*v1.Pod{
					util.BuildPod("c1", "p1", "", v1.PodPending,
						api.BuildResourceList("1", "1G"), "pg1",
						make(map[string]string), make(map[string]string)),
				},
				Nodes: []*v1.Node{
					util.BuildNode("n1", api.BuildResourceList("2", "4Gi",
						[]api.ScalarResource{{Name: "pods", Value: "10"}}...), make(map[string]string)),
					util.BuildNode("n2", api.BuildResourceList("4", "8Gi",
						[]api.ScalarResource{{Name: "pods", Value: "10"}}...), make(map[string]string)),
				},
				Queues: []*schedulingv1.Queue{
					util.BuildQueue("c1", 1, nil),
				},
				ExpectBindsNum: 1,
				ExpectBindMap: map[string]string{
					"c1/p1": "n1", // 紧凑策略：利用率更高的节点得分更高
				},
			},
			LeastRequestedWeight: 0,
			MostRequestedWeight:  1,
		},
		// ================================================================
		// 测试 3：BalancedAllocation 资源均衡策略
		// ================================================================
		// 场景：
		//   n1: 2 CPU, 2Gi 内存（均衡，CPU:Mem 比例 = 1:1）
		//   n2: 4 CPU, 2Gi 内存（不均衡，CPU 过剩）
		//   Pod 请求: 1 CPU, 1Gi 内存
		// 期望：n1 — 调度后 n1 的 CPU/Mem 使用比例仍然均衡，
		//   而 n2 本来就 CPU 过剩，再加 1 CPU 会加剧不均衡
		{
			TestCommonStruct: uthelper.TestCommonStruct{
				Name: "balanced allocation prefers balanced node",
				PodGroups: []*schedulingv1.PodGroup{
					util.BuildPodGroup("pg1", "c1", "c1", 0, nil, schedulingv1.PodGroupInqueue),
				},
				Pods: []*v1.Pod{
					util.BuildPod("c1", "p1", "", v1.PodPending,
						api.BuildResourceList("1", "1Gi"), "pg1",
						make(map[string]string), make(map[string]string)),
				},
				Nodes: []*v1.Node{
					// n1: 2 CPU, 2Gi 内存 — CPU/Mem 比例 1:1，均衡
					util.BuildNode("n1", api.BuildResourceList("2", "2Gi",
						[]api.ScalarResource{{Name: "pods", Value: "10"}}...), make(map[string]string)),
					// n2: 4 CPU, 2Gi 内存 — CPU 过剩，不均衡
					util.BuildNode("n2", api.BuildResourceList("4", "2Gi",
						[]api.ScalarResource{{Name: "pods", Value: "10"}}...), make(map[string]string)),
				},
				Queues: []*schedulingv1.Queue{
					util.BuildQueue("c1", 1, nil),
				},
				ExpectBindsNum: 1,
				ExpectBindMap: map[string]string{
					"c1/p1": "n1", // 均衡节点得分更高
				},
			},
			BalancedResourceWeight: 10, // 高权重确保均衡策略主导决策
		},
		// ================================================================
		// 测试 4：NodeAffinity 节点亲和性
		// ================================================================
		// 场景：
		//   n1: label zone=zone1
		//   n2: label zone=zone2
		//   Pod 的 PreferredDuringScheduling 规则：偏好 zone=zone1
		// 期望：n1 — 匹配 Pod 的亲和性偏好
		{
			TestCommonStruct: uthelper.TestCommonStruct{
				Name: "node affinity prefers labeled node",
				PodGroups: []*schedulingv1.PodGroup{
					util.BuildPodGroup("pg1", "c1", "c1", 0, nil, schedulingv1.PodGroupInqueue),
				},
				Pods: []*v1.Pod{
					util.BuildPodWithAffinity("c1", "p1", "", v1.PodPending,
						api.BuildResourceList("1", "1G"), "pg1",
						make(map[string]string), make(map[string]string),
						&v1.Affinity{
							NodeAffinity: &v1.NodeAffinity{
								PreferredDuringSchedulingIgnoredDuringExecution: []v1.PreferredSchedulingTerm{
									{
										Weight: 100,
										Preference: v1.NodeSelectorTerm{
											MatchExpressions: []v1.NodeSelectorRequirement{
												{
													Key:      "zone",
													Operator: v1.NodeSelectorOpIn,
													Values:   []string{"zone1"},
												},
											},
										},
									},
								},
							},
						}),
				},
				Nodes: []*v1.Node{
					util.BuildNode("n1", api.BuildResourceList("2", "4Gi",
						[]api.ScalarResource{{Name: "pods", Value: "10"}}...), map[string]string{"zone": "zone1"}),
					util.BuildNode("n2", api.BuildResourceList("2", "4Gi",
						[]api.ScalarResource{{Name: "pods", Value: "10"}}...), map[string]string{"zone": "zone2"}),
				},
				Queues: []*schedulingv1.Queue{
					util.BuildQueue("c1", 1, nil),
				},
				ExpectBindsNum: 1,
				ExpectBindMap: map[string]string{
					"c1/p1": "n1", // n1 匹配亲和性偏好 zone=zone1
				},
			},
			NodeAffinityWeight: 10,
		},
		// ================================================================
		// 测试 5：TaintToleration 污点容忍
		// ================================================================
		// 场景：
		//   n1: 无污点
		//   n2: 有 PreferNoSchedule 污点
		//   Pod: 没有配置 Tolerations
		// 期望：n1 — 没有污点的节点得分更高
		{
			TestCommonStruct: uthelper.TestCommonStruct{
				Name: "taint toleration prefers non-tainted node",
				PodGroups: []*schedulingv1.PodGroup{
					util.BuildPodGroup("pg1", "c1", "c1", 0, nil, schedulingv1.PodGroupInqueue),
				},
				Pods: []*v1.Pod{
					// Pod 没有配置任何 Tolerations
					util.BuildPodWithTolerations("c1", "p1", "", v1.PodPending,
						api.BuildResourceList("1", "1G"), "pg1",
						make(map[string]string), make(map[string]string), nil),
				},
				Nodes: []*v1.Node{
					// n1: 干净节点，无污点
					util.BuildNode("n1", api.BuildResourceList("2", "4Gi",
						[]api.ScalarResource{{Name: "pods", Value: "10"}}...), make(map[string]string)),
					// n2: 带有 PreferNoSchedule 污点
					{
						ObjectMeta: util.BuildNode("n2", api.BuildResourceList("2", "4Gi",
							[]api.ScalarResource{{Name: "pods", Value: "10"}}...), make(map[string]string)).ObjectMeta,
						Spec: v1.NodeSpec{
							Taints: []v1.Taint{
								{
									Key:    "key1",
									Value:  "value1",
									Effect: v1.TaintEffectPreferNoSchedule,
								},
							},
						},
						Status: util.BuildNode("n2", api.BuildResourceList("2", "4Gi",
							[]api.ScalarResource{{Name: "pods", Value: "10"}}...), make(map[string]string)).Status,
					},
				},
				Queues: []*schedulingv1.Queue{
					util.BuildQueue("c1", 1, nil),
				},
				ExpectBindsNum: 1,
				ExpectBindMap: map[string]string{
					"c1/p1": "n1", // 无污点节点得分更高
				},
			},
			TaintTolerationWeight: 10,
		},
		// ================================================================
		// 测试 6：InterPodAffinity Pod 间亲和性
		// ================================================================
		// 场景：
		//   n1: 已运行 p1（label service=s1）
		//   n3: 已运行 p-noise（label service=s2 — 不相关的 Pod）
		//   p2: 偏好与 label service=s1 的 Pod 在同一节点
		// 期望：n1 — n1 上有匹配 service=s1 的 p1
		{
			TestCommonStruct: uthelper.TestCommonStruct{
				Name: "inter-pod affinity prefers node with matching pod",
				PodGroups: []*schedulingv1.PodGroup{
					util.BuildPodGroup("pg1", "c1", "c1", 0, nil, schedulingv1.PodGroupInqueue),
				},
				Pods: []*v1.Pod{
					// 已在 n1 运行的 Pod，标签 service=s1
					util.BuildPod("c1", "p1", "n1", v1.PodRunning,
						api.BuildResourceList("1", "1G"), "pg1",
						map[string]string{"service": "s1"}, make(map[string]string)),
					// 噪音 Pod 在 n3，标签 service=s2（不应吸引 p2）
					util.BuildPod("c1", "p-noise", "n3", v1.PodRunning,
						api.BuildResourceList("1", "1G"), "pg1",
						map[string]string{"service": "s2"}, make(map[string]string)),
					// 待调度 Pod，偏好与 service=s1 的 Pod 共置
					util.BuildPodWithAffinity("c1", "p2", "", v1.PodPending,
						api.BuildResourceList("1", "1G"), "pg1",
						make(map[string]string), make(map[string]string),
						&v1.Affinity{
							PodAffinity: &v1.PodAffinity{
								PreferredDuringSchedulingIgnoredDuringExecution: []v1.WeightedPodAffinityTerm{
									{
										Weight: 100,
										PodAffinityTerm: v1.PodAffinityTerm{
											LabelSelector: &metav1.LabelSelector{
												MatchLabels: map[string]string{"service": "s1"},
											},
											TopologyKey: "kubernetes.io/hostname",
										},
									},
								},
							},
						}),
				},
				Nodes: []*v1.Node{
					util.BuildNode("n1", api.BuildResourceList("2", "4Gi",
						[]api.ScalarResource{{Name: "pods", Value: "10"}}...), map[string]string{"kubernetes.io/hostname": "n1"}),
					util.BuildNode("n2", api.BuildResourceList("2", "4Gi",
						[]api.ScalarResource{{Name: "pods", Value: "10"}}...), map[string]string{"kubernetes.io/hostname": "n2"}),
					util.BuildNode("n3", api.BuildResourceList("2", "4Gi",
						[]api.ScalarResource{{Name: "pods", Value: "10"}}...), map[string]string{"kubernetes.io/hostname": "n3"}),
				},
				Queues: []*schedulingv1.Queue{
					util.BuildQueue("c1", 1, nil),
				},
				ExpectBindsNum: 1,
				ExpectBindMap: map[string]string{
					"c1/p2": "n1", // n1 上有匹配 service=s1 的 p1
				},
			},
			PodAffinityWeight: 10,
		},
		// ================================================================
		// 测试 7：PodTopologySpread 拓扑分散
		// ================================================================
		// 场景：按 zone 拓扑域分散 app=foo 的 Pod
		//   zone1 (n1): 已运行 2 个 Pod (p1, p2)
		//   zone2 (n2): 已运行 1 个 Pod (p3)
		//   zone3 (n3): 已运行 0 个 Pod
		//   MaxSkew=1, ScheduleAnyway
		// 期望：n3 (zone3) — 最小化跨 zone 的 skew
		{
			TestCommonStruct: uthelper.TestCommonStruct{
				Name: "pod topology spread prefers node to reduce skew",
				PodGroups: []*schedulingv1.PodGroup{
					util.BuildPodGroup("pg1", "c1", "c1", 0, nil, schedulingv1.PodGroupInqueue),
				},
				Pods: []*v1.Pod{
					// zone1 (n1): 2 个 Pod
					util.BuildPod("c1", "p1", "n1", v1.PodRunning,
						api.BuildResourceList("1", "1G"), "pg1",
						map[string]string{"app": "foo"}, make(map[string]string)),
					util.BuildPod("c1", "p2", "n1", v1.PodRunning,
						api.BuildResourceList("1", "1G"), "pg1",
						map[string]string{"app": "foo"}, make(map[string]string)),
					// zone2 (n2): 1 个 Pod
					util.BuildPod("c1", "p3", "n2", v1.PodRunning,
						api.BuildResourceList("1", "1G"), "pg1",
						map[string]string{"app": "foo"}, make(map[string]string)),
					// 待调度 Pod，带有拓扑分散约束
					util.BuildPodWithTopologySpreadConstraints("c1", "p4", "", v1.PodPending,
						api.BuildResourceList("1", "1G"), "pg1",
						map[string]string{"app": "foo"}, make(map[string]string),
						[]v1.TopologySpreadConstraint{
							{
								MaxSkew:           1,
								TopologyKey:       "zone",
								WhenUnsatisfiable: v1.ScheduleAnyway,
								LabelSelector: &metav1.LabelSelector{
									MatchLabels: map[string]string{"app": "foo"},
								},
							},
						}),
				},
				Nodes: []*v1.Node{
					util.BuildNode("n1", api.BuildResourceList("4", "4Gi",
						[]api.ScalarResource{{Name: "pods", Value: "10"}}...), map[string]string{"zone": "zone1"}),
					util.BuildNode("n2", api.BuildResourceList("4", "4Gi",
						[]api.ScalarResource{{Name: "pods", Value: "10"}}...), map[string]string{"zone": "zone2"}),
					util.BuildNode("n3", api.BuildResourceList("4", "4Gi",
						[]api.ScalarResource{{Name: "pods", Value: "10"}}...), map[string]string{"zone": "zone3"}),
				},
				Queues: []*schedulingv1.Queue{
					util.BuildQueue("c1", 1, nil),
				},
				ExpectBindsNum: 1,
				ExpectBindMap: map[string]string{
					// zone3 是空的，将 Pod 放到 zone3 后 skew 最小：
					//   zone1=2, zone2=1, zone3=1 → max skew = 1 ≤ MaxSkew
					"c1/p4": "n3",
				},
			},
			PodTopologySpreadWeight: 10,
		},
	}

	trueValue := true

	for i, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			// 构造 tier 配置：gang 负责 JobReady，nodeorder 负责 NodeOrder
			tiers := []conf.Tier{
				{
					Plugins: []conf.PluginOption{
						{
							Name:            gang.PluginName,
							EnabledJobReady: &trueValue,
						},
						{
							Name:             PluginName,
							EnabledNodeOrder: &trueValue,
							Arguments: framework.Arguments{
								LeastRequestedWeight:    test.LeastRequestedWeight,
								MostRequestedWeight:     test.MostRequestedWeight,
								BalancedResourceWeight:  test.BalancedResourceWeight,
								NodeAffinityWeight:      test.NodeAffinityWeight,
								TaintTolerationWeight:   test.TaintTolerationWeight,
								PodAffinityWeight:       test.PodAffinityWeight,
								PodTopologySpreadWeight: test.PodTopologySpreadWeight,
								ImageLocalityWeight:     test.ImageLocalityWeight,
							},
						},
					},
				},
			}

			test.Plugins = plugins
			test.RegisterSession(tiers, nil)
			defer test.Close()

			// 运行 allocate action，触发 NodeOrderFn 调用链
			action := allocate.New()
			test.Run([]framework.Action{action})

			if err := test.CheckAll(i); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestInitPlugin 测试 InitPlugin 的插件创建逻辑。
//
// 验证两种场景：
//  1. 默认权重 — LeastAllocated/Balanced/NodeAffinity/ImageLocality 应被创建，
//     MostAllocated 不应被创建（默认权重为 0）。
//     ScorePlugins 中应有 InterPodAffinity/TaintToleration/PodTopologySpread。
//  2. 全零权重 — 所有子插件都不应被创建。
func TestInitPlugin(t *testing.T) {
	tests := []struct {
		name                    string
		weight                  priorityWeight
		expectNodeOrderKeys     []string // 期望在 NodeOrderScorePlugins 中的插件名
		expectNoNodeOrderKeys   []string // 期望不在 NodeOrderScorePlugins 中的插件名
		expectScorePlugins      []string // 期望在 ScorePlugins 中的插件名
		expectNotInScorePlugins []string // 期望不在 ScorePlugins 中的插件名
	}{
		{
			name:   "default weights",
			weight: calculateWeight(nil), // nil → 使用默认权重
			// 默认权重下：leastReqWeight=1 → LeastAllocated 被创建
			//              mostReqWeight=0 → MostAllocated 不被创建
			expectNodeOrderKeys: []string{
				noderesources.Name + "_LeastAllocated",
				noderesources.BalancedAllocationName,
				nodeaffinity.Name,
				imagelocality.Name,
			},
			expectNoNodeOrderKeys: []string{
				noderesources.Name + "_MostAllocated", // 默认 mostReqWeight=0
			},
			expectScorePlugins: []string{
				interpodaffinity.Name,
				tainttoleration.Name,
				podtopologyspread.Name,
			},
			expectNotInScorePlugins: []string{},
		},
		{
			name:                "all weights zero",
			weight:              priorityWeight{}, // 零值结构体，所有权重 = 0
			expectNodeOrderKeys: []string{},
			expectNoNodeOrderKeys: []string{
				noderesources.Name + "_LeastAllocated",
				noderesources.Name + "_MostAllocated",
				noderesources.BalancedAllocationName,
				nodeaffinity.Name,
				imagelocality.Name,
			},
			expectScorePlugins: []string{},
			expectNotInScorePlugins: []string{
				interpodaffinity.Name,
				tainttoleration.Name,
				podtopologyspread.Name,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 创建插件实例并覆盖权重
			pp := New(nil).(*NodeOrderPlugin)
			pp.weight = tt.weight

			// 构造最小的 K8s Framework Handle（fake client + informer）
			nodeMap := map[string]k8sframework.NodeInfo{}
			client := k8sfake.NewSimpleClientset()
			informerFactory := informers.NewSharedInformerFactory(client, 0)
			pp.Handle = k8s.NewFramework(
				nodeMap,
				k8s.WithClientSet(client),
				k8s.WithInformerFactory(informerFactory),
			)

			pp.InitPlugin()

			// 验证 NodeOrderScorePlugins
			for _, key := range tt.expectNodeOrderKeys {
				if _, exists := pp.NodeOrderScorePlugins[key]; !exists {
					t.Errorf("expected %s in NodeOrderScorePlugins, but not found", key)
				}
			}
			for _, key := range tt.expectNoNodeOrderKeys {
				if _, exists := pp.NodeOrderScorePlugins[key]; exists {
					t.Errorf("expected %s not in NodeOrderScorePlugins, but found", key)
				}
			}

			// 验证 ScorePlugins
			for _, pluginName := range tt.expectScorePlugins {
				if _, exists := pp.ScorePlugins[pluginName]; !exists {
					t.Errorf("expected %s in ScorePlugins, but not found", pluginName)
				}
			}
			for _, pluginName := range tt.expectNotInScorePlugins {
				if _, exists := pp.ScorePlugins[pluginName]; exists {
					t.Errorf("expected %s not in ScorePlugins, but found", pluginName)
				}
			}
		})
	}
}
