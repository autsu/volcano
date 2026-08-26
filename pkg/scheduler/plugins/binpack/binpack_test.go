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
	"math"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	schedulingv1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/uthelper"
	"volcano.sh/volcano/pkg/scheduler/util"
)

const (
	// eps 是浮点数比较的容差，用于判断两个 float64 分数是否相等。
	// 由于浮点运算存在精度损失，直接使用 == 比较可能导致误判。
	eps = 1e-8
)

// TestArguments 测试参数解析的正确性。
//
// 验证 calculateWeight() 对各种参数组合的处理：
//   - 正常正整数值的解析
//   - 扩展资源的逗号分隔列表解析
//   - 负权重值的自动纠正（→ 1）
//   - CPU/Memory 被正确合并到 BinPackingResources 查找表
//
// 测试流程：
//  1. 注册 binpack 插件构建器
//  2. 使用给定的 arguments 构建插件实例
//  3. 断言各权重字段的解析结果
func TestArguments(t *testing.T) {
	// 注册 binpack 插件的构建器，使后续的 GetPluginBuilder 可以找到它
	framework.RegisterPluginBuilder(PluginName, New)
	defer framework.CleanupPluginBuilders()

	// 构造测试参数：故意设置 example.com/foo 的权重为 -3，
	// 期望被自动纠正为 1
	arguments := framework.Arguments{
		"binpack.weight":                    10,
		"binpack.cpu":                       5,
		"binpack.memory":                    2,
		"binpack.resources":                 "nvidia.com/gpu, example.com/foo",
		"binpack.resources.nvidia.com/gpu":  7,
		"binpack.resources.example.com/foo": -3, // 负数 → 应被纠正为 1
	}

	// 获取插件构建器并创建插件实例
	builder, ok := framework.GetPluginBuilder(PluginName)
	if !ok {
		t.Fatalf("should have plugin named %s", PluginName)
	}

	plugin := builder(arguments)
	binpack, ok := plugin.(*binpackPlugin)
	if !ok {
		t.Fatalf("plugin should be %T, but not %T", binpack, plugin)
	}

	// 验证各字段的解析结果
	weight := binpack.weight
	if weight.BinPackingWeight != 10 {
		t.Errorf("weight should be 10, but not %v", weight.BinPackingWeight)
	}
	if weight.BinPackingCPU != 5 {
		t.Errorf("cpu should be 5, but not %v", weight.BinPackingCPU)
	}
	if weight.BinPackingMemory != 2 {
		t.Errorf("memory should be 2, but not %v", weight.BinPackingMemory)
	}

	// 验证 BinPackingResources 查找表的内容
	for name, weight := range weight.BinPackingResources {
		switch name {
		case "nvidia.com/gpu":
			if weight != 7 {
				t.Errorf("gpu should be 7, but not %v", weight)
			}
		case "example.com/foo":
			// -3 应被自动纠正为 1
			if weight != 1 {
				t.Errorf("example.com/foo should be 1, but not %v", weight)
			}
		case v1.ResourceCPU:
			// CPU 应被合并到查找表，值等于 BinPackingCPU
			if weight != 5 {
				t.Errorf("%v should be 5, but not %v", v1.ResourceCPU, weight)
			}
		case v1.ResourceMemory:
			// Memory 应被合并到查找表，值等于 BinPackingMemory
			if weight != 2 {
				t.Errorf("%v should be 2, but not %v", v1.ResourceMemory, weight)
			}
		default:
			t.Errorf("resource %s with weight %d should not appear", name, weight)
		}
	}
}

// addResource 是一个测试辅助函数，用于向 ResourceList 中添加资源。
// 它将字符串表示的资源量（如 "2", "4Gi"）解析为 Kubernetes Quantity 并添加到列表中。
func addResource(resourceList v1.ResourceList, name v1.ResourceName, need string) {
	resourceList[name] = resource.MustParse(need)
}

// TestNode 测试 BinPackingScore 在不同配置下的打分正确性。
//
// 测试场景设计：
//   - 4 个 Pod (p1, p2, p3, p4) 各有不同的资源需求和节点分配
//   - 3 个节点 (n1, n2, n3) 各有不同的资源容量
//   - 2 组不同的权重配置，验证不同权重下的打分结果
//
// Pod 详情：
//
//	p1: "c1/p1" — nodeName=n1, 需要 1CPU/1Gi 内存，无 GPU/FOO
//	p2: "c1/p2" — nodeName=n3, 需要 1.5CPU/0Gi 内存，无 GPU/FOO
//	p3: "c1/p3" — 未调度，需要 2CPU/10Gi 内存 + 2 GPU
//	p4: "c1/p4" — 未调度，需要 3CPU/4Gi 内存 + 3 example.com/foo
//
// 节点详情：
//
//	n1: 2 CPU, 4Gi 内存 (无 GPU, 无 FOO) — p1 已在此节点运行
//	n2: 4 CPU, 16Gi 内存, 4 GPU (无 FOO)   — 空节点
//	n3: 2 CPU, 4Gi 内存 (无 GPU, 16 FOO)   — p2 已占用 1.5 CPU
//
// 边界条件覆盖：
//   - CPU 容量不足 → 得分 0
//   - GPU 资源缺失 → 得分 0
//   - 正常装箱比较 → 更满的节点得分更高
func TestNode(t *testing.T) {
	// 定义测试用扩展资源名称
	GPU := v1.ResourceName("nvidia.com/gpu")
	FOO := v1.ResourceName("example.com/foo")

	// 创建 4 个测试 Pod
	// p1: 已调度到 n1，需 1 CPU + 1Gi 内存
	p1 := util.BuildPod("c1", "p1", "n1", v1.PodPending,
		api.BuildResourceList("1", "1Gi"), "pg1",
		make(map[string]string), make(map[string]string))

	// p2: 已调度到 n3，需 1.5 CPU，不占用内存
	p2 := util.BuildPod("c1", "p2", "n3", v1.PodPending,
		api.BuildResourceList("1.5", "0Gi"), "pg1",
		make(map[string]string), make(map[string]string))

	// p3: 待调度，需 2 CPU + 10Gi 内存 + 2 GPU
	p3 := util.BuildPod("c1", "p3", "", v1.PodPending,
		api.BuildResourceList("2", "10Gi"), "pg1",
		make(map[string]string), make(map[string]string))
	addResource(p3.Spec.Containers[0].Resources.Requests, GPU, "2")

	// p4: 待调度，需 3 CPU + 4Gi 内存 + 3 example.com/foo
	p4 := util.BuildPod("c1", "p4", "", v1.PodPending,
		api.BuildResourceList("3", "4Gi"), "pg1",
		make(map[string]string), make(map[string]string))
	addResource(p4.Spec.Containers[0].Resources.Requests, FOO, "3")

	// 创建 3 个测试节点
	n1 := util.BuildNode("n1", api.BuildResourceList("2", "4Gi",
		[]api.ScalarResource{{Name: "pods", Value: "10"}}...), make(map[string]string))

	n2 := util.BuildNode("n2", api.BuildResourceList("4", "16Gi",
		[]api.ScalarResource{{Name: "pods", Value: "10"}}...), make(map[string]string))
	addResource(n2.Status.Allocatable, GPU, "4") // n2 有 4 GPU

	n3 := util.BuildNode("n3", api.BuildResourceList("2", "4Gi",
		[]api.ScalarResource{{Name: "pods", Value: "10"}}...), make(map[string]string))
	addResource(n3.Status.Allocatable, FOO, "16") // n3 有 16 FOO

	// 创建测试用的 PodGroup 和 Queue
	pg1 := util.BuildPodGroup("pg1", "c1", "c1", 0, nil, "")
	queue1 := util.BuildQueue("c1", 1, nil)

	tests := []struct {
		uthelper.TestCommonStruct          // 嵌入通用测试结构
		arguments                framework.Arguments  // binpack 插件配置参数
		expected                 map[string]map[string]float64 // taskID → nodeName → expectedScore
	}{
		{
			// 测试用例 1：高 CPU 权重 + GPU/FOO 扩展资源权重
			// binpack.weight=10, cpu=2, mem=3, gpu=7, foo=8
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:      "single job with extended resources",
				Plugins:   map[string]framework.PluginBuilder{PluginName: New},
				PodGroups: []*schedulingv1.PodGroup{pg1},
				Queues:    []*schedulingv1.Queue{queue1},
				Pods:      []*v1.Pod{p1, p2, p3, p4},
				Nodes:     []*v1.Node{n1, n2, n3},
			},
			arguments: framework.Arguments{
				"binpack.weight":                    10,
				"binpack.cpu":                       2,
				"binpack.memory":                    3,
				"binpack.resources":                 "nvidia.com/gpu, example.com/foo",
				"binpack.resources.nvidia.com/gpu":  7,
				"binpack.resources.example.com/foo": 8,
			},
			expected: map[string]map[string]float64{
				"c1/p1": {
					// n1: p1 已在 n1 运行，n1 used = p1(1c, 1Gi)
					//   CPU: (1+1)×2/2 = 2.0
					//   Mem: (1Gi+1Gi)×3/4Gi = 1.5
					//   score = (2.0+1.5)/(2+3) × 100 × 10 = 3.5/5 × 1000 = 700
					"n1": 700,
					// n2: 空节点，p1 不在 n2，n2 used = 0
					//   CPU: (1+0)×2/4 = 0.5
					//   Mem: (1Gi+0)×3/16Gi = 0.1875
					//   score = (0.5+0.1875)/5 × 1000 = 137.5
					"n2": 137.5,
					// n3: p2 占用 1.5 CPU 在 n3, 剩余 0.5 CPU < p1 的 1 CPU
					//   → used+requested = 1.5+1 = 2.5 > 2 → 得分 0
					"n3": 0,
				},
				"c1/p2": {
					// n1: p1 已用 1 CPU，p2 需要 1.5 CPU
					//   → 1+1.5 = 2.5 > 2 → 得分 0
					"n1": 0,
					// n2: 空节点
					//   CPU: (1.5+0)×2/4 = 0.75
					//   Mem: (0+0)×3/16Gi = 0 (p2 内存请求为 0，被 ResourceNames() 跳过)
					//   score = 0.75/2 × 1000 = 375
					"n2": 375,
					// n3: p2 已在 n3
					//   CPU: (1.5+1.5)×2/2 = 3.0
					//   Mem: 0（p2 无内存请求）
					//   score = 3.0/2 × 1000 = 1500... 等等，但 expected 是 0？
					//
					//   注意！p2 的 NodeName 是 "n3"，表示 p2 已经在 n3 上。
					//   对于已在节点上的 task，打分时 used 包含自身。
					//   CPU: (1.5+1.5)×2/2 = 3.0, score = 3.0/2 × 1000 = 1500
					//   但 expected["c1/p2"]["n3"] = 0！
					//
					//   原因：p2 已经在 n3 上，但 n3 只有 2 CPU。
					//   p2 自己需要 1.5 CPU，加上 p2 自身的 used = 1.5
					//   request(1.5) + used(1.5) = 3.0 > allocatable(2)
					//   实际上...等等，used 应该包括 p2 自己的 usage。
					//   让我重新思考：
					//   p2 已调度到 n3，所以测试中 n3 的 used 包括 p2 的 1.5 CPU
					//   p2 的 request 也是 1.5 CPU
					//   usedFinally = 1.5 + 1.5 = 3.0 > capacity 2.0 → 得分 0！
					//   这是对的——同一个 task 被 double count 了！
					//
					//   这说明：对于已调度的 task，BinPackingScore 只在 task 不在该节点时
					//   才有意义。这在实际调度流程中是合理的，因为 allocate action
					//   只对 pending task 调用 NodeOrderFn。
					"n3": 0,
				},
				"c1/p3": {
					// p3 需要: 2 CPU, 10Gi 内存, 2 GPU
					// n1: 无 GPU → GPU request=2 但 n1 allocatable GPU=0
					//   ResourceBinPackingScore: capacity==0 → 0, nil (跳过)
					//   但 p3 需要 GPU，n1 没有 → 实际上 GPU 不会导致 error
					//   等等——如果 capacity==0，ResourceBinPackingScore 返回 0, nil
					//   不会导致错误。那为什么 expected p3 on n1 = 0？
					//   让我检查——p3 需要 2 CPU + 10Gi 内存
					//   n1 有 p1(1c, 1Gi) → n1 used=1c, 1Gi
					//   CPU: (2+1)×2/2 = 3.0 (但是 capacity 是 2c！)
					//   → request(2)+used(1)=3 > capacity(2) → 0, error
					//   所以 CPU 维度就失败了！
					"n1": 0,
					// n2: 有 4 CPU, 16Gi 内存, 4 GPU
					//   CPU: (2+0)×2/4 = 1.0
					//   Mem: (10Gi+0)×3/16Gi = 1.875
					//   GPU: (2+0)×7/4 = 3.5
					//   score = (1.0+1.875+3.5)/(2+3+7) × 1000 = 6.375/12 × 1000 = 531.25
					"n2": 531.25,
					// n3: 无 GPU，CPU (2c) 被 p2(1.5c) 占用
					//   CPU: (2+1.5)×2/2 = 3.5 > 2 → 0, error
					"n3": 0,
				},
				"c1/p4": {
					// p4 需要: 3 CPU, 4Gi 内存, 3 example.com/foo
					// n1: 无 FOO 资源
					//   CPU: (3+1)×2/2 = 4.0 > 2 → 0, error
					"n1": 0,
					// n2: 无 FOO 资源
					//   但 capacity FOO=0 → ResourceBinPackingScore 返回 0,nil
					//   CPU: (3+0)×2/4 = 1.5
					//   Mem: (4Gi+0)×3/16Gi = 0.75
					//   score = (1.5+0.75)/(2+3) × 1000 = 2.25/5 × 1000 = 450...
					//   但 expected 是 173.0769... 这说明 FOO 维度参与了计算
					//
					//   等等：capacity FOO=0，ResourceBinPackingScore 返回 0,nil (跳过)
					//   但 weightSum 包含了 FOO 的权重 8！
					//   score = (1.5+0.75)/(2+3+8) × 1000 = 2.25/13 × 1000 ≈ 173.0769 ✓
					//
					//   这暴露了一个微妙的问题：跳过容量为 0 的资源维度时，
					//   weightSum 仍然包含了该维度的权重！这意味着如果在 weightSum 中
					//   计入了那些 capacity==0 维度的权重，归一化会受到影响。
					//
					//   实际上检查代码——weightSum 在循环内是 resourceWeight 累加的，
					//   但 ResourceBinPackingScore 返回 0,nil 时（capacity==0），
					//   代码仍然执行了 weightSum += resourceWeight！
					//   这确实导致了分母变大，分数被稀释。
					//
					//   这是一个潜在的设计问题：capacity==0 时应该跳过 weightSum 累加。
					//   但这是当前代码的实际行为，测试也与此一致。
					"n2": 173.076923076,
					// n3: 有 FOO(16)，但 CPU 不足
					//   CPU: (3+1.5)×2/2 = 4.5 > 2 → 0, error
					"n3": 0,
				},
			},
		},
		{
			// 测试用例 2：低权重配置，验证不同权重下的行为
			// binpack.weight=1, cpu=1, mem=1, gpu=23
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:      "single job with high GPU weight",
				Plugins:   map[string]framework.PluginBuilder{PluginName: New},
				PodGroups: []*schedulingv1.PodGroup{pg1},
				Queues:    []*schedulingv1.Queue{queue1},
				Pods:      []*v1.Pod{p1, p2, p3, p4},
				Nodes:     []*v1.Node{n1, n2, n3},
			},
			arguments: framework.Arguments{
				"binpack.weight":                   1,
				"binpack.cpu":                      1,
				"binpack.memory":                   1,
				"binpack.resources":                "nvidia.com/gpu",
				"binpack.resources.nvidia.com/gpu": 23,
			},
			expected: map[string]map[string]float64{
				"c1/p1": {
					// p1 on n1 (p1 已在 n1):
					//   CPU: (1+1)×1/2 = 1.0
					//   Mem: (1Gi+1Gi)×1/4Gi = 0.5
					//   GPU: p1 没有 GPU 需求 → 不参与
					//   score = (1.0+0.5)/(1+1) × 100 × 1 = 1.5/2 × 100 = 75
					"n1": 75,
					// p1 on n2 (空节点):
					//   CPU: (1+0)×1/4 = 0.25
					//   Mem: (1Gi+0)×1/16Gi ≈ 0.0625
					//   score = (0.25+0.0625)/2 × 100 = 15.625
					"n2": 15.625,
					"n3": 0, // n3 CPU 被 p2 占满
				},
				"c1/p2": {
					"n1": 0,    // n1 CPU 被 p1 占满
					// p2 on n2 (空节点):
					//   CPU: (1.5+0)×1/4 = 0.375
					//   p2 内存请求 0 → ResourceNames() 排除 memory
					//   score = 0.375/1 × 100 = 37.5
					"n2": 37.5,
					"n3": 0,    // p2 在 n3 被 double count
				},
				"c1/p3": {
					"n1": 0,    // 无 GPU
					// p3 on n2 (空节点):
					//   CPU: (2+0)×1/4 = 0.5
					//   Mem: (10Gi+0)×1/16Gi = 0.625
					//   GPU: (2+0)×23/4 = 11.5
					//   score = (0.5+0.625+11.5)/(1+1+23) × 100 = 12.625/25 × 100 = 50.5
					"n2": 50.5,
					"n3": 0,    // 无 GPU + CPU 不足
				},
				"c1/p4": {
					"n1": 0,    // CPU 不足
					// p4 on n2 (空节点):
					//   CPU: (3+0)×1/4 = 0.75
					//   Mem: (4Gi+0)×1/16Gi = 0.25
					//   p4 的 FOO 需求 → weight 表中无 FOO → continue 跳过
					//   score = (0.75+0.25)/(1+1) × 100 = 1.0/2 × 100 = 50
					"n2": 50,
					"n3": 0,    // CPU 不足
				},
			},
		},
	}

	trueValue := true
	for i, test := range tests {
		// 构造 tier 配置：启用 binpack 的 NodeOrder
		tiers := []conf.Tier{
			{
				Plugins: []conf.PluginOption{
					{
						Name:             PluginName,
						EnabledNodeOrder: &trueValue, // 启用 NodeOrder 扩展点
						Arguments:        test.arguments,
					},
				},
			},
		}

		// 注册会话：这会触发 OnSessionOpen，注册 nodeOrderFn
		ssn := test.RegisterSession(tiers, nil)

		// 遍历所有 Job → Task → Node，验证每个 pair 的得分
		for _, job := range ssn.Jobs {
			for _, task := range job.Tasks {
				taskID := fmt.Sprintf("%s/%s", task.Namespace, task.Name)
				for _, node := range ssn.Nodes {
					// 通过 Session 调用注册的 NodeOrderFn
					score, err := ssn.NodeOrderFn(task, node)
					if err != nil {
						t.Errorf("case%d: task %s on node %s has err %v",
							i, taskID, node.Name, err)
						continue
					}

					// 验证得分与期望值一致（考虑浮点误差）
					if expectScore := test.expected[taskID][node.Name]; math.Abs(expectScore-score) > eps {
						t.Errorf("case%d: task %s on node %s expect have score %v, but get %v",
							i, taskID, node.Name, expectScore, score)
					}
				}
			}
		}
	}
}
