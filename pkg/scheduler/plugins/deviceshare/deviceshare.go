/*
Copyright 2024 The Volcano Authors.

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

// 本文件是 volcano 调度器的 deviceshare 插件主体，负责把各类设备（GPU/NPU）接入调度框架。
package deviceshare

import (
	// context 用于把上下文传入打分函数
	"context"
	// fmt 用于格式化错误信息
	"fmt"
	// math 用于分数取整
	"math"
	// reflect 用于判断接口值是否为 nil
	"reflect"
	// sync 提供一次性初始化与读写锁
	"sync"

	// v1 是 k8s 核心 API 的 Pod 类型
	v1 "k8s.io/api/core/v1"
	// klog 是 k8s 的日志库
	"k8s.io/klog/v2"
	// fwk 是 k8s 调度框架的状态类型
	fwk "k8s.io/kube-scheduler/framework"

	// api 是 volcano 调度器对节点/任务/Pod 的抽象
	"volcano.sh/volcano/pkg/scheduler/api"
	// hami 是昇腾 HAMi 模式的 VNPU 设备插件包
	"volcano.sh/volcano/pkg/scheduler/api/devices/ascend/hami"
	// vnpu 是昇腾 MindCluster 的 VNPU 设备插件包
	"volcano.sh/volcano/pkg/scheduler/api/devices/ascend/mindcluster/ascend310p/vnpu"
	// config 是设备的几何配置（ConfigMap 加载）
	"volcano.sh/volcano/pkg/scheduler/api/devices/config"
	// gpushare 是 NVIDIA 粗粒度共享（按显存/整卡）设备插件
	"volcano.sh/volcano/pkg/scheduler/api/devices/nvidia/gpushare"
	// vgpu 是 NVIDIA 细粒度虚拟化（HAMi/MIG）设备插件
	"volcano.sh/volcano/pkg/scheduler/api/devices/nvidia/vgpu"
	// framework 是 volcano 调度框架（Session/Plugin/Arguments 等）
	"volcano.sh/volcano/pkg/scheduler/framework"
	// vnpu310p 是本插件目录下对昇腾 VNPU 的具体实现（初始化/打分）
	vnpu310p "volcano.sh/volcano/pkg/scheduler/plugins/deviceshare/devices/ascend/310p/vnpu"
)

// PluginName indicates name of volcano scheduler plugin.
// PluginName 是本插件在 volcano 注册表中的名字。
const (
	// PluginName 插件的唯一标识，配置文件里用它来开关本插件
	PluginName = "deviceshare"
	// GPUSharingPredicate is the key for enabling GPU Sharing Predicate in YAML
	// GPUSharingPredicate 是配置项 key：开启 NVIDIA 按显存共享（gpushare 模式）
	GPUSharingPredicate = "deviceshare.GPUSharingEnable"
	// NodeLockEnable 是配置项 key：开启节点级设备锁（多调度器并发安全）
	NodeLockEnable = "deviceshare.NodeLockEnable"
	// GPUNumberPredicate 是配置项 key：开启 NVIDIA 按整卡数共享（gpu-number 模式）
	GPUNumberPredicate = "deviceshare.GPUNumberEnable"

	// VGPUEnable 是配置项 key：开启 NVIDIA 细粒度 vGPU（HAMi/MIG）模式
	VGPUEnable = "deviceshare.VGPUEnable"

	// AscendMindClusterVNPU 是配置项 key：开启昇腾 MindCluster VNPU（310P）模式
	AscendMindClusterVNPU = "deviceshare.AscendMindClusterVNPUEnable"
	// AscendHAMiVNPUEnable 是配置项 key：开启昇腾 HAMi VNPU（多芯片型号）模式
	AscendHAMiVNPUEnable = "deviceshare.AscendHAMiVNPUEnable"

	// SchedulePolicyArgument 是配置项 key：设备打分策略（binpack/spread 等）
	SchedulePolicyArgument = "deviceshare.SchedulePolicy"
	// ScheduleWeight 是配置项 key：设备打分在总分中的权重
	ScheduleWeight = "deviceshare.ScheduleWeight"

	// KnownGeometriesCMName 是配置项 key：MIG 已知几何模板所在 ConfigMap 名
	KnownGeometriesCMName = "deviceshare.KnownGeometriesCMName"
	// KnownGeometriesCMNamespace 是配置项 key：上述 ConfigMap 所在命名空间
	KnownGeometriesCMNamespace = "deviceshare.KnownGeometriesCMNamespace"
)

var (
	// once 保证各设备在进程内只注册一次（RegisterDevice 是幂等安全的，但避免重复日志）
	once sync.Once
)

// deviceSharePlugin 是 deviceshare 插件的运行时实例，持有配置、打分参数与跨会话持久状态。
type deviceSharePlugin struct {
	// Arguments given for the plugin
	// pluginArguments 是用户在 volcano 配置里传给本插件的全部参数
	pluginArguments framework.Arguments
	// schedulePolicy 是设备打分策略字符串（来自 SchedulePolicyArgument 配置项）
	schedulePolicy string
	// scheduleWeight 是设备打分权重整数（来自 ScheduleWeight 配置项）
	scheduleWeight int
	// lock protects persistedGPUs and persistedPodRules from concurrent access
	// across scheduling sessions and Allocate calls.
	// lock 保护下面两个 map 在多次调度会话与 Allocate 调用之间的并发读写
	lock sync.RWMutex
	// persistedGPUs survives across scheduling sessions. Maps
	// nodeName → namespace/name → set of GPU indices allocated to that pod.
	// Updated by Allocate, pruned by OnSessionOpen.
	// persistedGPUs 跨调度会话存活：结构为 节点名 → pod key → 该 pod 占用的 GPU 下标集合；
	// 由 Allocate 写入，由 OnSessionOpen 清理过期条目。
	persistedGPUs map[string]map[string]map[int]struct{}
	// persistedPodRules maps nodeName → namespace/name → set of rule indices.
	// persistedPodRules 跨调度会话存活：节点名 → pod key → 该 pod 命中的独占规则下标集合。
	persistedPodRules map[string]map[string]map[int]struct{}
}

// New return priority plugin
// New 是插件的构造函数，被 volcano 框架调用以创建本插件实例。
func New(arguments framework.Arguments) framework.Plugin {
	// 构造一个基础实例：初始化所有 map 与默认参数
	dsp := &deviceSharePlugin{
		pluginArguments:   arguments,
		schedulePolicy:    "",
		scheduleWeight:    0,
		persistedGPUs:     make(map[string]map[string]map[int]struct{}),
		persistedPodRules: make(map[string]map[string]map[int]struct{}),
	}
	// 根据配置把对应设备插件启用并注册到全局设备表
	enablePredicate(dsp)
	// 返回构造好的插件实例给框架
	return dsp
}

// Name 返回插件名，框架用它做标识与去重。
func (dp *deviceSharePlugin) Name() string {
	return PluginName
}

// enablePredicate 解析配置项，设置各设备包里的全局开关，并注册启用的设备。
func enablePredicate(dsp *deviceSharePlugin) {
	// Checks whether predicate.GPUSharingEnable is provided or not, if given, modifies the value in predicateEnable struct.
	// nodeLockEnable 是本地临时变量，读完后会同步给各设备包
	nodeLockEnable := false
	// 取出插件参数对象
	args := dsp.pluginArguments
	// 读取 gpushare 是否开启（按显存共享）
	args.GetBool(&gpushare.GpuSharingEnable, GPUSharingPredicate)
	// 读取 gpushare 是否开启（按整卡数共享）
	args.GetBool(&gpushare.GpuNumberEnable, GPUNumberPredicate)
	// 读取是否开启节点锁
	args.GetBool(&nodeLockEnable, NodeLockEnable)
	// 读取是否开启 NVIDIA vGPU 模式
	args.GetBool(&vgpu.VGPUEnable, VGPUEnable)
	// 读取是否开启昇腾 MindCluster VNPU 模式
	args.GetBool(&vnpu.AscendMindClusterVNPUEnable, AscendMindClusterVNPU)
	// 读取是否开启昇腾 HAMi VNPU 模式
	args.GetBool(&hami.AscendHAMiVNPUEnable, AscendHAMiVNPUEnable)

	// 把节点锁开关同步给各设备包（gpushare/vgpu/hami 共用）
	gpushare.NodeLockEnable = nodeLockEnable
	vgpu.NodeLockEnable = nodeLockEnable
	hami.NodeLockEnable = nodeLockEnable

	// 读取打分策略字符串
	args.GetString(&dsp.schedulePolicy, SchedulePolicyArgument)
	// 读取打分权重整数
	args.GetInt(&dsp.scheduleWeight, ScheduleWeight)
	// 把打分策略同步给 vgpu 设备包
	vgpu.SchedulePolicy = dsp.schedulePolicy

	// gpushare 两种模式互斥校验：不能同时开
	if gpushare.GpuSharingEnable && gpushare.GpuNumberEnable {
		klog.Fatal("can not define true in both gpu sharing and gpu number")
	}
	// gpushare 与 vgpu 互斥校验：不能同时开
	if (gpushare.GpuSharingEnable || gpushare.GpuNumberEnable) && vgpu.VGPUEnable {
		klog.Fatal("gpu-share and vgpu can't be used together")
	}

	// MIG 几何配置 ConfigMap 名，默认值；若用户配置了则覆盖
	knownGeometriesCMName := "volcano-vgpu-device-config"
	args.GetString(&knownGeometriesCMName, KnownGeometriesCMName)
	// MIG 几何配置 ConfigMap 命名空间，默认值；若用户配置了则覆盖
	knownGeometriesCMNamespace := "kube-system"
	args.GetString(&knownGeometriesCMNamespace, KnownGeometriesCMNamespace)
	// 加载几何配置（MIG 模板），供 vgpu 调度使用
	config.InitDevicesConfig(knownGeometriesCMName, knownGeometriesCMNamespace)
	// 把根据配置启用的设备注册到全局设备表
	registerDevices()
}

// registerDevices 把当前配置启用的所有设备注册到 volcano 全局设备表（只执行一次）。
func registerDevices() {
	// 借助 sync.Once 保证整个进程内只注册一次
	once.Do(func() {
		// 若开了 gpushare（显存共享或整卡数共享），注册 gpushare 设备名
		if gpushare.GpuSharingEnable || gpushare.GpuNumberEnable {
			api.RegisterDevice(gpushare.DeviceName)
		}
		// 若开了 vgpu，注册 vgpu 设备名
		if vgpu.VGPUEnable {
			api.RegisterDevice(vgpu.DeviceName)
		}
		// 若开了昇腾 MindCluster VNPU，注册 310P 设备名
		if vnpu.AscendMindClusterVNPUEnable {
			api.RegisterDevice(vnpu.DeviceName)
		}
		// 若开了昇腾 HAMi VNPU，遍历配置里的每种昇腾芯片型号逐个注册
		if hami.AscendHAMiVNPUEnable {
			for _, vnpu := range config.GetConfig().VNPUs {
				klog.V(3).Infof("register device %s", vnpu.CommonWord)
				api.RegisterDevice(vnpu.CommonWord)
			}
		}
	})
}

// createStatus 构造一个带错误码与原因的设备状态对象，供预选失败时返回。
func createStatus(code int, reason string) *api.Status {
	// 用给定 code 与 reason 填充状态结构
	status := api.Status{
		Code:   code,
		Reason: reason,
	}
	// 返回状态指针
	return &status
}

// getDeviceScore 计算单个 Pod 在单个节点上的设备打分总和（仅 vgpu/gpushare 这类使用 NodeOrderFn 的设备）。
func getDeviceScore(ctx context.Context, pod *v1.Pod, node *api.NodeInfo, schedulePolicy string) (int64, *fwk.Status) {
	// 累加分数
	s := float64(0)
	// 遍历该节点上注册的所有设备（Others 是 设备名→设备接口 的映射）
	for deviceType, device := range node.Others {
		// 只有该设备声明 Pod 需要它，才参与打分
		if device.(api.Devices).HasDeviceRequest(pod) {
			// Only process device types that use NodeOrderFn (vgpu and gpushare)
			// vnpu devices use BatchNodeOrderFn, skip them here
			// 跳过 VNPU：VNPU 走批量打分（BatchNodeOrderFn），这里只处理单节点打分设备
			if deviceType != vnpu.DeviceName {
				// 调用该设备的 ScoreNode 累加分数
				ns := device.(api.Devices).ScoreNode(pod, schedulePolicy)
				s += ns
			}
		}
	}
	// 打印调试日志：任务在该节点的设备得分
	klog.V(4).Infof("deviceScore for task %s/%s is: %v", pod.Namespace, pod.Name, s)
	// 四舍五入转成整数分（0.5 向上取整）
	return int64(math.Floor(s + 0.5)), nil
}

// getDeviceScoresInBatch 对一组候选节点批量计算 VNPU 设备打分（VNPU 需要跨节点联合排序）。
func getDeviceScoresInBatch(pod *v1.Pod, schedulePolicy string, allDevices []api.Devices) []float64 {
	// 根据第一个设备的具体类型做分派
	switch d := allDevices[0].(type) {
	case *vnpu.NPUDevices:
		// if you need to rewrite your score policy, add a case here
		// 若是昇腾 VNPU 设备，走本插件目录下的批量打分实现
		return vnpu310p.ScoreBatchNodes(pod, schedulePolicy, d, allDevices)
	default:
		// 其它设备类型不支持批量打分，返回空数组
		score := make([]float64, 0)
		return score
	}
}

// initScoreMap 构造 节点名→0 分的初始打分表，遇到 nil 节点跳过。
func initScoreMap(nodes []*api.NodeInfo) map[string]float64 {
	// 预分配容量，避免扩容
	scoreMap := make(map[string]float64, len(nodes))
	for _, node := range nodes {
		// 跳过 nil 节点，避免后续访问空指针
		if reflect.ValueOf(node).IsNil() {
			continue
		}
		// 该节点初始分数为 0
		scoreMap[node.Name] = 0.0
	}
	// 返回初始化好的打分表
	return scoreMap
}

// initializeDevicesWithSession 用当前调度会话 ssn 初始化每个节点上的每个设备（需要 ssn 的设备才初始化）。
func initializeDevicesWithSession(ssn *framework.Session) {
	// 遍历所有节点
	for _, nodeInfo := range ssn.Nodes { // initialize every device in every node with global ssn
		// 遍历全局已注册的设备名
		for _, val := range api.RegisteredDevices {
			// 取出该节点上对应的设备接口
			if dev, ok := nodeInfo.Others[val].(api.Devices); ok {
				// 调用设备初始化；失败仅告警，不阻断整体调度
				if err := initializeDevice(dev, ssn, nodeInfo); err != nil {
					klog.Warningf("Failed to initialize devices with session for node %s: %v", nodeInfo.Name, err)
				}
			}
		}
	}
}

// initialization function for different devices
// initializeDevice 按设备具体类型分派初始化逻辑（目前只为昇腾 310P 做初始化）。
func initializeDevice(device api.Devices, ssn *framework.Session, nodeInfo *api.NodeInfo) error {
	// 类型断言到具体设备类型
	switch d := device.(type) {
	case *vnpu.NPUDevices:
		// 仅当开启昇腾 MindCluster VNPU 时才初始化
		if vnpu.AscendMindClusterVNPUEnable {
			klog.V(3).Infof("initialize ascend310p device.")
			// 调用本插件目录下的 VNPU 初始化实现
			return vnpu310p.InitVNPUDevice(d, ssn, nodeInfo)
		}
	}
	// 其它设备无需基于 ssn 的初始化，返回 nil
	return nil
}

// OnSessionOpen 在每个调度会话打开时由框架调用：包装独占设备、初始化设备、注册预选与打分函数。
func (dp *deviceSharePlugin) OnSessionOpen(ssn *framework.Session) {
	// Wrap GPU devices with exclusivity-aware wrappers if rules are configured.
	// This must happen before initializeDevicesWithSession and predicate registration
	// so that the wrapped devices are used throughout the scheduling cycle.
	// 若配置了 GPU 独占规则，先把每个节点的 GPUDevices 包一层独占包装器；
	// 必须在设备初始化与预选注册之前完成，使整个调度周期都用包装后的设备。
	dp.wrapGPUDevicesForExclusivity(ssn)

	// initialize devices which needs ssn as input
	// 用会话初始化需要 ssn 的设备（如昇腾 VNPU）
	initializeDevicesWithSession(ssn)

	// Register event handlers to update task info in PodLister & nodeMap
	// 注册预选函数：决定任务能否调度到某节点
	ssn.AddPredicateFn(dp.Name(), func(task *api.TaskInfo, node *api.NodeInfo) error {
		// 收集所有预选失败状态
		predicateStatus := make([]*api.Status, 0)
		// Check PredicateWithCache
		// 遍历所有已注册设备（也就是 volcano configmap 里配置的设备，比如
		//  - name: deviceshare
		// 	  arguments:
		//      deviceshare.VGPUEnable: true # enable vgpu
		for _, val := range api.RegisteredDevices {
			// 取出节点上对应的设备接口
			if dev, ok := node.Others[val].(api.Devices); ok {
				// 若设备接口值为 nil，跳过并打日志
				if reflect.ValueOf(dev).IsNil() {
					klog.V(4).Infof("device %s is null, skipping it", val)
					continue
				}
				// 该 Pod 不请求此设备，跳过
				if !dev.HasDeviceRequest(task.Pod) {
					klog.V(4).Infof("pod %s/%s did not request device %s on %s, skipping it", task.Pod.Namespace, task.Pod.Name, val, node.Name)
					continue
				}
				// 调用设备预选：检查 Pod 能否放入该节点
				code, msg, err := dev.FilterNode(task.Pod, dp.schedulePolicy)
				if err != nil {
					// 预选出错，记录状态并返回 FitErr
					klog.V(4).Infof("pod %s/%s fit failed. device %s node %s err %v", task.Pod.Namespace, task.Pod.Name, val, node.Name, err)
					predicateStatus = append(predicateStatus, createStatus(code, msg))
					return api.NewFitErrWithStatus(task, node, predicateStatus...)
				}
				// 构造过滤状态对象
				filterNodeStatus := createStatus(code, msg)
				if filterNodeStatus.Code != api.Success {
					// 预选不通过，记录状态并返回 FitErr
					predicateStatus = append(predicateStatus, filterNodeStatus)
					return api.NewFitErrWithStatus(task, node, predicateStatus...)
				}
			} else {
				// 类型断言失败，告警并跳过该设备
				klog.Warningf("Devices %s assertion conversion failed, skip", val)
			}
		}

		// 预选通过，打调试日志
		klog.V(4).Infof("checkDevices predicates Task <%s/%s> on Node <%s>: fit ",
			task.Namespace, task.Name, node.Name)

		// 预选成功，返回 nil
		return nil
	})

	// 注册单节点打分函数：计算某任务在某节点的设备得分
	ssn.AddNodeOrderFn(dp.Name(), func(task *api.TaskInfo, node *api.NodeInfo) (float64, error) {
		// DeviceScore
		// 初始节点分为 0
		nodeScore := float64(0)
		// 权重大于 0 才需要计算设备分
		if dp.scheduleWeight > 0 {
			// 计算设备得分（仅 vgpu/gpushare）
			score, status := getDeviceScore(context.TODO(), task.Pod, node, dp.schedulePolicy)
			if !status.IsSuccess() {
				// 打分失败则记录告警并返回错误
				klog.Warningf("Node: %s, Calculate Device Score Failed because of Error: %v", node.Name, status.AsError())
				return 0, status.AsError()
			}

			// TODO: we should use a separate plugin for devices, and separate them from predicates and nodeOrder plugin.
			// 设备得分乘以权重作为本节点最终分数（TODO：未来应把设备拆成独立插件）
			nodeScore = float64(score) * float64(dp.scheduleWeight)
			klog.V(5).Infof("Node: %s, task<%s/%s> Device Score weight %d, score: %f", node.Name, task.Namespace, task.Name, dp.scheduleWeight, nodeScore)
		}
		// 返回该节点的设备得分
		return nodeScore, nil
	})

	// 注册批量打分函数：VNPU 需要跨节点联合排序，所以单独批量计算
	ssn.AddBatchNodeOrderFn(dp.Name(), func(task *api.TaskInfo, nodes []*api.NodeInfo) (map[string]float64, error) {
		// 初始化所有节点为 0 分
		scoreMap := initScoreMap(nodes)
		// 权重大于 0 才需要计算
		if dp.scheduleWeight > 0 {
			// 遍历已注册设备类型
			for _, deviceType := range api.RegisteredDevices {
				// only process devices which needs score nodes in batch
				// 只处理走批量打分路径的设备（VNPU），其余跳过
				if deviceType != vnpu.DeviceName {
					continue
				}

				//get all nodes' device of this kind
				//allDevices store all devices of the global nodes
				// 收集这批候选节点里该类型设备接口
				allDevices := make([]api.Devices, 0)
				for _, node := range nodes {
					// 取出节点上对应设备
					device, ok := node.Others[deviceType]
					if ok {
						// 类型断言为设备接口
						if deviceInterface, isDeviceInterface := device.(api.Devices); isDeviceInterface {
							// 若设备为 nil（未初始化），需要处理
							if reflect.ValueOf(deviceInterface).IsNil() {
								// nil 但 Pod 需要该设备：报错；nil 且 Pod 不需：跳过
								if deviceInterface == nil || deviceInterface.HasDeviceRequest(task.Pod) {
									return nil, fmt.Errorf("node not initialized with device %s", deviceType)
								}
								klog.V(4).Infof("pod %s/%s did not request device %s on %s, skipping it", task.Pod.Namespace, task.Pod.Name, deviceType, nodes[0].Name)
								continue
							}
							// 收集非 nil 设备接口
							allDevices = append(allDevices, deviceInterface)
						}
					} else {
						// 类型断言失败，告警跳过
						klog.Warningf("Devices %s assertion conversion failed, skip", deviceType)
					}
				}

				// Check if there are devices available for scoring
				// 没有任何可打分设备，跳过往下
				if len(allDevices) == 0 {
					klog.V(4).Infof("No devices of type %s found for scoring", deviceType)
					continue
				}

				// 批量计算分数（VNPU 专用）
				scores := getDeviceScoresInBatch(task.Pod, dp.schedulePolicy, allDevices)
				// Ensure score array length matches nodes count
				// 分数数组长度必须和候选节点数一致，否则跳过（防止越界）
				if len(scores) != len(nodes) {
					klog.Warningf("Score array length (%d) doesn't match nodes length (%d) for device type %s", len(scores), len(nodes), deviceType)
					continue
				}

				// 把每个节点的分数乘以权重累加进 scoreMap
				for i := range nodes {
					finalScore := scores[i] * float64(dp.scheduleWeight)
					scoreMap[nodes[i].Node.Name] += finalScore
					klog.V(5).Infof("Node: %s, task<%s/%s> Device Score weight %d, score: %f", nodes[i].Name, task.Namespace, task.Name, dp.scheduleWeight, finalScore)
				}
			}
		}
		// 返回 节点名→分数 表
		return scoreMap, nil
	})
}

// OnSessionClose 调度会话关闭时无清理逻辑（持久状态保留在插件实例上）。
func (dp *deviceSharePlugin) OnSessionClose(ssn *framework.Session) {}
