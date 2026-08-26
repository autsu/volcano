/*
Copyright 2023 The Volcano Authors.

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

package vgpu // vgpu 包：基于 HAMICore / MIG 的细粒度 GPU 共享调度设备插件

import (
	"github.com/pkg/errors"                                            // errors 提供带上下文的错误包装
	v1 "k8s.io/api/core/v1"                                            // v1 是 Kubernetes 核心 API（Pod、Node）
	"k8s.io/client-go/kubernetes"                                      // kubernetes 是 Kubernetes 客户端集
	"k8s.io/klog/v2"                                                   // klog 是 Kubernetes 日志库
	"strconv"                                                          // strconv 用于整数与时间格式化
	"strings"                                                          // strings 用于字符串包含判断
	"time"                                                             // time 用于记录设备分配时间
	"volcano.sh/volcano/pkg/scheduler/api/devices"                     // devices 定义调度器设备插件通用接口与状态码
	deviceconfig "volcano.sh/volcano/pkg/scheduler/api/devices/config" // deviceconfig 提供 vGPU 节点注册注解与资源配置
	"volcano.sh/volcano/pkg/scheduler/plugins/util/nodelock"           // nodelock 提供节点级分布式锁工具
)

// GPUUsage 记录某个 Pod 在单张 GPU 设备上的占用情况
type GPUUsage struct {
	// UsedMem 是该 Pod 在该设备上占用的显存（单位 MiB）
	UsedMem uint
	// UsedCore 是该 Pod 在该设备上占用的算力百分比（0-100）
	UsedCore uint
	// PodGroupKey 是该 Pod 所属 PodGroup 的命名空间/名称键，空表示无组
	PodGroupKey string // namespace/name of PodGroup, or "" if pod has no group
}

// GPUDevice include gpu id, memory and the pods that are sharing it.
// GPUDevice 描述单张物理 GPU 卡的细粒度共享状态（含 MIG 模板与占用账本）
type GPUDevice struct {
	// GPU ID
	// ID 是该 GPU 卡在当前节点上的唯一序号
	ID int
	// Node this GPU Device belongs
	// Node 是这张卡所属节点的名称
	Node string
	// GPU Unique ID
	// UUID 是这张物理 GPU 的全球唯一标识
	UUID string
	// The resource usage by pods that are sharing this GPU
	// PodMap 以 Pod UID 为键，记录每个共享该卡的 Pod 的占用明细
	PodMap map[string]*GPUUsage
	// memory per card
	// Memory 是这张卡的总显存容量（单位 MiB）
	Memory uint
	// max sharing number
	// Number 是这张卡允许的最大共享 Pod 数量（共享上限）
	Number uint
	// type of this number
	// Type 是设备型号（如 A100-SXM4-80GB），用于型号匹配
	Type string
	// Health condition of this GPU
	// Health 表示该卡是否健康（true 为健康）
	Health bool
	// number of allocated
	// UsedNum 是当前已分配（共享）到该卡的 Pod 数量
	UsedNum uint
	// number of device memory allocated
	// UsedMem 是当前该卡已被分配的显存总量
	UsedMem uint
	// number of core used
	// UsedCore 是当前该卡已被分配的算力百分比之和（0-100，可超过单卡限制由逻辑约束）
	UsedCore uint
	// MigTemplate for this GPU
	// MigTemplate 是这张卡可用的 MIG 几何模板（仅 MIG 模式有意义）
	MigTemplate []deviceconfig.Geometry
	/// MigUsage for this GPU
	/// MigUsage 记录这张卡上 MIG 实例的实际占用情况（仅 MIG 模式有意义）
	MigUsage deviceconfig.MigInUse
}

// GPUDevices 是某个节点上所有 GPU 卡的集合，是 vgpu 插件在调度器缓存中的核心对象
type GPUDevices struct {
	// Name 是所属节点的名称
	Name string
	// Mode GPU sharing mode
	// Mode 是节点的共享模式（hami-core / mig）
	Mode string
	// We cache score in filter step according to schedulePolicy, to avoid recalculating in score
	// Score 是在预选阶段缓存的打分结果，避免打分时重复计算
	Score float64
	// Device 以卡序号为键，保存该节点上每一张 GPU 卡的共享状态
	Device map[int]*GPUDevice
	// Sharing sharing handler
	// Sharing 是共享策略处理器（HAMICore 或 MIG 工厂），负责 TryAdd/Add/Sub 的具体切分逻辑
	Sharing SharingFactory
}

// NewGPUDevice creates a device
// NewGPUDevice 构造并返回一个单张 GPU 卡的初始状态对象
func NewGPUDevice(id int, mem uint) *GPUDevice {
	// 返回填充了基础字段与空占用账本的 GPUDevice 指针
	return &GPUDevice{
		ID:       id,                         // 设置卡序号
		Memory:   mem,                        // 设置总显存
		PodMap:   make(map[string]*GPUUsage), // 初始化空的 Pod 占用账本
		UsedNum:  0,                          // 初始已分配数量为 0
		UsedMem:  0,                          // 初始已分配显存为 0
		UsedCore: 0,                          // 初始已分配算力为 0
	}
}

// NewGPUDevices 依据节点上的 vGPU 注册注解与 Node Allocatable 资源，构造该节点的全部 GPU 卡集合
func NewGPUDevices(name string, node *v1.Node) *GPUDevices {
	// 节点为空直接返回 nil
	if node == nil {
		return nil // 无节点信息，无法构造
	}
	// 节点尚未报告 vGPU 资源时，说明节点侧设备插件还没有完成注册，暂不构造设备集合
	if node.Status.Allocatable != nil {
		gpuNumberRes, gpuNumberExists := node.Status.Allocatable[v1.ResourceName(deviceconfig.VolcanoVGPUNumber)]
		if !gpuNumberExists || gpuNumberRes.Value() == 0 {
			klog.V(3).Infof("Node %s does not have allocatable %s resource or value is 0, returning nil", node.Name, deviceconfig.VolcanoVGPUNumber)
			return nil
		}

		vgpuCoresRes, vgpuCoresExists := node.Status.Allocatable[v1.ResourceName(deviceconfig.VolcanoVGPUCores)]
		if !vgpuCoresExists || vgpuCoresRes.Value() == 0 {
			klog.V(3).Infof("Node %s does not have allocatable %s resource or value is 0, returning nil", node.Name, deviceconfig.VolcanoVGPUCores)
			return nil
		}

		vgpuMemoryRes, vgpuMemoryExists := node.Status.Allocatable[v1.ResourceName(deviceconfig.VolcanoVGPUMemory)]
		if !vgpuMemoryExists || vgpuMemoryRes.Value() == 0 {
			klog.V(3).Infof("Node %s does not have allocatable %s resource or value is 0, returning nil", node.Name, deviceconfig.VolcanoVGPUMemory)
			return nil
		}
	} else {
		klog.V(3).Infof("Node %s does not have allocatable resources information, returning nil", node.Name)
		return nil
	}
	// 读取节点上的 vGPU 设备注册注解（由 HAMi 设备插件写入）
	annos, ok := node.Annotations[deviceconfig.VolcanoVGPURegister]
	// 缺少注册注解说明未启用 vGPU，返回 nil
	if !ok {
		return nil // 未注册 vGPU 设备
	}
	// 解析注册注解，得到节点级设备集合与共享模式
	nodedevices, sharingMode := decodeNodeDevices(name, annos)
	// 解析失败或没有任何卡，返回 nil
	if (nodedevices == nil) || len(nodedevices.Device) == 0 {
		return nil // 无可用设备
	}
	// 根据共享模式取得对应的共享处理器工厂
	sharingHandler, _ := GetSharingHandler(sharingMode)
	// 打印解析出的共享模式
	klog.V(3).Infoln("GPU sharing mode: ", sharingMode)
	// 遍历每张卡，初始化其 Prometheus 指标基线
	for _, val := range nodedevices.Device {
		klog.V(3).InfoS("Nvidia Device registered name", "name", nodedevices.Name, "val", *val) // 记录设备注册信息
		ResetDeviceMetrics(val.UUID, node.Name, float64(val.Memory))                            // 重置该卡指标
	}

	// 把共享处理器挂载到设备集合上
	nodedevices.Sharing = sharingHandler
	// 返回构造完成的节点 GPU 设备集合
	return nodedevices
}

// ScoreNode 由 devicescore 插件在打分时调用，直接返回预选阶段缓存的打分
func (gs *GPUDevices) ScoreNode(pod *v1.Pod, schedulePolicy string) float64 {
	/* TODO: we need a base score to be campatable with preemption, it means a node without evicting a task has
	   a higher score than those needs to evict a task */
	// 使用预选阶段缓存的 Score，避免重复计算
	// Use cached stored in filter state in order to avoid recalculating.
	return gs.Score
}

// GetIgnoredDevices 返回希望调度器忽略的设备名列表；vgpu 返回空切片表示不忽略任何设备
func (gs *GPUDevices) GetIgnoredDevices() []string {
	// 返回空切片，语义上表示“没有需要忽略的设备”
	return []string{}
}

// AddQueueResource 返回需要覆盖到 queue.allocated 的资源占用（按资源名聚合该 Pod 在各设备上的占用）
func (gs *GPUDevices) AddQueueResource(pod *v1.Pod) map[string]float64 {
	// 设备集合为空时返回空映射
	if gs == nil {
		return map[string]float64{} // 无设备，返回空
	}
	// 打印进入 AddQueueResource 的调试日志
	klog.V(5).InfoS("AddQueueResource", "Name", pod.Name)
	// 初始化返回的资源占用映射
	res := map[string]float64{}
	// 从 Pod 注解读取分配到的设备详情编码串
	ids, ok := pod.Annotations[AssignedIDsAnnotations]
	// 注解不存在则报错并返回空
	if !ok {
		klog.Errorf("pod %s has no annotation volcano.sh/devices-to-allocate", pod.Name) // 记录缺少注解
		return res                                                                       // 返回空映射
	}
	// 解码得到每个容器分配到的设备列表
	podDev := DecodePodDevices(ids)
	// 遍历每个容器的设备分配
	for _, val := range podDev {
		// 遍历该容器内每张设备的占用明细
		for _, deviceused := range val {
			// 遍历节点上的每张物理卡，匹配 UUID
			for _, gsdevice := range gs.Device {
				// 若该物理卡 UUID 包含分配明细中的 UUID，则累加其资源占用
				if strings.Contains(deviceused.UUID, gsdevice.UUID) {
						res[getConfig().ResourceMemoryName] += float64(deviceused.Usedmem * 1000) // 累加显存占用（转换为调度器统计使用的 milli 单位）
					res[getConfig().ResourceCoreName] += float64(deviceused.Usedcores * 1000) // 累加算力占用（换算为毫核）
				}
			}
		}
	}
	// 打印聚合结果的调试日志
	klog.V(4).InfoS("AddQueueResource", "Name=", pod.Name, "res=", res)
	// 返回聚合后的资源占用
	return res
}

// AddResource adds the pod to GPU pool if it is assigned
// AddResource 当 Pod 已被分配（带注解）时，把它登记进对应 GPU 卡的占用账本（缓存记账）
func (gs *GPUDevices) AddResource(pod *v1.Pod) {
	// 设备集合为空则直接返回
	if gs == nil {
		return // 无设备，跳过
	}

	// 调用内部 addResource，基于 Pod 注解完成真正的记账
	gs.addResource(pod.Annotations, pod)
}

// addResource 解析 Pod 的设备分配注解，并把每个容器的占用写入对应物理卡的账本
func (gs *GPUDevices) addResource(annotations map[string]string, pod *v1.Pod) {
	// 从注解读取分配到的设备详情编码串
	ids, ok := annotations[AssignedIDsAnnotations]
	// 注解不存在则报错并返回
	if !ok {
		klog.Errorf("pod %s has no annotation volcano.sh/devices-to-allocate", pod.Name) // 记录缺少注解
		return                                                                           // 跳过记账
	}
	// 解码得到每个容器分配到的设备列表
	podDev := DecodePodDevices(ids)
	// 遍历每个容器的设备分配
	for _, val := range podDev {
		// 遍历该容器内每张设备的占用明细
		for _, deviceused := range val {
			// 遍历节点上的每张物理卡，匹配 UUID
			for index, gsdevice := range gs.Device {
				// 命中该物理卡后，调用共享处理器把占用写入账本
				if strings.Contains(deviceused.UUID, gsdevice.UUID) {
					// 通过共享工厂把显存/算力占用登记到该卡
					err := gs.Sharing.AddPod(gsdevice, deviceused.Usedmem, deviceused.Usedcores, string(pod.UID), deviceused.UUID)
					// 写入成功则补充 PodGroup 键与指标
					if err == nil {
						// 若账本中已有该 Pod 的占用记录，则补全其 PodGroup 键
						if u := gsdevice.PodMap[string(pod.UID)]; u != nil {
							u.PodGroupKey = getPodGroupKey(pod) // 设置所属 PodGroup
						}
						// 更新该卡的 Prometheus 指标
						gs.AddPodMetrics(index, string(pod.UID), pod.Name)
					} else {
						// 写入失败记录错误
						klog.ErrorS(err, "add resource failed") // 记录记账失败
					}
					// 同一张物理卡匹配后即跳出内层遍历
					break
				}
			}
		}
	}
}

// addToPodMap 仅把 Pod 的占用增量写入对应物理卡的 PodMap（用于分配后立即同步内存态，不等 API 回写）
func (gs *GPUDevices) addToPodMap(annotations map[string]string, pod *v1.Pod) {
	// 从注解读取分配到的设备详情编码串
	ids, ok := annotations[AssignedIDsAnnotations]
	// 注解不存在则报错并返回
	if !ok {
		klog.Errorf("pod %s has no annotation volcano.sh/devices-to-allocate", pod.Name) // 记录缺少注解
		return                                                                           // 跳过
	}
	// 解码得到每个容器分配到的设备列表
	podDev := DecodePodDevices(ids)
	// 遍历每个容器的设备分配
	for _, val := range podDev {
		// 遍历该容器内每张设备的占用明细
		for _, deviceused := range val {
			// 遍历节点上的每张物理卡，匹配 UUID
			for _, gsdevice := range gs.Device {
				// 命中该物理卡后，把占用增量累加到 PodMap
				if strings.Contains(deviceused.UUID, gsdevice.UUID) {
					// 取出该 Pod 的 UID
					podUID := string(pod.UID)
					// 检查账本中是否已有该 Pod 的记录
					_, ok := gsdevice.PodMap[podUID]
					// 不存在则先创建一条空占用记录
					if !ok {
						gsdevice.PodMap[podUID] = &GPUUsage{
							UsedMem:     0,                   // 初始显存占用 0
							UsedCore:    0,                   // 初始算力占用 0
							PodGroupKey: getPodGroupKey(pod), // 设置所属 PodGroup
						}
					}

					// 累加该 Pod 在该卡上的显存占用
					gsdevice.PodMap[podUID].UsedMem += deviceused.Usedmem
					// 累加该 Pod 在该卡上的算力占用
					gsdevice.PodMap[podUID].UsedCore += deviceused.Usedcores
					// 同步刷新 PodGroup 键（以最新为准）
					gsdevice.PodMap[podUID].PodGroupKey = getPodGroupKey(pod)
				}
			}
		}
	}
}

// SubResource frees the gpu hold by the pod
// SubResource 当 Pod 被删除/释放时，从对应 GPU 卡的占用账本中扣减其占用（缓存记账回滚）
func (gs *GPUDevices) SubResource(pod *v1.Pod) {
	// 设备集合为空则直接返回
	if gs == nil {
		return // 无设备，跳过
	}
	// 从 Pod 注解读取分配到的设备详情编码串
	ids, ok := pod.Annotations[AssignedIDsAnnotations]
	// 注解不存在则直接返回
	if !ok {
		return // 无分配信息，跳过
	}
	// 解码得到每个容器分配到的设备列表
	podDev := DecodePodDevices(ids)
	// 遍历每个容器的设备分配
	for _, val := range podDev {
		// 遍历该容器内每张设备的占用明细
		for _, deviceused := range val {
			// 遍历节点上的每张物理卡，匹配 UUID
			for index, gsdevice := range gs.Device {
				// 命中该物理卡后，调用共享处理器扣减占用
				if strings.Contains(deviceused.UUID, gsdevice.UUID) {
					// 通过共享工厂从该卡扣减显存/算力占用
					err := gs.Sharing.SubPod(gsdevice, uint(deviceused.Usedmem), uint(deviceused.Usedcores), string(pod.UID), deviceused.UUID)
					// 扣减失败记录错误，成功则更新指标
					if err != nil {
						klog.ErrorS(err, "sub resource failed") // 记录扣减失败
					} else {
						// 扣减成功则更新该卡的 Prometheus 指标
						gs.SubPodMetrics(index, string(pod.UID), pod.Name)
					}
					// 同一张物理卡匹配后即跳出内层遍历
					break
				}
			}
		}
	}
}

// HasDeviceRequest 判断该 Pod 是否向 vgpu 插件申请了 GPU 资源
func (gs *GPUDevices) HasDeviceRequest(pod *v1.Pod) bool {
	// 仅当 vGPU 开关打开且 Pod 含有 vGPU 资源请求时才认为有设备请求
	if VGPUEnable && checkVGPUResourcesInPod(pod) {
		return true // 存在 vGPU 资源请求
	}
	// 否则没有相关请求
	return false
}

// Release 在回滚路径（如 UnPipeline）上调用，把 Pod 从设备缓存中释放（此时可能尚未走 SubResource）
func (gs *GPUDevices) Release(kubeClient kubernetes.Interface, pod *v1.Pod) error {
	// Release is required for rollback paths (e.g. UnPipeline) where NodeInfo
	// does not invoke subResource for Pipelined tasks.
	// 直接复用 SubResource 完成缓存释放
	gs.SubResource(pod)
	// 返回成功
	return nil
}

// FilterNode 是谓词（预选）入口：检查 Pod 能否放入当前节点，并把打分缓存到 gs.Score
func (gs *GPUDevices) FilterNode(pod *v1.Pod, schedulePolicy string) (int, string, error) {
	// 仅当 vGPU 开关打开才执行预选
	if VGPUEnable {
		// 打印进入 vGPU 预选的调试日志
		klog.V(4).Infoln("hami-vgpu DeviceSharing starts filtering pods", pod.Name)
		// 预选：replicate=true 在快照上试算（无副作用），不会污染真实缓存；
		// 算出的打分以标量形式取出，存到真实 gs.Score 供后续 ScoreNode 使用（占用试算本身随快照丢弃）。
		fit, _, score, err := checkNodeGPUSharingPredicateAndScore(pod, gs, true, schedulePolicy)
		// 不满足或发生错误则判定为不可调度，并返回原因
		if err != nil || !fit {
			klog.ErrorS(err, "Failed to fitler node to vgpu task", "pod", pod.Name) // 记录预选失败
			return devices.Unschedulable, "hami-vgpuDeviceSharing error", err       // 返回不可调度码与原因
		}
		// 预选通过，把计算出的打分缓存到节点设备集合，供后续 ScoreNode 使用
		gs.Score = score
		// 打印预选成功的调试日志
		klog.V(4).Infoln("hami-vgpu DeviceSharing successfully filters pods")
	}
	// 返回成功状态码与空原因
	return devices.Success, "", nil
}

// Allocate 是分配（绑定）动作：挑卡、把分配结果编码写回 Pod 注解，并同步内存态缓存
func (gs *GPUDevices) Allocate(kubeClient kubernetes.Interface, pod *v1.Pod) error {
	// 仅当 vGPU 开关打开才执行分配
	if VGPUEnable {
		// 打印进入分配的调试日志
		klog.V(4).Infoln("hami-vgpu DeviceSharing:Into AllocateToPod", pod.Name)
		// 若该 Pod 已在当前节点分配过（幂等保护），直接跳过避免重复分配
		if alreadyAssignedOnNode(pod, gs.Name) {
			klog.V(4).InfoS("hami-vgpu DeviceSharing: skip duplicate AllocateToPod",
				"pod", pod.Name, "namespace", pod.Namespace, "node", gs.Name) // 记录跳过重复分配
			return nil // 直接返回成功
		}
		// 绑定：replicate=false 直接在真实缓存上分配并真正占用资源（此次占用会被持久化提交）。
		fit, device, _, err := checkNodeGPUSharingPredicateAndScore(pod, gs, false, SchedulePolicy)
		// 不满足或发生错误则分配失败，返回错误
		if err != nil || !fit {
			klog.ErrorS(err, "Failed to allocate vgpu task", "pod", pod.Name) // 记录分配失败
			return err                                                        // 返回错误
		}
		// 若启用了节点锁，则对节点加锁防止并发分配冲突
		if NodeLockEnable {
			nodelock.UseClient(kubeClient)               // 设置节点锁使用的 kube 客户端
			err = nodelock.LockNode(gs.Name, DeviceName) // 对该节点加名为 hamivgpu 的锁
			// 加锁失败则返回错误
			if err != nil {
				return errors.Errorf("node %s locked for %s hamivgpu lockname %s", gs.Name, pod.Name, err.Error()) // 返回加锁失败错误
			}
		}

		// 构造需要写回 Pod 的注解集合
		annotations := make(map[string]string)
		// 记录分配到的节点名
		annotations[AssignedNodeAnnotations] = gs.Name
		// 记录分配时间戳
		annotations[AssignedTimeAnnotations] = strconv.FormatInt(time.Now().Unix(), 10)
		// 记录分配到的设备详情编码串
		annotations[AssignedIDsAnnotations] = encodePodDevices(device)
		// 同时写入“待分配”快照（与分配结果一致）
		annotations[AssignedIDsToAllocateAnnotations] = annotations[AssignedIDsAnnotations]

		// 记录设备绑定阶段为 allocating
		annotations[DeviceBindPhase] = "allocating"
		// 记录绑定时间戳
		annotations[BindTimeAnnotations] = strconv.FormatInt(time.Now().Unix(), 10)
		// Keep in-memory pod object in sync so rollback paths (UnPipeline ->
		// Deallocate -> Release) can see allocated IDs before apiserver watch
		// catches up.
		// 先把分配结果同步到内存中的 Pod 对象注解，保证回滚路径在 API 回写前即可读到
		if pod.Annotations == nil {
			pod.Annotations = map[string]string{} // 初始化注解映射
		}
		// 把上面构造的注解合并进 Pod 内存对象
		for k, v := range annotations {
			pod.Annotations[k] = v // 覆盖/写入各个注解键
		}
		// To avoid that the pod allocated info updating latency, add it first
		// 为避免分配信息回写延迟，先同步到调度器内存态的设备账本
		gs.addToPodMap(annotations, pod)
		// 通过 API 把注解真正补丁到 Pod 对象
		err = patchPodAnnotations(kubeClient, pod, annotations)
		// 补丁失败则直接返回错误
		if err != nil {
			return err // 返回补丁失败错误
		}

		// 记录分配成功的调试日志
		klog.V(3).Infoln("DeviceSharing:Allocate Success")
	}
	// 分配流程结束，返回成功
	return nil
}

// DeepCopy returns a deep copy of GPUDevices for use in dry-run simulation.
// DeepCopy 返回 GPUDevices 的设备结构副本，供抢占/拓扑感知等 dry-run 模拟使用；当前 MIG UsageList 内层 UsedIndex 切片仍共享底层存储
func (gs *GPUDevices) DeepCopy() interface{} {
	// 原对象为 nil 时直接返回 nil
	if gs == nil {
		return nil // 空对象无需拷贝
	}
	// 构造新的 GPUDevices 外壳，复制基础字段并预分配卡映射
	cp := &GPUDevices{
		Name:    gs.Name,                                  // 复制节点名
		Mode:    gs.Mode,                                  // 复制共享模式
		Score:   gs.Score,                                 // 复制缓存打分
		Sharing: gs.Sharing,                               // 共享处理器为接口，直接复用（无需深拷贝）
		Device:  make(map[int]*GPUDevice, len(gs.Device)), // 预分配卡映射容量
	}
	// 逐卡复制设备结构并重建 PodMap
	for id, dev := range gs.Device {
		// 构造单卡的深拷贝，复制基础字段并新建 Pod 映射
		newDev := &GPUDevice{
			ID:       dev.ID,       // 复制卡序号
			Node:     dev.Node,     // 复制所属节点
			UUID:     dev.UUID,     // 复制设备 UUID
			Memory:   dev.Memory,   // 复制总显存
			Number:   dev.Number,   // 复制共享上限
			Type:     dev.Type,     // 复制设备型号
			Health:   dev.Health,   // 复制健康状态
			UsedNum:  dev.UsedNum,  // 复制已分配数量
			UsedMem:  dev.UsedMem,  // 复制已分配显存
			UsedCore: dev.UsedCore, // 复制已分配算力
			// 复制 MIG 占用账本的 Index 与 UsageList 外层切片；内部 UsedIndex 切片仍由 copy 浅复制
			MigUsage: deviceconfig.MigInUse{
				Index:     dev.MigUsage.Index,                                   // 复制当前使用中的 MIG 组索引
				UsageList: make(deviceconfig.MIGS, len(dev.MigUsage.UsageList)), // 预分配 MIG 占用列表
			},
			PodMap: make(map[string]*GPUUsage, len(dev.PodMap)), // 预分配 Pod 占用账本
		}
		// 拷贝 MIG 占用列表内容
		copy(newDev.MigUsage.UsageList, dev.MigUsage.UsageList)
		// 若该卡有 MIG 模板，则深拷贝模板切片
		if len(dev.MigTemplate) > 0 {
			// 预分配模板切片
			newDev.MigTemplate = make([]deviceconfig.Geometry, len(dev.MigTemplate))
			// 逐组拷贝模板（含实例列表）
			for i, g := range dev.MigTemplate {
				// 构造新的几何模板，复制组名并预分配实例列表
				ng := deviceconfig.Geometry{
					Group:     g.Group,                                            // 复制组名
					Instances: make([]deviceconfig.MigTemplate, len(g.Instances)), // 预分配实例列表
				}
				// 拷贝实例列表
				copy(ng.Instances, g.Instances)
				// 放入新模板
				newDev.MigTemplate[i] = ng
			}
		}
		// 逐条拷贝 Pod 占用账本（值拷贝 GPUUsage）
		for uid, usage := range dev.PodMap {
			u := *usage             // 解引用得到值拷贝
			newDev.PodMap[uid] = &u // 以新指针放入新账本
		}
		// 把拷贝好的卡放入新集合
		cp.Device[id] = newDev
	}
	// 返回设备结构副本
	return cp
}

// alreadyAssignedOnNode 判断 Pod 是否已经在指定节点上完成过分配（幂等保护）
func alreadyAssignedOnNode(pod *v1.Pod, nodeName string) bool {
	// Pod、注解或节点名为空时直接返回 false
	if pod == nil || pod.Annotations == nil || nodeName == "" {
		return false // 参数不足，视为未分配
	}

	// 当 Pod 已标记分配到该节点且带有设备分配详情时，认为已分配
	return pod.Annotations[AssignedNodeAnnotations] == nodeName &&
		pod.Annotations[AssignedIDsAnnotations] != ""
}
