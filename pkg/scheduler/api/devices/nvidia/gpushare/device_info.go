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

package gpushare // gpushare 包实现了基于显存/整卡数量的粗粒度 GPU 共享调度设备插件

import (
	"context"                                     // context 用于向 Kubernetes API 传递请求上下文（如超时、取消）
	"fmt"                                         // fmt 用于格式化错误信息与字符串
	"github.com/pkg/errors"                       // errors 提供带堆栈的错误包装
	v1 "k8s.io/api/core/v1"                       // v1 是 Kubernetes 核心 API（Pod、Node 等）
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1" // metav1 提供 PatchOptions 等元数据类型
	"k8s.io/apimachinery/pkg/types"               // types 提供 JSONPatchType 等补丁类型常量
	"k8s.io/client-go/kubernetes"                 // kubernetes 是 Kubernetes 客户端集，用于调用 API
	"k8s.io/klog/v2"                              // klog 是 Kubernetes 日志库

	"volcano.sh/volcano/pkg/scheduler/api/devices"           // devices 定义调度器设备插件通用接口与状态码
	"volcano.sh/volcano/pkg/scheduler/plugins/util/nodelock" // nodelock 提供节点级分布式锁工具
)

// GPUDevice include gpu id, memory and the pods that are sharing it.
// GPUDevice 描述单张物理 GPU 卡的共享状态：卡号、显存容量，以及正在共享这张卡的 Pod 集合
type GPUDevice struct {
	// GPU ID
	// GPU ID 是该 GPU 卡在当前节点上的唯一序号（从 0 开始）
	ID int
	// The pods that are sharing this GPU
	// PodMap 以 Pod UID 为键，记录当前正在使用（共享）这张 GPU 卡的 Pod 对象
	PodMap map[string]*v1.Pod
	// memory per card
	// Memory 是这张 GPU 卡的总显存容量（单位 MiB）
	Memory uint
}

// GPUDevices 是某个节点上所有 GPU 卡的集合，是 gpushare 插件在调度器缓存中的核心对象
type GPUDevices struct {
	// Name 是所属节点的名称
	Name string

	// Device 以卡序号为键，保存该节点上每一张 GPU 卡的共享状态
	Device map[int]*GPUDevice
}

// NewGPUDevice creates a device
// NewGPUDevice 构造并返回一个单张 GPU 卡的初始状态对象
func NewGPUDevice(id int, mem uint) *GPUDevice {
	// 返回填充了 ID、显存容量，以及空 Pod 映射的 GPUDevice 指针
	return &GPUDevice{
		ID:     id,                   // 设置 GPU 卡序号
		Memory: mem,                  // 设置该卡显存总量
		PodMap: map[string]*v1.Pod{}, // 初始化空的共享 Pod 集合
	}
}

// NewGPUDevices 根据节点 Capacity 与健康状态注解构造该节点的全部 GPU 卡集合；若节点未启用 gpushare 则返回 nil
func NewGPUDevices(name string, node *v1.Node) *GPUDevices {
	// 节点为空直接返回 nil，无法构建设备集合
	if node == nil {
		return nil // 没有节点信息，放弃构造
	}
	// 读取节点 Capacity 中的 GPU 显存扩展资源
	memory, ok := node.Status.Capacity[VolcanoGPUResource]
	// 节点未声明 gpu-memory 资源，说明未启用显存共享，返回 nil
	if !ok {
		return nil // 缺少 volcano.sh/gpu-memory 容量，无法构建设备
	}
	// 取出显存总量的数值（Quantity 转为 int64）
	totalMemory := memory.Value()

	// 读取节点 Capacity 中的 GPU 整卡数量扩展资源
	res, ok := node.Status.Capacity[VolcanoGPUNumber]
	// 节点未声明 gpu-number 资源，返回 nil
	if !ok {
		return nil // 缺少 volcano.sh/gpu-number 容量，无法构建设备
	}
	// 取出整卡数量数值
	gpuNumber := res.Value()
	// 整卡数量为 0 是非法的，记录告警并返回 nil
	if gpuNumber == 0 {
		klog.Warningf("invalid %s=%s", VolcanoGPUNumber, res.String()) // 打印非法配置告警
		return nil                                                     // 数量非法，放弃构造
	}

	// 按显存总量除以卡数，得到每张卡的显存容量（平均分配）
	memoryPerCard := uint(totalMemory / gpuNumber)
	// 创建节点级 GPU 设备集合对象
	gpudevices := GPUDevices{}
	// 初始化卡序号到卡对象的映射
	gpudevices.Device = make(map[int]*GPUDevice)
	// 记录所属节点名称
	gpudevices.Name = name
	// 为每张卡构造一个 GPUDevice 并落入映射
	for i := 0; i < int(gpuNumber); i++ {
		gpudevices.Device[i] = NewGPUDevice(i, memoryPerCard) // 序号 i，显存为单卡容量
	}
	// 找出节点上被标记为不健康的 GPU 卡
	unhealthyGPUs := getUnhealthyGPUs(&gpudevices, node)
	// 从设备集合中剔除这些不健康的卡
	for i := range unhealthyGPUs {
		klog.V(4).Infof("delete unhealthy gpu id %d from GPUDevices", unhealthyGPUs[i]) // 记录剔除日志
		delete(gpudevices.Device, unhealthyGPUs[i])                                     // 删除该卡
	}
	// 返回构造完成的节点 GPU 设备集合
	return &gpudevices
}

// GetIgnoredDevices return device names which wish vc-scheduler to ignore
// GetIgnoredDevices 返回希望调度器忽略的设备名列表；gpushare 返回空设备名哨兵，实际不忽略任何资源
func (gs *GPUDevices) GetIgnoredDevices() []string {
	// 返回仅含空字符串的切片；IgnoredDevicesList 对空字符串不做匹配，因此不会忽略任何资源
	return []string{""}
}

// AddResource adds the pod to GPU pool if it is assigned
// AddResource 当 Pod 已被分配（带注解）时，把它登记进对应 GPU 卡的共享 Pod 集合（缓存记账）
func (gs *GPUDevices) AddResource(pod *v1.Pod) {
	// 取出该 Pod 申请的 GPU 显存量
	gpuRes := getGPUMemoryOfPod(pod)
	// 取出该 Pod 申请的 GPU 整卡数量
	gpuNumRes := getGPUNumberOfPod(pod)
	// 只有当 Pod 确实申请了显存或整卡时才需要记账
	if gpuRes > 0 || gpuNumRes > 0 {
		// 从 Pod 注解中读出被分配到的 GPU 卡序号列表
		ids := GetGPUIndex(pod)
		// 遍历每个被分配到的卡序号
		for _, id := range ids {
			// 若对应卡存在，则把该 Pod 记入其共享集合
			if dev := gs.Device[id]; dev != nil {
				dev.PodMap[string(pod.UID)] = pod // 以 Pod UID 为键登记
			}
		}
	}
}

// SubResource frees the gpu hold by the pod
// SubResource 当 Pod 被删除/释放时，从对应 GPU 卡的共享集合中移除它（缓存记账回滚）
func (gs *GPUDevices) SubResource(pod *v1.Pod) {
	// 取出该 Pod 申请的 GPU 显存量
	gpuRes := getGPUMemoryOfPod(pod)
	// 取出该 Pod 申请的 GPU 整卡数量
	gpuNumRes := getGPUNumberOfPod(pod)
	// 只有当 Pod 申请了显存或整卡时才需要清理
	if gpuRes > 0 || gpuNumRes > 0 {
		// 从 Pod 注解中读出被分配到的 GPU 卡序号列表
		ids := GetGPUIndex(pod)
		// 遍历每个卡序号
		for _, id := range ids {
			// 若对应卡存在，则从共享集合中删除该 Pod
			if dev := gs.Device[id]; dev != nil {
				delete(dev.PodMap, string(pod.UID)) // 移除该 Pod 的记账
			}
		}
	}
}

// HasDeviceRequest 判断该 Pod 是否向 gpushare 插件申请了 GPU 资源（用于决定要不要走本插件预选）
func (gs *GPUDevices) HasDeviceRequest(pod *v1.Pod) bool {
	// 若显存共享已启用且 Pod 申请了显存，或整卡共享已启用且 Pod 申请了整卡，则视为有设备请求
	if GpuSharingEnable && getGPUMemoryOfPod(pod) > 0 ||
		GpuNumberEnable && getGPUNumberOfPod(pod) > 0 {
		return true // 存在 gpushare 资源请求
	}
	// 否则没有相关请求
	return false
}

// AddQueueResource 返回需要覆盖到 queue.allocated 的资源占用；gpushare 不实现队列级占用，返回空
func (gs *GPUDevices) AddQueueResource(pod *v1.Pod) map[string]float64 {
	// 返回空的资源占用映射，表示本插件不向队列占用上报额外资源
	return map[string]float64{}
}

// Release 在回滚路径（如 UnPipeline）上调用，用于把 Pod 从设备缓存中释放（此时可能尚未走 SubResource）
func (gs *GPUDevices) Release(kubeClient kubernetes.Interface, pod *v1.Pod) error {
	// 从 Pod 注解读出分配到的 GPU 卡序号
	ids := GetGPUIndex(pod)
	// 构造移除 GPU 注解的补丁（同时移除 predicate-time 与 gpu-index）
	patch := RemoveGPUIndexPatch()
	// 调用 Kubernetes API 对该 Pod 执行 JSON Patch，清除调度分配的注解
	_, err := kubeClient.CoreV1().Pods(pod.Namespace).Patch(context.TODO(), pod.Name, types.JSONPatchType, []byte(patch), metav1.PatchOptions{})
	// 补丁执行失败则包装错误返回
	if err != nil {
		return errors.Errorf("patch pod %s failed with patch %s: %v", pod.Name, patch, err) // 返回带上下文的错误
	}

	// 遍历被分配到的卡序号，从缓存的设备集合中移除该 Pod
	for _, id := range ids {
		// 若对应卡存在，则删除其共享集合中该 Pod 的记账
		if dev, ok := gs.Device[id]; ok {
			delete(dev.PodMap, string(pod.UID)) // 移除 Pod 记账
		}
	}

	// 记录释放完成的调试日志
	klog.V(4).Infof("predicates with gpu sharing, update pod %s/%s deallocate from node [%s]", pod.Namespace, pod.Name, gs.Name)
	// 释放成功
	return nil
}

// FilterNode 是谓词（预选）入口：检查 Pod 是否能放入当前节点，返回状态码、原因与错误
func (gs *GPUDevices) FilterNode(pod *v1.Pod, schedulePolicy string) (int, string, error) {
	// 打印进入预选的调试日志
	klog.V(4).Infoln("DeviceSharing:Into FitInPod", pod.Name)
	// 若显存共享开关打开，则执行显存维度的预选
	if GpuSharingEnable {
		// 检查 Pod 的显存请求能否在当前节点得到满足
		fit, err := checkNodeGPUSharingPredicate(pod, gs)
		// 不满足或发生错误则判定为不可调度，并返回原因
		if err != nil || !fit {
			klog.Errorln("deviceSharing err=", err.Error())                            // 记录失败日志
			return devices.Unschedulable, fmt.Sprintf("GpuShare %s", err.Error()), err // 返回不可调度码与原因
		}
	}
	// 若整卡共享开关打开，则执行整卡数量维度的预选
	if GpuNumberEnable {
		// 检查 Pod 的整卡请求能否在当前节点得到满足
		fit, err := checkNodeGPUNumberPredicate(pod, gs)
		// 不满足或发生错误则判定为不可调度，并返回原因
		if err != nil || !fit {
			klog.Errorln("deviceSharing err=", err.Error())                             // 记录失败日志
			return devices.Unschedulable, fmt.Sprintf("GpuNumber %s", err.Error()), err // 返回不可调度码与原因
		}
	}
	// 所有启用的维度都通过，预选成功
	klog.V(4).Infoln("DeviceSharing:FitInPod successed")
	// 返回成功状态码与空原因
	return devices.Success, "", nil
}

// GetStatus 用于调试与监控，gpushare 暂未实现有意义的设备状态，返回空字符串
func (gs *GPUDevices) GetStatus() string {
	// 返回空字符串，表示无额外状态信息
	return ""
}

// DeepCopy returns a deep copy of GPUDevices for use in dry-run simulation.
// DeepCopy 返回 GPUDevices 的独立设备副本，供抢占/拓扑感知等 dry-run 模拟使用；Pod 指针仍与原对象共享
func (gs *GPUDevices) DeepCopy() interface{} {
	// 原对象为 nil 时直接返回 nil
	if gs == nil {
		return nil // 空对象无需拷贝
	}
	// 构造新的 GPUDevices 外壳，复制名称并按原大小预分配卡映射
	cp := &GPUDevices{
		Name:   gs.Name,                                  // 复制节点名
		Device: make(map[int]*GPUDevice, len(gs.Device)), // 预分配卡映射容量
	}
	// 逐卡复制设备结构并重建 PodMap
	for id, dev := range gs.Device {
		// 构造单卡副本，复制基础字段并新建 Pod 映射
		newDev := &GPUDevice{
			ID:     dev.ID,                                    // 复制卡序号
			Memory: dev.Memory,                                // 复制显存容量
			PodMap: make(map[string]*v1.Pod, len(dev.PodMap)), // 预分配 Pod 映射
		}
		// 复制每个共享 Pod 的引用
		for uid, pod := range dev.PodMap {
			newDev.PodMap[uid] = pod // 复制 Pod 指针（此处为浅拷贝引用，满足模拟需求）
		}
		// 把拷贝好的卡放入新集合
		cp.Device[id] = newDev
	}
	// 返回设备结构副本
	return cp
}

// ScoreNode 由 devicescore 插件在打分时调用；gpushare 不做设备级打分，固定返回 0
func (gs *GPUDevices) ScoreNode(pod *v1.Pod, schedulePolicy string) float64 {
	// 返回 0 表示本插件不参与节点打分
	return 0
}

// Allocate 是分配（绑定）动作：把 Pod 真正落到节点上，并通过补丁把分配结果写回 Pod 注解
func (gs *GPUDevices) Allocate(kubeClient kubernetes.Interface, pod *v1.Pod) error {
	// 打印进入分配流程的调试日志
	klog.V(4).Infoln("DeviceSharing:Into AllocateToPod", pod.Name)
	// 若 Pod 申请了显存，则按显存维度分配一张卡
	if getGPUMemoryOfPod(pod) > 0 {
		// 显存分配路径若启用了节点锁，则先对节点加锁防止并发分配冲突
		if NodeLockEnable {
			nodelock.UseClient(kubeClient)           // 设置节点锁使用的 kube 客户端
			err := nodelock.LockNode(gs.Name, "gpu") // 对该节点加名为 gpu 的锁
			// 加锁失败则直接返回错误
			if err != nil {
				return errors.Errorf("node %s locked for lockname gpushare %s", gs.Name, err.Error()) // 返回加锁失败错误
			}
		}
		// 按显存请求挑选一张空闲显存足够的卡，返回候选序号列表
		ids := predicateGPUbyMemory(pod, gs)
		// 没有可用卡则报错返回
		if len(ids) == 0 {
			return errors.Errorf("the node %s can't place the pod %s in ns %s", pod.Spec.NodeName, pod.Name, pod.Namespace) // 返回无可用卡错误
		}
		// 显存共享模式取第一张候选卡即可（单卡分配）
		id := ids[0]
		// 构造写入 Pod 注解的补丁，记录分配到的卡序号
		patch := AddGPUIndexPatch([]int{id})
		// 调用 API 对 Pod 执行 JSON Patch，写入 gpu-index 等注解
		pod, err := kubeClient.CoreV1().Pods(pod.Namespace).Patch(context.TODO(), pod.Name, types.JSONPatchType, []byte(patch), metav1.PatchOptions{})
		// 补丁失败则报错返回
		if err != nil {
			return errors.Errorf("patch pod %s failed with patch %s: %v", pod.Name, patch, err) // 返回补丁失败错误
		}
		// 从缓存中取出对应卡对象
		dev, ok := gs.Device[id]
		// 卡不存在则报错返回
		if !ok {
			return errors.Errorf("failed to get GPU %d from node %s", id, gs.Name) // 返回取卡失败错误
		}
		// 把该 Pod 登记进这张卡的共享集合（更新缓存）
		dev.PodMap[string(pod.UID)] = pod
		// 记录分配完成的调试日志
		klog.V(4).Infof("predicates with gpu sharing, update pod %s/%s allocate to node [%s]", pod.Namespace, pod.Name, gs.Name)
	}
	// 若 Pod 申请了整卡，则按整卡数量维度分配若干张卡
	if getGPUNumberOfPod(pod) > 0 {
		// 按整卡请求挑选若干张空闲卡，返回候选序号列表
		ids := predicateGPUbyNumber(pod, gs)
		// 没有足够空闲卡则报错返回
		if len(ids) == 0 {
			return errors.Errorf("the node %s can't place the pod %s in ns %s", pod.Spec.NodeName, pod.Name, pod.Namespace) // 返回无可用卡错误
		}
		// 构造写入 Pod 注解的补丁，记录分配到的多张卡序号
		patch := AddGPUIndexPatch(ids)
		// 调用 API 对 Pod 执行 JSON Patch，写入 gpu-index 等注解
		pod, err := kubeClient.CoreV1().Pods(pod.Namespace).Patch(context.TODO(), pod.Name, types.JSONPatchType, []byte(patch), metav1.PatchOptions{})
		// 补丁失败则报错返回
		if err != nil {
			return errors.Errorf("patch pod %s failed with patch %s: %v", pod.Name, patch, err) // 返回补丁失败错误
		}
		// 遍历每张被分配到的卡
		for _, id := range ids {
			// 从缓存中取出对应卡对象
			dev, ok := gs.Device[id]
			// 卡不存在则报错返回
			if !ok {
				return errors.Errorf("failed to get GPU %d from node %s", id, gs.Name) // 返回取卡失败错误
			}
			// 把该 Pod 登记进这张卡的共享集合（更新缓存）
			dev.PodMap[string(pod.UID)] = pod
		}
		// 记录整卡分配完成的调试日志
		klog.V(4).Infof("predicates with gpu number, update pod %s/%s allocate to node [%s]", pod.Namespace, pod.Name, gs.Name)
	}
	// 分配流程结束，返回成功
	return nil
}
