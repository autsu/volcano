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
	"github.com/prometheus/client_golang/prometheus"          // prometheus 是监控指标库
	"github.com/prometheus/client_golang/prometheus/promauto" // promauto 自动把收集器注册到默认注册表
)

const (
	// VolcanoSubSystemName - subsystem name in prometheus used by volcano
	// VolcanoSubSystemName 是 Prometheus 中 volcano 子系统的名称
	VolcanoSubSystemName = "volcano"

	// OnSessionOpen label
	// OnSessionOpen 是会话开启相关的标签名
	OnSessionOpen = "OnSessionOpen"

	// OnSessionClose label
	// OnSessionClose 是会话关闭相关的标签名
	OnSessionClose = "OnSessionClose"
)

// 以下变量定义了一组按 devID/NodeName（部分含 podName）分维度的 Prometheus 指标，用于观测 vGPU 卡的资源占用
var (
	// VGPUDevicesSharedNumber 记录每张卡上共享的 vGPU 任务数量
	VGPUDevicesSharedNumber = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: VolcanoSubSystemName,                         // 归属 volcano 子系统
			Name:      "vgpu_device_shared_number",                  // 指标名：共享任务数
			Help:      "The number of vgpu tasks sharing this card", // 指标说明：本卡上共享的 vGPU 任务数
		},
		[]string{"devID", "NodeName"}, // 维度：设备 ID 与节点名
	)
	// VGPUDevicesAllocatedMemory 记录每张卡已分配的 vGPU 显存
	VGPUDevicesAllocatedMemory = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: VolcanoSubSystemName,                               // 归属 volcano 子系统
			Name:      "vgpu_device_allocated_memory",                     // 指标名：已分配显存
			Help:      "The number of vgpu memory allocated in this card", // 指标说明：本卡已分配的 vGPU 显存
		},
		[]string{"devID", "NodeName"}, // 维度：设备 ID 与节点名
	)
	// VGPUDevicesAllocatedCores 记录每张卡已分配的 vGPU 算力百分比
	VGPUDevicesAllocatedCores = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: VolcanoSubSystemName,                                         // 归属 volcano 子系统
			Name:      "vgpu_device_allocated_cores",                                // 指标名：已分配算力
			Help:      "The percentage of gpu compute cores allocated in this card", // 指标说明：本卡已分配的算力百分比
		},
		[]string{"devID", "NodeName"}, // 维度：设备 ID 与节点名
	)
	// VGPUDevicesMemoryTotal 记录每张卡的总显存上限
	VGPUDevicesMemoryTotal = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: VolcanoSubSystemName,                             // 归属 volcano 子系统
			Name:      "vgpu_device_memory_limit",                       // 指标名：显存上限
			Help:      "The number of total device memory in this card", // 指标说明：本卡总显存
		},
		[]string{"devID", "NodeName"}, // 维度：设备 ID 与节点名
	)
	// VGPUPodMemoryAllocated 记录某个 Pod 在某张卡上分配的显存
	VGPUPodMemoryAllocated = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: VolcanoSubSystemName,                                 // 归属 volcano 子系统
			Name:      "vgpu_device_memory_allocation_for_a_certain_pod",    // 指标名：单 Pod 显存分配
			Help:      "The vgpu device memory allocated for a certain pod", // 指标说明：某 Pod 的 vGPU 显存分配
		},
		[]string{"devID", "NodeName", "podName"}, // 维度：设备 ID、节点名、Pod 名
	)
	// VGPUPodCoreAllocated 记录某个 Pod 在某张卡上分配的算力百分比
	VGPUPodCoreAllocated = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: VolcanoSubSystemName,                               // 归属 volcano 子系统
			Name:      "vgpu_device_core_allocation_for_a_certain_pod",    // 指标名：单 Pod 算力分配
			Help:      "The vgpu device core allocated for a certain pod", // 指标说明：某 Pod 的 vGPU 算力分配
		},
		[]string{"devID", "NodeName", "podName"}, // 维度：设备 ID、节点名、Pod 名
	)
)

// GetStatus 用于调试与监控，vgpu 暂未实现有意义的设备状态，返回空字符串
func (gs *GPUDevices) GetStatus() string {
	// 返回空字符串，表示无额外状态信息
	return ""
}

// ResetDeviceMetrics 重置某张卡的指标基线（在节点设备注册时调用）
func ResetDeviceMetrics(UUID string, nodeName string, memory float64) {
	// 设置该卡总显存上限指标
	VGPUDevicesMemoryTotal.WithLabelValues(UUID, nodeName).Set(memory)
	// 重置共享任务数为 0
	VGPUDevicesSharedNumber.WithLabelValues(UUID, nodeName).Set(0)
	// 重置已分配算力为 0
	VGPUDevicesAllocatedCores.WithLabelValues(UUID, nodeName).Set(0)
	// 重置已分配显存为 0
	VGPUDevicesAllocatedMemory.WithLabelValues(UUID, nodeName).Set(0)

	// 删除该卡下所有按 Pod 维度的显存指标
	VGPUPodMemoryAllocated.DeletePartialMatch(prometheus.Labels{"devID": UUID})
	// 删除该卡下所有按 Pod 维度的算力指标
	VGPUPodCoreAllocated.DeletePartialMatch(prometheus.Labels{"devID": UUID})
}

// AddPodMetrics 在某个 Pod 占用被登记后，更新该卡与 Pod 的 Prometheus 指标
func (gs *GPUDevices) AddPodMetrics(index int, podUID, podName string) {
	// 取出对应卡的 UUID 与节点名
	UUID := gs.Device[index].UUID
	NodeName := gs.Device[index].Node
	// 取出该 Pod 在卡上的占用记录
	usage := gs.Device[index].PodMap[podUID]
	// 若占用记录为空，仅刷新卡级聚合指标
	if usage == nil {
		// 更新卡级共享任务数
		VGPUDevicesSharedNumber.WithLabelValues(UUID, NodeName).Set(float64(gs.Device[index].UsedNum))
		// 更新卡级已分配算力
		VGPUDevicesAllocatedCores.WithLabelValues(UUID, NodeName).Set(float64(gs.Device[index].UsedCore))
		// 更新卡级已分配显存
		VGPUDevicesAllocatedMemory.WithLabelValues(UUID, NodeName).Set(float64(gs.Device[index].UsedMem))
		// 提前返回
		return
	}
	// 更新该 Pod 的显存分配指标
	VGPUPodMemoryAllocated.WithLabelValues(UUID, NodeName, podName).Set(float64(usage.UsedMem))
	// 更新该 Pod 的算力分配指标
	VGPUPodCoreAllocated.WithLabelValues(UUID, NodeName, podName).Set(float64(usage.UsedCore))
	// 卡级共享任务数 +1
	VGPUDevicesSharedNumber.WithLabelValues(UUID, NodeName).Inc()
	// 更新卡级已分配算力
	VGPUDevicesAllocatedCores.WithLabelValues(UUID, NodeName).Set(float64(gs.Device[index].UsedCore))
	// 更新卡级已分配显存
	VGPUDevicesAllocatedMemory.WithLabelValues(UUID, NodeName).Set(float64(gs.Device[index].UsedMem))
}

// SubPodMetrics 在某个 Pod 占用被释放后，更新该卡与 Pod 的 Prometheus 指标（-1 或清理）
func (gs *GPUDevices) SubPodMetrics(index int, podUID, podName string) {
	// 取出对应卡的 UUID 与节点名
	UUID := gs.Device[index].UUID
	NodeName := gs.Device[index].Node
	// 取出该 Pod 在卡上的占用记录
	usage := gs.Device[index].PodMap[podUID]
	// 若占用记录为空，仅刷新卡级聚合指标
	if usage == nil {
		// 删除该 Pod 的显存指标
		VGPUPodMemoryAllocated.DeleteLabelValues(UUID, NodeName, podName)
		// 删除该 Pod 的算力指标
		VGPUPodCoreAllocated.DeleteLabelValues(UUID, NodeName, podName)
		// 更新卡级共享任务数
		VGPUDevicesSharedNumber.WithLabelValues(UUID, NodeName).Set(float64(gs.Device[index].UsedNum))
		// 更新卡级已分配算力
		VGPUDevicesAllocatedCores.WithLabelValues(UUID, NodeName).Set(float64(gs.Device[index].UsedCore))
		// 更新卡级已分配显存
		VGPUDevicesAllocatedMemory.WithLabelValues(UUID, NodeName).Set(float64(gs.Device[index].UsedMem))
		// 提前返回
		return
	}
	// 更新该 Pod 的显存分配指标
	VGPUPodMemoryAllocated.WithLabelValues(UUID, NodeName, podName).Set(float64(usage.UsedMem))
	// 更新该 Pod 的算力分配指标
	VGPUPodCoreAllocated.WithLabelValues(UUID, NodeName, podName).Set(float64(usage.UsedCore))
	// 若 Pod 显存占用已归零，则从账本移除该 Pod 并清理其指标
	if usage.UsedMem == 0 {
		// 从卡账本删除该 Pod 记录
		delete(gs.Device[index].PodMap, podUID)
		// 删除该 Pod 的显存指标
		VGPUPodMemoryAllocated.DeleteLabelValues(UUID, NodeName, podName)
		// 删除该 Pod 的算力指标
		VGPUPodCoreAllocated.DeleteLabelValues(UUID, NodeName, podName)
	}
	// 卡级共享任务数 -1
	VGPUDevicesSharedNumber.WithLabelValues(UUID, NodeName).Dec()
	// 更新卡级已分配算力
	VGPUDevicesAllocatedCores.WithLabelValues(UUID, NodeName).Set(float64(gs.Device[index].UsedCore))
	// 更新卡级已分配显存
	VGPUDevicesAllocatedMemory.WithLabelValues(UUID, NodeName).Set(float64(gs.Device[index].UsedMem))
}
