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

package vgpu // vgpu 包实现了基于 HAMICore / MIG 的细粒度 GPU 虚拟化共享调度设备插件

const (
	// DeviceName used to indicate this device
	// DeviceName 是该设备插件的标识名，调度器用它区分不同的设备类型
	DeviceName = "hamivgpu"

	// GPUInUse 是 Pod 注解的键，声明“只允许使用”的 GPU 型号白名单（逗号分隔）
	GPUInUse = "nvidia.com/use-gputype"
	// GPUNoUse 是 Pod 注解的键，声明“禁止使用”的 GPU 型号黑名单（逗号分隔）
	GPUNoUse = "nvidia.com/nouse-gputype"
	// AssignedTimeAnnotations 是 Pod 注解的键，记录被分配（allocate）的时间戳
	AssignedTimeAnnotations = "volcano.sh/vgpu-time"
	// AssignedIDsAnnotations 是 Pod 注解的键，记录分配给 Pod 的设备详情编码串（核心分配结果）
	AssignedIDsAnnotations = "volcano.sh/vgpu-ids-new"
	// AssignedIDsToAllocateAnnotations 是 Pod 注解的键，记录“待分配”的设备详情（与分配结果一致的快照）
	AssignedIDsToAllocateAnnotations = "volcano.sh/devices-to-allocate"
	// AssignedNodeAnnotations 是 Pod 注解的键，记录被分配到的节点名
	AssignedNodeAnnotations = "volcano.sh/vgpu-node"
	// BindTimeAnnotations 是 Pod 注解的键，记录绑定（bind）的时间戳
	BindTimeAnnotations = "volcano.sh/bind-time"
	// DeviceBindPhase 是 Pod 注解的键，记录设备绑定阶段（如 allocating）
	DeviceBindPhase = "volcano.sh/bind-phase"

	// NvidiaGPUDevice 表示 NVIDIA 设备类型标识
	NvidiaGPUDevice = "NVIDIA"

	// PredicateTime is the key of predicate time
	// PredicateTime 是 Pod 注解的键，记录谓词（预选）发生的时间戳
	PredicateTime = "volcano.sh/predicate-time"
	// GPUIndex is the key of gpu index
	// GPUIndex 是 Pod 注解的键，记录分配到的 GPU 卡序号列表（与 gpushare 共用语义）
	GPUIndex = "volcano.sh/gpu-index"

	// UnhealthyGPUIDs list of unhealthy gpu ids
	// UnhealthyGPUIDs 是节点注解的键，记录节点上不健康的 GPU 卡 ID 列表
	UnhealthyGPUIDs = "volcano.sh/gpu-unhealthy-ids"

	// binpack means the lower device memory remained after this allocation, the better
	// binpackPolicy 是调度策略名：装箱，尽量把任务压到已用显存多的卡上，剩余空闲显存最少
	binpackPolicy = "binpack"
	// spread means better put this task into an idle GPU card than a shared GPU card
	// spreadPolicy 是调度策略名：分散，尽量把任务放到空闲卡上而非已共享的卡
	spreadPolicy = "spread"
	// 101 means wo don't assign defaultMemPercentage value
	// DefaultMemPercentage 表示“未指定显存百分比”的哨兵值（101 表示未设置百分比请求）
	DefaultMemPercentage = 101
	// binpackMultiplier 是 binpack 策略下打分时的显存占用率乘数
	binpackMultiplier = 100
	// spreadMultiplier 是 spread 策略下空闲卡的基础加分乘数
	spreadMultiplier = 100

	// GPUModeAnnotation 是 Pod 注解的键；Pod 声明后，要求与节点共享模式一致
	GPUModeAnnotation = "volcano.sh/vgpu-mode"
	// VGPUPodGroupPolicyAnnotation 是 Pod 注解的键，声明同一 PodGroup 内的放置策略
	VGPUPodGroupPolicyAnnotation = "volcano.sh/vgpu-podgroup-policy"
	// VGPUPodGroupPolicySpreadValue 表示该策略取值为 spread：同组 Pod 尽量分散到不同设备
	VGPUPodGroupPolicySpreadValue = "spread"
	// vGPUControllerHAMICore 是共享模式标识：基于 HAMICore 的细粒度虚拟化切分
	vGPUControllerHAMICore = "hami-core"
	// vGPUControllerMIG 是共享模式标识：基于 NVIDIA MIG 的硬隔离切分
	vGPUControllerMIG = "mig"
	// vGPUControllerMPS 是保留的共享模式标识；当前模式归一化逻辑不会选择 MPS 工厂
	vGPUControllerMPS = "mps"
)

// 包级开关与策略参数，由 deviceshare 插件在启动时按配置注入
var (
	// VGPUEnable 控制是否启用 vgpu（HAMICore/MIG）共享调度
	VGPUEnable bool
	// NodeLockEnable 控制分配时是否对节点加分布式锁
	NodeLockEnable bool
	// SchedulePolicy 是全局调度打分策略（binpack / spread / 空）
	SchedulePolicy string
)

// ContainerDevice 描述一个容器在某个物理 GPU 设备上分配到的“虚拟设备”切片
type ContainerDevice struct {
	// UUID 是分配到的设备标识：HAMICore 通常为物理 GPU UUID，MIG 模式为带实例位置的 MIG ID
	UUID string
	// device type, like NVIDIA, MLU
	// Type 是设备类型，如 NVIDIA、MLU 等
	Type string
	// Usedmem 是该容器在该设备上分配到的显存（单位 MiB）
	Usedmem uint
	// Usedcores 是该容器在该设备上分配到的算力百分比（0-100）
	Usedcores uint
}

// ContainerDevices 是一个容器内分配到的多个设备切片的集合
type ContainerDevices []ContainerDevice
