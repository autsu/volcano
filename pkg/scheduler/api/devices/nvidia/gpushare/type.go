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

package gpushare

// GpuSharingEnable 控制是否启用基于显存（volcano.sh/gpu-memory）的 GPU 共享调度
var GpuSharingEnable bool

// NodeLockEnable 控制分配 GPU 时是否对节点加分布式锁，避免并发分配冲突
var NodeLockEnable bool

// GpuNumberEnable 控制是否启用基于整卡数量（volcano.sh/gpu-number）的 GPU 共享调度
var GpuNumberEnable bool

const (
	// DeviceName used to indicate this device
	// DeviceName 是该设备插件的标识名，调度器用它来区分不同的设备类型
	DeviceName = "GpuShare"

	// VolcanoGPUResource extended gpu resource
	// VolcanoGPUResource 是扩展资源名，表示 Pod 申请的 GPU 显存（单位 MiB）
	VolcanoGPUResource = "volcano.sh/gpu-memory"
	// VolcanoGPUNumber virtual GPU card number
	// VolcanoGPUNumber 是扩展资源名，表示 Pod 申请的整卡（虚拟 GPU 卡）数量
	VolcanoGPUNumber = "volcano.sh/gpu-number"

	// PredicateTime is the key of predicate time
	// PredicateTime 是 Pod 注解的键，记录谓词（调度预选）发生的时间戳
	PredicateTime = "volcano.sh/predicate-time"
	// GPUIndex is the key of gpu index
	// GPUIndex 是 Pod 注解的键，记录该 Pod 被分配到的 GPU 卡索引列表
	GPUIndex = "volcano.sh/gpu-index"

	// UnhealthyGPUIDs list of unhealthy gpu ids
	// UnhealthyGPUIDs 是节点注解的键，记录节点上不健康（不可用）的 GPU 卡 ID 列表
	UnhealthyGPUIDs = "volcano.sh/gpu-unhealthy-ids"
)
