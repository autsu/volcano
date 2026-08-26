/*
Copyright 2025 Volcano Authors.

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

// SharingFactory 是共享切分策略的抽象接口：不同模式（HAMICore / MIG）实现各自落到物理卡的逻辑
type SharingFactory interface {
	// TryAddPod try to add pod, not add the pod to the device pod map
	// TryAddPod 把占用加到卡账本但不登记 PodMap；预选阶段作用于快照，绑定阶段作用于真实设备对象，返回设备分配 ID
	TryAddPod(gd *GPUDevice, mem uint, core uint) (bool, string)
	// AddPod truely add pod, add it to the device pod map
	// AddPod 真正把 Pod 占用落到卡上并更新 PodMap（带幂等），用于正式分配
	AddPod(gd *GPUDevice, mem uint, core uint, podUID string, devID string) error
	// SubPod substract the pod and remove it from the device pod map
	// SubPod 把 Pod 占用从卡上扣减并移除 PodMap 记录，用于释放
	SubPod(gd *GPUDevice, mem uint, core uint, podUID string, devID string) error
}

// sharingRegistry 是按共享模式名映射到具体工厂实现的注册表
var sharingRegistry = make(map[string]SharingFactory)

// RegisterFactory 把一个共享模式工厂注册到注册表（由各工厂的 init 调用）
func RegisterFactory(mode string, factory SharingFactory) {
	// 以模式名为键存入工厂
	sharingRegistry[mode] = factory
}

// GetSharingHandler 根据模式名取出对应的共享工厂（第二个返回值表示是否找到）
func GetSharingHandler(sharingMode string) (SharingFactory, bool) {
	// 从注册表中查找
	s, ok := sharingRegistry[sharingMode]
	// 返回工厂与是否找到
	return s, ok
}
