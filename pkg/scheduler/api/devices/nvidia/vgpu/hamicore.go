/*
Copyright 2025 The Volcano Authors.

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
	"fmt"            // fmt 用于构造错误
	"k8s.io/klog/v2" // klog 是 Kubernetes 日志库
)

// HAMICoreFactory 是 HAMICore 共享模式的工厂实现（把 Pod 的显存/算力直接累加/扣减到整卡账本）
type HAMICoreFactory struct{}

// init 在包加载时把 HAMICore 工厂注册到共享处理器注册表
func init() {
	// 以 hami-core 为键注册工厂
	RegisterFactory(vGPUControllerHAMICore, HAMICoreFactory{})
}

// TryAddPod 把 Pod 占用加到卡上（仅更新聚合账本，不登记 PodMap），返回是否成功与该卡 UUID；是否为试算由调用方传入的对象决定
func (f HAMICoreFactory) TryAddPod(gd *GPUDevice, mem uint, core uint) (bool, string) {
	// 已分配数量 +1
	gd.UsedNum++
	// 已分配显存累加
	gd.UsedMem += mem
	// 已分配算力累加
	gd.UsedCore += core

	// 始终成功，并返回该卡 UUID 作为分配到的设备标识
	return true, gd.UUID
}

// AddPod 真正把 Pod 占用落到卡上：更新账本并登记到 PodMap（带幂等判断）
func (f HAMICoreFactory) AddPod(gd *GPUDevice, mem uint, core uint, podUID string, devID string) error {
	// 若 Pod 已在账本中则跳过，保证幂等
	if _, ok := gd.PodMap[podUID]; ok {
		return nil // 已存在，直接返回
	}
	// 为该 Pod 创建一条占用记录
	gd.PodMap[podUID] = &GPUUsage{
		UsedMem:  0, // 初始显存 0
		UsedCore: 0, // 初始算力 0
	}
	// 已分配数量 +1
	gd.UsedNum++
	// 已分配显存累加
	gd.UsedMem += mem
	// 已分配算力累加
	gd.UsedCore += core

	// 把本次占用明细累加到该 Pod 的账本记录
	gd.PodMap[podUID].UsedMem += mem
	gd.PodMap[podUID].UsedCore += core

	// 打印记账完成的调试日志
	klog.V(4).Infoln("add Pod: ", podUID, mem, gd.PodMap[podUID].UsedMem, gd.PodMap[podUID].UsedCore)
	// 返回成功
	return nil
}

// SubPod 把 Pod 占用从卡上扣减：更新账本并移除 PodMap 记录
func (f HAMICoreFactory) SubPod(gd *GPUDevice, mem uint, core uint, podUID string, devID string) error {
	// 检查 Pod 是否在账本中
	_, ok := gd.PodMap[podUID]
	// 不在账本中则返回错误
	if !ok {
		return fmt.Errorf("pod not exist in GPU pod map") // 返回 Pod 不存在错误
	}

	// 已分配数量 -1
	gd.UsedNum--
	// 已分配显存扣减
	gd.UsedMem -= mem
	// 已分配算力扣减
	gd.UsedCore -= core
	klog.V(4).Infoln("sub Pod: ", podUID, mem) // 打印扣减调试日志
	// 从账本中移除该 Pod 记录
	delete(gd.PodMap, podUID)
	// 返回成功
	return nil
}
