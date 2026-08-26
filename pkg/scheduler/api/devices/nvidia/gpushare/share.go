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

package gpushare // gpushare 包：基于显存/整卡数量的粗粒度 GPU 共享调度逻辑

import (
	"fmt"     // fmt 用于格式化错误与字符串
	"sort"    // sort 用于对候选卡序号排序
	"strconv" // strconv 用于字符串与整数互转
	"strings" // strings 用于字符串切分与替换
	"time"    // time 用于生成分配时间戳

	v1 "k8s.io/api/core/v1" // v1 是 Kubernetes 核心 API（Pod 等）
	"k8s.io/klog/v2"        // klog 是 Kubernetes 日志库
)

// getDevicesIdleGPUMemory returns all the idle GPU memory by gpu card.
// getDevicesIdleGPUMemory 计算每张卡当前的空闲显存量（总显存减去已用显存），以卡序号为键返回
func getDevicesIdleGPUMemory(gs *GPUDevices) map[int]uint {
	// 取得每张卡的总显存映射
	devicesAllGPUMemory := getDevicesAllGPUMemory(gs)
	// 取得每张卡已使用的显存映射
	devicesUsedGPUMemory := getDevicesUsedGPUMemory(gs)
	// 初始化结果映射
	res := map[int]uint{}
	// 遍历每张卡的总显存
	for id, allMemory := range devicesAllGPUMemory {
		// 若该卡有已用记录，则空闲 = 总量 - 已用
		if usedMemory, found := devicesUsedGPUMemory[id]; found {
			res[id] = allMemory - usedMemory // 计算空闲显存
		} else {
			// 没有已用记录则空闲等于总量
			res[id] = allMemory // 整卡空闲
		}
	}
	// 返回每张卡的空闲显存
	return res
}

// getDevicesUsedGPUMemory 汇总每张卡当前已使用的显存（按卡序号返回）
func getDevicesUsedGPUMemory(gs *GPUDevices) map[int]uint {
	// 初始化结果映射
	res := map[int]uint{}
	// 遍历节点上的每张卡
	for _, device := range gs.Device {
		// 调用单卡方法累加其已用显存
		res[device.ID] = device.getUsedGPUMemory() // 记录该卡已用显存
	}
	// 返回已用显存映射
	return res
}

// getDevicesAllGPUMemory 汇总每张卡的总显存（按卡序号返回）
func getDevicesAllGPUMemory(gs *GPUDevices) map[int]uint {
	// 初始化结果映射
	res := map[int]uint{}
	// 遍历节点上的每张卡
	for _, device := range gs.Device {
		// 直接取卡的显存容量
		res[device.ID] = device.Memory // 记录该卡总显存
	}
	// 返回总显存映射
	return res
}

// GetDevicesIdleGPU returns all the idle gpu card.
// GetDevicesIdleGPU 返回当前账本中没有 Pod 的 GPU 卡序号列表；返回顺序不保证稳定
func getDevicesIdleGPUs(gs *GPUDevices) []int {
	// 初始化结果切片
	res := []int{}
	// 遍历节点上的每张卡
	for _, device := range gs.Device {
		// 若该卡处于空闲状态则收集其序号
		if device.isIdleGPU() {
			res = append(res, device.ID) // 加入空闲卡列表
		}
	}
	// 返回空闲卡序号列表
	return res
}

// getUnhealthyGPUs returns all the unhealthy GPU id.
// getUnhealthyGPUs 解析节点注解中的不健康 GPU 列表，返回需要剔除的卡序号
func getUnhealthyGPUs(gs *GPUDevices, node *v1.Node) (unhealthyGPUs []int) {
	// 初始化返回值（命名返回变量）
	unhealthyGPUs = []int{}
	// 从节点注解中读取不健康 GPU 列表字符串
	devicesStr, ok := node.Annotations[UnhealthyGPUIDs]

	// 注解不存在说明没有不健康卡，直接返回空
	if !ok {
		return // 无注解，返回空列表
	}

	// 按逗号切分出每个卡 ID 字符串
	idsStr := strings.Split(devicesStr, ",")
	// 逐个解析卡 ID
	for _, sid := range idsStr {
		// 把字符串转换为整数
		id, err := strconv.Atoi(sid)
		// 解析失败仅告警，跳过该 ID
		if err != nil {
			klog.Warningf("Failed to parse unhealthy gpu id %s due to %v", sid, err) // 记录解析失败
		} else {
			// 解析成功则加入不健康列表
			unhealthyGPUs = append(unhealthyGPUs, id) // 收集不健康卡 ID
		}
	}
	// 返回收集到的不健康卡列表
	return
}

// GetGPUIndex returns the index list of gpu cards
// GetGPUIndex 从 Pod 注解中读出该 Pod 被分配到的 GPU 卡序号列表
func GetGPUIndex(pod *v1.Pod) []int {
	// Pod 没有任何注解时直接返回 nil
	if len(pod.Annotations) == 0 {
		return nil // 无注解，无分配信息
	}

	// 读取 gpu-index 注解的值
	value, found := pod.Annotations[GPUIndex]
	// 注解不存在则返回 nil
	if !found {
		return nil // 未分配卡序号
	}

	// 按逗号切分出各个卡序号字符串
	ids := strings.Split(value, ",")
	// 切分结果为空说明注解格式异常
	if len(ids) == 0 {
		klog.Errorf("invalid gpu index annotation %s=%s", GPUIndex, value) // 记录无效注解
		return nil                                                         // 返回空
	}

	// 预分配整数切片
	idSlice := make([]int, len(ids))
	// 逐个把字符串转换为整数
	for idx, id := range ids {
		// 字符串转整数
		j, err := strconv.Atoi(id)
		// 转换失败则记录错误并返回 nil
		if err != nil {
			klog.Errorf("invalid %s=%s", GPUIndex, value) // 记录无效序号
			return nil                                    // 返回空
		}
		// 转换成功则存入结果切片
		idSlice[idx] = j // 记录卡序号
	}
	// 返回解析出的卡序号列表
	return idSlice
}

// checkNodeGPUSharingPredicate checks if a pod with gpu requirement can be scheduled on a node.
// checkNodeGPUSharingPredicate 检查申请了显存的 Pod 能否在当前节点上找到足够空闲显存的卡
func checkNodeGPUSharingPredicate(pod *v1.Pod, gs *GPUDevices) (bool, error) {
	// no gpu sharing request
	// 若无显存请求，视为默认满足（不占用本插件维度）
	if getGPUMemoryOfPod(pod) <= 0 {
		return true, nil // 无显存请求，直接通过
	}
	// 按显存条件挑选可用卡
	ids := predicateGPUbyMemory(pod, gs)
	// 没有可用卡则返回不可调度及原因
	if len(ids) == 0 {
		return false, fmt.Errorf("no enough gpu memory on node %s", gs.Name) // 显存不足
	}
	// 找到可用卡，预选通过
	return true, nil
}

// checkNodeGPUNumberPredicate 检查申请了整卡的 Pod 能否在当前节点上找到足够数量的空闲卡
func checkNodeGPUNumberPredicate(pod *v1.Pod, gs *GPUDevices) (bool, error) {
	//no gpu number request
	// 若无整卡请求，视为默认满足
	if getGPUNumberOfPod(pod) <= 0 {
		return true, nil // 无整卡请求，直接通过
	}
	// 按整卡数量条件挑选可用卡
	ids := predicateGPUbyNumber(pod, gs)
	// 没有足够空闲卡则返回不可调度及原因
	if len(ids) == 0 {
		return false, fmt.Errorf("no enough gpu number on node %s", gs.Name) // 整卡不足
	}
	// 找到足够空闲卡，预选通过
	return true, nil
}

// predicateGPUbyMemory returns the available GPU ID
// predicateGPUbyMemory 返回所有空闲显存大于等于 Pod 显存请求的卡序号（升序）
func predicateGPUbyMemory(pod *v1.Pod, gs *GPUDevices) []int {
	// 取出 Pod 申请的显存量
	gpuRequest := getGPUMemoryOfPod(pod)
	// 计算每张卡的空闲显存
	allocatableGPUs := getDevicesIdleGPUMemory(gs)

	// 声明候选卡序号切片
	var devIDs []int

	// 遍历每张卡的空闲显存
	for devID := range allocatableGPUs {
		// 仅当该卡空闲显存足够才加入候选
		if availableGPU, ok := allocatableGPUs[devID]; ok && availableGPU >= gpuRequest {
			devIDs = append(devIDs, devID) // 加入候选卡
		}
	}
	// 对候选卡序号排序，保证分配稳定（优先小号卡）
	sort.Ints(devIDs)
	// 返回候选卡序号列表
	return devIDs
}

// predicateGPU returns the available GPU IDs
// predicateGPUbyNumber 从空闲卡列表中返回前 N 张卡（N 为 Pod 申请的整卡数量）；卡的顺序不保证稳定
func predicateGPUbyNumber(pod *v1.Pod, gs *GPUDevices) []int {
	// 取出 Pod 申请的整卡数量
	gpuRequest := getGPUNumberOfPod(pod)
	// 取出所有空闲卡序号
	allocatableGPUs := getDevicesIdleGPUs(gs)

	// 空闲卡数量不足则报错并返回空
	if len(allocatableGPUs) < gpuRequest {
		klog.Errorf("Not enough gpu cards") // 记录卡不足
		return nil                          // 返回空
	}

	// 取空闲列表前 gpuRequest 张作为分配结果；该列表来自 map 遍历，不保证按卡号排序
	return allocatableGPUs[:gpuRequest]
}

// escapeJSONPointer 按 RFC6901 转义 JSON Pointer 中的特殊字符（~ 与 /）
func escapeJSONPointer(p string) string {
	// Escaping reference name using https://tools.ietf.org/html/rfc6901
	// 先把 ~ 转义为 ~0（必须在 / 之前处理，避免冲突）
	p = strings.Replace(p, "~", "~0", -1)
	// 再把 / 转义为 ~1
	p = strings.Replace(p, "/", "~1", -1)
	// 返回转义后的字符串
	return p
}

// AddGPUIndexPatch returns the patch adding GPU index
// AddGPUIndexPatch 构造一个 JSON Patch 字符串，用于向 Pod 注解写入 predicate-time 与 gpu-index
func AddGPUIndexPatch(ids []int) string {
	// 把序号切片格式化成逗号分隔字符串（去掉切片默认格式的方括号与空格）
	idsstring := strings.Trim(strings.Replace(fmt.Sprint(ids), " ", ",", -1), "[]")
	// 第一段 add 操作：向 Pod 注解写入 predicate-time（分配时间戳）
	return fmt.Sprintf(`[{"op": "add", "path": "/metadata/annotations/%s", "value":"%d"},`+
		// 第二段 add 操作：向 Pod 注解写入 gpu-index（分配到的卡序号列表）
		`{"op": "add", "path": "/metadata/annotations/%s", "value": "%s"}]`,
		// 依次传入：转义后的 predicate-time 键、当前纳秒时间戳、转义后的 gpu-index 键、序号字符串
		escapeJSONPointer(PredicateTime), time.Now().UnixNano(),
		escapeJSONPointer(GPUIndex), idsstring)
}

// RemoveGPUIndexPatch returns the patch removing GPU index
// RemoveGPUIndexPatch 构造一个 JSON Patch 字符串，用于从 Pod 注解移除 predicate-time 与 gpu-index
func RemoveGPUIndexPatch() string {
	// 第一段 remove 操作：移除 predicate-time 注解
	return fmt.Sprintf(`[{"op": "remove", "path": "/metadata/annotations/%s"},`+
		// 第二段 remove 操作：移除 gpu-index 注解
		`{"op": "remove", "path": "/metadata/annotations/%s"}]`, escapeJSONPointer(PredicateTime), escapeJSONPointer(GPUIndex))
}

// getUsedGPUMemory calculates the used memory of the device.
// getUsedGPUMemory 累加该卡上所有非终态 Pod 申请的显存，得到卡的已用显存
func (g *GPUDevice) getUsedGPUMemory() uint {
	// 初始化已用显存为 0
	res := uint(0)
	// 遍历共享这张卡的每个 Pod
	for _, pod := range g.PodMap {
		// 已进入 Succeeded 或 Failed 终态的 Pod 不再占用显存，跳过
		if pod.Status.Phase == v1.PodSucceeded || pod.Status.Phase == v1.PodFailed {
			continue // 终态 Pod 不计入占用
		} else {
			// 取该 Pod 申请的显存并累加
			gpuRequest := getGPUMemoryOfPod(pod)
			res += gpuRequest // 累加显存占用
		}
	}
	// 返回该卡已用显存总量
	return res
}

// isIdleGPU check if the device is idled.
// isIdleGPU 判断该卡是否完全空闲（没有任何 Pod 共享）
func (g *GPUDevice) isIdleGPU() bool {
	// 共享 Pod 集合为空即视为空闲
	return len(g.PodMap) == 0
}

// getGPUMemoryPod returns the GPU memory required by the pod.
// getGPUMemoryOfPod 计算 Pod 申请的 GPU 显存：普通容器之和与初始化容器最大值取较大者
func getGPUMemoryOfPod(pod *v1.Pod) uint {
	// 记录初始化容器中的最大显存请求
	var initMem uint
	// 遍历初始化容器，取其中显存请求的最大值
	for _, container := range pod.Spec.InitContainers {
		res := getGPUMemoryOfContainer(container.Resources)
		// 若当前初始化容器显存更大则更新
		if initMem < res {
			initMem = res // 更新最大值
		}
	}

	// 记录普通容器的显存请求之和
	var mem uint
	// 遍历普通容器累加显存
	for _, container := range pod.Spec.Containers {
		mem += getGPUMemoryOfContainer(container.Resources) // 累加显存
	}

	// 普通容器之和与初始化容器最大值比较，取较大者
	if mem > initMem {
		return mem // 普通容器之和更大
	}
	// 否则返回初始化容器最大值
	return initMem
}

// getGPUMemoryOfContainer returns the GPU memory required by the container.
// getGPUMemoryOfContainer 从容器的资源 Limits 中读取 volcano.sh/gpu-memory 显存请求
func getGPUMemoryOfContainer(resources v1.ResourceRequirements) uint {
	// 初始化显存为 0
	var mem uint
	// 若容器 Limits 中声明了 gpu-memory 资源则取值
	if val, ok := resources.Limits[VolcanoGPUResource]; ok {
		mem = uint(val.Value()) // 转为 uint 显存值
	}
	// 返回显存请求（未声明则为 0）
	return mem
}

// getGPUNumberOfPod returns the number of GPUs required by the pod.
// getGPUNumberOfPod 计算 Pod 申请的整卡数量：普通容器之和与初始化容器最大值取较大者
func getGPUNumberOfPod(pod *v1.Pod) int {
	// 记录普通容器申请的整卡数量之和
	var gpus int
	// 遍历普通容器累加整卡数
	for _, container := range pod.Spec.Containers {
		gpus += getGPUNumberOfContainer(container.Resources) // 累加整卡数
	}

	// 记录初始化容器中的最大整卡请求
	var initGPUs int
	// 遍历初始化容器，取其中整卡请求的最大值
	for _, container := range pod.Spec.InitContainers {
		res := getGPUNumberOfContainer(container.Resources)
		// 若当前初始化容器整卡数更大则更新
		if initGPUs < res {
			initGPUs = res // 更新最大值
		}
	}

	// 普通容器之和与初始化容器最大值比较，取较大者
	if gpus > initGPUs {
		return gpus // 普通容器之和更大
	}
	// 否则返回初始化容器最大值
	return initGPUs
}

// getGPUNumberOfContainer returns the number of GPUs required by the container.
// getGPUNumberOfContainer 从容器的资源 Limits 中读取 volcano.sh/gpu-number 整卡数量请求
func getGPUNumberOfContainer(resources v1.ResourceRequirements) int {
	// 初始化整卡数为 0
	var gpus int
	// 若容器 Limits 中声明了 gpu-number 资源则取值
	if val, ok := resources.Limits[VolcanoGPUNumber]; ok {
		gpus = int(val.Value()) // 转为 int 整卡数
	}
	// 返回整卡数量（未声明则为 0）
	return gpus
}
