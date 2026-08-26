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

// 本文件实现 VNPU 的批量节点打分（BatchNodeOrderFn）：跨候选节点联合排序并赋分。
package vnpu310p

import (
	// reflect 用于判断节点指针是否为 nil
	"reflect"

	// v1 是 k8s 核心 Pod 类型
	v1 "k8s.io/api/core/v1"
	// klog 是 k8s 日志库
	"k8s.io/klog/v2"

	// api 是 volcano 调度器对 Pod/节点/设备的抽象与 Devices 接口
	"volcano.sh/volcano/pkg/scheduler/api"
	// vnpu 是底层昇腾 VNPU 设备插件包
	"volcano.sh/volcano/pkg/scheduler/api/devices/ascend/mindcluster/ascend310p/vnpu"
	// util 是第三方 mindcluster 公共工具/常量
	"volcano.sh/volcano/third_party/mindcluster/common/util"
)

// ScoreBatchNodes 对一组候选节点计算 VNPU 批量打分：按空闲资源排序后给首个（或首个非降级）节点高分。
func ScoreBatchNodes(pod *v1.Pod, schedulePolicy string, device api.Devices, neighbours []api.Devices) []float64 {
	// 收集邻居节点的 NodeInf 与降级缓存
	var neighbourNodeInfs []*vnpu.NodeInf
	// 记录已被降级的节点名
	var podDowngradeCache []string
	for _, neighbour := range neighbours {
		// 类型断言为底层 NPUDevices
		if npuNeighbour, ok := neighbour.(*vnpu.NPUDevices); ok {
			// 收集节点信息
			neighbourNodeInfs = append(neighbourNodeInfs, &npuNeighbour.NodeInf)
			// 若该节点在降级缓存里，记录其名
			_, ok := npuNeighbour.DowngradeCache[pod.Name]
			if ok {
				podDowngradeCache = append(podDowngradeCache, npuNeighbour.NodeInf.Name)
			}
		}
	}

	// init score-map
	// 初始化所有节点为 0 分
	scoreMap := initScoreMap(neighbourNodeInfs)

	// 按空闲资源排序节点
	nodesSorted := orderVNodesByFreeResource(neighbourNodeInfs)
	if len(nodesSorted) == 0 {
		klog.V(util.LogErrorLev).Infof("dynamic vnpu task<%s> ScoreBestNPUNodes err: sorted nodes len 0", pod.Name)
		// Return array with same length as neighbours, all zeros
		// 排序结果为空，返回与邻居等长的全零分数
		return make([]float64, len(neighbours))
	}

	// 2. give the first node high score, none nodes are downgraded
	// 2. 没有降级节点：给空闲资源最少的第一个节点加高分
	if len(podDowngradeCache) == 0 {
		// 确保该节点在 scoreMap 里有初始项
		_, sOK := scoreMap[nodesSorted[0].Name]
		if !sOK {
			scoreMap[nodesSorted[0].Name] = 0.0
		}
		// 加 8 分（NPUIndex8）
		scoreMap[nodesSorted[0].Name] += util.NPUIndex8
	} else {
		// 3. if downgrade nodes exists, skip, util find none-downgraded nodes and add score
		// 3. 存在降级节点：跳过它们，给首个非降级节点更高分（8*2），其余降级节点给 8 分
		for _, node := range nodesSorted {
			// 判断本节点是否在被降级集合里
			downgradeFlag := false
			for _, dNode := range podDowngradeCache {
				if node.Name == dNode {
					downgradeFlag = true
					break
				}
			}
			if !downgradeFlag {
				// 非降级节点：给 16 分并结束（只给最优节点高分）
				scoreMap[node.Name] += util.NPUIndex8 * util.NPUIndex2
				break
			}
			// 降级节点：给 8 分
			scoreMap[node.Name] += util.NPUIndex8
		}
	}

	// Convert scoreMap to array in the same order as neighbours
	// 把 scoreMap 按邻居原始顺序转换为分数数组
	scores := make([]float64, len(neighbours))
	for i, neighbour := range neighbours {
		if npuNeighbour, ok := neighbour.(*vnpu.NPUDevices); ok {
			if score, exists := scoreMap[npuNeighbour.NodeInf.Name]; exists {
				// 填入对应位置的分数
				scores[i] = score
			}
		}
	}

	// 返回与邻居等长的分数数组
	return scores
}

// initScoreMap 构造 节点名→0 分的初始打分表，遇 nil 节点跳过。
func initScoreMap(nodes []*vnpu.NodeInf) map[string]float64 {
	// 预分配容量
	scoreMap := make(map[string]float64, len(nodes))
	for _, node := range nodes {
		// 跳过 nil 节点
		if reflect.ValueOf(node).IsNil() {
			continue
		}
		// 初始 0 分
		scoreMap[node.Name] = 0.0
	}
	// 返回初始打分表
	return scoreMap
}
