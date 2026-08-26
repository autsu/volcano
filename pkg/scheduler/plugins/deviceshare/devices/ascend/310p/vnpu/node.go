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

// 本文件提供把 VNPU 节点按空闲 AiCore 排序的能力，供批量打分时使用。
package vnpu310p

import (
	// sort 用于对节点列表排序
	"sort"

	// vnpu 是底层昇腾 VNPU 设备插件包（NodeInf 类型）
	"volcano.sh/volcano/pkg/scheduler/api/devices/ascend/mindcluster/ascend310p/vnpu"
	// util 是第三方 mindcluster 公共工具/常量
	"volcano.sh/volcano/third_party/mindcluster/common/util"
)

// orderVNodesByFreeResource 把节点列表按空闲 AiCore 升序排序后返回。
func orderVNodesByFreeResource(nodes []*vnpu.NodeInf) []*vnpu.NodeInf {
	// 转为可排序的 vNodesList 类型
	tempVNodes := vNodesList(nodes)
	// 调用标准库排序（Less 决定顺序）
	sort.Sort(tempVNodes)
	// 返回排序后的节点列表
	return tempVNodes
}

// vNodesList 是 []*vnpu.NodeInf 的别名，实现 sort.Interface 以支持排序。
type vNodesList []*vnpu.NodeInf

// Len for order.
// Len 返回节点数量（sort.Interface 必需）。
func (vNodes vNodesList) Len() int {
	return len(vNodes)
}

// Less for order.
// Less 决定排序规则：按空闲 AiCore 升序（空闲少的排前面）；缺字段时做边界处理。
func (vNodes vNodesList) Less(i, j int) bool {
	// 下标越界保护
	if i > vNodes.Len() || j > vNodes.Len() {
		return false
	}
	// 取节点 i 的空闲 AiCore
	iIdleAiCore, ok := vNodes[i].Idle[util.AscendNPUCore]
	if !ok {
		// i 无该字段则视为“不小于”j
		return false
	}
	// 取节点 j 的空闲 AiCore
	jIdleAiCore, ok := vNodes[j].Idle[util.AscendNPUCore]
	if !ok {
		// j 无该字段则 i 视为更小（排前面）
		return true
	}
	// 空闲 AiCore 少的排在前面
	return iIdleAiCore < jIdleAiCore
}

// Swap for order.
// Swap 交换两个节点位置（sort.Interface 必需）。
func (vNodes vNodesList) Swap(i, j int) {
	// 下标越界保护
	if i > vNodes.Len() || j > vNodes.Len() {
		return
	}
	vNodes[i], vNodes[j] = vNodes[j], vNodes[i]
}
