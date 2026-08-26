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
	"fmt"     // fmt 用于格式化 MIG ID 与错误
	"sort"    // sort 用于按显存升序排序 MIG 实例
	"strconv" // strconv 用于解析 MIG ID 中的整型位置
	"strings" // strings 用于查找括号定位 MIG ID

	"k8s.io/klog/v2" // klog 是 Kubernetes 日志库

	"volcano.sh/volcano/pkg/scheduler/api/devices/config" // config 提供 MIG 几何与占用类型
)

// MIGFactory 是 MIG（Multi-Instance GPU）共享模式的工厂实现（硬隔离地把 Pod 切到具体 MIG 实例）
type MIGFactory struct{}

// init 在包加载时把 MIG 工厂注册到共享处理器注册表
func init() {
	// 以 mig 为键注册工厂
	RegisterFactory(vGPUControllerMIG, MIGFactory{})
}

// TryAddPod 把 Pod 占用落到某个 MIG 实例的账本上（不登记 PodMap），返回是否成功与分配到的 MIG 设备 ID；是否为试算由调用方传入的对象决定
func (f MIGFactory) TryAddPod(gd *GPUDevice, mem uint, core uint) (bool, string) {
	// 取出请求的显存
	requestMemory := mem
	// 读取显存换算因子（部分配置下显存需按因子放大/还原）
	memoryFactor := getConfig().GPUMemoryFactor
	// 若因子大于 1，则把请求显存按因子放大后再去匹配 MIG 实例
	if memoryFactor > 1 {
		requestMemory = requestMemory * memoryFactor
		klog.V(5).Infof("rawRequestMemory: %d, realRequestMemory: %d, memoryFactor: %d", mem, requestMemory, memoryFactor) // 记录换算前后显存
	}
	// 在卡的 MIG 模板与占用中查找一个能容纳该显存请求的实例
	found, dev, usedMem := findMatch(gd.UUID, requestMemory, gd.MigUsage, gd.MigTemplate)
	// 没找到合适实例则失败
	if !found {
		return false, "" // 无可用 MIG 实例
	}
	// 还原真实（未放大）的显存占用
	realMem := usedMem
	// 若因子大于 1，则把匹配到的实例显存再除以因子还原
	if memoryFactor > 1 {
		realMem = usedMem / memoryFactor
		klog.V(5).Infof("rawUsedMemory: %d, realUsedMemory: %d, memoryFactor: %d", usedMem, realMem, memoryFactor) // 记录还原前后显存
	}

	// 已分配数量 +1
	gd.UsedNum++
	// 已分配显存累加（还原后的真实值）
	gd.UsedMem += realMem
	// 已分配算力累加
	gd.UsedCore += core
	// 返回成功与分配到的 MIG 设备 ID（含 UUID 与位置）
	return true, dev
}

// AddPod 真正把 Pod 占用落到某个 MIG 实例上：解析 MIG ID、更新 MIG 占用账本与 PodMap
func (f MIGFactory) AddPod(gd *GPUDevice, mem uint, core uint, podUID string, devID string) error {
	// 解析 MIG 设备 ID，得到组名与位置
	group, index, err := decodeMIGID(devID)
	// 解析失败直接返回错误
	if err != nil {
		klog.ErrorS(err, "Failed to add pod") // 记录解析失败
		return err                            // 返回错误
	}

	// 检查 Pod 是否已在账本中
	_, ok := gd.PodMap[podUID]
	// 不在账本则创建一条空占用记录
	if !ok {
		gd.PodMap[podUID] = &GPUUsage{
			UsedMem:  0, // 初始显存 0
			UsedCore: 0, // 初始算力 0
		}
	} else {
		// 已存在则直接返回（幂等）
		return nil // 已登记，跳过
	}
	// 把本次占用写入 MIG 实例占用账本，返回该实例的显存
	usedMem := addMigUsed(gd, group, index)
	// 还原真实（未放大）的显存占用
	realMem := usedMem
	// 若因子大于 1，则把实例显存除以因子还原
	memoryFactor := getConfig().GPUMemoryFactor
	if memoryFactor > 1 {
		realMem = usedMem / memoryFactor
		klog.V(5).Infof("rawUsedMemory: %d, realUsedMemory: %d, memoryFactor: %d", usedMem, realMem, memoryFactor) // 记录还原前后显存
	}
	// 已分配数量 +1
	gd.UsedNum++
	// 已分配显存累加（还原后的真实值）
	gd.UsedMem += realMem
	// 已分配算力累加
	gd.UsedCore += core

	// 把本次占用明细累加到该 Pod 的账本记录
	gd.PodMap[podUID].UsedMem += realMem
	gd.PodMap[podUID].UsedCore += core

	// 打印记账完成的调试日志
	klog.V(4).Infoln("add Pod: ", podUID, realMem, gd.PodMap[podUID].UsedMem, gd.PodMap[podUID].UsedCore)
	// 返回成功
	return nil
}

// SubPod 把 Pod 占用从某个 MIG 实例上扣减：解析 MIG ID、更新 MIG 占用账本与 PodMap
func (f MIGFactory) SubPod(gd *GPUDevice, mem uint, core uint, podUID string, devID string) error {
	// 解析 MIG 设备 ID，得到组名与位置
	groupName, index, err := decodeMIGID(devID)
	// 解析失败返回错误
	if err != nil {
		return fmt.Errorf("Failed to sub pod: %v", err) // 返回解析失败错误
	}

	// 检查 Pod 是否在账本中
	_, ok := gd.PodMap[podUID]
	// 不在账本中则返回错误
	if !ok {
		return fmt.Errorf("pod not exist in GPU pod map") // 返回 Pod 不存在错误
	}

	// 把本次占用从 MIG 实例占用账本扣减，返回该实例的显存
	usedMem := subMigUsed(gd, groupName, index)
	// 还原真实（未放大）的显存占用
	realMem := usedMem
	// 若因子大于 1，则把实例显存除以因子还原
	memoryFactor := getConfig().GPUMemoryFactor
	if memoryFactor > 1 {
		realMem = usedMem / memoryFactor
		klog.V(5).Infof("rawUsedMemory: %d, realUsedMemory: %d, memoryFactor: %d", usedMem, realMem, memoryFactor) // 记录还原前后显存
	}
	// 已分配数量 -1
	gd.UsedNum--
	// 已分配显存扣减（还原后的真实值）
	gd.UsedMem -= realMem
	// 已分配算力扣减
	gd.UsedCore -= core

	// 打印扣减完成的调试日志
	klog.V(4).Infoln("sub Pod: ", podUID, realMem)
	// 从账本中移除该 Pod 记录
	delete(gd.PodMap, podUID)
	// 返回成功
	return nil
}

// Try to find a match
// findMatch 在卡的 MIG 几何模板与当前占用中，找到一个能满足显存请求的 MIG 实例，返回设备 ID 与实例显存
func findMatch(
	uuid string,
	requestMem uint,
	usage config.MigInUse,
	allowedGeometries []config.Geometry,
) (bool, string, uint) {
	// If a group is already in use
	// 若已经有某个 MIG 组在被使用（usage.Index >= 0），则固定在该组内继续找空闲实例
	if usage.Index >= 0 {
		// 取出正在使用的 MIG 组
		group := allowedGeometries[usage.Index]
		// 在该组内尝试挑一个满足条件的实例
		fitted, position, realMem := pickFromGroup(group, usage.UsageList, requestMem)
		// 找到则编码出完整的 MIG 设备 ID 并返回
		if fitted {
			MIGID := encodeMIGID(uuid, group.Group, position)
			return true, MIGID, realMem // 命中，返回设备 ID 与显存
		} else {
			// 组内无可用实例时直接失败，不会尝试其他几何组
			return false, "", 0 // 无可用实例
		}
	}

	// No group in use yet, try groups in order
	// 尚无组在使用，则按顺序遍历所有可用几何组，找到第一个能容纳请求的实例
	for _, group := range allowedGeometries {
		// 在全新组内尝试挑一个实例（usage 为空）
		fitted, position, realMem := pickFromGroup(group, nil, requestMem)
		// 找到则编码 MIG 设备 ID 并返回
		if fitted {
			MIGID := encodeMIGID(uuid, group.Group, position)
			return true, MIGID, realMem // 命中，返回设备 ID 与显存
		} else {
			// 该组放不下，继续尝试下一组
			continue // 尝试下一组
		}
	}
	// 所有组都放不下，失败
	return false, "", 0
}

/*
The uuid for mig device is like this: GPU-0fc3eda5-e98b-a25b-5b0d-cf5c855d1448[group2-3]
The group2 is the name of the group; The "3" is the position in the group which is
resource count before this resource group + in-resource index - 1.
For example: the group define like this:
  - models: [ "A100-SXM4-80GB", "A100 80GB PCIe", "A100-PCIE-80GB"]
    allowedGeometries:
  - group: "group2"
    geometries:
  - name: 2g.20gb
    memory: 20480
    count: 3
  - name: 1g.10gb
    memory: 10240
    count: 1

The position of "1g.10gb" in group2 is 3 + 1 - 1. "3" is the resource count before
"1g.10gb", the "1" is in-resource index.
// 中文说明：MIG 设备 ID 形如 GPU-<uuid>[<组名>-<位置>]，其中<位置> = 该组前面所有实例的数量之和 + 实例内序号 - 1；
// 例如 group2 含 2g.20gb×3 与 1g.10gb×1，则 1g.10gb 的位置 = 3 + 1 - 1 = 3。
*/
// pickFromGroup 在单个 MIG 几何组内，按显存升序尝试挑一个能容纳请求的实例，返回是否命中、位置与实例显存
func pickFromGroup(group config.Geometry, usage config.MIGS, requestMemory uint) (bool, int, uint) {
	// 定义带索引的实例结构，便于后续按显存排序
	type MigTemplateWithIndex struct {
		Index    int                // 原实例在组内的下标
		Instance config.MigTemplate // 实例模板（名称/显存/数量）
	}
	// 初始化实例列表
	var instances []MigTemplateWithIndex
	// 先把组内每个实例包装成带索引的结构
	for i, inst := range group.Instances {
		instances = append(instances, MigTemplateWithIndex{
			Index:    i,    // 记录原下标
			Instance: inst, // 记录实例模板
		})
	}
	// 按实例显存升序排序，优先用小显存实例（更贴合请求、碎片更少）
	sort.Slice(instances, func(i, j int) bool {
		return instances[i].Instance.Memory < instances[j].Instance.Memory
	})

	// 遍历排序后的实例
	for _, inst := range instances {
		// 全新组（usage 为空）且该实例显存足够，则直接选中
		if len(usage) == 0 && inst.Instance.Memory >= requestMemory {
			// 计算该实例在组内的全局位置
			position := getPosition(group, inst.Instance.Count, []int{}, inst.Index)
			klog.V(4).Infoln("pick mig group with no used group: ", inst.Index, inst.Instance.Memory, group.Group) // 记录选中日志
			return true, position, inst.Instance.Memory                                                            // 返回命中、位置与实例显存
		}
		// 已用组：遍历每个已被占用的实例，找同类实例的空闲槽位
		for _, usedInst := range usage {
			// 仅匹配同名实例（同型号 MIG 片）
			if usedInst.Name == inst.Instance.Name {
				// 计算空闲数量 = 总数量 - 已用数量
				available := inst.Instance.Count - len(usedInst.UsedIndex)
				// 有空闲槽位且显存足够才选中
				if available > 0 && inst.Instance.Memory >= requestMemory {
					// 计算该空闲槽位在组内的全局位置
					position := getPosition(group, inst.Instance.Count, usedInst.UsedIndex, inst.Index)
					klog.V(4).Infoln("pick mig group with used group: ", usedInst.Name, inst.Instance.Name, position) // 记录选中日志
					return true, position, inst.Instance.Memory                                                       // 返回命中、位置与实例显存
				}
			}
		}
	}
	// 遍历完仍未找到合适实例
	klog.V(2).Infoln("pick mig group but no suitalbe")
	// 返回未命中
	return false, -1, 0
}

// getPosition 计算某个实例内第一个未占用槽位在“整个 MIG 组”中的全局位置
func getPosition(group config.Geometry, count int, usedIndex []int, index int) int {
	// 初始化位置为 0
	position := 0
	// 先把该实例之前所有实例的数量累加，得到起始偏移
	for i := 0; i < index; i++ {
		position += group.Instances[i].Count // 累加前面实例的总数
	}

	// 在当前实例内找到第一个未被占用的槽位序号
	for i := 0; i < count; i++ {
		// 假设当前槽位未被占用
		found := false
		// 检查该槽位是否已在已用列表中
		for _, v := range usedIndex {
			if v == i {
				found = true // 已被占用
				break        // 跳出内层检查
			}
		}
		// 若未被占用，则以此为位置并结束查找
		if !found {
			position += i // 加上实例内偏移
			break         // 找到即停
		}
	}

	// 返回全局位置
	return position
}

// findPosition 是 getPosition 的逆运算：由全局位置反查出实例下标与实例内资源下标
func findPosition(group config.Geometry, position int) (instanceIndex, resourceIndex int) {
	// 初始化累加和为 0
	sum := 0
	// 遍历组内实例，按累计数量定位落在哪个实例
	for i, instance := range group.Instances {
		// 若位置落在当前实例的区间内，则返回其实例下标与实例内偏移
		if position < sum+instance.Count {
			return i, position - sum // 返回实例下标与资源下标
		}
		// 否则累加当前实例数量，继续向后查找
		sum += instance.Count
	}
	// 找不到对应实例（越界）则返回 -1
	return -1, -1
}

// encodeMIGID 把 UUID、组名、位置编码成 MIG 设备 ID 字符串
func encodeMIGID(uuid, group string, position int) string {
	// 拼接成 GPU-<uuid>[<组名>-<位置>] 形式
	return fmt.Sprintf("%s[%s-%d]", uuid, group, position)
}

// decodeMIGID 解析 MIG 设备 ID 字符串，取出组名与位置（逆运算）
func decodeMIGID(id string) (group string, position int, err error) {
	// 查找左方括号
	start := strings.Index(id, "[")
	// 查找右方括号
	end := strings.Index(id, "]")

	// 括号缺失或顺序错误则格式非法
	if start == -1 || end == -1 || start > end {
		return "", 0, fmt.Errorf("invalid format") // 返回格式错误
	}

	// 取出方括号内的内容（组名-位置）
	content := id[start+1 : end]
	// 按短横线切分
	parts := strings.Split(content, "-")
	// 必须恰好分成两段（组名与位置）
	if len(parts) != 2 {
		return "", 0, fmt.Errorf("invalid format inside brackets") // 返回内部格式错误
	}

	// 第一段为组名
	group = parts[0]
	// 第二段解析为整数位置
	position, err = strconv.Atoi(parts[1])
	// 解析失败返回错误
	if err != nil {
		return "", 0, fmt.Errorf("invalid index: %v", err) // 返回索引错误
	}

	// 返回解析出的组名与位置
	return group, position, nil
}

// addMigUsed 把一次占用登记到指定 MIG 实例（按组名+位置定位实例），返回该实例的显存
func addMigUsed(gd *GPUDevice, groupName string, position int) uint {
	// 遍历卡上的 MIG 模板组，匹配组名
	for groupIndex, group := range gd.MigTemplate {
		// 仅处理组名匹配的那一组
		if group.Group == groupName {
			klog.V(4).Infoln("add mig used: ", group.Group, groupIndex) // 记录命中的组
			// 标记当前使用中的组索引
			gd.MigUsage.Index = groupIndex
			// 由全局位置反查实例下标与实例内资源下标
			instanceIndex, resourceIndex := findPosition(group, position)
			// 取出该实例的 MIG 片名称
			migName := group.Instances[instanceIndex].Name
			klog.V(4).Infoln("add mig: ", migName, gd.MigUsage.UsageList) // 记录即将占用的实例
			// 遍历当前占用列表，找同名实例并把该槽位插入已用索引
			for i := range gd.MigUsage.UsageList {
				if gd.MigUsage.UsageList[i].Name == migName {
					gd.MigUsage.UsageList[i].UsedIndex = insert(gd.MigUsage.UsageList[i].UsedIndex, resourceIndex) // 插入占用的位置
					return gd.MigUsage.UsageList[i].Memory                                                         // 返回该实例显存
				}
			}
			// 若该实例尚未出现在占用列表中，则新建一条占用记录
			newUsage := config.MigTemplateUsage{
				Name:      group.Instances[instanceIndex].Name,   // 实例名
				Memory:    group.Instances[instanceIndex].Memory, // 实例显存
				InUse:     true,                                  // 标记为使用中
				UsedIndex: []int{resourceIndex},                  // 初始占用的位置
			}
			// 把新占用记录追加到使用列表
			gd.MigUsage.UsageList = append(gd.MigUsage.UsageList, newUsage)
			// 返回该实例显存
			return group.Instances[instanceIndex].Memory
		}
	}
	// 未找到对应组，返回 0
	return 0
}

// subMigUsed 把一次占用从指定 MIG 实例扣减（按组名+位置定位实例），返回该实例的显存
func subMigUsed(gd *GPUDevice, groupName string, position int) uint {
	// 遍历卡上的 MIG 模板组，匹配组名
	for groupIndex, group := range gd.MigTemplate {
		// 仅处理组名匹配的那一组
		if group.Group == groupName {
			// 标记当前使用中的组索引
			gd.MigUsage.Index = groupIndex
			// 由全局位置反查实例下标与实例内资源下标
			instanceIndex, resourceIndex := findPosition(group, position)
			// 取出该实例的 MIG 片名称
			migName := group.Instances[instanceIndex].Name
			// 遍历当前占用列表，找同名实例并移除该槽位
			for i := range gd.MigUsage.UsageList {
				// 跳过非同名实例
				if gd.MigUsage.UsageList[i].Name != migName {
					continue // 名称不匹配跳过
				}
				// 从已用索引中移除该位置
				gd.MigUsage.UsageList[i].UsedIndex = remove(gd.MigUsage.UsageList[i].UsedIndex, resourceIndex)
				klog.V(4).Infoln("sub mig used after remove:", gd.MigUsage.UsageList[i].UsedIndex) // 记录移除后的索引
				// 取出该实例显存（返回值）
				mem := gd.MigUsage.UsageList[i].Memory
				// 若该实例已无任何占用，则从占用列表中删除整条记录
				if len(gd.MigUsage.UsageList[i].UsedIndex) == 0 {
					gd.MigUsage.UsageList = append(gd.MigUsage.UsageList[:i], gd.MigUsage.UsageList[i+1:]...) // 删除该实例记录
				}
				// 返回该实例显存
				return mem
			}
			// 未找到对应实例占用，返回 0
			return 0
		}
	}
	// 未找到对应组，返回 0
	return 0
}

// Insert a value in order
// insert 把一个位置值按升序插入已用索引列表；若值已存在（非法重复占用）则原样返回
func insert(list []int, value int) []int {
	// 记录插入前的列表，便于调试
	klog.V(4).Infoln("insert mig used list before: ", list, value)
	// 找到 value 应插入的位置 i（保持升序，且跳过重复值）
	i := 0
	for ; i < len(list); i++ {
		// 小于当前元素则插在此处
		if value < list[i] {
			break // 找到插入点
		} else if value == list[i] {
			// 值已存在属非法占用，记录错误并原样返回
			klog.Error("insert mig used list but invalid gpu location: ", list, value) // 记录非法位置
			return list                                                                // 返回原列表
		}
	}
	// 在位置 i 插入 value（先扩容，再整体后移，再赋值）
	list = append(list, 0)
	copy(list[i+1:], list[i:])
	list[i] = value
	// 记录插入后的列表
	klog.V(4).Infoln("insert mig used list after: ", list)
	// 返回插入后的列表
	return list
}

// Remove first occurrence of a value
// remove 从列表中删除第一个等于 value 的元素（保持其余顺序）
func remove(list []int, value int) []int {
	// 记录删除前的列表，便于调试
	klog.V(4).Info("remove mig used list before: ", list, value)
	// 遍历查找目标值
	for i, v := range list {
		// 找到则删除该元素（拼接前后两段）
		if v == value {
			return append(list[:i], list[i+1:]...) // 返回删除后的列表
		}
	}
	// 未找到则原样返回
	return list // value not found
}
