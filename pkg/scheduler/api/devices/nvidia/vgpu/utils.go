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
	"context"                                             // context 用于向 Kubernetes API 传递请求上下文
	"encoding/json"                                       // json 用于把注解结构体序列化为补丁
	"errors"                                              // errors 用于构造简单错误
	"fmt"                                                 // fmt 用于格式化错误与字符串
	v1 "k8s.io/api/core/v1"                               // v1 是 Kubernetes 核心 API（Pod、Node）
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"         // metav1 提供 PatchOptions
	k8stypes "k8s.io/apimachinery/pkg/types"              // k8stypes 提供补丁类型常量
	"k8s.io/client-go/kubernetes"                         // kubernetes 是 Kubernetes 客户端集
	"k8s.io/klog/v2"                                      // klog 是 Kubernetes 日志库
	"sort"                                                // sort 用于按策略排序候选卡
	"strconv"                                             // strconv 用于整数与时间格式化
	"strings"                                             // strings 用于字符串匹配与切分
	"volcano.sh/apis/pkg/apis/scheduling/v1beta1"         // v1beta1 提供 PodGroup 注解键
	"volcano.sh/volcano/pkg/scheduler/api/devices"        // devices 提供资源请求解析与客户端
	"volcano.sh/volcano/pkg/scheduler/api/devices/config" // config 提供 vGPU 配置与 MIG 几何类型
)

// extractGeometryFromType 根据设备型号字符串，从配置中查找匹配的 MIG 几何模板
func extractGeometryFromType(t string) ([]config.Geometry, error) {
	// 若全局配置已加载，则遍历其 MIG 几何列表
	if config.GetConfig() != nil {
		// 遍历每个几何分组（每个分组含若干型号与一个几何模板）
		for _, val := range config.GetConfig().NvidiaConfig.MigGeometriesList {
			// 标记是否在该分组中找到匹配的型号
			found := false
			// 遍历该分组支持的型号列表
			for _, migDevType := range val.Models {
				// 设备型号包含配置的型号即视为匹配
				if strings.Contains(t, migDevType) {
					found = true // 命中型号
				}
			}
			// 找到匹配则返回该分组的几何模板
			if found {
				return val.Geometries, nil // 返回几何模板
			}
		}
	}
	// 未找到匹配，返回空模板与错误
	return []config.Geometry{}, errors.New("mig type not found") // 未找到 MIG 类型
}

// decodeNodeDevices 解析节点注册注解（由 HAMi 设备插件写入），构造节点级 GPUDevices 与共享模式
func decodeNodeDevices(name, str string) (*GPUDevices, string) {
	// 注解需包含冒号分隔的多卡信息，否则返回 nil
	if !strings.Contains(str, ":") {
		return nil, "" // 格式不符，返回空
	}
	// 按冒号切分出每张卡的描述片段
	tmp := strings.Split(str, ":")
	// 初始化返回的设备集合外壳
	retval := &GPUDevices{
		Name:   name,                     // 记录节点名
		Device: make(map[int]*GPUDevice), // 初始化卡映射
		Score:  float64(0),               // 初始打分 0
	}
	// 默认共享模式为 HAMICore
	sharingMode := vGPUControllerHAMICore
	// 遍历每张卡的描述片段（index 即为卡序号）
	for index, val := range tmp {
		// 片段需包含逗号才视为有效卡描述
		if strings.Contains(val, ",") {
			// 按逗号切分各字段
			items := strings.Split(val, ",")
			// 字段不足 6 个说明格式错误
			if len(items) < 6 {
				klog.Error("wrong Node GPU info: ", val) // 记录错误格式
				return nil, ""                           // 返回空
			}
			// 解析共享上限数量
			count, _ := strconv.Atoi(items[1])
			// 解析显存容量
			devmem, _ := strconv.Atoi(items[2])
			// 解析健康状态
			health, _ := strconv.ParseBool(items[4])
			// 构造单卡对象
			i := GPUDevice{
				ID:          index,                      // 卡序号
				Node:        name,                       // 所属节点
				UUID:        items[0],                   // 设备 UUID
				Number:      uint(count),                // 共享上限
				Memory:      uint(devmem),               // 总显存
				Type:        items[3],                   // 设备型号
				PodMap:      make(map[string]*GPUUsage), // 初始化占用账本
				Health:      health,                     // 健康状态
				MigTemplate: []config.Geometry{},        // 初始空 MIG 模板
				MigUsage: config.MigInUse{
					Index: -1}, // 初始无 MIG 组在使用
			}
			// 根据第 6 个字段确定共享模式
			sharingMode = getSharingMode(items[5])
			// 若为 MIG 模式，则尝试从配置提取该型号的 MIG 几何模板
			if sharingMode == vGPUControllerMIG {
				var err error
				i.MigTemplate, err = extractGeometryFromType(i.Type)
				// 提取失败则回退到 HAMICore 模式
				if err != nil {
					sharingMode = vGPUControllerHAMICore
					klog.ErrorS(err, "extract mig geometry error and fall back to hamicore mode") // 记录回退
				}
			}
			// 把构造好的卡放入设备集合
			retval.Device[index] = &i
		}
	}
	// 记录最终确定的共享模式
	retval.Mode = sharingMode
	// 返回设备集合与共享模式
	return retval, sharingMode
}

// encodeContainerDevices 把一个容器分配到的多张设备编码为单个字符串片段
func encodeContainerDevices(cd []ContainerDevice) string {
	// 初始化编码结果字符串
	tmp := ""
	// 遍历每张设备，拼接 UUID,型号,显存,算力
	for _, val := range cd {
		tmp += val.UUID + "," + val.Type + "," + strconv.Itoa(int(val.Usedmem)) + "," + strconv.Itoa(int(val.Usedcores)) + ":" // 以冒号分隔各设备
	}
	// 打印编码结果调试日志
	klog.V(4).Infoln("Encoded container Devices=", tmp)
	// 返回编码后的字符串
	return tmp
	//return strings.Join(cd, ",")
}

// encodePodDevices 把一个 Pod（多容器）的设备分配编码为单个字符串（容器间以分号分隔）
func encodePodDevices(pd []ContainerDevices) string {
	// 初始化每个容器的编码切片
	var ss []string
	// 逐个容器编码
	for _, cd := range pd {
		ss = append(ss, encodeContainerDevices(cd)) // 编码单个容器设备
	}
	// 以分号连接各容器编码
	return strings.Join(ss, ";")
}

// decodeContainerDevices 把单个容器的设备编码字符串还原为 ContainerDevices 列表
func decodeContainerDevices(str string) ContainerDevices {
	// 字符串为空直接返回空列表
	if len(str) == 0 {
		return ContainerDevices{} // 空输入
	}
	// 按冒号切分出每张设备片段
	cd := strings.Split(str, ":")
	// 初始化容器设备列表
	contdev := ContainerDevices{}
	// 临时设备结构
	tmpdev := ContainerDevice{}
	// 字符串为空再次防御性返回
	if len(str) == 0 {
		return contdev // 空输入
	}
	// 遍历每张设备片段
	for _, val := range cd {
		// 片段需包含逗号才视为有效
		if strings.Contains(val, ",") {
			// 按逗号切分字段
			tmpstr := strings.Split(val, ",")
			// 填充 UUID 与型号
			tmpdev.UUID = tmpstr[0]
			tmpdev.Type = tmpstr[1]
			// 解析显存占用
			devmem, _ := strconv.ParseInt(tmpstr[2], 10, 32)
			tmpdev.Usedmem = uint(devmem)
			// 解析算力占用
			devcores, _ := strconv.ParseInt(tmpstr[3], 10, 32)
			tmpdev.Usedcores = uint(devcores)
			// 把该设备加入容器设备列表
			contdev = append(contdev, tmpdev)
		}
	}
	// 返回解析出的容器设备列表
	return contdev
}

// DecodePodDevices parses the vgpu-ids-new annotation into per-container device lists.
// DecodePodDevices 把 Pod 的设备分配注解（分号分隔多容器）解析为按容器的设备列表
func DecodePodDevices(str string) []ContainerDevices {
	// 字符串为空直接返回空列表
	if len(str) == 0 {
		return []ContainerDevices{} // 空输入
	}
	// 初始化结果列表
	var pd []ContainerDevices
	// 按分号切分每个容器的编码
	for _, s := range strings.Split(str, ";") {
		// 解码单个容器的设备列表
		cd := decodeContainerDevices(s)
		// 加入结果
		pd = append(pd, cd)
	}
	// 返回按容器的设备分配列表
	return pd
}

// getPodGroupKey returns a unique key for the pod's group (namespace/name of PodGroup),
// or "" if the pod has no group annotation. Used to avoid placing two pods from the same
// PodGroup on the same vgpu device. Uses the same annotation keys as the job/podgroup
// controllers (KubeGroupNameAnnotationKey and VolcanoGroupNameAnnotationKey).
// getPodGroupKey 返回 Pod 所属 PodGroup 的命名空间/名称键；无组则返回空（用于同组分散放置）
func getPodGroupKey(pod *v1.Pod) string {
	// Pod 或注解为空时返回空
	if pod == nil || pod.Annotations == nil {
		return "" // 无 Pod 或注解
	}
	// 优先读取 Kubernetes 标准的 group 注解
	groupName := pod.Annotations[v1beta1.KubeGroupNameAnnotationKey]
	// 若为空则尝试读取 Volcano 的 group 注解
	if groupName == "" {
		groupName = pod.Annotations[v1beta1.VolcanoGroupNameAnnotationKey]
	}
	// 仍为空的说明没有组
	if groupName == "" {
		return "" // 无 PodGroup
	}
	// 返回 namespace/name 形式的组键
	return pod.Namespace + "/" + groupName
}

// deviceHasPodFromSameGroup returns true if the device already has a pod from the same
// PodGroup as currentKey (and that pod has non-zero usage), so we should not place
// another pod from the same group on this device.
// deviceHasPodFromSameGroup 判断该设备是否已存在同组且有占用的 Pod（用于避免同组 Pod 挤在同一卡）
func deviceHasPodFromSameGroup(gd *GPUDevice, currentKey string) bool {
	// 设备为空或当前组键为空时直接返回 false
	if gd == nil || currentKey == "" {
		return false // 参数不足
	}
	// 遍历该设备上的每个 Pod 占用
	for _, usage := range gd.PodMap {
		// 跳过空记录
		if usage == nil {
			continue // 空占用跳过
		}
		// 若存在同组且显存或算力占用非零的 Pod，则返回 true
		if usage.PodGroupKey == currentKey && (usage.UsedMem > 0 || usage.UsedCore > 0) {
			return true // 命中同组占用
		}
	}
	// 未发现同组占用
	return false
}

// checkVGPUResourcesInPod 判断 Pod 的容器是否声明了 vGPU 资源（显存名或数量名）
func checkVGPUResourcesInPod(pod *v1.Pod) bool {
	// 遍历 Pod 的普通容器
	for _, container := range pod.Spec.Containers {
		// 检查是否声明了 vGPU 显存资源
		_, ok := container.Resources.Limits[v1.ResourceName(getConfig().ResourceMemoryName)]
		// 声明显存资源则返回 true
		if ok {
			return true // 存在显存资源请求
		}
		// 检查是否声明了 vGPU 数量资源
		_, ok = container.Resources.Limits[v1.ResourceName(getConfig().ResourceCountName)]
		// 声明数量资源则返回 true
		if ok {
			return true // 存在数量资源请求
		}
	}
	// 没有任何 vGPU 资源请求
	return false
}

// resourcereqs 从 Pod 中提取出 NVIDIA 设备的资源请求列表（每张卡的数量/显存/算力）
func resourcereqs(pod *v1.Pod) []devices.ContainerDeviceRequest {
	// 取出各类资源的配置名
	countName := getConfig().ResourceCountName
	memoryName := getConfig().ResourceMemoryName
	percentageName := getConfig().ResourceMemoryPercentageName
	coreName := getConfig().ResourceCoreName
	// 调用通用解析函数抽取 NVIDIA 设备的资源请求
	return devices.ExtractResourceRequest(pod, "NVIDIA", countName, memoryName, percentageName, coreName)
}

// checkGPUtype 根据 Pod 的类型黑白名单注解，判断某型号 GPU 是否被允许使用
func checkGPUtype(annos map[string]string, cardtype string) bool {
	// 读取“只允许使用”的型号白名单
	inuse, ok := annos[GPUInUse]
	// 存在白名单时，只有命中白名单才允许
	if ok {
		// 单一型号（无逗号）的情况
		if !strings.Contains(inuse, ",") {
			// 当前卡型号包含白名单型号则允许
			if strings.Contains(strings.ToUpper(cardtype), strings.ToUpper(inuse)) {
				return true // 命中白名单
			}
		} else {
			// 多型号（逗号分隔）的情况，逐个匹配
			for _, val := range strings.Split(inuse, ",") {
				// 命中任一白名单型号则允许
				if strings.Contains(strings.ToUpper(cardtype), strings.ToUpper(val)) {
					return true // 命中白名单
				}
			}
		}
		// 未命中白名单则不允许
		return false
	}
	// 读取“禁止使用”的型号黑名单
	nouse, ok := annos[GPUNoUse]
	// 存在黑名单时，命中黑名单才禁止
	if ok {
		// 单一型号（无逗号）的情况
		if !strings.Contains(nouse, ",") {
			// 命中黑名单型号则禁止
			if strings.Contains(strings.ToUpper(cardtype), strings.ToUpper(nouse)) {
				return false // 命中黑名单
			}
		} else {
			// 多型号（逗号分隔）的情况，逐个匹配
			for _, val := range strings.Split(nouse, ",") {
				// 命中任一黑名单型号则禁止
				if strings.Contains(strings.ToUpper(cardtype), strings.ToUpper(val)) {
					return false // 命中黑名单
				}
			}
		}
		// 未命中黑名单则允许
		return true
	}
	// 既无白名单也无黑名单时，默认允许
	return true
}

// checkType 检查设备类型是否与请求匹配（NVIDIA 还需再过一次型号黑白名单校验）
func checkType(annos map[string]string, d GPUDevice, n devices.ContainerDeviceRequest) bool {
	//General type check, NVIDIA->NVIDIA MLU->MLU
	// 先做通用类型匹配：设备型号需包含请求的设备类型
	if !strings.Contains(d.Type, n.Type) {
		return false // 类型不匹配
	}
	// 若为 NVIDIA 设备，再走型号黑白名单校验
	if n.Type == NvidiaGPUDevice {
		return checkGPUtype(annos, d.Type) // 返回型号校验结果
	}
	// 其它设备类型记录未识别错误（理论上不会走到这里）
	klog.Errorf("Unrecognized device %v", n.Type)
	// 返回 false
	return false
}

// getGPUDeviceSnapShot is not a strict deep copy, the pointer item is same with origin.
// getGPUDeviceSnapShot 生成“刚好够用”的快照（非严格深拷贝）：
//   - PodMap 深拷贝：试分配时往快照里塞的 Pod 占用不会串回真实缓存；
//   - Sharing（切分策略处理器）等无状态对象直接复用，MigTemplate 共享只读的底层切片；
//   - MigUsage（MIG 实例占用）单独深拷贝，因为它在试分配时会被改写。
//
// 之所以不直接整份深拷贝，是为了在“预选要在快照上反复试算”的路径上省开销。
func getGPUDeviceSnapShot(snap *GPUDevices) *GPUDevices {
	// 构造快照外壳，复制基础字段
	ret := GPUDevices{
		Name:    snap.Name,                // 复制节点名
		Device:  make(map[int]*GPUDevice), // 初始化卡映射
		Score:   float64(0),               // 初始打分 0
		Sharing: snap.Sharing,             // 共享无状态处理器实例
	}
	// 逐卡拷贝
	for index, val := range snap.Device {
		// 跳过空卡
		if val != nil {
			// 深拷贝每张卡的 PodMap
			podMapCopy := make(map[string]*GPUUsage, len(val.PodMap))
			// 逐条值拷贝 Pod 占用
			for uid, usage := range val.PodMap {
				if usage != nil {
					u := *usage          // 值拷贝
					podMapCopy[uid] = &u // 放入新账本
				}
			}
			// 构造单卡快照（复制基础字段）
			ret.Device[index] = &GPUDevice{
				ID:          val.ID,          // 卡序号
				Node:        val.Node,        // 所属节点
				UUID:        val.UUID,        // 设备 UUID
				PodMap:      podMapCopy,      // 深拷贝的占用账本
				Memory:      val.Memory,      // 总显存
				Number:      val.Number,      // 共享上限
				Type:        val.Type,        // 设备型号
				Health:      val.Health,      // 健康状态
				UsedNum:     val.UsedNum,     // 已分配数量
				UsedMem:     val.UsedMem,     // 已分配显存
				UsedCore:    val.UsedCore,    // 已分配算力
				MigTemplate: val.MigTemplate, // MIG 模板底层切片复用（只读）
				MigUsage:    val.MigUsage,    // 先复制 MIG 占用结构，随后由 deepCopyMigInUse 完成深拷贝
			}
			// 额外做一次 MIG 占用的深拷贝（复制切片内容）
			ret.Device[index].MigUsage = deepCopyMigInUse(val.MigUsage)
			// 打印快照拷贝的调试日志
			klog.V(4).Infoln("getGPUDeviceSnapShot:", ret.Device[index].UsedMem, val.UsedMem, ret.Device[index].MigUsage, val.MigUsage)
		}
	}
	// 返回快照
	return &ret
}

// deepCopyMigInUse 深拷贝 MIG 占用结构（复制 Index 与 UsageList 切片内容）
func deepCopyMigInUse(src config.MigInUse) config.MigInUse {
	// 复制组索引
	dst := config.MigInUse{
		Index: src.Index,
	}

	// 预分配 UsageList 容量
	dst.UsageList = make(config.MIGS, len(src.UsageList))
	// 逐条拷贝 MIG 实例占用
	for i, usage := range src.UsageList {
		dst.UsageList[i] = config.MigTemplateUsage{
			Name:      usage.Name,                        // 实例名
			Memory:    usage.Memory,                      // 实例显存
			InUse:     usage.InUse,                       // 是否在使用
			UsedIndex: make([]int, len(usage.UsedIndex)), // 预分配已用索引列表
		}
		// 拷贝已用索引内容
		copy(dst.UsageList[i].UsedIndex, usage.UsedIndex)
	}

	// 返回快照结果
	return dst
}

// getSharingMode by default, we use hami core as the partitioning mode
// getSharingMode 把字符串形式的模式归一化为内部枚举；未知模式一律回退到 HAMICore
func getSharingMode(mode string) string {
	// 按模式字符串分支
	switch mode {
	case vGPUControllerMIG:
		return vGPUControllerMIG // 明确为 MIG
	default:
		return vGPUControllerHAMICore // 默认走 HAMICore
	}
}

// checkNodeGPUSharingPredicate checks if a pod with gpu requirement can be scheduled on a node.
// checkNodeGPUSharingPredicateAndScore 是核心的“试分配”函数：在设备集合上尝试把 Pod 的各容器 vGPU 请求
// 落到具体卡上（过程中会改动 UsedMem/UsedCore/UsedNum 等占用字段），返回是否可调度、分配到的设备详情、
// 累计打分与错误。
//
// 为什么需要 replicate 这个开关？——因为它要被调度的两个阶段复用，但必须产生不同副作用：
//  1. 预选（FilterNode）：调度器会对【每一个候选节点】都跑一遍“能不能放下”，这个过程必须无副作用。
//     若直接改真实缓存，测 node A 时就把它“假占用”了，哪怕 Pod 最后分到 node B，A 也会凭空少一块资源，
//     导致集群资源统计失真、误判资源不足。故预选传 replicate=true，在 getGPUDeviceSnapShot 生成的
//     快照副本上试算，函数返回后副本即被丢弃，真实缓存纹丝不动（只有算分这个标量被取出、存回 gs.Score）。
//  2. 绑定（Allocate）：调度器已选定节点，且只调用这一次，此时传 replicate=false，直接在真实缓存上
//     落账，把试分配时的占用真正提交，资源被永久占用。
//
// 一句话：replicate 把“反复试算的预选”和“一次性提交的绑定”隔开——同一套算法跑两遍，只有绑定那遍动账。
//
// ─────────────────────────────────────────────────────────────────────────────
// 【具体例子：用一份真实输入把整函数流程串一遍】
// 下面用一份贴近真实 HAMi 环境的输入，逐行对照上面的代码（建议开两个窗口对照着读）。
//
// 1) 节点当前注解（由 HAMi 设备插件写到节点上，由 decodeNodeDevices 解析）
//
//	Node.Status.Allocatable 至少应包含以下三个非零资源（NewGPUDevices 会先检查它们）：
//	  volcano.sh/vgpu-number: 2
//	  volcano.sh/vgpu-cores: 200
//	  volcano.sh/vgpu-memory: 48000
//
//	volcano.sh/node-vgpu-register:
//	  GPU-uuid-0,8,24000,NVIDIA-A100-SXM4-40GB,true,hami-core:GPU-uuid-1,8,24000,NVIDIA-A100-SXM4-40GB,true,hami-core
//	  单卡字段拆解（以第一个 GPU-uuid-0 为例；冒号后的 GPU-uuid-1 结构完全相同，多卡之间用冒号分隔）：
//	    ① UUID        : GPU-uuid-0        （设备唯一 ID）
//	    ② 共享上限     : 8                （Number，这张卡最多被切成几份；是上限，不是当前已切份数）
//	    ③ 总显存(MiB)  : 24000            （整卡显存容量，非当前已用显存）
//	    ④ 型号         : NVIDIA-A100-SXM4-40GB （GPU 型号；checkType 会检查它是否包含请求类型 NVIDIA）
//	    ⑤ 健康(布尔)   : true             （设备是否健康）
//	    ⑥ 共享模式     : hami-core         （HAMICore 细粒度切分；若写成 mig 则为 MIG 硬隔离）
//	    【重要】本注解只描述卡的静态规格（容量/上限/型号/健康/模式），不含“当前已用”字段。
//	          当前已用由调度器缓存维护，而非从节点读回：解析时 UsedMem/UsedCore/UsedNum 初值=0，
//	          分配 Pod 时经 Sharing.TryAddPod（累加 UsedNum/UsedMem/UsedCore）+ addToPodMap 写进 PodMap，
//	          Pod 删除/抢占时经 SubPod 扣减。即“当前已用”= 调度器自认为分配出去了多少，非 nvidia-smi 的真实占用。
//	  多张卡之间用冒号分隔。解析后得到：
//	    gs.Device = {
//	      0: {UUID:GPU-uuid-0, Number:8, Memory:24000, Type:"NVIDIA-A100-SXM4-40GB", Health:true, UsedNum:0, UsedMem:0, UsedCore:0, PodMap:{}},
//	      1: {UUID:GPU-uuid-1, Number:8, Memory:24000, ... 同上},
//	    }
//	    gs.Mode = vGPUControllerHAMICore ("hami-core")
//
// 2) 请求的 Pod（某容器 Resources.Limits）
//
//	volcano.sh/vgpu-number: 2      → 要 2 张卡
//	volcano.sh/vgpu-memory: 8000   → 每张卡 8000 MiB 显存
//	volcano.sh/vgpu-cores:  50     → 每张卡 50% 算力
//	（未写 volcano.sh/vgpu-memory-percentage → MemPercentagereq 默认 101，表示“没用百分比”）
//	  经 resourcereqs → ExtractResourceRequest 解析出一条容器请求：
//	    val = {Nums:2, Memreq:8000, MemPercentagereq:101, Coresreq:50, Type:"NVIDIA"}
//
// 3) 设 schedulePolicy="binpack"、replicate=true（预选阶段），逐步走：
//
//	① checkVGPUResourcesInPod → true（容器有 vgpu-memory limit）
//	② pod 没写 volcano.sh/vgpu-mode → ok=false → 跳过“模式必须一致”校验
//	③ ctrReq = [val]，len=1 ≠ 0
//	④ replicate=true → gs = getGPUDeviceSnapShot(gssnap)：两张卡 UsedNum/UsedMem/UsedCore=0，
//	   PodMap 深拷贝为空（此时 gs 是一份快照副本，下面的修改不影响真实缓存）
//	⑤ pod 没写 volcano.sh/vgpu-podgroup-policy=spread → currentPodGroupKey=""
//	⑥ 外层循环遍历 ctrReq（本例仅 1 个容器请求 val）：
//	     - val.Nums(2) <= len(gs.Device)(2) → 满足，不触发“卡数量不足”
//	     - sortedDeviceIndicesByPolicy(gs,"binpack")：两张卡 UsedMem 都是 0（并列），
//	       按 index 升序 → 候选顺序 [0, 1]
//	     - 遍历候选卡：
//	         i=0 (Device[0]):
//	           · 共享上限判断 Number(8) <= UsedNum(0)? → 否（8>0，卡远未满，不跳过，继续）
//	           · 同组分散 currentPodGroupKey=="" → 跳过该判断（本例无 PodGroup，无需分散）
//	           · memreqForCard: MemPercentagereq==101 → 走 else → 8000(MiB)
//	           · 剩余显存 24000-0=24000 >= 8000 → ✓（够）
//	           · 算力校验 UsedCore(0)+50=50 <= 100 → ✓（够）
//	           · 独占判断 Coresreq==100? → 否（本例要 50，不要求整卡独占）
//	           · 已满卡判断 UsedCore==100 && Coresreq==0? → 否（卡算力未满）
//	           · checkType: "NVIDIA-A100-SXM4-40GB" 含请求类型 "NVIDIA"，且 checkGPUtype 无黑白名单
//	             （nvidia.com/use-gputype / nvidia.com/nouse-gputype 均未设置）→ true
//	           · Sharing.TryAddPod(Device[0],8000,50)：HAMICore 模式只更新聚合计数器——
//	             UsedMem+=8000、UsedCore+=50、UsedNum+=1（注意：TryAddPod 不写 PodMap，
//	             真正登记 PodMap 要等绑定阶段的 addToPodMap）→ fit=true, uuid=GPU-uuid-0
//	           · 记 tentativeAllocs += {Device[0], mem:8000, core:50}（失败可回滚）
//	           · val.Nums>0 → val.Nums-- 变 1；devs += {UUID:GPU-uuid-0, Usedmem:8000, Usedcores:50}
//	           · score += GPUScore(binpack, Device[0]) = 100*(8000/24000) ≈ 33.3
//	           · val.Nums==0? 否 → 继续内层循环，去下一张卡
//	         i=1 (Device[1]): 与上完全相同 → fit=true, uuid=GPU-uuid-1；
//	           val.Nums-- 变 0；devs += {GPU-uuid-1,...}；score += 100*(8000/24000) ≈ 33.3
//	           （此时 Device[1] 也变成 UsedMem:8000/UsedCore:50/UsedNum:1，但都在快照上）
//	           val.Nums==0 → break 内层循环
//	     - 内层结束 val.Nums==0 → 不回滚；ctrdevs += devs（=两张卡）
//	⑦ 返回 (true, ctrdevs, score≈66.7, nil)
//
//	预选阶段到此结束：因为 gs 是快照，真实 gssnap 的 UsedMem/UsedCore/UsedNum 仍全为 0，
//	没有被污染；只有返回的 score 标量被 FilterNode 取出、赋给 gssnap.Score 用于后续选节点。
//
// 4) 绑定阶段（replicate=false）：同一套流程，但 gs=gssnap 本身，TryAddPod 直接改真实缓存，
//
//	Device[0]/[1] 的 UsedMem=8000/UsedCore=50/UsedNum=1 被真正提交；
//	最终 Allocate 调 encodePodDevices(ctrdevs) 把结果编码写入 Pod 注解 volcano.sh/vgpu-ids-new：
//	  "GPU-uuid-0,NVIDIA-A100-SXM4-40GB,8000,50:GPU-uuid-1,NVIDIA-A100-SXM4-40GB,8000,50"
//	（容器间以分号分隔、每张卡以冒号分隔、字段为 UUID,型号,显存,算力；HAMi 设备插件据此真正把容器绑到卡）
//
// 5) 失败/回滚分支示例（体会“原子试分配”）：
//
//	· 若 Pod 改成要 3 张卡：⑥开头 val.Nums(3) > len(gs.Device)(2) →
//	  rollbackTentative + return false，"no enough gpu cards on node"。
//	· 若某卡只剩 5000MiB 空闲：剩余显存判断 (24000-UsedMem) < memreqForCard(8000) → continue 跳过；
//	  若所有候选卡都放不下，内层结束 val.Nums 仍 >0 → rollbackTentative + return false，"not enough gpu fitted"。
//	在本例的 HAMICore 路径中，rollbackTentative 把前面试探成功写入的 UsedNum/UsedMem/UsedCore 逐条扣回去，
//	因此可以保证“要么全部分配成功、要么聚合账本完全不占用”。但 MIG 的 TryAddPod 还会修改 MigUsage，
//	当前 rollbackTentative 没有回滚 MigUsage，因此不能把这条原子性结论泛化到 MIG 路径。
//
// ─────────────────────────────────────────────────────────────────────────────
func checkNodeGPUSharingPredicateAndScore(pod *v1.Pod, gssnap *GPUDevices, replicate bool, schedulePolicy string) (bool, []ContainerDevices, float64, error) {
	// no gpu sharing request
	// 初始化累计打分
	score := float64(0)
	// 若 Pod 没有 vGPU 资源请求，直接判定可调度且无需分配设备
	if !checkVGPUResourcesInPod(pod) {
		return true, []ContainerDevices{}, 0, nil // 无请求，直接通过
	}

	// if the pod specify the sharing mode but the device is not in the same mode, return not fitted;
	// if the pod does not speficy the sharing mode, any device mode will be fitted
	// 读取 Pod 声明的共享模式（可选）；若声明了且与节点模式不一致则不可调度
	podSharingMode, ok := pod.Annotations[GPUModeAnnotation]
	// 声明了模式且不一致时返回错误
	if ok && podSharingMode != gssnap.Mode {
		return false, []ContainerDevices{}, 0, fmt.Errorf("pod required sharing mode %s is not the same as the node mode %s", podSharingMode, gssnap.Mode) // 模式不匹配
	}

	// 抽取 Pod 的 NVIDIA 设备资源请求列表
	ctrReq := resourcereqs(pod)
	// 没有解析出请求也直接判定可调度
	if len(ctrReq) == 0 {
		return true, []ContainerDevices{}, 0, nil // 无请求，直接通过
	}

	// 根据 replicate 决定操作对象：这是“试算”与“落账”的分水岭。
	// 预选阶段（replicate=true）必须对真实缓存零副作用，否则每测一个候选节点都会假占用资源、
	// 造成集群资源凭空消失；故在快照副本上跑试分配，函数返回后副本即弃，真实缓存不受影响。
	// 绑定阶段（replicate=false）则直接在真实缓存上正式占用，结果会持久化。
	var gs *GPUDevices
	// replicate 为真 ⇒ 用快照试算（无副作用）；为假 ⇒ 用真实缓存落账（占资源）
	if replicate {
		gs = getGPUDeviceSnapShot(gssnap)
	} else {
		gs = gssnap
	}
	// 初始化当前 Pod 的组键（用于 spread 同组分散）
	var currentPodGroupKey string
	// 仅当 Pod 声明了 PodGroup spread 策略时才计算组键
	if pod.Annotations[VGPUPodGroupPolicyAnnotation] == VGPUPodGroupPolicySpreadValue {
		currentPodGroupKey = getPodGroupKey(pod) // 计算所属 PodGroup 键
	}

	// 定义一次“试探性分配”的记录结构，便于失败时回滚
	type tentativeAlloc struct {
		device *GPUDevice // 被分配到的卡
		mem    uint       // 分配的显存
		core   uint       // 分配的算力
	}
	// 回滚函数：把试探分配的资源占用从卡上扣减回去
	rollbackTentative := func(allocs []tentativeAlloc) {
		// 遍历每条试探分配
		for _, alloc := range allocs {
			// 跳过空设备
			if alloc.device == nil {
				continue // 空设备跳过
			}
			// 回滚已分配数量（下限为 0）
			if alloc.device.UsedNum > 0 {
				alloc.device.UsedNum-- // 数量减 1
			}
			// 回滚已分配显存（不足则置 0）
			if alloc.device.UsedMem >= alloc.mem {
				alloc.device.UsedMem -= alloc.mem // 减去显存
			} else {
				alloc.device.UsedMem = 0 // 防止下溢
			}
			// 回滚已分配算力（不足则置 0）
			if alloc.device.UsedCore >= alloc.core {
				alloc.device.UsedCore -= alloc.core // 减去算力
			} else {
				alloc.device.UsedCore = 0 // 防止下溢
			}
		}
	}

	// 初始化试探分配记录与按容器的分配结果
	tentativeAllocs := []tentativeAlloc{}
	ctrdevs := []ContainerDevices{}
	// 遍历每个容器的设备请求
	for _, val := range ctrReq {
		// 本容器分配到的设备切片
		devs := []ContainerDevice{}
		// 请求的卡数量超过节点总卡数，直接回滚并返回不可调度
		if int(val.Nums) > len(gs.Device) {
			rollbackTentative(tentativeAllocs)                                                           // 回滚已试探分配
			return false, []ContainerDevices{}, 0, fmt.Errorf("no enough gpu cards on node %s", gs.Name) // 卡数量不足
		}
		// 打印当前容器分配的调试日志
		klog.V(3).InfoS("Allocating device for container", "request", val)

		// 按调度策略得到排序后的候选卡序号
		for _, i := range sortedDeviceIndicesByPolicy(gs, schedulePolicy) {
			// 打印该卡当前请求与占用状态的调试日志
			klog.V(3).InfoS("Scoring pod request", "memReq", val.Memreq, "memPercentageReq", val.MemPercentagereq, "coresReq", val.Coresreq, "Nums", val.Nums, "Index", i, "ID", gs.Device[i].ID)
			klog.V(3).InfoS("Current Device", "Index", i, "TotalMemory", gs.Device[i].Memory, "UsedMemory", gs.Device[i].UsedMem, "UsedCores", gs.Device[i].UsedCore, "UsedNum", gs.Device[i].UsedNum, "Number", gs.Device[i].Number, "replicate", replicate)
			// 该卡已达共享上限则跳过
			if gs.Device[i].Number <= uint(gs.Device[i].UsedNum) {
				continue // 共享数已满
			}
			// 若为同组 spread 策略且该卡已有同组占用，则跳过（分散放置）
			if currentPodGroupKey != "" && deviceHasPodFromSameGroup(gs.Device[i], currentPodGroupKey) {
				continue // 同组已占用，跳过
			}
			// 计算本卡单卡需满足的显存请求量
			memreqForCard := uint(0)
			// if we have mempercentage request, we ignore the mem request for every cards
			// 若指定了显存百分比，则按总显存乘以百分比计算单卡显存需求，忽略绝对显存值
			if val.MemPercentagereq != 101 {
				memreqForCard = uint(float64(gs.Device[i].Memory) * float64(val.MemPercentagereq) / 100.0)
			} else { // == 101 说明没有指定百分比，则按绝对显存请求量计算，101 是一个不可能的百分比值，作为标记
				memreqForCard = uint(val.Memreq) // 否则使用绝对显存请求
			}
			// 该卡剩余显存不足则跳过
			if int(gs.Device[i].Memory)-int(gs.Device[i].UsedMem) < int(memreqForCard) {
				continue // 剩余显存不够
			}
			// 该卡已分配算力加上本次请求会超过 100 则跳过
			if gs.Device[i].UsedCore+uint(val.Coresreq) > 100 {
				continue // 算力将超限
			}
			// Coresreq=100 indicates it want this card exclusively
			// 若请求独占整卡算力（cores=100），但卡已被占用则跳过
			if val.Coresreq == 100 && gs.Device[i].UsedNum > 0 {
				continue // 需要独占但卡非空
			}
			// You can't allocate core=0 job to an already full GPU
			// 不能把零算力请求分配到已满（算力已用满）的卡
			if gs.Device[i].UsedCore == 100 && val.Coresreq == 0 {
				continue // 卡算力已满且本请求无算力
			}
			// 类型（含型号黑白名单）校验不通过则跳过
			if !checkType(pod.Annotations, *gs.Device[i], val) {
				klog.Errorln("failed checktype", gs.Device[i].Type, val.Type) // 记录类型校验失败
				continue                                                      // 类型不匹配
			}
			// 调用共享处理器尝试把占用落到该卡（HAMICore 直接累加，MIG 找实例）
			fit, uuid := gs.Sharing.TryAddPod(gs.Device[i], memreqForCard, uint(val.Coresreq))
			// 试探失败则跳过该卡
			if !fit {
				klog.V(3).Info(gs.Device[i].ID, "not fit") // 记录不适配
				continue                                   // 该卡放不下
			}
			// 记录本次试探分配，便于失败回滚
			tentativeAllocs = append(tentativeAllocs, tentativeAlloc{
				device: gs.Device[i],       // 目标卡
				mem:    memreqForCard,      // 分配显存
				core:   uint(val.Coresreq), // 分配算力
			})
			//total += gs.Devices[i].Count
			//free += node.Devices[i].Count - node.Devices[i].Used
			// 若还有剩余卡数量需求，则扣减需求计数并记录分配到的设备
			if val.Nums > 0 {
				val.Nums--                            // 剩余需求减 1
				klog.V(3).Info("fitted uuid: ", uuid) // 记录命中的 UUID
				devs = append(devs, ContainerDevice{
					UUID:      uuid,               // 分配到的设备标识（物理 GPU UUID 或 MIG ID）
					Type:      val.Type,           // 设备类型
					Usedmem:   memreqForCard,      // 分配显存
					Usedcores: uint(val.Coresreq), // 分配算力
				})
				// 按策略累加该卡的打分
				score += GPUScore(schedulePolicy, gs.Device[i])
			}
			// 需求已满足则跳出候选卡循环
			if val.Nums == 0 {
				break // 本容器分配完成
			}
		}
		// 循环结束仍有未满足的卡需求，说明节点放不下，回滚并返回不可调度
		if val.Nums > 0 {
			rollbackTentative(tentativeAllocs)                                                      // 回滚已试探分配
			return false, []ContainerDevices{}, 0, fmt.Errorf("not enough gpu fitted on this node") // 节点容量不足
		}
		// 本容器分配成功，加入结果
		ctrdevs = append(ctrdevs, devs)
	}
	// 全部容器分配成功，返回可调度、设备详情、打分
	return true, ctrdevs, score, nil
}

// sortedDeviceIndicesByPolicy 按调度策略返回候选卡序号的遍历顺序
func sortedDeviceIndicesByPolicy(gs *GPUDevices, schedulePolicy string) []int {
	// 取得节点卡数量
	n := len(gs.Device)
	// 初始化序号切片
	idx := make([]int, 0, n)
	// 按策略分支排序
	switch schedulePolicy {
	case binpackPolicy:
		// binpack：优先挑“已用显存多”的卡（把任务压到更满的卡）
		for i := range gs.Device {
			idx = append(idx, i) // 收集所有序号
		}
		// 按已用显存降序排序，显存相同则按序号升序
		sort.Slice(idx, func(a, b int) bool {
			da, db := gs.Device[idx[a]], gs.Device[idx[b]]
			if da.UsedMem != db.UsedMem {
				return da.UsedMem > db.UsedMem // 已用显存多者优先
			}
			return idx[a] < idx[b] // 显存相同按序号
		})
	case spreadPolicy:
		// spread：优先挑“已共享数量少”的卡（把任务分散到更空的卡）
		for i := range gs.Device {
			idx = append(idx, i) // 收集所有序号
		}
		// 按已共享数量升序排序，数量相同则按序号升序
		sort.Slice(idx, func(a, b int) bool {
			da, db := gs.Device[idx[a]], gs.Device[idx[b]]
			if da.UsedNum != db.UsedNum {
				return da.UsedNum < db.UsedNum // 已共享少者优先
			}
			return idx[a] < idx[b] // 数量相同按序号
		})
	default:
		// 默认：逆序遍历（从大号卡到小号卡）
		for i := n - 1; i >= 0; i-- {
			idx = append(idx, i) // 逆序收集
		}
	}
	// 返回排序后的候选序号
	return idx
}

// GPUScore 根据调度策略计算单张卡对当前 Pod 的打分值
func GPUScore(schedulePolicy string, device *GPUDevice) float64 {
	// 初始化打分
	var score float64
	// 按策略分支
	switch schedulePolicy {
	case binpackPolicy:
		// binpack：已用显存占比越高打分越高（鼓励装箱）
		score = binpackMultiplier * (float64(device.UsedMem) / float64(device.Memory))
	case spreadPolicy:
		// spread：完全空闲的卡给予基础加分（鼓励分散）
		if device.UsedNum == 0 {
			score = spreadMultiplier // 空闲卡加分
		}
	default:
		// 默认策略不打分
		score = float64(0)
	}
	// 返回计算出的打分
	return score
}

// patchPodAnnotations 通过 StrategicMergePatch 把注解写回 Pod 对象
func patchPodAnnotations(kubeClient kubernetes.Interface, pod *v1.Pod, annotations map[string]string) error {
	// 定义补丁中的 metadata 结构（仅注解）
	type patchMetadata struct {
		Annotations map[string]string `json:"annotations,omitempty"`
	}
	// 定义顶层 Pod 补丁结构
	type patchPod struct {
		Metadata patchMetadata `json:"metadata"`
		//Spec     patchSpec     `json:"spec,omitempty"`
	}

	// 构造补丁对象并填入注解
	p := patchPod{}
	p.Metadata.Annotations = annotations

	// 序列化为 JSON
	bytes, err := json.Marshal(p)
	// 序列化失败直接返回错误
	if err != nil {
		return err // 返回序列化错误
	}
	// 调用 API 对该 Pod 执行 StrategicMergePatch
	_, err = kubeClient.CoreV1().Pods(pod.Namespace).
		Patch(context.Background(), pod.Name, k8stypes.StrategicMergePatchType, bytes, metav1.PatchOptions{})
	// 补丁执行失败记录错误（但仍返回 err 供上层判断）
	if err != nil {
		klog.Errorf("patch pod %v failed, %v", pod.Name, err) // 记录补丁失败
	}

	// 返回 API 调用结果（可能为 nil）
	return err
}

// getConfig 返回当前的 NVIDIA 设备配置；配置未加载时回退到默认配置
func getConfig() config.NvidiaConfig {
	// 若全局配置已加载则返回其 NvidiaConfig 段
	if config.GetConfig() != nil {
		return config.GetConfig().NvidiaConfig
	}
	// 否则返回默认设备配置中的 NvidiaConfig 段
	return config.GetDefaultDevicesConfig().NvidiaConfig
}
