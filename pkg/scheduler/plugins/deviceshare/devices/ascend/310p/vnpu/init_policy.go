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

// 本文件是昇腾 MindCluster VNPU（以 310P 为主，兼容多种芯片）在 deviceshare 插件里的
// 初始化与节点信息构建逻辑：把 ssn/node 信息灌入底层 vnpu.NPUDevices。
package vnpu310p

import (
	// errors 用于构造错误
	"errors"
	// fmt 用于格式化字符串
	"fmt"
	// reflect 用于反射判断结构体字段
	"reflect"
	// strconv 用于字符串与整数互转
	"strconv"
	// strings 用于字符串分割/前缀判断
	"strings"

	// yaml 用于配置（conf.Configuration）与内部 config.Configuration 互转
	"gopkg.in/yaml.v2"
	// v1 是 k8s 核心 Pod 类型
	v1 "k8s.io/api/core/v1"
	// klog 是 k8s 日志库
	"k8s.io/klog/v2"

	// api 是 volcano 调度器对节点/任务的抽象
	"volcano.sh/volcano/pkg/scheduler/api"
	// vnpu 是底层昇腾 MindCluster VNPU 设备插件包（NPUDevices/VChip/VResource 等）
	"volcano.sh/volcano/pkg/scheduler/api/devices/ascend/mindcluster/ascend310p/vnpu"
	// conf 是 volcano 调度配置类型
	"volcano.sh/volcano/pkg/scheduler/conf"
	// framework 是 volcano 调度框架（Session 类型）
	"volcano.sh/volcano/pkg/scheduler/framework"
	// k8s 是第三方 mindcluster 提供的 k8s 工具（设备/节点信息获取）
	"volcano.sh/volcano/third_party/mindcluster/common/k8s"
	// util 是第三方 mindcluster 公共工具/常量
	"volcano.sh/volcano/third_party/mindcluster/common/util"
	// config 是第三方 mindcluster 配置类型
	"volcano.sh/volcano/third_party/mindcluster/config"
	// plugin 是第三方 mindcluster 插件常量（虚拟芯片模板名等）
	"volcano.sh/volcano/third_party/mindcluster/plugin"
)

// InitVNPUDevice 是整个 VNPU 设备初始化的入口：填充框架上下文、CM informer 与节点信息。
func InitVNPUDevice(device *vnpu.NPUDevices, ssn *framework.Session, nodeInfo *api.NodeInfo) error {
	// 会话为空则直接报错返回
	if ssn == nil {
		klog.V(util.LogDebugLev).Infof("InitVNPUDevice failed: %s.", util.ArgumentError)
		return errors.New(util.ArgumentError)
	}

	klog.V(util.LogDebugLev).Infof("enter %s InitVNPUDevice.", "DeviceShare")
	defer klog.V(util.LogDebugLev).Infof("leave %s InitNPUSession.", "DeviceShare")

	// use information in ssn and nodeInfo to initialize device struct, and exclude api package
	// 用 ssn 与 nodeInfo 初始化设备结构（填充框架属性、模板、参数等）
	initVolcanoFrameFromSsn(device, ssn)

	// 初始化 ConfigMap informer（供 clusterd / device-plugin 使用）
	initCmInformer(device)

	// 用 ssn 里的节点信息初始化该节点的 VNPU 状态
	initNodeFromSsn(device, nodeInfo)

	return nil
}

// initCmInformer init cm informer, support cluster info manager and device plugin
// initCmInformer 初始化 ConfigMap informer，支撑集群信息管理与设备插件。
func initCmInformer(device *vnpu.NPUDevices) {
	// 若会话未携带 kube 客户端，告警并直接返回
	if device.FrameAttr.KubeClient == nil {
		klog.V(util.LogErrorLev).Info("kube client in session is nil")
		return
	}
	// 用 kube 客户端与是否使用 clusterd 标志初始化 CM informer
	k8s.InitCmInformer(device.FrameAttr.KubeClient, device.FrameAttr.UseClusterD)
}

// initVolcanoFrameFromSsn 用 ssn 填充框架属性，并载入虚拟芯片模板与动/静态参数。
func initVolcanoFrameFromSsn(device *vnpu.NPUDevices, ssn *framework.Session) {
	// 会话为空则告警返回
	if ssn == nil {
		klog.V(util.LogErrorLev).Infof("InitVolcanoFrameFromSsn failed: %s.", util.ArgumentError)
		return
	}

	// 把 volcano 调度配置转换为内部 config.Configuration 列表
	configs := getConfigurationByKey(initConfsFromSsn(ssn.Configurations))
	// 记录会话 UID
	device.FrameAttr.UID = ssn.UID
	// 记录 kube 客户端
	device.FrameAttr.KubeClient = ssn.KubeClient()
	// 记录 informer 工厂
	device.FrameAttr.InformerFactory = ssn.InformerFactory()
	// 构造各芯片型号的虚拟芯片模板（VJobTemplate）：型号→模板名→资源量
	device.FrameAttr.VJobTemplate = map[string]map[string]vnpu.VResource{
		util.Ascend310P: {
			plugin.VNPUTempVir01:        {Aicore: 1, Aicpu: 1, DVPP: plugin.AscendDVPPEnabledNull},
			plugin.VNPUTempVir02:        {Aicore: util.NPUIndex2, Aicpu: util.NPUIndex2, DVPP: plugin.AscendDVPPEnabledNull},
			plugin.VNPUTempVir02C1:      {Aicore: util.NPUIndex2, Aicpu: 1, DVPP: plugin.AscendDVPPEnabledNull},
			plugin.VNPUTempVir04:        {Aicore: util.NPUIndex4, Aicpu: util.NPUIndex4, DVPP: plugin.AscendDVPPEnabledNull},
			plugin.VNPUTempVir04C3:      {Aicore: util.NPUIndex4, Aicpu: util.NPUIndex3, DVPP: plugin.AscendDVPPEnabledNull},
			plugin.VNPUTempVir04C3NDVPP: {Aicore: util.NPUIndex4, Aicpu: util.NPUIndex3, DVPP: plugin.AscendDVPPEnabledOff},
			plugin.VNPUTempVir04C4cDVPP: {Aicore: util.NPUIndex4, Aicpu: util.NPUIndex4, DVPP: plugin.AscendDVPPEnabledOn},
		},
		util.Ascend910: {
			plugin.VNPUTempVir02: {Aicore: util.NPUIndex2, Aicpu: 1, DVPP: plugin.AscendDVPPEnabledNull},
			plugin.VNPUTempVir04: {Aicore: util.NPUIndex4, Aicpu: 1, DVPP: plugin.AscendDVPPEnabledNull},
			plugin.VNPUTempVir08: {Aicore: util.NPUIndex8, Aicpu: util.NPUIndex3, DVPP: plugin.AscendDVPPEnabledNull},
			plugin.VNPUTempVir16: {Aicore: util.NPUIndex16, Aicpu: util.NPUIndex7, DVPP: plugin.AscendDVPPEnabledNull},
		},
		plugin.ChipTypeB1: {
			plugin.VNPUTempVir06: {Aicore: util.NPUIndex6, Aicpu: util.NPUIndex1, DVPP: plugin.AscendDVPPEnabledNull},
			plugin.VNPUTempVir03: {Aicore: util.NPUIndex3, Aicpu: util.NPUIndex1, DVPP: plugin.AscendDVPPEnabledNull},
			plugin.VNPUTempVir12: {Aicore: util.NPUIndex12, Aicpu: util.NPUIndex3, DVPP: plugin.AscendDVPPEnabledNull},
		},
		plugin.ChipTypeB2C: {
			plugin.VNPUTempVir06: {Aicore: util.NPUIndex6, Aicpu: util.NPUIndex1, DVPP: plugin.AscendDVPPEnabledNull},
			plugin.VNPUTempVir03: {Aicore: util.NPUIndex3, Aicpu: util.NPUIndex1, DVPP: plugin.AscendDVPPEnabledNull},
			plugin.VNPUTempVir12: {Aicore: util.NPUIndex12, Aicpu: util.NPUIndex3, DVPP: plugin.AscendDVPPEnabledNull},
		},
		plugin.ChipTypeB2: {
			plugin.VNPUTempVir06: {Aicore: util.NPUIndex6, Aicpu: util.NPUIndex1, DVPP: plugin.AscendDVPPEnabledNull},
			plugin.VNPUTempVir03: {Aicore: util.NPUIndex3, Aicpu: util.NPUIndex1, DVPP: plugin.AscendDVPPEnabledNull},
			plugin.VNPUTempVir12: {Aicore: util.NPUIndex12, Aicpu: util.NPUIndex3, DVPP: plugin.AscendDVPPEnabledNull},
		},
		plugin.ChipTypeB3: {
			plugin.VNPUTempVir05: {Aicore: util.NPUIndex5, Aicpu: util.NPUIndex1, DVPP: plugin.AscendDVPPEnabledNull},
			plugin.VNPUTempVir10: {Aicore: util.NPUIndex10, Aicpu: util.NPUIndex3, DVPP: plugin.AscendDVPPEnabledNull},
		},
		plugin.ChipTypeB4: {
			plugin.VNPUB4TempVir05:     {Aicore: util.NPUIndex5, Aicpu: util.NPUIndex1, DVPP: plugin.AscendDVPPEnabledNull},
			plugin.VNPUB4TempVir10C3NM: {Aicore: util.NPUIndex10, Aicpu: util.NPUIndex3, DVPP: plugin.AscendDVPPEnabledOff},
			plugin.VNPUB4TempVir10C4M:  {Aicore: util.NPUIndex10, Aicpu: util.NPUIndex4, DVPP: plugin.AscendDVPPEnabledOn},
			plugin.VNPUB4TempVir10:     {Aicore: util.NPUIndex10, Aicpu: util.NPUIndex3, DVPP: plugin.AscendDVPPEnabledNull},
		},
	}
	// 初始化动态参数（依赖配置）
	initDynamicParameters(device, configs)
	// 初始化静态参数（只做一次）
	initStaticParameters(device, configs)
}

// initStaticParameters
// initStaticParameters 初始化只需执行一次的静态参数（依赖 sync.Once）。
func initStaticParameters(device *vnpu.NPUDevices, configs map[string]string) {
	// 用 OnceInit 保证只初始化一次
	device.FrameAttr.OnceInit.Do(func() {
		device.FrameAttr.UseClusterD = getUseClusterDConfig(configs)
		device.FrameAttr.SelfMaintainAvailCard = getSelfMaintainAvailCard(configs)
		klog.V(util.LogWarningLev).Infof("init static parameters, UseClusterD"+
			" is <%v>", device.FrameAttr.UseClusterD)
	})
}

// getUseClusterDConfig check use cluster info manager by config, default true
// getUseClusterDConfig 读取是否使用集群信息管理器（clusterd），默认 true。
func getUseClusterDConfig(conf map[string]string) bool {
	// 读取配置项
	useClusterInfoManager, ok := conf[util.UseClusterInfoManager]
	if !ok {
		klog.V(util.LogDebugLev).Info("CheckUseCIMByConfig doesn't exist useClusterInfoManager.")
		// 缺省视为使用
		return true
	}
	// 显式配置为 "true" 才返回 true
	return useClusterInfoManager == "true"
}

// getSelfMaintainAvailCard check volcano self maintain available card by config, default true
// getSelfMaintainAvailCard 读取 volcano 是否自维护可用卡列表，默认 true。
func getSelfMaintainAvailCard(conf map[string]string) bool {
	// 读取配置项
	selfMaintainAvailCard, ok := conf[util.SelfMaintainAvailCard]
	if !ok {
		klog.V(util.LogDebugLev).Info("CheckUseCIMByConfig doesn't exist self-maintain-available-card.")
		// 缺省视为自维护
		return true
	}
	// 显式为 "true" 才返回 true
	return selfMaintainAvailCard == "true"
}

// initDynamicParameters
// initDynamicParameters 初始化动态参数（可在每次初始化时刷新）。
func initDynamicParameters(device *vnpu.NPUDevices, configs map[string]string) {
	// 设备/配置为空则告警返回
	if device == nil || configs == nil {
		klog.V(util.LogInfoLev).Infof("InitCache failed: %s.", util.ArgumentError)
		return
	}
	// 读取预置虚拟设备（静态切分）开关
	device.FrameAttr.PresetVirtualDevice = getPresetVirtualDeviceConfig(configs)
}

// getPresetVirtualDeviceConfig get VNPU segmentEnable by init plugin parameters, return true if static
// getPresetVirtualDeviceConfig 读取 VNPU 是否启用预置（静态）切分，默认 false。
func getPresetVirtualDeviceConfig(conf map[string]string) bool {
	// get segmentEnable by user configuration
	// 读取切分使能配置项
	segmentEnable, ok := conf[util.SegmentEnable]
	if !ok {
		klog.V(util.LogDebugLev).Info("checkVNPUSegmentEnable doesn't exist presetVirtualDevice.")
		// 缺省为 false（动态切分）
		return false
	}
	// 显式为 "true" 才返回 true
	return segmentEnable == "true"
}

// initConfsFromSsn init confs from session
// initConfsFromSsn 把 volcano 的 conf.Configuration 列表转换为内部 config.Configuration 列表（YAML 中转）。
func initConfsFromSsn(confs []conf.Configuration) []config.Configuration {
	// 用于 YAML 序列化
	var out []byte
	var err error
	// 预分配目标切片
	newConfs := make([]config.Configuration, len(confs))
	for idx, cfg := range confs {
		// 构造目标结构体指针
		newCfg := &config.Configuration{}
		// 先把 volcano 配置序列化为 YAML
		out, err = yaml.Marshal(cfg)
		if err != nil {
			klog.V(util.LogInfoLev).Infof("Marshal configuration failed: %s.", err)
			// 失败则跳过该项
			continue
		}
		// 再反序列化到内部配置结构
		if err = yaml.Unmarshal(out, newCfg); err != nil {
			klog.V(util.LogInfoLev).Infof("Unmarshal configuration failed: %s.", err)
			continue
		}
		// 写入对应位置
		newConfs[idx] = *newCfg
	}
	// 返回转换后的配置列表
	return newConfs
}

// getConfigurationByKey called by GetConfigFromSchedulerConfigMap
// getConfigurationByKey 在配置列表里找到名为 CMInitParamKey 的那条，返回其参数 map。
func getConfigurationByKey(configurations []config.Configuration) map[string]string {
	// 遍历查找指定名字的配置
	for _, cf := range configurations {
		if cf.Name == util.CMInitParamKey {
			return cf.Arguments
		}
	}
	// 没找到返回空 map
	return map[string]string{}
}

// initNodeFromSsn 用 ssn 里的节点信息初始化该节点的 VNPU 设备状态。
func initNodeFromSsn(device *vnpu.NPUDevices, nodeInfo *api.NodeInfo) {
	klog.V(util.LogDebugLev).Infof("Entering initNodeFron Ssn function")

	// 1.obtain device infos, and if node not in session, its device info should not keep in cache
	// 1. 获取设备信息（含 clusterd 与自维护可用卡逻辑）
	deviceInfos := k8s.GetDeviceInfoAndSetInformerStart(nodeInfo, device.FrameAttr.UseClusterD,
		device.FrameAttr.SelfMaintainAvailCard)
	// 2. obtain node infos of noded configmap
	// 2. 获取 noded 的节点信息（来自 ConfigMap）
	nodeInfosOfNodeD := k8s.GetNodeDInfo(nodeInfo)
	// 3. obtain switch infos of switch configmap
	//switchInfos := k8s.GetSwitchInfos(nodeInfo)

	// 用设备信息、noded 信息与模板初始化该节点；若出错且非“无资源”错误则告警（仍不放入节点列表）
	if err := initNPUNodeByNodeInf(device, nodeInfo, deviceInfos, nodeInfosOfNodeD, device.FrameAttr.VJobTemplate); err != nil &&
		!strings.Contains(err.Error(), vnpu.NoneResourceErr) {
		klog.V(util.LogErrorLev).Infof("InitNodeFromSsn %s %s, not put in nodes.", nodeInfo.Name, err)
	}
}

// initNPUNodeByNodeInf init NPU node from node info and cm.
// initNPUNodeByNodeInf 综合节点信息与 CM 数据，构建该节点的 NPU 设备状态。
func initNPUNodeByNodeInf(device *vnpu.NPUDevices, npuNode *api.NodeInfo, deviceInfo k8s.NodeDeviceInfoWithID,
	nodeInfoOfNodeD k8s.NodeDNodeInfo, vJobTemplate map[string]map[string]vnpu.VResource) error {
	klog.V(util.LogDebugLev).Infof("Entering initNPUNodeByNodeInf function")

	// 设备或节点为空则报错
	if device == nil || npuNode == nil {
		klog.V(util.LogInfoLev).Infof("InitNPUNodeByNodeInf failed: %s.", util.ArgumentError)
		return errors.New(util.ArgumentError)
	}
	// 取出节点的 NPU 容量（capacity）
	capability := getNPUNodeCapacity(npuNode)
	// 若容量里不含华为 NPU 资源，说明未启用 NPU，报错
	if !util.IsMapHasNPUResource(capability, util.HwPreName) {
		return fmt.Errorf("node %s npu resource is not enable", npuNode.Name)
	}
	// 设备列表为空（未启用 clusterd/设备信息）报错
	if deviceInfo.DeviceList == nil {
		return fmt.Errorf("node %s device info or clusterd info is not enable", npuNode.Name)
	}
	// 记录节点名
	device.NodeInf.Name = npuNode.Name
	// 记录 NPU 容量
	device.Capability = capability
	// 记录基础设备信息注解
	device.BaseDeviceInfo = npuNode.Node.Annotations[util.BaseDeviceInfoKey]
	// 记录可分配资源里的标量资源
	device.NodeInf.Allocate = npuNode.Allocatable.ScalarResources
	// 记录空闲资源里的标量资源
	device.Idle = npuNode.Idle.ScalarResources
	// 记录节点标签
	device.Label = npuNode.Node.Labels
	// 记录节点 IP 地址
	device.Address = getNPUNodeAddress(npuNode)
	//device.Tasks = npuNode.Tasks
	// 同步注解（节点注解 + 历史设备信息 + noded 状态）
	syncAnnotation(device, npuNode, nodeInfoOfNodeD)
	// 用设备信息更新底层设备列表缓存
	updateNPUNodeDeviceInfos(device, deviceInfo)

	// 若设置 VNPU 信息出错则打调试日志（不阻断）
	if setVNPUErr := setNodeVNPUInfo(device, npuNode, vJobTemplate); setVNPUErr != nil {
		klog.V(util.LogDebugLev).Infof("setNodeVNPUInfo %s %s", npuNode.Name, setVNPUErr)
	}
	klog.V(util.LogDebugLev).Infof("initNPUNodeByNodeInf <%s> success %#v", npuNode.Name, device.NodeInf)
	return nil
}

// setNodeVNPUInfo 设置该节点的 VNPU 具体信息：芯片属性、总资源/芯片数、虚拟芯片。
func setNodeVNPUInfo(device *vnpu.NPUDevices, ni *api.NodeInfo, jobTemplate map[string]map[string]vnpu.VResource) error {
	// 若动态 VNode 资源未初始化则报错
	if !checkDyVNodeResourceInitialized(device) {
		return fmt.Errorf("setNodeVNPUInfo %s: DyVNode resource not initialized", device.NodeInf.Name)
	}

	// 1. get chipKind like Ascend910, chipLabel like Ascend310P-8
	// 1. 设置芯片型号（chipKind）等属性
	if err := setChipPropertiesFromNPUNode(device); err != nil {
		return fmt.Errorf("setNodeVNPUInfo %s: %v", device.NodeInf.Name, err)
	}

	// 2. get resource capacity, totalChipNum, freeChipNum
	// 2. 设置总资源、总芯片数、空闲芯片数
	if err := setTotalResAndChipNumByTemplates(device); err != nil {
		return fmt.Errorf("setNodeVNPUInfo node %s: %v", device.NodeInf.Name, err)
	}

	// 3. create vChips on node and update vChip resource
	// 3. 在节点上创建虚拟芯片并更新其资源
	if err := initVChips(device, ni, jobTemplate); err != nil {
		return fmt.Errorf("setNodeVNPUInfo node %s: %v", device.NodeInf.Name, err)
	}

	// 标记为有效 VNode
	device.ValidVNode = true
	klog.V(util.LogDebugLev).Infof("setNodeVNPUInfo %s initialisation success:<%#v>", device.NodeInf.Name, device.NPUDevice)
	return nil
}

// initVChips 创建节点上的虚拟芯片（vChip）并把已有 Pod 的资源计入。
func initVChips(device *vnpu.NPUDevices, ni *api.NodeInfo, taskTemplate map[string]map[string]vnpu.VResource) error {
	// 取单芯片总资源
	chipTotalRes := getVChipTotalRes(device)
	// 3. create new VChip by freeCardID whole card
	// 按空闲整卡创建新 vChip
	if err := createNodeNewVChips(device, chipTotalRes); err != nil {
		klog.V(util.LogDebugLev).Infof("vNode %s %s.", device.NodeInf.Name, util.SafePrint(err))
	}
	// 标记不健康芯片
	if err := setUnhealthyChipIds(device); err != nil {
		klog.V(util.LogDebugLev).Infof("vNode %s %s.", device.NodeInf.Name, err)
	}
	// 4. update VChips and create VChips for chips being occupied
	// 遍历节点任务，把 NPU 任务资源累加进对应 vChip
	for _, ti := range ni.Tasks {
		if !isNPUTask(ti) {
			// 非 NPU 任务跳过
			continue
		}
		addNPUResource(device, ti.Pod, chipTotalRes, taskTemplate)
	}

	return nil
}

// addNPUResource update all pod resource to node
// addNPUResource 把一个 Pod 占用的 NPU 资源更新到节点（整机卡或 VNPU 卡分别处理）。
func addNPUResource(device *vnpu.NPUDevices, pod *v1.Pod, chipTotalRes vnpu.VResource,
	taskTemplate map[string]map[string]vnpu.VResource) {
	// 读取 AscendNPUCore 注解
	coreNameStr, ok := pod.Annotations[util.AscendNPUCore]
	if !ok {
		klog.V(util.LogDebugLev).Infof("addNPUResource pod %s %s no value", pod.Name, util.AscendNPUCore)
		return
	}

	// 若注解表明是整机卡占用，走整机卡逻辑
	if isPodWholeCardFromAscendCore(coreNameStr) {
		addNPUResourceWholeCard(device, pod)
		return
	}
	// 否则按 VNPU 卡（虚拟切分）处理
	addNPUResourceVNPUCard(device, pod, chipTotalRes, taskTemplate)
}

// addNPUResourceVNPUCard ascendStr Ascend310P-4c.3cpu.ndvpp-100(VNPUID)-1(physic ID)_1（vgroupID）
// addNPUResourceVNPUCard 把一个 VNPU（虚拟切分）卡 Pod 的资源计入对应 vChip。
func addNPUResourceVNPUCard(device *vnpu.NPUDevices, pod *v1.Pod, chipTotalRes vnpu.VResource,
	taskTemplate map[string]map[string]vnpu.VResource) {
	// 1. get physics id
	// 1. 取物理卡 ID
	physicsID, err := getCardPhysicsIDFromAscendCore(pod, false)
	if err != nil || len(physicsID) != util.NPUIndex1 {
		klog.V(util.LogErrorLev).Infof("addNPUResourceVNPUCard get pod<%s> card physics id failed", pod.Name)
		return
	}
	// 若该物理卡不健康则跳过
	_, isCardunhealthy := device.UnhealthyChipIds[physicsID[0]]
	if isCardunhealthy {
		klog.V(util.LogErrorLev).Infof("addNPUResourceVNPUCard get pod<%s> card is unhealthy", pod.Name)
		return
	}
	// 2. add chip to node
	// 2. 取出或新建该物理卡的 vChip
	curVChip, ok := device.Chips[physicsID[0]]
	if !ok {
		curVChip = NewVChip(device, physicsID[0], chipTotalRes)
		device.Chips[physicsID[0]] = curVChip
	}
	// 标记不稳定（若本 Pod 资源不稳定或之前已不稳定）
	curVChip.Unstable = curVChip.IsPodResUnstable(pod) || curVChip.Unstable
	// 记录真实使用的卡 ID
	curVChip.AddRealCardID(pod.Annotations[util.AscendNPUPodRealUse])
	// 把 Pod 加入 vChip 的 PodMap
	curVChip.AddPodToPodMap(pod)
	// 标记该卡使用切分
	curVChip.SetSegmentFlag(true)

	// 3. get resource of pod
	// 3. 解析 Pod 占用的资源量
	podVResource := getPodUsedRes(device, pod, taskTemplate)
	if podVResource == nil {
		klog.V(util.LogErrorLev).Infof("addNPUResource resolving pod<%s> resource failed", pod.Name)
		return
	}
	// 4. update node properties
	// 4. 更新 vChip 的资源账本：已用加、空闲减、刷新 DVPP
	curVChip.UsedRes.Add(*podVResource)
	curVChip.FreeRes.Sub(*podVResource)
	curVChip.UpdateDVPP(podVResource.DVPP)
}

// getPodUsedRes 解析一个 Pod 占用的 VNPU 资源（从注解 + 模板查表）。
func getPodUsedRes(device *vnpu.NPUDevices, pod *v1.Pod, taskTemplate map[string]map[string]vnpu.VResource) *vnpu.VResource {
	// 读取 AscendNPUCore 注解
	realStr, ok := pod.Annotations[util.AscendNPUCore]
	if !ok {
		klog.V(util.LogErrorLev).Infof("getPodUsedRes get pod<%s> %s value failed", pod.Name,
			util.AscendNPUCore)
		return nil
	}
	// 按 "-" 拆分（如 Ascend310P-4c3cpu...）
	ascendRealSplit := strings.Split(realStr, "-")
	if len(ascendRealSplit) != util.NPUIndex2 {
		klog.V(util.LogErrorLev).Infof("getPodUsedRes get pod<%s> %s format error", pod.Name, realStr)
		return nil
	}
	// 310P 用 ChipKind 查模板，其它用 ChipType
	if device.ChipKind == util.Ascend310P {
		return getResourceFromTemplate(device.ChipKind, ascendRealSplit[1], taskTemplate)
	}
	return getResourceFromTemplate(device.ChipType, ascendRealSplit[1], taskTemplate)
}

// getResourceFromTemplate nodeType like Ascend310P, templateString like "vir04_3c_ndvpp"
// getResourceFromTemplate 按芯片型号与模板名从模板表查出资源量。
func getResourceFromTemplate(nodeType string, templateString string,
	taskTemplate map[string]map[string]vnpu.VResource) *vnpu.VResource {
	// 先按型号取模板表
	taskNodeTemplate, ok := taskTemplate[nodeType]
	if !ok {
		return nil
	}
	// 再按模板名取资源
	taskResource, ok := taskNodeTemplate[templateString]
	if !ok {
		return nil
	}
	// 返回该资源的指针
	return &taskResource
}

// NewVChip create new vChip
// NewVChip 创建一个物理卡对应的虚拟芯片（vChip），初始化资源账本。
func NewVChip(device *vnpu.NPUDevices, id int, totalRes vnpu.VResource) *vnpu.VChip {
	// 设备为空则报错返回 nil
	if device == nil {
		klog.V(util.LogDebugLev).Infof("NewVChip failed: %s", util.ArgumentError)
		return nil
	}
	// 虚拟芯片名：ChipKind-编号
	chipName := device.ChipKind + "-" + strconv.Itoa(id)
	vChip := vnpu.VChip{
		PodMap:   make(map[string]*v1.Pod, util.MapInitNum),
		Name:     chipName,
		Kind:     device.ChipKind,
		CoreNum:  device.AiCorePerChip,
		TotalRes: totalRes,
		FreeRes:  totalRes,
	}
	// 初始 DVPP 状态设置
	vChip.TotalRes.DVPP = plugin.AscendDVPPEnabledOff
	vChip.UsedRes.DVPP = plugin.AscendDVPPEnabledOff
	vChip.FreeRes.DVPP = plugin.AscendDVPPEnabledOn

	// 双卡服务器标记
	if strings.HasPrefix(device.ServerType, util.ServerTypeDual) {
		vChip.SetIsDual(true)
	}

	// 返回新 vChip 指针
	return &vChip
}

// getCardPhysicsIDFromAscendCore get card physics id from 0,1/0-vir04
// getCardPhysicsIDFromAscendCore 从 AscendNPUCore 注解解析物理卡 ID（整卡或 VNPU 两种形式）。
func getCardPhysicsIDFromAscendCore(pod *v1.Pod, isWholeCard bool) ([]int, error) {
	// 收集解析出的 ID
	physicsIDs := make([]int, 0)
	// Pod 为空报错
	if pod == nil {
		return physicsIDs, fmt.Errorf("pod is nil")
	}
	// 读取注解
	coreNameStr, ok := pod.Annotations[util.AscendNPUCore]
	if !ok {
		return physicsIDs, fmt.Errorf("getCardPhysicsIDFromAscendCore vnpu device <%s> get %s value failed",
			pod.Name, util.AscendNPUCore)
	}

	// 非整卡：从 "0-vir04" 这种形式取物理卡号
	if !isWholeCard {
		phyCardID, err := getVNPUCardIDFromAscendCore(coreNameStr)
		if err != nil {
			return physicsIDs, fmt.Errorf("getCardPhysicsIDFromAscendCore vnpu device <%s> get id failed",
				coreNameStr)
		}
		physicsIDs = append(physicsIDs, phyCardID)
		return physicsIDs, nil
	}
	// 整卡：按 "," 拆多个卡号（如 Ascend910-0,Ascend910-1）
	coreNameSplit := strings.Split(coreNameStr, ",")
	for _, id := range coreNameSplit {
		phyCardID, err := strconv.Atoi(id)
		if err != nil {
			return physicsIDs, fmt.Errorf("getCardPhysicsIDFromAscendCore device <%s> get physics id failed",
				coreNameStr)
		}
		physicsIDs = append(physicsIDs, phyCardID)
	}
	return physicsIDs, nil
}

// getVNPUCardIDFromAscendCore 从 "0-vir04" 形式解析出物理卡号（取 "-" 前部分）。
func getVNPUCardIDFromAscendCore(coreNameStr string) (int, error) {
	// 按 "-" 拆分
	coreNameSplit := strings.Split(coreNameStr, "-")
	if len(coreNameSplit) != util.NPUIndex2 {
		return 0, fmt.Errorf("getVNPUCardIDFromAscendCore vnpu real device <%s> format error", coreNameStr)
	}
	// 把前半部分转成整数
	phyCardID, err := strconv.Atoi(coreNameSplit[0])
	if err != nil {
		return 0, fmt.Errorf("getVNPUCardIDFromAscendCore vnpu device <%s> get physics id failed", coreNameStr)
	}
	return phyCardID, nil
}

// addNPUResourceWholeCard Ascend910-0,Ascend910-1
// addNPUResourceWholeCard 把整机卡（多卡）Pod 的资源计入对应 vChip。
func addNPUResourceWholeCard(device *vnpu.NPUDevices, pod *v1.Pod) {
	// 解析整卡物理 ID 列表
	physicsID, err := getCardPhysicsIDFromAscendCore(pod, true)
	if err != nil || len(physicsID) == 0 {
		return
	}
	for _, id := range physicsID {
		// 跳过不健康的卡
		_, isCardunhealthy := device.UnhealthyChipIds[id]
		if isCardunhealthy {
			continue
		}
		// 1. get resource of pod, which is chip total resource
		// 1. 整机卡占用即占满整卡总资源
		podVResource := getVChipTotalRes(device)

		// 2. get chip id
		// 2. 取或新建该卡 vChip
		curVChip, ok := device.Chips[id]
		if !ok {
			curVChip = NewVChip(device, id, podVResource)
			device.Chips[id] = curVChip
		}

		// 3. update node
		// 3. 更新 vChip：标记不稳定、记录真实卡、加入 PodMap、占满资源
		curVChip.Unstable = curVChip.IsPodResUnstable(pod) || curVChip.Unstable
		curVChip.AddRealCardID(strconv.Itoa(id))
		curVChip.AddPodToPodMap(pod)
		curVChip.UsedRes.Add(podVResource)
		curVChip.FreeRes.Sub(podVResource)
	}
}

// isPodWholeCardFromAscendCore judge if card is whole card 0,1/0-vir04
// isPodWholeCardFromAscendCore 判断注解表示的是否为整机卡（无 "-" 即整机卡）。
func isPodWholeCardFromAscendCore(coreCardName string) bool {
	// 按 "," 拆多卡
	temp := strings.Split(coreCardName, ",")
	for _, cardName := range temp {
		// 单卡按 "-" 拆，长度 1 说明是整机卡
		singleCardTemp := strings.Split(cardName, "-")
		if len(singleCardTemp) == util.NPUIndex1 {
			return true
		}
	}
	return false
}

// isNPUTask to judge the task either is NPU task or not.
// isNPUTask 判断一个任务是否为 NPU 任务（资源里含 huawei.com/ 前缀）。
func isNPUTask(nT *api.TaskInfo) bool {
	// 遍历标量资源种类
	for k := range nT.Resreq.ScalarResources {
		// must contain "huawei.com/"
		// 含华为前缀即视为 NPU 资源
		if strings.Contains(string(k), util.HwPreName) {
			return true
		}
	}
	return false
}

// setUnhealthyChipIds 把本节点不健康的芯片 ID 收集进 UnhealthyChipIds。
func setUnhealthyChipIds(device *vnpu.NPUDevices) error {
	// 从节点/设备信息取不健康卡 ID
	unhealthyCardIDs, getErr := getCardIDsFromNodeAndDeviceInfo(device, vnpu.UnhealthyCardSuffix)
	if getErr != nil {
		return fmt.Errorf("getFreeCardIDsFromDeviceInfo %s", getErr)
	}
	for _, unhealthyCardID := range unhealthyCardIDs {
		// 记录为不健康
		device.NPUDevice.UnhealthyChipIds[unhealthyCardID] = struct{}{}
	}
	return nil
}

// getCardIDsFromNodeAndDeviceInfo 从节点注解里解析出某类（健康/不健康）芯片 ID 列表。
func getCardIDsFromNodeAndDeviceInfo(device *vnpu.NPUDevices, cardHealthTypeSuffix string) ([]int, error) {
	// 1. get health chips
	// 1. 拼出注解 key：huawei.com/<ChipKind><后缀>
	ChipsStr, ok := device.Annotation[util.HwPreName+device.NPUDevice.ChipKind+cardHealthTypeSuffix]
	if !ok {
		klog.V(util.LogDebugLev).Infof("%s get healthy card failed", device.NodeInf.Name)
		return nil, fmt.Errorf("no key: %s", util.HwPreName+device.NPUDevice.ChipKind)
	}

	// 按 "," 拆卡号字符串
	CardIDs := make([]int, 0)
	Chips := strings.Split(ChipsStr, ",")
	for _, chip := range Chips {
		if chip == "" {
			// 跳过空串
			continue
		}
		// 去掉 "ChipKind-" 前缀得到纯数字
		strID := strings.TrimPrefix(chip, device.NPUDevice.ChipKind+"-")
		// 转整数
		chipID, aErr := strconv.Atoi(strID)
		if aErr != nil {
			klog.V(util.LogDebugLev).Infof("%s %s covert to int %s", chip, strID, util.SafePrint(aErr))
			continue
		}
		CardIDs = append(CardIDs, chipID)
	}

	// 解析结果为空则报错
	if len(CardIDs) == 0 {
		return nil, fmt.Errorf("nil cards in %s", device.NodeInf.Name)
	}
	return CardIDs, nil
}

// createNodeNewVChips 按健康卡列表为本节点每个卡创建 vChip。
func createNodeNewVChips(device *vnpu.NPUDevices, chipTotalRes vnpu.VResource) error {
	// 取健康卡 ID 列表
	healthyCardIDs, getErr := getCardIDsFromNodeAndDeviceInfo(device, vnpu.CardHealthySuffix)
	if getErr != nil {
		return fmt.Errorf("getFreeCardIDsFromDeviceInfo %s", util.SafePrint(getErr))
	}
	klog.V(util.LogDebugLev).Infof("createNodeNewVChips healthy chips: %#v", healthyCardIDs)
	for _, freeCardID := range healthyCardIDs {
		// 为每个健康卡创建 vChip 并登记
		device.NPUDevice.Chips[freeCardID] = NewVChip(device, freeCardID, chipTotalRes)
	}
	return nil
}

// setTotalResAndChipNumByTemplates set totalRes, totalChipNum and serverType
// setTotalResAndChipNumByTemplates 依据容量与模板计算总资源、总芯片数与服务器类型。
func setTotalResAndChipNumByTemplates(device *vnpu.NPUDevices) error {
	// 1. get and set total AiCore from capability like Capacity: huawei.com/npu-core: 56
	// 1. 从容量取总 AiCore（npu-core），换算成芯片总核数
	totalCore, ok := device.Capability[util.AscendNPUCore]
	if !ok {
		return fmt.Errorf("getTotalResFromNpuNode no resource <%s>", util.AscendNPUCore)
	}

	// 总核数除以 1000 得到每张卡的 AiCore 总数（NPUHexKilo=1000）
	device.NPUDevice.TotalRes.Aicore = int(totalCore / util.NPUHexKilo)
	klog.V(util.LogDebugLev).Infof("DEBUG: node %s, totalCore from Capability: %f", device.NodeInf.Name, totalCore)
	klog.V(util.LogDebugLev).Infof("DEBUG: node %s, after division: %d", device.NodeInf.Name, int(totalCore/util.NPUHexKilo))

	// 取每芯片的 AiCore 数
	numCorePerChip, err := getVChipCoreNum(device)
	if err != nil || numCorePerChip == 0 {
		return fmt.Errorf("getTotalChipNum error: %v or numCorePerChip zero number: %d",
			util.SafePrint(err), numCorePerChip)
	}
	device.AiCorePerChip = numCorePerChip

	// 2.2 get totalChipNum use totalChipNum = totalCoreNum / coreNumPerChip
	// 2.2 总芯片数 = 总 AiCore / 每芯片 AiCore
	totalChipNum, err := getTotalChipNum(device)
	if err != nil {
		return fmt.Errorf("getTotalResFromNpuNode failed: %v", err)
	}
	device.NPUDevice.TotalChipNum = totalChipNum

	// 2.2 get cpuNum per chip use totalAiCpuNum = aicpuNumPerChip * totalChipNum
	// 2.2 每芯片 AiCpu 数由模板查得，再乘总芯片数得到总 AiCpu
	templates := initTemplate()
	cpuPerChip := getCpuNumPerChip(device, templates)
	if cpuPerChip == util.ErrorInt {
		return errors.New("getTotalResFromNpuNode get aicpu from template failed")
	}
	device.NPUDevice.TotalRes.Aicpu = cpuPerChip * totalChipNum
	device.NPUDevice.TotalRes.DVPP = plugin.AscendDVPPEnabledOff

	return nil
}

// getCpuNumPerChip 按芯片型号与总核数从模板里匹配出每芯片 AiCpu 数。
func getCpuNumPerChip(device *vnpu.NPUDevices, templates []VTemplate) int {
	// 默认错误值
	cpuPerChip := util.ErrorInt
	for _, temp := range templates {
		// 型号需匹配（ChipKind 或 ChipType 任一），且模板 AICore*总芯片数需等于总 AiCore
		if (temp.ChipKind != device.NPUDevice.ChipKind && temp.ChipKind != device.NPUDevice.ChipType) ||
			temp.AICore*device.TotalChipNum != device.NPUDevice.TotalRes.Aicore {
			continue
		}
		// 命中模板，取 AICPU
		cpuPerChip = temp.AICPU
	}
	return cpuPerChip
}

// VTemplate 是节点级虚拟芯片模板（用于匹配总资源推算每芯片规格）。
type VTemplate struct {
	// ChipKind Ascend910/Ascend310P
	// ChipKind 芯片型号（如 Ascend910/Ascend310P）
	ChipKind string
	// AICore 每模板的 AiCore 数
	AICore int
	// AICPU 每模板的 AiCpu 数
	AICPU int
	// DVPPEnable DVPP 使能状态
	DVPPEnable string
}

// initTemplate 返回内置的芯片模板表（覆盖 310P 与各 B 系列芯片）。
func initTemplate() []VTemplate {
	// 预分配容量为 7（NPUIndex7）
	nodeTemplate := make([]VTemplate, util.NPUIndex7)
	if len(nodeTemplate) < util.NPUIndex7 {
		return nodeTemplate
	}
	// 310P：8 核 7 cpu
	nodeTemplate[0] = VTemplate{
		ChipKind: util.Ascend310P,
		AICore:   util.NPUIndex8,
		AICPU:    util.NPUIndex7,
	}
	// B1：25 核 6 cpu
	nodeTemplate[util.NPUIndex1] = VTemplate{
		ChipKind: plugin.ChipTypeB1,
		AICore:   util.CoreNum25,
		AICPU:    util.CpuNum6,
	}
	// B2C：24 核 6 cpu
	nodeTemplate[util.NPUIndex2] = VTemplate{
		ChipKind: plugin.ChipTypeB2C,
		AICore:   util.CoreNum24,
		AICPU:    util.CpuNum6,
	}
	// B3：20 核 6 cpu
	nodeTemplate[util.NPUIndex3] = VTemplate{
		ChipKind: plugin.ChipTypeB3,
		AICore:   util.CoreNum20,
		AICPU:    util.CpuNum6,
	}
	// B4：20 核 6 cpu
	nodeTemplate[util.NPUIndex4] = VTemplate{
		ChipKind: plugin.ChipTypeB4,
		AICore:   util.CoreNum20,
		AICPU:    util.CpuNum6,
	}
	// B2：24 核 6 cpu
	nodeTemplate[util.NPUIndex5] = VTemplate{
		ChipKind: plugin.ChipTypeB2,
		AICore:   util.CoreNum24,
		AICPU:    util.CpuNum6,
	}
	// 另一档 310P：10 核 7 cpu
	nodeTemplate[util.NPUIndex6] = VTemplate{
		ChipKind: util.Ascend310P,
		AICore:   util.CoreNum10,
		AICPU:    util.NPUIndex7,
	}
	return nodeTemplate
}

// getTotalChipNum used after aicorePerChip set
// getTotalChipNum 用总 AiCore 除以每芯片 AiCore 得到总芯片数（要求整除且非零）。
func getTotalChipNum(device *vnpu.NPUDevices) (int, error) {
	// 整除计算
	totalChipNum := device.TotalRes.Aicore / device.AiCorePerChip
	if device.TotalRes.Aicore%device.AiCorePerChip != 0 {
		return 0, errors.New("getTotalChipNum error: total resource cannot be divided by coreNumPerChip")
	}
	if totalChipNum == 0 {
		return 0, errors.New("getTotalChipNum error: total chip number zero")
	}
	return totalChipNum, nil
}

// getVChipCoreNum 从 ServerType 标签（如 Ascend310P-8）解析每芯片 AiCore 数。
func getVChipCoreNum(device *vnpu.NPUDevices) (int, error) {
	// 按 "-" 拆分 ServerType
	serverTypeSplit := strings.Split(device.ServerType, "-")
	if len(serverTypeSplit) < util.NPUIndex2 {
		return 0, fmt.Errorf("getVChipCoreNum serverType %s format error", device.ServerType)
	}
	// 取第二段转整数（即每芯片核数）
	coreNum, err := strconv.Atoi(serverTypeSplit[1])
	if err != nil {
		return 0, fmt.Errorf("getVChipCoreNum serverType %s split error", device.ServerType)
	}
	return coreNum, nil
}

// getVChipTotalRes 用总资源除以总芯片数，得到单芯片总资源。
func getVChipTotalRes(device *vnpu.NPUDevices) vnpu.VResource {
	// 每芯片 AiCore = 总 AiCore / 总芯片数
	AiCore := device.TotalRes.Aicore / device.TotalChipNum
	// 每芯片 AiCpu = 总 AiCpu / 总芯片数
	AiCpu := device.TotalRes.Aicpu / device.TotalChipNum
	return vnpu.VResource{
		Aicore: AiCore,
		Aicpu:  AiCpu,
		DVPP:   plugin.AscendDVPPEnabledOff,
	}
}

// checkDyVNodeResourceInitialized 判断动态 VNode 资源是否已初始化（容量里的 npu-core > 0）。
func checkDyVNodeResourceInitialized(device *vnpu.NPUDevices) bool {
	return device.Capability[util.AscendNPUCore] > 0
}

// setChipPropertiesFromNPUNode returns chipKind, chipLabel, accType
// setChipPropertiesFromNPUNode 从节点标签/注解设置芯片型号、服务器类型、芯片类型与空闲芯片数。
func setChipPropertiesFromNPUNode(device *vnpu.NPUDevices) error {
	// 1. set ChipKind(Ascend910/Ascend310/Ascend310P)
	// 1. 设置 ChipKind
	chipKind, err := GetChipKindFromNpuNode(device)
	if err != nil {
		return fmt.Errorf("setNodeVNPUInfo node %s: %v", device.NodeInf.Name, err)
	}
	device.NPUDevice.ChipKind = chipKind

	// 2. set ServerType(like Ascend310P-10-dual/Ascend910-30)
	// 2. 设置 ServerType（来自节点标签）
	chipLabel, ok := device.Label[util.ServerType]
	if !ok {
		return fmt.Errorf("setNodeVNPUInfo node %s no node label <%s>", device.NodeInf.Name, util.ServerType)
	}
	device.NPUDevice.ServerType = chipLabel

	// 3. set ChipType
	// 3. 设置 ChipType（来自节点标签）
	chipType, ok := device.Label[vnpu.ChipTypeKey]
	if !ok {
		return fmt.Errorf("setNodeVNPUInfo node %s no node label <%s>", device.NodeInf.Name, vnpu.ChipTypeKey)
	}
	device.NPUDevice.ChipType = chipType

	// 4. set free chip num from device-info
	// 4. 从设备信息注解里取空闲芯片数
	nodeFreeChips, ok := device.Annotation[util.HwPreName+device.NPUDevice.ChipKind]
	if !ok {
		return errors.New("getFreeChipNum failed")
	}

	// 按 "," 拆分得到空闲芯片数
	nodeFreeChipsSplit := strings.Split(nodeFreeChips, ",")
	device.NPUDevice.FreeChipNum = len(nodeFreeChipsSplit)

	return nil
}

// GetChipKindFromNpuNode input huawei-Ascend910 return Ascend910/Ascend310p/Ascend310
// GetChipKindFromNpuNode 从节点 accelerator 标签（如 huawei-Ascend910）解析出芯片型号。
func GetChipKindFromNpuNode(device *vnpu.NPUDevices) (string, error) {
	// 读取 accelerator 标签
	tempVal, ok := device.Label[util.Accelerator]
	if !ok {
		return "", fmt.Errorf("getChipKindFromNpuNode label %s absent", util.Accelerator)
	}
	// 按 "-" 拆分
	chipKind := strings.Split(tempVal, "-")
	if len(chipKind) < util.NPUIndex2 {
		return "", fmt.Errorf("getChipKindFromNpuNode label %s value %s %s", util.Accelerator,
			chipKind, plugin.FormatIncorrectError)
	}
	// 返回第二段（型号）
	return chipKind[1], nil
}

// updateNPUNodeDeviceInfos return true if device info was updated, else return false
// updateNPUNodeDeviceInfos 若设备信息有更新则刷新底层注解缓存。
func updateNPUNodeDeviceInfos(device *vnpu.NPUDevices, data k8s.NodeDeviceInfoWithID) {
	// 缓存更新时间不早于新数据则跳过刷新
	if device.DevInfoUpdateTime >= data.UpdateTime {
		klog.V(util.LogDebugLev).Infof("device info is not update, skip refresh cache")
		return
	}
	// 记录 SuperPodID
	device.SuperPodID = data.SuperPodID

	// 用 volcano 缓存机制刷新设备信息
	updateNPUNodeDeviceInfosWithVolcanoCache(device, data, data.UpdateTime)

	// 更新本节点缓存时间戳
	device.DevInfoUpdateTime = data.UpdateTime
	klog.V(util.LogDebugLev).Infof("update device info for node<%s> annotations: %v", device.NodeInf.Name, device.Annotation)
}

// updateNPUNodeDeviceInfosWithVolcanoCache 把设备列表信息写回注解，带缓存一致性处理。
func updateNPUNodeDeviceInfosWithVolcanoCache(device *vnpu.NPUDevices, data k8s.NodeDeviceInfoWithID, updateTime int64) {
	// 遍历设备列表
	for k, v := range data.DeviceList {
		// if k does not represent huawei.com/Ascend910/310/310P continue
		// 形如 huawei.com/Ascend910 的 key 含多个 "-"，直接覆盖
		if len(strings.Split(k, "-")) > 1 {
			device.Annotation[k] = v
			continue
		}
		// if time interval over 10s continue
		// 距上次更新超过阈值则直接覆盖
		if updateTime-device.DevInfoUpdateTime > vnpu.DeviceInfoForceUpdateInterval {
			device.Annotation[k] = v
			continue
		}
		// 否则做健康卡列表合并，避免抖动丢失卡
		device.Annotation[k] = getRealHealthyDeviceList(device, k, device.Annotation[k], v)
	}
}

// getRealHealthyDeviceList 在缓存与新设备信息间做合并，返回合并后的健康卡列表字符串。
func getRealHealthyDeviceList(device *vnpu.NPUDevices, deviceKey, oldList, newList string) string {
	// if cache card list is empty or device info is empty. update by device info
	// 缓存或新列表为空，直接以新列表为准
	if len(oldList) == 0 || len(newList) == 0 {
		return newList
	}
	// 拆分成集合
	newDeviceList := strings.Split(newList, ",")
	oldDeviceList := strings.Split(oldList, ",")

	// if cache is not equal k8s or device info is equal k8s. update by device info
	// 若缓存空闲数与实际不一致，或新列表数与空闲数一致，则以新列表为准
	if int(device.Idle[v1.ResourceName(deviceKey)]/util.NPUHexKilo) != len(oldDeviceList) ||
		int(device.Idle[v1.ResourceName(deviceKey)]/util.NPUHexKilo) == len(newDeviceList) {
		return newList
	}

	klog.V(util.LogDebugLev).Infof("DEBUG: node %s, totalIdle from Capability: %d", device.NodeInf.Name, int(device.Idle[v1.ResourceName(deviceKey)]/util.NPUHexKilo))

	// 取新旧交集作为最终健康卡列表（保留两边都存在的卡，避免误删）
	oldDevices := make(map[string]struct{})
	for _, device := range oldDeviceList {
		oldDevices[device] = struct{}{}
	}
	var deviceListCache []string
	for _, newDevice := range newDeviceList {
		if _, ok := oldDevices[newDevice]; !ok {
			continue
		}
		deviceListCache = append(deviceListCache, newDevice)
	}
	klog.V(util.LogWarningLev).Infof("update device info for node<%s> annotations: %#v", device.NodeInf.Name, deviceListCache)
	return strings.Join(deviceListCache, ",")
}

// syncAnnotation 4 parts, 1 v1.node annotations, 2 last session device infos, 3 switch info, 4 noded info
// syncAnnotation 把四类注解合并进设备 Annotation：节点注解、上轮设备信息、交换机信息、noded 状态。
func syncAnnotation(device *vnpu.NPUDevices, npuNode *api.NodeInfo, nodeInfoOfNodeD k8s.NodeDNodeInfo) {
	// 先清空再用 existAnno 重建
	existAnno := make(map[string]string)
	// 1. sync v1.node annotations
	// 1. 合并节点自身注解
	for k, v := range npuNode.Node.Annotations {
		existAnno[k] = v
	}
	// 2. last session device infos
	// 2. 合并上一轮设备信息里含华为前缀的注解（保留历史设备状态）
	for annoKey, annoValue := range device.Annotation {
		if strings.Contains(annoKey, util.HwPreName) {
			existAnno[annoKey] = annoValue
			continue
		}
	}
	// 3. noded info. adding noded reported info into NPUNode.Annotation including node healthy status
	// when there are no faults on the node, node info cm does not exist
	// 3. 合并 noded 报告的节点健康状态（无故障时常不存在 CM）
	if nodeInfoOfNodeD.NodeStatus != "" {
		existAnno[vnpu.NodedNodeHealtyStatuskey] = nodeInfoOfNodeD.NodeStatus
	} else {
		existAnno[vnpu.NodedNodeHealtyStatuskey] = util.NodeHealthyByNodeD
	}
	// 写回设备注解
	device.Annotation = existAnno
}

// getNPUNodeCapacity get npu node Capacity by diff volcano version
// getNPUNodeCapacity 反射取出节点的 Capacity（兼容新旧字段名 OldCapacity/NewCapacity）。
func getNPUNodeCapacity(npuNode *api.NodeInfo) map[v1.ResourceName]float64 {
	klog.V(util.LogDebugLev).Infof("Enter getNPUNodeCapacity function")

	// 反射取节点结构
	valueOfP := reflect.ValueOf(*npuNode)
	if valueOfP.Kind() != reflect.Struct {
		return nil
	}
	for i := 0; i < valueOfP.NumField(); i++ {
		// 只关心 OldCapacity 或 NewCapacity 字段
		if valueOfP.Type().Field(i).Name != vnpu.OldCapacity && valueOfP.Type().Field(i).Name != vnpu.NewCapacity {
			continue
		}
		// 取出字段并断言为 *api.Resource，返回其标量资源
		if capacity, ok := valueOfP.Field(i).Interface().(*api.Resource); ok {
			return capacity.ScalarResources
		}
		klog.V(util.LogErrorLev).Info("get capacity failed by not meet the resource type")
		return nil
	}
	return nil
}

// getNPUNodeAddress get npu node address
// getNPUNodeAddress 取节点的内网 IP 地址。
func getNPUNodeAddress(npuNode *api.NodeInfo) string {
	// 遍历地址列表找 InternalIP
	for _, addr := range npuNode.Node.Status.Addresses {
		if addr.Type == v1.NodeInternalIP {
			return addr.Address
		}
	}
	// 没有则返回空串
	return ""
}
