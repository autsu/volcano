/*
Copyright 2026 The Volcano Authors.

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

// 本文件实现「GPU 独占」包装器：在 vgpu.GPUDevices 外层包一层，
// 让命中独占规则的 Pod 获得专用物理 GPU，不与其它命中同规则的 Pod 共享。
package deviceshare

import (
	// fmt 用于格式化规则字符串
	"fmt"

	// v1 是 k8s 核心 Pod 类型
	v1 "k8s.io/api/core/v1"
	// kubernetes 是 k8s 客户端接口类型
	"k8s.io/client-go/kubernetes"
	// klog 是 k8s 日志库
	"k8s.io/klog/v2"

	// api 是 volcano 调度器对 Pod/节点的抽象与 Devices 接口
	"volcano.sh/volcano/pkg/scheduler/api"
	// vgpu 是 NVIDIA 细粒度虚拟化设备插件
	"volcano.sh/volcano/pkg/scheduler/api/devices/nvidia/vgpu"
	// framework 是 volcano 调度框架（Session/Arguments）
	"volcano.sh/volcano/pkg/scheduler/framework"
)

const (
	// GPUExclusiveRulesKey is the argument key for exclusivity rules.
	// Each rule is a map of label key → value. Pods matching ALL labels in a rule
	// get exclusive GPU access — no sharing with other rule-matching pods.
	// GPUExclusiveRulesKey 是配置项 key：独占规则列表。
	// 每条规则是一组 label key→value，Pod 须同时匹配所有 label 才算命中，
	// 命中后获得独占 GPU，不与其它命中同规则的 Pod 共享。
	GPUExclusiveRulesKey = "deviceshare.GPUExclusiveRules"
)

// podKey returns a unique key for the pod using namespace/name to avoid
// cross-namespace collisions.
// podKey 用 命名空间/名称 拼出 Pod 的唯一 key，避免跨命名空间冲突。
func podKey(pod *v1.Pod) string {
	// 拼接 namespace/name 作为唯一键
	return pod.Namespace + "/" + pod.Name
}

// exclusiveRule defines a set of label key-value pairs.
// A pod matches this rule if it carries ALL specified labels with matching values.
// exclusiveRule 表示一条独占规则：一组 label key→value。
// Pod 须携带规则里的全部 label 且值匹配，才算命中这条规则。
type exclusiveRule struct {
	// labels 是规则里要求的 label 键值对集合
	labels map[string]string
}

// String 把规则格式化为字符串，便于日志打印。
func (r exclusiveRule) String() string {
	// 把 labels map 格式化输出
	return fmt.Sprintf("%v", r.labels)
}

// gpuExclusiveConfig 持有解析后的所有独占规则。
type gpuExclusiveConfig struct {
	// rules 是解析得到的独占规则列表
	rules []exclusiveRule
}

// loadGPUExclusiveConfig 从插件参数里读取并解析独占规则配置。
func loadGPUExclusiveConfig(args framework.Arguments) gpuExclusiveConfig {
	// 构造空配置
	cfg := gpuExclusiveConfig{}
	// 若参数里存在该配置项，则解析成规则列表
	if rawRules, ok := args[GPUExclusiveRulesKey]; ok {
		cfg.rules = parseExclusiveRules(rawRules)
	}
	// 返回配置（可能 rules 为空）
	return cfg
}

// parseExclusiveRules converts the raw YAML-decoded rules into typed exclusiveRule slices.
// Expected input: []interface{} where each element is a map of label key → value.
// parseExclusiveRules 把 YAML 解码后的原始结构转换为独占规则切片。
// 期望输入是 []interface{}，每个元素是 map（label key→value）。
func parseExclusiveRules(raw interface{}) []exclusiveRule {
	// 尝试断言为切片
	slice, ok := raw.([]interface{})
	if !ok {
		// 类型不符直接返回 nil
		return nil
	}
	// 用于收集解析出的规则
	var rules []exclusiveRule
	for _, item := range slice {
		// 每条规则先准备一个空的 label 集合
		labels := make(map[string]string)
		// 根据 YAML 解码出的具体 map 类型分别处理
		switch m := item.(type) {
		case map[string]interface{}:
			// 键为字符串的 map：提取字符串值
			for k, v := range m {
				if sv, ok := v.(string); ok {
					labels[k] = sv
				}
			}
		case map[interface{}]interface{}:
			// 键为任意接口（YAML 常见）的 map：先转字符串键再提取值
			for k, v := range m {
				if sk, ok := k.(string); ok {
					if sv, ok := v.(string); ok {
						labels[sk] = sv
					}
				}
			}
		}
		// 只要成功解析到 label，就追加为一条规则
		if len(labels) > 0 {
			rules = append(rules, exclusiveRule{labels: labels})
		}
	}
	// 返回解析后的规则切片
	return rules
}

// matchingRules returns the indices of rules that the pod matches.
// A pod matches a rule if it has ALL label:value pairs specified in that rule.
// matchingRules 返回该 Pod 命中的规则下标列表。
// 命中条件：Pod 须具备规则里的全部 label:value 对。
func matchingRules(pod *v1.Pod, rules []exclusiveRule) []int {
	// 无 label 的 Pod 不可能命中，直接返回 nil
	if pod.Labels == nil {
		return nil
	}
	// 收集命中的规则下标
	var matched []int
	for i, rule := range rules {
		// 逐条判断 Pod 是否命中
		if podMatchesRule(pod, rule) {
			matched = append(matched, i)
		}
	}
	// 返回命中下标集合
	return matched
}

// podMatchesRule returns true if the pod has ALL label:value pairs in the rule.
// podMatchesRule 判断 Pod 是否命中某条规则（须包含规则里的全部 label:value）。
func podMatchesRule(pod *v1.Pod, rule exclusiveRule) bool {
	// 无 label 一定不命中
	if pod.Labels == nil {
		return false
	}
	// 遍历规则要求的每个 label，缺失或值不符即返回 false
	for k, v := range rule.labels {
		if podVal, ok := pod.Labels[k]; !ok || podVal != v {
			return false
		}
	}
	// 全部满足才算命中
	return true
}

// exclusiveGPUDevices wraps vgpu.GPUDevices to enforce that pods matching
// an exclusivity rule get dedicated physical GPUs, not shared with other rule-matching pods.
//
// For pods that don't match any rule, all operations delegate directly to the
// inner GPUDevices.
//
// For pods matching one or more rules, FilterNode and Allocate temporarily cap
// reserved GPUs (set Number = UsedNum) so the vGPU allocator skips them,
// then restore Number afterwards.
// exclusiveGPUDevices 包装 vgpu.GPUDevices，强制让命中独占规则的 Pod
// 获得专属物理 GPU，不与其它命中同规则的 Pod 共享。
// 不命中任何规则的 Pod，所有操作直接透传给内层 GPUDevices。
// 命中规则的 Pod，在 FilterNode/Allocate 时临时把被预留 GPU 的 Number 设为 UsedNum，
// 让 vGPU 分配器跳过它们，结束后再恢复 Number。
type exclusiveGPUDevices struct {
	// inner 是被包装的底层 vgpu.GPUDevices 实例
	inner *vgpu.GPUDevices
	// cfg 是独占规则配置
	cfg gpuExclusiveConfig
	// nodeName 是本包装器所属节点名
	nodeName string
	// plugin 是反向引用到 deviceshare 插件（用于跨会话持久化）
	plugin *deviceSharePlugin
	// ruleGPUs[ruleIndex] = set of GPU indices used by pods matching that rule
	// ruleGPUs 记录每条规则当前占用了哪些 GPU 下标
	ruleGPUs map[int]map[int]struct{}
	// podRules[namespace/name] = set of rule indices the pod matches
	// podRules 记录每个 Pod 命中了哪些规则下标
	podRules map[string]map[int]struct{}
	// podUIDs maps namespace/name → podUID for PodMap lookups (upstream uses UID as key)
	// podUIDs 缓存 pod key → pod UID，便于在底层 PodMap（以 UID 为键）里查找
	podUIDs map[string]string
}

// Compile-time check that exclusiveGPUDevices implements api.Devices.
// 编译期断言：确保 exclusiveGPUDevices 实现了 api.Devices 接口。
var _ api.Devices = (*exclusiveGPUDevices)(nil)

// reservedGPUsForPod returns the GPU indices reserved by the rules that
// the pod matches. Only same-rule exclusivity is enforced: pods matching
// different rules can still share GPUs with each other.
// reservedGPUsForPod 返回该 Pod 命中的规则所预留的 GPU 下标集合。
// 仅强制「同规则」独占：命中不同规则的 Pod 之间仍可共享 GPU。
func (a *exclusiveGPUDevices) reservedGPUsForPod(pod *v1.Pod) map[int]struct{} {
	// 先算出该 Pod 命中的规则下标
	matched := matchingRules(pod, a.cfg.rules)
	if len(matched) == 0 {
		// 未命中任何规则，无预留
		return nil
	}
	// 汇总这些规则占用的所有 GPU 下标
	result := make(map[int]struct{})
	for _, ruleIdx := range matched {
		if gpuSet, ok := a.ruleGPUs[ruleIdx]; ok {
			for gpuIdx := range gpuSet {
				result[gpuIdx] = struct{}{}
			}
		}
	}
	// 返回预留的 GPU 下标集合
	return result
}

// capGPUs temporarily sets Number = UsedNum on the given GPU indices so the
// vGPU allocator skips them. Returns saved values for restoreGPUs.
// capGPUs 临时把给定 GPU 下标的 Number 设为 UsedNum，使 vGPU 分配器视为已满而跳过。
// 返回被修改前的原始 Number，供 restoreGPUs 恢复。
func (a *exclusiveGPUDevices) capGPUs(gpuIndices map[int]struct{}) map[int]uint {
	// 保存每个被改 GPU 的原始 Number
	saved := make(map[int]uint, len(gpuIndices))
	for idx := range gpuIndices {
		// 取出底层设备并校验非空
		if dev, ok := a.inner.Device[idx]; ok && dev != nil {
			// 记下原值
			saved[idx] = dev.Number
			// 把可用卡数压到已用数，使分配器跳过
			dev.Number = dev.UsedNum
		}
	}
	// 返回原值表
	return saved
}

// restoreGPUs 用 capGPUs 保存的原值恢复各 GPU 的 Number。
func (a *exclusiveGPUDevices) restoreGPUs(saved map[int]uint) {
	// 逐个恢复
	for idx, num := range saved {
		if dev, ok := a.inner.Device[idx]; ok && dev != nil {
			// 还原 Number
			dev.Number = num
		}
	}
}

// trackPodFromPodMap registers a pod in podRules and adds GPU mappings based
// on PodMap entries only. This is safe to call from AddResource because it
// does NOT rebuild ruleGPUs from scratch — it only adds new entries.
// trackPodFromPodMap 仅依据底层 PodMap 把 Pod 登记进 podRules 并补充 GPU 映射。
// 它只在已有基础上追加，不重建 ruleGPUs，因此可安全地在 AddResource 里调用。
func (a *exclusiveGPUDevices) trackPodFromPodMap(pod *v1.Pod) {
	// 计算命中规则
	matched := matchingRules(pod, a.cfg.rules)
	if len(matched) == 0 {
		// 未命中则无需追踪
		return
	}
	// 把命中规则集合存入 podRules
	ruleSet := make(map[int]struct{}, len(matched))
	for _, idx := range matched {
		ruleSet[idx] = struct{}{}
	}
	a.podRules[podKey(pod)] = ruleSet

	// Only add GPU mappings for this pod if it appears in PodMap.
	// 仅当该 Pod 已出现在底层 PodMap 中，才建立 GPU 映射
	podUID := string(pod.UID)
	for gpuIdx, dev := range a.inner.Device {
		if dev == nil {
			// 跳过空设备
			continue
		}
		// 若底层 PodMap 里有这个 Pod，则为命中的每条规则记录该 GPU
		if _, ok := dev.PodMap[podUID]; ok {
			for ruleIdx := range ruleSet {
				if a.ruleGPUs[ruleIdx] == nil {
					// 首次记录该规则时初始化集合
					a.ruleGPUs[ruleIdx] = make(map[int]struct{})
				}
				a.ruleGPUs[ruleIdx][gpuIdx] = struct{}{}
			}
		}
	}
}

// untrackPod removes a pod's rule associations and GPU reservations.
// untrackPod 移除某 Pod 的规则关联与 GPU 预留。
func (a *exclusiveGPUDevices) untrackPod(pod *v1.Pod) {
	// 取出 Pod 的 key 与命中规则集合
	pk := podKey(pod)
	ruleSet, ok := a.podRules[pk]
	if !ok {
		// 没有记录过，直接返回
		return
	}
	// 从 podRules 删除该 Pod
	delete(a.podRules, pk)

	// 逐条规则检查其 GPU 是否还被其它 Pod 占用
	for ruleIdx := range ruleSet {
		gpuSet := a.ruleGPUs[ruleIdx]
		if gpuSet == nil {
			// 该规则无 GPU 集合，跳过
			continue
		}
		for gpuIdx := range gpuSet {
			// 取出该 GPU 设备
			dev, ok := a.inner.Device[gpuIdx]
			if !ok || dev == nil {
				continue
			}
			// 标记该 GPU 是否仍被其它已追踪 Pod 使用
			podUID := string(pod.UID)
			stillUsed := false
			for uid := range dev.PodMap {
				if uid != podUID {
					// 遍历其它已追踪 Pod，若其 UID 出现在本 GPU，说明仍被占用
					for otherKey := range a.podRules {
						if a.podUIDs[otherKey] == uid {
							stillUsed = true
							break
						}
					}
					if stillUsed {
						break
					}
				}
			}
			if !stillUsed {
				// 不再被占用则从规则 GPU 集合里移除
				delete(gpuSet, gpuIdx)
			}
		}
		// 该规则无 GPU 后，删除该规则条目
		if len(gpuSet) == 0 {
			delete(a.ruleGPUs, ruleIdx)
		}
	}
}

// --- api.Devices interface ---

// AddResource 在 Pod 资源入账时调用：先透传底层，再登记 podUID，最后按 PodMap 追踪独占。
func (a *exclusiveGPUDevices) AddResource(pod *v1.Pod) {
	// 透传给底层 vGPU 入账
	a.inner.AddResource(pod)
	// 缓存 pod key → UID
	a.podUIDs[podKey(pod)] = string(pod.UID)
	// 依据底层 PodMap 追踪该 Pod 的独占关系
	a.trackPodFromPodMap(pod)
}

// SubResource 在 Pod 资源出账时调用：先透传底层，再解除追踪与 UID 缓存。
func (a *exclusiveGPUDevices) SubResource(pod *v1.Pod) {
	// 透传给底层 vGPU 出账
	a.inner.SubResource(pod)
	// 解除该 Pod 的独占追踪
	a.untrackPod(pod)
	// 删除 pod UID 缓存
	delete(a.podUIDs, podKey(pod))
}

// AddQueueResource 透传到底层，返回队列资源占用视图。
func (a *exclusiveGPUDevices) AddQueueResource(pod *v1.Pod) map[string]float64 {
	// 直接委托底层实现
	return a.inner.AddQueueResource(pod)
}

// HasDeviceRequest 透传到底层，判断 Pod 是否请求了设备。
func (a *exclusiveGPUDevices) HasDeviceRequest(pod *v1.Pod) bool {
	// 直接委托底层实现
	return a.inner.HasDeviceRequest(pod)
}

// FilterNode 预选：若 Pod 命中规则且有预留 GPU，临时把预留 GPU 设为已满再预选，结束后恢复。
func (a *exclusiveGPUDevices) FilterNode(pod *v1.Pod, policy string) (int, string, error) {
	// 取出该 Pod 命中的预留 GPU
	reserved := a.reservedGPUsForPod(pod)
	if len(reserved) > 0 {
		// 有预留：把预留 GPU 压满
		saved := a.capGPUs(reserved)
		// 在压满状态下跑预选
		code, msg, err := a.inner.FilterNode(pod, policy)
		// 恢复 GPU 原值
		a.restoreGPUs(saved)
		return code, msg, err
	}
	// 无预留，直接透传预选
	return a.inner.FilterNode(pod, policy)
}

// ScoreNode 打分直接透传到底层。
func (a *exclusiveGPUDevices) ScoreNode(pod *v1.Pod, policy string) float64 {
	// 直接委托底层实现
	return a.inner.ScoreNode(pod, policy)
}

// Allocate 真正绑定：对命中规则的 Pod，临时压满预留 GPU 再绑定，结束后恢复并登记独占关系与持久化。
func (a *exclusiveGPUDevices) Allocate(kubeClient kubernetes.Interface, pod *v1.Pod) error {
	// 先算命中规则
	matched := matchingRules(pod, a.cfg.rules)
	if len(matched) == 0 {
		// 未命中则直接透传底层绑定
		return a.inner.Allocate(kubeClient, pod)
	}

	// 取出预留 GPU 并打印调试信息
	reserved := a.reservedGPUsForPod(pod)
	klog.V(4).Infof("gpuexclusive: Allocate pod=%s, matched=%v, reserved=%v, ruleGPUs=%v", pod.Name, matched, reserved, a.ruleGPUs)
	if len(reserved) > 0 {
		// 有预留：压满后绑定，再恢复
		saved := a.capGPUs(reserved)
		err := a.inner.Allocate(kubeClient, pod)
		a.restoreGPUs(saved)
		if err != nil {
			// 绑定失败则返回错误
			return err
		}
	} else {
		// 无预留：直接绑定
		if err := a.inner.Allocate(kubeClient, pod); err != nil {
			return err
		}
	}

	// 构造命中规则集合
	ruleSet := make(map[int]struct{}, len(matched))
	for _, idx := range matched {
		ruleSet[idx] = struct{}{}
	}
	// 记录 Pod → 命中规则
	pk := podKey(pod)
	a.podRules[pk] = ruleSet

	// Detect newly allocated GPUs by checking the PodMap for this pod's UID.
	// inner.Allocate updates PodMap via addToPodMap, making this reliable
	// (unlike UsedNum which is not updated during the allocation phase).
	// 通过底层 PodMap 里该 Pod 的 UID 找出本次新分配的 GPU（比 UsedNum 可靠）。
	newGPUs := make(map[int]struct{})
	podUID := string(pod.UID)
	for idx, dev := range a.inner.Device {
		if dev == nil {
			// 跳过空设备
			continue
		}
		// 若该 GPU 的 PodMap 含本 Pod，记为本 Pod 新分配，并写入各命中规则
		if _, ok := dev.PodMap[podUID]; ok {
			newGPUs[idx] = struct{}{}
			for ruleIdx := range ruleSet {
				if a.ruleGPUs[ruleIdx] == nil {
					a.ruleGPUs[ruleIdx] = make(map[int]struct{})
				}
				a.ruleGPUs[ruleIdx][idx] = struct{}{}
			}
		}
	}

	// Persist across scheduling sessions.
	// 跨调度会话持久化：把新分配 GPU 与命中规则写入插件实例，
	// 供后续会话（节点上已有 Pod）重建独占关系。
	if a.plugin != nil && a.nodeName != "" {
		// 加写锁保护跨会话状态
		a.plugin.lock.Lock()
		if a.plugin.persistedGPUs[a.nodeName] == nil {
			a.plugin.persistedGPUs[a.nodeName] = make(map[string]map[int]struct{})
		}
		a.plugin.persistedGPUs[a.nodeName][pk] = newGPUs
		if a.plugin.persistedPodRules[a.nodeName] == nil {
			a.plugin.persistedPodRules[a.nodeName] = make(map[string]map[int]struct{})
		}
		a.plugin.persistedPodRules[a.nodeName][pk] = ruleSet
		a.plugin.lock.Unlock()
	}

	// 打印调试日志
	klog.V(4).Infof("gpuexclusive: allocated pod %s, newGPUs=%v, ruleGPUs=%v",
		pk, newGPUs, a.ruleGPUs)
	return nil
}

// Release 解绑：先透传底层释放，再解除独占追踪。
func (a *exclusiveGPUDevices) Release(kubeClient kubernetes.Interface, pod *v1.Pod) error {
	// 透传底层释放
	err := a.inner.Release(kubeClient, pod)
	if err != nil {
		// 释放失败返回错误
		return err
	}
	// 解除独占追踪
	a.untrackPod(pod)
	return nil
}

// GetIgnoredDevices 透传到底层，返回忽略的设备列表。
func (a *exclusiveGPUDevices) GetIgnoredDevices() []string {
	// 直接委托底层实现
	return a.inner.GetIgnoredDevices()
}

// GetStatus 透传到底层，返回设备状态描述。
func (a *exclusiveGPUDevices) GetStatus() string {
	// 直接委托底层实现
	return a.inner.GetStatus()
}

// DeepCopy returns a deep copy of exclusiveGPUDevices for use in dry-run simulation.
// cfg, plugin, and nodeName are configuration/references shared safely; only the
// mutable tracking maps and inner device state are deep-copied.
// DeepCopy 返回本包装器深拷贝，供调度器 dry-run 模拟使用。
// cfg/plugin/nodeName 是共享安全的引用，无需复制；只有可变追踪 map 与内层设备状态需深拷贝。
func (a *exclusiveGPUDevices) DeepCopy() interface{} {
	// 自身为 nil 直接返回 nil
	if a == nil {
		return nil
	}
	// 构造基础拷贝，预先分配各 map
	cp := &exclusiveGPUDevices{
		cfg:      a.cfg,
		nodeName: a.nodeName,
		plugin:   a.plugin,
		ruleGPUs: make(map[int]map[int]struct{}, len(a.ruleGPUs)),
		podRules: make(map[string]map[int]struct{}, len(a.podRules)),
		podUIDs:  make(map[string]string, len(a.podUIDs)),
	}
	// 深拷贝 ruleGPUs 内层集合
	for ruleIdx, gpuSet := range a.ruleGPUs {
		newSet := make(map[int]struct{}, len(gpuSet))
		for g := range gpuSet {
			newSet[g] = struct{}{}
		}
		cp.ruleGPUs[ruleIdx] = newSet
	}
	// 深拷贝 podRules 内层集合
	for podKey, ruleSet := range a.podRules {
		newSet := make(map[int]struct{}, len(ruleSet))
		for r := range ruleSet {
			newSet[r] = struct{}{}
		}
		cp.podRules[podKey] = newSet
	}
	// 拷贝 podUIDs（值是字符串，直接复制即可）
	for k, v := range a.podUIDs {
		cp.podUIDs[k] = v
	}
	// 深拷贝底层设备状态
	if a.inner != nil {
		cp.inner = a.inner.DeepCopy().(*vgpu.GPUDevices)
	}
	// 返回深拷贝对象
	return cp
}

// wrapGPUDevicesForExclusivity wraps each node's GPUDevices with the exclusivity-aware
// wrapper during OnSessionOpen. Called from deviceshare's OnSessionOpen.
// wrapGPUDevicesForExclusivity 在 OnSessionOpen 时把每个节点的 GPUDevices 包上独占包装器。
func (dp *deviceSharePlugin) wrapGPUDevicesForExclusivity(ssn *framework.Session) {
	// 加写锁保护插件级状态
	dp.lock.Lock()
	defer dp.lock.Unlock()

	// 加载独占规则配置
	cfg := loadGPUExclusiveConfig(dp.pluginArguments)

	klog.V(4).Infof("gpuexclusive config: rules=%v", cfg.rules)

	if len(cfg.rules) == 0 {
		// 未配置任何规则，跳过包装
		klog.V(2).Info("gpuexclusive: no rules configured, skipping GPU exclusivity wrapping")
		return
	}

	// 遍历所有节点
	for _, node := range ssn.Nodes {
		if node.Others == nil {
			// 该节点无 Others 映射，跳过
			continue
		}
		// 取出该节点的 vgpu 设备对象
		devObj, ok := node.Others[vgpu.DeviceName]
		if !ok || devObj == nil {
			// 没有 vgpu 设备，跳过
			continue
		}
		// 类型断言到底层 GPUDevices
		inner, ok := devObj.(*vgpu.GPUDevices)
		if !ok || inner == nil {
			// 类型不符，跳过
			continue
		}

		// GPU exclusivity only applies to hami-core (software vGPU) mode.
		// Dynamic MIG nodes have hardware-level isolation and don't need it.
		// 独占仅适用于 hami-core（软件 vGPU）模式；MIG 节点硬件级隔离，不需要。
		if inner.Mode != "" && inner.Mode != "hami-core" {
			klog.V(4).Infof("gpuexclusive: skipping node %s with GPU mode %q (only hami-core supported)", node.Name, inner.Mode)
			continue
		}

		// Find existing pods on this node and compute their rule matches.
		// 遍历本节点已有 Pod，计算每个 Pod 命中的规则，建立基础映射
		podRules := make(map[string]map[int]struct{})
		podUIDs := make(map[string]string)
		uidToKey := make(map[string]string)
		for _, task := range node.Tasks {
			if task.Pod == nil {
				// 无 Pod 跳过
				continue
			}
			pk := podKey(task.Pod)
			uid := string(task.Pod.UID)
			podUIDs[pk] = uid
			uidToKey[uid] = pk
			matched := matchingRules(task.Pod, cfg.rules)
			if len(matched) == 0 {
				// 未命中则无需记录
				continue
			}
			ruleSet := make(map[int]struct{}, len(matched))
			for _, idx := range matched {
				ruleSet[idx] = struct{}{}
			}
			podRules[pk] = ruleSet
		}

		// Build UUID → device index map for annotation-based lookup.
		// 建立底层 GPU UUID → 设备下标 的映射，便于从注解里查 GPU
		uuidToIdx := make(map[string]int, len(inner.Device))
		for idx, dev := range inner.Device {
			if dev != nil {
				uuidToIdx[dev.UUID] = idx
			}
		}

		// Build initial ruleGPUs from multiple sources.
		// 从多个来源汇总初始 ruleGPUs（规则→GPU 集合）
		ruleGPUs := make(map[int]map[int]struct{})

		// Source 1: PodMap
		// 来源 1：底层 PodMap（已在内存态的占用）
		for gpuIdx, dev := range inner.Device {
			if dev == nil {
				continue
			}
			for podUID := range dev.PodMap {
				pk := uidToKey[podUID]
				if ruleSet, ok := podRules[pk]; ok {
					for ruleIdx := range ruleSet {
						if ruleGPUs[ruleIdx] == nil {
							ruleGPUs[ruleIdx] = make(map[int]struct{})
						}
						ruleGPUs[ruleIdx][gpuIdx] = struct{}{}
					}
				}
			}
		}

		// Source 2: Pod annotations
		// 来源 2：Pod 注解（已分配设备编码在 vgpu 注解里，但 PodMap 尚未同步的情况）
		for pk, ruleSet := range podRules {
			podUID := podUIDs[pk]
			alreadyTracked := false
			for _, dev := range inner.Device {
				if dev == nil {
					continue
				}
				if _, ok := dev.PodMap[podUID]; ok {
					// 已在 PodMap 里则视为已追踪
					alreadyTracked = true
					break
				}
			}
			if alreadyTracked {
				// 已追踪则跳过来源 2
				continue
			}
			// 从注解里解码该 Pod 占用的设备
			for _, task := range node.Tasks {
				if task.Pod == nil || podKey(task.Pod) != pk {
					continue
				}
				ann, ok := task.Pod.Annotations[vgpu.AssignedIDsAnnotations]
				if !ok || ann == "" {
					break
				}
				for _, contDevs := range vgpu.DecodePodDevices(ann) {
					for _, cd := range contDevs {
						if gpuIdx, ok := uuidToIdx[cd.UUID]; ok {
							for ruleIdx := range ruleSet {
								if ruleGPUs[ruleIdx] == nil {
									ruleGPUs[ruleIdx] = make(map[int]struct{})
								}
								ruleGPUs[ruleIdx][gpuIdx] = struct{}{}
							}
						}
					}
				}
				break
			}
		}

		// Source 3: Persisted state from previous scheduling cycles.
		// 来源 3：上一调度周期持久化的状态（persistedGPUs/persistedPodRules）
		if persisted, ok := dp.persistedGPUs[node.Name]; ok {
			persistedRules := dp.persistedPodRules[node.Name]
			for pk, gpuSet := range persisted {
				if _, inPodRules := podRules[pk]; !inPodRules {
					// 当前节点已无该 Pod，跳过
					continue
				}
				podUID := podUIDs[pk]
				alreadyTracked := false
				for _, dev := range inner.Device {
					if dev == nil {
						continue
					}
					if _, ok := dev.PodMap[podUID]; ok {
						alreadyTracked = true
						break
					}
				}
				if alreadyTracked {
					// 已在 PodMap 里则跳过来源 3
					continue
				}
				ruleSet := persistedRules[pk]
				if ruleSet == nil {
					continue
				}
				for gpuIdx := range gpuSet {
					for ruleIdx := range ruleSet {
						if ruleGPUs[ruleIdx] == nil {
							ruleGPUs[ruleIdx] = make(map[int]struct{})
						}
						ruleGPUs[ruleIdx][gpuIdx] = struct{}{}
					}
				}
			}
		}

		// Prune persisted entries for pods no longer on this node.
		// 清理持久化状态里已不在本节点的 Pod 条目
		activePods := make(map[string]bool, len(node.Tasks))
		for _, task := range node.Tasks {
			if task.Pod != nil {
				activePods[podKey(task.Pod)] = true
			}
		}
		if persisted, ok := dp.persistedGPUs[node.Name]; ok {
			for pk := range persisted {
				if !activePods[pk] {
					// 不在活跃 Pod 集合里则删除 GPU 与规则持久化
					delete(persisted, pk)
					if pr, ok := dp.persistedPodRules[node.Name]; ok {
						delete(pr, pk)
					}
				}
			}
		}

		// 构造独占包装器并替换节点上原来的 vgpu 设备对象
		wrapper := &exclusiveGPUDevices{
			inner:    inner,
			cfg:      cfg,
			nodeName: node.Name,
			plugin:   dp,
			ruleGPUs: ruleGPUs,
			podRules: podRules,
			podUIDs:  podUIDs,
		}
		node.Others[vgpu.DeviceName] = wrapper

		// 打印调试日志
		klog.V(4).Infof("gpuexclusive: OnSessionOpen node=%s, podRules=%v, ruleGPUs=%v, tasks=%d",
			node.Name, podRules, ruleGPUs, len(node.Tasks))
	}
}
