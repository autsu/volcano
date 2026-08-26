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

/*
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

package devices

import (
	"context"
	"encoding/json"
	"fmt"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
)

// These are predefined codes used in a Status.
const (
	// Success means that plugin ran correctly and found pod schedulable.
	// NOTE: A nil status is also considered as "Success".
	Success int = iota
	// Error is used for internal plugin errors, unexpected input, etc.
	Error
	// Unschedulable is used when a plugin finds a pod unschedulable. The scheduler might attempt to
	// preempt other pods to get this pod scheduled. Use UnschedulableAndUnresolvable to make the
	// scheduler skip preemption.
	// The accompanying status message should explain why the pod is unschedulable.
	Unschedulable
	// UnschedulableAndUnresolvable is used when a plugin finds a pod unschedulable and
	// preemption would not change anything. Plugins should return Unschedulable if it is possible
	// that the pod can get scheduled with preemption.
	// The accompanying status message should explain why the pod is unschedulable.
	UnschedulableAndUnresolvable
	// Wait is used when a Permit plugin finds a pod scheduling should wait.
	Wait
	// Skip is used when a Bind plugin chooses to skip binding.
	Skip
)

var kubeClient *kubernetes.Clientset

func GetClient() kubernetes.Interface {
	var err error
	if kubeClient == nil {
		kubeClient, err = NewClient()
		if err != nil {
			klog.ErrorS(err, "deviceshare initClient failed")
		}
	}
	return kubeClient
}

// NewClient connects to an API server
func NewClient() (*kubernetes.Clientset, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	client, err := kubernetes.NewForConfig(config)
	return client, err
}

func GetNode(nodename string) (*v1.Node, error) {
	if nodename == "" {
		klog.ErrorS(nil, "Node name is empty")
		return nil, fmt.Errorf("nodename is empty")
	}

	klog.V(5).InfoS("Fetching node", "nodeName", nodename)
	n, err := GetClient().CoreV1().Nodes().Get(context.Background(), nodename, metav1.GetOptions{})
	if err != nil {
		switch {
		case apierrors.IsNotFound(err):
			klog.ErrorS(err, "Node not found", "nodeName", nodename)
			return nil, fmt.Errorf("node %s not found", nodename)
		case apierrors.IsUnauthorized(err):
			klog.ErrorS(err, "Unauthorized to access node", "nodeName", nodename)
			return nil, fmt.Errorf("unauthorized to access node %s", nodename)
		default:
			klog.ErrorS(err, "Failed to get node", "nodeName", nodename)
			return nil, fmt.Errorf("failed to get node %s: %v", nodename, err)
		}
	}

	klog.V(5).InfoS("Successfully fetched node", "nodeName", nodename)
	return n, nil
}

func PatchPodAnnotations(kubeClient kubernetes.Interface, pod *v1.Pod, annotations map[string]string) error {
	type patchMetadata struct {
		Annotations map[string]string `json:"annotations,omitempty"`
	}
	type patchPod struct {
		Metadata patchMetadata `json:"metadata"`
		//Spec     patchSpec     `json:"spec,omitempty"`
	}

	p := patchPod{}
	p.Metadata.Annotations = annotations

	bytes, err := json.Marshal(p)
	if err != nil {
		return err
	}
	_, err = kubeClient.CoreV1().Pods(pod.Namespace).
		Patch(context.Background(), pod.Name, k8stypes.StrategicMergePatchType, bytes, metav1.PatchOptions{})
	if err != nil {
		klog.Errorf("patch pod %v failed, %v", pod.Name, err)
	}

	return err
}

func PatchNodeAnnotations(node *v1.Node, annotations map[string]string) error {
	type patchMetadata struct {
		Annotations map[string]string `json:"annotations,omitempty"`
	}
	type patchPod struct {
		Metadata patchMetadata `json:"metadata"`
		//Spec     patchSpec     `json:"spec,omitempty"`
	}

	p := patchPod{}
	p.Metadata.Annotations = annotations

	bytes, err := json.Marshal(p)
	if err != nil {
		return err
	}
	_, err = GetClient().CoreV1().Nodes().
		Patch(context.Background(), node.Name, k8stypes.StrategicMergePatchType, bytes, metav1.PatchOptions{})
	if err != nil {
		klog.Infoln("annotations=", annotations)
		klog.Infof("patch node %v failed, %v", node.Name, err)
	}
	return err
}

// ExtractResourceRequest 从 Pod 的每个容器里抽取 NVIDIA 设备的资源请求（卡数/显存/算力），
// 返回 ContainerDeviceRequest 切片。它兼容两种显存表达方式：
//   - Memreq：绝对显存，单位 MiB；
//   - MemPercentagereq：相对整张卡显存的百分比（0–100）。
//
// 关键点：101 是哨兵值（对应常量 DefaultMemPercentage），表示“用户未用百分比方式提需求”，
// 因为它落在合法百分比区间（0–100）之外，不会被误当成真实百分比；只有当 MemPercentagereq==101
// 且 Memreq==0（两者都没写）时，才兜底设成 100，表示“要一整张卡”。
func ExtractResourceRequest(pod *v1.Pod, resourceType, countName, memoryName, percentageName, coreName string) []ContainerDeviceRequest {
	// 构造“卡数”资源的 ResourceName
	resourceName := v1.ResourceName(countName)
	// 构造“绝对显存”资源的 ResourceName
	resourceMem := v1.ResourceName(memoryName)
	// 收集所有容器解析出的设备请求
	counts := []ContainerDeviceRequest{}

	//Count Nvidia GPU
	// 遍历 Pod 的每个容器，逐个解析其设备请求
	for i := 0; i < len(pod.Spec.Containers); i++ {
		// singledevice 标记“是否只声明了显存而没有单独声明卡数”
		singledevice := false
		// 优先从 Limits 取卡数资源
		v, ok := pod.Spec.Containers[i].Resources.Limits[resourceName]
		// 卡数资源不存在时，退而取绝对显存资源，并标记单设备（卡数默认按 1 处理）
		if !ok {
			v, ok = pod.Spec.Containers[i].Resources.Limits[resourceMem]
			singledevice = true
		}
		// 取到了卡数或显存资源才继续解析本容器
		if ok {
			// 默认卡数为 1
			n := int64(1)
			// 非单设备模式时，把资源值解析为真实卡数
			if !singledevice {
				n, _ = v.AsInt64()
			}
			// 绝对显存请求，默认 0
			memnum := int32(0)
			// 先取 Limits 里的绝对显存
			mem, ok := pod.Spec.Containers[i].Resources.Limits[resourceMem]
			// Limits 没写则退到 Requests
			if !ok {
				mem, ok = pod.Spec.Containers[i].Resources.Requests[resourceMem]
			}
			// 取到了就转成 int32 存入 memnum
			if ok {
				memnums, ok := mem.AsInt64()
				if ok {
					memnum = int32(memnums)
				}
			}
			// 显存百分比请求，默认 101（哨兵：表示用户没用百分比方式提需求）
			mempnum := int32(101)
			// 配置了百分比资源名时才解析百分比注解
			if percentageName != "" {
				// 构造“显存百分比”资源的 ResourceName
				resourceMemPercentage := v1.ResourceName(percentageName)
				// 先取 Limits 里的百分比
				mem, ok = pod.Spec.Containers[i].Resources.Limits[resourceMemPercentage]
				// Limits 没写则退到 Requests
				if !ok {
					mem, ok = pod.Spec.Containers[i].Resources.Requests[resourceMemPercentage]
				}
				// 取到了就用真实百分比（0–100）覆盖默认值
				if ok {
					mempnums, ok := mem.AsInt64()
					if ok {
						mempnum = int32(mempnums)
					}
				}
			}
			// 兜底：既没写百分比（仍为 101）也没写绝对显存（memnum==0）时，视为要整张卡（100%）
			if mempnum == 101 && memnum == 0 {
				mempnum = 100
			}
			// 算力（核心占比）请求，默认 0
			corenum := int32(0)
			// 配置了算力资源名时才解析
			if coreName != "" {
				// 构造“算力”资源的 ResourceName
				resourceCores := v1.ResourceName(coreName)
				// 先取 Limits 里的算力
				core, ok := pod.Spec.Containers[i].Resources.Limits[resourceCores]
				// Limits 没写则退到 Requests
				if !ok {
					core, ok = pod.Spec.Containers[i].Resources.Requests[resourceCores]
				}
				// 取到了就转成 int32 存入 corenum
				if ok {
					corenums, ok := core.AsInt64()
					if ok {
						corenum = int32(corenums)
					}
				}
			}
			// 汇总本容器的设备请求并追加到结果切片
			counts = append(counts, ContainerDeviceRequest{
				Nums:             int32(n),       // 卡数
				Type:             resourceType,   // 设备类型（如 NVIDIA）
				Memreq:           memnum,         // 绝对显存（MiB）
				MemPercentagereq: int32(mempnum), // 显存百分比；101=未设置，100=整卡
				Coresreq:         corenum,        // 算力占比
			})
		}
	}
	// 打印解析结果（调试用）
	klog.V(3).Infoln("counts=", counts)
	// 返回所有容器的设备请求
	return counts
}
