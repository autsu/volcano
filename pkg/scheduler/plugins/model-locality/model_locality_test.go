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

package modellocality

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
)

func TestPredicateSameNode(t *testing.T) {
	plugin := New(framework.Arguments{}).(*modelLocalityPlugin)
	placements := map[string]*instancePlacement{
		"llama-a": {
			nodes: map[string]struct{}{"node-a": {}},
		},
	}
	task := taskInfo("pending", "llama-a", SameNode, "llama-7b", "")

	if err := plugin.predicate(task, nodeInfo("node-a", "", ""), placements); err != nil {
		t.Fatalf("same node should pass: %v", err)
	}
	if err := plugin.predicate(task, nodeInfo("node-b", "", ""), placements); err == nil {
		t.Fatalf("different node should be rejected")
	}
}

func TestPredicateSameNVLink(t *testing.T) {
	plugin := New(framework.Arguments{}).(*modelLocalityPlugin)
	placements := map[string]*instancePlacement{
		"llama-a": {
			nodes:        map[string]struct{}{"node-a": {}},
			nvlinkDomain: "nvlink-a",
		},
	}
	task := taskInfo("pending", "llama-a", SameNVLink, "llama-7b", "")

	if err := plugin.predicate(task, nodeInfo("node-b", "nvlink-a", ""), placements); err != nil {
		t.Fatalf("same NVLink domain should pass: %v", err)
	}
	if err := plugin.predicate(task, nodeInfo("node-c", "nvlink-b", ""), placements); err == nil {
		t.Fatalf("different NVLink domain should be rejected")
	}
}

func TestNodeOrderPrefersCachedModel(t *testing.T) {
	plugin := New(framework.Arguments{
		cacheHitWeightArg:          2,
		sameTopologyBonusWeightArg: 0,
	}).(*modelLocalityPlugin)
	task := taskInfo("pending", "llama-a", SameNVLink, "llama-7b", "")

	cachedScore := plugin.nodeOrder(task, nodeInfo("node-a", "nvlink-a", "llama-7b,qwen-7b"), nil)
	coldScore := plugin.nodeOrder(task, nodeInfo("node-b", "nvlink-a", "qwen-7b"), nil)

	if cachedScore <= coldScore {
		t.Fatalf("cached node should score higher, cached=%v cold=%v", cachedScore, coldScore)
	}
}

func taskInfo(name, instance, topology, model, nodeName string) *api.TaskInfo {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       uuid.NewUUID(),
			Name:      name,
			Namespace: "default",
			Annotations: map[string]string{
				DefaultInstanceAnnotation: instance,
				DefaultTopologyAnnotation: topology,
				DefaultModelAnnotation:    model,
			},
		},
	}
	task := api.NewTaskInfo(pod)
	task.NodeName = nodeName
	return task
}

func nodeInfo(name, nvlinkDomain, cachedModels string) *api.NodeInfo {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				DefaultNVLinkLabel: nvlinkDomain,
			},
			Annotations: map[string]string{
				DefaultModelCacheAnnotation: cachedModels,
			},
		},
	}
	return api.NewNodeInfo(node)
}
