# Model Locality Scheduling Demo

This example shows a small custom Volcano scheduler plugin named `model-locality`.

It demonstrates two common model-serving scheduling requirements:

1. Pods of one logical serving instance must stay on the same physical node or in the same simulated NVLink domain.
2. When scaling out, pods prefer nodes that already cache the requested model weights.

The implementation lives in `pkg/scheduler/plugins/model-locality`.

## Why Predicate And NodeOrder

Volcano plugins are registered in `pkg/scheduler/plugins/factory.go` and receive a scheduling `Session` in `OnSessionOpen`.

This demo uses two extension points:

- `Predicate`: hard filter. If a pod must stay in a specific topology domain, nodes outside that domain are rejected.
- `NodeOrder`: soft score. If a node has cached model weights, it receives extra score, but other nodes are still valid fallback choices.

Do not implement the cache check as a hard Predicate unless you truly want pods to remain Pending when no node has the model cached.

## Enable The Plugin

Add `model-locality` to `volcano-scheduler.conf`, usually in the same tier as `predicates` and `nodeorder`:

```yaml
actions: "enqueue, allocate, backfill"
tiers:
- plugins:
  - name: priority
  - name: gang
    enablePreemptable: false
  - name: conformance
- plugins:
  - name: overcommit
  - name: drf
    enablePreemptable: false
  - name: predicates
  - name: proportion
  - name: nodeorder
  - name: binpack
  - name: model-locality
    arguments:
      cache-hit.weight: 20
      same-topology-bonus.weight: 3
```

## Simulate NVLink Domains And Model Cache

```bash
kubectl label node node-a model-locality.volcano.sh/nvlink-domain=nvlink-a
kubectl label node node-b model-locality.volcano.sh/nvlink-domain=nvlink-a
kubectl label node node-c model-locality.volcano.sh/nvlink-domain=nvlink-b

kubectl annotate node node-a model-locality.volcano.sh/cached-models=llama-7b,qwen-7b
kubectl annotate node node-c model-locality.volcano.sh/cached-models=llama-70b
```

In a real system, the cached-model annotation would normally be written by a P2P distribution agent, JuiceFS/Alluxio cache controller, or node-side daemon.

## Workload Annotations

```yaml
metadata:
  annotations:
    scheduling.k8s.io/group-name: llama-serving
    model-locality.volcano.sh/instance: llama-serving-a
    model-locality.volcano.sh/topology: same-nvlink
    model-locality.volcano.sh/model: llama-7b
```

`model-locality.volcano.sh/topology` accepts:

- `same-node`: all pods with the same instance annotation must run on one node.
- `same-nvlink`: all pods with the same instance annotation must run on nodes with the same NVLink-domain label.

`model-locality.volcano.sh/model` is used only for scoring cached weights.

## Files

- `scheduler-config.yaml`: example Volcano scheduler ConfigMap.
- `nodes.yaml`: example node labels and annotations.
- `podgroup-and-pods.yaml`: example PodGroup and pods.
