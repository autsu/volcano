/*
Copyright 2018 The Kubernetes Authors.
Copyright 2018-2025 The Volcano Authors.

Modifications made by Volcano authors:
- Enhanced gang scheduling validation with task-level validity checks
- Improved preemption logic to respect gang scheduling constraints
- Added support for job starving detection and enhanced pipeline state management

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

// Package gang 实现 Volcano 调度器的 Gang Scheduling（成组调度）插件。
//
// Gang Scheduling 的核心语义：一个 Job 的所有 Task 必须作为一个整体被调度。
// 只有当满足 MinAvailable 个 Task 能被分配到节点时，整个 Job 才能开始运行。
// 这类似于分布式事务中的"全或无"（All-or-Nothing）语义。
//
// 典型使用场景：
//   - 分布式训练（如 TensorFlow PS/Worker 模式）：所有 PS 和 Worker 必须同时启动
//   - MPI 作业：所有 rank 必须同时运行
//   - 多副本服务：需要一定数量的副本同时就绪
//
// gang 插件是 Volcano 调度器中最核心的插件之一，它贯穿了调度流程的多个环节：
//
//	JobValid        → 验证 Job 是否有足够的合法 Task（ValidJobFn）
//	JobReady        → 判断 Job 是否达到 MinAvailable（ReadyJobFn）
//	JobPipelined    → 判断 Job 是否处于 Pipeline 状态（PipelinedFn）
//	JobOrder        → 对 Job 排序：就绪优先（JobOrderFn）
//	Preemptable     → Gang 感知的抢占：保护 victim 的 MinAvailable（PreemptableFn）
//	Reclaimable     → Gang 感知的回收：同上逻辑（ReclaimableFn）
//	JobStarving     → 判断 Job 是否"饥饿"——需要更多资源（JobStarvingFn）
//	OnSessionClose  → 报告未调度 Job 的状态和原因
//
// 配置示例（通常不需要额外参数，gang 插件行为由 PodGroup.Spec.MinMember 控制）：
//
//	tiers:
//	- plugins:
//	  - name: gang
package gang

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	"volcano.sh/apis/pkg/apis/scheduling"
	"volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/metrics"
	"volcano.sh/volcano/pkg/scheduler/plugins/util"
)

// PluginName 是 gang 插件在 Volcano 调度器中的唯一名称。
const PluginName = "gang"

// gangPlugin 是 gang 调度的核心结构体，实现了 framework.Plugin 接口。
//
// 与其他插件（如 binpack、nodeorder）不同，gang 插件主要不是参与打分，
// 而是贯穿整个调度流程的决策环节：
//   - 准入控制（JobValid）：Job 是否具备被调度的基本条件
//   - 状态判定（JobReady / JobPipelined / JobStarving）：Job 当前处于什么状态
//   - 优先级排序（JobOrder）：哪个 Job 应该被优先调度
//   - 抢占保护（Preemptable / Reclaimable）：Gang 语义下的安全抢占
type gangPlugin struct {
	// pluginArguments 保存调度配置参数（当前 gang 插件不需要额外参数）
	pluginArguments framework.Arguments
}

// New 是 gang 插件的工厂函数。
// gang 插件不需要特殊的配置参数，其行为由 PodGroup.Spec.MinMember 控制。
func New(arguments framework.Arguments) framework.Plugin {
	return &gangPlugin{pluginArguments: arguments}
}

func (gp *gangPlugin) Name() string {
	return PluginName
}

// OnSessionOpen 在每次调度会话开始时注册所有回调函数。
//
// 这是 gang 插件的核心方法，它一口气注册了 10+ 个回调函数，
// 覆盖了 Volcano 调度框架的多个扩展点，形成一个完整的 Gang Scheduling 保障体系：
//
//	┌──────────────────────────────────────────────────────┐
//	│                  Gang 调度保障体系                     │
//	├──────────────┬───────────────────────────────────────┤
//	│ 准入控制      │ JobValidFn   → 验证 Task 合法性        │
//	│ 状态判定      │ JobReadyFn   → Ready/Pipelined/Starving │
//	│ 优先级排序    │ JobOrderFn   → 就绪 Job 优先           │
//	│ 抢占保护      │ PreemptableFn → 保护 victim 的 MinAvailable │
//	│ 回收保护      │ ReclaimableFn → 同上                   │
//	│ 会话结束      │ OnSessionClose → 报告状态和指标         │
//	└──────────────┴───────────────────────────────────────┘
func (gp *gangPlugin) OnSessionOpen(ssn *framework.Session) {

	// =====================================================================
	// 1. JobValidFn — Job 合法性校验（准入控制的第一道门）
	// =====================================================================
	// 验证三个层面：
	//   a) CheckTaskValid  — 每种 TaskRole 的可用 Task 数 ≥ TaskMinAvailable
	//   b) CheckSubJobValid — 每种 SubJobGroup 的可用 SubJob 数 ≥ MinSubJobs
	//   c) ValidTaskNum     — 全局可用 Task 总数 ≥ MinAvailable
	//
	// 如果验证失败，Job 会被标记为 NotEnoughPods / NotEnoughPodsOfTask，
	// 调度器会跳过该 Job，不会尝试为其分配资源。
	validJobFn := func(obj interface{}) *api.ValidateResult {
		job, ok := obj.(*api.JobInfo)
		if !ok {
			return &api.ValidateResult{
				Pass:    false,
				Message: fmt.Sprintf("Failed to convert <%v> to *JobInfo", obj),
			}
		}

		// 检查 a)：每种 Role 的 Task 数量是否满足 TaskMinAvailable
		// 例如 PS 角色需要 ≥2 个可用 Pod，Worker 角色需要 ≥3 个可用 Pod
		if valid := job.CheckTaskValid(); !valid {
			return &api.ValidateResult{
				Pass:    false,
				Reason:  v1beta1.NotEnoughPodsOfTaskReason,
				Message: "Not enough valid pods of each task for gang-scheduling",
			}
		}

		// 检查 b)：SubJob（子任务组）数量和完整度是否满足要求
		if valid := job.CheckSubJobValid(); !valid {
			return &api.ValidateResult{
				Pass:    false,
				Reason:  v1beta1.NotEnoughPodsOfTaskReason,
				Message: "Not enough valid subGroups of each task for gang-scheduling",
			}
		}

		// 检查 c)：全局可用 Task 总数 ≥ MinAvailable
		// 注：ValidTaskNum 统计的"可用"包括 Pending/Pipelined/Allocated/Succeeded 等状态
		vtn := job.ValidTaskNum()
		if vtn < job.MinAvailable {
			return &api.ValidateResult{
				Pass:   false,
				Reason: v1beta1.NotEnoughPodsReason,
				Message: fmt.Sprintf("Not enough valid tasks for gang-scheduling, valid: %d, min: %d",
					vtn, job.MinAvailable),
			}
		}
		return nil // nil 表示验证通过
	}
	ssn.AddJobValidFn(gp.Name(), validJobFn)

	// =====================================================================
	// 2. PreemptableFn / ReclaimableFn — Gang 感知的抢占和回收保护
	// =====================================================================
	// 核心逻辑：只抢占"超额"的 Task，保护每个 Job 至少保留 MinAvailable 个 Ready Task。
	//
	// 算法：
	//   对于每个候选 victim task：
	//     1. 找到它所属的 Job
	//     2. 计算该 Job 当前有多少 Ready Task
	//     3. 如果 ReadyTaskNum > MinAvailable → 说明有"超额"Task，可以抢一个
	//     4. 如果 ReadyTaskNum ≤ MinAvailable → 不能再抢了，保护 Gang 语义
	//
	// 示例：
	//   Job A: MinAvailable=3, 当前有 5 个 Ready Task
	//     → 可以抢最多 2 个 Task（5-3=2 个超额）
	//   Job B: MinAvailable=3, 当前有 3 个 Ready Task
	//     → 一个都不能抢，否则 Gang 语义被破坏
	preemptableFn := func(preemptor *api.TaskInfo, preemptees []*api.TaskInfo) ([]*api.TaskInfo, int) {
		var victims []*api.TaskInfo
		// jobOccupiedMap 缓存每个 Job 的当前 Ready Task 数，避免重复计算
		jobOccupiedMap := map[api.JobID]int32{}

		for _, preemptee := range preemptees {
			job := ssn.Jobs[preemptee.Job]
			if job == nil {
				// 孤儿 Task：PodGroup 已被删除，但 Task 还残留在缓存中
				klog.Warningf("[gang] Skip preemptee <%s/%s>: job <%s> not found in session (orphaned task from deleted PodGroup)",
					preemptee.Namespace, preemptee.Name, preemptee.Job)
				continue
			}

			// 惰性初始化：首次遇到该 Job 时才查询 ReadyTaskNum
			if _, found := jobOccupiedMap[job.UID]; !found {
				jobOccupiedMap[job.UID] = job.ReadyTaskNum()
			}

			// Gang 保护核心：只有在超额（> MinAvailable）时才允许抢占
			if jobOccupiedMap[job.UID] > job.MinAvailable {
				jobOccupiedMap[job.UID]-- // 模拟：假设该 Task 被抢走
				victims = append(victims, preemptee)
			} else {
				klog.V(4).Infof("Can not preempt task <%v/%v> because job %s ready num(%d) <= MinAvailable(%d) for gang-scheduling",
					preemptee.Namespace, preemptee.Name, job.Name, jobOccupiedMap[job.UID], job.MinAvailable)
			}
		}

		klog.V(4).Infof("Victims from Gang plugins, victims=%+v preemptor=%s", victims, preemptor)

		return victims, util.Permit
	}

	// 抢占和回收使用相同的保护逻辑
	ssn.AddReclaimableFn(gp.Name(), preemptableFn)
	ssn.AddPreemptableFn(gp.Name(), preemptableFn)

	// UnifiedEvictableFn: Gang 层面的统一驱逐接口。
	// 在 bundle 模型（safe/whole split）下，MinAvailable 约束由 bundle 层管理，
	// 因此 gang 插件对统一驱逐路径放行所有候选 Task。
	ssn.AddUnifiedEvictableFn(gp.Name(), func(_ *api.EvictionContext, candidates []*api.TaskInfo) ([]*api.TaskInfo, int) {
		return candidates, util.Permit
	})

	// =====================================================================
	// 3. JobOrderFn — Job 优先级排序
	// =====================================================================
	// 排序规则：已就绪（Ready）的 Job 排在未就绪的 Job 之后。
	//
	// 设计理念：调度器应该优先将资源分配给"接近完成"的 Job（就差几个 Task 就达到 MinAvailable），
	// 而不是那些刚开始、还没任何 Task 就绪的 Job。这减少了"部分资源已分配但永远等不到完整 Gang"的浪费。
	//
	// 返回值语义（CompareFn 约定）：
	//   -1: l < r（l 优先）
	//    1: l > r（r 优先）
	//    0: 相等
	//   这里：如果 l 就绪而 r 未就绪 → 返回 1 → r 优先（未就绪的排前面）
	//         如果 r 就绪而 l 未就绪 → 返回 -1 → l 优先（未就绪的排前面）
	//         都就绪或都未就绪 → 返回 0
	jobOrderFn := func(l, r interface{}) int {
		lv := l.(*api.JobInfo)
		rv := r.(*api.JobInfo)

		lReady := lv.IsReady()
		rReady := rv.IsReady()

		klog.V(4).Infof("Gang JobOrderFn: <%v/%v> is ready: %t, <%v/%v> is ready: %t",
			lv.Namespace, lv.Name, lReady, rv.Namespace, rv.Name, rReady)

		if lReady && rReady {
			return 0
		}

		if lReady {
			return 1 // l 已就绪 → l 排在后面，r 优先
		}

		if rReady {
			return -1 // r 已就绪 → r 排在后面，l 优先
		}

		return 0
	}
	ssn.AddJobOrderFn(gp.Name(), jobOrderFn)

	// SubJobOrderFn: 与 JobOrderFn 相同逻辑，但是对 SubJob 排序
	subJobOrderFn := func(l, r interface{}) int {
		lv := l.(*api.SubJobInfo)
		rv := r.(*api.SubJobInfo)

		lReady := lv.IsReady()
		rReady := rv.IsReady()

		klog.V(4).Infof("Gang SubJobOrderFn: <%v> is ready: %t, <%v> is ready: %t",
			lv.UID, lReady, rv.UID, rReady)

		if lReady && rReady {
			return 0
		}

		if lReady {
			return 1
		}

		if rReady {
			return -1
		}

		return 0
	}
	ssn.AddSubJobOrderFn(gp.Name(), subJobOrderFn)

	// =====================================================================
	// 4. JobReadyFn — Job 就绪判定
	// =====================================================================
	// 一个 Job 被认为"就绪"需要同时满足三个条件：
	//   a) CheckTaskReady    — 每种 TaskRole 的已分配 Task ≥ TaskMinAvailable
	//   b) CheckSubJobReady  — 每种 SubJobGroup 的就绪 SubJob ≥ MinSubJobs
	//   c) IsReady           — ReadyTaskNum + BestEffortPending ≥ MinAvailable
	//
	// 只有当 Job 就绪后，调度器才会实际执行 Bind 操作将 Pod 绑定到节点。
	ssn.AddJobReadyFn(gp.Name(), func(obj interface{}) bool {
		ji := obj.(*api.JobInfo)
		if ji.CheckTaskReady() && ji.CheckSubJobReady() && ji.IsReady() {
			return true
		}
		return false
	})

	// SubJobReadyFn: SubJob 级别的就绪判定
	ssn.AddSubJobReadyFn(gp.Name(), func(obj interface{}) bool {
		sji := obj.(*api.SubJobInfo)
		return sji.IsReady()
	})

	// =====================================================================
	// 5. PipelinedFn — Job Pipeline 状态判定
	// =====================================================================
	// Pipelined 状态的含义：Job 虽然还没完全 Ready（没有足够的 Bound/Running Task），
	// 但已经有足够多的 Task 处于 Pipelined 状态（被调度器分配了节点但等待资源释放）。
	//
	// 这是 Volcano 的"透支"机制：允许 Job 在资源不足时进入等待队列，
	// 一旦被抢占的 Task 释放资源，Pipelined Task 就能转为 Ready。
	//
	// Pipelined 判定条件与 Ready 类似，但计数时额外包含 Pipelined 状态的 Task：
	//   IsPipelined() → WaitingTaskNum + ReadyTaskNum + BestEffortPending ≥ MinAvailable
	pipelinedFn := func(obj interface{}) int {
		ji := obj.(*api.JobInfo)
		if ji.CheckTaskPipelined() && ji.CheckSubJobPipelined() && ji.IsPipelined() {
			return util.Permit
		}
		return util.Reject
	}
	ssn.AddJobPipelinedFn(gp.Name(), pipelinedFn)

	// SubJobPipelinedFn: SubJob 级别的 Pipeline 状态判定
	ssn.AddSubJobPipelinedFn(gp.Name(), func(obj interface{}) int {
		sji := obj.(*api.SubJobInfo)
		if sji.IsPipelined() {
			return util.Permit
		}
		return util.Reject
	})

	// =====================================================================
	// 6. JobStarvingFn — Job "饥饿"状态判定
	// =====================================================================
	// Starving（饥饿）的含义：Job 当前占有（Ready + Pipelined）的 Task 数量
	// 不足 MinAvailable，需要抢占其他 Job 的资源才能满足 Gang 条件。
	//
	// Starving 是触发抢占的充分条件：
	//   - 非 Starving → 已经满足或接近满足 MinAvailable，不需要抢占
	//   - Starving     → 需要抢占资源，调度器会寻找合适的 victim
	//
	// IsStarving() → WaitingTaskNum + ReadyTaskNum < MinAvailable
	jobStarvingFn := func(obj interface{}) bool {
		ji := obj.(*api.JobInfo)
		// 在抢占场景中，只关心 Job 级别的 MinAvailable，不关心 TaskMinAvailable
		return ji.IsStarving()
	}
	ssn.AddJobStarvingFns(gp.Name(), jobStarvingFn)
}

// OnSessionClose 在每次调度会话结束时被调用。
//
// 主要职责：
//  1. 遍历所有 Job，检查是否有未就绪的 Job
//  2. 对未就绪的 Job，计算"差多少 Task 才能满足 MinAvailable"
//  3. 更新 PodGroup Condition（Unschedulable / Scheduled）
//  4. 上报调度指标（unScheduleJobCount, unscheduleTaskCount）
//
// 状态转换逻辑：
//
//	Job.IsReady() == true
//	  → PodGroup Condition = Scheduled（调度成功）
//	Job.IsReady() == false
//	  → PodGroup Condition = UnschedulableType
//	  → Reason = NotEnoughResources
//	  → Message = "X/Y tasks in gang unschedulable: ..."
func (gp *gangPlugin) OnSessionClose(ssn *framework.Session) {
	var unreadyTaskCount int32
	var unScheduleJobCount int

	for _, job := range ssn.Jobs {
		// 跳过没有任何 Task 的空 Job（防御性检查）
		if len(job.Tasks) == 0 {
			continue
		}

		if !job.IsReady() {
			// 计算还差多少 Task 才能满足 MinAvailable。
			//
			// schedulableTaskNum 的计算逻辑：
			//   已 Ready 的 Task 数 + Pending 中已分配过节点（从上次事务中回滚）的 Task 数
			//
			// 为什么需要 LastTransaction？
			//   在抢占场景中，一个 Task 可能在本轮调度中被回滚（状态从 Allocated → Pending）。
			//   这些 Task 在上一轮事务中已经被分配过节点，本轮应该被优先考虑。
			//   LastTransaction 记录了 Task 在上一轮事务中的状态和分配结果。
			schedulableTaskNum := func() (num int32) {
				for _, task := range job.TaskStatusIndex[api.Pending] {
					ctx := task.GetTransactionContext()
					if task.LastTransaction != nil {
						ctx = *task.LastTransaction
					}
					// AllocatedStatus 包含 Bound/Binding/Running/Allocated
					if api.AllocatedStatus(ctx.Status) {
						num++
					}
				}
				return num + job.ReadyTaskNum()
			}

			// 计算缺口：需要 MinAvailable 个 Ready Task，当前差了 unreadyTaskCount 个
			unreadyTaskCount = job.MinAvailable - schedulableTaskNum()
			msg := fmt.Sprintf("%v/%v tasks in gang unschedulable: %v",
				unreadyTaskCount, len(job.Tasks), job.FitError())

			unScheduleJobCount++

			// 只有未终止的 Job 才更新重试计数（避免对已完成的 Job 错误累加）
			if !ssn.IsJobTerminated(job.UID) {
				metrics.RegisterJobRetries(job.Name)
			}

			// 更新 PodGroup Condition → Unschedulable
			jc := &scheduling.PodGroupCondition{
				Type:               scheduling.PodGroupUnschedulableType,
				Status:             v1.ConditionTrue,
				LastTransitionTime: metav1.Now(),
				TransitionID:       string(ssn.UID),
				Reason:             v1beta1.NotEnoughResourcesReason,
				Message:            msg,
			}

			if err := ssn.UpdatePodGroupCondition(job, jc); err != nil {
				klog.Errorf("Failed to update job <%s/%s> condition: %v",
					job.Namespace, job.Name, err)
			}
		} else {
			// Job 已就绪 → 更新 PodGroup Condition → Scheduled
			jc := &scheduling.PodGroupCondition{
				Type:               scheduling.PodGroupScheduled,
				Status:             v1.ConditionTrue,
				LastTransitionTime: metav1.Now(),
				TransitionID:       string(ssn.UID),
				Reason:             "tasks in gang are ready to be scheduled",
				Message:            "",
			}

			if err := ssn.UpdatePodGroupCondition(job, jc); err != nil {
				klog.Errorf("Failed to update job <%s/%s> condition: %v",
					job.Namespace, job.Name, err)
			}
		}

		// 上报未调度 Task 数指标（用于监控和告警）
		if !ssn.IsJobTerminated(job.UID) {
			metrics.UpdateUnscheduleTaskCount(job.Name, int(unreadyTaskCount))
		}
		unreadyTaskCount = 0
	}

	// 上报未调度 Job 总数
	metrics.UpdateUnscheduleJobCount(unScheduleJobCount)
}
