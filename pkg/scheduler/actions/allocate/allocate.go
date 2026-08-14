/*
 Copyright 2021 The Volcano Authors.

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

// Package allocate 实现 Volcano 调度器的核心分配逻辑。
//
// allocate action 是 Volcano 调度流程中最核心的一环，负责为 Job 的 Pending Task
// 分配节点资源。它实现了完整的调度流水线：
//
//	Queue 排序 → Job 排序 → Task 排序 → Predicate 过滤 → NodeOrder 打分 → 分配/Bind
//
// 两条分配路径：
//   - 正常路径（allocateResourcesForTasks）：逐 Task → 逐节点 → 分配
//   - 拓扑/SubJob 路径（allocateForJob）：HyperNode 梯度搜索 + dry-run 多方案择优
//
// 与 Gang 协作：只有在 JobReady（凑够 MinAvailable 个 Allocated Task）时才 Commit Bind，
// Pipelined 状态只保存方案不 Commit，保证 Gang 的 All-or-Nothing 语义。
//
// 关键设计：
//   - 两级梯度节点选择：Idle 优先（Allocated），FutureIdle 降级（Pipelined）
//   - 事务性分配：stmt.Allocate/Pipeline 记录操作，Commit 才真正生效
//   - Dry-run 模式：HyperNode 试分配后 Discard，选最优方案 Recover
package allocate

import (
	"fmt"
	"math"
	"slices"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"

	"volcano.sh/apis/pkg/apis/scheduling"
	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/cmd/scheduler/app/options"
	"volcano.sh/volcano/pkg/features"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/metrics"
	"volcano.sh/volcano/pkg/scheduler/util"
	commonutil "volcano.sh/volcano/pkg/util"
)

// allocateContext 是单次调度周期的上下文，承载所有需要调度的 Queue / Job / Task。
//
// 三层优先级队列结构：
//
//	queues              → QueueOrderFn 排序 → 决定先处理哪个 Queue
//	jobsByQueue[queue]  → JobOrderFn 排序   → 决定 Queue 内先处理哪个 Job
//	tasksNoHardTopology → TaskOrderFn 排序  → 决定 Job 内先分配哪个 Task
//
// 为什么分 queues 和 jobsByQueue 两层？
//   - Queue 之间存在资源竞争，QueueOrderFn（如 proportion）决定谁先拿资源
//   - 同一 Queue 内的 Job 之间也需要排序，JobOrderFn（如 gang 的未就绪优先）决定顺序
//   - 每次只 Pop 一个 Queue 的一个 Job，处理完立刻 Push 回 Queue，重新排队
type allocateContext struct {
	queues              *util.PriorityQueue                 // queue of *api.QueueInfo
	jobsByQueue         map[api.QueueID]*util.PriorityQueue // queue of *api.JobInfo
	jobWorksheet        map[api.JobID]*JobWorksheet
	tasksNoHardTopology map[api.JobID]*util.PriorityQueue // queue of *api.TaskInfo, job without any hard network topology policy use this queue
}

type JobWorksheet struct {
	subJobs          *util.PriorityQueue // queue of *api.SubJobInfo
	subJobWorksheets map[api.SubJobID]*SubJobWorksheet
}

func (w *JobWorksheet) ShallowCopyFrom(another *JobWorksheet) {
	if another == nil {
		return
	}
	w.subJobs = another.subJobs
	w.subJobWorksheets = another.subJobWorksheets
}

func (w *JobWorksheet) Empty() bool {
	return w.subJobs == nil || w.subJobs.Empty()
}

func (w *JobWorksheet) Clone() *JobWorksheet {
	subJobWorksheets := make(map[api.SubJobID]*SubJobWorksheet)
	for subJobID, tasks := range w.subJobWorksheets {
		subJobWorksheets[subJobID] = tasks.Clone()
	}
	return &JobWorksheet{
		subJobs:          w.subJobs.Clone(),
		subJobWorksheets: subJobWorksheets,
	}
}

type SubJobWorksheet struct {
	tasks *util.PriorityQueue // queue of *api.TaskInfo
}

func (w *SubJobWorksheet) ShallowCopyFrom(another *SubJobWorksheet) {
	if another == nil {
		return
	}
	w.tasks = another.tasks
}

func (w *SubJobWorksheet) Empty() bool {
	return w.tasks == nil || w.tasks.Empty()
}

func (w *SubJobWorksheet) Clone() *SubJobWorksheet {
	return &SubJobWorksheet{
		tasks: w.tasks.Clone(),
	}
}

// Action 是 allocate action 的结构体，实现 framework.Action 接口。
//
// 生命周期：New() → Initialize() → Execute() → UnInitialize()
// 每次调度周期创建一个新实例，Execute 只调用一次。
//
// recorder 用于记录 Job/SubJob 在哪个 HyperNode 上成功分配，
// 在 Commit 后调用 UpdateDecisionToJob 更新 Job.AllocatedHyperNode，
// 在 RecoverSubJobStatus 中回滚 SubJob 状态。
type Action struct {
	session *framework.Session
	// configured flag for error cache
	enablePredicateErrorCache bool

	recorder *Recorder
}

func New() *Action {
	return &Action{
		enablePredicateErrorCache: true, // default to enable it
	}
}

func (alloc *Action) Name() string {
	return "allocate"
}

func (alloc *Action) Initialize() {}

func (alloc *Action) parseArguments(ssn *framework.Session) {
	arguments := framework.GetArgOfActionFromConf(ssn.Configurations, alloc.Name())
	arguments.GetBool(&alloc.enablePredicateErrorCache, conf.EnablePredicateErrCacheKey)
}

// Execute 是 allocate action 的入口。
// 遵循 Volcano 调度流水线五步法：
//   1. QueueOrderFn 挑一个 Queue
//   2. JobOrderFn 从 Queue 里挑一个 Job
//   3. TaskOrderFn 从 Job 里挑一个 Task
//   4. PredicateFn 过滤 Task 不能放的节点
//   5. NodeOrderFn 打分选出最佳节点
//
// 实际执行分三步：解析参数 → 构建上下文 → 主分配循环
func (alloc *Action) Execute(ssn *framework.Session) {
	klog.V(5).Infof("Enter Allocate ...")
	defer klog.V(5).Infof("Leaving Allocate ...")

	alloc.parseArguments(ssn)

	// the allocation for pod may have many stages
	// 1. pick a queue named Q (using ssn.QueueOrderFn)
	// 2. pick a job named J from Q (using ssn.JobOrderFn)
	// 3. pick a task T from J (using ssn.TaskOrderFn)
	// 4. use predicateFn to filter out node that T can not be allocated on.
	// 5. use ssn.NodeOrderFn to judge the best node and assign it to T

	alloc.session = ssn
	alloc.recorder = NewRecorder()
	actx := alloc.buildAllocateContext()
	klog.V(3).Infof("Try to allocate resource to %d Queues", actx.queues.Len())
	alloc.allocateResources(actx)
}

func (alloc *Action) buildAllocateContext() *allocateContext {
	ssn := alloc.session

	actx := &allocateContext{
		queues:              util.NewPriorityQueue(ssn.QueueOrderFn), // queues sort queues by QueueOrderFn.
		jobsByQueue:         make(map[api.QueueID]*util.PriorityQueue),
		jobWorksheet:        make(map[api.JobID]*JobWorksheet),
		tasksNoHardTopology: make(map[api.JobID]*util.PriorityQueue),
	}

	for _, job := range ssn.Jobs {
		// If not config enqueue action, change Pending pg into Inqueue state to avoid blocking job scheduling.
		if job.IsPending() {
			if conf.EnabledActionMap["enqueue"] {
				klog.V(4).Infof("Job <%s/%s> Queue <%s> skip allocate, reason: job status is pending.",
					job.Namespace, job.Name, job.Queue)
				continue
			} else {
				klog.V(4).Infof("Job <%s/%s> Queue <%s> status update from pending to inqueue, reason: no enqueue action is configured.",
					job.Namespace, job.Name, job.Queue)
				job.PodGroup.Status.Phase = scheduling.PodGroupInqueue
			}
		}

		if vr := ssn.JobValid(job); vr != nil && !vr.Pass {
			klog.V(4).Infof("Job <%s/%s> Queue <%s> skip allocate, reason: %v, message %v", job.Namespace, job.Name, job.Queue, vr.Reason, vr.Message)
			continue
		}

		if _, found := ssn.Queues[job.Queue]; !found {
			klog.Warningf("Skip adding Job <%s/%s> because its queue %s is not found",
				job.Namespace, job.Name, job.Queue)
			continue
		}

		if !ssn.HyperNodesReadyToSchedule && job.ContainsNetworkTopology() {
			klog.V(4).Infof("Job <%s/%s> Queue <%s> skip allocate, reason: hyperNodes are not ready for scheduling",
				job.Namespace, job.Name, job.Queue)
			continue
		}

		worksheet := alloc.organizeJobWorksheet(job)
		if worksheet.Empty() {
			continue
		}

		if _, found := actx.jobsByQueue[job.Queue]; !found {
			actx.jobsByQueue[job.Queue] = util.NewPriorityQueue(ssn.JobOrderFn)
			actx.queues.Push(ssn.Queues[job.Queue])
		}

		klog.V(4).Infof("Added Job <%s/%s> into Queue <%s>", job.Namespace, job.Name, job.Queue)
		actx.jobsByQueue[job.Queue].Push(job)
		actx.jobWorksheet[job.UID] = worksheet

		// job without any hard network topology policy use actx.tasksNoHardTopology
		if !job.ContainsHardTopology() {
			if subJobWorksheet, exist := worksheet.subJobWorksheets[job.DefaultSubJobID()]; exist {
				actx.tasksNoHardTopology[job.UID] = subJobWorksheet.tasks
			}
		}
	}

	return actx
}

func (alloc *Action) organizeJobWorksheet(job *api.JobInfo) *JobWorksheet {
	ssn := alloc.session

	subJobs := make([]*api.SubJobInfo, 0, len(job.SubJobs))
	subJobCountMap := map[api.SubJobGID]int32{}
	for _, subJob := range job.SubJobs {
		if ssn.SubJobReady(job, subJob) {
			// Record the number of subJobs that have been satisfied in subGroupPolicy
			subJobCountMap[subJob.GID]++
		} else {
			// Filter out subJobs that are already ready.
			subJobs = append(subJobs, subJob)
		}
	}
	slices.SortFunc(subJobs, func(l, r *api.SubJobInfo) int {
		if !ssn.SubJobOrderFn(l, r) {
			return 1
		}
		return -1
	})
	// Find the smallest set of subJobs that meets the requirements for job execution.
	requireSubJobs := sets.Set[api.SubJobID]{}
	for _, subJob := range subJobs {
		if subJobCountMap[subJob.GID] < job.MinSubJobs[subJob.GID] {
			requireSubJobs.Insert(subJob.UID)
			subJobCountMap[subJob.GID]++
		}
	}
	jWorksheet := &JobWorksheet{
		subJobs: util.NewPriorityQueue(func(l, r interface{}) bool {
			lv := l.(*api.SubJobInfo)
			rv := r.(*api.SubJobInfo)

			lreq := requireSubJobs.Has(lv.UID)
			rreq := requireSubJobs.Has(rv.UID)
			if lreq != rreq {
				return lreq
			}
			return ssn.SubJobOrderFn(l, r)
		}),
		subJobWorksheets: make(map[api.SubJobID]*SubJobWorksheet),
	}

	for subJobID, subJob := range job.SubJobs {
		sjWorksheet := &SubJobWorksheet{
			tasks: util.NewPriorityQueue(ssn.TaskOrderFn),
		}

		for _, task := range subJob.TaskStatusIndex[api.Pending] {
			// Skip tasks with external (non-Volcano) scheduling gates
			// Allow Volcano-managed gates (they'll be handled by capacity plugin)
			if task.SchGated && !api.HasOnlyVolcanoSchedulingGate(task.Pod) {
				klog.V(4).Infof("Task <%v/%v> has external scheduling gate, skip it.",
					task.Namespace, task.Name)
				continue
			}

			// Skip BestEffort task in 'allocate' action.
			if task.Resreq.IsEmpty() {
				klog.V(4).Infof("Task <%v/%v> is BestEffort task, skip it.",
					task.Namespace, task.Name)
				continue
			}
			sjWorksheet.tasks.Push(task)
		}

		if !sjWorksheet.Empty() {
			jWorksheet.subJobs.Push(subJob)
			jWorksheet.subJobWorksheets[subJobID] = sjWorksheet
		}
	}

	return jWorksheet
}

// allocateResources 是 allocate action 的主分配循环。
//
// 循环逻辑：
//  1. 从 PriorityQueue 中 Pop 一个 Queue
//  2. 检查 Queue 是否 Overused（超配额）
//  3. 从 Queue 的 Job 队列中 Pop 一个 Job
//  4. 根据 Job 类型走不同分配路径
//  5. 只有 JobReady 才 Commit（Gang 语义：凑不够 MinAvailable 不 Bind）
//  6. Queue 推回，重新排队
//
// 关键设计：
//   - 每次迭代只处理一个 Job，Queue 立刻推回
//   - 如果 Job 还有剩余 Pending Task → Push 回 Job 队列，下一轮继续
//   - OuterLoop 处理完所有 Queue 才退出（queues.Empty()）
//   - JobReady 时才 Commit，Pipelined 状态不 Commit（stmt 自动丢弃）
func (alloc *Action) allocateResources(actx *allocateContext) {
	ssn := alloc.session

	queues := actx.queues
	for {
		if queues.Empty() {
			break
		}

		queue := queues.Pop().(*api.QueueInfo)

		if ssn.Overused(queue) {
			klog.V(3).Infof("Queue <%s> is overused, ignore it.", queue.Name)
			continue
		}

		jobs, found := actx.jobsByQueue[queue.UID]
		if !found || jobs.Empty() {
			klog.V(4).Infof("Can not find jobs for queue %s.", queue.Name)
			continue
		}

		job := jobs.Pop().(*api.JobInfo)
		// Currently, both hard-mode network topology scheduling and subjob level scheduling use allocateForJob.
		// TODO: In the future, we may need to unify the logic of network topology-aware scheduling and normal scheduling.
		if job.ContainsHardTopology() || job.ContainsSubJobPolicy() {
			jobWorksheet := actx.jobWorksheet[job.UID]

			klog.V(3).InfoS("Try to allocate resource for job contains hard topology or subjob policy", "queue", queue.Name, "job", job.UID,
				"allocatedHyperNode", job.AllocatedHyperNode, "subJobNum", jobWorksheet.subJobs.Len())
			stmt := alloc.allocateForJob(job, jobWorksheet, ssn.HyperNodes[framework.ClusterTopHyperNode])
			if stmt != nil && ssn.JobReady(job) { // do not commit stmt when job is pipelined
				stmt.Commit()
				ssn.MarkJobDirty(job.UID)
				alloc.recorder.UpdateDecisionToJob(job, ssn.HyperNodes)

				// There are still left tasks that need to be allocated when min available < replicas, put the job back
				if !jobWorksheet.Empty() {
					jobs.Push(job)
				}
			}
		} else {
			subJob, sjExist := job.SubJobs[job.DefaultSubJobID()]
			tasks, tasksExist := actx.tasksNoHardTopology[job.UID]
			if sjExist && tasksExist {
				klog.V(3).InfoS("Try to allocate resource", "queue", queue.Name, "job", job.UID, "taskNum", tasks.Len())
				stmt := alloc.allocateResourcesForTasks(subJob, tasks, framework.ClusterTopHyperNode)
				if stmt != nil && ssn.JobReady(job) { // do not commit stmt when job is pipelined
					stmt.Commit()

					// There are still left tasks that need to be allocated when min available < replicas, put the job back
					if tasks.Len() > 0 {
						jobs.Push(job)
					}
				}
			} else {
				klog.ErrorS(nil, "Can not find default subJob or tasks for job", "job", job.UID,
					"subJobExist", sjExist, "tasksExist", tasksExist)
			}
		}

		// Put back the queue to priority queue after job's resource allocating finished,
		// To ensure that the priority of the queue is calculated based on the latest resource allocation situation.
		queues.Push(queue)
	}
}

func (alloc *Action) allocateForJob(job *api.JobInfo, jobWorksheet *JobWorksheet, hyperNodeToAllocate *api.HyperNodeInfo) *framework.Statement {
	ssn := alloc.session

	if jobWorksheet == nil || jobWorksheet.Empty() {
		klog.V(4).InfoS("Empty job worksheet", "job", job.UID)
		return nil
	}

	alloc.recorder.SnapshotSubJobStatus(job, jobWorksheet)

	hyperNodeGradients := ssn.HyperNodeGradientForJobFn(job, hyperNodeToAllocate, api.PurposeAllocate)
	for gradient, hyperNodes := range hyperNodeGradients {
		stmtBackup := make(map[string]*framework.Statement)   // backup the statement after the job is allocated to a hyperNode
		jobWorksheetsBackup := make(map[string]*JobWorksheet) // backup the job worksheet after the job is allocated to a hyperNode
		subJobsAllocationScores := make(map[string]float64)   // save the subJobs allocation score of the job allocated to a hyperNode

		for _, hyperNode := range hyperNodes {
			var stmtList []*framework.Statement
			var subJobsAllocationScore float64

			// Clone jobWorksheet and rest job's fit err to make sure it's a clean cache when everytime filter a hyperNode and do not affect each other between hyperNodes.
			job.ResetFitErr()
			jobWorksheetCopy := jobWorksheet.Clone()
			klog.V(3).InfoS("Try to allocate resource for job in hyperNode", "job", job.UID, "hyperNode", hyperNode.Name)

			for !jobWorksheetCopy.subJobs.Empty() {
				subJob := jobWorksheetCopy.subJobs.Pop().(*api.SubJobInfo)
				subJobWorksheet := jobWorksheetCopy.subJobWorksheets[subJob.UID]

				stmt, allocationScore := alloc.allocateForSubJob(subJob, subJobWorksheet, hyperNode)

				if stmt != nil && len(stmt.Operations()) > 0 {
					stmtList = append(stmtList, stmt)
					subJobsAllocationScore += allocationScore
					// push back when subJob is ready and remain pending task
					if !subJobWorksheet.Empty() {
						jobWorksheetCopy.subJobs.Push(subJob)
					}

					if ssn.JobReady(job) {
						break
					}
				}
			}
			// reset the subJobs to initial status
			alloc.recorder.RecoverSubJobStatus(job)

			mergedStmt := framework.SaveOperations(stmtList...)
			if len(mergedStmt.Operations()) == 0 {
				continue // skip recording this empty solution
			}
			if ssn.JobReady(job) || ssn.JobPipelined(job) {
				stmtBackup[hyperNode.Name] = mergedStmt                          // backup successful solution
				jobWorksheetsBackup[hyperNode.Name] = jobWorksheetCopy           // backup remains subJobs
				subJobsAllocationScores[hyperNode.Name] = subJobsAllocationScore // save the subJobs allocation score of the job
			}

			// dry run in every hyperNode
			for _, stmt := range stmtList {
				stmt.Discard()
			}
		}

		if len(subJobsAllocationScores) == 0 {
			klog.V(5).InfoS("Find solution for job fail", "job", job.UID, "gradient", gradient)
			continue // try next gradient
		}

		bestHyperNode, err := alloc.selectBestHyperNodeForJob(subJobsAllocationScores, job)
		if err != nil {
			klog.ErrorS(err, "Cannot find best hyper node for job", "job", job.UID, "gradient", gradient)
			return nil
		}

		// recover the stmt
		bestStmt := stmtBackup[bestHyperNode]
		finalStmt := framework.NewStatement(ssn)
		if err = finalStmt.RecoverOperations(bestStmt); err != nil {
			klog.ErrorS(err, "Failed to recover operations", "job", job.UID, "hyperNode", bestHyperNode)
			return nil
		}

		// inherit the remains worksheet after allocate to the best hyperNode
		jobWorksheet.ShallowCopyFrom(jobWorksheetsBackup[bestHyperNode])

		alloc.recorder.SaveJobDecision(job.UID, bestHyperNode)
		klog.V(3).InfoS("Allocate job to hyperNode success", "job", job.UID, "hyperNode", bestHyperNode)

		return finalStmt
	}

	klog.V(5).InfoS("Cannot find any solution for job", "job", job.UID)
	return nil
}

func (alloc *Action) allocateForSubJob(subJob *api.SubJobInfo, subJobWorksheet *SubJobWorksheet, hyperNodeForJob *api.HyperNodeInfo) (*framework.Statement, float64) {
	ssn := alloc.session
	job := ssn.Jobs[subJob.Job]

	if subJobWorksheet == nil || subJobWorksheet.Empty() {
		klog.V(4).InfoS("Empty subJob worksheet", "job", subJob.Job, "subJob", subJob.UID)
		return nil, 0
	}

	klog.V(3).InfoS("Try to allocate resource for subJob", "job", subJob.Job, "subJob", subJob.UID,
		"allocatedHyperNode", subJob.AllocatedHyperNode, "nominatedHyperNode", subJob.NominatedHyperNode,
		"taskNum", subJobWorksheet.tasks.Len())

	if subJob.NominatedHyperNode != "" {
		if stmt, score, ok := alloc.allocateFromNomination(subJob, subJobWorksheet, hyperNodeForJob); ok {
			return stmt, score
		}
	}

	hyperNodeGradients := ssn.HyperNodeGradientForSubJobFn(subJob, hyperNodeForJob, api.PurposeAllocate)
	for gradient, hyperNodes := range hyperNodeGradients {
		stmtBackup := make(map[string]*framework.Statement)         // backup the statement after the subJob is allocated to a hyperNode
		subJobWorksheetsBackup := make(map[string]*SubJobWorksheet) // backup the subJob worksheet after the subJob is allocated to a hyperNode

		for _, hyperNode := range hyperNodes {
			// Clone subJobWorksheet and rest subJob's fit err to make sure it's a clean cache when everytime filter a hyperNode and do not affect each other between hyperNodes.
			job.ResetSubJobFitErr(subJob.UID)
			subJobWorksheetCopy := subJobWorksheet.Clone()

			klog.V(3).InfoS("Try to allocate resource for tasks in subJob", "job", subJob.Job,
				"subJob", subJob.UID, "taskNum", subJobWorksheetCopy.tasks.Len(), "hyperNode", hyperNode.Name)
			stmt := alloc.allocateResourcesForTasks(subJob, subJobWorksheetCopy.tasks, hyperNode.Name)

			if stmt != nil && len(stmt.Operations()) > 0 {
				stmtBackup[hyperNode.Name] = framework.SaveOperations(stmt)  // backup successful solution
				subJobWorksheetsBackup[hyperNode.Name] = subJobWorksheetCopy // backup remains tasks
				stmt.Discard()                                               // dry run in every hyperNode
			}
		}

		if len(stmtBackup) == 0 {
			klog.V(5).InfoS("Find solution for subJob fail", "subJob", subJob.UID, "gradient", gradient)
			continue // try next gradient
		}

		// select the best solution
		bestHyperNode, bestScore, err := alloc.selectBestHyperNodeForSubJob(stmtBackup, subJob)
		if err != nil {
			klog.ErrorS(err, "Cannot find best hyper node for subJob", "subJob", subJob.UID, "gradient", gradient)
			return nil, 0
		}

		// recover the stmt and update subJob's allocatedHyperNode field
		bestStmt := stmtBackup[bestHyperNode]
		finalStmt := framework.NewStatement(ssn)
		if err = finalStmt.RecoverOperations(bestStmt); err != nil {
			klog.ErrorS(err, "Failed to recover operations", "subJob", subJob.UID, "hyperNode", bestHyperNode)
			return nil, 0
		}
		newAllocatedHyperNode := ssn.HyperNodes.GetLCAHyperNode(subJob.AllocatedHyperNode, bestHyperNode)
		subJob.AllocatedHyperNode = newAllocatedHyperNode

		// inherit the remains worksheet after allocate to the best hyperNode
		subJobWorksheet.ShallowCopyFrom(subJobWorksheetsBackup[bestHyperNode])

		alloc.recorder.SaveSubJobDecision(subJob.Job, hyperNodeForJob.Name, subJob.UID, newAllocatedHyperNode)
		klog.V(3).InfoS("Allocate subJob to hyperNode success", "subJob", subJob.UID,
			"hyperNode", bestHyperNode, "score", bestScore, "newAllocatedHyperNode", newAllocatedHyperNode)

		return finalStmt, bestScore
	}

	klog.V(5).InfoS("Cannot find any solution for subJob", "subJob", subJob.UID)
	return nil, 0
}

// selectBestHyperNodeForJob return the best hyperNode for the job,
// it will score and select the best hyperNode among all available hyperNodes.
func (alloc *Action) selectBestHyperNodeForJob(subJobsAllocationScores map[string]float64, job *api.JobInfo) (string, error) {
	highestScore := math.Inf(-1)
	bestHyperNode := ""
	for hyperNode, score := range subJobsAllocationScores {
		if score > highestScore {
			highestScore = score
			bestHyperNode = hyperNode
		}
	}

	if bestHyperNode == "" {
		return "", fmt.Errorf("no solution found for job %s", job.UID)
	}

	return bestHyperNode, nil
}

// selectBestHyperNodeForSubJob return the best hyperNode for the subJob,
// it will score and select the best hyperNode among all available hyperNodes.
func (alloc *Action) selectBestHyperNodeForSubJob(stmts map[string]*framework.Statement, subJob *api.SubJobInfo) (string, float64, error) {
	if len(stmts) <= 0 {
		return "", 0, fmt.Errorf("no solution found for subJob %s", subJob.UID)
	}

	ssn := alloc.session
	candidateHyperNodeGroups := make(map[string][]*api.NodeInfo)
	for hyperNode := range stmts {
		candidateHyperNodeGroups[hyperNode] = ssn.RealNodesList[hyperNode]
	}

	hyperNodeScores, err := util.PrioritizeHyperNodes(candidateHyperNodeGroups, subJob, ssn.HyperNodeOrderMapFn)
	if err != nil {
		return "", 0, fmt.Errorf("prioritize hyperNodes for subJob %s fail: %w", subJob.UID, err)
	}

	bestHyperNode, bestScore := util.SelectBestHyperNodeAndScore(hyperNodeScores)
	if bestHyperNode == "" {
		return "", 0, fmt.Errorf("cannot find best hyperNode for subJob %s", subJob.UID)
	}
	return bestHyperNode, bestScore, nil
}

// nominationPlanEntry pairs a pending task with its NominatedHyperNode leaf node.
type nominationPlanEntry struct {
	task *api.TaskInfo
	node *api.NodeInfo
}

// allocateFromNomination is the quick path to allocate a subJob's pending
// tasks based on NominatedHyperNode + per-task NominatedNodeName, skipping
// the gradient search. On any validation miss it clears the nomination so
// the caller falls back to the regular allocate path.
func (alloc *Action) allocateFromNomination(subJob *api.SubJobInfo, subJobWorksheet *SubJobWorksheet, hyperNodeForJob *api.HyperNodeInfo) (stmt *framework.Statement, score float64, ok bool) {
	ssn := alloc.session
	job := ssn.Jobs[subJob.Job]
	queue := ssn.Queues[job.Queue]
	pinned := subJob.NominatedHyperNode

	defer func() {
		if !ok {
			invalidateSubJobNomination(subJob, subJobWorksheet)
		}
	}()

	leafNodes, exist := ssn.RealNodesList[pinned]
	if !exist || len(leafNodes) == 0 {
		klog.V(3).InfoS("NominatedHyperNode no longer in topology, falling back to normal allocation process",
			"subJob", subJob.UID, "nominatedHyperNode", pinned)
		return nil, 0, false
	}
	leafNodeNames := sets.New[string]()
	for _, n := range leafNodes {
		if n != nil {
			leafNodeNames.Insert(n.Name)
		}
	}

	plan, validated := alloc.validateNomination(subJob, subJobWorksheet, queue, leafNodeNames)
	if !validated {
		return nil, 0, false
	}

	stmt = framework.NewStatement(ssn)
	for _, p := range plan {
		if subJob.WithNetworkTopology() {
			p.task.JobAllocatedHyperNode = pinned
		}
		if err := alloc.allocateResourcesForTask(stmt, p.task, p.node, job); err != nil {
			klog.ErrorS(err, "Allocate from nomination fail, falling back to normal allocation process",
				"subJob", subJob.UID, "task", p.task.UID, "node", p.node.Name)
			stmt.Discard()
			return nil, 0, false
		}
	}

	// Validation ran on a clone of tasks; drain the real worksheet so
	// allocateForSubJob's caller observes Empty() and does not re-enqueue
	// this subJob into the gradient search.
	for !subJobWorksheet.tasks.Empty() {
		subJobWorksheet.tasks.Pop()
	}
	newAllocatedHyperNode := ssn.HyperNodes.GetLCAHyperNode(subJob.AllocatedHyperNode, pinned)
	subJob.AllocatedHyperNode = newAllocatedHyperNode
	alloc.recorder.SaveSubJobDecision(subJob.Job, hyperNodeForJob.Name, subJob.UID, newAllocatedHyperNode)
	klog.V(3).InfoS("Allocate subJob from nomination success", "subJob", subJob.UID,
		"nominatedHyperNode", pinned, "newAllocatedHyperNode", newAllocatedHyperNode)
	return stmt, 0, true
}

// validateNomination checks each pending task's NominatedNodeName against the
// pinned hyperNode's leaf set plus PrePredicate/Predicate. On any miss the
// caller invalidates the nomination.
//
// TODO: the per-task check sequence overlaps with allocateResourcesForTasks's
// pre-bind path; consider unifying once we are ready to touch the regular
// allocate path.
func (alloc *Action) validateNomination(subJob *api.SubJobInfo, subJobWorksheet *SubJobWorksheet, queue *api.QueueInfo, leafNodeNames sets.Set[string]) ([]nominationPlanEntry, bool) {
	ssn := alloc.session
	pinned := subJob.NominatedHyperNode
	ph := util.NewPredicateHelper()
	plan := make([]nominationPlanEntry, 0, subJobWorksheet.tasks.Len())
	preview := subJobWorksheet.tasks.Clone()
	for !preview.Empty() {
		task := preview.Pop().(*api.TaskInfo)
		if !ssn.Allocatable(queue, task) {
			klog.V(3).InfoS("Task with nominated node is not allocatable, falling back to normal allocation process",
				"queue", queue.Name, "subJob", subJob.UID, "task", task.UID)
			return nil, false
		}
		nominated := task.Pod.Status.NominatedNodeName
		if nominated == "" {
			klog.V(3).InfoS("Task missing NominatedNodeName under NominatedHyperNode, falling back to normal allocation process",
				"subJob", subJob.UID, "task", task.UID, "nominatedHyperNode", pinned)
			return nil, false
		}
		if !leafNodeNames.Has(nominated) {
			klog.V(3).InfoS("Task NominatedNodeName outside NominatedHyperNode leaf set, falling back to normal allocation process",
				"subJob", subJob.UID, "task", task.UID, "nominated", nominated, "nominatedHyperNode", pinned)
			return nil, false
		}
		nodeInfo, ok := ssn.Nodes[nominated]
		if !ok || nodeInfo == nil {
			klog.V(3).InfoS("NominatedNodeName not found in session nodes, falling back to normal allocation process",
				"subJob", subJob.UID, "task", task.UID, "nominated", nominated)
			return nil, false
		}
		if err := ssn.PrePredicateFn(task); err != nil {
			klog.V(3).InfoS("PrePredicate failed against nominated node, falling back to normal allocation process",
				"subJob", subJob.UID, "task", task.UID, "node", nominated, "err", err)
			return nil, false
		}
		predicateNodes, _ := ph.PredicateNodes(task, []*api.NodeInfo{nodeInfo}, alloc.predicate, alloc.enablePredicateErrorCache, ssn.NodesInShard)
		if len(predicateNodes) == 0 {
			klog.V(3).InfoS("Predicate failed against nominated node, falling back to normal allocation process",
				"subJob", subJob.UID, "task", task.UID, "node", nominated)
			return nil, false
		}
		plan = append(plan, nominationPlanEntry{task: task, node: nodeInfo})
	}
	return plan, true
}

func invalidateSubJobNomination(subJob *api.SubJobInfo, subJobWorksheet *SubJobWorksheet) {
	subJob.NominatedHyperNode = ""
	if subJobWorksheet == nil {
		return
	}
	preview := subJobWorksheet.tasks.Clone()
	for !preview.Empty() {
		task := preview.Pop().(*api.TaskInfo)
		if task.Pod != nil && task.Pod.Status.NominatedNodeName != "" {
			task.Pod.Status.NominatedNodeName = ""
		}
	}
}

// allocateResourcesForTasks 是正常分配路径（无硬拓扑/SubJob 策略）的核心函数。
//
// 参数：
//   - subJob: 当前要调度的 SubJob
//   - tasks: PriorityQueue of *api.TaskInfo，按 TaskOrderFn 排序
//   - hyperNode: 当前搜索的 HyperNode 名
//
// 对一个 SubJob 的所有 Pending Task 逐 Task 分配节点：
//  1. Allocatable 检查 → 过滤 Gate task → 检查 FitError 缓存
//  2. PrePredicateFn → 过滤（失败则 break，终止整个 SubJob）
//  3. NominatedNodeName 快速路径 → 普通 Predicate 全节点过滤
//  4. Predicate 失败 → NeedContinueAllocating 决定 break/continue
//  5. prioritizeNodes → 两级梯度打分 → 选出最佳节点
//  6. allocateResourcesForTask → Idle 够则 Allocate，否则 FutureIdle 够则 Pipeline
//  7. SubJobReady 检查 → 凑够 MinAvailable 就停
//
// 返回值：
//   - SubJobReady → 返回 stmt（可 Commit）
//   - SubJobPipelined → 返回 stmt（不 Commit）
//   - 都不满足 → Discard → 返回 nil
//
// 关键行为：
//   - PrePredicateFn 失败 → break（终止 SubJob，不是 continue）
//   - PredicateFn 失败 → NeedContinueAllocating() 决定是否继续
//   - 每分配一个 Task 检查一次 SubJobReady，够了就停
func (alloc *Action) allocateResourcesForTasks(subJob *api.SubJobInfo, tasks *util.PriorityQueue, hyperNode string) *framework.Statement {
	ssn := alloc.session

	job := ssn.Jobs[subJob.Job]
	queue := ssn.Queues[job.Queue]
	nodes, exist := ssn.RealNodesList[hyperNode]
	if !exist || len(nodes) == 0 {
		klog.V(4).InfoS("There is no node in hyperNode", "job", job.UID, "hyperNode", hyperNode)
		return nil
	}

	nodeNameSet := make(map[string]struct{}, len(nodes))
	for _, n := range nodes {
		if n != nil {
			nodeNameSet[n.Name] = struct{}{}
		}
	}

	stmt := framework.NewStatement(ssn)
	ph := util.NewPredicateHelper()

	allocatedHyperNode := subJob.AllocatedHyperNode

	for !tasks.Empty() {
		task := tasks.Pop().(*api.TaskInfo)
		// 判断 queue 是否有足够的资源来分配 task，如果没有就跳过这个 task，继续下一个 task 的分配。
		if !ssn.Allocatable(queue, task) {
			klog.V(3).Infof("Queue <%s> is overused when considering task <%s>, ignore it.", queue.Name, task.Name)
			continue
		}

		// If task passed allocation check and has the QueueAllocationGate, initiate async gate removal.
		// Gate will be removed by the background worker (best effort).
		if utilfeature.DefaultFeatureGate.Enabled(features.SchedulingGatesQueueAdmission) &&
			task.SchGated && api.HasQueueAllocationGateAnnotation(task.Pod) {
			klog.V(3).Infof("Task %s/%s has the QueueAllocationGate, queue async gate removal", task.Namespace, task.Name)
			ssn.SchGateManager().Enqueue(task)
		}

		// Skip gated tasks. If someone added the Volcano gate without the opt-in annotation,
		// warn them since the gate will never be removed automatically.
		if task.SchGated {
			if api.HasOnlyVolcanoSchedulingGate(task.Pod) && !api.HasQueueAllocationGateAnnotation(task.Pod) {
				klog.Warningf("Task %s/%s has Volcano scheduling gate but missing the opt-in annotation %q; gate will not be removed automatically",
					task.Namespace, task.Name, schedulingv1beta1.QueueAllocationGateKey)
			}
			continue
		}

		// check if the task with its spec has already predicates failed
		if job.TaskHasFitErrors(subJob.UID, task) {
			msg := fmt.Sprintf("Task %s with role spec %s has already predicated failed, skip", task.Name, task.TaskRole)
			klog.V(5).Info(msg)
			fitErrors := api.NewFitErrors()
			fitErrors.SetError(msg)
			job.NodesFitErrors[task.UID] = fitErrors
			continue
		}

		klog.V(3).Infof("There are <%d> nodes for Job <%v/%v>", len(nodes), job.Namespace, job.Name)

		// PrePredicateFn 是 Predicate 的预处理步骤。
		// 如果 PrePredicate 失败，说明当前调度上下文有问题，整个 SubJob 应该停止分配。
		// 注意这里是 break 不是 continue —— PrePredicate 失败意味着此 SubJob 无法继续。
		if err := ssn.PrePredicateFn(task); err != nil {
			klog.V(3).Infof("PrePredicate for task %s/%s failed for: %v", task.Namespace, task.Name, err)
			fitErrors := api.NewFitErrors()
			for _, ni := range nodes {
				fitErrors.SetNodeError(ni.Name, err)
			}
			job.NodesFitErrors[task.UID] = fitErrors
			break
		}

		var predicateNodes []*api.NodeInfo
		var fitErrors *api.FitErrors

		// "NominatedNodeName" can potentially be set in a previous scheduling cycle as a result of preemption.
		// This node is likely the only candidate that will fit the pod, and hence we try it first before iterating over all nodes.
		// Only honor it when the nominated node belongs to this iteration's leaf set (defense in depth against cross-domain leaks).
		if nominated := task.Pod.Status.NominatedNodeName; len(nominated) > 0 {
			if _, inLeafSet := nodeNameSet[nominated]; inLeafSet {
				if nominatedNodeInfo, ok := ssn.Nodes[nominated]; ok && task.InitResreq.LessEqual(nominatedNodeInfo.FutureIdle(), api.Zero) {
					predicateNodes, fitErrors = ph.PredicateNodes(task, []*api.NodeInfo{nominatedNodeInfo}, alloc.predicate, alloc.enablePredicateErrorCache, ssn.NodesInShard)
				}
			}
		}

		// If the nominated node is not found or the nominated node is not suitable for the task, we need to find a suitable node for the task from all nodes.
		if len(predicateNodes) == 0 {
			predicateNodes, fitErrors = ph.PredicateNodes(task, nodes, alloc.predicate, alloc.enablePredicateErrorCache, ssn.NodesInShard)
		}

		if len(predicateNodes) == 0 {
			// TODO: Need to add PostFilter extension point implementation here. For example, the DRA plugin includes the PostFilter extension point,
			// but the DRA's PostFilter only occurs in extreme error conditions: Suppose a pod uses two claims. In the first scheduling attempt,
			// a node is picked and PreBind manages to update the first claim so that it is allocated and reserved for the pod.
			// But then updating the second claim fails (e.g., apiserver down) and the scheduler has to retry. During the next pod scheduling attempt,
			// the original node is no longer usable for other reasons. Other nodes are not usable either because of the allocated claim.
			// The DRA scheduler plugin detects that and then when scheduling fails (= no node passed filtering), it recovers by de-allocating the allocated claim in PostFilter.
			if fitErrors != nil && hyperNode != framework.ClusterTopHyperNode {
				fitErrors.SetHyperNode(hyperNode)
			}
			job.NodesFitErrors[task.UID] = fitErrors
			// Assume that all left tasks are allocatable, but can not meet gang-scheduling min member,
			// so we should break from continuously allocating.
			// otherwise, should continue to find other allocatable task
			if job.NeedContinueAllocating(subJob.UID) {
				continue
			} else {
				break
			}
		}

		if subJob.WithNetworkTopology() {
			task.JobAllocatedHyperNode = allocatedHyperNode
		}

		// 两级梯度打分：Idle 优先（Allocated），FutureIdle 降级（Pipelined）
		bestNode, _ := alloc.prioritizeNodes(ssn, task, predicateNodes)
		if bestNode == nil {
			continue
		}

		if err := alloc.allocateResourcesForTask(stmt, task, bestNode, job); err != nil {
			klog.ErrorS(err, "Allocate resources for task fail", "task", task.Name)
			continue
		}

		if subJob.WithNetworkTopology() {
			allocatedHyperNode = getNewAllocatedHyperNode(ssn, bestNode.Name, allocatedHyperNode)
		}

		// 每分配一个 Task 就检查 SubJobReady。
		// 一旦满足 MinAvailable，不再继续分配多余 Task（gang 语义下分配多余 Task 无意义）。
		if ssn.SubJobReady(job, subJob) {
			break
		}
	}

	if ssn.SubJobReady(job, subJob) {
		klog.V(3).InfoS("SubJob ready, return statement", "job", job.UID, "subJob", subJob.UID)
		if subJob.IsSoftTopologyMode() {
			subJob.AllocatedHyperNode = allocatedHyperNode
		}
		return stmt
	} else if ssn.SubJobPipelined(job, subJob) {
		klog.V(3).InfoS("SubJob pipelined, return statement", "job", job.UID, "subJob", subJob.UID)
		return stmt
	}

	stmt.Discard()
	return nil
}

// getNewAllocatedHyperNode Obtain the newly allocated hyperNode for the job in soft topology mode
func getNewAllocatedHyperNode(ssn *framework.Session, bestNode string, jobAllocatedHyperNode string) string {
	hyperNode := util.FindHyperNodeForNode(bestNode, ssn.RealNodesList, ssn.HyperNodesTiers, ssn.HyperNodesSetByTier)
	if hyperNode != "" {
		if jobAllocatedHyperNode == "" {
			return hyperNode
		}
		return ssn.HyperNodes.GetLCAHyperNode(hyperNode, jobAllocatedHyperNode)
	}
	return jobAllocatedHyperNode
}

// prioritizeNodes 从 Predicate 通过的节点中选出最佳节点。
//
// 两级梯度机制：
//
//	梯度 1: Idle ≥ Request       → 当前空闲足够 → Allocate（直接分配）
//	梯度 2: FutureIdle ≥ Request  → Idle+Releasing 够 → Pipeline（等释放）
//
// 四个候选列表（按优先级从高到低）：
//  1. idleCandidateNodes              → Idle 够 + 本 Shard
//  2. idleCandidateNodesInOtherShards → Idle 够 + 其他 Shard
//  3. futureIdleCandidateNodes        → FutureIdle 够 + 本 Shard
//  4. futureIdleCandidateNodesInOtherShards → FutureIdle 够 + 其他 Shard
//
// 优先使用梯度 1，只有当梯度 1 一个节点都找不到时，才降级到梯度 2。
// 这对应了 allocateResourcesForTask 的两种分配方式：
//   - Idle 够 → Allocate  → Status = Allocated
//   - FutureIdle 够 → Pipeline → Status = Pipelined
//
// 如果候选节点 >1，调用 NodeOrderFn（binpack/nodeorder 等）打分，BestNodeFn 选最优。
func (alloc *Action) prioritizeNodes(ssn *framework.Session, task *api.TaskInfo, predicateNodes []*api.NodeInfo) (*api.NodeInfo, float64) {
	// Candidate nodes are divided into two gradients:
	// - the first gradient node: a list of free nodes that satisfy the task resource request;
	// - The second gradient node: the node list whose sum of node idle resources and future idle meets the task resource request;
	// Score the first gradient node first. If the first gradient node meets the requirements, ignore the second gradient node list,
	// otherwise, score the second gradient node and select the appropriate node.
	shardingMode := options.ServerOpts.ShardingMode
	var candidateNodes [][]*api.NodeInfo
	var idleCandidateNodes []*api.NodeInfo
	var futureIdleCandidateNodes []*api.NodeInfo
	var idleCandidateNodesInOtherShards []*api.NodeInfo
	var futureIdleCandidateNodesInOtherShards []*api.NodeInfo
	for _, n := range predicateNodes {
		if task.InitResreq.LessEqual(n.Idle, api.Zero) {
			if shardingMode == commonutil.SoftShardingMode && !ssn.NodesInShard.Has(n.Name) {
				idleCandidateNodesInOtherShards = append(idleCandidateNodesInOtherShards, n)
			} else {
				idleCandidateNodes = append(idleCandidateNodes, n)
			}
		} else if task.InitResreq.LessEqual(n.FutureIdle(), api.Zero) {
			if shardingMode == commonutil.SoftShardingMode && !ssn.NodesInShard.Has(n.Name) {
				futureIdleCandidateNodesInOtherShards = append(futureIdleCandidateNodesInOtherShards, n)
			} else {
				futureIdleCandidateNodes = append(futureIdleCandidateNodes, n)
			}
		} else {
			klog.V(5).Infof("Predicate filtered node %v, idle: %v and future idle: %v do not meet the requirements of task: %v",
				n.Name, n.Idle, n.FutureIdle(), task.Name)
		}
	}

	// To allocate to nodes with enough resource and nodes within shard of this scheduler first, allocation of Pod follow below order:
	// 1. Node with IDLE resource in shard for this scheduler
	// 2. Node with IDLE resource in shard for other scheduler  (empty if sharding mode is not soft)
	// 3. Node with Future IDLE resource in shard for this scheduler
	// 4. Node with Future IDLE resource in shard for other scheduler (empty if sharding mode is not soft)
	candidateNodes = append(candidateNodes, idleCandidateNodes)
	candidateNodes = append(candidateNodes, idleCandidateNodesInOtherShards)
	candidateNodes = append(candidateNodes, futureIdleCandidateNodes)
	candidateNodes = append(candidateNodes, futureIdleCandidateNodesInOtherShards)

	var bestNode *api.NodeInfo
	var higestScore float64
	for index, nodes := range candidateNodes {
		if klog.V(5).Enabled() {
			for _, node := range nodes {
				klog.V(5).Infof("node %v, idle: %v, future idle: %v", node.Name, node.Idle, node.FutureIdle())
			}
		}
		switch {
		case len(nodes) == 0:
			klog.V(5).Infof("Task: %v, no matching node is found in the candidateNodes（index: %d） list.", task.Name, index)
		case len(nodes) == 1: // If only one node after predicate, just use it.
			bestNode = nodes[0]
		case len(nodes) > 1: // If more than one node after predicate, using "the best" one
			nodeScores := util.PrioritizeNodes(task, nodes, ssn.BatchNodeOrderFn, ssn.NodeOrderMapFn, ssn.NodeOrderReduceFn)

			bestNode = ssn.BestNodeFn(task, nodeScores)
			if bestNode == nil {
				bestNode, higestScore = util.SelectBestNodeAndScore(nodeScores)
			}
		}

		// If a proper node is found in idleCandidateNodes, skip futureIdleCandidateNodes and directly return the node information.
		if bestNode != nil {
			break
		}
	}
	return bestNode, higestScore
}

// allocateResourcesForTask 将一个 Task 分配到指定节点的具体操作。
//
// 这是 allocate 的最终环节，对应两级梯度节点选择的两种结果：
//
//	节点 Idle ≥ Task 请求 → stmt.Allocate(task, node)
//	  → task.Status = Allocated
//	  → task.Pod.Spec.NodeName = hostname
//	  → node.Idle 减少，node.Used 增加
//
//	节点 Idle 不够但 FutureIdle(=Idle+Releasing) 够 → stmt.Pipeline(task, node.Name)
//	  → task.Status = Pipelined
//	  → task.NodeName = hostname
//	  → 等 node.Releasing 资源释放后自动转 Allocated
//
// 注意：Allocate/Pipeline 只是记录操作到 stmt.operations，并不立即生效。
// 真正生效需要 stmt.Commit() 调用，而 Commit 只有在 JobReady 条件下才执行。
func (alloc *Action) allocateResourcesForTask(stmt *framework.Statement, task *api.TaskInfo, node *api.NodeInfo, job *api.JobInfo) (err error) {
	// Allocate idle resource to the task.
	if task.InitResreq.LessEqual(node.Idle, api.Zero) {
		klog.V(3).Infof("Binding Task <%v/%v> to node <%v>", task.Namespace, task.Name, node.Name)
		if err = stmt.Allocate(task, node); err != nil {
			klog.Errorf("Failed to bind Task %v on %v in Session %v, err: %v",
				task.UID, node.Name, alloc.session.UID, err)
		} else {
			metrics.UpdateE2eSchedulingDurationByJob(job.Name, string(job.Queue), job.Namespace, metrics.Duration(job.CreationTimestamp.Time))
			metrics.UpdateE2eSchedulingLastTimeByJob(job.Name, string(job.Queue), job.Namespace, time.Now())
		}
		return
	}

	klog.V(3).Infof("Predicates failed in allocate for task <%s/%s> on node <%s> with limited resources",
		task.Namespace, task.Name, node.Name)

	// Allocate releasing resource to the task if any.
	if task.InitResreq.LessEqual(node.FutureIdle(), api.Zero) {
		klog.V(3).Infof("Pipelining Task <%v/%v> to node <%v> for <%v> on <%v>",
			task.Namespace, task.Name, node.Name, task.InitResreq, node.Releasing)
		if err = stmt.Pipeline(task, node.Name, false); err != nil {
			klog.Errorf("Failed to pipeline Task %v on %v in Session %v for %v.",
				task.UID, node.Name, alloc.session.UID, err)
		} else {
			metrics.UpdateE2eSchedulingDurationByJob(job.Name, string(job.Queue), job.Namespace, metrics.Duration(job.CreationTimestamp.Time))
			metrics.UpdateE2eSchedulingLastTimeByJob(job.Name, string(job.Queue), job.Namespace, time.Now())
		}
	}
	return
}

func (alloc *Action) predicate(task *api.TaskInfo, node *api.NodeInfo) error {
	// Check for Resource Predicate
	var statusSets api.StatusSets
	if ok, resources := task.InitResreq.LessEqualWithResourcesName(node.FutureIdle(), api.Zero); !ok {
		statusSets = append(statusSets, &api.Status{Code: api.Unschedulable, Reason: api.WrapInsufficientResourceReason(resources)})
		return api.NewFitErrWithStatus(task, node, statusSets...)
	}
	return alloc.session.PredicateForAllocateAction(task, node)
}

func (alloc *Action) UnInitialize() {}
