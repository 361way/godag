package engine

import (
	"context"
	"strings"
	"sync"
	"time"

	"dag-app/internal/dag"
	"dag-app/internal/executor"
)

// NodeStatus 节点执行状态
type NodeStatus string

const (
	StatusPending NodeStatus = "pending" // 等待执行
	StatusRunning NodeStatus = "running" // 执行中
	StatusSuccess NodeStatus = "success" // 成功
	StatusFailed  NodeStatus = "failed"  // 失败
	StatusSkipped NodeStatus = "skipped" // 被跳过（禁用或上游失败）
)

// NodeResult 单个节点的运行结果快照
type NodeResult struct {
	ID        string     `json:"id"`
	Name      string     `json:"name"`
	Status    NodeStatus `json:"status"`
	ExitCode  int        `json:"exit_code"`
	Stdout    string     `json:"stdout"`
	Stderr    string     `json:"stderr"`
	Error     string     `json:"error"`
	StartedAt *time.Time `json:"started_at"`
	EndedAt   *time.Time `json:"ended_at"`
}

// Run 表示一次完整的 DAG 执行记录
type Run struct {
	ID        string                 `json:"id"`
	StartedAt time.Time              `json:"started_at"`
	EndedAt   *time.Time             `json:"ended_at"`
	Finished  bool                   `json:"finished"`
	Nodes     map[string]*NodeResult `json:"nodes"`
	mu        sync.Mutex
}

// Snapshot 返回 Run 的只读拷贝，便于并发安全地序列化
func (r *Run) Snapshot() *Run {
	r.mu.Lock()
	defer r.mu.Unlock()
	nodes := make(map[string]*NodeResult, len(r.Nodes))
	for k, v := range r.Nodes {
		cp := *v
		nodes[k] = &cp
	}
	return &Run{
		ID:        r.ID,
		StartedAt: r.StartedAt,
		EndedAt:   r.EndedAt,
		Finished:  r.Finished,
		Nodes:     nodes,
	}
}

// Engine DAG 执行引擎
type Engine struct {
	maxParallel int
}

// New 创建引擎，maxParallel 为最大并发数，<=0 表示不限制
func New(maxParallel int) *Engine {
	return &Engine{maxParallel: maxParallel}
}

type completion struct {
	id     string
	status NodeStatus
	stdout string
}

// envKey 将节点 ID 规范化为合法的环境变量名片段（大写，非字母数字转下划线）
func envKey(id string) string {
	var b strings.Builder
	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r - 32)
		case (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9'):
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	return b.String()
}

// Execute 并发执行 DAG，遵循依赖关系；被禁用的节点及其下游会被级联跳过
func (e *Engine) Execute(ctx context.Context, d *dag.DAG, run *Run) {
	// 初始化每个节点的状态
	run.mu.Lock()
	for id, node := range d.Nodes {
		run.Nodes[id] = &NodeResult{ID: id, Name: node.Name, Status: StatusPending}
	}
	run.mu.Unlock()

	var (
		mu          sync.Mutex
		started     = make(map[string]bool, len(d.Nodes))
		done        = make(map[string]NodeStatus, len(d.Nodes))
		outputs     = make(map[string]string, len(d.Nodes)) // 各节点 stdout，用于传递给下游
		completions = make(chan completion, len(d.Nodes))
		sem         chan struct{}
	)
	if e.maxParallel > 0 {
		sem = make(chan struct{}, e.maxParallel)
	}

	// launchTask 异步执行一个节点，extraEnv 为上游输出注入的环境变量
	launchTask := func(id string, extraEnv []string) {
		node := d.Nodes[id]
		go func() {
			if sem != nil {
				sem <- struct{}{}
				defer func() { <-sem }()
			}
			now := time.Now()
			run.update(id, func(nr *NodeResult) {
				nr.Status = StatusRunning
				nr.StartedAt = &now
			})

			res := executor.Execute(ctx, node, extraEnv)

			end := time.Now()
			status := StatusSuccess
			if res.Err != nil || res.ExitCode != 0 {
				status = StatusFailed
			}
			run.update(id, func(nr *NodeResult) {
				nr.Status = status
				nr.ExitCode = res.ExitCode
				nr.Stdout = res.Stdout
				nr.Stderr = res.Stderr
				if res.Err != nil {
					nr.Error = res.Err.Error()
				}
				nr.EndedAt = &end
			})
			completions <- completion{id: id, status: status, stdout: res.Stdout}
		}()
	}

	// trySchedule 扫描所有未启动节点，将依赖已满足者调度执行或跳过
	trySchedule := func() {
		mu.Lock()
		defer mu.Unlock()
		for _, id := range d.Order {
			if started[id] {
				continue
			}
			node := d.Nodes[id]
			ready := true
			anyBad := false
			for _, dep := range node.Deps {
				st, ok := done[dep]
				if !ok {
					ready = false
					break
				}
				if st != StatusSuccess {
					anyBad = true
				}
			}
			if !ready {
				continue
			}
			started[id] = true
			// 上游存在失败/跳过，或节点被禁用 -> 跳过
			if anyBad || !node.IsEnabled() {
				run.update(id, func(nr *NodeResult) { nr.Status = StatusSkipped })
				// 缓冲区足够，不会阻塞
				completions <- completion{id: id, status: StatusSkipped}
			} else {
				// 收集上游节点的输出，注入为环境变量供下游读取
				var extraEnv []string
				var combined strings.Builder
				for _, dep := range node.Deps {
					out := outputs[dep]
					extraEnv = append(extraEnv, "DAG_DEP_"+envKey(dep)+"_OUTPUT="+out)
					combined.WriteString(out)
				}
				extraEnv = append(extraEnv, "DAG_UPSTREAM_OUTPUT="+combined.String())
				launchTask(id, extraEnv)
			}
		}
	}

	// 初始调度
	trySchedule()

	// 主循环：每完成一个节点，记录状态并尝试调度后继
	finished := 0
	total := len(d.Nodes)
	for finished < total {
		c := <-completions
		mu.Lock()
		done[c.id] = c.status
		outputs[c.id] = c.stdout
		mu.Unlock()
		finished++
		trySchedule()
	}

	end := time.Now()
	run.mu.Lock()
	run.Finished = true
	run.EndedAt = &end
	run.mu.Unlock()
}

// 以下为 Run 的并发安全辅助方法

func (r *Run) update(id string, fn func(*NodeResult)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if nr, ok := r.Nodes[id]; ok {
		fn(nr)
	}
}
