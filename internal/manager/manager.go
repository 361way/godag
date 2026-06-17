package manager

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"dag-app/internal/config"
	"dag-app/internal/dag"
	"dag-app/internal/engine"
	"dag-app/internal/model"
	"dag-app/internal/sink"
)

// Manager 统筹 DAG、节点启停状态与执行历史
type Manager struct {
	mu          sync.RWMutex
	cfg         *model.DAGConfig
	dag         *dag.DAG
	maxParallel int
	configPath  string // 配置文件路径，用于持久化；为空时不写盘

	runs      []*engine.Run      // 执行历史（最新在前）
	running   bool               // 是否有正在进行的执行
	cancelRun context.CancelFunc // 取消当前正在执行的 run（用于手动停止）
	sink      sink.Sink          // 运行记录持久化后端（可选，nil 表示仅内存）
}

// New 基于配置创建 Manager。configPath 用于将界面修改持久化回配置文件，可为空。
func New(cfg *model.DAGConfig, maxParallel int, configPath string) (*Manager, error) {
	d, err := dag.Build(cfg)
	if err != nil {
		return nil, err
	}
	return &Manager{
		cfg:         cfg,
		dag:         d,
		maxParallel: maxParallel,
		configPath:  configPath,
		runs:        make([]*engine.Run, 0),
	}, nil
}

// Info DAG 概要信息
type Info struct {
	ID          string          `json:"id"`
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Schedule    *model.Schedule `json:"schedule"`
	Nodes       []*NodeView     `json:"nodes"`
	Running     bool            `json:"running"`
	TopoOrder   []string        `json:"topo_order"`
}

// NodeView 提供给前端的节点视图（包含全部可编辑字段）
type NodeView struct {
	ID         string         `json:"id"`
	Name       string         `json:"name"`
	Type       model.TaskType `json:"type"`
	Deps       []string       `json:"deps"`
	Enabled    bool           `json:"enabled"`
	Command    string         `json:"command"`
	Script     string         `json:"script"`
	Args       []string       `json:"args"`
	WorkDir    string         `json:"workdir"`
	Env        []string       `json:"env"`
	TimeoutSec int            `json:"timeout_sec"`
	PythonBin  string         `json:"python_bin"`
	X          float64        `json:"x"`
	Y          float64        `json:"y"`
}

// GetInfo 返回当前 DAG 信息
func (m *Manager) GetInfo() *Info {
	m.mu.RLock()
	defer m.mu.RUnlock()

	views := make([]*NodeView, 0, len(m.cfg.Nodes))
	for _, n := range m.cfg.Nodes {
		views = append(views, &NodeView{
			ID:         n.ID,
			Name:       n.Name,
			Type:       n.Type,
			Deps:       n.Deps,
			Enabled:    n.IsEnabled(),
			Command:    n.Command,
			Script:     n.Script,
			Args:       n.Args,
			WorkDir:    n.WorkDir,
			Env:        n.Env,
			TimeoutSec: n.TimeoutSec,
		PythonBin:  n.PythonBin,
		X:          n.X,
		Y:          n.Y,
		})
	}
	topo, _ := m.dag.TopoOrder()

	return &Info{
		ID:          m.cfg.ID,
		Name:        m.cfg.Name,
		Description: m.cfg.Description,
		Schedule:    m.cfg.Schedule,
		Nodes:       views,
		Running:     m.running,
		TopoOrder:   topo,
	}
}

// ID 返回流水线唯一标识
func (m *Manager) ID() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.cfg.ID
}

// GetSchedule 返回当前调度配置的副本（可能为 nil）
func (m *Manager) GetSchedule() *model.Schedule {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.cfg.Schedule == nil {
		return nil
	}
	cp := *m.cfg.Schedule
	return &cp
}

// IsRunning 返回是否有执行进行中
func (m *Manager) IsRunning() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.running
}

// UpdateSchedule 更新调度配置并持久化。type 变更或重新启用时会重置一次性任务的 Fired 标记。
func (m *Manager) UpdateSchedule(sch *model.Schedule) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cfg.Schedule = sch
	return m.persistLocked()
}

// MarkScheduleFired 标记一次性调度已触发并持久化，避免重复执行
func (m *Manager) MarkScheduleFired() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cfg.Schedule == nil {
		return nil
	}
	m.cfg.Schedule.Fired = true
	return m.persistLocked()
}

// SetNodeEnabled 启用/禁用指定节点
func (m *Manager) SetNodeEnabled(id string, enabled bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	node, ok := m.dag.Nodes[id]
	if !ok {
		return fmt.Errorf("节点不存在: %s", id)
	}
	node.SetEnabled(enabled)
	// 同步到配置并持久化
	for _, n := range m.cfg.Nodes {
		if n.ID == id {
			n.SetEnabled(enabled)
			break
		}
	}
	return m.persistLocked()
}

// SetNodePosition 更新节点在画布上的坐标并持久化（不触发 DAG 重建，供拖拽使用）
func (m *Manager) SetNodePosition(id string, x, y float64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	found := false
	for _, n := range m.cfg.Nodes {
		if n.ID == id {
			n.X, n.Y = x, y
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("节点不存在: %s", id)
	}
	if node, ok := m.dag.Nodes[id]; ok {
		node.X, node.Y = x, y
	}
	return m.persistLocked()
}

// SaveNode 新增或更新一个节点（按 ID 区分），校验通过后重建 DAG 并持久化
func (m *Manager) SaveNode(n *model.Node) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.running {
		return fmt.Errorf("执行进行中，无法修改任务")
	}
	if n == nil || n.ID == "" {
		return fmt.Errorf("节点 id 不能为空")
	}
	candidate := make([]*model.Node, 0, len(m.cfg.Nodes)+1)
	found := false
	for _, existing := range m.cfg.Nodes {
		if existing.ID == n.ID {
			candidate = append(candidate, n)
			found = true
		} else {
			candidate = append(candidate, existing)
		}
	}
	if !found {
		candidate = append(candidate, n)
	}
	return m.commitNodesLocked(candidate)
}

// DeleteNode 删除指定节点；若有其它节点依赖它，重建时会校验失败并回滚
func (m *Manager) DeleteNode(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.running {
		return fmt.Errorf("执行进行中，无法修改任务")
	}
	candidate := make([]*model.Node, 0, len(m.cfg.Nodes))
	found := false
	for _, existing := range m.cfg.Nodes {
		if existing.ID == id {
			found = true
			continue
		}
		candidate = append(candidate, existing)
	}
	if !found {
		return fmt.Errorf("节点不存在: %s", id)
	}
	return m.commitNodesLocked(candidate)
}

// UpdateMeta 更新 DAG 名称与描述并持久化
func (m *Manager) UpdateMeta(name, description string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.running {
		return fmt.Errorf("执行进行中，无法修改")
	}
	m.cfg.Name = name
	m.cfg.Description = description
	m.dag.Name = name
	m.dag.Description = description
	return m.persistLocked()
}

// commitNodesLocked 用候选节点列表重建并校验 DAG，成功后替换内存状态并持久化。
// 调用方需持有写锁。
func (m *Manager) commitNodesLocked(candidate []*model.Node) error {
	newCfg := &model.DAGConfig{
		ID:          m.cfg.ID,
		Name:        m.cfg.Name,
		Description: m.cfg.Description,
		Schedule:    m.cfg.Schedule,
		Nodes:       candidate,
	}
	d, err := dag.Build(newCfg)
	if err != nil {
		return err
	}
	m.cfg = newCfg
	m.dag = d
	return m.persistLocked()
}

// persistLocked 将当前配置写回文件。configPath 为空时跳过。调用方需持有写锁。
func (m *Manager) persistLocked() error {
	if m.configPath == "" {
		return nil
	}
	if err := config.Save(m.configPath, m.cfg); err != nil {
		return fmt.Errorf("持久化配置失败: %w", err)
	}
	return nil
}

// TriggerRun 触发一次新的执行；若已有执行在进行中则返回错误。
// ctx 为外部传入的上下文，内部会用 WithCancel 派生一个可取消的子上下文。
func (m *Manager) TriggerRun(ctx context.Context) (*engine.Run, error) {
	m.mu.Lock()
	if m.running {
		m.mu.Unlock()
		return nil, fmt.Errorf("已有执行正在进行中，请稍后再试")
	}
	m.running = true
	runCtx, cancel := context.WithCancel(context.Background())
	m.cancelRun = cancel // 存储取消函数，供 ForceStop 使用

	run := &engine.Run{
		ID:        time.Now().Format("20060102-150405.000"),
		StartedAt: time.Now(),
		Nodes:     make(map[string]*engine.NodeResult),
	}
	m.runs = append([]*engine.Run{run}, m.runs...)
	// 限制历史长度
	if len(m.runs) > 50 {
		m.runs = m.runs[:50]
	}
	d := m.dag
	eng := engine.New(m.maxParallel)
	m.mu.Unlock()

	go func() {
		eng.Execute(runCtx, d, run)
		cancel() // 释放 cancel 资源
		m.mu.Lock()
		m.running = false
		m.cancelRun = nil
		sk := m.sink
		pid := m.cfg.ID
		m.mu.Unlock()
		// 运行结束后持久化到外部后端（若已配置插件）
		if sk != nil {
			if err := sk.Save(runToRecord(pid, run)); err != nil {
				log.Printf("持久化运行记录失败 [%s/%s]: %v", pid, run.ID, err)
			}
		}
	}()

	return run, nil
}

// SetSink 设置运行记录持久化后端（nil 表示仅内存）。
func (m *Manager) SetSink(s sink.Sink) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sink = s
}

// LoadHistory 从持久化后端加载最近 limit 条历史记录到内存，
// 使服务重启后历史仍可见。未配置后端或后端不支持查询时为空操作。
func (m *Manager) LoadHistory(limit int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sink == nil {
		return nil
	}
	recs, err := m.sink.List(m.cfg.ID, limit)
	if err != nil {
		return err
	}
	runs := make([]*engine.Run, 0, len(recs))
	for _, rec := range recs {
		runs = append(runs, recordToRun(rec))
	}
	m.runs = runs
	return nil
}

// runToRecord 将内存 Run 转换为可持久化的 RunRecord。
func runToRecord(pipelineID string, r *engine.Run) *sink.RunRecord {
	snap := r.Snapshot()
	rec := &sink.RunRecord{
		PipelineID: pipelineID,
		RunID:      snap.ID,
		StartedAt:  snap.StartedAt,
		Finished:   snap.Finished,
	}
	if snap.EndedAt != nil {
		rec.EndedAt = *snap.EndedAt
	}
	for _, nr := range snap.Nodes {
		n := sink.NodeRecord{
			ID:       nr.ID,
			Name:     nr.Name,
			Status:   string(nr.Status),
			ExitCode: nr.ExitCode,
			Stdout:   nr.Stdout,
			Stderr:   nr.Stderr,
			Error:    nr.Error,
		}
		if nr.StartedAt != nil {
			n.Started = *nr.StartedAt
		}
		if nr.EndedAt != nil {
			n.Ended = *nr.EndedAt
		}
		rec.Nodes = append(rec.Nodes, n)
	}
	return rec
}

// recordToRun 将持久化记录还原为内存 Run（用于历史恢复）。
func recordToRun(rec *sink.RunRecord) *engine.Run {
	run := &engine.Run{
		ID:        rec.RunID,
		StartedAt: rec.StartedAt,
		Finished:  rec.Finished,
		Nodes:     make(map[string]*engine.NodeResult, len(rec.Nodes)),
	}
	if !rec.EndedAt.IsZero() {
		e := rec.EndedAt
		run.EndedAt = &e
	}
	for i := range rec.Nodes {
		nr := rec.Nodes[i]
		res := &engine.NodeResult{
			ID:       nr.ID,
			Name:     nr.Name,
			Status:   engine.NodeStatus(nr.Status),
			ExitCode: nr.ExitCode,
			Stdout:   nr.Stdout,
			Stderr:   nr.Stderr,
			Error:    nr.Error,
		}
		if !nr.Started.IsZero() {
			s := nr.Started
			res.StartedAt = &s
		}
		if !nr.Ended.IsZero() {
			e := nr.Ended
			res.EndedAt = &e
		}
		run.Nodes[nr.ID] = res
	}
	return run
}

// ForceStop 强制停止当前正在执行的任务（取消上下文）。
// 无任务执行时返回错误。
func (m *Manager) ForceStop() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.running || m.cancelRun == nil {
		return fmt.Errorf("当前没有正在执行的任务")
	}
	m.cancelRun()
	return nil
}

// GetRun 根据 ID 获取执行记录快照
func (m *Manager) GetRun(id string) (*engine.Run, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, r := range m.runs {
		if r.ID == id {
			return r.Snapshot(), nil
		}
	}
	return nil, fmt.Errorf("执行记录不存在: %s", id)
}

// LatestRun 返回最近一次执行快照
func (m *Manager) LatestRun() *engine.Run {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.runs) == 0 {
		return nil
	}
	return m.runs[0].Snapshot()
}

// ListRuns 返回执行历史摘要（按时间倒序）
func (m *Manager) ListRuns() []map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]map[string]interface{}, 0, len(m.runs))
	for _, r := range m.runs {
		snap := r.Snapshot()
		result = append(result, map[string]interface{}{
			"id":         snap.ID,
			"started_at": snap.StartedAt,
			"ended_at":   snap.EndedAt,
			"finished":   snap.Finished,
		})
	}
	sort.SliceStable(result, func(i, j int) bool {
		return result[i]["started_at"].(time.Time).After(result[j]["started_at"].(time.Time))
	})
	return result
}
