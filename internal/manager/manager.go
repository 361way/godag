package manager

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"dag-app/internal/config"
	"dag-app/internal/dag"
	"dag-app/internal/engine"
	"dag-app/internal/model"
)

// Manager 统筹 DAG、节点启停状态与执行历史
type Manager struct {
	mu          sync.RWMutex
	cfg         *model.DAGConfig
	dag         *dag.DAG
	maxParallel int
	configPath  string // 配置文件路径，用于持久化；为空时不写盘

	runs    []*engine.Run // 执行历史（最新在前）
	running bool          // 是否有正在进行的执行
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
	Name        string        `json:"name"`
	Description string        `json:"description"`
	Nodes       []*NodeView   `json:"nodes"`
	Running     bool          `json:"running"`
	TopoOrder   []string      `json:"topo_order"`
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
		})
	}
	topo, _ := m.dag.TopoOrder()

	return &Info{
		Name:        m.cfg.Name,
		Description: m.cfg.Description,
		Nodes:       views,
		Running:     m.running,
		TopoOrder:   topo,
	}
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
		Name:        m.cfg.Name,
		Description: m.cfg.Description,
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

// TriggerRun 触发一次新的执行；若已有执行在进行中则返回错误
func (m *Manager) TriggerRun(ctx context.Context) (*engine.Run, error) {
	m.mu.Lock()
	if m.running {
		m.mu.Unlock()
		return nil, fmt.Errorf("已有执行正在进行中，请稍后再试")
	}
	m.running = true
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
		eng.Execute(ctx, d, run)
		m.mu.Lock()
		m.running = false
		m.mu.Unlock()
	}()

	return run, nil
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
