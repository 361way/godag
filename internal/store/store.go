// Package store 管理多个流水线（Pipeline），每个流水线对应目录下的一个配置文件，
// 并各自由一个 manager.Manager 负责运行时状态、执行历史与持久化。
package store

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	"dag-app/internal/config"
	"dag-app/internal/manager"
	"dag-app/internal/model"
	"dag-app/internal/sink"
)

var idPattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// Store 流水线集合
type Store struct {
	mu          sync.RWMutex
	dir         string
	maxParallel int
	mgrs        map[string]*manager.Manager
	paths       map[string]string // id -> 配置文件路径
	order       []string          // 保持流水线展示顺序
	sink        sink.Sink         // 运行记录持久化后端（可选）
}

// New 创建 Store 并从目录加载全部流水线配置文件
func New(dir string, maxParallel int) (*Store, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("创建流水线目录失败: %w", err)
	}
	s := &Store{
		dir:         dir,
		maxParallel: maxParallel,
		mgrs:        make(map[string]*manager.Manager),
		paths:       make(map[string]string),
		order:       make([]string, 0),
	}
	if err := s.loadDir(); err != nil {
		return nil, err
	}
	return s, nil
}

// loadDir 扫描目录加载所有 .yaml/.yml/.json 流水线文件
func (s *Store) loadDir() error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return fmt.Errorf("读取流水线目录失败: %w", err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		ext := strings.ToLower(filepath.Ext(e.Name()))
		if ext != ".yaml" && ext != ".yml" && ext != ".json" {
			continue
		}
		path := filepath.Join(s.dir, e.Name())
		if err := s.loadFile(path); err != nil {
			return fmt.Errorf("加载 %s 失败: %w", e.Name(), err)
		}
	}
	return nil
}

// loadFile 加载单个流水线文件
func (s *Store) loadFile(path string) error {
	cfg, err := config.Load(path)
	if err != nil {
		return err
	}
	// 缺少 ID 时以文件名（去扩展名）补齐并回写
	if cfg.ID == "" {
		base := filepath.Base(path)
		cfg.ID = strings.TrimSuffix(base, filepath.Ext(base))
		_ = config.Save(path, cfg)
	}
	if _, exists := s.mgrs[cfg.ID]; exists {
		return fmt.Errorf("流水线 ID 重复: %s", cfg.ID)
	}
	mgr, err := manager.New(cfg, s.maxParallel, path)
	if err != nil {
		return err
	}
	s.mgrs[cfg.ID] = mgr
	s.paths[cfg.ID] = path
	s.order = append(s.order, cfg.ID)
	return nil
}

// ImportConfig 将一个已有配置作为初始流水线导入到目录（用于兼容旧的单文件启动）。
// 仅当目录中尚无任何流水线时才执行。
func (s *Store) ImportConfig(cfg *model.DAGConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.order) > 0 {
		return nil
	}
	if cfg.ID == "" {
		if cfg.Name != "" && idPattern.MatchString(cfg.Name) {
			cfg.ID = cfg.Name
		} else {
			cfg.ID = "default"
		}
	}
	path := filepath.Join(s.dir, cfg.ID+".yaml")
	if err := config.Save(path, cfg); err != nil {
		return err
	}
	return s.loadFile(path)
}

// SetSink 为所有现有流水线设置持久化后端，并加载各自的历史记录。
// 后续通过 Create 新建的流水线也会自动继承该后端。
func (s *Store) SetSink(sk sink.Sink) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sink = sk
	for _, id := range s.order {
		m := s.mgrs[id]
		m.SetSink(sk)
		if err := m.LoadHistory(50); err != nil {
			log.Printf("加载持久化历史失败 [%s]: %v", id, err)
		}
	}
}

// SinkInfo 返回当前持久化后端名称及是否已启用持久化。
func (s *Store) SinkInfo() (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.sink == nil {
		return "", false
	}
	return s.sink.Name(), true
}

// Count 返回流水线数量
func (s *Store) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.order)
}

// List 返回所有流水线的概要信息（按展示顺序）
func (s *Store) List() []*manager.Info {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*manager.Info, 0, len(s.order))
	for _, id := range s.order {
		out = append(out, s.mgrs[id].GetInfo())
	}
	return out
}

// All 返回全部 Manager（供调度器遍历）
func (s *Store) All() []*manager.Manager {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*manager.Manager, 0, len(s.order))
	for _, id := range s.order {
		out = append(out, s.mgrs[id])
	}
	return out
}

// Get 按 ID 获取 Manager；id 为空时返回第一个流水线
func (s *Store) Get(id string) (*manager.Manager, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if id == "" {
		if len(s.order) == 0 {
			return nil, fmt.Errorf("当前没有任何流水线")
		}
		return s.mgrs[s.order[0]], nil
	}
	mgr, ok := s.mgrs[id]
	if !ok {
		return nil, fmt.Errorf("流水线不存在: %s", id)
	}
	return mgr, nil
}

// Create 新建一个流水线（含一个默认 shell 节点）
func (s *Store) Create(id, name, description string) (*manager.Manager, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id = strings.TrimSpace(id)
	if !idPattern.MatchString(id) {
		return nil, fmt.Errorf("流水线 ID 仅允许字母、数字、下划线和短横线")
	}
	if _, exists := s.mgrs[id]; exists {
		return nil, fmt.Errorf("流水线 ID 已存在: %s", id)
	}
	if name == "" {
		name = id
	}
	enabled := true
	cfg := &model.DAGConfig{
		ID:          id,
		Name:        name,
		Description: description,
		Nodes: []*model.Node{
			{ID: "start", Name: "开始", Type: model.TaskShell, Command: "echo hello", Enabled: &enabled, X: 80, Y: 60},
		},
	}
	path := filepath.Join(s.dir, id+".yaml")
	if err := config.Save(path, cfg); err != nil {
		return nil, err
	}
	mgr, err := manager.New(cfg, s.maxParallel, path)
	if err != nil {
		_ = os.Remove(path)
		return nil, err
	}
	if s.sink != nil {
		mgr.SetSink(s.sink)
	}
	s.mgrs[id] = mgr
	s.paths[id] = path
	s.order = append(s.order, id)
	return mgr, nil
}

// Delete 删除流水线及其配置文件
func (s *Store) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	mgr, ok := s.mgrs[id]
	if !ok {
		return fmt.Errorf("流水线不存在: %s", id)
	}
	if mgr.IsRunning() {
		return fmt.Errorf("流水线执行进行中，无法删除")
	}
	if path, ok := s.paths[id]; ok {
		_ = os.Remove(path)
	}
	delete(s.mgrs, id)
	delete(s.paths, id)
	for i, x := range s.order {
		if x == id {
			s.order = append(s.order[:i], s.order[i+1:]...)
			break
		}
	}
	return nil
}
