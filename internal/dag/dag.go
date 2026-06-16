package dag

import (
	"fmt"

	"dag-app/internal/model"
)

// DAG 表示一个有向无环图
type DAG struct {
	Name        string
	Description string
	// 节点映射 id -> node
	Nodes map[string]*model.Node
	// 邻接表：node id -> 其下游节点 id 列表
	Children map[string][]string
	// 入度：node id -> 依赖数量
	InDegree map[string]int
	// 保持原始顺序的节点 id 列表
	Order []string
}

// Build 根据配置构建并校验 DAG
func Build(cfg *model.DAGConfig) (*DAG, error) {
	if cfg == nil {
		return nil, fmt.Errorf("配置为空")
	}
	if len(cfg.Nodes) == 0 {
		return nil, fmt.Errorf("DAG 至少需要一个节点")
	}

	d := &DAG{
		Name:        cfg.Name,
		Description: cfg.Description,
		Nodes:       make(map[string]*model.Node),
		Children:    make(map[string][]string),
		InDegree:    make(map[string]int),
		Order:       make([]string, 0, len(cfg.Nodes)),
	}

	// 注册节点，检查重复 ID
	for _, n := range cfg.Nodes {
		if n.ID == "" {
			return nil, fmt.Errorf("存在缺少 id 的节点")
		}
		if _, exists := d.Nodes[n.ID]; exists {
			return nil, fmt.Errorf("节点 id 重复: %s", n.ID)
		}
		if n.Type != model.TaskShell && n.Type != model.TaskGolang && n.Type != model.TaskPython {
			return nil, fmt.Errorf("节点 %s 的类型非法: %s", n.ID, n.Type)
		}
		d.Nodes[n.ID] = n
		d.InDegree[n.ID] = 0
		d.Order = append(d.Order, n.ID)
	}

	// 构建依赖关系
	for _, n := range cfg.Nodes {
		for _, dep := range n.Deps {
			if dep == n.ID {
				return nil, fmt.Errorf("节点 %s 不能依赖自身", n.ID)
			}
			if _, ok := d.Nodes[dep]; !ok {
				return nil, fmt.Errorf("节点 %s 依赖的节点不存在: %s", n.ID, dep)
			}
			d.Children[dep] = append(d.Children[dep], n.ID)
			d.InDegree[n.ID]++
		}
	}

	// 环检测
	if err := d.detectCycle(); err != nil {
		return nil, err
	}

	return d, nil
}

// detectCycle 使用 Kahn 算法检测环
func (d *DAG) detectCycle() error {
	inDeg := make(map[string]int, len(d.InDegree))
	for k, v := range d.InDegree {
		inDeg[k] = v
	}

	queue := make([]string, 0)
	for id, deg := range inDeg {
		if deg == 0 {
			queue = append(queue, id)
		}
	}

	visited := 0
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		visited++
		for _, child := range d.Children[cur] {
			inDeg[child]--
			if inDeg[child] == 0 {
				queue = append(queue, child)
			}
		}
	}

	if visited != len(d.Nodes) {
		return fmt.Errorf("DAG 中存在环，无法构成有向无环图")
	}
	return nil
}

// TopoOrder 返回一个拓扑排序结果（仅用于展示/调试）
func (d *DAG) TopoOrder() ([]string, error) {
	inDeg := make(map[string]int, len(d.InDegree))
	for k, v := range d.InDegree {
		inDeg[k] = v
	}

	// 为了结果稳定，按原始顺序选取入度为 0 的节点
	result := make([]string, 0, len(d.Nodes))
	remaining := make(map[string]bool, len(d.Nodes))
	for _, id := range d.Order {
		remaining[id] = true
	}

	for len(result) < len(d.Nodes) {
		progressed := false
		for _, id := range d.Order {
			if remaining[id] && inDeg[id] == 0 {
				result = append(result, id)
				remaining[id] = false
				progressed = true
				for _, child := range d.Children[id] {
					inDeg[child]--
				}
			}
		}
		if !progressed {
			return nil, fmt.Errorf("拓扑排序失败：可能存在环")
		}
	}
	return result, nil
}
