package model

// TaskType 任务类型
type TaskType string

const (
	TaskShell  TaskType = "shell"  // 执行 shell 命令
	TaskGolang TaskType = "golang" // 执行 go run
	TaskPython TaskType = "python" // 执行 python 脚本
)

// Node 表示 DAG 中的一个任务节点
type Node struct {
	// 节点唯一标识
	ID string `json:"id" yaml:"id"`
	// 节点显示名称
	Name string `json:"name" yaml:"name"`
	// 任务类型: shell / golang / python
	Type TaskType `json:"type" yaml:"type"`
	// 依赖的上游节点 ID 列表
	Deps []string `json:"deps" yaml:"deps"`
	// 是否启用，默认为 true。可通过参数/接口动态开关
	Enabled *bool `json:"enabled" yaml:"enabled"`

	// 执行内容（三选一，依据 Type 决定如何使用）
	// shell:  直接执行的命令字符串
	// golang: 需要 go run 的 .go 文件路径或目录
	// python: 需要执行的 .py 文件路径
	Command string `json:"command" yaml:"command"`
	// 内联脚本内容（可选，优先级低于 Command）。
	// 当提供 Script 时，会写入临时文件后执行。
	Script string `json:"script" yaml:"script"`

	// 执行参数
	Args []string `json:"args" yaml:"args"`
	// 工作目录
	WorkDir string `json:"workdir" yaml:"workdir"`
	// 额外环境变量 KEY=VALUE
	Env []string `json:"env" yaml:"env"`
	// 超时时间（秒），0 表示不超时
	TimeoutSec int `json:"timeout_sec" yaml:"timeout_sec"`
}

// IsEnabled 返回节点是否启用（默认启用）
func (n *Node) IsEnabled() bool {
	if n.Enabled == nil {
		return true
	}
	return *n.Enabled
}

// SetEnabled 设置启用状态
func (n *Node) SetEnabled(v bool) {
	n.Enabled = &v
}

// DAGConfig DAG 定义，可由 YAML 或 JSON 反序列化得到
type DAGConfig struct {
	// DAG 名称
	Name string `json:"name" yaml:"name"`
	// 描述
	Description string `json:"description" yaml:"description"`
	// 任务节点列表
	Nodes []*Node `json:"nodes" yaml:"nodes"`
}
