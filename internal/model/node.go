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

	// Python 解释器路径（可选）。多 venv 环境时指定完整路径，如 /path/to/venv/bin/python3
	// 留空则自动查找 python3 → python
	PythonBin string `json:"python_bin" yaml:"python_bin,omitempty"`

	// 画布坐标（用于 N8N 风格可视化编辑器，持久化节点布局）
	X float64 `json:"x" yaml:"x,omitempty"`
	Y float64 `json:"y" yaml:"y,omitempty"`
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

// ScheduleType 调度类型
type ScheduleType string

const (
	// ScheduleOnce 一次性任务，在指定时间执行一次
	ScheduleOnce ScheduleType = "once"
	// ScheduleCron 周期性任务，按 crontab 表达式重复执行
	ScheduleCron ScheduleType = "cron"
)

// Schedule 流水线的计划任务配置
type Schedule struct {
	// 是否启用调度
	Enabled bool `json:"enabled" yaml:"enabled"`
	// 调度类型：once（一次性）/ cron（周期性）
	Type ScheduleType `json:"type" yaml:"type"`
	// 周期性：5 字段 crontab 表达式（分 时 日 月 周）
	Cron string `json:"cron" yaml:"cron,omitempty"`
	// 一次性：执行时间，格式 "2006-01-02 15:04"
	At string `json:"at" yaml:"at,omitempty"`
	// 一次性任务是否已触发（内部状态，触发后置 true 不再执行）
	Fired bool `json:"fired" yaml:"fired,omitempty"`
}

// DAGConfig DAG 定义，可由 YAML 或 JSON 反序列化得到
type DAGConfig struct {
	// 流水线唯一标识（多流水线场景下使用，同时作为持久化文件名）
	ID string `json:"id" yaml:"id,omitempty"`
	// DAG 名称
	Name string `json:"name" yaml:"name"`
	// 描述
	Description string `json:"description" yaml:"description"`
	// 计划任务配置（可选）
	Schedule *Schedule `json:"schedule" yaml:"schedule,omitempty"`
	// 任务节点列表
	Nodes []*Node `json:"nodes" yaml:"nodes"`
}
