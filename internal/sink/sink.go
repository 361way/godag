// Package sink 定义运行记录的持久化后端（Sink）接口及数据模型。
//
// 该包同时被主程序与各插件（plugins/*）导入，作为二者之间的契约。
// Go plugin 要求主程序与 .so 插件共享同一接口包，因此该包必须保持稳定、
// 且主程序与插件须用相同的 Go 版本与依赖版本编译。
//
// 设计动机：DuckDB / bbolt / SQLite / 本地日志文件 / S3 在本系统中本质上
// 都是“把一次 DAG 执行记录写到某处”的存储后端，故统一抽象为 Sink。
package sink

import "time"

// NodeRecord 单个节点的执行结果（持久化用，与 engine.NodeResult 解耦，
// 避免插件反向依赖 engine 包）。
type NodeRecord struct {
	ID       string    `json:"id"`
	Name     string    `json:"name"`
	Status   string    `json:"status"`
	ExitCode int       `json:"exit_code"`
	Stdout   string    `json:"stdout"`
	Stderr   string    `json:"stderr"`
	Error    string    `json:"error"`
	Started  time.Time `json:"started_at"`
	Ended    time.Time `json:"ended_at"`
}

// RunRecord 一次完整 DAG 执行的记录。
type RunRecord struct {
	PipelineID string       `json:"pipeline_id"`
	RunID      string       `json:"run_id"`
	StartedAt  time.Time    `json:"started_at"`
	EndedAt    time.Time    `json:"ended_at"`
	Finished   bool         `json:"finished"`
	Nodes      []NodeRecord `json:"nodes"`
}

// Sink 运行记录持久化后端。所有实现需保证 Save/List 的并发安全。
type Sink interface {
	// Name 返回后端名称（如 duckdb / bbolt / sqlite / logfile / s3）。
	Name() string
	// Open 用配置初始化后端。config 仅包含非敏感项（路径、bucket、endpoint 等）；
	// 凭证类信息（如 AccessKey/SecretKey）应由插件自行从环境变量读取，不经此传入。
	Open(config map[string]string) error
	// Save 持久化一次执行记录（在一次运行结束时调用）。
	Save(rec *RunRecord) error
	// List 返回指定流水线最近 limit 条执行记录，按开始时间倒序（新→旧）。
	// 用于服务重启后恢复历史展示。不支持查询的后端可返回 (nil, nil)。
	List(pipelineID string, limit int) ([]*RunRecord, error)
	// Close 释放底层资源（连接、文件句柄等）。
	Close() error
}
