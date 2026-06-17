// duckdb 插件（薄壳）：将运行记录存入 DuckDB 分析型数据库。
//
// 重要设计说明（为什么是“薄壳 + 子进程”而非直接链接 duckdb）：
//
//	DuckDB 的 C++ 静态库无法在 Go plugin（buildmode=plugin / dlopen）中运行——
//	.so 能编译，但首次进入 duckdb C 接口（如 duckdb_open_ext）即触发 SIGABRT。
//	而把 duckdb 编进普通可执行文件则完全正常。因此本插件本身【不链接 duckdb】，
//	保持为纯 Go，可被主程序正常 dlopen；真正的 duckdb 读写交给独立的边车子进程
//	duckdb-helper（见 cmd/duckdb-helper），二者通过 stdin/stdout 上的 JSON 协议通信。
//
// 这样既满足“通过 .so 动态加载”的契约，又让 duckdb 真正可用。
//
// 配置项（-sink-config）：
//
//	path=数据库文件路径（默认 run-records.duckdb）
//	helper=duckdb-helper 可执行文件路径（默认依次尝试 ./plugins/build/duckdb-helper、
//	       环境变量 DUCKDB_HELPER、PATH 中的 duckdb-helper）
//
// 编译（见 build-plugins.sh）：
//
//	CGO_ENABLED=1 go build -o plugins/build/duckdb-helper ./cmd/duckdb-helper
//	go build -buildmode=plugin -o plugins/build/duckdb.so ./plugins/duckdb
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"

	"dag-app/internal/sink"
)

// request / response 必须与 cmd/duckdb-helper 中的定义保持 JSON 兼容。
type request struct {
	Op         string          `json:"op"`
	Path       string          `json:"path,omitempty"`
	Record     *sink.RunRecord `json:"record,omitempty"`
	PipelineID string          `json:"pipeline_id,omitempty"`
	Limit      int             `json:"limit,omitempty"`
}

type response struct {
	OK      bool              `json:"ok"`
	Error   string            `json:"error,omitempty"`
	Records []*sink.RunRecord `json:"records,omitempty"`
}

type duckdbSink struct {
	mu   sync.Mutex
	cmd  *exec.Cmd
	stin io.WriteCloser
	enc  *json.Encoder
	dec  *json.Decoder
}

func (s *duckdbSink) Name() string { return "duckdb" }

// resolveHelper 按优先级定位 duckdb-helper 可执行文件。
func resolveHelper(config map[string]string) (string, error) {
	candidates := []string{}
	if h := config["helper"]; h != "" {
		candidates = append(candidates, h)
	}
	if h := os.Getenv("DUCKDB_HELPER"); h != "" {
		candidates = append(candidates, h)
	}
	candidates = append(candidates, "plugins/build/duckdb-helper", "./duckdb-helper")
	for _, c := range candidates {
		if fi, err := os.Stat(c); err == nil && !fi.IsDir() {
			return c, nil
		}
	}
	// 退而求其次：从 PATH 查找。
	if p, err := exec.LookPath("duckdb-helper"); err == nil {
		return p, nil
	}
	return "", fmt.Errorf("找不到 duckdb-helper 可执行文件；请用 -sink-config helper=<path> 指定，或将其放到 plugins/build/ 或 PATH 中")
}

func (s *duckdbSink) Open(config map[string]string) error {
	helper, err := resolveHelper(config)
	if err != nil {
		return err
	}
	cmd := exec.Command(helper)
	cmd.Stderr = os.Stderr // 子进程/duckdb 的日志直通父进程 stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("启动 duckdb-helper 失败: %w", err)
	}
	s.cmd = cmd
	s.stin = stdin
	s.enc = json.NewEncoder(stdin)
	s.dec = json.NewDecoder(stdout)

	resp, err := s.callLocked(request{Op: "open", Path: config["path"]})
	if err != nil {
		s.terminate()
		return fmt.Errorf("初始化 duckdb 失败: %w", err)
	}
	_ = resp
	return nil
}

// callLocked 发送一次请求并读取一次响应；调用方需自行决定是否加锁。
// 此处内部加锁，保证 Save/List 并发安全。
func (s *duckdbSink) call(req request) (*response, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.callLocked(req)
}

func (s *duckdbSink) callLocked(req request) (*response, error) {
	if s.enc == nil || s.dec == nil {
		return nil, fmt.Errorf("duckdb-helper 未就绪")
	}
	if err := s.enc.Encode(req); err != nil {
		return nil, fmt.Errorf("写入 duckdb-helper 失败: %w", err)
	}
	var resp response
	if err := s.dec.Decode(&resp); err != nil {
		return nil, fmt.Errorf("读取 duckdb-helper 响应失败: %w", err)
	}
	if !resp.OK && resp.Error != "" {
		return &resp, fmt.Errorf("%s", resp.Error)
	}
	return &resp, nil
}

func (s *duckdbSink) Save(rec *sink.RunRecord) error {
	_, err := s.call(request{Op: "save", Record: rec})
	return err
}

func (s *duckdbSink) List(pipelineID string, limit int) ([]*sink.RunRecord, error) {
	resp, err := s.call(request{Op: "list", PipelineID: pipelineID, Limit: limit})
	if err != nil {
		return nil, err
	}
	return resp.Records, nil
}

func (s *duckdbSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.enc != nil {
		// 通知子进程优雅关闭（忽略错误，随后强制收尾）。
		_, _ = s.callLocked(request{Op: "close"})
	}
	return s.terminate()
}

// terminate 关闭管道并等待子进程退出。
func (s *duckdbSink) terminate() error {
	if s.stin != nil {
		s.stin.Close()
		s.stin = nil
	}
	s.enc = nil
	s.dec = nil
	if s.cmd != nil {
		err := s.cmd.Wait()
		s.cmd = nil
		return err
	}
	return nil
}

// Sink 为插件导出符号。
var Sink sink.Sink = &duckdbSink{}
