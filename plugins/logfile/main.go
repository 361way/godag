// logfile 插件：将每次运行记录以 JSON Lines 形式追加到本地日志文件。
//
// 每个流水线对应一个文件 <dir>/<pipelineID>.log，每行一条 RunRecord 的 JSON。
// 纯标准库实现，无第三方依赖，可直接 go build -buildmode=plugin 编译。
//
// 配置项（-sink-config）：
//
//	dir=运行日志目录（默认 run-logs）
//
// 编译：
//
//	go build -buildmode=plugin -o plugins/build/logfile.so ./plugins/logfile
package main

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"

	"dag-app/internal/sink"
)

type logfileSink struct {
	mu  sync.Mutex
	dir string
}

func (s *logfileSink) Name() string { return "logfile" }

func (s *logfileSink) Open(config map[string]string) error {
	s.dir = config["dir"]
	if s.dir == "" {
		s.dir = "run-logs"
	}
	return os.MkdirAll(s.dir, 0o755)
}

func (s *logfileSink) path(pipelineID string) string {
	// 清洗 pipelineID，避免路径穿越
	safe := filepath.Base(pipelineID)
	return filepath.Join(s.dir, safe+".log")
}

func (s *logfileSink) Save(rec *sink.RunRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, err := os.OpenFile(s.path(rec.PipelineID), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	_, err = f.Write(b)
	return err
}

func (s *logfileSink) List(pipelineID string, limit int) ([]*sink.RunRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, err := os.Open(s.path(pipelineID))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	var all []*sink.RunRecord
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024) // 支持较大单行
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec sink.RunRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			continue // 跳过损坏行
		}
		cp := rec
		all = append(all, &cp)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	// 倒序（新→旧）并截断
	out := make([]*sink.RunRecord, 0, len(all))
	for i := len(all) - 1; i >= 0; i-- {
		out = append(out, all[i])
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (s *logfileSink) Close() error { return nil }

// Sink 为插件导出符号，供主程序 plugin.Lookup("Sink") 获取。
var Sink sink.Sink = &logfileSink{}
