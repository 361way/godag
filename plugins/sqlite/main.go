// sqlite 插件：将运行记录存入 SQLite 数据库。
//
// 使用纯 Go 驱动 modernc.org/sqlite（无需 CGO 与系统 sqlite 库），
// 与 plugin 模式兼容性更好。运行记录的完整内容以 JSON 存于 data 列，
// 同时冗余若干列便于按流水线/时间查询。
//
// 配置项（-sink-config）：
//
//	path=数据库文件路径（默认 run-records.sqlite）
//
// 依赖：modernc.org/sqlite（纯 Go）
// 编译：
//
//	go get modernc.org/sqlite
//	go build -buildmode=plugin -o plugins/build/sqlite.so ./plugins/sqlite
package main

import (
	"database/sql"
	"encoding/json"
	"time"

	"dag-app/internal/sink"

	_ "modernc.org/sqlite"
)

type sqliteSink struct {
	db *sql.DB
}

func (s *sqliteSink) Name() string { return "sqlite" }

func (s *sqliteSink) Open(config map[string]string) error {
	path := config["path"]
	if path == "" {
		path = "run-records.sqlite"
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return err
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS runs (
		pipeline_id TEXT NOT NULL,
		run_id      TEXT NOT NULL,
		started_at  TEXT,
		finished    INTEGER,
		data        TEXT NOT NULL,
		PRIMARY KEY (pipeline_id, run_id)
	)`); err != nil {
		db.Close()
		return err
	}
	_, _ = db.Exec(`CREATE INDEX IF NOT EXISTS idx_runs_pipeline_started ON runs(pipeline_id, started_at DESC)`)
	s.db = db
	return nil
}

func (s *sqliteSink) Save(rec *sink.RunRecord) error {
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	finished := 0
	if rec.Finished {
		finished = 1
	}
	// 使用参数绑定，避免 SQL 注入
	_, err = s.db.Exec(
		`INSERT OR REPLACE INTO runs(pipeline_id, run_id, started_at, finished, data) VALUES(?,?,?,?,?)`,
		rec.PipelineID, rec.RunID, rec.StartedAt.Format(time.RFC3339Nano), finished, string(b),
	)
	return err
}

func (s *sqliteSink) List(pipelineID string, limit int) ([]*sink.RunRecord, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.Query(
		`SELECT data FROM runs WHERE pipeline_id = ? ORDER BY started_at DESC LIMIT ?`,
		pipelineID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*sink.RunRecord
	for rows.Next() {
		var data string
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		var rec sink.RunRecord
		if err := json.Unmarshal([]byte(data), &rec); err != nil {
			continue
		}
		cp := rec
		out = append(out, &cp)
	}
	return out, rows.Err()
}

func (s *sqliteSink) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// Sink 为插件导出符号。
var Sink sink.Sink = &sqliteSink{}
