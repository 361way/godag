// bbolt 插件：将运行记录存入 go.etcd.io/bbolt 嵌入式 KV 数据库。
//
// 每个流水线一个 bucket，key = RunID（按时间格式天然有序），value = RunRecord 的 JSON。
//
// 配置项（-sink-config）：
//
//	path=数据库文件路径（默认 run-records.bolt）
//
// 依赖：go.etcd.io/bbolt（纯 Go）
// 编译：
//
//	go get go.etcd.io/bbolt
//	go build -buildmode=plugin -o plugins/build/bbolt.so ./plugins/bbolt
package main

import (
	"encoding/json"
	"time"

	"dag-app/internal/sink"

	bolt "go.etcd.io/bbolt"
)

type bboltSink struct {
	db *bolt.DB
}

func (s *bboltSink) Name() string { return "bbolt" }

func (s *bboltSink) Open(config map[string]string) error {
	path := config["path"]
	if path == "" {
		path = "run-records.bolt"
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 3 * time.Second})
	if err != nil {
		return err
	}
	s.db = db
	return nil
}

func (s *bboltSink) Save(rec *sink.RunRecord) error {
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		bkt, err := tx.CreateBucketIfNotExists([]byte(rec.PipelineID))
		if err != nil {
			return err
		}
		return bkt.Put([]byte(rec.RunID), b)
	})
}

func (s *bboltSink) List(pipelineID string, limit int) ([]*sink.RunRecord, error) {
	var out []*sink.RunRecord
	err := s.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte(pipelineID))
		if bkt == nil {
			return nil
		}
		c := bkt.Cursor()
		// RunID 形如 20060102-150405.000，字典序即时间序，倒序遍历得到新→旧
		for k, v := c.Last(); k != nil; k, v = c.Prev() {
			var rec sink.RunRecord
			if err := json.Unmarshal(v, &rec); err != nil {
				continue
			}
			cp := rec
			out = append(out, &cp)
			if limit > 0 && len(out) >= limit {
				break
			}
		}
		return nil
	})
	return out, err
}

func (s *bboltSink) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// Sink 为插件导出符号。
var Sink sink.Sink = &bboltSink{}
