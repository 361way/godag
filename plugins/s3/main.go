// s3 插件：将运行记录以对象形式上传到 S3 兼容存储（AWS S3 / MinIO / 腾讯云 COS 等）。
//
// 对象 key 形如 <prefix>/<pipelineID>/<runID>.json。
//
// 配置项（-sink-config，均为非敏感项）：
//
//	endpoint=对象存储 endpoint（如 s3.amazonaws.com、cos.ap-guangzhou.myqcloud.com）
//	bucket=桶名（必填）
//	region=区域（可选）
//	prefix=对象前缀（可选，默认 dag-runs）
//	use_ssl=true|false（默认 true）
//
// 凭证（遵循 secrets env-only 安全原则，禁止经命令行/配置传入）：
//
//	环境变量 S3_ACCESS_KEY / S3_SECRET_KEY
//	或 AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
//
// 依赖：github.com/minio/minio-go/v7（纯 Go）
// 编译：
//
//	go get github.com/minio/minio-go/v7
//	go build -buildmode=plugin -o plugins/build/s3.so ./plugins/s3
package main

import (
	"context"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"dag-app/internal/sink"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type s3Sink struct {
	client *minio.Client
	bucket string
	prefix string
}

func (s *s3Sink) Name() string { return "s3" }

func envCreds() (string, string) {
	ak := os.Getenv("S3_ACCESS_KEY")
	sk := os.Getenv("S3_SECRET_KEY")
	if ak == "" {
		ak = os.Getenv("AWS_ACCESS_KEY_ID")
	}
	if sk == "" {
		sk = os.Getenv("AWS_SECRET_ACCESS_KEY")
	}
	return ak, sk
}

func (s *s3Sink) Open(config map[string]string) error {
	endpoint := config["endpoint"]
	s.bucket = config["bucket"]
	if endpoint == "" || s.bucket == "" {
		return fmt.Errorf("s3 插件需要配置 endpoint 与 bucket")
	}
	s.prefix = config["prefix"]
	if s.prefix == "" {
		s.prefix = "dag-runs"
	}
	useSSL := config["use_ssl"] != "false"

	ak, sk := envCreds()
	if ak == "" || sk == "" {
		return fmt.Errorf("缺少 S3 凭证：请设置环境变量 S3_ACCESS_KEY/S3_SECRET_KEY（或 AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY）")
	}

	cli, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(ak, sk, ""),
		Secure: useSSL,
		Region: config["region"],
	})
	if err != nil {
		return err
	}
	s.client = cli
	return nil
}

func (s *s3Sink) objectKey(pipelineID, runID string) string {
	return fmt.Sprintf("%s/%s/%s.json", s.prefix, pipelineID, runID)
}

func (s *s3Sink) Save(rec *sink.RunRecord) error {
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err = s.client.PutObject(ctx, s.bucket, s.objectKey(rec.PipelineID, rec.RunID),
		bytes.NewReader(b), int64(len(b)), minio.PutObjectOptions{ContentType: "application/json"})
	return err
}

func (s *s3Sink) List(pipelineID string, limit int) ([]*sink.RunRecord, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("%s/%s/", s.prefix, pipelineID)

	var keys []string
	for obj := range s.client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		if strings.HasSuffix(obj.Key, ".json") {
			keys = append(keys, obj.Key)
		}
	}
	// key 含 runID（时间序），按 key 倒序得到新→旧
	sort.Sort(sort.Reverse(sort.StringSlice(keys)))
	if limit > 0 && len(keys) > limit {
		keys = keys[:limit]
	}

	var out []*sink.RunRecord
	for _, key := range keys {
		o, err := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
		if err != nil {
			continue
		}
		var buf bytes.Buffer
		if _, err := buf.ReadFrom(o); err != nil {
			o.Close()
			continue
		}
		o.Close()
		var rec sink.RunRecord
		if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
			continue
		}
		cp := rec
		out = append(out, &cp)
	}
	return out, nil
}

func (s *s3Sink) Close() error { return nil }

// Sink 为插件导出符号。
var Sink sink.Sink = &s3Sink{}
