package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"

	"dag-app/internal/model"
)

// Load 根据文件扩展名自动选择 YAML 或 JSON 解析
func Load(path string) (*model.DAGConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件失败: %w", err)
	}

	var cfg model.DAGConfig
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			return nil, fmt.Errorf("解析 YAML 失败: %w", err)
		}
	case ".json":
		if err := json.Unmarshal(data, &cfg); err != nil {
			return nil, fmt.Errorf("解析 JSON 失败: %w", err)
		}
	default:
		// 尝试先 JSON 再 YAML
		if json.Unmarshal(data, &cfg) != nil {
			if err := yaml.Unmarshal(data, &cfg); err != nil {
				return nil, fmt.Errorf("无法识别的配置格式（需 .yaml/.yml/.json）: %w", err)
			}
		}
	}

	return &cfg, nil
}

// Save 将配置写回文件，依据扩展名选择 YAML 或 JSON 格式
func Save(path string, cfg *model.DAGConfig) error {
	var (
		data []byte
		err  error
	)
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".json":
		data, err = json.MarshalIndent(cfg, "", "  ")
	default: // .yaml/.yml 或其它，统一按 YAML 写出
		data, err = yaml.Marshal(cfg)
	}
	if err != nil {
		return fmt.Errorf("序列化配置失败: %w", err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("写入配置文件失败: %w", err)
	}
	return nil
}
