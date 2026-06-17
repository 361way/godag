package sink

import (
	"fmt"
	"plugin"
	"strings"
)

// Load 通过 Go plugin 机制加载一个 .so 持久化插件并完成初始化。
//
// 约定：每个插件以 package main 编译为 .so，并导出一个名为 "Sink" 的包级变量，
// 类型为本包的 Sink 接口：
//
//	var Sink sink.Sink = &myBackend{}
//
// plugin.Lookup("Sink") 返回的是指向该接口变量的指针（*sink.Sink）。
func Load(path string, config map[string]string) (Sink, error) {
	p, err := plugin.Open(path)
	if err != nil {
		return nil, fmt.Errorf("打开插件 %s 失败: %w", path, err)
	}
	sym, err := p.Lookup("Sink")
	if err != nil {
		return nil, fmt.Errorf("插件 %s 缺少导出符号 Sink: %w", path, err)
	}
	sp, ok := sym.(*Sink)
	if !ok {
		return nil, fmt.Errorf("插件 %s 的 Sink 符号类型不匹配（应为 sink.Sink）", path)
	}
	s := *sp
	if s == nil {
		return nil, fmt.Errorf("插件 %s 的 Sink 为空", path)
	}
	if err := s.Open(config); err != nil {
		return nil, fmt.Errorf("初始化插件 %s 失败: %w", s.Name(), err)
	}
	return s, nil
}

// ParseConfig 将 "k1=v1,k2=v2" 形式的字符串解析为配置 map。
func ParseConfig(s string) map[string]string {
	out := make(map[string]string)
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			continue
		}
		out[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
	}
	return out
}
