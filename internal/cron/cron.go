// Package cron 提供一个零依赖的标准 5 字段 crontab 解析器。
// 字段顺序：分(0-59) 时(0-23) 日(1-31) 月(1-12) 周(0-6，0=周日)。
// 每个字段支持：*、具体数字、a-b 范围、*/n 步进、a-b/n、以及逗号分隔的列表。
package cron

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Schedule 已解析的 cron 表达式，使用位掩码表示每个字段允许的取值
type Schedule struct {
	minute uint64 // bit 0-59
	hour   uint64 // bit 0-23
	dom    uint64 // bit 1-31（日）
	month  uint64 // bit 1-12
	dow    uint64 // bit 0-6（周，0=周日）

	domStar bool // 日字段是否为 *
	dowStar bool // 周字段是否为 *
}

type fieldSpec struct {
	min, max int
}

var specs = []fieldSpec{
	{0, 59}, // minute
	{0, 23}, // hour
	{1, 31}, // day of month
	{1, 12}, // month
	{0, 6},  // day of week
}

// Parse 解析 crontab 表达式，失败返回错误
func Parse(expr string) (*Schedule, error) {
	fields := strings.Fields(strings.TrimSpace(expr))
	if len(fields) != 5 {
		return nil, fmt.Errorf("cron 表达式需要 5 个字段（分 时 日 月 周），实际 %d 个", len(fields))
	}

	masks := make([]uint64, 5)
	for i, f := range fields {
		m, err := parseField(f, specs[i])
		if err != nil {
			return nil, fmt.Errorf("第 %d 个字段 %q 非法: %w", i+1, f, err)
		}
		masks[i] = m
	}

	return &Schedule{
		minute:  masks[0],
		hour:    masks[1],
		dom:     masks[2],
		month:   masks[3],
		dow:     masks[4],
		domStar: fields[2] == "*",
		dowStar: fields[4] == "*",
	}, nil
}

// parseField 解析单个字段为位掩码
func parseField(field string, sp fieldSpec) (uint64, error) {
	var mask uint64
	for _, part := range strings.Split(field, ",") {
		m, err := parseRange(strings.TrimSpace(part), sp)
		if err != nil {
			return 0, err
		}
		mask |= m
	}
	if mask == 0 {
		return 0, fmt.Errorf("空字段")
	}
	return mask, nil
}

// parseRange 解析形如 *、a、a-b、*/n、a-b/n 的单元
func parseRange(part string, sp fieldSpec) (uint64, error) {
	step := 1
	rangePart := part
	if slash := strings.Index(part, "/"); slash >= 0 {
		rangePart = part[:slash]
		s, err := strconv.Atoi(part[slash+1:])
		if err != nil || s <= 0 {
			return 0, fmt.Errorf("步进值非法")
		}
		step = s
	}

	var lo, hi int
	if rangePart == "*" {
		lo, hi = sp.min, sp.max
	} else if dash := strings.Index(rangePart, "-"); dash >= 0 {
		var err error
		if lo, err = strconv.Atoi(rangePart[:dash]); err != nil {
			return 0, fmt.Errorf("范围下界非法")
		}
		if hi, err = strconv.Atoi(rangePart[dash+1:]); err != nil {
			return 0, fmt.Errorf("范围上界非法")
		}
	} else {
		v, err := strconv.Atoi(rangePart)
		if err != nil {
			return 0, fmt.Errorf("数值非法")
		}
		lo, hi = v, v
	}

	if lo < sp.min || hi > sp.max || lo > hi {
		return 0, fmt.Errorf("取值需在 %d-%d 之间", sp.min, sp.max)
	}

	var mask uint64
	for v := lo; v <= hi; v += step {
		mask |= 1 << uint(v)
	}
	return mask, nil
}

// Match 判断给定时间（精确到分钟）是否匹配该 cron 表达式
func (s *Schedule) Match(t time.Time) bool {
	if s.minute&(1<<uint(t.Minute())) == 0 {
		return false
	}
	if s.hour&(1<<uint(t.Hour())) == 0 {
		return false
	}
	if s.month&(1<<uint(t.Month())) == 0 {
		return false
	}

	domMatch := s.dom&(1<<uint(t.Day())) != 0
	dowMatch := s.dow&(1<<uint(int(t.Weekday()))) != 0

	// 标准 vixie cron 语义：当日、周字段均被限定（非 *）时取并集，否则取交集
	if s.domStar || s.dowStar {
		return domMatch && dowMatch
	}
	return domMatch || dowMatch
}
