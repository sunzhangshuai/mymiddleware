package consumer

import (
	"errors"
)

// OffsetManager 偏移量管理器
type OffsetManager struct {
	position    *int64 // 拉取偏移量
	committed   int64  // 提交偏移量
	leaderEpoch int32
	metadata    string
	needCommit  bool // 是否需要提交
}

// NewOffsetManager 偏移量管理器
func NewOffsetManager() *OffsetManager {
	return &OffsetManager{
		position:  nil,
		committed: 0,
	}
}

// Position 更新拉取偏移量，
func (s *OffsetManager) Position(offset int64) error {
	if !s.HasValidPosition() {
		return errors.New("offset 无效")
	}
	s.position = &offset
	return nil
}

// Seek 重置偏移量
func (s *OffsetManager) Seek(offset int64) {
	s.position = &offset
}

// HasValidPosition 拉取偏移量是否可用
func (s *OffsetManager) HasValidPosition() bool {
	return s.position != nil
}

// Committed 更新提交偏移量
func (s *OffsetManager) Committed(offset int64) {
	s.committed = offset
}
