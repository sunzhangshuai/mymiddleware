package consumer

type Messages struct {
	data  map[string]map[int32]bool // 数据
	order map[string][]int32        // 数据的顺序
}

func NewMessages() *Messages {
	return &Messages{
		data:  nil,
		order: nil,
	}
}
