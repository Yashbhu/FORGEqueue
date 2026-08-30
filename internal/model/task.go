package model

type TaskMetaData struct {
	ID         string `json:"id"`          // 16 byte
	TaskType   string `json:"task_type"`   // 16 byte
	Payload    []byte `json:"payload"`     // 24 bytes
	MaxRetries int32  `json:"max_retries"` // 4 bytes
}
