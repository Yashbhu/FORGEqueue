package model

type TaskMetaData struct {
	ID         string `json:"id"`          // 16 byte
	TaskType   string `json:"task_type"`   // 16 byte
	Payload    []byte `json:"payload"`     // 24 bytes
	MaxRetries int32  `json:"max_retries"` // 4 bytes
	// Attempts counts how many times this task has been executed and
	// failed. It lives in the body so it travels with the task through
	// every queue and survives restarts. Defaults to 0 when the task was
	// enqueued before this field existed.
	Attempts int32 `json:"attempts"` // 4 bytes
}
