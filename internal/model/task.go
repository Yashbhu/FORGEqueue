package model

type TaskMetaData struct {
	ID         string `json:"id"`          // 16 byte
	TaskType   string `json:"task_type"`   // 16 byte
	Payload    []byte `json:"payload"`     // 24 bytes
	MaxRetries int32  `json:"max_retries"` // 4 bytes
	// how many times this task ran and failed. lives in the body so it
	// travels with the task and survives restarts. defaults to 0.
	Attempts int32 `json:"attempts"` // 4 bytes
}
