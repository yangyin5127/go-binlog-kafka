package main

type RowData struct {
	BinLogFile string `json:"binlog_file"`
	LogPos     uint32 `json:"log_pos"`
	Action     string `json:"action"`
	Table      string `json:"table"`
	Gtid       string `json:"gtid"`
	Timestamp  uint32 `json:"timestamp"`
	ServerID   uint32 `json:"server_id"`
	Schema     string `json:"schema"`
	// insert
	Values map[string]interface{} `json:"values,omitempty"`
	// before values
	BeforeValues map[string]interface{} `json:"before_values,omitempty"`
	// after vaulues
	AfterValues map[string]interface{} `json:"after_values,omitempty"`
}
