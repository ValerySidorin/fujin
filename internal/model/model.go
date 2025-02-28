package model

type Consumer struct {
	Name       string `json:"name"`
	AutoCommit bool   `json:"auto_commit"`
}

type Info struct {
	Consumers map[string]Consumer
}
