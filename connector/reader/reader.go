package reader

type ReaderType byte

const (
	Unknown ReaderType = iota
	Subscriber
	Consumer
)
