package client

func boolToByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}
