package assert

func NotNil(v any) {
	if v == nil {
		panic("value is nil")
	}
}
