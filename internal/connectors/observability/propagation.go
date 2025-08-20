package observability

import "strings"

type byteHeadersCarrier struct {
	hs *[][]byte
}

func newByteHeadersCarrier(hs *[][]byte) byteHeadersCarrier { return byteHeadersCarrier{hs: hs} }

func (c byteHeadersCarrier) Get(key string) string {
	if c.hs == nil {
		return ""
	}
	lower := strings.ToLower(key)
	headers := *c.hs
	for i := 0; i+1 < len(headers); i += 2 {
		if strings.ToLower(string(headers[i])) == lower {
			return string(headers[i+1])
		}
	}
	return ""
}

func (c byteHeadersCarrier) Set(key, value string) {
	if c.hs == nil {
		return
	}
	headers := *c.hs
	headers = append(headers, []byte(key), []byte(value))
	*c.hs = headers
}

func (c byteHeadersCarrier) Keys() []string {
	if c.hs == nil || *c.hs == nil {
		return nil
	}
	headers := *c.hs
	keys := make([]string, 0, len(headers)/2)
	for i := 0; i+1 < len(headers); i += 2 {
		keys = append(keys, string(headers[i]))
	}
	return keys
}
