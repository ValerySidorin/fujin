package resp

import "strings"

func ParseConfigEndpoint(conf map[string]any) string {
	initAddressRaw, ok := conf["init_address"].([]any)
	if !ok {
		return ""
	}
	var initAddressStr []string
	for _, ia := range initAddressRaw {
		if addrStr, sOk := ia.(string); sOk {
			initAddressStr = append(initAddressStr, addrStr)
		}
	}
	return strings.Join(initAddressStr, ",")
}
