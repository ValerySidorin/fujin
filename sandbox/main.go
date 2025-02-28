package main

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
)

func main() {
	buf := make([]byte, 6)
	binary.BigEndian.PutUint16(buf[:2], 12)
	binary.BigEndian.PutUint32(buf[2:6], 12)
	fmt.Println(buf)

	epoch := int32(binary.BigEndian.Uint16(buf[:2]))
	offset := int64(binary.BigEndian.Uint32(buf[2:6]))
	fmt.Println(epoch, offset)
}

func encode(text string) []byte {
	return []byte(text)
}

func decode(bytes string) string {
	byteStrArr := strings.Split(bytes, " ")
	byteArr := make([]byte, 0, len(byteStrArr))
	for _, val := range byteStrArr {
		b, _ := strconv.Atoi(val)
		byteArr = append(byteArr, byte(b))
	}

	return string(byteArr)
}
