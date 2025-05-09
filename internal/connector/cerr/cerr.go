package cerr

import (
	"errors"
	"fmt"
)

var (
	ErrNotSupported = errors.New("not supported")
	ErrValidateConf = errors.New("validate config")
)

func ValidationErr(text string) error {
	return fmt.Errorf(text+": %w", ErrValidateConf)
}
