package test

import (
	"testing"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

func TestNopWriterContractCompliant(t *testing.T) {
	underlying := newWriter()
	if got := connector.EnforceWriterContract(underlying); got != underlying {
		t.Fatal("nop writer was wrapped despite satisfying the writer contract")
	}
}
