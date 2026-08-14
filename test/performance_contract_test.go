package test

import (
	"bytes"
	"encoding/binary"
	"runtime"
	"strings"
	"testing"

	v1 "github.com/fujin-io/fujin/public/proto/fujin/v1"
)

type performanceContract struct {
	PayloadBytes         []int
	SmallMessageCounts   []int
	LargeMessageCounts   []int
	Concurrency          []int
	BatchSizes           []int
	MaxBatchPayloadBytes int
	Transports           []string
	Samples              int
	InconclusiveSamples  int
	RegressionAlpha      float64
	AllocationRegression bool
}

var approvedPerformanceContract = performanceContract{
	PayloadBytes:         []int{1, 128, 1024, 32 * 1024, 1024 * 1024},
	SmallMessageCounts:   []int{10_000, 100_000, 1_000_000},
	LargeMessageCounts:   []int{1_000, 10_000},
	Concurrency:          []int{1, 16, 128},
	BatchSizes:           []int{1, 32, 256},
	MaxBatchPayloadBytes: 4 * 1024 * 1024,
	Transports:           []string{"tcp", "quic", "unix", "grpc"},
	Samples:              10,
	InconclusiveSamples:  20,
	RegressionAlpha:      0.05,
	AllocationRegression: false,
}

func TestPerformanceContract(t *testing.T) {
	contract := approvedPerformanceContract
	assertIntsEqual(t, contract.PayloadBytes, []int{1, 128, 1024, 32 * 1024, 1024 * 1024})
	assertIntsEqual(t, contract.SmallMessageCounts, []int{10_000, 100_000, 1_000_000})
	assertIntsEqual(t, contract.LargeMessageCounts, []int{1_000, 10_000})
	assertIntsEqual(t, contract.Concurrency, []int{1, 16, 128})
	assertIntsEqual(t, contract.BatchSizes, []int{1, 32, 256})
	if contract.MaxBatchPayloadBytes != 4*1024*1024 {
		t.Fatalf("max batch payload bytes: got %d", contract.MaxBatchPayloadBytes)
	}
	if len(contract.Transports) != 4 {
		t.Fatalf("transports: got %v", contract.Transports)
	}
	if contract.Samples < 5 || contract.InconclusiveSamples <= contract.Samples {
		t.Fatalf("invalid sample policy: initial=%d inconclusive=%d", contract.Samples, contract.InconclusiveSamples)
	}
	if contract.RegressionAlpha != 0.05 {
		t.Fatalf("regression alpha: got %v", contract.RegressionAlpha)
	}
	if contract.AllocationRegression {
		t.Fatal("allocation increases must block the performance gate")
	}
}

func benchmarkBatchSizes(payloadSize int) []int {
	batchSizes := make([]int, 0, len(approvedPerformanceContract.BatchSizes))
	for _, batchSize := range approvedPerformanceContract.BatchSizes {
		if payloadSize <= approvedPerformanceContract.MaxBatchPayloadBytes/batchSize {
			batchSizes = append(batchSizes, batchSize)
		}
	}
	return batchSizes
}

func TestPerformanceBatchMatrixBoundsWirePayload(t *testing.T) {
	covered := make(map[int]bool, len(approvedPerformanceContract.BatchSizes))
	for _, payloadSize := range approvedPerformanceContract.PayloadBytes {
		batchSizes := benchmarkBatchSizes(payloadSize)
		if len(batchSizes) == 0 {
			t.Fatalf("payload %d has no batch size", payloadSize)
		}
		for _, batchSize := range batchSizes {
			covered[batchSize] = true
			if payloadSize*batchSize > approvedPerformanceContract.MaxBatchPayloadBytes {
				t.Fatalf("payload=%d batch=%d exceeds wire bound", payloadSize, batchSize)
			}
		}
	}
	for _, batchSize := range approvedPerformanceContract.BatchSizes {
		if !covered[batchSize] {
			t.Fatalf("batch size %d is not covered", batchSize)
		}
	}
}

func TestPerformanceEnvironmentFingerprint(t *testing.T) {
	t.Logf("os=%s arch=%s go=%s gomaxprocs=%d cpus=%d tags=fujin,grpc", runtime.GOOS, runtime.GOARCH, runtime.Version(), runtime.GOMAXPROCS(0), runtime.NumCPU())
}

func assertIntsEqual(t *testing.T, got, want []int) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestValidateProduceBenchmarkResponsesHandlesFragmentedFrames(t *testing.T) {
	responses := []byte{byte(v1.RESP_CODE_BIND), byte(v1.STATUS_OK), 0, 0, 0, 0}
	for range 2 {
		responses = append(responses, byte(v1.RESP_CODE_PRODUCE), 0, 0, 0, 0, byte(v1.STATUS_OK))
	}
	responses = append(responses, byte(v1.RESP_CODE_DISCONNECT))

	count, err := validateProduceBenchmarkResponses(&singleByteReader{reader: bytes.NewReader(responses)})
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("produce response count: got %d, want 2", count)
	}
}

func TestValidateProduceBenchmarkResponsesReturnsProduceError(t *testing.T) {
	responses := []byte{byte(v1.RESP_CODE_BIND), byte(v1.STATUS_OK), 0, 0, 0, 0}
	responses = append(responses, byte(v1.RESP_CODE_PRODUCE), 0, 0, 0, 0, byte(v1.STATUS_UNAVAILABLE), byte(v1.OUTCOME_UNKNOWN))
	responses = appendFujinString(responses, "UNAVAILABLE")
	message := "broker unavailable"
	responses = appendFujinString(responses, message)
	responses = binary.BigEndian.AppendUint16(responses, 0)

	count, err := validateProduceBenchmarkResponses(bytes.NewReader(responses))
	if count != 0 {
		t.Fatalf("produce response count: got %d, want 0", count)
	}
	if err == nil || !strings.Contains(err.Error(), message) {
		t.Fatalf("expected broker error, got %v", err)
	}
}

type singleByteReader struct {
	reader *bytes.Reader
}

func (r *singleByteReader) Read(p []byte) (int, error) {
	if len(p) > 1 {
		p = p[:1]
	}
	return r.reader.Read(p)
}
