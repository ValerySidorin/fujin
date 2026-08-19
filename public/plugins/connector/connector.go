// Package connector provides the connector plugin seam used by Session Core.
package connector

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	cmwconfig "github.com/fujin-io/fujin/public/plugins/middleware/connector/config"
	"unicode/utf8"
)

var (
	ErrRouteNotFound        = errors.New("connector route not found")
	ErrOperationUnsupported = errors.New("connector operation unsupported")
	ErrFetchBusy            = errors.New("connector fetch already active")
	ErrInvalidMessageID     = errors.New("invalid connector message ID")
	ErrInvalidHeaders       = errors.New("invalid connector headers")
)

// AcceptanceGuarantee is the strongest acknowledgement a successful Produce observes.
type AcceptanceGuarantee uint8

const (
	AcceptanceUnspecified AcceptanceGuarantee = iota
	AcceptanceLocal
	AcceptancePeer
	AcceptanceDurable
)

func (g AcceptanceGuarantee) String() string {
	switch g {
	case AcceptanceLocal:
		return "local_accept"
	case AcceptancePeer:
		return "peer_accept"
	case AcceptanceDurable:
		return "durable_accept"
	default:
		return "unspecified"
	}
}

// AckGranularity describes the scope of a successful ACK.
type AckGranularity uint8

const (
	AckUnsupported AckGranularity = iota
	AckSingle
	AckCumulative
)

// NackEffect describes the observable effect of a successful NACK.
type NackEffect uint8

const (
	NackUnsupported NackEffect = iota
	NackRequeue
	NackRelease
	NackDrop
)

// SettlementProfile describes manual settlement semantics for a route.
type SettlementProfile struct {
	Ack  AckGranularity
	Nack NackEffect
}

// RouteProfile is the immutable operation and guarantee contract for one route.
type RouteProfile struct {
	Produce          bool
	Headers          bool
	Transactions     bool
	Subscribe        bool
	Fetch            bool
	ManualSettlement bool
	ProduceGuarantee AcceptanceGuarantee
	Settlement       SettlementProfile
}

// Validate rejects contradictory profiles before a configuration generation is published.
func (p RouteProfile) Validate(route string) error {
	if p.Produce && p.ProduceGuarantee == AcceptanceUnspecified {
		return fmt.Errorf("route %q: produce guarantee is required", route)
	}
	if !p.Produce && p.ProduceGuarantee != AcceptanceUnspecified {
		return fmt.Errorf("route %q: produce guarantee without produce capability", route)
	}
	if p.Transactions && !p.Produce {
		return fmt.Errorf("route %q: transactions require produce capability", route)
	}
	if p.Headers && !p.Produce && !p.Subscribe && !p.Fetch {
		return fmt.Errorf("route %q: headers require a message operation", route)
	}
	if p.ManualSettlement {
		if !p.Subscribe && !p.Fetch {
			return fmt.Errorf("route %q: manual settlement requires a read capability", route)
		}
		if p.Settlement.Ack == AckUnsupported {
			return fmt.Errorf("route %q: manual settlement requires ACK semantics", route)
		}
	} else if p.Settlement.Ack != AckUnsupported || p.Settlement.Nack != NackUnsupported {
		return fmt.Errorf("route %q: settlement profile without manual settlement capability", route)
	}
	return nil
}

// ValidateHeaders validates Fujin's canonical unordered multimap representation.
// Keys are non-empty UTF-8 strings, values are arbitrary bytes, and duplicate keys are valid.
func ValidateHeaders(headers [][]byte) error {
	if len(headers)%2 != 0 {
		return fmt.Errorf("%w: expected key/value pairs", ErrInvalidHeaders)
	}
	for i := 0; i < len(headers); i += 2 {
		if len(headers[i]) == 0 {
			return fmt.Errorf("%w: header key %d is empty", ErrInvalidHeaders, i/2)
		}
		if !utf8.Valid(headers[i]) {
			return fmt.Errorf("%w: header key %d is not UTF-8", ErrInvalidHeaders, i/2)
		}
	}
	return nil
}

// ValidateMessageIDPayload validates an adapter payload before binary decoding.
// fixedLen is the broker metadata prefix length; sourceRequired permits a trailing source.
func ValidateMessageIDPayload(payload []byte, fixedLen int, sourceRequired bool) error {
	if fixedLen < 0 {
		return fmt.Errorf("%w: invalid expected length %d", ErrInvalidMessageID, fixedLen)
	}
	if sourceRequired {
		if len(payload) > fixedLen {
			return nil
		}
		return fmt.Errorf("%w: expected more than %d bytes, got %d", ErrInvalidMessageID, fixedLen, len(payload))
	}
	if len(payload) != fixedLen {
		return fmt.Errorf("%w: expected %d bytes, got %d", ErrInvalidMessageID, fixedLen, len(payload))
	}
	return nil
}

// Reader is a broker reader. Subscribe invokes ready exactly once after the strongest
// readiness observable by the adapter has been established and before message delivery.
type Reader interface {
	Subscribe(ctx context.Context, ready func() error, h func(message []byte, source string, args ...any)) error
	SubscribeWithHeaders(ctx context.Context, ready func() error, h func(message []byte, source string, headers [][]byte, args ...any)) error
	// Fetch invokes fetchResponseHandler exactly once before any msgHandler call.
	// The reported count equals the number of subsequent synchronous message callbacks.
	Fetch(ctx context.Context, n uint32, fetchResponseHandler func(n uint32, err error), msgHandler func(message []byte, source string, args ...any))
	FetchWithHeaders(ctx context.Context, n uint32, fetchResponseHandler func(n uint32, err error), msgHandler func(message []byte, source string, headers [][]byte, args ...any))
	// ACK/NACK callback groups may arrive in either order, but callbacks are serialized
	// per invocation and each adapter message ID is reported exactly once after top-level success.
	Ack(ctx context.Context, msgIDs [][]byte, ackHandler func(error), ackMsgHandler func([]byte, error))
	Nack(ctx context.Context, msgIDs [][]byte, nackHandler func(error), nackMsgHandler func([]byte, error))
	MsgIDArgsLen() int
	EncodeMsgID(buf []byte, source string, args ...any) []byte
	AutoCommit() bool
}

// Writer accepts a message exactly once and invokes its callback exactly once. Flush is
// a snapshot barrier for every operation accepted before the call.
type Writer interface {
	Produce(ctx context.Context, msg []byte, callback func(err error))
	HProduce(ctx context.Context, msg []byte, headers [][]byte, callback func(err error))
	Flush(ctx context.Context) error
	BeginTx(ctx context.Context) error
	CommitTx(ctx context.Context) error
	RollbackTx(ctx context.Context) error
}

// ReadCloser is a session-scoped reader lease.
type ReadCloser interface {
	Reader
	io.Closer
}

// WriteCloser is a session-scoped writer lease.
type WriteCloser interface {
	Writer
	io.Closer
}

// Runtime is owned by one immutable configuration generation and may share physical
// resources across session-scoped reader and writer leases.
type Runtime interface {
	NewReader(route string, autoSettle bool, l *slog.Logger) (ReadCloser, error)
	NewWriter(route string, l *slog.Logger) (WriteCloser, error)
	Close(context.Context) error
}

// MiddlewareChain is a generation-scoped, prevalidated connector middleware chain.
// Implementations own any compiled plugin resources until Close is called after generation drain.
type MiddlewareChain interface {
	WrapReader(ReadCloser, string, *slog.Logger) (ReadCloser, error)
	WrapWriter(WriteCloser, string, *slog.Logger) (WriteCloser, error)
	Close(context.Context) error
}

// MiddlewareCompileFunc validates and compiles one connector's complete middleware chain.
type MiddlewareCompileFunc func([]cmwconfig.Config, *slog.Logger) (MiddlewareChain, error)

// Compiled is side-effect-free validated connector configuration.
type Compiled interface {
	Routes() map[string]RouteProfile
	OpenRuntime(l *slog.Logger) (Runtime, error)
}

// EagerRuntimeCompiled opts a connector into opening its generation runtime before
// publication. Compile must remain side-effect-free; broker I/O belongs in OpenRuntime.
type EagerRuntimeCompiled interface {
	OpenRuntimeEagerly() bool
}

// ExclusiveRuntimeCompiled reports resources that cannot be opened while an
// older generation owns them, such as ZeroMQ bind endpoints.
type ExclusiveRuntimeCompiled interface {
	ExclusiveRuntimeKeys() []string
}

// ErrRuntimeDrainRequired means an active generation must retire before the
// requested runtime can claim an unchanged exclusive resource.
var ErrRuntimeDrainRequired = errors.New("connector runtime drain required")

// CompileFunc decodes, normalizes, and validates connector settings without broker I/O.
type CompileFunc func(settings any) (Compiled, error)

// ConfigValueConverterFunc converts and validates one runtime override value.
type ConfigValueConverterFunc func(settingPath string, value string) (any, error)

// Descriptor is static plugin metadata. Neither Converter nor Compile may create broker resources.
type Descriptor struct {
	Converter ConfigValueConverterFunc
	Compile   CompileFunc
}

// StaticCompiled creates an immutable compiled connector from profiles and a runtime opener.
func StaticCompiled(profiles map[string]RouteProfile, open func(*slog.Logger) (Runtime, error)) (Compiled, error) {
	if len(profiles) == 0 {
		return nil, errors.New("connector has no routes")
	}
	copyProfiles := make(map[string]RouteProfile, len(profiles))
	for route, profile := range profiles {
		if route == "" {
			return nil, errors.New("connector route name is empty")
		}
		if err := profile.Validate(route); err != nil {
			return nil, err
		}
		copyProfiles[route] = profile
	}
	if open == nil {
		return nil, errors.New("connector runtime opener is nil")
	}
	return &staticCompiled{profiles: copyProfiles, open: open}, nil
}

type staticCompiled struct {
	profiles map[string]RouteProfile
	open     func(*slog.Logger) (Runtime, error)
}

func (c *staticCompiled) Routes() map[string]RouteProfile {
	profiles := make(map[string]RouteProfile, len(c.profiles))
	for route, profile := range c.profiles {
		profiles[route] = profile
	}
	return profiles
}
func (c *staticCompiled) OpenRuntime(l *slog.Logger) (Runtime, error) { return c.open(l) }

var (
	descriptors = make(map[string]Descriptor)
	mu          sync.RWMutex
)

// Register registers a static connector descriptor.
func Register(protocol string, descriptor Descriptor) error {
	if protocol == "" {
		return errors.New("connector protocol is empty")
	}
	if descriptor.Compile == nil {
		return fmt.Errorf("connector %q compile function is nil", protocol)
	}
	mu.Lock()
	defer mu.Unlock()
	if _, exists := descriptors[protocol]; exists {
		return fmt.Errorf("connector descriptor for protocol %q already registered", protocol)
	}
	descriptors[protocol] = descriptor
	return nil
}

// Get returns a connector descriptor by protocol name.
func Get(protocol string) (Descriptor, bool) {
	mu.RLock()
	defer mu.RUnlock()
	descriptor, ok := descriptors[protocol]
	return descriptor, ok
}

// GetConfigValueConverter returns the descriptor's side-effect-free override converter.
func GetConfigValueConverter(protocol string) ConfigValueConverterFunc {
	descriptor, ok := Get(protocol)
	if !ok {
		return nil
	}
	return descriptor.Converter
}

// List returns registered connector protocol names.
func List() []string {
	mu.RLock()
	defer mu.RUnlock()
	names := make([]string, 0, len(descriptors))
	for protocol := range descriptors {
		names = append(names, protocol)
	}
	return names
}
