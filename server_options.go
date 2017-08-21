package grpc

import (
	"math"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/tap"
)

const (
	defaultServerMaxReceiveMessageSize = 1024 * 1024 * 4
	defaultServerMaxSendMessageSize    = math.MaxInt32
)

type options struct {
	creds       credentials.TransportCredentials
	codec       Codec
	cp          Compressor
	dc          Decompressor
	unaryInt    UnaryServerInterceptor
	streamInt   StreamServerInterceptor
	inTapHandle tap.ServerInHandle
	// FIXME(irfansharif): Stripped out all stats handling for optimized codepaths. Re-add.
	statsHandler          stats.Handler
	maxConcurrentStreams  uint32
	maxReceiveMessageSize int
	maxSendMessageSize    int
	useHandlerImpl        bool // use http.Handler-based server
	tracingEnabled        bool
	unknownStreamDesc     *StreamDesc
	keepaliveParams       keepalive.ServerParameters
	keepalivePolicy       keepalive.EnforcementPolicy
	initialWindowSize     int32
	initialConnWindowSize int32
}

func defaultServerOptions() options {
	return options{
		maxReceiveMessageSize: defaultServerMaxReceiveMessageSize,
		maxSendMessageSize:    defaultServerMaxSendMessageSize,
		codec:                 protoCodec{},
		// FIXME(irfansharif): Tracing set to false by default.
		tracingEnabled: false,
	}
}

// A ServerOption sets options such as credentials, codec and keepalive parameters, etc.
type ServerOption func(*options)

// InitialWindowSize returns a ServerOption that sets window size for stream.
// The lower bound for window size is 64K and any value smaller than that will be ignored.
func InitialWindowSize(s int32) ServerOption {
	return func(o *options) {
		o.initialWindowSize = s
	}
}

// InitialConnWindowSize returns a ServerOption that sets window size for a connection.
// The lower bound for window size is 64K and any value smaller than that will be ignored.
func InitialConnWindowSize(s int32) ServerOption {
	return func(o *options) {
		o.initialConnWindowSize = s
	}
}

// KeepaliveParams returns a ServerOption that sets keepalive and max-age parameters for the server.
func KeepaliveParams(kp keepalive.ServerParameters) ServerOption {
	return func(o *options) {
		o.keepaliveParams = kp
	}
}

// KeepaliveEnforcementPolicy returns a ServerOption that sets keepalive enforcement policy for the server.
func KeepaliveEnforcementPolicy(kep keepalive.EnforcementPolicy) ServerOption {
	return func(o *options) {
		o.keepalivePolicy = kep
	}
}

// CustomCodec returns a ServerOption that sets a codec for message marshaling and unmarshaling.
func CustomCodec(codec Codec) ServerOption {
	return func(o *options) {
		o.codec = codec
	}
}

// RPCCompressor returns a ServerOption that sets a compressor for outbound messages.
func RPCCompressor(cp Compressor) ServerOption {
	return func(o *options) {
		o.cp = cp
	}
}

// RPCDecompressor returns a ServerOption that sets a decompressor for inbound messages.
func RPCDecompressor(dc Decompressor) ServerOption {
	return func(o *options) {
		o.dc = dc
	}
}

// MaxMsgSize returns a ServerOption to set the max message size in bytes the server can receive.
// If this is not set, gRPC uses the default limit. Deprecated: use MaxRecvMsgSize instead.
func MaxMsgSize(m int) ServerOption {
	return MaxRecvMsgSize(m)
}

// MaxRecvMsgSize returns a ServerOption to set the max message size in bytes the server can receive.
// If this is not set, gRPC uses the default 4MB.
func MaxRecvMsgSize(m int) ServerOption {
	return func(o *options) {
		o.maxReceiveMessageSize = m
	}
}

// MaxSendMsgSize returns a ServerOption to set the max message size in bytes the server can send.
// If this is not set, gRPC uses the default 4MB.
func MaxSendMsgSize(m int) ServerOption {
	return func(o *options) {
		o.maxSendMessageSize = m
	}
}

// MaxConcurrentStreams returns a ServerOption that will apply a limit on the number
// of concurrent streams to each ServerTransport.
func MaxConcurrentStreams(n uint32) ServerOption {
	return func(o *options) {
		o.maxConcurrentStreams = n
	}
}

// Creds returns a ServerOption that sets credentials for server connections.
func Creds(c credentials.TransportCredentials) ServerOption {
	return func(o *options) {
		o.creds = c
	}
}

// UnaryInterceptor returns a ServerOption that sets the UnaryServerInterceptor for the
// server. Only one unary interceptor can be installed. The construction of multiple
// interceptors (e.g., chaining) can be implemented at the caller.
func UnaryInterceptor(i UnaryServerInterceptor) ServerOption {
	return func(o *options) {
		if o.unaryInt != nil {
			panic("The unary server interceptor was already set and may not be reset.")
		}
		o.unaryInt = i
	}
}

// Tracing returns a Server option that allows gRPC tracing to be enabled/disabled.
func Tracing(enabled bool) ServerOption {
	return func(o *options) {
		o.tracingEnabled = enabled
	}
}

// StreamInterceptor returns a ServerOption that sets the StreamServerInterceptor for the
// server. Only one stream interceptor can be installed.
func StreamInterceptor(i StreamServerInterceptor) ServerOption {
	return func(o *options) {
		if o.streamInt != nil {
			panic("The stream server interceptor was already set and may not be reset.")
		}
		o.streamInt = i
	}
}

// InTapHandle returns a ServerOption that sets the tap handle for all the server
// transport to be created. Only one can be installed.
func InTapHandle(h tap.ServerInHandle) ServerOption {
	return func(o *options) {
		if o.inTapHandle != nil {
			panic("The tap handle was already set and may not be reset.")
		}
		o.inTapHandle = h
	}
}

// StatsHandler returns a ServerOption that sets the stats handler for the server.
func StatsHandler(h stats.Handler) ServerOption {
	return func(o *options) {
		o.statsHandler = h
	}
}

// UnknownServiceHandler returns a ServerOption that allows for adding a custom
// unknown service handler. The provided method is a bidi-streaming RPC service
// handler that will be invoked instead of returning the "unimplemented" gRPC
// error whenever a request is received for an unregistered service or method.
// The handling function has full access to the Context of the request and the
// stream, and the invocation passes through interceptors.
func UnknownServiceHandler(streamHandler StreamHandler) ServerOption {
	return func(o *options) {
		o.unknownStreamDesc = &StreamDesc{
			StreamName: "unknown_service_handler",
			Handler:    streamHandler,
			// We need to assume that the users of the streamHandler will want to use both.
			ClientStreams: true,
			ServerStreams: true,
		}
	}
}
