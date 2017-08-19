package grpc

import (
	"math"
	"net"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/transport"
)

// dialOptions configure a Dial call. dialOptions are set by the DialOption
// values passed to Dial.
type dialOptions struct {
	unaryInt    UnaryClientInterceptor
	streamInt   StreamClientInterceptor
	codec       Codec
	cp          Compressor
	dc          Decompressor
	bs          backoffStrategy
	balancer    Balancer
	block       bool
	insecure    bool
	timeout     time.Duration
	scChan      <-chan ServiceConfig
	copts       transport.ConnectOptions
	callOptions []CallOption
}

const (
	defaultClientMaxReceiveMessageSize = 1024 * 1024 * 4
	defaultClientMaxSendMessageSize    = math.MaxInt32
)

// DialOption configures how we set up the connection.
type DialOption func(*dialOptions)

// WithInitialWindowSize returns a DialOption which sets the value for initial window size on a stream.
// The lower bound for window size is 64K and any value smaller than that will be ignored.
func WithInitialWindowSize(s int32) DialOption {
	return func(o *dialOptions) {
		o.copts.InitialWindowSize = s
	}
}

// WithInitialConnWindowSize returns a DialOption which sets the value for initial window size on a connection.
// The lower bound for window size is 64K and any value smaller than that will be ignored.
func WithInitialConnWindowSize(s int32) DialOption {
	return func(o *dialOptions) {
		o.copts.InitialConnWindowSize = s
	}
}

// WithMaxMsgSize returns a DialOption which sets the maximum message size the client can receive. Deprecated: use WithDefaultCallOptions(MaxCallRecvMsgSize(s)) instead.
func WithMaxMsgSize(s int) DialOption {
	return WithDefaultCallOptions(MaxCallRecvMsgSize(s))
}

// WithDefaultCallOptions returns a DialOption which sets the default CallOptions for calls over the connection.
func WithDefaultCallOptions(cos ...CallOption) DialOption {
	return func(o *dialOptions) {
		o.callOptions = append(o.callOptions, cos...)
	}
}

// WithCodec returns a DialOption which sets a codec for message marshaling and unmarshaling.
func WithCodec(c Codec) DialOption {
	return func(o *dialOptions) {
		o.codec = c
	}
}

// WithCompressor returns a DialOption which sets a CompressorGenerator for generating message
// compressor.
func WithCompressor(cp Compressor) DialOption {
	return func(o *dialOptions) {
		o.cp = cp
	}
}

// WithDecompressor returns a DialOption which sets a DecompressorGenerator for generating
// message decompressor.
func WithDecompressor(dc Decompressor) DialOption {
	return func(o *dialOptions) {
		o.dc = dc
	}
}

// WithBalancer returns a DialOption which sets a load balancer.
func WithBalancer(b Balancer) DialOption {
	return func(o *dialOptions) {
		o.balancer = b
	}
}

// WithServiceConfig returns a DialOption which has a channel to read the service configuration.
func WithServiceConfig(c <-chan ServiceConfig) DialOption {
	return func(o *dialOptions) {
		o.scChan = c
	}
}

// WithBackoffMaxDelay configures the dialer to use the provided maximum delay
// when backing off after failed connection attempts.
func WithBackoffMaxDelay(md time.Duration) DialOption {
	return WithBackoffConfig(BackoffConfig{MaxDelay: md})
}

// WithBackoffConfig configures the dialer to use the provided backoff
// parameters after connection failures.
//
// Use WithBackoffMaxDelay until more parameters on BackoffConfig are opened up
// for use.
func WithBackoffConfig(b BackoffConfig) DialOption {
	// Set defaults to ensure that provided BackoffConfig is valid and
	// unexported fields get default values.
	setDefaults(&b)
	return withBackoff(b)
}

// withBackoff sets the backoff strategy used for retries after a
// failed connection attempt.
//
// This can be exported if arbitrary backoff strategies are allowed by gRPC.
func withBackoff(bs backoffStrategy) DialOption {
	return func(o *dialOptions) {
		o.bs = bs
	}
}

// WithBlock returns a DialOption which makes caller of Dial blocks until the underlying
// connection is up. Without this, Dial returns immediately and connecting the server
// happens in background.
func WithBlock() DialOption {
	return func(o *dialOptions) {
		o.block = true
	}
}

// WithInsecure returns a DialOption which disables transport security for this ClientConn.
// Note that transport security is required unless WithInsecure is set.
func WithInsecure() DialOption {
	return func(o *dialOptions) {
		o.insecure = true
	}
}

// WithTransportCredentials returns a DialOption which configures a
// connection level security credentials (e.g., TLS/SSL).
func WithTransportCredentials(creds credentials.TransportCredentials) DialOption {
	return func(o *dialOptions) {
		o.copts.TransportCredentials = creds
	}
}

// WithPerRPCCredentials returns a DialOption which sets
// credentials and places auth state on each outbound RPC.
func WithPerRPCCredentials(creds credentials.PerRPCCredentials) DialOption {
	return func(o *dialOptions) {
		o.copts.PerRPCCredentials = append(o.copts.PerRPCCredentials, creds)
	}
}

// WithTimeout returns a DialOption that configures a timeout for dialing a ClientConn
// initially. This is valid if and only if WithBlock() is present.
// Deprecated: use DialContext and context.WithTimeout instead.
func WithTimeout(d time.Duration) DialOption {
	return func(o *dialOptions) {
		o.timeout = d
	}
}

// WithDialer returns a DialOption that specifies a function to use for dialing network addresses.
// If FailOnNonTempDialError() is set to true, and an error is returned by f, gRPC checks the error's
// Temporary() method to decide if it should try to reconnect to the network address.
func WithDialer(f func(string, time.Duration) (net.Conn, error)) DialOption {
	return func(o *dialOptions) {
		o.copts.Dialer = func(ctx context.Context, addr string) (net.Conn, error) {
			if deadline, ok := ctx.Deadline(); ok {
				return f(addr, deadline.Sub(time.Now()))
			}
			return f(addr, 0)
		}
	}
}

// WithStatsHandler returns a DialOption that specifies the stats handler
// for all the RPCs and underlying network connections in this ClientConn.
func WithStatsHandler(h stats.Handler) DialOption {
	return func(o *dialOptions) {
		o.copts.StatsHandler = h
	}
}

// FailOnNonTempDialError returns a DialOption that specifies if gRPC fails on non-temporary dial errors.
// If f is true, and dialer returns a non-temporary error, gRPC will fail the connection to the network
// address and won't try to reconnect.
// The default value of FailOnNonTempDialError is false.
// This is an EXPERIMENTAL API.
func FailOnNonTempDialError(f bool) DialOption {
	return func(o *dialOptions) {
		o.copts.FailOnNonTempDialError = f
	}
}

// WithUserAgent returns a DialOption that specifies a user agent string for all the RPCs.
func WithUserAgent(s string) DialOption {
	return func(o *dialOptions) {
		o.copts.UserAgent = s
	}
}

// WithKeepaliveParams returns a DialOption that specifies keepalive paramaters for the client transport.
func WithKeepaliveParams(kp keepalive.ClientParameters) DialOption {
	return func(o *dialOptions) {
		o.copts.KeepaliveParams = kp
	}
}

// WithUnaryInterceptor returns a DialOption that specifies the interceptor for unary RPCs.
func WithUnaryInterceptor(f UnaryClientInterceptor) DialOption {
	return func(o *dialOptions) {
		o.unaryInt = f
	}
}

// WithStreamInterceptor returns a DialOption that specifies the interceptor for streaming RPCs.
func WithStreamInterceptor(f StreamClientInterceptor) DialOption {
	return func(o *dialOptions) {
		o.streamInt = f
	}
}

// WithAuthority returns a DialOption that specifies the value to be used as
// the :authority pseudo-header. This value only works with WithInsecure and
// has no effect if TransportCredentials are present.
func WithAuthority(a string) DialOption {
	return func(o *dialOptions) {
		o.copts.Authority = a
	}
}
