package transport

import (
	"io"
	"sync"

	"golang.org/x/net/context"
	"google.golang.org/grpc/metadata"
)

// StreamOptimized represents an RPC in the transport layer.
type StreamOptimized struct {
	id uint32
	// ctx is the associated context of the stream.
	ctx context.Context
	// cancel is always nil for client side Stream.
	cancel context.CancelFunc
	// goAway is closed when the server sent GoAways signal before this stream was initiated.
	goAway chan struct{}
	// method records the associated RPC method of the stream.
	method                   string
	recvCompressionAlgorithm string
	sendCompressionAlgorithm string
	buf                      *recvBuffer
	trReader                 io.Reader
	fc                       *inFlow
	recvQuota                uint32

	// TODO: Remote this unused variable.
	// The accumulated inbound quota pending for window update.
	updateQuota uint32

	// Callback to state application's intentions to read data. This
	// is used to adjust flow control, if need be.
	requestRead func(int)

	sendQuotaPool *quotaPool
	// header caches the received header metadata.
	header metadata.MD
	// The key-value map of trailer metadata.
	trailer metadata.MD

	mu struct {
		sync.RWMutex

		// headerOK becomes true from the first header is about to send.
		headerOk bool
		state    streamState
	}
}

// RecvCompress returns the compression algorithm applied to the inbound
// message. It is empty string if there is no compression applied.
func (s *StreamOptimized) RecvCompressionAlgorithm() string {
	return s.recvCompressionAlgorithm
}

// SetSendCompress sets the compression algorithm to the stream.
func (s *StreamOptimized) SetSendCompressionAlgorithm(str string) {
	s.sendCompressionAlgorithm = str
}

// FIXME(irfansharif): This is an anti-pattern.
// Context returns the context of the stream.
func (s *StreamOptimized) Context() context.Context {
	return s.ctx
}

// FIXME(irfansharif): Can this be removed?
// Method returns the method for the stream.
func (s *StreamOptimized) Method() string {
	return s.method
}

// FIXME(irfansharif): Could be removed if transport level interface is fixed.
// SetHeader sets the header metadata. This can be called multiple times.
// Server side only.
func (s *StreamOptimized) SetHeader(md metadata.MD) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.headerOk || s.mu.state == streamDone {
		return ErrIllegalHeaderWrite
	}
	if md.Len() == 0 {
		return nil
	}
	s.header = metadata.Join(s.header, md)
	return nil
}

// FIXME(irfansharif): Could be removed if transport level interface is fixed.
// SetTrailer sets the trailer metadata which will be sent with the RPC status
// by the server. This can be called multiple times. Server side only.
func (s *StreamOptimized) SetTrailer(md metadata.MD) error {
	if md.Len() == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.trailer = metadata.Join(s.trailer, md)
	return nil
}

func (s *StreamOptimized) write(m recvMsg) {
	s.buf.put(m)
}

// Read reads all p bytes from the wire for this stream.
func (s *StreamOptimized) Read(p []byte) (n int, err error) {
	// Don't request a read if there was an error earlier
	if er := s.trReader.(*transportReader).er; er != nil {
		return 0, er
	}
	s.requestRead(len(p))
	return io.ReadFull(s.trReader, p)
}
