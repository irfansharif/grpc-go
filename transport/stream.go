package transport

import (
	"io"
	"sync"

	"golang.org/x/net/context"
	"golang.org/x/net/http2"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type streamState uint8

const (
	streamActive    streamState = iota
	streamWriteDone             // EndStream sent
	streamReadDone              // EndStream received
	streamDone                  // the entire stream is finished.
)

// Stream represents an RPC in the transport layer.
type Stream struct {
	id uint32
	// nil for client side Stream.
	st ServerTransport
	// ctx is the associated context of the stream.
	ctx context.Context
	// cancel is always nil for client side Stream.
	cancel context.CancelFunc
	// done is closed when the final status arrives.
	done chan struct{}
	// goAway is closed when the server sent GoAways signal before this stream was initiated.
	goAway chan struct{}
	// method records the associated RPC method of the stream.
	method       string
	recvCompress string
	sendCompress string
	buf          *recvBuffer
	trReader     io.Reader
	fc           *inFlow
	recvQuota    uint32

	// TODO: Remote this unused variable.
	// The accumulated inbound quota pending for window update.
	updateQuota uint32

	// Callback to state application's intentions to read data. This
	// is used to adjust flow control, if need be.
	requestRead func(int)

	sendQuotaPool *quotaPool
	// Close headerChan to indicate the end of reception of header metadata.
	headerChan chan struct{}
	// header caches the received header metadata.
	header metadata.MD
	// The key-value map of trailer metadata.
	trailer metadata.MD

	mu sync.RWMutex // guard the following
	// headerOK becomes true from the first header is about to send.
	headerOk bool
	state    streamState
	// true iff headerChan is closed. Used to avoid closing headerChan
	// multiple times.
	headerDone bool
	// the status error received from the server.
	status *status.Status
	// rstStream indicates whether a RST_STREAM frame needs to be sent
	// to the server to signify that this stream is closing.
	rstStream bool
	// rstError is the error that needs to be sent along with the RST_STREAM frame.
	rstError http2.ErrCode
	// bytesSent and bytesReceived indicates whether any bytes have been sent or
	// received on this stream.
	bytesSent     bool
	bytesReceived bool
}

// RecvCompress returns the compression algorithm applied to the inbound
// message. It is empty string if there is no compression applied.
func (s *Stream) RecvCompress() string {
	return s.recvCompress
}

// SetSendCompress sets the compression algorithm to the stream.
func (s *Stream) SetSendCompress(str string) {
	s.sendCompress = str
}

// Done returns a chanel which is closed when it receives the final status
// from the server.
func (s *Stream) Done() <-chan struct{} {
	return s.done
}

// GoAway returns a channel which is closed when the server sent GoAways signal
// before this stream was initiated.
func (s *Stream) GoAway() <-chan struct{} {
	return s.goAway
}

// Header acquires the key-value pairs of header metadata once it
// is available. It blocks until i) the metadata is ready or ii) there is no
// header metadata or iii) the stream is canceled/expired.
func (s *Stream) Header() (metadata.MD, error) {
	var err error
	select {
	case <-s.ctx.Done():
		err = ContextErr(s.ctx.Err())
	case <-s.goAway:
		err = ErrStreamDrain
	case <-s.headerChan:
		return s.header.Copy(), nil
	}
	// Even if the stream is closed, header is returned if available.
	select {
	case <-s.headerChan:
		return s.header.Copy(), nil
	default:
	}
	return nil, err
}

// Trailer returns the cached trailer metedata. Note that if it is not called
// after the entire stream is done, it could return an empty MD. Client
// side only.
func (s *Stream) Trailer() metadata.MD {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.trailer.Copy()
}

// ServerTransport returns the underlying ServerTransport for the stream.
// The client side stream always returns nil.
func (s *Stream) ServerTransport() ServerTransport {
	return s.st
}

// Context returns the context of the stream.
func (s *Stream) Context() context.Context {
	return s.ctx
}

// Method returns the method for the stream.
func (s *Stream) Method() string {
	return s.method
}

// Status returns the status received from the server.
func (s *Stream) Status() *status.Status {
	return s.status
}

// SetHeader sets the header metadata. This can be called multiple times.
// Server side only.
func (s *Stream) SetHeader(md metadata.MD) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.headerOk || s.state == streamDone {
		return ErrIllegalHeaderWrite
	}
	if md.Len() == 0 {
		return nil
	}
	s.header = metadata.Join(s.header, md)
	return nil
}

// SetTrailer sets the trailer metadata which will be sent with the RPC status
// by the server. This can be called multiple times. Server side only.
func (s *Stream) SetTrailer(md metadata.MD) error {
	if md.Len() == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.trailer = metadata.Join(s.trailer, md)
	return nil
}

func (s *Stream) write(m recvMsg) {
	s.buf.put(m)
}

// Read reads all p bytes from the wire for this stream.
func (s *Stream) Read(p []byte) (n int, err error) {
	// Don't request a read if there was an error earlier
	if er := s.trReader.(*transportReader).er; er != nil {
		return 0, er
	}
	s.requestRead(len(p))
	return io.ReadFull(s.trReader, p)
}
