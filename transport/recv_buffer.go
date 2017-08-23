package transport

import "errors"

// recvBufferReaderOptimized implements io.Reader interface to read the data from
// recvBuffer.
type recvBufferReaderOptimized struct {
	closeCh <-chan struct{}
	goAway  chan struct{}
	recv    *recvBuffer
	last    []byte // Stores the remaining data in the previous calls.
	err     error
}

// Read reads the next len(p) bytes from last. If last is drained, it tries to
// read additional data from recv. It blocks if there no additional data available
// in recv. If Read returns any non-nil error, it will continue to return that error.
func (r *recvBufferReaderOptimized) Read(p []byte) (n int, err error) {
	if r.err != nil {
		return 0, r.err
	}
	n, r.err = r.read(p)
	return n, r.err
}

func (r *recvBufferReaderOptimized) read(p []byte) (n int, err error) {
	if r.last != nil && len(r.last) > 0 {
		// Read remaining data left in last call.
		copied := copy(p, r.last)
		r.last = r.last[copied:]
		return copied, nil
	}
	select {
	case <-r.closeCh:
		return 0, errors.New("FIXME(irfansharif)")
	case <-r.goAway:
		return 0, ErrStreamDrain
	case m := <-r.recv.get():
		r.recv.load()
		if m.err != nil {
			return 0, m.err
		}
		copied := copy(p, m.data)
		r.last = m.data[copied:]
		return copied, nil
	}
}
