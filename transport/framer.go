package transport

import (
	"bufio"
	"io"
	"net"
	"sync/atomic"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

type framer struct {
	numWriters int32
	reader     io.Reader
	writer     *bufio.Writer
	fr         *http2.Framer
}

func newFramer(conn net.Conn) *framer {
	f := &framer{
		reader: bufio.NewReaderSize(conn, http2IOBufSize),
		writer: bufio.NewWriterSize(conn, http2IOBufSize),
	}
	f.fr = http2.NewFramer(f.writer, f.reader)
	// Opt-in to Frame reuse API on framer to reduce garbage.
	// Frames aren't safe to read from after a subsequent call to ReadFrame.
	f.fr.SetReuseFrames()
	f.fr.ReadMetaHeaders = hpack.NewDecoder(http2InitHeaderTableSize, nil)
	return f
}

func (f *framer) adjustNumWriters(i int32) int32 {
	return atomic.AddInt32(&f.numWriters, i)
}

// The following writeXXX functions can only be called when the caller gets
// unblocked from writableChan channel (i.e., owns the privilege to write).

func (f *framer) writeContinuation(forceFlush bool, streamID uint32, endHeaders bool, headerBlockFragment []byte) error {
	if err := f.fr.WriteContinuation(streamID, endHeaders, headerBlockFragment); err != nil {
		return err
	}
	if forceFlush {
		return f.writer.Flush()
	}
	return nil
}

func (f *framer) writeData(forceFlush bool, streamID uint32, endStream bool, data []byte) error {
	if err := f.fr.WriteData(streamID, endStream, data); err != nil {
		return err
	}
	if forceFlush {
		return f.writer.Flush()
	}
	return nil
}

func (f *framer) writeGoAway(forceFlush bool, maxStreamID uint32, code http2.ErrCode, debugData []byte) error {
	if err := f.fr.WriteGoAway(maxStreamID, code, debugData); err != nil {
		return err
	}
	if forceFlush {
		return f.writer.Flush()
	}
	return nil
}

func (f *framer) writeHeaders(forceFlush bool, p http2.HeadersFrameParam) error {
	if err := f.fr.WriteHeaders(p); err != nil {
		return err
	}
	if forceFlush {
		return f.writer.Flush()
	}
	return nil
}

func (f *framer) writePing(forceFlush, ack bool, data [8]byte) error {
	if err := f.fr.WritePing(ack, data); err != nil {
		return err
	}
	if forceFlush {
		return f.writer.Flush()
	}
	return nil
}

func (f *framer) writePriority(forceFlush bool, streamID uint32, p http2.PriorityParam) error {
	if err := f.fr.WritePriority(streamID, p); err != nil {
		return err
	}
	if forceFlush {
		return f.writer.Flush()
	}
	return nil
}

func (f *framer) writePushPromise(forceFlush bool, p http2.PushPromiseParam) error {
	if err := f.fr.WritePushPromise(p); err != nil {
		return err
	}
	if forceFlush {
		return f.writer.Flush()
	}
	return nil
}

func (f *framer) writeRSTStream(forceFlush bool, streamID uint32, code http2.ErrCode) error {
	if err := f.fr.WriteRSTStream(streamID, code); err != nil {
		return err
	}
	if forceFlush {
		return f.writer.Flush()
	}
	return nil
}

func (f *framer) writeSettings(forceFlush bool, settings ...http2.Setting) error {
	if err := f.fr.WriteSettings(settings...); err != nil {
		return err
	}
	if forceFlush {
		return f.writer.Flush()
	}
	return nil
}

func (f *framer) writeSettingsAck(forceFlush bool) error {
	if err := f.fr.WriteSettingsAck(); err != nil {
		return err
	}
	if forceFlush {
		return f.writer.Flush()
	}
	return nil
}

func (f *framer) writeWindowUpdate(forceFlush bool, streamID, incr uint32) error {
	if err := f.fr.WriteWindowUpdate(streamID, incr); err != nil {
		return err
	}
	if forceFlush {
		return f.writer.Flush()
	}
	return nil
}

func (f *framer) flushWrite() error {
	return f.writer.Flush()
}

func (f *framer) readFrame() (http2.Frame, error) {
	return f.fr.ReadFrame()
}

func (f *framer) errorDetail() error {
	return f.fr.ErrorDetail()
}
