/*
 *
 * Copyright 2014 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package grpc

import (
	"bytes"
	"io"
	"sync"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/transport"
)

// serverStreamOptimized implements a server side Stream.
type serverStreamOptimized struct {
	t  transport.ServerTransportOptimized
	s  *transport.StreamOptimized
	p  *parser
	cp Compressor
	dc Decompressor

	cbuf  *bytes.Buffer
	codec Codec

	maxReceiveMessageSize int
	maxSendMessageSize    int
	statsHandler          stats.Handler

	mu struct {
		sync.Mutex
		tracer *tracerInfo
	}
}

func (ss *serverStreamOptimized) Context() context.Context {
	return ss.s.Context()
}

func (ss *serverStreamOptimized) SendHeader(md metadata.MD) error {
	return ss.t.WriteHeader(ss.s, md)
}

func (ss *serverStreamOptimized) SetHeader(md metadata.MD) error {
	if md.Len() == 0 {
		return nil
	}
	return ss.s.SetHeader(md)
}

func (ss *serverStreamOptimized) SetTrailer(md metadata.MD) {
	if md.Len() == 0 {
		return
	}
	ss.s.SetTrailer(md)
	return
}

func (ss *serverStreamOptimized) SendMsg(m interface{}) (err error) {
	defer func() {
		ss.mu.Lock()
		if ss.mu.tracer != nil && ss.mu.tracer.tr != nil {
			if err == nil {
				ss.mu.tracer.tr.LazyLog(&payload{sent: true, msg: m}, true)
			} else {
				ss.mu.tracer.tr.LazyPrintf(err.Error())
				ss.mu.tracer.tr.SetError()
			}
		}
		ss.mu.Unlock()
		if err != nil && err != io.EOF {
			st, _ := status.FromError(toRPCErr(err))
			ss.t.WriteStatus(ss.s, st)
		}
	}()
	var outPayload *stats.OutPayload
	if ss.statsHandler != nil {
		outPayload = &stats.OutPayload{}
	}
	out, err := encode(ss.codec, m, ss.cp, ss.cbuf, outPayload)
	defer func() {
		if ss.cbuf != nil {
			ss.cbuf.Reset()
		}
	}()
	if err != nil {
		return err
	}
	if len(out) > ss.maxSendMessageSize {
		return Errorf(codes.ResourceExhausted, "trying to send message larger than max (%d vs. %d)", len(out), ss.maxSendMessageSize)
	}
	if err := ss.t.Write(ss.s, out, &transport.Options{Last: false}); err != nil {
		return toRPCErr(err)
	}
	if outPayload != nil {
		outPayload.SentTime = time.Now()
		ss.statsHandler.HandleRPC(ss.s.Context(), outPayload)
	}
	return nil
}

func (ss *serverStreamOptimized) RecvMsg(m interface{}) (err error) {
	defer func() {
		ss.mu.Lock()
		if ss.mu.tracer != nil && ss.mu.tracer.tr != nil {
			if err == nil {
				ss.mu.tracer.tr.LazyLog(&payload{sent: false, msg: m}, true)
			} else if err != io.EOF {
				ss.mu.tracer.tr.LazyPrintf(err.Error())
				ss.mu.tracer.tr.SetError()
			}
		}
		ss.mu.Unlock()
	}()
	var inPayload *stats.InPayload
	if ss.statsHandler != nil {
		inPayload = &stats.InPayload{}
	}
	if err := recvOptimized(ss.p, ss.codec, ss.s, ss.dc, m, ss.maxReceiveMessageSize, inPayload); err != nil {
		if err == io.EOF {
			return err
		}
		if err == io.ErrUnexpectedEOF {
			err = Errorf(codes.Internal, io.ErrUnexpectedEOF.Error())
		}
		err = toRPCErr(err)
		st, _ := status.FromError(err)
		ss.t.WriteStatus(ss.s, st)
		return err
	}
	if inPayload != nil {
		ss.statsHandler.HandleRPC(ss.s.Context(), inPayload)
	}
	return nil
}
