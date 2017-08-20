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
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/context"
	"golang.org/x/net/http2"
	"golang.org/x/net/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/internal"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/transport"
)

// Server is a gRPC server to serve RPC requests.
//
// FIXME(irfansharif): Document each member more thoroughly.
// FIXME(irfansharif): Think about how {Graceful,}Stop interoperate using
// sync.Cond and nil maps.
type Server struct {
	opts options

	// Locking notes: To avoid deadlocks the following lock order must be
	// obeyed: Server.mu < Server.eventsMu.
	//
	// (It is not required to acquire every lock in sequence, but when multiple
	// locks are held at the same time, it is incorrect to acquire a lock with
	// a "lesser" value in the sequence after one with "greater" value.)

	mu struct {
		sync.Mutex
		listeners map[net.Listener]struct{}
		conns     map[io.Closer]struct{}
		serving   bool
		drain     bool
		// A CondVar to let GracefulStop() blocks until all the pending RPCs are finished
		// and all the transport goes away.
		cv *sync.Cond

		// This should be used instead of checking if Server.mu.conns == nil or
		// Server.mu.listeners == nil.
		stopCh chan struct{}
	}

	// FIXME(irfansharif): Talk about lifetime, initialized/written to only
	// during registration/pre-Server.Start.
	services map[string]*service // service name -> service info
	eventsMu struct {
		sync.Mutex
		log trace.EventLog
	}
}

// NewServer creates a gRPC server which has no service registered and has not
// started to accept requests yet.
func NewServer(opt ...ServerOption) *Server {
	opts := defaultServerOptions()
	for _, o := range opt {
		o(&opts)
	}
	s := &Server{
		opts:     opts,
		services: make(map[string]*service),
	}
	s.mu.listeners = make(map[net.Listener]struct{})
	s.mu.conns = make(map[io.Closer]struct{})
	s.mu.cv = sync.NewCond(&s.mu)
	s.mu.stopCh = make(chan struct{})

	if EnableTracing {
		_, file, line, _ := runtime.Caller(1)
		s.eventsMu.log = trace.NewEventLog("grpc.Server", fmt.Sprintf("%s:%d", file, line))
	}
	return s
}

// RegisterService registers a service and its implementation to the gRPC
// server. It is called from the IDL generated code. This must be called before
// invoking Serve.
func (s *Server) RegisterService(sd *ServiceDesc, ss interface{}) {
	ht := reflect.TypeOf(sd.HandlerType).Elem()
	st := reflect.TypeOf(ss)
	if !st.Implements(ht) {
		grpclog.Fatalf("grpc: Server.RegisterService found the handler of type %v that does not satisfy %v", st, ht)
	}
	s.register(sd, ss)
}

var (
	// ErrServerStopped indicates that the operation is now illegal because of
	// the server being stopped.
	ErrServerStopped = errors.New("grpc: the server has been stopped")
)

// Serve accepts incoming connections on the listener lis, creating a new
// ServerTransport and service goroutine for each. The service goroutines
// read gRPC requests and then call the registered handlers to reply to them.
// Serve returns when lis.Accept fails with fatal errors. lis will be closed when
// this method returns.
//
// Serve always returns non-nil error.
func (s *Server) Serve(lis net.Listener) error {
	s.mu.Lock()
	s.logEventf("serving")
	s.mu.serving = true

	if s.mu.stopCh == nil {
		// We've already been stopped.
		s.mu.Unlock()
		lis.Close()
		return ErrServerStopped
	}
	// FIXME(irfansharif): Comment how we're holding to a reference.
	stopCh := s.mu.stopCh
	s.mu.listeners[lis] = struct{}{}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.mu.stopCh == nil {
			return
		}
		lis.Close()
		delete(s.mu.listeners, lis)
	}()

	// For temporary errors we back-off exponentially.
	const minWait time.Duration = 5 * time.Millisecond
	const maxWait time.Duration = time.Second
	waitTime := minWait

	t := time.NewTimer(0)
	// FIXME(irfansharif): To ensure we don't leak resources. Is this true?
	defer t.Stop()

	// Drain the timer so we can re-use it below in the tight loop.
	// FIXME(irfansharif): Explain this pattern.
	<-t.C

	for {
		conn, err := lis.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				waitTime *= 2
				if waitTime > maxWait {
					waitTime = maxWait
				}

				s.logEventf("Accept error: %v; retrying in %v", err, waitTime)

				t.Reset(waitTime)
				select {
				case <-t.C:
				case <-stopCh:
				}
				continue
			}

			s.logEventf("Done serving; Accept = %v", err)
			return err
		}
		// We reset waitTime.
		waitTime = minWait

		// Start a new goroutine to deal with the accepted net.Conn so we don't
		// stall this listener.Accept loop goroutine.
		//
		// FIXME(irfansharif): This is the point of divergence from the
		// existing implementation. Put this thing behind some flag so we can
		// just fall back to default implementation.
		go s.handleConn(conn)
	}
}

// handleConn is run in its own goroutine and handles a just-accepted
// connection that has not had any I/O performed on it yet.
func (s *Server) handleConn(c net.Conn) {
	conn, authInfo, err := s.authenticate(c)
	if err != nil {
		s.logErrorf("ServerHandshake(%q) failed: %v", c.RemoteAddr(), err)
		grpclog.Warningf("grpc: Server.Serve failed to complete security handshake from %q: %v", c.RemoteAddr(), err)

		// If serverHandShake returns ErrConnDispatched, keep rawConn open.
		if err != credentials.ErrConnDispatched {
			c.Close()
		}
		return
	}

	s.serveHTTP2TransportOptimized(conn, authInfo)
}

// handleRawConn is run in its own goroutine and handles a just-accepted
// connection that has not had any I/O performed on it yet.
func (s *Server) handleRawConn(rawConn net.Conn) {
	conn, authInfo, err := s.authenticate(rawConn)
	if err != nil {
		s.logErrorf("ServerHandshake(%q) failed: %v", rawConn.RemoteAddr(), err)
		grpclog.Warningf("grpc: Server.Serve failed to complete security handshake from %q: %v", rawConn.RemoteAddr(), err)
		// If serverHandShake returns ErrConnDispatched, keep rawConn open.
		if err != credentials.ErrConnDispatched {
			rawConn.Close()
		}
		return
	}

	if s.opts.useHandlerImpl {
		s.serveUsingHandler(conn)
	} else {
		s.serveHTTP2Transport(conn, authInfo)
	}
}

// serveHTTP2Transport sets up a HTTP/2 transport (using the gRPC HTTP2 server
// transport in transport/http2_server_optimized.go) and serves streams on it.
// This is run in its own goroutine (it does network I/O in
// transport.NewServerTransport).
func (s *Server) serveHTTP2TransportOptimized(conn net.Conn, authInfo credentials.AuthInfo) {
	config := &transport.ServerConfig{
		MaxStreams:            s.opts.maxConcurrentStreams,
		AuthInfo:              authInfo,
		InTapHandle:           s.opts.inTapHandle,
		StatsHandler:          s.opts.statsHandler,
		KeepaliveParams:       s.opts.keepaliveParams,
		KeepalivePolicy:       s.opts.keepalivePolicy,
		InitialWindowSize:     s.opts.initialWindowSize,
		InitialConnWindowSize: s.opts.initialConnWindowSize,
	}
	st, err := transport.NewServerTransportOptimized("http2", conn, config)
	if err != nil {
		s.logErrorf("NewServerTransport(%q) failed: %v", conn.RemoteAddr(), err)
		conn.Close()
		grpclog.Warningln("grpc: Server.Serve failed to create ServerTransport: ", err)
		return
	}
	if !s.addConn(st) {
		st.Close()
		return
	}
	s.serveStreamsOptimized(st)
}

// serveHTTP2Transport sets up a http/2 transport (using the
// gRPC http2 server transport in transport/http2_server.go) and
// serves streams on it.
// This is run in its own goroutine (it does network I/O in
// transport.NewServerTransport).
func (s *Server) serveHTTP2Transport(c net.Conn, authInfo credentials.AuthInfo) {
	config := &transport.ServerConfig{
		MaxStreams:            s.opts.maxConcurrentStreams,
		AuthInfo:              authInfo,
		InTapHandle:           s.opts.inTapHandle,
		StatsHandler:          s.opts.statsHandler,
		KeepaliveParams:       s.opts.keepaliveParams,
		KeepalivePolicy:       s.opts.keepalivePolicy,
		InitialWindowSize:     s.opts.initialWindowSize,
		InitialConnWindowSize: s.opts.initialConnWindowSize,
	}
	st, err := transport.NewServerTransport("http2", c, config)
	if err != nil {
		s.logErrorf("NewServerTransport(%q) failed: %v", c.RemoteAddr(), err)
		c.Close()
		grpclog.Warningln("grpc: Server.Serve failed to create ServerTransport: ", err)
		return
	}
	if !s.addConn(st) {
		st.Close()
		return
	}
	s.serveStreams(st)
}

func (s *Server) serveStreamsOptimized(st transport.ServerTransportOptimized) {
	var wg sync.WaitGroup
	handler := func(stream *transport.StreamOptimized) {
		wg.Add(1)
		go func() {
			s.handleStreamOptimized(st, stream, s.tracerOptimized(st, stream))
			wg.Done()
		}()
	}
	tctx := func(ctx context.Context, method string) context.Context {
		if !EnableTracing {
			return ctx
		}
		tr := trace.New(fmt.Sprintf("grpc.Recv.%s", methodFamily(method)), method)
		return trace.NewContext(ctx, tr)
	}

	st.HandleStreams(handler, tctx)
	wg.Wait()

	st.Close()
	s.removeConn(st)
}

func (s *Server) serveStreams(st transport.ServerTransport) {
	defer s.removeConn(st)
	defer st.Close()
	var wg sync.WaitGroup
	st.HandleStreams(func(stream *transport.Stream) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.handleStream(st, stream, s.tracer(st, stream))
		}()
	}, func(ctx context.Context, method string) context.Context {
		if !EnableTracing {
			return ctx
		}
		tr := trace.New("grpc.Recv."+methodFamily(method), method)
		return trace.NewContext(ctx, tr)
	})
	wg.Wait()
}

var _ http.Handler = (*Server)(nil)

// serveUsingHandler is called from handleRawConn when s is configured
// to handle requests via the http.Handler interface. It sets up a
// net/http.Server to handle the just-accepted conn. The http.Server
// is configured to route all incoming requests (all HTTP/2 streams)
// to ServeHTTP, which creates a new ServerTransport for each stream.
// serveUsingHandler blocks until conn closes.
//
// This codepath is only used when Server.TestingUseHandlerImpl has
// been configured. This lets the end2end tests exercise the ServeHTTP
// method as one of the environment types.
//
// conn is the *tls.Conn that's already been authenticated.
func (s *Server) serveUsingHandler(conn net.Conn) {
	if !s.addConn(conn) {
		conn.Close()
		return
	}
	defer s.removeConn(conn)
	h2s := &http2.Server{
		MaxConcurrentStreams: s.opts.maxConcurrentStreams,
	}
	h2s.ServeConn(conn, &http2.ServeConnOpts{
		Handler: s,
	})
}

// ServeHTTP implements the Go standard library's http.Handler
// interface by responding to the gRPC request r, by looking up
// the requested gRPC method in the gRPC server s.
//
// The provided HTTP request must have arrived on an HTTP/2
// connection. When using the Go standard library's server,
// practically this means that the Request must also have arrived
// over TLS.
//
// To share one port (such as 443 for https) between gRPC and an
// existing http.Handler, use a root http.Handler such as:
//
//   if r.ProtoMajor == 2 && strings.HasPrefix(
//   	r.Header.Get("Content-Type"), "application/grpc") {
//   	grpcServer.ServeHTTP(w, r)
//   } else {
//   	yourMux.ServeHTTP(w, r)
//   }
//
// Note that ServeHTTP uses Go's HTTP/2 server implementation which is totally
// separate from grpc-go's HTTP/2 server. Performance and features may vary
// between the two paths. ServeHTTP does not support some gRPC features
// available through grpc-go's HTTP/2 server, and it is currently EXPERIMENTAL
// and subject to change.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	st, err := transport.NewServerHandlerTransport(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !s.addConn(st) {
		st.Close()
		return
	}
	defer s.removeConn(st)
	s.serveStreams(st)
}

// traceInfo returns a traceInfo and associates it with stream, if tracing is enabled.
// If tracing is not enabled, it returns nil.
func (s *Server) tracerOptimized(st transport.ServerTransportOptimized, stream *transport.StreamOptimized) (tracer *tracerInfo) {
	tr, ok := trace.FromContext(stream.Context())
	if !ok {
		return nil
	}

	tracer = &tracerInfo{
		tr: tr,
	}
	tracer.firstLine.client = false
	tracer.firstLine.remoteAddr = st.RemoteAddr()

	if dl, ok := stream.Context().Deadline(); ok {
		tracer.firstLine.deadline = dl.Sub(time.Now())
	}
	return tracer
}

// tracer returns a tracer and associates it with stream, if tracing is enabled.
// If tracing is not enabled, it returns nil.
func (s *Server) tracer(st transport.ServerTransport, stream *transport.Stream) (tracer *tracerInfo) {
	tr, ok := trace.FromContext(stream.Context())
	if !ok {
		return nil
	}

	tracer = &tracerInfo{
		tr: tr,
	}
	tracer.firstLine.client = false
	tracer.firstLine.remoteAddr = st.RemoteAddr()

	if dl, ok := stream.Context().Deadline(); ok {
		tracer.firstLine.deadline = dl.Sub(time.Now())
	}
	return tracer
}

func (s *Server) addConn(c io.Closer) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.conns == nil || s.mu.drain {
		return false
	}
	s.mu.conns[c] = struct{}{}
	return true
}

func (s *Server) removeConn(c io.Closer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.conns != nil {
		delete(s.mu.conns, c)
		s.mu.cv.Signal()
	}
}

func (s *Server) sendResponseOptimized(t transport.ServerTransportOptimized, stream *transport.StreamOptimized, msg interface{}, cp Compressor, opts *transport.Options) error {
	var (
		cbuf       *bytes.Buffer
		outPayload *stats.OutPayload
	)
	if cp != nil {
		cbuf = new(bytes.Buffer)
	}
	if s.opts.statsHandler != nil {
		outPayload = &stats.OutPayload{}
	}
	p, err := encode(s.opts.codec, msg, cp, cbuf, outPayload)
	if err != nil {
		grpclog.Errorln("grpc: server failed to encode response: ", err)
		return err
	}
	if len(p) > s.opts.maxSendMessageSize {
		return status.Errorf(codes.ResourceExhausted, "grpc: trying to send message larger than max (%d vs. %d)", len(p), s.opts.maxSendMessageSize)
	}
	err = t.Write(stream, p, opts)
	if err == nil && outPayload != nil {
		outPayload.SentTime = time.Now()
		s.opts.statsHandler.HandleRPC(stream.Context(), outPayload)
	}
	return err
}

func (s *Server) sendResponse(t transport.ServerTransport, stream *transport.Stream, msg interface{}, cp Compressor, opts *transport.Options) error {
	var (
		cbuf       *bytes.Buffer
		outPayload *stats.OutPayload
	)
	if cp != nil {
		cbuf = new(bytes.Buffer)
	}
	if s.opts.statsHandler != nil {
		outPayload = &stats.OutPayload{}
	}
	p, err := encode(s.opts.codec, msg, cp, cbuf, outPayload)
	if err != nil {
		grpclog.Errorln("grpc: server failed to encode response: ", err)
		return err
	}
	if len(p) > s.opts.maxSendMessageSize {
		return status.Errorf(codes.ResourceExhausted, "grpc: trying to send message larger than max (%d vs. %d)", len(p), s.opts.maxSendMessageSize)
	}
	err = t.Write(stream, p, opts)
	if err == nil && outPayload != nil {
		outPayload.SentTime = time.Now()
		s.opts.statsHandler.HandleRPC(stream.Context(), outPayload)
	}
	return err
}

func (s *Server) processUnaryRPCOptimized(t transport.ServerTransportOptimized, stream *transport.StreamOptimized, srv *service, md *MethodDesc, tracer *tracerInfo) (err error) {
	sh := s.opts.statsHandler
	if sh != nil {
		begin := &stats.Begin{
			BeginTime: time.Now(),
		}
		sh.HandleRPC(stream.Context(), begin)
		defer func() {
			end := &stats.End{
				EndTime: time.Now(),
			}
			if err != nil && err != io.EOF {
				end.Error = toRPCErr(err)
			}
			sh.HandleRPC(stream.Context(), end)
		}()
	}
	if tracer != nil {
		defer tracer.tr.Finish()
		tracer.firstLine.client = false
		tracer.tr.LazyPrintf(tracer.firstLine.String())
		defer func() {
			if err != nil && err != io.EOF {
				tracer.tr.LazyPrintf(err.Error())
				tracer.tr.SetError()
			}
		}()
	}
	if s.opts.cp != nil {
		// NOTE: this needs to be ahead of all handling, https://github.com/grpc/grpc-go/issues/686.
		stream.SetSendCompressionAlgorithm(s.opts.cp.Type())
	}
	p := &parser{r: stream}
	pf, req, err := p.recvMsg(s.opts.maxReceiveMessageSize)
	if err == io.EOF {
		// The entire stream is done (for unary RPC only).
		return err
	}
	if err == io.ErrUnexpectedEOF {
		err = Errorf(codes.Internal, io.ErrUnexpectedEOF.Error())
	}
	if err != nil {
		if st, ok := status.FromError(err); ok {
			if e := t.WriteStatus(stream, st); e != nil {
				grpclog.Warningf("grpc: Server.processUnaryRPC failed to write status %v", e)
			}
		} else {
			switch st := err.(type) {
			case transport.ConnectionError:
				// Nothing to do here.
			case transport.StreamError:
				if e := t.WriteStatus(stream, status.New(st.Code, st.Desc)); e != nil {
					grpclog.Warningf("grpc: Server.processUnaryRPC failed to write status %v", e)
				}
			default:
				panic(fmt.Sprintf("grpc: Unexpected error (%T) from recvMsg: %v", st, st))
			}
		}
		return err
	}

	if err := checkRecvPayload(pf, stream.RecvCompressionAlgorithm(), s.opts.dc); err != nil {
		if st, ok := status.FromError(err); ok {
			if e := t.WriteStatus(stream, st); e != nil {
				grpclog.Warningf("grpc: Server.processUnaryRPC failed to write status %v", e)
			}
			return err
		}
		if e := t.WriteStatus(stream, status.New(codes.Internal, err.Error())); e != nil {
			grpclog.Warningf("grpc: Server.processUnaryRPC failed to write status %v", e)
		}

		// TODO checkRecvPayload always return RPC error. Add a return here if necessary.
	}
	var inPayload *stats.InPayload
	if sh != nil {
		inPayload = &stats.InPayload{
			RecvTime: time.Now(),
		}
	}
	df := func(v interface{}) error {
		if inPayload != nil {
			inPayload.WireLength = len(req)
		}
		if pf == compressionMade {
			var err error
			req, err = s.opts.dc.Do(bytes.NewReader(req))
			if err != nil {
				return Errorf(codes.Internal, err.Error())
			}
		}
		if len(req) > s.opts.maxReceiveMessageSize {
			// TODO: Revisit the error code. Currently keep it consistent with
			// java implementation.
			return status.Errorf(codes.ResourceExhausted, "grpc: received message larger than max (%d vs. %d)", len(req), s.opts.maxReceiveMessageSize)
		}
		if err := s.opts.codec.Unmarshal(req, v); err != nil {
			return status.Errorf(codes.Internal, "grpc: error unmarshalling request: %v", err)
		}
		if inPayload != nil {
			inPayload.Payload = v
			inPayload.Data = req
			inPayload.Length = len(req)
			sh.HandleRPC(stream.Context(), inPayload)
		}
		if tracer != nil {
			tracer.tr.LazyLog(&payload{sent: false, msg: v}, true)
		}
		return nil
	}
	reply, appErr := md.Handler(srv.server, stream.Context(), df, s.opts.unaryInt)
	if appErr != nil {
		appStatus, ok := status.FromError(appErr)
		if !ok {
			// Convert appErr if it is not a grpc status error.
			appErr = status.Error(convertCode(appErr), appErr.Error())
			appStatus, _ = status.FromError(appErr)
		}
		if e := t.WriteStatus(stream, appStatus); e != nil {
			grpclog.Warningf("grpc: Server.processUnaryRPC failed to write status: %v", e)
		}
		return appErr
	}
	if tracer != nil {
		tracer.tr.LazyPrintf("OK")
	}
	opts := &transport.Options{
		Last:  true,
		Delay: false,
	}
	if err := s.sendResponseOptimized(t, stream, reply, s.opts.cp, opts); err != nil {
		if err == io.EOF {
			// The entire stream is done (for unary RPC only).
			return err
		}
		if s, ok := status.FromError(err); ok {
			if e := t.WriteStatus(stream, s); e != nil {
				grpclog.Warningf("grpc: Server.processUnaryRPC failed to write status: %v", e)
			}
		} else {
			switch st := err.(type) {
			case transport.ConnectionError:
				// Nothing to do here.
			case transport.StreamError:
				if e := t.WriteStatus(stream, status.New(st.Code, st.Desc)); e != nil {
					grpclog.Warningf("grpc: Server.processUnaryRPC failed to write status %v", e)
				}
			default:
				panic(fmt.Sprintf("grpc: Unexpected error (%T) from sendResponse: %v", st, st))
			}
		}
		return err
	}
	if tracer != nil {
		tracer.tr.LazyLog(&payload{sent: true, msg: reply}, true)
	}
	// TODO: Should we be logging if writing status failed here, like above?
	// Should the logging be in WriteStatus?  Should we ignore the WriteStatus
	// error or allow the stats handler to see it?
	return t.WriteStatus(stream, status.New(codes.OK, ""))
}

func (s *Server) processUnaryRPC(t transport.ServerTransport, stream *transport.Stream, srv *service, md *MethodDesc, tracer *tracerInfo) (err error) {
	sh := s.opts.statsHandler
	if sh != nil {
		begin := &stats.Begin{
			BeginTime: time.Now(),
		}
		sh.HandleRPC(stream.Context(), begin)
		defer func() {
			end := &stats.End{
				EndTime: time.Now(),
			}
			if err != nil && err != io.EOF {
				end.Error = toRPCErr(err)
			}
			sh.HandleRPC(stream.Context(), end)
		}()
	}
	if tracer != nil {
		defer tracer.tr.Finish()
		tracer.firstLine.client = false
		tracer.tr.LazyPrintf(tracer.firstLine.String())
		defer func() {
			if err != nil && err != io.EOF {
				tracer.tr.LazyPrintf(err.Error())
				tracer.tr.SetError()
			}
		}()
	}
	if s.opts.cp != nil {
		// NOTE: this needs to be ahead of all handling, https://github.com/grpc/grpc-go/issues/686.
		stream.SetSendCompress(s.opts.cp.Type())
	}
	p := &parser{r: stream}
	pf, req, err := p.recvMsg(s.opts.maxReceiveMessageSize)
	if err == io.EOF {
		// The entire stream is done (for unary RPC only).
		return err
	}
	if err == io.ErrUnexpectedEOF {
		err = Errorf(codes.Internal, io.ErrUnexpectedEOF.Error())
	}
	if err != nil {
		if st, ok := status.FromError(err); ok {
			if e := t.WriteStatus(stream, st); e != nil {
				grpclog.Warningf("grpc: Server.processUnaryRPC failed to write status %v", e)
			}
		} else {
			switch st := err.(type) {
			case transport.ConnectionError:
				// Nothing to do here.
			case transport.StreamError:
				if e := t.WriteStatus(stream, status.New(st.Code, st.Desc)); e != nil {
					grpclog.Warningf("grpc: Server.processUnaryRPC failed to write status %v", e)
				}
			default:
				panic(fmt.Sprintf("grpc: Unexpected error (%T) from recvMsg: %v", st, st))
			}
		}
		return err
	}

	if err := checkRecvPayload(pf, stream.RecvCompress(), s.opts.dc); err != nil {
		if st, ok := status.FromError(err); ok {
			if e := t.WriteStatus(stream, st); e != nil {
				grpclog.Warningf("grpc: Server.processUnaryRPC failed to write status %v", e)
			}
			return err
		}
		if e := t.WriteStatus(stream, status.New(codes.Internal, err.Error())); e != nil {
			grpclog.Warningf("grpc: Server.processUnaryRPC failed to write status %v", e)
		}

		// TODO checkRecvPayload always return RPC error. Add a return here if necessary.
	}
	var inPayload *stats.InPayload
	if sh != nil {
		inPayload = &stats.InPayload{
			RecvTime: time.Now(),
		}
	}
	df := func(v interface{}) error {
		if inPayload != nil {
			inPayload.WireLength = len(req)
		}
		if pf == compressionMade {
			var err error
			req, err = s.opts.dc.Do(bytes.NewReader(req))
			if err != nil {
				return Errorf(codes.Internal, err.Error())
			}
		}
		if len(req) > s.opts.maxReceiveMessageSize {
			// TODO: Revisit the error code. Currently keep it consistent with
			// java implementation.
			return status.Errorf(codes.ResourceExhausted, "grpc: received message larger than max (%d vs. %d)", len(req), s.opts.maxReceiveMessageSize)
		}
		if err := s.opts.codec.Unmarshal(req, v); err != nil {
			return status.Errorf(codes.Internal, "grpc: error unmarshalling request: %v", err)
		}
		if inPayload != nil {
			inPayload.Payload = v
			inPayload.Data = req
			inPayload.Length = len(req)
			sh.HandleRPC(stream.Context(), inPayload)
		}
		if tracer != nil {
			tracer.tr.LazyLog(&payload{sent: false, msg: v}, true)
		}
		return nil
	}
	reply, appErr := md.Handler(srv.server, stream.Context(), df, s.opts.unaryInt)
	if appErr != nil {
		appStatus, ok := status.FromError(appErr)
		if !ok {
			// Convert appErr if it is not a grpc status error.
			appErr = status.Error(convertCode(appErr), appErr.Error())
			appStatus, _ = status.FromError(appErr)
		}
		if tracer != nil {
			tracer.tr.LazyPrintf(appStatus.Message())
			tracer.tr.SetError()
		}
		if e := t.WriteStatus(stream, appStatus); e != nil {
			grpclog.Warningf("grpc: Server.processUnaryRPC failed to write status: %v", e)
		}
		return appErr
	}
	if tracer != nil {
		tracer.tr.LazyPrintf("OK")
	}
	opts := &transport.Options{
		Last:  true,
		Delay: false,
	}
	if err := s.sendResponse(t, stream, reply, s.opts.cp, opts); err != nil {
		if err == io.EOF {
			// The entire stream is done (for unary RPC only).
			return err
		}
		if s, ok := status.FromError(err); ok {
			if e := t.WriteStatus(stream, s); e != nil {
				grpclog.Warningf("grpc: Server.processUnaryRPC failed to write status: %v", e)
			}
		} else {
			switch st := err.(type) {
			case transport.ConnectionError:
				// Nothing to do here.
			case transport.StreamError:
				if e := t.WriteStatus(stream, status.New(st.Code, st.Desc)); e != nil {
					grpclog.Warningf("grpc: Server.processUnaryRPC failed to write status %v", e)
				}
			default:
				panic(fmt.Sprintf("grpc: Unexpected error (%T) from sendResponse: %v", st, st))
			}
		}
		return err
	}
	if tracer != nil {
		tracer.tr.LazyLog(&payload{sent: true, msg: reply}, true)
	}
	// TODO: Should we be logging if writing status failed here, like above?
	// Should the logging be in WriteStatus?  Should we ignore the WriteStatus
	// error or allow the stats handler to see it?
	return t.WriteStatus(stream, status.New(codes.OK, ""))
}

func (s *Server) processStreamingRPCOptimized(t transport.ServerTransportOptimized, stream *transport.StreamOptimized, srv *service, sd *StreamDesc, tracer *tracerInfo) (err error) {
	sh := s.opts.statsHandler
	if sh != nil {
		begin := &stats.Begin{
			BeginTime: time.Now(),
		}
		sh.HandleRPC(stream.Context(), begin)
		defer func() {
			end := &stats.End{
				EndTime: time.Now(),
			}
			if err != nil && err != io.EOF {
				end.Error = toRPCErr(err)
			}
			sh.HandleRPC(stream.Context(), end)
		}()
	}
	if s.opts.cp != nil {
		stream.SetSendCompressionAlgorithm(s.opts.cp.Type())
	}
	ss := &serverStreamOptimized{
		t:     t,
		s:     stream,
		p:     &parser{r: stream},
		codec: s.opts.codec,
		cp:    s.opts.cp,
		dc:    s.opts.dc,
		maxReceiveMessageSize: s.opts.maxReceiveMessageSize,
		maxSendMessageSize:    s.opts.maxSendMessageSize,
		statsHandler:          sh,
	}
	if ss.cp != nil {
		ss.cbuf = new(bytes.Buffer)
	}
	if tracer != nil {
		ss.mu.tracer = tracer
		ss.mu.tracer.tr.LazyPrintf(tracer.firstLine.String())
		defer func() {
			ss.mu.Lock()
			if err != nil && err != io.EOF {
				ss.mu.tracer.tr.LazyPrintf(err.Error())
				ss.mu.tracer.tr.SetError()
			}
			ss.mu.tracer.tr.Finish()
			ss.mu.tracer.tr = nil
			ss.mu.Unlock()
		}()
	}
	var appErr error
	var server interface{}
	if srv != nil {
		server = srv.server
	}
	if s.opts.streamInt == nil {
		appErr = sd.Handler(server, ss)
	} else {
		info := &StreamServerInfo{
			FullMethod:     stream.Method(),
			IsClientStream: sd.ClientStreams,
			IsServerStream: sd.ServerStreams,
		}
		appErr = s.opts.streamInt(server, ss, info, sd.Handler)
	}
	if appErr != nil {
		appStatus, ok := status.FromError(appErr)
		if !ok {
			switch err := appErr.(type) {
			case transport.StreamError:
				appStatus = status.New(err.Code, err.Desc)
			default:
				appStatus = status.New(convertCode(appErr), appErr.Error())
			}
			appErr = appStatus.Err()
		}
		if tracer != nil {
			ss.mu.Lock()
			ss.mu.tracer.tr.LazyPrintf(appStatus.Message())
			ss.mu.tracer.tr.SetError()
			ss.mu.Unlock()
		}
		t.WriteStatus(ss.s, appStatus)
		// TODO: Should we log an error from WriteStatus here and below?
		return appErr
	}
	if tracer != nil {
		ss.mu.Lock()
		ss.mu.tracer.tr.LazyPrintf("OK")
		ss.mu.Unlock()
	}
	return t.WriteStatus(ss.s, status.New(codes.OK, ""))
}

func (s *Server) processStreamingRPC(t transport.ServerTransport, stream *transport.Stream, srv *service, sd *StreamDesc, tracer *tracerInfo) (err error) {
	sh := s.opts.statsHandler
	if sh != nil {
		begin := &stats.Begin{
			BeginTime: time.Now(),
		}
		sh.HandleRPC(stream.Context(), begin)
		defer func() {
			end := &stats.End{
				EndTime: time.Now(),
			}
			if err != nil && err != io.EOF {
				end.Error = toRPCErr(err)
			}
			sh.HandleRPC(stream.Context(), end)
		}()
	}
	if s.opts.cp != nil {
		stream.SetSendCompress(s.opts.cp.Type())
	}
	ss := &serverStream{
		t:     t,
		s:     stream,
		p:     &parser{r: stream},
		codec: s.opts.codec,
		cp:    s.opts.cp,
		dc:    s.opts.dc,
		maxReceiveMessageSize: s.opts.maxReceiveMessageSize,
		maxSendMessageSize:    s.opts.maxSendMessageSize,
		tracer:                tracer,
		statsHandler:          sh,
	}
	if ss.cp != nil {
		ss.cbuf = new(bytes.Buffer)
	}
	if tracer != nil {
		tracer.tr.LazyPrintf(tracer.firstLine.String())
		defer func() {
			ss.mu.Lock()
			if err != nil && err != io.EOF {
				ss.tracer.tr.LazyPrintf(err.Error())
				ss.tracer.tr.SetError()
			}
			ss.tracer.tr.Finish()
			ss.tracer.tr = nil
			ss.mu.Unlock()
		}()
	}
	var appErr error
	var server interface{}
	if srv != nil {
		server = srv.server
	}
	if s.opts.streamInt == nil {
		appErr = sd.Handler(server, ss)
	} else {
		info := &StreamServerInfo{
			FullMethod:     stream.Method(),
			IsClientStream: sd.ClientStreams,
			IsServerStream: sd.ServerStreams,
		}
		appErr = s.opts.streamInt(server, ss, info, sd.Handler)
	}
	if appErr != nil {
		appStatus, ok := status.FromError(appErr)
		if !ok {
			switch err := appErr.(type) {
			case transport.StreamError:
				appStatus = status.New(err.Code, err.Desc)
			default:
				appStatus = status.New(convertCode(appErr), appErr.Error())
			}
			appErr = appStatus.Err()
		}
		if tracer != nil {
			ss.mu.Lock()
			ss.tracer.tr.LazyPrintf(appStatus.Message())
			ss.tracer.tr.SetError()
			ss.mu.Unlock()
		}
		t.WriteStatus(ss.s, appStatus)
		// TODO: Should we log an error from WriteStatus here and below?
		return appErr
	}
	if tracer != nil {
		ss.mu.Lock()
		ss.tracer.tr.LazyPrintf("OK")
		ss.mu.Unlock()
	}
	return t.WriteStatus(ss.s, status.New(codes.OK, ""))
}

func (s *Server) handleStreamOptimized(t transport.ServerTransportOptimized, stream *transport.StreamOptimized, tracer *tracerInfo) {
	service, method, valid := parseServiceMethod(stream.Method())
	if !valid {
		errDesc := fmt.Sprintf("malformed method name: %q", stream.Method())
		if err := t.WriteStatus(stream, status.New(codes.ResourceExhausted, errDesc)); err != nil {
			grpclog.Warningf("grpc: Server.handleStream failed to write status: %v", err)
		}
		if tracer != nil {
			tracer.tr.LazyPrintf(errDesc)
			tracer.tr.SetError()
			tracer.tr.Finish()
		}
		return
	}
	srv, ok := s.services[service]
	if !ok {
		if unknownDesc := s.opts.unknownStreamDesc; unknownDesc != nil {
			s.processStreamingRPCOptimized(t, stream, nil, unknownDesc, tracer)
			return
		}
		errDesc := fmt.Sprintf("unknown service: %v", service)
		if tracer != nil {
			tracer.tr.LazyPrintf(errDesc)
			tracer.tr.SetError()
		}
		if err := t.WriteStatus(stream, status.New(codes.Unimplemented, errDesc)); err != nil {
			if tracer != nil {
				tracer.tr.LazyPrintf(err.Error())
				tracer.tr.SetError()
			}
			grpclog.Warningf("grpc: Server.handleStream failed to write status: %v", err)
		}
		if tracer != nil {
			tracer.tr.Finish()
		}
		return
	}
	// Unary RPC or Streaming RPC?
	if md, ok := srv.md[method]; ok {
		s.processUnaryRPCOptimized(t, stream, srv, md, tracer)
		return
	}
	if sd, ok := srv.sd[method]; ok {
		s.processStreamingRPCOptimized(t, stream, srv, sd, tracer)
		return
	}
	errDesc := fmt.Sprintf("unknown method: %v", method)
	if tracer != nil {
		tracer.tr.LazyPrintf(errDesc)
		tracer.tr.SetError()
	}
	if unknownDesc := s.opts.unknownStreamDesc; unknownDesc != nil {
		s.processStreamingRPCOptimized(t, stream, nil, unknownDesc, tracer)
		return
	}

	if err := t.WriteStatus(stream, status.New(codes.Unimplemented, errDesc)); err != nil {
		if tracer != nil {
			tracer.tr.LazyPrintf(err.Error())
			tracer.tr.SetError()
		}
		grpclog.Warningf("grpc: Server.handleStream failed to write status: %v", err)
	}
	if tracer != nil {
		tracer.tr.Finish()
	}
}

func (s *Server) handleStream(t transport.ServerTransport, stream *transport.Stream, tracer *tracerInfo) {
	sm := stream.Method()
	if sm != "" && sm[0] == '/' {
		sm = sm[1:]
	}
	pos := strings.LastIndex(sm, "/")
	if pos == -1 {
		errDesc := fmt.Sprintf("malformed method name: %q", stream.Method())
		if tracer != nil {
			tracer.tr.LazyPrintf(errDesc)
			tracer.tr.SetError()
		}
		if err := t.WriteStatus(stream, status.New(codes.ResourceExhausted, errDesc)); err != nil {
			if tracer != nil {
				tracer.tr.LazyPrintf(err.Error())
				tracer.tr.SetError()
			}
			grpclog.Warningf("grpc: Server.handleStream failed to write status: %v", err)
		}
		if tracer != nil {
			tracer.tr.Finish()
		}
		return
	}
	service := sm[:pos]
	method := sm[pos+1:]
	srv, ok := s.services[service]
	if !ok {
		if unknownDesc := s.opts.unknownStreamDesc; unknownDesc != nil {
			s.processStreamingRPC(t, stream, nil, unknownDesc, tracer)
			return
		}
		errDesc := fmt.Sprintf("unknown service: %v", service)
		if tracer != nil {
			tracer.tr.LazyPrintf(errDesc)
			tracer.tr.SetError()
		}
		if err := t.WriteStatus(stream, status.New(codes.Unimplemented, errDesc)); err != nil {
			if tracer != nil {
				tracer.tr.LazyPrintf(err.Error())
				tracer.tr.SetError()
			}
			grpclog.Warningf("grpc: Server.handleStream failed to write status: %v", err)
		}
		if tracer != nil {
			tracer.tr.Finish()
		}
		return
	}
	// Unary RPC or Streaming RPC?
	if md, ok := srv.md[method]; ok {
		s.processUnaryRPC(t, stream, srv, md, tracer)
		return
	}
	if sd, ok := srv.sd[method]; ok {
		s.processStreamingRPC(t, stream, srv, sd, tracer)
		return
	}
	errDesc := fmt.Sprintf("unknown method %v", method)
	if tracer != nil {
		tracer.tr.LazyPrintf(errDesc)
		tracer.tr.SetError()
	}
	if unknownDesc := s.opts.unknownStreamDesc; unknownDesc != nil {
		s.processStreamingRPC(t, stream, nil, unknownDesc, tracer)
		return
	}
	if err := t.WriteStatus(stream, status.New(codes.Unimplemented, errDesc)); err != nil {
		if tracer != nil {
			tracer.tr.LazyPrintf(err.Error())
			tracer.tr.SetError()
		}
		grpclog.Warningf("grpc: Server.handleStream failed to write status: %v", err)
	}
	if tracer != nil {
		tracer.tr.Finish()
	}
}

// Stop stops the gRPC server. It immediately closes all open connections and
// listeners.
// It cancels all active RPCs on the server side and the corresponding pending
// RPCs on the client side will get notified by connection errors.
func (s *Server) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.mu.stopCh == nil {
		// We've been stopped already.
		return
	}

	for lis := range s.mu.listeners {
		lis.Close()
	}
	for conn := range s.mu.conns {
		conn.Close()
	}

	s.mu.listeners = nil
	s.mu.conns = nil

	// FIXME(irfansharif): Comment why we close Server.mu.stopCh only after
	// closing all listeners.
	close(s.mu.stopCh)
	s.mu.stopCh = nil

	// Notify Server.GracefulStop if Server.GracefulStop was running when
	// Server.Stop was called.
	s.mu.cv.Signal()

	s.eventsMu.Lock()
	s.eventsMu.log.Finish()
	s.eventsMu.Unlock()
}

// GracefulStop stops the gRPC server gracefully. It stops the server from
// accepting new connections and RPCs and blocks until all the pending RPCs are
// finished.
func (s *Server) GracefulStop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.mu.stopCh == nil {
		// We've been stopped already.
		return
	}

	for lis := range s.mu.listeners {
		lis.Close()
	}
	s.mu.listeners = nil

	// FIXME(irfansharif): Drain needs to be supported by the optimized
	// transport.
	if !s.mu.drain {
		for conn := range s.mu.conns {
			if drainer, ok := conn.(drainer); ok {
				drainer.Drain()
			}
		}
		s.mu.drain = true
	}
	for len(s.mu.conns) != 0 {
		s.mu.cv.Wait()
	}

	if s.mu.stopCh == nil {
		// We've been pre-empted by Server.Stop.
		return
	}
	s.mu.conns = nil

	close(s.mu.stopCh)
	s.mu.stopCh = nil

	s.eventsMu.Lock()
	s.eventsMu.log.Finish()
	s.eventsMu.Unlock()
}

// FIXME(irfansharif): Should these functions be deprecated? All the way down
// to the transport layer. What's the right use of this?
//
// SetHeader sets the header metadata.
// When called multiple times, all the provided metadata will be merged.
// All the metadata will be sent out when one of the following happens:
//  - grpc.SendHeader() is called;
//  - The first response is sent out;
//  - An RPC status is sent out (error or success).
func SetHeader(ctx context.Context, md metadata.MD) error {
	if md.Len() == 0 {
		return nil
	}
	stream, ok := transport.StreamFromContext(ctx)
	if !ok {
		return Errorf(codes.Internal, "grpc: failed to fetch the stream from the context %v", ctx)
	}
	return stream.SetHeader(md)
}

// FIXME(irfansharif): Should these functions be deprecated? All the way down
// to the transport layer. What's the right use of this?
//
// SendHeader sends header metadata. It may be called at most once.
// The provided md and headers set by SetHeader() will be sent.
func SendHeader(ctx context.Context, md metadata.MD) error {
	stream, ok := transport.StreamFromContext(ctx)
	if !ok {
		return Errorf(codes.Internal, "grpc: failed to fetch the stream from the context %v", ctx)
	}
	t := stream.ServerTransport()
	if t == nil {
		grpclog.Fatalf("grpc: SendHeader: %v has no ServerTransport to send header metadata.", stream)
	}
	if err := t.WriteHeader(stream, md); err != nil {
		return toRPCErr(err)
	}
	return nil
}

// FIXME(irfansharif): Should these functions be deprecated? All the way down
// to the transport layer.
//
// SetTrailer sets the trailer metadata that will be sent when an RPC returns.
// When called more than once, all the provided metadata will be merged.
func SetTrailer(ctx context.Context, md metadata.MD) error {
	if md.Len() == 0 {
		return nil
	}
	stream, ok := transport.StreamFromContext(ctx)
	if !ok {
		return Errorf(codes.Internal, "grpc: failed to fetch the stream from the context %v", ctx)
	}
	return stream.SetTrailer(md)
}

func (s *Server) authenticate(rawConn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	if s.opts.creds == nil {
		return rawConn, nil, nil
	}
	return s.opts.creds.ServerHandshake(rawConn)
}

// logEventf records an event in s's event log, unless s has been stopped
// (indicated by a nil log).
// REQUIRES s.eventsMu is not held.
func (s *Server) logEventf(format string, a ...interface{}) {
	s.eventsMu.Lock()
	defer s.eventsMu.Unlock()

	if s.eventsMu.log != nil {
		s.eventsMu.log.Printf(format, a...)
	}
}

// logErrorf records an error in s's event log, unless s has been stopped
// (indicated by a nil log).
// REQUIRES s.eventsMu is not held.
func (s *Server) logErrorf(format string, a ...interface{}) {
	s.eventsMu.Lock()
	defer s.eventsMu.Unlock()

	if s.eventsMu.log != nil {
		s.eventsMu.log.Errorf(format, a...)
	}
}

func init() {
	internal.TestingCloseConns = func(arg interface{}) {
		arg.(*Server).testingCloseConns()
	}
	internal.TestingUseHandlerImpl = func(arg interface{}) {
		arg.(*Server).opts.useHandlerImpl = true
	}
}

// testingCloseConns closes all existing transports but keeps s.lis
// accepting new connections.
func (s *Server) testingCloseConns() {
	s.mu.Lock()
	for conn := range s.mu.conns {
		conn.Close()
		delete(s.mu.conns, conn)
	}
	s.mu.Unlock()
}
