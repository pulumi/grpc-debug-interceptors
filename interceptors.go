// Copyright 2016-2022, Pulumi Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package interceptors

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"reflect"
	"sync"

	"google.golang.org/grpc"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc/metadata"
)

// Logs all gRPC converations in JSON format.
//
// To enable, call InitDebugInterceptors first in your process main to
// configure the location of the Go file.
func DebugServerInterceptor() grpc.UnaryServerInterceptor {
	return debugInterceptorInstance.interceptor()
}

// Like debugServerInterceptor but for streaming calls.
func DebugStreamServerInterceptor() grpc.StreamServerInterceptor {
	return debugInterceptorInstance.streamInterceptor()
}

// Like debugServerInterceptor but for GRPC client connections.
func DebugClientInterceptor() grpc.UnaryClientInterceptor {
	return debugInterceptorInstance.clientInterceptor()
}

// Like debugClientInterceptor but for streaming calls.
func DebugStreamClientInterceptor() grpc.StreamClientInterceptor {
	return debugInterceptorInstance.streamClientInterceptor()
}

type debugInterceptor struct {
	logFile string
	mutex   sync.Mutex
}

func (i *debugInterceptor) interceptor() grpc.UnaryServerInterceptor {
	if i.logFile == "" {
		return i.noInterceptor()
	}
	return i.loggingInterceptor()
}

func (i *debugInterceptor) streamInterceptor() grpc.StreamServerInterceptor {
	if i.logFile == "" {
		return i.noStreamInterceptor()
	}
	return i.loggingStreamServerInterceptor()
}

func (i *debugInterceptor) clientInterceptor() grpc.UnaryClientInterceptor {
	if i.logFile == "" {
		return i.noClientInterceptor()
	}
	return i.loggingClientInterceptor()
}

func (i *debugInterceptor) streamClientInterceptor() grpc.StreamClientInterceptor {
	if i.logFile == "" {
		return i.noStreamClientInterceptor()
	}
	return i.loggingStreamClientInterceptor()
}

func (i *debugInterceptor) loggingInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{},
		info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		log := debugInterceptorLogEntry{Method: info.FullMethod}
		i.trackRequest(&log, req)
		resp, err := handler(ctx, req)
		i.trackResponse(&log, resp)
		i.record(log)
		return resp, err
	}
}

func (i *debugInterceptor) loggingStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ssWrapped := &debugServerStream{
			interceptor:       i,
			method:            info.FullMethod,
			innerServerStream: ss,
		}
		err := handler(srv, ssWrapped)
		return err
	}
}

func (i *debugInterceptor) loggingClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{},
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		// Ignoring weird entries with empty method and nil req and reply.
		if method == "" {
			return invoker(ctx, method, req, reply, cc, opts...)
		}

		log := debugInterceptorLogEntry{Method: method}
		i.trackRequest(&log, req)
		err := invoker(ctx, method, req, reply, cc, opts...)
		i.trackResponse(&log, reply)
		i.record(log)
		return err
	}
}

func (i *debugInterceptor) loggingStreamClientInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string,
		streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {

		stream, err := streamer(ctx, desc, cc, method, opts...)

		wrappedStream := &debugClientStream{
			innerClientStream: stream,
			interceptor:       i,
			method:            method,
		}

		return wrappedStream, err
	}
}

func (i *debugInterceptor) clearLogFile() error {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	return os.WriteFile(i.logFile, []byte{}, 0600)
}

func (i *debugInterceptor) record(log debugInterceptorLogEntry) {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	f, err := os.OpenFile(i.logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		panic(fmt.Sprintf("Failed to append GRPC debug logs to file %s: %v", i.logFile, err))
	}
	defer f.Close()

	if err := json.NewEncoder(f).Encode(log); err != nil {
		panic(fmt.Sprintf("Failed to encode GRPC debug logs: %v", err))
	}
}

func (*debugInterceptor) noInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{},
		_ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		return handler(ctx, req)
	}
}

func (*debugInterceptor) noStreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream,
		info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return handler(srv, ss)
	}
}

func (*debugInterceptor) noClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{},
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption) error {
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func (*debugInterceptor) noStreamClientInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string,
		streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		return streamer(ctx, desc, cc, method, opts...)
	}
}

func (*debugInterceptor) track(log *debugInterceptorLogEntry, err error) {
	log.Errors = append(log.Errors, err.Error())
}

func (i *debugInterceptor) trackRequest(log *debugInterceptorLogEntry, req interface{}) {
	j, err := i.transcode(req)
	if err != nil {
		i.track(log, err)
	} else {
		log.Request = j
	}
}

func (i *debugInterceptor) trackResponse(log *debugInterceptorLogEntry, resp interface{}) {
	j, err := i.transcode(resp)
	if err != nil {
		i.track(log, err)
	} else {
		log.Response = j
	}
}

func (*debugInterceptor) transcode(obj interface{}) (json.RawMessage, error) {
	if obj == nil {
		return json.RawMessage("null"), nil
	}

	m, ok := obj.(proto.Message)
	if !ok {
		return json.RawMessage("null"),
			fmt.Errorf("Failed to decode, expecting proto.Message, got %v",
				reflect.TypeOf(obj))
	}

	jsonSer := jsonpb.Marshaler{}
	buf := bytes.Buffer{}
	if err := jsonSer.Marshal(&buf, m); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Wraps grpc.ServerStream with interceptor hooks for SendMsg, RecvMsg.
type debugServerStream struct {
	innerServerStream grpc.ServerStream
	interceptor       *debugInterceptor
	method            string
}

func (dss *debugServerStream) errorEntry(err error) debugInterceptorLogEntry {
	return debugInterceptorLogEntry{
		Method: dss.method,
		Errors: []string{err.Error()},
	}
}

func (dss *debugServerStream) SetHeader(md metadata.MD) error {
	return dss.innerServerStream.SetHeader(md)
}

func (dss *debugServerStream) SendHeader(md metadata.MD) error {
	return dss.innerServerStream.SendHeader(md)
}

func (dss *debugServerStream) SetTrailer(md metadata.MD) {
	dss.innerServerStream.SetTrailer(md)
}

func (dss *debugServerStream) Context() context.Context {
	return dss.innerServerStream.Context()
}

func (dss *debugServerStream) SendMsg(m interface{}) error {
	err := dss.innerServerStream.SendMsg(m)
	if err != nil {
		dss.interceptor.record(dss.errorEntry(err))
	} else {
		req, err := dss.interceptor.transcode(m)
		if err != nil {
			dss.interceptor.record(dss.errorEntry(err))
		} else {
			dss.interceptor.record(debugInterceptorLogEntry{
				Method:  dss.method,
				Request: req,
			})
		}
	}
	return err
}

func (dss *debugServerStream) RecvMsg(m interface{}) error {
	err := dss.innerServerStream.RecvMsg(m)
	if err == io.EOF {
		return err
	} else if err != nil {
		dss.interceptor.record(dss.errorEntry(err))
	} else {
		resp, err := dss.interceptor.transcode(m)
		if err != nil {
			dss.interceptor.record(dss.errorEntry(err))
		} else {
			dss.interceptor.record(debugInterceptorLogEntry{
				Method:   dss.method,
				Response: resp,
			})
		}
	}
	return err
}

var _ grpc.ServerStream = &debugServerStream{}

// Wraps grpc.ClientStream with interceptor hooks for SendMsg, RecvMsg.
type debugClientStream struct {
	innerClientStream grpc.ClientStream
	interceptor       *debugInterceptor
	method            string
}

func (d *debugClientStream) errorEntry(err error) debugInterceptorLogEntry {
	return debugInterceptorLogEntry{
		Method: d.method,
		Errors: []string{err.Error()},
	}
}

func (d *debugClientStream) Header() (metadata.MD, error) {
	return d.innerClientStream.Header()
}

func (d *debugClientStream) Trailer() metadata.MD {
	return d.innerClientStream.Trailer()
}

func (d *debugClientStream) CloseSend() error {
	return d.innerClientStream.CloseSend()
}

func (d *debugClientStream) Context() context.Context {
	return d.innerClientStream.Context()
}

func (d *debugClientStream) SendMsg(m interface{}) error {
	err := d.innerClientStream.SendMsg(m)
	if err != nil {
		d.interceptor.record(d.errorEntry(err))
	} else {
		req, err := d.interceptor.transcode(m)
		if err != nil {
			d.interceptor.record(d.errorEntry(err))
		} else {
			d.interceptor.record(debugInterceptorLogEntry{
				Method:  d.method,
				Request: req,
			})
		}
	}
	return err
}

func (d *debugClientStream) RecvMsg(m interface{}) error {
	err := d.innerClientStream.RecvMsg(m)
	if err == io.EOF {
		return err
	} else if err != nil {
		d.interceptor.record(d.errorEntry(err))
	} else {
		resp, err := d.interceptor.transcode(m)
		if err != nil {
			d.interceptor.record(d.errorEntry(err))
		} else {
			d.interceptor.record(debugInterceptorLogEntry{
				Method:   d.method,
				Response: resp,
			})
		}
	}
	return err
}

var _ grpc.ClientStream = &debugClientStream{}
