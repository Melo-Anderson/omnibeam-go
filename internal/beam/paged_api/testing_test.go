package paged_api

import (
	"bytes"
	"context"
	"io"
)

type fakeStorageWriter struct {
	tempCreated   string
	finalURI      string
	createCalls   int
	tempCommitted bool
	tempAborted   bool
	buf           bytes.Buffer
	createErr     error
	commitErr     error
}

func (f *fakeStorageWriter) CreateTemp(_ context.Context, finalURI string) (string, io.WriteCloser, error) {
	f.createCalls++
	if f.createErr != nil {
		return "", nil, f.createErr
	}
	f.finalURI = finalURI
	f.tempCreated = "temp-" + finalURI
	return f.tempCreated, &nopWriteCloser{&f.buf}, nil
}

func (f *fakeStorageWriter) CommitTemp(_ context.Context, _, _ string) error {
	if f.commitErr != nil {
		return f.commitErr
	}
	f.tempCommitted = true
	return nil
}

func (f *fakeStorageWriter) AbortTemp(_ context.Context, _ string) error {
	f.tempAborted = true
	return nil
}

type nopWriteCloser struct {
	io.Writer
}

func (nopWriteCloser) Close() error { return nil }
