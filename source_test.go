package dbfly

import (
	"errors"
	"io"
	"io/fs"
	"testing"
)

type mockFS struct {
	openErr error
	content []byte
	closed  bool
}

type mockFile struct {
	content   []byte
	readCount int
	mockFS    *mockFS
}

func (f *mockFile) Read(p []byte) (n int, err error) {
	if f.readCount >= len(f.content) {
		return 0, io.EOF
	}
	n = copy(p, f.content[f.readCount:])
	f.readCount += n
	return n, nil
}

func (f *mockFile) Close() error {
	f.mockFS.closed = true
	return nil
}

func (f *mockFile) Stat() (fs.FileInfo, error) { return nil, nil }

func (m *mockFS) Open(name string) (fs.File, error) {
	if m.openErr != nil {
		return nil, m.openErr
	}
	return &mockFile{content: m.content, mockFS: m}, nil
}

func TestFSSource_Read_Success(t *testing.T) {
	content := []byte("hello world")
	mfs := &mockFS{content: content}
	src := NewFSSource(mfs)

	data, err := src.Read("test.txt")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != string(content) {
		t.Fatalf("expected %q, got %q", content, data)
	}
	if !mfs.closed {
		t.Fatal("file was not closed")
	}
}

func TestFSSource_Read_NotFound(t *testing.T) {
	mfs := &mockFS{openErr: fs.ErrNotExist}
	src := NewFSSource(mfs)

	_, err := src.Read("missing.txt")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestFSSource_Read_Fallback(t *testing.T) {
	content := []byte("from second fs")
	fs1 := &mockFS{openErr: fs.ErrNotExist}
	fs2 := &mockFS{content: content}
	src := NewFSSource(fs1, fs2)

	data, err := src.Read("test.txt")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != string(content) {
		t.Fatalf("expected %q, got %q", content, data)
	}
	if !fs2.closed {
		t.Fatal("second fs file was not closed")
	}
}

func TestFSSource_Read_AllFailed(t *testing.T) {
	fs1 := &mockFS{openErr: errors.New("fs1 error")}
	fs2 := &mockFS{openErr: errors.New("fs2 error")}
	src := NewFSSource(fs1, fs2)

	_, err := src.Read("test.txt")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}
