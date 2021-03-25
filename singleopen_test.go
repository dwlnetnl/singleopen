package singleopen

import (
	"io/fs"
	"testing"
	"testing/fstest"
)

func TestFS(t *testing.T) {
	mustOpen := func(fsys fs.FS, name string) fs.File {
		t.Helper()
		f, err := fsys.Open(name)
		if err != nil {
			t.Fatal(err)
		}
		return f
	}
	mustClose := func(f fs.File) {
		t.Helper()
		err := f.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	wantRefCount := func(f fs.File, want int) {
		t.Helper()
		got := f.(*fileReaderAt).refc
		if got != want {
			t.Errorf("got ref count %d, want: %d", got, want)
		}
	}
	sameFile := func(f1, f2 fs.File) bool {
		return f1.(*fileReaderAt).file == f2.(*fileReaderAt).file
	}

	fsys := &FS{FS: fstest.MapFS{
		"sub/dir/file1": &fstest.MapFile{},
	}}
	if err := fstest.TestFS(fsys, "sub/dir/file1"); err != nil {
		t.Fatal(err)
	}

	f1 := mustOpen(fsys, "sub/dir/file1")
	// t.Fatal("\n" + spew.Sdump(f1))
	f2 := mustOpen(fsys, "sub/dir/file1")
	wantRefCount(f1, 2)
	if !sameFile(f1, f2) {
		t.Error("f1 != f2")
	}

	fsys.KeepLast(8)
	mustClose(f1)
	mustClose(f2)
	wantRefCount(f1, 0)

	sub, err := fs.Sub(fsys, "sub")
	if err != nil {
		t.Fatal(err)
	}
	f3 := mustOpen(sub, "dir/file1")
	f4 := mustOpen(sub, "dir/file1")
	if !sameFile(f3, f1) {
		t.Error("f3 != f1")
	}
	if !sameFile(f3, f4) {
		t.Error("f3 != f4")
	}
	wantRefCount(f3, 2)
	mustClose(f3)

	subdir, err := fs.Sub(sub, "dir")
	if err != nil {
		t.Fatal(err)
	}
	f5 := mustOpen(subdir, "file1")
	f6 := mustOpen(subdir, "file1")
	wantRefCount(f3, 3)
	if !sameFile(f5, f6) {
		t.Error("f5 != f6")
	}
	if !sameFile(f5, f4) {
		t.Error("f5 != f4")
	}

	fsys.KeepLast(0)

	mustClose(f4)
	mustClose(f5)
	mustClose(f6)
	wantRefCount(f3, 0)

	if f3.Close() != fs.ErrClosed {
		t.Error("file is not closed")
	}
}
