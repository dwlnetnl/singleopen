// Package singleopen provides functionality for reusing
// file handles when possible.
package singleopen

import (
	"errors"
	"io"
	"io/fs"
	"sync"

	"golang.org/x/sync/singleflight"

	"github.com/dwlnetnl/singleopen/internal/lru"
)

// FS is a file system that reuses file handles.
type FS struct {
	// FS is the underlying file system used to open files.
	// The underlying file system should return files that
	// implement io.ReaderAt. If the file is just a fs.File
	// calls to Read will be synchronised.
	FS fs.FS

	opener singleflight.Group
	mu     sync.Mutex // protects all below
	files  map[string]*file
	cache  *lru.Cache
	closer chan *file
}

var _ fs.FS = (*FS)(nil)

// Open opens a file or returns the already open file.
// Only regular files are being reused. Before opening
// Stat is being called to determine the kind of file.
func (fsys *FS) Open(name string) (fs.File, error) {
	fsys.mu.Lock()
	f, ok := fsys.files[name]
	if ok {
		f.refc++
		fsys.mu.Unlock()
		if ra, ok := f.File.(io.ReaderAt); ok {
			return &fileReaderAt{f, ra, 0}, nil
		}
		return f, nil
	}

	// get file from close cache
	if fsys.cache != nil {
		cv, ok := fsys.cache.Get(name)
		if ok {
			f := cv.(*file)
			f.refc++ // increment before cache removal
			fsys.cache.Remove(name)
			fsys.files[name] = f
			fsys.mu.Unlock()
			if ra, ok := f.File.(io.ReaderAt); ok {
				return &fileReaderAt{f, ra, 0}, nil
			}
			return f, nil
		}
	}

	// unlock while opening file
	fsys.mu.Unlock()

	// call stat to detect if a directory is being opened
	// use fs support for stat
	if sfs, ok := fsys.FS.(fs.StatFS); ok {
		fi, err := sfs.Stat(name)
		if err != nil {
			if errors.Is(err, (*fs.PathError)(nil)) {
				return nil, err
			}
			return nil, &fs.PathError{Op: "open", Path: name, Err: err}
		}
		if fi.IsDir() {
			return fsys.FS.Open(name)
		}
		f, err := fsys.open(name)
		if err != nil {
			return nil, err
		}
		if ra, ok := f.File.(io.ReaderAt); ok {
			return &fileReaderAt{f, ra, 0}, nil
		}
		return f, nil
	}

	// do stat on opened file
	f, err := fsys.open(name)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, &fs.PathError{Op: "open", Path: name, Err: err}
	}
	if fi.IsDir() {
		// remove from reusable files and close cache
		fsys.mu.Lock()
		delete(fsys.files, name)
		if fsys.cache != nil {
			fsys.cache.Remove(name)
		}
		fsys.mu.Unlock()
		// strip file reuse wrapper
		ff := f.File
		f = nil
		return ff, nil
	}
	if ra, ok := f.File.(io.ReaderAt); ok {
		return &fileReaderAt{f, ra, 0}, nil
	}
	return f, nil
}

func (fsys *FS) open(name string) (*file, error) {
	v, err, shared := fsys.opener.Do(name, func() (interface{}, error) {
		ff, err := fsys.FS.Open(name)
		if err != nil {
			return nil, err
		}
		f := &file{
			File: ff,
			fsys: fsys,
			name: name,
			refc: 1,
		}
		fsys.mu.Lock()
		if fsys.files == nil {
			fsys.files = make(map[string]*file)
		}
		fsys.files[name] = f
		fsys.mu.Unlock()
		return f, nil
	})
	if err != nil {
		return nil, err
	}

	f := v.(*file)
	if shared {
		// increment reference count, file open was shared
		fsys.mu.Lock()
		if f.refc == 0 {
			// retry, file is already closed
			fsys.mu.Unlock()
			return fsys.open(name)
		}
		f.refc++
		fsys.mu.Unlock()
	}

	return f, nil
}

// KeepLast enables a cache that keeps the last n recently
// closed files open. If n <= 0, the cache is disabled.
func (fsys *FS) KeepLast(n int) {
	fsys.mu.Lock()
	if n <= 0 {
		// disable close cache by removing the reference while
		// holding the lock and clearing the cache (and closing
		// cached files on eviction) without holding the lock
		cc := fsys.cache
		fsys.cache = nil // disable sends on fsys.closer
		close(fsys.closer)
		fsys.closer = nil
		fsys.mu.Unlock()
		// from now on just close files on cache eviction
		cc.OnEvicted = func(key lru.Key, value interface{}) {
			f := value.(*file)
			f.File.Close()
		}
		cc.Clear()
		return
	}

	defer fsys.mu.Unlock()
	if fsys.cache == nil {
		fsys.cache = &lru.Cache{
			MaxEntries: n,
			OnEvicted: func(key lru.Key, value interface{}) {
				// fsys.mu is held in this function
				f := value.(*file)
				if f.refc == 0 {
					f.fsys.closer <- f
				}
			},
		}
		fsys.closer = make(chan *file, n)
		go fsys.fileCloser()
		return
	}

	if fsys.cache.MaxEntries < n {
		fsys.cache.MaxEntries = n
	}
}

func (fsys *FS) fileCloser() {
	for f := range fsys.closer {
		f.close()
	}
}

type file struct {
	fs.File
	fsys *FS
	name string
	refc int // protected by fsys.mu
	read sync.Mutex
}

var _ fs.File = (*file)(nil)

func (f *file) Read(b []byte) (int, error) {
	f.read.Lock()
	defer f.read.Unlock()
	return f.File.Read(b)
}

func (f *file) Close() error {
	f.fsys.mu.Lock()
	if f.refc == 0 {
		f.fsys.mu.Unlock()
		return fs.ErrClosed
	}
	f.refc--
	if f.refc < 0 {
		panic("negative reference count")
	}
	if f.refc == 0 {
		closeFile := true
		if f.fsys.cache != nil {
			f.fsys.cache.Add(f.name, f)
			closeFile = false
		}
		delete(f.fsys.files, f.name)
		f.fsys.mu.Unlock()
		if !closeFile {
			return nil
		}
		return f.close()
	}
	f.fsys.mu.Unlock()
	return nil
}

func (f *file) close() error {
	err := f.File.Close()
	f.File = nil // panic on use after close
	return err
}

type fileReaderAt struct {
	*file
	io.ReaderAt
	offset int64
}

var (
	_ fs.File     = (*fileReaderAt)(nil)
	_ io.ReaderAt = (*fileReaderAt)(nil)
	_ io.Seeker   = (*fileReaderAt)(nil)
)

func (f *fileReaderAt) Read(p []byte) (n int, err error) {
	n, err = f.ReadAt(p, f.offset)
	f.offset += int64(n)
	return n, err
}

func (f *fileReaderAt) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		// offset += 0
	case io.SeekCurrent:
		offset += f.offset
	case io.SeekEnd:
		fi, err := f.Stat()
		if err != nil {
			return 0, err
		}
		size := fi.Size()
		offset += size
		if offset > size {
			offset = -1 // return error
		}
	}
	if offset < 0 {
		return 0, &fs.PathError{Op: "seek", Path: f.name, Err: fs.ErrInvalid}
	}
	f.offset = offset
	return offset, nil
}
