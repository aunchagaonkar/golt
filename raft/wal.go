package raft

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	pb "github.com/aunchagaonkar/golt/proto"
	"google.golang.org/protobuf/proto"
)

type WAL struct {
	mu      sync.Mutex
	file    *os.File
	offsets []int64
	path    string
}

func OpenWAL(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL dir: %w", err)
	}

	path := filepath.Join(dir, "raft.log")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL: %w", err)
	}

	w := &WAL{
		file:    f,
		offsets: make([]int64, 0),
		path:    path,
	}
	if err := w.recoverOffsets(); err != nil {
		return nil, fmt.Errorf("failed to recover offsets: %w", err)
	}

	return w, nil
}

func (w *WAL) recoverOffsets() error {
	if _, err := w.file.Seek(0, 0); err != nil {
		return err
	}
	info, err := w.file.Stat()
	if err != nil {
		return err
	}
	fileSize := info.Size()
	offset := int64(0)

	for offset < fileSize {
		w.offsets = append(w.offsets, offset)
		header := make([]byte, 8)
		n, err := w.file.ReadAt(header, offset)
		if err != nil {
			return fmt.Errorf("failed to read length header at %d: %w", offset, err)
		}
		if n != 8 {
			return fmt.Errorf("short read of length header at %d", offset)
		}

		length := binary.LittleEndian.Uint64(header)
		offset += 8 + int64(length)
	}

	if _, err := w.file.Seek(0, 2); err != nil {
		return err
	}

	return nil
}

func (w *WAL) Append(entry *pb.LogEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	data, err := proto.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal failed: %w", err)
	}

	info, err := w.file.Stat()
	if err != nil {
		return err
	}
	startOffset := info.Size()

	header := make([]byte, 8)
	binary.LittleEndian.PutUint64(header, uint64(len(data)))
	if _, err := w.file.Write(header); err != nil {
		return err
	}

	if _, err := w.file.Write(data); err != nil {
		return err
	}

	w.offsets = append(w.offsets, startOffset)

	return nil
}

func (w *WAL) Truncate(index uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if index == 0 {
		return fmt.Errorf("invalid truncate index 0")
	}

	if index > uint64(len(w.offsets)) {
		return nil
	}

	delOffset := w.offsets[index-1]

	if err := w.file.Truncate(delOffset); err != nil {
		return fmt.Errorf("truncate failed: %w", err)
	}

	if _, err := w.file.Seek(delOffset, 0); err != nil {
		return fmt.Errorf("seek failed: %w", err)
	}

	w.offsets = w.offsets[:index-1]

	return nil
}

func (w *WAL) ReadAll() ([]*pb.LogEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	entries := make([]*pb.LogEntry, 0, len(w.offsets))

	for i, offset := range w.offsets {

		header := make([]byte, 8)
		if _, err := w.file.ReadAt(header, offset); err != nil {
			return nil, fmt.Errorf("read header at %d failed: %w", offset, err)
		}
		length := binary.LittleEndian.Uint64(header)

		data := make([]byte, length)
		if _, err := w.file.ReadAt(data, offset+8); err != nil {
			return nil, fmt.Errorf("read data at %d failed: %w", offset+8, err)
		}

		entry := &pb.LogEntry{}
		if err := proto.Unmarshal(data, entry); err != nil {
			return nil, fmt.Errorf("unmarshal entry %d failed: %w", i+1, err)
		}

		entries = append(entries, entry)
	}

	return entries, nil
}

func (w *WAL) Compact(keepEntries []*pb.LogEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	tmpPath := w.path + ".new"
	f, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create compaction file: %w", err)
	}

	var newOffsets []int64

	for _, entry := range keepEntries {
		data, err := proto.Marshal(entry)
		if err != nil {
			f.Close()
			return fmt.Errorf("marshal failed during compaction: %w", err)
		}

		info, err := f.Stat()
		if err != nil {
			f.Close()
			return err
		}
		startOffset := info.Size()

		lenBuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(lenBuf, uint64(len(data)))
		if _, err := f.Write(lenBuf); err != nil {
			f.Close()
			return err
		}

		if _, err := f.Write(data); err != nil {
			f.Close()
			return err
		}

		newOffsets = append(newOffsets, startOffset)
	}

	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	f.Close()

	if err := w.file.Close(); err != nil {
		return fmt.Errorf("failed to close old WAL: %w", err)
	}

	if err := os.Rename(tmpPath, w.path); err != nil {
		return fmt.Errorf("failed to rename compacted WAL: %w", err)
	}

	newF, err := os.OpenFile(w.path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to reopen WAL: %w", err)
	}

	w.file = newF
	w.offsets = newOffsets

	return nil
}

func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Sync()
}
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}
