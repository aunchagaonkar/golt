package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

type SSTable struct {
	path string
}

func OpenSSTable(path string) *SSTable {
	return &SSTable{path: path}
}

func WriteSSTable(path string, keys []string, data map[string]Entry) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create sstable: %w", err)
	}
	defer f.Close()

	for _, k := range keys {
		entry := data[k]

		/* format:
		keyLen  - 4 bytes
		key     - keyLen bytes
		deleted - 1 byte
		valLen  - 4 bytes
		val     - valLen bytes
		*/

		if err := binary.Write(f, binary.LittleEndian, uint32(len(k))); err != nil {
			return err
		}
		if _, err := f.Write([]byte(k)); err != nil {
			return err
		}

		deletedByte := uint8(0)
		if entry.Deleted {
			deletedByte = 1
		}
		if err := binary.Write(f, binary.LittleEndian, deletedByte); err != nil {
			return err
		}

		if err := binary.Write(f, binary.LittleEndian, uint32(len(entry.Value))); err != nil {
			return err
		}
		if _, err := f.Write([]byte(entry.Value)); err != nil {
			return err
		}
	}
	return nil
}

func (s *SSTable) Search(key string) (string, bool, bool) {
	f, err := os.Open(s.path)
	if err != nil {
		return "", false, false
	}
	defer f.Close()

	for {
		var keyLen uint32
		if err := binary.Read(f, binary.LittleEndian, &keyLen); err != nil {
			if err == io.EOF {
				break
			}
			return "", false, false
		}

		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(f, keyBytes); err != nil {
			return "", false, false
		}
		currentKey := string(keyBytes)

		var deletedByte uint8
		if err := binary.Read(f, binary.LittleEndian, &deletedByte); err != nil {
			return "", false, false
		}
		deleted := deletedByte == 1

		var valLen uint32
		if err := binary.Read(f, binary.LittleEndian, &valLen); err != nil {
			return "", false, false
		}
		valBytes := make([]byte, valLen)
		if _, err := io.ReadFull(f, valBytes); err != nil {
			return "", false, false
		}

		if currentKey == key {
			return string(valBytes), true, deleted
		}

		if currentKey > key {
			return "", false, false
		}
	}

	return "", false, false
}

func (s *SSTable) Scan() (map[string]Entry, error) {
	f, err := os.Open(s.path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	data := make(map[string]Entry)

	for {
		var keyLen uint32
		if err := binary.Read(f, binary.LittleEndian, &keyLen); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(f, keyBytes); err != nil {
			return nil, err
		}
		key := string(keyBytes)

		var deletedByte uint8
		if err := binary.Read(f, binary.LittleEndian, &deletedByte); err != nil {
			return nil, err
		}
		deleted := deletedByte == 1

		var valLen uint32
		if err := binary.Read(f, binary.LittleEndian, &valLen); err != nil {
			return nil, err
		}

		valBytes := make([]byte, valLen)
		if _, err := io.ReadFull(f, valBytes); err != nil {
			return nil, err
		}
		value := string(valBytes)

		data[key] = Entry{Value: value, Deleted: deleted}
	}
	return data, nil
}
