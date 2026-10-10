// SPDX-License-Identifier: AGPL-3.0-or-later

package indexer

import (
	"compress/gzip"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/rs/zerolog/log"

	"github.com/asciimoo/hister/config"
)

const (
	dataDirName   = "data"
	htmlSubdir    = "html"
	faviconSubdir = "favicon"
)

// Data files are named after their codec: <key>.gz or <key>.zst. Reads accept
// either extension so the configured codec can change without migrating data.
const (
	gzipExt = ".gz"
	zstdExt = ".zst"
)

var storedExtensions = []string{gzipExt, zstdExt}

func extForCompression(compression string) string {
	if compression == config.CompressionZstd {
		return zstdExt
	}
	return gzipExt
}

// dataStore manages the content-addressed file-system store for HTML and
// favicon data. A fixed pool of 256 per-shard RWMutexes (indexed by the first
// byte of the SHA-256 key) serialises concurrent operations on files in the
// same shard while allowing fully parallel access across different shards.
//
// The struct is shared between the live indexer and the temporary indexer
// created during reindex so that both always use the same locks.
type dataStore struct {
	dir         string
	compression string
	shards      [256]sync.RWMutex
}

func newDataStore(dir, compression string) *dataStore {
	return &dataStore{dir: dir, compression: compression}
}

// shard returns the mutex responsible for key, using the first byte of the
// hex-encoded SHA-256 string as the shard index (256 shards total).
func (ds *dataStore) shard(key string) *sync.RWMutex {
	if len(key) < 2 {
		return &ds.shards[0]
	}
	hi := hexNibble(key[0])
	lo := hexNibble(key[1])
	return &ds.shards[hi<<4|lo]
}

// hexNibble converts a lowercase hex character to its numeric value.
func hexNibble(c byte) byte {
	switch {
	case c >= '0' && c <= '9':
		return c - '0'
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10
	default:
		return 0
	}
}

// write compresses data and writes it to the content-addressed store under
// subdir. The SHA-256 hash is computed first (no lock needed), then the
// key's shard is exclusively locked for the stat+create to prevent races
// between a concurrent delete of the same key.
func (ds *dataStore) write(subdir string, data []byte) (string, error) {
	if len(data) == 0 {
		return "", nil
	}
	sum := sha256.Sum256(data)
	key := fmt.Sprintf("%x", sum)
	mu := ds.shard(key)
	mu.Lock()
	defer mu.Unlock()
	if err := writeFileLocked(ds.dir, subdir, key, data, ds.compression); err != nil {
		return "", err
	}
	return key, nil
}

// Recompress converts all stored HTML and favicon files to the configured codec.
func (ds *dataStore) Recompress() (RecompressStats, error) {
	var stats RecompressStats
	for _, subdir := range []string{htmlSubdir, faviconSubdir} {
		if err := ds.recompress(subdir, &stats); err != nil {
			return stats, fmt.Errorf("recompress %s data: %w", subdir, err)
		}
	}
	return stats, nil
}

// read decompresses and returns the data file identified by key. Acquires a
// shared lock on the key's shard, allowing concurrent reads of the same file.
func (ds *dataStore) read(subdir, key string) ([]byte, error) {
	mu := ds.shard(key)
	mu.RLock()
	defer mu.RUnlock()
	return readFileLocked(ds.dir, subdir, key, ds.compression)
}

// deleteIfOrphaned acquires the exclusive shard lock for key, calls refCount
// to check whether any indexed document still references it, and removes the
// file if the count is zero. Holding the lock across both the check and the
// remove makes the operation atomic with respect to concurrent writes or
// deletes of the same key.
func (ds *dataStore) deleteIfOrphaned(field, subdir, key string, refCount func(string, string) uint64) {
	mu := ds.shard(key)
	mu.Lock()
	defer mu.Unlock()
	if refCount(field, key) == 0 {
		if err := removeDataFiles(ds.dir, subdir, key); err != nil {
			log.Warn().Err(err).Str("key", key).Str("subdir", subdir).Msg("failed to remove orphaned data file")
		} else {
			log.Debug().Str("key", key).Str("subdir", subdir).Msg("removed orphaned data file on document update")
		}
	}
}

// cleanup removes unreferenced data files under subdir. For each candidate file
// the per-key shard lock is acquired and refCount is called while holding the
// lock, so the check and the removal are atomic with respect to concurrent
// writes or deletes of the same key
func (ds *dataStore) cleanup(field, subdir string, refCount func(string, string) uint64) (int, error) {
	root := filepath.Join(ds.dir, subdir)
	if _, err := os.Stat(root); os.IsNotExist(err) {
		return 0, nil
	}
	removed := 0
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Warn().Err(err).Str("path", path).Msg("error accessing data file during cleanup")
			return nil
		}
		if info.IsDir() || !isStoredExt(filepath.Ext(path)) {
			return nil
		}
		// Reconstruct the key from the 3-level directory prefix + filename.
		// rel is like "aa/bb/cc/ddeeff....gz" or "aa/bb/cc/ddeeff....zst"
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return nil
		}
		parts := splitPath(rel)
		if len(parts) != 4 {
			return nil
		}
		// parts[3] is the filename with its codec suffix; strip it to get the hash tail.
		ext := filepath.Ext(parts[3])
		key := parts[0] + parts[1] + parts[2] + parts[3][:len(parts[3])-len(ext)]
		mu := ds.shard(key)
		mu.Lock()
		if refCount(field, key) == 0 {
			if rerr := os.Remove(path); rerr != nil {
				log.Warn().Err(rerr).Str("path", path).Msg("failed to remove orphaned data file")
			} else {
				removed++
				log.Debug().Str("key", key).Str("subdir", subdir).Msg("removed orphaned data file")
			}
		}
		mu.Unlock()
		return nil
	})
	return removed, err
}

func isStoredExt(ext string) bool {
	return ext == gzipExt || ext == zstdExt
}

// dataFilePath returns the gzip path for a stored data file, the historical
// layout. Layout: {dataDir}/{subdir}/{key[0:2]}/{key[2:4]}/{key[4:6]}/{key[6:]}.gz
func dataFilePath(dataDir, subdir, key string) string {
	return dataFilePathExt(dataDir, subdir, key, gzipExt)
}

func dataFilePathExt(dataDir, subdir, key, ext string) string {
	return filepath.Join(dataDir, subdir, key[0:2], key[2:4], key[4:6], key[6:]+ext)
}

// removeDataFiles deletes the data file for key in every supported codec.
func removeDataFiles(dataDir, subdir, key string) error {
	var errs []error
	for _, ext := range storedExtensions {
		if err := os.Remove(dataFilePathExt(dataDir, subdir, key, ext)); err != nil && !os.IsNotExist(err) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// writeFileLocked writes data compressed with the configured codec to the path
// derived from key. If a file for the key already exists in any codec it is
// left unchanged (same hash = same content). The caller must hold the relevant
// shard lock.
func writeFileLocked(dataDir, subdir, key string, data []byte, compression string) error {
	for _, ext := range storedExtensions {
		if _, err := os.Stat(dataFilePathExt(dataDir, subdir, key, ext)); err == nil {
			return nil
		}
	}
	return writeDataFile(dataFilePathExt(dataDir, subdir, key, extForCompression(compression)), data)
}

// writeDataFile compresses data with the codec implied by fpath's extension and
// creates the file. On any failure the partially written file is removed so that
// later writes are not skipped in favour of a truncated file.
func writeDataFile(fpath string, data []byte) (err error) {
	if err := os.MkdirAll(filepath.Dir(fpath), 0o755); err != nil {
		return fmt.Errorf("create data directory: %w", err)
	}
	f, err := os.Create(fpath)
	if err != nil {
		return fmt.Errorf("create data file: %w", err)
	}
	defer func() {
		if cerr := f.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("close data file: %w", cerr)
		}
		if err != nil {
			_ = os.Remove(fpath)
		}
	}()
	if filepath.Ext(fpath) == zstdExt {
		return writeZstd(f, data)
	}
	return writeGzip(f, data)
}

// RecompressStats reports the result of a recompression pass.
type RecompressStats struct {
	Converted int
	// Skipped counts files already in the target codec.
	Skipped int
	// Unrecognized counts stored files whose path does not match the data layout.
	Unrecognized int
	// Unreadable counts stored files that failed to decode and were left in place.
	Unreadable int
}

// recompress rewrites every data file under subdir that is not already in the
// target codec. Each file is handled under its key's shard lock: the new file is
// written first, then the old one is removed, so a failure never loses data.
func (ds *dataStore) recompress(subdir string, stats *RecompressStats) error {
	root := filepath.Join(ds.dir, subdir)
	if _, err := os.Stat(root); os.IsNotExist(err) {
		return nil
	}
	target := extForCompression(ds.compression)
	return filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || !isStoredExt(filepath.Ext(path)) {
			return nil
		}
		ext := filepath.Ext(path)
		if ext == target {
			stats.Skipped++
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		parts := splitPath(rel)
		if len(parts) != 4 {
			stats.Unrecognized++
			return nil
		}
		key := parts[0] + parts[1] + parts[2] + parts[3][:len(parts[3])-len(ext)]
		mu := ds.shard(key)
		mu.Lock()
		defer mu.Unlock()
		data, err := readDataPath(path, ext)
		if err != nil {
			// Leave unreadable files in place so nothing is lost; report and continue.
			log.Warn().Err(err).Str("path", path).Msg("skipping unreadable data file")
			stats.Unreadable++
			return nil
		}
		newPath := dataFilePathExt(ds.dir, subdir, key, target)
		if _, statErr := os.Stat(newPath); statErr != nil {
			if err := writeDataFile(newPath, data); err != nil {
				_ = os.Remove(newPath)
				return err
			}
		}
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("remove old data file: %w", err)
		}
		stats.Converted++
		return nil
	})
}

// readDataPath decodes the data file at fpath, which must use the codec ext.
func readDataPath(fpath, ext string) ([]byte, error) {
	f, err := os.Open(fpath)
	if err != nil {
		return nil, err
	}
	return decodeDataFile(f, ext, fpath)
}

func writeGzip(f io.Writer, data []byte) error {
	w := gzip.NewWriter(f)
	if _, err := w.Write(data); err != nil {
		return fmt.Errorf("write compressed data: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("flush compressed data: %w", err)
	}
	return nil
}

// zstdEncoderPool reuses zstd encoders across writes; each encoder owns buffers
// and worker goroutines, which are costly to allocate per document.
var zstdEncoderPool = sync.Pool{
	New: func() any {
		enc, err := zstd.NewWriter(nil)
		if err != nil {
			return nil
		}
		return enc
	},
}

func writeZstd(f io.Writer, data []byte) error {
	w, _ := zstdEncoderPool.Get().(*zstd.Encoder)
	if w == nil {
		var err error
		if w, err = zstd.NewWriter(nil); err != nil {
			return fmt.Errorf("create zstd writer: %w", err)
		}
	}
	defer zstdEncoderPool.Put(w)
	w.Reset(f)
	if _, err := w.Write(data); err != nil {
		_ = w.Close()
		return fmt.Errorf("write compressed data: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("flush compressed data: %w", err)
	}
	return nil
}

// readFileLocked decompresses and returns the data file identified by key. The
// preferred codec is tried first, then the other one, so files written before
// the codec was changed remain readable. The caller must hold the relevant shard lock.
func readFileLocked(dataDir, subdir, key, compression string) ([]byte, error) {
	preferred := extForCompression(compression)
	order := []string{preferred}
	for _, ext := range storedExtensions {
		if ext != preferred {
			order = append(order, ext)
		}
	}
	var openErr error
	for _, ext := range order {
		fpath := dataFilePathExt(dataDir, subdir, key, ext)
		f, err := os.Open(fpath)
		if err != nil {
			if openErr == nil {
				openErr = err
			}
			continue
		}
		data, err := decodeDataFile(f, ext, fpath)
		return data, err
	}
	return nil, fmt.Errorf("open data file: %w", openErr)
}

func decodeDataFile(f *os.File, ext, fpath string) ([]byte, error) {
	defer func() {
		if cerr := f.Close(); cerr != nil {
			log.Warn().Err(cerr).Str("path", fpath).Msg("failed to close data file")
		}
	}()
	if ext == zstdExt {
		dec, err := zstd.NewReader(f)
		if err != nil {
			return nil, fmt.Errorf("create zstd reader: %w", err)
		}
		defer dec.Close()
		data, err := io.ReadAll(dec)
		if err != nil {
			return nil, fmt.Errorf("decompress data: %w", err)
		}
		return data, nil
	}
	r, err := gzip.NewReader(f)
	if err != nil {
		return nil, fmt.Errorf("create gzip reader: %w", err)
	}
	defer func() {
		if cerr := r.Close(); cerr != nil {
			log.Warn().Err(cerr).Str("path", fpath).Msg("failed to close gzip reader")
		}
	}()
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("decompress data: %w", err)
	}
	return data, nil
}

// splitPath splits a filepath into its individual components.
func splitPath(p string) []string {
	var parts []string
	for {
		dir, file := filepath.Split(filepath.Clean(p))
		if file == "" || file == "." {
			break
		}
		parts = append([]string{file}, parts...)
		p = dir
		if dir == "" || dir == "." {
			break
		}
	}
	return parts
}
