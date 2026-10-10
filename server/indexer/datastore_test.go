// SPDX-License-Identifier: AGPL-3.0-or-later

package indexer

import (
	"bytes"
	"os"
	"testing"
)

func TestDataStoreCompressionRoundTrip(t *testing.T) {
	dir := t.TempDir()
	payload := []byte("<p>preview body</p>")
	gz := newDataStore(dir, "gzip")
	zs := newDataStore(dir, "zstd")

	key, err := gz.write(htmlSubdir, payload)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := zs.read(htmlSubdir, key); err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("zstd store read gzip file = %q, %v", got, err)
	}

	zkey, err := zs.write(htmlSubdir, []byte("zstd content"))
	if err != nil {
		t.Fatal(err)
	}
	if got, err := gz.read(htmlSubdir, zkey); err != nil || string(got) != "zstd content" {
		t.Fatalf("gzip store read zstd file = %q, %v", got, err)
	}
	if got, err := zs.read(htmlSubdir, zkey); err != nil || string(got) != "zstd content" {
		t.Fatalf("zstd store read zstd file = %q, %v", got, err)
	}
}

func TestRecompressConvertsToConfiguredCodec(t *testing.T) {
	dir := t.TempDir()
	gz := newDataStore(dir, "gzip")
	key, err := gz.write(htmlSubdir, []byte("<p>old</p>"))
	if err != nil {
		t.Fatal(err)
	}
	zs := newDataStore(dir, "zstd")
	stats, err := zs.Recompress()
	if err != nil {
		t.Fatal(err)
	}
	if stats.Converted != 1 {
		t.Fatalf("converted = %d, want 1", stats.Converted)
	}
	if _, err := os.Stat(dataFilePathExt(dir, htmlSubdir, key, zstdExt)); err != nil {
		t.Fatalf("zstd file missing: %v", err)
	}
	if _, err := os.Stat(dataFilePathExt(dir, htmlSubdir, key, gzipExt)); !os.IsNotExist(err) {
		t.Fatalf("gzip file still present: %v", err)
	}
	if got, err := zs.read(htmlSubdir, key); err != nil || string(got) != "<p>old</p>" {
		t.Fatalf("read after recompress = %q, %v", got, err)
	}
	stats, err = zs.Recompress()
	if err != nil || stats.Converted != 0 || stats.Skipped != 1 {
		t.Fatalf("second pass stats = %+v, %v", stats, err)
	}
}
