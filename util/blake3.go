package util

import (
	"encoding/hex"
	"io"

	"lukechampine.com/blake3"
)

/// Generate the Blake3 hash of a file and return it as a Hex string.
func Blake3Hash(fi io.Reader) string {
	// TODO: More explicit error handling.

	// Initialize a new 32-byte Hasher
	hash := blake3.New(32, nil)
	// Write to the Hasher from our file handle
	io.TeeReader(fi, hash)
	hashStr := hex.EncodeToString(hash.Sum(nil))
	return hashStr
}
