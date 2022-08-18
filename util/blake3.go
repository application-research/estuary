package util

import (
	"encoding/hex"
	"io"
	"io/ioutil"

	blake3 "lukechampine.com/blake3"
)

// TODO: This is a first attempt! It would be nice if we can implement Chunking in Go.
// TODO: Really look into whether we can do this in a DAG service so we don't have to double hash.
/// Generate the Blake3 hash of a file and return it as a Hex string.
func Blake3Hash(fi io.Reader) (string, error) {
	// Read the file into a buffer
	buf, err := ioutil.ReadAll(fi)
	if err != nil {
		return "", err
	}
	// Hash the buffer
	hash := blake3.Sum256(buf)
	hashStr := hex.EncodeToString(hash[:])
	return hashStr, nil
}
