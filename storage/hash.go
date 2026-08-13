package storage

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"slices"
)

// HashType はハッシュの種類です。
type HashType string

// 対応しているハッシュの種類。
const (
	MD5    HashType = "md5"
	SHA1   HashType = "sha1"
	SHA256 HashType = "sha256"
	// DropboxContent は Dropbox 独自の content hash です。
	// 4MiB ごとのブロックの SHA-256 を連結し、それをもう一度 SHA-256 したものです。
	DropboxContent HashType = "dropbox"
	// QuickXor は OneDrive の quickXorHash です。
	QuickXor HashType = "quickxor"
)

// HashSet はストレージが扱えるハッシュの集合です。
// 前にあるものほど優先されます。
type HashSet []HashType

// Has は指定した種類を含むかを返します。
func (s HashSet) Has(t HashType) bool {
	return slices.Contains(s, t)
}

// CommonHash は両方が扱えるハッシュのうち、もっとも優先度の高いものを返します。
// 共通するものがなければ false を返します。
func CommonHash(a, b HashSet) (HashType, bool) {
	for _, t := range a {
		if b.Has(t) {
			return t, true
		}
	}
	return "", false
}

// NewHash はハッシュの種類に対応する hash.Hash を返します。
func NewHash(t HashType) (hash.Hash, error) {
	switch t {
	case MD5:
		return md5.New(), nil
	case SHA1:
		return sha1.New(), nil
	case SHA256:
		return sha256.New(), nil
	case DropboxContent:
		return NewDropboxContentHash(), nil
	}
	return nil, fmt.Errorf("%w: ハッシュ %q", ErrUnsupported, t)
}

// MultiHasher は、複数のハッシュを同時に計算する io.Writer と、
// 結果を取り出す関数を返します。
//
// 転送しながら io.TeeReader で流し込むことで、
// 内容を2度読まずに検証用のハッシュを得られます。
func MultiHasher(types ...HashType) (io.Writer, func() map[HashType]string, error) {
	hashes := make(map[HashType]hash.Hash, len(types))
	writers := make([]io.Writer, 0, len(types))

	for _, t := range types {
		h, err := NewHash(t)
		if err != nil {
			return nil, nil, err
		}
		hashes[t] = h
		writers = append(writers, h)
	}

	sum := func() map[HashType]string {
		result := make(map[HashType]string, len(hashes))
		for t, h := range hashes {
			result[t] = hex.EncodeToString(h.Sum(nil))
		}
		return result
	}

	if len(writers) == 0 {
		return io.Discard, sum, nil
	}
	return io.MultiWriter(writers...), sum, nil
}

// dropboxBlockSize は Dropbox の content hash が使うブロックの大きさです。
const dropboxBlockSize = 4 * 1024 * 1024

// dropboxContentHash は Dropbox 独自の content hash を計算します。
//
// 内容を 4MiB ごとのブロックに区切って各ブロックの SHA-256 を求め、
// それらを連結したものをもう一度 SHA-256 したものが content hash です。
//
// hash.Hash の Sum は状態を変えてはいけないため、確定したブロックの
// ダイジェストを保持しておき、Sum のたびに端数を足して計算します。
// 保持する量は 4MiB あたり 32 バイトなので、350GiB のファイルでも
// 3MB 程度に収まります。
type dropboxContentHash struct {
	block    hash.Hash
	blockLen int
	// digests は確定したブロックの SHA-256 を連結したものです。
	digests []byte
}

// NewDropboxContentHash は Dropbox の content hash を計算する hash.Hash を返します。
func NewDropboxContentHash() hash.Hash {
	return &dropboxContentHash{block: sha256.New()}
}

func (d *dropboxContentHash) Write(p []byte) (int, error) {
	written := len(p)
	for len(p) > 0 {
		n := min(dropboxBlockSize-d.blockLen, len(p))
		if _, err := d.block.Write(p[:n]); err != nil {
			return 0, err
		}
		d.blockLen += n
		p = p[n:]

		if d.blockLen == dropboxBlockSize {
			d.digests = d.block.Sum(d.digests)
			d.block.Reset()
			d.blockLen = 0
		}
	}
	return written, nil
}

func (d *dropboxContentHash) Sum(b []byte) []byte {
	// d.digests をそのまま Sum に渡すと、余った容量に書き込まれて
	// 後続の Write と干渉しうるので複製する。
	digests := make([]byte, len(d.digests), len(d.digests)+sha256.Size)
	copy(digests, d.digests)
	if d.blockLen > 0 {
		digests = d.block.Sum(digests)
	}

	sum := sha256.Sum256(digests)
	return append(b, sum[:]...)
}

func (d *dropboxContentHash) Reset() {
	d.block.Reset()
	d.blockLen = 0
	d.digests = nil
}

func (d *dropboxContentHash) Size() int      { return sha256.Size }
func (d *dropboxContentHash) BlockSize() int { return dropboxBlockSize }
