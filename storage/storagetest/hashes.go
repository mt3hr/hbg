package storagetest

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/mt3hr/hbg/storage"
)

// knownHash は既知の内容に対する参照値を返します。
// 参照値を用意していない種類ではエラーを返します。
func knownHash(ht storage.HashType, content string) (string, error) {
	switch ht {
	case storage.MD5:
		sum := md5.Sum([]byte(content))
		return hex.EncodeToString(sum[:]), nil
	case storage.SHA1:
		sum := sha1.Sum([]byte(content))
		return hex.EncodeToString(sum[:]), nil
	case storage.SHA256:
		sum := sha256.Sum256([]byte(content))
		return hex.EncodeToString(sum[:]), nil
	case storage.DropboxContent:
		// 4MiB 未満なので、1ブロックの SHA-256 をさらに SHA-256 したもの
		block := sha256.Sum256([]byte(content))
		overall := sha256.Sum256(block[:])
		return hex.EncodeToString(overall[:]), nil
	}
	return "", fmt.Errorf("参照値を用意していないハッシュ: %s", ht)
}
