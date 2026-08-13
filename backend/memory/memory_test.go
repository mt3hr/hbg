package memory_test

import (
	"testing"

	"github.com/mt3hr/hbg/backend/memory"
	"github.com/mt3hr/hbg/storage"
	"github.com/mt3hr/hbg/storage/storagetest"
)

// テスト用の偽ストレージであっても、適合性テストは通す。
// 実装がずれていると、これを使ったエンジンのテストが
// 実際の挙動とかけ離れてしまうため。
func TestConformance(t *testing.T) {
	storagetest.Run(t, storagetest.Harness{
		NewStorage: func(t *testing.T) (storage.Storage, string) {
			return memory.New("memory"), "/root"
		},
		// 件数の多いディレクトリは適合性の確認済みなので、
		// メモリ実装では数を減らして時間を節約する。
		LargeDirCount: 1100,
	})
}
