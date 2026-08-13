package legacy_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/mt3hr/hbg"
	"github.com/mt3hr/hbg/backend/legacy"
	"github.com/mt3hr/hbg/storage"
	"github.com/mt3hr/hbg/storage/storagetest"
)

// アダプタ越しでも適合性テストが通ることを確認します。
//
// 旧実装のローカルファイルシステムを包んで試すことで、
// アダプタ自体の変換が正しいかを、実際に動くバックエンドで検証できます。
// Dropbox と Google Drive もこのアダプタを通るので、
// ここが通っていればアダプタ由来の不具合は除外できます。
func TestConformanceThroughAdapter(t *testing.T) {
	storagetest.Run(t, storagetest.Harness{
		NewStorage: func(t *testing.T) (storage.Storage, string) {
			inner := hbg.NewLocalFileSystem("legacy-local")
			s := legacy.Wrap(inner, &storage.Features{
				ModTimePrecision: time.Second,
				CanSetModTime:    true,
				ImplicitDirs:     true,
				EmptyDirs:        true,
				// 旧実装は書き込み先へ直接書くので不可分ではない。
				AtomicPut: false,
			})
			return s, filepath.ToSlash(t.TempDir())
		},
		// 旧実装は1件ずつ os.Stat を呼ぶため遅い。件数を減らす。
		LargeDirCount: 1100,
	})
}

func TestAdapterUnwrap(t *testing.T) {
	inner := hbg.NewLocalFileSystem("x")
	a := legacy.Wrap(inner, nil)

	if a.Unwrap() != inner {
		t.Error("Unwrap が元のストレージを返さない")
	}
	if a.Type() != inner.Type() {
		t.Errorf("Type = %q, want %q", a.Type(), inner.Type())
	}
	if a.Name() != inner.Name() {
		t.Errorf("Name = %q, want %q", a.Name(), inner.Name())
	}
}

// Features を渡さない場合でも、まともな既定値が入ることを確認します。
func TestAdapterDefaultFeatures(t *testing.T) {
	a := legacy.Wrap(hbg.NewLocalFileSystem("x"), nil)

	f := a.Features()
	if f == nil {
		t.Fatal("Features が nil")
	}
	if f.ModTimePrecision <= 0 {
		t.Error("ModTimePrecision が設定されていない")
	}
	// 旧実装は不可分な書き込みをしないので、真であってはいけない。
	if f.AtomicPut {
		t.Error("旧実装なのに AtomicPut が真になっている")
	}
}
