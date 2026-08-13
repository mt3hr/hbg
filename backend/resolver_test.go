package backend_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/mt3hr/hbg/backend"
	"github.com/mt3hr/hbg/storage"

	// 種別を登録するために読み込む
	_ "github.com/mt3hr/hbg/backend/local"
	_ "github.com/mt3hr/hbg/backend/memory"
)

// countingBackend は、何回組み立てられたかを数えるテスト用の種別です。
type countingBackend struct {
	mu       sync.Mutex
	builds   int
	failWith error
}

func (c *countingBackend) register(t *testing.T, typ string) {
	t.Helper()
	backend.Register(backend.Descriptor{
		Type:    typ,
		Summary: "テスト用",
		New: func(_ context.Context, name string, _ backend.Params) (storage.Storage, error) {
			c.mu.Lock()
			c.builds++
			failWith := c.failWith
			c.mu.Unlock()

			if failWith != nil {
				return nil, failWith
			}
			return newStub(name, typ), nil
		},
	})
}

func (c *countingBackend) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.builds
}

type stub struct {
	name string
	typ  string
	storage.Storage
}

func newStub(name, typ string) storage.Storage { return &stub{name: name, typ: typ} }

func (s *stub) Type() string                { return s.typ }
func (s *stub) Name() string                { return s.name }
func (s *stub) Features() *storage.Features { return &storage.Features{} }
func (s *stub) Close() error                { return nil }

// 使わないストレージが組み立てられないことを確認します。
//
// 以前はどのコマンドでも設定にあるすべてのストレージを構築しており、
// ローカルのファイルを一覧するだけでクラウドの認証が走っていました。
func TestResolverIsLazy(t *testing.T) {
	cloud := &countingBackend{}
	cloud.register(t, "test-lazy-cloud")

	r, err := backend.NewResolver([]backend.Entry{
		{Name: "local", Type: "local"},
		{Name: "cloud", Type: "test-lazy-cloud"},
	})
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	defer r.Close()

	// 名前を並べるだけでは組み立てない
	if names := r.Names(); len(names) != 2 {
		t.Errorf("Names = %v", names)
	}
	if cloud.count() != 0 {
		t.Errorf("名前を並べただけで %d 回組み立てられた", cloud.count())
	}

	// ローカルを取り出してもクラウドは組み立てられない
	if _, err := r.Get(context.Background(), "local"); err != nil {
		t.Fatalf("Get(local): %v", err)
	}
	if cloud.count() != 0 {
		t.Errorf("ローカルを使っただけでクラウドが %d 回組み立てられた", cloud.count())
	}
	if opened := r.OpenedNames(); len(opened) != 1 || opened[0] != "local" {
		t.Errorf("組み立て済み = %v, want [local]", opened)
	}

	// クラウドを取り出したときに初めて組み立てられる
	if _, err := r.Get(context.Background(), "cloud"); err != nil {
		t.Fatalf("Get(cloud): %v", err)
	}
	if cloud.count() != 1 {
		t.Errorf("組み立て回数 = %d, want 1", cloud.count())
	}
}

// 2回目以降は同じものが返ることを確認します。
func TestResolverReusesInstance(t *testing.T) {
	b := &countingBackend{}
	b.register(t, "test-reuse")

	r, err := backend.NewResolver([]backend.Entry{{Name: "s", Type: "test-reuse"}})
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	defer r.Close()

	first, err := r.Get(context.Background(), "s")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	second, err := r.Get(context.Background(), "s")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if first != second {
		t.Error("呼び出しごとに別のストレージが返っている")
	}
	if b.count() != 1 {
		t.Errorf("組み立て回数 = %d, want 1", b.count())
	}
}

// 設定の誤りは、接続を待たずに検出されることを確認します。
func TestNewResolverValidatesEagerly(t *testing.T) {
	b := &countingBackend{}
	b.register(t, "test-validate")

	t.Run("名前の重複", func(t *testing.T) {
		_, err := backend.NewResolver([]backend.Entry{
			{Name: "dup", Type: "test-validate"},
			{Name: "dup", Type: "local"},
		})
		if err == nil {
			t.Fatal("名前が重複しているのに成功した")
		}
		if !strings.Contains(err.Error(), "重複") {
			t.Errorf("err = %v", err)
		}
	})

	t.Run("知らない種別", func(t *testing.T) {
		_, err := backend.NewResolver([]backend.Entry{
			{Name: "x", Type: "そんな種別はない"},
		})
		if err == nil {
			t.Fatal("知らない種別なのに成功した")
		}
		if !strings.Contains(err.Error(), "不明") {
			t.Errorf("err = %v", err)
		}
	})

	t.Run("名前がない", func(t *testing.T) {
		_, err := backend.NewResolver([]backend.Entry{{Type: "local"}})
		if err == nil {
			t.Fatal("名前がないのに成功した")
		}
	})

	// 検証のためにストレージが組み立てられていないこと
	if b.count() != 0 {
		t.Errorf("設定の検証だけで %d 回組み立てられた", b.count())
	}
}

func TestResolverGetUnknownName(t *testing.T) {
	r, err := backend.NewResolver([]backend.Entry{{Name: "local", Type: "local"}})
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	defer r.Close()

	_, err = r.Get(context.Background(), "ない名前")
	if err == nil {
		t.Fatal("知らない名前なのに成功した")
	}
	// 設定にある名前を案内すること
	if !strings.Contains(err.Error(), "local") {
		t.Errorf("使える名前が案内されていない: %v", err)
	}
}

// 組み立てに失敗した場合、次回また試されることを確認します。
func TestResolverDoesNotCacheFailures(t *testing.T) {
	b := &countingBackend{failWith: errors.New("認証に失敗しました")}
	b.register(t, "test-failure")

	r, err := backend.NewResolver([]backend.Entry{{Name: "s", Type: "test-failure"}})
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	defer r.Close()

	if _, err := r.Get(context.Background(), "s"); err == nil {
		t.Fatal("失敗するはずが成功した")
	}
	if _, err := r.Get(context.Background(), "s"); err == nil {
		t.Fatal("失敗するはずが成功した")
	}
	if b.count() != 2 {
		t.Errorf("組み立て回数 = %d, want 2（失敗が記憶されている）", b.count())
	}
}

func TestDescriptorsAreSorted(t *testing.T) {
	ds := backend.Descriptors()
	if len(ds) < 2 {
		t.Skip("登録されている種別が少ないため飛ばします")
	}
	for i := 1; i < len(ds); i++ {
		if ds[i-1].Type > ds[i].Type {
			t.Errorf("種別が並び替えられていない: %s の後に %s", ds[i-1].Type, ds[i].Type)
		}
	}
}

// ヘルプの一覧が登録内容から作られることを確認します。
// 以前はヘルプに種別が直接書かれており、実装のない ftp が案内されていました。
func TestTypesReflectRegistrations(t *testing.T) {
	types := backend.Types()

	found := map[string]bool{}
	for _, typ := range types {
		found[typ] = true
	}
	for _, want := range []string{"local", "memory"} {
		if !found[want] {
			t.Errorf("登録済みの %q が一覧にない: %v", want, types)
		}
	}
	if found["ftp"] {
		t.Error("実装されていない ftp が一覧に出ている")
	}
}
