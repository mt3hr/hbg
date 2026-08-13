package dircache

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestEnsureCreatesOnce(t *testing.T) {
	var c Cache
	ctx := context.Background()

	calls := 0
	create := func(context.Context, string) error {
		calls++
		return nil
	}

	for range 3 {
		if err := c.Ensure(ctx, "/a/b", create); err != nil {
			t.Fatalf("Ensure: %v", err)
		}
	}
	if calls != 1 {
		t.Errorf("作成の回数 = %d, want 1", calls)
	}
}

// 用意に失敗したら覚えないことを確かめます。
// 覚えてしまうと、次から「ある」ものとして扱ってしまいます。
func TestEnsureDoesNotRememberFailure(t *testing.T) {
	var c Cache
	ctx := context.Background()

	wantErr := errors.New("作れません")
	calls := 0
	create := func(context.Context, string) error {
		calls++
		if calls == 1 {
			return wantErr
		}
		return nil
	}

	if err := c.Ensure(ctx, "/a", create); !errors.Is(err, wantErr) {
		t.Fatalf("Ensure = %v, want %v", err, wantErr)
	}
	if c.Knows("/a") {
		t.Error("失敗したのに覚えている")
	}

	if err := c.Ensure(ctx, "/a", create); err != nil {
		t.Fatalf("2回目の Ensure: %v", err)
	}
	if !c.Knows("/a") {
		t.Error("成功したのに覚えていない")
	}
}

// 起点そのものは覚える必要がないことを確かめます。
func TestEnsureSkipsRoot(t *testing.T) {
	var c Cache
	ctx := context.Background()

	for _, dir := range []string{"", ".", "/"} {
		called := false
		if err := c.Ensure(ctx, dir, func(context.Context, string) error {
			called = true
			return nil
		}); err != nil {
			t.Fatalf("Ensure(%q): %v", dir, err)
		}
		if called {
			t.Errorf("Ensure(%q) で作成が呼ばれた", dir)
		}
	}
}

// 消したディレクトリを忘れることを確かめます。
//
// 覚えたままだと、同じ名前で作り直したときに「まだある」と
// 思い込んで、用意されていない場所へ書きにいってしまいます。
func TestForgetDropsDescendants(t *testing.T) {
	var c Cache

	c.Remember("/a")
	c.Remember("/a/b")
	c.Remember("/a/b/c")
	c.Remember("/ab")

	c.Forget("/a")

	for _, dir := range []string{"/a", "/a/b", "/a/b/c"} {
		if c.Knows(dir) {
			t.Errorf("%s を忘れていない", dir)
		}
	}
	// 名前が似ているだけのものは残ること。
	if !c.Knows("/ab") {
		t.Error("関係のない /ab まで忘れている")
	}
}

// 並行して呼ばれても作成が1度だけになることを確かめます。
func TestEnsureIsSerialized(t *testing.T) {
	var c Cache
	ctx := context.Background()

	var mu sync.Mutex
	calls := 0

	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = c.Ensure(ctx, "/同じ場所", func(context.Context, string) error {
				mu.Lock()
				calls++
				mu.Unlock()
				return nil
			})
		}()
	}
	wg.Wait()

	if calls != 1 {
		t.Errorf("作成の回数 = %d, want 1", calls)
	}
}
