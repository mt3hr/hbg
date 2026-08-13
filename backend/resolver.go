package backend

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/mt3hr/hbg/storage"
)

// Entry は設定ファイルに書かれたストレージ1件ぶんです。
type Entry struct {
	// Name はコマンドで "名前:パス" として指定する名前です。
	Name string
	// Type はストレージの種別です。
	Type string
	// Params は種別ごとの設定です。
	Params Params
}

// Resolver は名前からストレージを解決します。
//
// 実際に使われるまでストレージを組み立てないので、
// 設定に書いてあるだけで使っていないクラウドの認証は走りません。
type Resolver struct {
	entries map[string]Entry
	order   []string

	mu   sync.Mutex
	open map[string]storage.Storage
}

// NewResolver は設定から Resolver を作ります。
//
// 名前の重複と、知らない種別はここで検出します。
// ネットワークへの接続は行わないので、設定の誤りは接続を待たずに分かります。
func NewResolver(entries []Entry) (*Resolver, error) {
	r := &Resolver{
		entries: make(map[string]Entry, len(entries)),
		order:   make([]string, 0, len(entries)),
		open:    map[string]storage.Storage{},
	}

	for _, e := range entries {
		if e.Name == "" {
			return nil, fmt.Errorf("ストレージに名前が設定されていません（種別 %q）", e.Type)
		}
		if _, exists := r.entries[e.Name]; exists {
			return nil, fmt.Errorf("ストレージ名が重複しています: %q", e.Name)
		}
		if _, ok := Lookup(e.Type); !ok {
			return nil, fmt.Errorf("ストレージ %q の種別が不明です: %q（使えるのは %s）",
				e.Name, e.Type, strings.Join(Types(), ", "))
		}

		r.entries[e.Name] = e
		r.order = append(r.order, e.Name)
	}
	return r, nil
}

// Names は設定されているストレージ名を並べて返します。
// ストレージの組み立ては行いません。
func (r *Resolver) Names() []string {
	out := append([]string{}, r.order...)
	sort.Strings(out)
	return out
}

// Has は指定した名前が設定されているかを返します。
func (r *Resolver) Has(name string) bool {
	_, ok := r.entries[name]
	return ok
}

// TypeOf は指定した名前の種別を返します。
func (r *Resolver) TypeOf(name string) (string, bool) {
	e, ok := r.entries[name]
	if !ok {
		return "", false
	}
	return e.Type, true
}

// Get は名前からストレージを返します。
// 初めて呼ばれたときに組み立て、以降は同じものを返します。
func (r *Resolver) Get(ctx context.Context, name string) (storage.Storage, error) {
	e, ok := r.entries[name]
	if !ok {
		return nil, fmt.Errorf("ストレージ %q は設定にありません（設定にあるのは %s）",
			name, strings.Join(r.Names(), ", "))
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if s, ok := r.open[name]; ok {
		return s, nil
	}

	s, err := New(ctx, e.Type, e.Name, e.Params)
	if err != nil {
		return nil, err
	}
	r.open[name] = s
	return s, nil
}

// Close は組み立て済みのストレージをすべて閉じます。
func (r *Resolver) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var firstErr error
	for _, s := range r.open {
		if err := s.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	r.open = map[string]storage.Storage{}
	return firstErr
}

// OpenedNames は実際に組み立てられたストレージの名前を返します。
// 遅延して組み立てられていることを確認するために使います。
func (r *Resolver) OpenedNames() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]string, 0, len(r.open))
	for name := range r.open {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}
