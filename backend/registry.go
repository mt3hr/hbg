// Package backend は、ストレージの種別を登録して名前から解決する仕組みです。
//
// 以前は設定からストレージを組み立てる関数に、種別ごとの分岐が
// 直接書き並べられていました。そのため新しい種別を足すたびに
// 6箇所ほどを書き換える必要があり、さらに、どのコマンドを実行しても
// 設定にあるすべてのストレージが構築されていました。
// ローカルのファイルを一覧するだけでクラウドの認証が走る状態です。
//
// ここでは種別ごとの構築方法を登録制にし、実際に使うストレージだけを
// 必要になった時点で組み立てます。
package backend

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/mt3hr/hbg/storage"
)

// Params はバックエンド固有の設定です。
// 設定ファイルの、名前と種別を除いた項目が入ります。
type Params map[string]string

// Get は値を取り出します。無ければ空文字を返します。
func (p Params) Get(key string) string { return p[strings.ToLower(key)] }

// Set は値を設定します。
func (p Params) Set(key, value string) { p[strings.ToLower(key)] = value }

// Descriptor はバックエンドの種別1つぶんの定義です。
type Descriptor struct {
	// Type は設定ファイルで指定する種別名です。
	Type string
	// Summary はヘルプに1行で出す説明です。
	Summary string
	// ConfigDoc は設定ファイルの雛形に入れる断片です。
	ConfigDoc string
	// New はストレージを組み立てます。
	// ここで初めてネットワークへの接続や認証を行います。
	New func(ctx context.Context, name string, params Params) (storage.Storage, error)
}

var (
	mu          sync.RWMutex
	descriptors = map[string]Descriptor{}
)

// Register はバックエンドの種別を登録します。
// 各バックエンドの init から呼びます。同じ種別を二重に登録すると panic します。
func Register(d Descriptor) {
	mu.Lock()
	defer mu.Unlock()

	if d.Type == "" {
		panic("backend: 種別名が空です")
	}
	if d.New == nil {
		panic("backend: " + d.Type + " の New が設定されていません")
	}
	if _, exists := descriptors[d.Type]; exists {
		panic("backend: 種別 " + d.Type + " が二重に登録されています")
	}
	descriptors[d.Type] = d
}

// Lookup は種別に対応する定義を返します。
func Lookup(typ string) (Descriptor, bool) {
	mu.RLock()
	defer mu.RUnlock()

	d, ok := descriptors[typ]
	return d, ok
}

// Descriptors は登録されているすべての定義を種別名の順で返します。
//
// ヘルプの一覧や、設定ファイルの雛形はここから組み立てます。
// 以前はヘルプの文字列に種別が直接書かれており、実装のない ftp が
// いつまでも案内されていました。
func Descriptors() []Descriptor {
	mu.RLock()
	defer mu.RUnlock()

	out := make([]Descriptor, 0, len(descriptors))
	for _, d := range descriptors {
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Type < out[j].Type })
	return out
}

// Types は登録されている種別名を並べて返します。
func Types() []string {
	ds := Descriptors()
	out := make([]string, 0, len(ds))
	for _, d := range ds {
		out = append(out, d.Type)
	}
	return out
}

// New は種別と名前からストレージを組み立てます。
func New(ctx context.Context, typ, name string, params Params) (storage.Storage, error) {
	d, ok := Lookup(typ)
	if !ok {
		return nil, fmt.Errorf("知らないストレージの種別です: %q（使えるのは %s）",
			typ, strings.Join(Types(), ", "))
	}
	if params == nil {
		params = Params{}
	}
	return d.New(ctx, name, params)
}
