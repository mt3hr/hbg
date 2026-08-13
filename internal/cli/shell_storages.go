package cli

import (
	"context"
	"sort"

	"github.com/mt3hr/hbg/backend"
	hbgstorage "github.com/mt3hr/hbg/storage"
)

// lazyStorages は、対話シェルが扱うストレージの集合です。
//
// シェルは名前の一覧や補完のために全体を見渡しますが、
// 実際に触るのは切り替えたストレージだけです。
// ここで組み立てを遅らせることで、シェルを開くだけで
// 設定にあるクラウドすべての認証が走るのを避けます。
type lazyStorages struct {
	ctx      context.Context
	resolver *backend.Resolver

	// paths はストレージごとの現在位置です。
	paths map[hbgstorage.Storage]string
	// byName は組み立て済みのストレージです。
	byName map[string]hbgstorage.Storage
}

func newLazyStorages(ctx context.Context, resolver *backend.Resolver) *lazyStorages {
	return &lazyStorages{
		ctx:      ctx,
		resolver: resolver,
		paths:    map[hbgstorage.Storage]string{},
		byName:   map[string]hbgstorage.Storage{},
	}
}

// names は設定されているストレージ名を返します。組み立ては行いません。
func (l *lazyStorages) names() []string {
	names := l.resolver.Names()
	sort.Strings(names)
	return names
}

// get は名前からストレージを返します。必要なら組み立てます。
func (l *lazyStorages) get(name string) (hbgstorage.Storage, error) {
	if s, ok := l.byName[name]; ok {
		return s, nil
	}

	s, err := l.resolver.Get(l.ctx, name)
	if err != nil {
		return nil, err
	}
	l.byName[name] = s
	if _, ok := l.paths[s]; !ok {
		l.paths[s] = "/"
	}
	return s, nil
}

// setPath はストレージの現在位置を設定します。
func (l *lazyStorages) setPath(s hbgstorage.Storage, p string) {
	l.paths[s] = p
}

// opened は組み立て済みのストレージを返します。
// 補完のように「今すぐ触れるもの」だけを対象にしたい場面で使います。
func (l *lazyStorages) opened() []hbgstorage.Storage {
	out := make([]hbgstorage.Storage, 0, len(l.byName))
	for _, s := range l.byName {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name() < out[j].Name() })
	return out
}
