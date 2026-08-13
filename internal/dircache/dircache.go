// Package dircache は、作ったディレクトリを覚えておくための小さな道具です。
//
// WebDAV・OneDrive・FTP はどれも、親ディレクトリがない場所への書き込みを
// 断ります。かといって書き込みのたびに用意し直すと、要求の数が倍になり、
// 同じディレクトリへ並行して書いたときには衝突もします。
//
// 断られてから作ってやり直す、という順にはできません。断られた時点で
// 本文の送信が始まっていることがあり、読み手を巻き戻せないためです。
//
// そこで、一度作った（あるいはあると分かった）ものを覚えておきます。
// 転送の側が先にディレクトリを用意するので、ふだんは書き込みのときに
// 要求が増えません。
package dircache

import (
	"context"
	"strings"
	"sync"
)

// Cache は「ある」と分かっているディレクトリの集まりです。
// ゼロ値のまま使えます。
type Cache struct {
	mu    sync.Mutex
	known map[string]struct{}
}

// Ensure はディレクトリを用意します。
//
// すでに用意したことがあれば create は呼ばれません。
// 同じディレクトリへ並行して呼ばれても、create は1度だけ走ります。
func (c *Cache) Ensure(ctx context.Context, dir string, create func(context.Context, string) error) error {
	if !meaningful(dir) || c.Knows(dir) {
		return nil
	}

	// 作成がぶつからないよう、ここは1つずつ行う。
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.known[dir]; ok {
		return nil
	}
	if err := create(ctx, dir); err != nil {
		return err
	}
	c.rememberLocked(dir)
	return nil
}

// Knows は、そのディレクトリを用意済みかどうかを返します。
func (c *Cache) Knows(dir string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.known[dir]
	return ok
}

// Remember はディレクトリを用意済みとして覚えます。
func (c *Cache) Remember(dir string) {
	if !meaningful(dir) {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rememberLocked(dir)
}

func (c *Cache) rememberLocked(dir string) {
	if c.known == nil {
		c.known = map[string]struct{}{}
	}
	c.known[dir] = struct{}{}
}

// Forget は、そのディレクトリと配下の記憶を捨てます。
// 消したあとに「まだある」と思い込まないようにするためのものです。
func (c *Cache) Forget(dir string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.known, dir)

	prefix := strings.TrimSuffix(dir, "/") + "/"
	for k := range c.known {
		if strings.HasPrefix(k, prefix) {
			delete(c.known, k)
		}
	}
}

// meaningful は、覚える意味のあるパスかを返します。
// 起点そのものは常にあるので覚えません。
func meaningful(dir string) bool {
	switch dir {
	case "", ".", "/":
		return false
	}
	return true
}
