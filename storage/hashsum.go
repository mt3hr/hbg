package storage

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

	"golang.org/x/sync/errgroup"
)

// HashFile はファイルのハッシュを、どのストレージからでも求めます。
//
// まず GetHash と同じく、メタデータや Hasher から得られるものを使います。
// それができない（ErrUnsupported）ときに限り、内容を読み出して計算します。
// そのため、ハッシュを持たないストレージでは内容をすべて転送することになります。
//
// 転送の比較（--checksum）には使わないでください。比較のためだけに
// 全件を読み出すことになるので、そちらは GetHash を使います。
func HashFile(ctx context.Context, s Storage, info FileInfo, ht HashType) (string, error) {
	if info.IsDir {
		return "", fmt.Errorf("%w: %s", ErrIsDir, info.Path)
	}
	// 読み出しても計算できない種類なら、通信する前に断る。
	h, err := NewHash(ht)
	if err != nil {
		return "", err
	}

	sum, err := GetHash(ctx, s, &info, ht)
	if err == nil && sum != "" {
		return strings.ToLower(sum), nil
	}
	if err != nil && !errors.Is(err, ErrUnsupported) {
		return "", err
	}

	rc, _, err := s.Open(ctx, info.Path)
	if err != nil {
		return "", err
	}
	defer rc.Close()

	if _, err := io.Copy(h, rc); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// TreeEntry はディレクトリの中の1ファイルぶんの結果です。
type TreeEntry struct {
	// RelPath はたどり始めたディレクトリからの相対パスです。区切りは "/" です。
	RelPath string
	// Size はバイト数です。分からなければ SizeUnknown です。
	Size int64
	// Hash は求めたハッシュです。失敗したときは空です。
	Hash string
	// Err は求められなかった理由です。
	Err error
}

// TreeResult はディレクトリ全体の結果です。
type TreeResult struct {
	// Entries は相対パスのバイト順に並べた全件です。失敗した件も含みます。
	Entries []TreeEntry
	// Sum はディレクトリ全体を1つにまとめた値です。
	//
	// 「<ハッシュ>  <相対パス>\n」を Entries の順に連ねた一覧を、
	// もう一度同じ種類でハッシュしたものです。sha256 なら、一覧を
	// sha256sum に通した値と一致します。
	//
	// 1件でも失敗していれば空です。欠けた一覧から作った値を、
	// 正しい値のように見せないためです。
	Sum string
	// Failed は求められなかった件数です。
	Failed int
}

// HashTree は dir の下にあるファイルをすべてたどり、それぞれのハッシュと、
// ディレクトリ全体をまとめた値を求めます。
//
// ファイルごとのハッシュは HashFile で求め、workers 件まで並行して処理します。
// 1件の失敗では止まらず、TreeEntry.Err に記録して続けます。
// 一覧の取得に失敗したときと ctx が取り消されたときは、全体を止めてエラーを返します。
//
// 空のディレクトリは結果に含まれず、まとめた値にも影響しません。
// 空のディレクトリを表せないストレージがあるので、どこで求めても
// 同じ内容なら同じ値になるようにするためです。
//
// fn が nil でなければ、1件求めるたびに呼びます。順番は終わった順で、
// 同時に呼ばれることはありません。進み具合の表示に使えます。
func HashTree(ctx context.Context, s Storage, dir string, ht HashType, workers int, fn func(TreeEntry)) (TreeResult, error) {
	if _, err := NewHash(ht); err != nil {
		return TreeResult{}, err
	}
	if workers < 1 {
		workers = 1
	}

	var (
		mu      sync.Mutex
		entries []TreeEntry
	)
	record := func(e TreeEntry) {
		mu.Lock()
		defer mu.Unlock()
		entries = append(entries, e)
		if fn != nil {
			fn(e)
		}
	}

	g, gctx := errgroup.WithContext(ctx)
	// 1つはたどる役に回すので、ハッシュを求める役と合わせて workers+1。
	g.SetLimit(workers + 1)

	var walk func(dir, rel string) error
	walk = func(dir, rel string) error {
		// 子のディレクトリは一覧を閉じてからたどる。
		// List のコールバックの中で List を呼ぶと、一覧を開いたまま
		// 次の通信を始めることになり、それを許さないバックエンドがある。
		type subdir struct{ path, rel string }
		var subdirs []subdir
		err := s.List(gctx, dir, func(fi FileInfo) error {
			childRel := fi.Name
			if rel != "" {
				childRel = rel + "/" + fi.Name
			}
			if fi.IsDir {
				subdirs = append(subdirs, subdir{path: fi.Path, rel: childRel})
				return nil
			}
			g.Go(func() error {
				sum, err := HashFile(gctx, s, fi, ht)
				record(TreeEntry{RelPath: childRel, Size: fi.Size, Hash: sum, Err: err})
				return nil
			})
			return nil
		})
		if err != nil {
			return err
		}
		for _, d := range subdirs {
			if err := walk(d.path, d.rel); err != nil {
				return err
			}
		}
		return nil
	}

	g.Go(func() error { return walk(dir, "") })
	if err := g.Wait(); err != nil {
		return TreeResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return TreeResult{}, err
	}

	sort.Slice(entries, func(i, j int) bool { return entries[i].RelPath < entries[j].RelPath })

	result := TreeResult{Entries: entries}
	for _, e := range entries {
		if e.Err != nil {
			result.Failed++
		}
	}
	if result.Failed == 0 {
		sum, err := treeSum(entries, ht)
		if err != nil {
			return TreeResult{}, err
		}
		result.Sum = sum
	}
	return result, nil
}

// FormatHashLine は sha256sum と同じ「<ハッシュ>  <パス>」の1行を返します。
// 末尾の改行は含みません。
func FormatHashLine(sum, p string) string {
	return sum + "  " + p
}

// treeSum は一覧をまとめた値を求めます。
func treeSum(entries []TreeEntry, ht HashType) (string, error) {
	h, err := NewHash(ht)
	if err != nil {
		return "", err
	}
	for _, e := range entries {
		if _, err := io.WriteString(h, FormatHashLine(e.Hash, e.RelPath)+"\n"); err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
