package transfer

import (
	"context"
	"sort"
	"strings"

	"github.com/mt3hr/hbg/storage"
)

// 同期での削除は、転送とは危険の度合いが違います。転送は失敗しても
// 元のファイルが残りますが、削除は取り返しがつきません。
// そのため、次の決まりで扱います。
//
//   - 転送に1件でも失敗があれば削除しない。
//     コピー元を読めなかっただけで「向こうには無い」と判断してしまうと、
//     取っておきたいものを消すことになる。
//
//   - 絞り込みで対象外になったものは消さない。
//     --include で絞ったときに、対象外のものが消えては困る。
//
//   - 深いものから消す。ディレクトリは中身が無くならないと消せない。
//
//   - コピー元にないディレクトリは、中をたどって1件ずつ控える。
//     まるごと消すほうが要求は少ないが、それだと絞り込みで守った
//     はずのものまで巻き込む。

// partSuffix は書き込み中のファイルに付く印です。
// 各ストレージが同じものを使っています。
const partSuffix = ".hbgpart"

// extraneous は、コピー元にないのにコピー先にあるものです。
type extraneous struct {
	// path はコピー先のパスです。
	path string
	// rel はコピー元の起点からの相対パスです。表示に使います。
	rel string
	// isDir はディレクトリかどうかです。
	isDir bool
	size  int64
}

// collectExtraneous は、コピー元にないものを控えます。
//
// 走査の途中で呼ばれます。実際に消すのは、転送がすべて終わってからです。
func (e *engine) collectExtraneous(
	ctx context.Context,
	dstEntries []storage.FileInfo,
	srcNames map[string]struct{},
	relDir string,
) error {
	if !e.opts.Delete {
		return nil
	}

	for _, entry := range dstEntries {
		if _, ok := srcNames[e.nameKey(entry.Name)]; ok {
			continue
		}
		if strings.HasSuffix(entry.Name, partSuffix) {
			// 書き込み中のものは、転送の側が片付ける。
			continue
		}

		rel := joinRel(relDir, entry.Name)
		if !e.deletable(rel, entry) {
			continue
		}

		if entry.IsDir {
			// 中をたどってから、このディレクトリ自身を控える。
			if err := e.collectDirContents(ctx, entry.Path, rel); err != nil {
				return err
			}
		}
		e.addExtraneous(entry, rel)
	}
	return nil
}

// collectDirContents はコピー元にないディレクトリの中を控えます。
func (e *engine) collectDirContents(ctx context.Context, dir, relDir string) error {
	entries, err := storage.ListAll(ctx, e.opts.Dst, dir)
	if err != nil {
		if storage.IsNotFound(err) {
			return nil
		}
		return err
	}

	for _, entry := range entries {
		rel := joinRel(relDir, entry.Name)
		if !e.deletable(rel, entry) {
			continue
		}
		if entry.IsDir {
			if err := e.collectDirContents(ctx, entry.Path, rel); err != nil {
				return err
			}
		}
		e.addExtraneous(entry, rel)
	}
	return nil
}

// addExtraneous は1件を控えます。
func (e *engine) addExtraneous(entry storage.FileInfo, rel string) {
	e.extraMu.Lock()
	defer e.extraMu.Unlock()

	e.extra = append(e.extra, extraneous{
		path:  entry.Path,
		rel:   rel,
		isDir: entry.IsDir,
		size:  entry.Size,
	})
}

// deletable は、それを消してよいかを返します。
func (e *engine) deletable(rel string, entry storage.FileInfo) bool {
	if e.opts.Filter == nil {
		return true
	}
	// 絞り込みで対象外になったものは、そもそも転送していない。
	// 転送していないものを消すのは筋が通らない。
	if entry.IsDir {
		return e.opts.Filter.MatchDir(rel)
	}
	return e.opts.Filter.Match(rel, entry.Size)
}

// joinRel は相対パスを繋げます。
func joinRel(dir, name string) string {
	if dir == "" {
		return name
	}
	return dir + "/" + name
}

// deleteExtraneous は控えておいたものを消します。
//
// 転送に失敗があった場合は何もしません。
func (e *engine) deleteExtraneous(ctx context.Context) {
	if !e.opts.Delete {
		return
	}

	e.extraMu.Lock()
	targets := e.extra
	e.extra = nil
	e.extraMu.Unlock()

	if len(targets) == 0 {
		return
	}

	if failed := e.failedCount(); failed > 0 {
		// 読めなかったものを「向こうには無い」と取り違えたくない。
		e.reporter.Logf("転送に %d件失敗したため、削除は行いませんでした（%d件が対象でした）",
			failed, len(targets))
		return
	}

	// 深いものから消す。ディレクトリは中身が無くならないと消せない。
	sort.Slice(targets, func(i, j int) bool {
		return depth(targets[i].rel) > depth(targets[j].rel)
	})

	for _, target := range targets {
		if err := ctx.Err(); err != nil {
			return
		}
		e.deleteOne(ctx, target)
	}
}

// deleteOne は1件を消します。
func (e *engine) deleteOne(ctx context.Context, target extraneous) {
	e.notifyDecision(DecisionEvent{
		Path:   target.rel,
		Size:   target.size,
		Action: ActionDelete,
		Reason: "コピー元にない",
	})

	if e.opts.DryRun {
		e.recordDeleted()
		e.reporter.Logf("削除（予定） %s", target.rel)
		return
	}

	err := doWithRetry(ctx, e.opts.Retry, func(ctx context.Context, _ int) error {
		if waitErr := e.limits.wait(ctx, e.opts.Dst); waitErr != nil {
			return waitErr
		}
		return e.opts.Dst.Remove(ctx, target.path)
	}, nil).err

	if err != nil {
		if storage.IsNotFound(err) {
			// 先に親ごと消えている。数えるだけにする。
			e.recordDeleted()
			return
		}
		e.recordDeleteFailure(err)
		e.reporter.Logf("削除に失敗しました %s: %v", target.rel, err)
		return
	}

	e.recordDeleted()
	e.reporter.Logf("削除 %s", target.rel)
}

// depth はパスの深さを返します。
func depth(rel string) int {
	if rel == "" {
		return 0
	}
	return strings.Count(rel, "/") + 1
}
