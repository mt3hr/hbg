package transfer

import (
	"context"
	"path"
	"strings"

	"github.com/mt3hr/hbg/storage"
)

// scan は転送対象を探して tasks へ流します。
//
// 見つけたそばから送るので、走査の完了を待たずに転送が始まります。
func (e *engine) scan(ctx context.Context, srcInfo storage.FileInfo, tasks chan<- task) error {
	if !srcInfo.IsDir {
		// 1ファイルだけの転送
		return e.scanFile(ctx, srcInfo, e.opts.DstDir, srcInfo.Name, tasks)
	}

	// ディレクトリを転送するときは、転送先にその名前のディレクトリを作る。
	// hbg copy local:/a/photos dropbox:/backup なら
	// dropbox:/backup/photos に入る。
	dstDir := path.Join(e.opts.DstDir, srcInfo.Name)
	return e.scanDir(ctx, srcInfo.Path, dstDir, "", tasks)
}

// scanDir はディレクトリを再帰的に走査します。
//
// relDir はコピー元の起点からの相対パスです。絞り込みに使います。
func (e *engine) scanDir(ctx context.Context, srcDir, dstDir, relDir string, tasks chan<- task) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	// 転送先のディレクトリを先に用意する。
	//
	// 中身が空でもここで作られるので、空のディレクトリも転送先に残る。
	// 以前は転送先のパスを子ファイルの親から導いていたため、
	// 子を持たないディレクトリは作られず、しかも続く一覧が失敗していた。
	dstEntries, err := e.ensureDir(ctx, dstDir)
	if err != nil {
		return err
	}

	// 転送元を一覧する。
	// 全件をここで持つのは1ディレクトリぶんだけなので、
	// 件数が増えても使用メモリは膨らまない。
	if limitErr := e.limits.wait(ctx, e.opts.Src); limitErr != nil {
		return limitErr
	}
	entries, err := storage.ListAll(ctx, e.opts.Src, srcDir)
	if err != nil {
		return err
	}

	e.scanDirs.Add(1)

	dstByName := e.indexByName(dstEntries)

	// 同期での削除のために、コピー元にある名前を控えておく。
	srcNames := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		srcNames[e.nameKey(entry.Name)] = struct{}{}
	}
	if err := e.collectExtraneous(ctx, dstEntries, srcNames, relDir); err != nil {
		return err
	}

	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}

		rel := path.Join(relDir, entry.Name)

		if entry.IsDir {
			if !e.opts.Filter.MatchDir(rel) {
				continue
			}
			child := path.Join(dstDir, entry.Name)
			if err := e.scanDir(ctx, entry.Path, child, rel, tasks); err != nil {
				return err
			}
			continue
		}

		if !e.opts.Filter.Match(rel, entry.Size) {
			continue
		}

		e.scanFiles.Add(1)
		if entry.Size > 0 {
			e.scanBytes.Add(entry.Size)
		}
		e.reporter.ScanProgress(e.scanDirs.Load(), e.scanFiles.Load(), e.scanBytes.Load())

		if err := e.considerFile(ctx, entry, dstDir, rel, dstByName, tasks); err != nil {
			return err
		}
	}
	return nil
}

// scanFile は1ファイルだけを対象にします。
func (e *engine) scanFile(ctx context.Context, info storage.FileInfo, dstDir, rel string, tasks chan<- task) error {
	dstEntries, err := e.ensureDir(ctx, dstDir)
	if err != nil {
		return err
	}

	if !e.opts.Filter.Match(rel, info.Size) {
		return nil
	}

	e.scanFiles.Add(1)
	if info.Size > 0 {
		e.scanBytes.Add(info.Size)
	}
	e.reporter.ScanProgress(0, 1, e.scanBytes.Load())

	return e.considerFile(ctx, info, dstDir, rel, e.indexByName(dstEntries), tasks)
}

// considerFile は1ファイルの転送要否を判断し、必要なら転送の指示を出します。
func (e *engine) considerFile(
	ctx context.Context,
	srcInfo storage.FileInfo,
	dstDir, rel string,
	dstByName map[string]storage.FileInfo,
	tasks chan<- task,
) error {
	var dstInfo *storage.FileInfo
	if found, ok := dstByName[e.nameKey(srcInfo.Name)]; ok {
		dstInfo = &found
	}

	action, reason, err := e.comparer.Decide(ctx, srcInfo, dstInfo)
	if err != nil {
		// 判断に失敗した場合は、安全側に倒して転送する。
		e.reporter.Logf("警告: %s の判断に失敗したため転送します: %v", rel, err)
		action = ActionCopy
	}

	e.notifyDecision(DecisionEvent{
		Path:   rel,
		Size:   srcInfo.Size,
		Action: action,
		Reason: reason,
	})

	if action == ActionSkip {
		e.recordSkip(srcInfo.Size)
		e.reporter.Skipped(srcInfo.Name, srcInfo.Size)
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case tasks <- task{
		srcPath: srcInfo.Path,
		dstDir:  dstDir,
		name:    srcInfo.Name,
		relPath: rel,
		size:    srcInfo.Size,
	}:
	}
	return nil
}

// indexByName は転送先の一覧を名前で引けるようにします。
func (e *engine) indexByName(entries []storage.FileInfo) map[string]storage.FileInfo {
	out := make(map[string]storage.FileInfo, len(entries))
	for _, entry := range entries {
		out[e.nameKey(entry.Name)] = entry
	}
	return out
}

// nameKey は照合に使う名前を返します。
// 大文字小文字を区別しないストレージでは小文字に揃えます。
func (e *engine) nameKey(name string) string {
	if f := e.opts.Dst.Features(); f != nil && f.CaseInsensitive {
		return strings.ToLower(name)
	}
	return name
}

// ensureDir は転送先のディレクトリを用意し、その中身を返します。
//
// 同じディレクトリを何度も作らないよう、作成済みのものは覚えておきます。
func (e *engine) ensureDir(ctx context.Context, dir string) ([]storage.FileInfo, error) {
	if e.opts.DryRun {
		// 実際には作らないので、中身は空として扱う。
		entries, err := storage.ListAll(ctx, e.opts.Dst, dir)
		if err != nil {
			return nil, nil //nolint:nilerr // まだ無いだけなので空として続ける
		}
		return entries, nil
	}

	if err := e.limits.wait(ctx, e.opts.Dst); err != nil {
		return nil, err
	}

	entries, err := storage.ListAll(ctx, e.opts.Dst, dir)
	if err == nil {
		e.markDirMade(dir)
		return entries, nil
	}

	// 一覧できないのは、まだ無いからかもしれない。作ってから読み直す。
	if e.dirAlreadyMade(dir) {
		return nil, err
	}

	mkErr := doWithRetry(ctx, e.opts.Retry, func(ctx context.Context, _ int) error {
		if waitErr := e.limits.wait(ctx, e.opts.Dst); waitErr != nil {
			return waitErr
		}
		return e.opts.Dst.Mkdir(ctx, dir)
	}, nil).err
	if mkErr != nil {
		return nil, mkErr
	}
	e.markDirMade(dir)

	if limitErr := e.limits.wait(ctx, e.opts.Dst); limitErr != nil {
		return nil, limitErr
	}
	entries, err = storage.ListAll(ctx, e.opts.Dst, dir)
	if err != nil {
		return nil, err
	}
	return entries, nil
}

func (e *engine) markDirMade(dir string) {
	e.dirsMu.Lock()
	defer e.dirsMu.Unlock()
	e.madeDirs[dir] = struct{}{}
}

func (e *engine) dirAlreadyMade(dir string) bool {
	e.dirsMu.Lock()
	defer e.dirsMu.Unlock()
	_, ok := e.madeDirs[dir]
	return ok
}
