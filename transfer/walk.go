package transfer

import (
	"context"
	"path"
	"slices"
	"strings"

	"github.com/mt3hr/hbg/storage"
)

// scan は転送対象を探して tasks へ流します。
//
// 見つけたそばから送るので、走査の完了を待たずに転送が始まります。
func (e *engine) scan(ctx context.Context, srcInfo storage.FileInfo, tasks chan<- task) error {
	if !srcInfo.IsDir {
		// 1ファイルだけの転送
		return e.scanFile(ctx, srcInfo, e.opts.DstDir, tasks)
	}

	// ディレクトリを転送するときは、転送先にその名前のディレクトリを作る。
	// hbg copy local:/a/photos dropbox:/backup なら
	// dropbox:/backup/photos に入る。
	dstDir := path.Join(e.opts.DstDir, srcInfo.Name)
	return e.scanDir(ctx, srcInfo.Path, dstDir, tasks)
}

// scanDir はディレクトリを再帰的に走査します。
func (e *engine) scanDir(ctx context.Context, srcDir, dstDir string, tasks chan<- task) error {
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

	// 転送先の中身を名前で引けるようにする。
	dstByName := make(map[string]storage.FileInfo, len(dstEntries))
	for _, entry := range dstEntries {
		key := entry.Name
		if f := e.opts.Dst.Features(); f != nil && f.CaseInsensitive {
			key = strings.ToLower(key)
		}
		dstByName[key] = entry
	}

	compare := e.opts.Compare
	compare.ModifyWindow = resolveModifyWindow(compare.ModifyWindow, e.opts.Src, e.opts.Dst)

	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if e.ignored(entry.Name) {
			continue
		}

		if entry.IsDir {
			child := path.Join(dstDir, entry.Name)
			if err := e.scanDir(ctx, entry.Path, child, tasks); err != nil {
				return err
			}
			continue
		}

		e.scanFiles.Add(1)
		if entry.Size > 0 {
			e.scanBytes.Add(entry.Size)
		}
		e.reporter.ScanProgress(e.scanDirs.Load(), e.scanFiles.Load(), e.scanBytes.Load())

		lookupName := entry.Name
		if f := e.opts.Dst.Features(); f != nil && f.CaseInsensitive {
			lookupName = strings.ToLower(lookupName)
		}
		if compare.Decide(storage.FileInfo{
			Name:    lookupName,
			Size:    entry.Size,
			ModTime: entry.ModTime,
		}, dstByName) == ActionSkip {
			e.recordSkip(entry.Size)
			e.reporter.Skipped(entry.Name, entry.Size)
			continue
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case tasks <- task{srcPath: entry.Path, dstDir: dstDir, name: entry.Name, size: entry.Size}:
		}
	}
	return nil
}

// scanFile は1ファイルだけを対象にします。
func (e *engine) scanFile(ctx context.Context, info storage.FileInfo, dstDir string, tasks chan<- task) error {
	dstEntries, err := e.ensureDir(ctx, dstDir)
	if err != nil {
		return err
	}

	e.scanFiles.Add(1)
	if info.Size > 0 {
		e.scanBytes.Add(info.Size)
	}
	e.reporter.ScanProgress(0, 1, e.scanBytes.Load())

	dstByName := make(map[string]storage.FileInfo, len(dstEntries))
	for _, entry := range dstEntries {
		dstByName[entry.Name] = entry
	}

	compare := e.opts.Compare
	compare.ModifyWindow = resolveModifyWindow(compare.ModifyWindow, e.opts.Src, e.opts.Dst)

	if compare.Decide(info, dstByName) == ActionSkip {
		e.recordSkip(info.Size)
		e.reporter.Skipped(info.Name, info.Size)
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case tasks <- task{srcPath: info.Path, dstDir: dstDir, name: info.Name, size: info.Size}:
	}
	return nil
}

// ignored は、その名前が転送対象外かを返します。
func (e *engine) ignored(name string) bool {
	return slices.Contains(e.opts.Ignore, name)
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
