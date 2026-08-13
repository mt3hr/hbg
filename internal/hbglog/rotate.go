package hbglog

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// ローテーションの既定値。
const (
	DefaultMaxSizeBytes = 10 * 1024 * 1024 // 10MiB
	DefaultMaxBackups   = 5
)

// rotatingFile はサイズで世代交代するログファイルです。
//
// 外部ライブラリ（lumberjack など）に依存せず、必要な範囲だけを実装します。
// 世代は hbg_info.log.1, hbg_info.log.2 ... と番号が大きいほど古くなります。
type rotatingFile struct {
	path       string
	maxSize    int64
	maxBackups int

	mu   sync.Mutex
	file *os.File
	size int64
}

func newRotatingFile(path string, maxSize int64, maxBackups int) (*rotatingFile, error) {
	if maxSize <= 0 {
		maxSize = DefaultMaxSizeBytes
	}
	if maxBackups < 0 {
		maxBackups = 0
	}

	r := &rotatingFile{
		path:       path,
		maxSize:    maxSize,
		maxBackups: maxBackups,
	}
	if err := r.open(); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *rotatingFile) open() error {
	if err := os.MkdirAll(filepath.Dir(r.path), 0o700); err != nil {
		return fmt.Errorf("ログディレクトリを作成できませんでした: %w", err)
	}

	f, err := os.OpenFile(r.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("ログファイルを開けませんでした %s: %w", r.path, err)
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return fmt.Errorf("ログファイルの情報を取得できませんでした %s: %w", r.path, err)
	}

	r.file = f
	r.size = info.Size()
	return nil
}

// Write はログを書き込みます。上限を超えたら世代交代します。
func (r *rotatingFile) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.file == nil {
		return 0, os.ErrClosed
	}

	// 1レコードは分割せず、書く前に判定する。
	if r.size+int64(len(p)) > r.maxSize && r.size > 0 {
		if err := r.rotate(); err != nil {
			// ローテーションに失敗しても書き込み自体は続ける。
			// ログのために本来の処理を止めるべきではない。
			fmt.Fprintf(os.Stderr, "hbg: 警告: ログを世代交代できませんでした: %v\n", err)
		}
	}

	n, err := r.file.Write(p)
	r.size += int64(n)
	return n, err
}

// rotate は現在のファイルを退避し、新しいファイルを開きます。
func (r *rotatingFile) rotate() error {
	if err := r.file.Close(); err != nil {
		return err
	}

	// 古い世代から順に番号をずらす。
	for i := r.maxBackups; i >= 1; i-- {
		src := fmt.Sprintf("%s.%d", r.path, i)
		if _, err := os.Stat(src); err != nil {
			continue
		}
		if i == r.maxBackups {
			// 一番古い世代は捨てる
			_ = os.Remove(src)
			continue
		}
		_ = os.Rename(src, fmt.Sprintf("%s.%d", r.path, i+1))
	}

	if r.maxBackups > 0 {
		if err := os.Rename(r.path, r.path+".1"); err != nil {
			// 退避できない場合は truncate して続ける
			_ = os.Remove(r.path)
		}
	} else {
		_ = os.Remove(r.path)
	}

	return r.open()
}

// Close はファイルを閉じます。
func (r *rotatingFile) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.file == nil {
		return nil
	}
	err := r.file.Close()
	r.file = nil
	return err
}

// removeOldLogs は保持期間を過ぎたログを削除します。
// maxAge が 0 以下の場合は何もしません。
func removeOldLogs(dir string, maxAge time.Duration) {
	if maxAge <= 0 {
		return
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}

	cutoff := time.Now().Add(-maxAge)
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasPrefix(e.Name(), "hbg") || !strings.Contains(e.Name(), ".log") {
			continue
		}
		names = append(names, e.Name())
	}
	sort.Strings(names)

	for _, name := range names {
		path := filepath.Join(dir, name)
		info, err := os.Stat(path)
		if err != nil || info.ModTime().After(cutoff) {
			continue
		}
		_ = os.Remove(path)
	}
}
