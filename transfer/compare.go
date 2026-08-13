package transfer

import (
	"time"

	"github.com/mt3hr/hbg/storage"
)

// Action は1ファイルに対して行うことです。
type Action int

const (
	// ActionSkip は転送しないことを表します。
	ActionSkip Action = iota
	// ActionCopy は転送することを表します。
	ActionCopy
)

// ComparePolicy は転送の要否を判断する規則です。
type ComparePolicy struct {
	// ModifyWindow はこの時間以内の更新時刻の差を同一とみなします。
	ModifyWindow time.Duration
}

// DefaultComparePolicy は既定の判断規則です。
func DefaultComparePolicy() ComparePolicy {
	return ComparePolicy{ModifyWindow: time.Second}
}

// resolveModifyWindow は、両側の分解能から実際の許容幅を決めます。
func resolveModifyWindow(configured time.Duration, src, dst storage.Storage) time.Duration {
	window := configured

	// 更新時刻の分解能が粗いストレージに合わせる。
	// Dropbox は秒までしか保持しないので、それより細かく比べても意味がない。
	for _, s := range []storage.Storage{src, dst} {
		if f := s.Features(); f != nil && f.ModTimePrecision > window {
			window = f.ModTimePrecision
		}
	}
	return window
}

// Decide は、コピー元のファイルを転送すべきかを判断します。
//
// 判断の規則は「コピー先に同名のものがあり、更新時刻の差が
// 許容幅以内で、かつサイズが一致する」ならスキップ、です。
//
// 注意: 時刻の差は絶対値で見ています。そのためコピー先のほうが
// 新しくても、差が許容幅を超えていれば上書きされます。
// これは以前からの挙動で、次のフェーズで --update として
// 選べるようにする予定です。
func (p ComparePolicy) Decide(src storage.FileInfo, dstEntries map[string]storage.FileInfo) Action {
	dst, ok := dstEntries[src.Name]
	if !ok {
		return ActionCopy
	}

	if src.Size != dst.Size {
		return ActionCopy
	}

	diff := src.ModTime.UTC().Sub(dst.ModTime.UTC())
	if diff < 0 {
		diff = -diff
	}
	if diff > p.ModifyWindow {
		return ActionCopy
	}
	return ActionSkip
}
