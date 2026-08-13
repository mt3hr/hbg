package transfer

import (
	"fmt"
	"path"
	"strings"

	glb "github.com/gobwas/glob"
)

// Filter は転送対象を絞り込む条件です。
type Filter struct {
	// Ignore は名前が完全に一致したら除外します。
	Ignore []string
	// Include が空でなければ、いずれかに一致するものだけを対象にします。
	Include []glb.Glob
	// Exclude はいずれかに一致するものを除外します。
	Exclude []glb.Glob
	// MinSize より小さいものを除外します。0 なら制限しません。
	MinSize int64
	// MaxSize より大きいものを除外します。0 なら制限しません。
	MaxSize int64
}

// FilterSpec は Filter を組み立てるための指定です。
type FilterSpec struct {
	Ignore  []string
	Include []string
	Exclude []string
	MinSize int64
	MaxSize int64
}

// NewFilter はパターンを解釈して Filter を作ります。
//
// パターンの誤りはここでエラーになります。
// 以前はコピー元のパスをそのままパターンとして MustCompile に渡しており、
// [ や { を含むファイル名を指定するとパニックしていました。
func NewFilter(spec FilterSpec) (*Filter, error) {
	f := &Filter{
		Ignore:  spec.Ignore,
		MinSize: spec.MinSize,
		MaxSize: spec.MaxSize,
	}

	var err error
	if f.Include, err = compileGlobs(spec.Include, "--include"); err != nil {
		return nil, err
	}
	if f.Exclude, err = compileGlobs(spec.Exclude, "--exclude"); err != nil {
		return nil, err
	}
	return f, nil
}

func compileGlobs(patterns []string, flag string) ([]glb.Glob, error) {
	if len(patterns) == 0 {
		return nil, nil
	}

	out := make([]glb.Glob, 0, len(patterns))
	for _, p := range patterns {
		// 区切り文字を指定して、* がディレクトリの境界を越えないようにする。
		// 境界を越えたい場合は ** を使う。
		g, err := glb.Compile(p, '/')
		if err != nil {
			return nil, fmt.Errorf("%s のパターンが不正です %q: %w", flag, p, err)
		}
		out = append(out, g)
	}
	return out, nil
}

// Match は、そのファイルを転送対象とするかを返します。
//
// relPath はコピー元の起点からの相対パスです。
func (f *Filter) Match(relPath string, size int64) bool {
	if f == nil {
		return true
	}

	name := path.Base(relPath)

	// 名前が完全に一致するものは除外する。
	// ディレクトリにも適用されるので、その中身ごと対象外になる。
	for _, ignore := range f.Ignore {
		if name == ignore {
			return false
		}
	}

	// 相対パスと名前のどちらでも照合する。
	// 以前は名前の完全一致しか見ておらず、
	// 「特定のディレクトリの下だけ除く」といった指定ができなかった。
	for _, g := range f.Exclude {
		if g.Match(relPath) || g.Match(name) {
			return false
		}
	}

	if len(f.Include) > 0 {
		matched := false
		for _, g := range f.Include {
			if g.Match(relPath) || g.Match(name) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	if size >= 0 {
		if f.MinSize > 0 && size < f.MinSize {
			return false
		}
		if f.MaxSize > 0 && size > f.MaxSize {
			return false
		}
	}
	return true
}

// MatchDir は、そのディレクトリに入るかを返します。
//
// 除外の指定に当てはまるディレクトリは、中身ごと見ません。
// 含める指定はファイルに対して働くので、ここでは見ません。
func (f *Filter) MatchDir(relPath string) bool {
	if f == nil {
		return true
	}

	name := path.Base(relPath)
	for _, ignore := range f.Ignore {
		if name == ignore {
			return false
		}
	}
	for _, g := range f.Exclude {
		if g.Match(relPath) || g.Match(name) ||
			// "logs/**" のような指定でディレクトリ自体も除く
			g.Match(strings.TrimSuffix(relPath, "/")+"/") {
			return false
		}
	}
	return true
}
