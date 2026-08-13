package transfer

import (
	"context"
	"fmt"
	"strings"
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
	// ActionDelete はコピー先から消すことを表します。
	// 同期で --delete を指定したときにだけ使われます。
	ActionDelete
)

// CompareField は比較に使う項目です。
type CompareField string

const (
	// CompareSize はサイズを比べます。
	CompareSize CompareField = "size"
	// CompareModTime は最終更新時刻を比べます。
	CompareModTime CompareField = "modtime"
	// CompareHash は内容のハッシュを比べます。
	CompareHash CompareField = "hash"
)

// ParseCompareFields は "size,modtime" のような指定を解釈します。
func ParseCompareFields(s string) ([]CompareField, error) {
	fields := []CompareField{}
	for _, part := range strings.Split(s, ",") {
		switch f := CompareField(strings.ToLower(strings.TrimSpace(part))); f {
		case "":
			continue
		case CompareSize, CompareModTime, CompareHash:
			fields = append(fields, f)
		default:
			return nil, fmt.Errorf("知らない比較項目です: %q（size, modtime, hash のいずれか）", part)
		}
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("比較項目が指定されていません")
	}
	return fields, nil
}

// ComparePolicy は転送の要否を判断する規則です。
type ComparePolicy struct {
	// Fields は比較に使う項目です。
	Fields []CompareField
	// ModifyWindow はこの時間以内の更新時刻の差を同一とみなします。
	// 0 なら両側の分解能から自動的に決めます。
	ModifyWindow time.Duration
	// Update が真なら、コピー先のほうが新しい場合は転送しません。
	Update bool
	// IgnoreExisting が真なら、コピー先にあるものは内容を問わず転送しません。
	IgnoreExisting bool
}

// DefaultComparePolicy は既定の判断規則です。
func DefaultComparePolicy() ComparePolicy {
	return ComparePolicy{
		Fields: []CompareField{CompareSize, CompareModTime},
		Update: true,
	}
}

func (p ComparePolicy) uses(f CompareField) bool {
	for _, v := range p.Fields {
		if v == f {
			return true
		}
	}
	return false
}

// Comparer は、あるストレージの組み合わせに対する判断を行います。
//
// 許容幅とハッシュの種類は組み合わせによって決まるので、
// 転送のたびに決め直さなくて済むようここで解決しておきます。
type Comparer struct {
	policy   ComparePolicy
	src, dst storage.Storage
	window   time.Duration
	// hashType は両側が扱える共通のハッシュです。無ければ空です。
	hashType storage.HashType
}

// NewComparer は判断器を作ります。
//
// ハッシュでの比較を求められたのに共通のハッシュがない場合は、
// ここでエラーにします。黙ってサイズ比較に落とすと、
// 利用者は検証されたつもりで検証されていないことになるためです。
func NewComparer(policy ComparePolicy, src, dst storage.Storage) (*Comparer, error) {
	c := &Comparer{
		policy: policy,
		src:    src,
		dst:    dst,
		window: resolveModifyWindow(policy.ModifyWindow, src, dst),
	}

	if policy.uses(CompareHash) {
		ht, ok := commonHash(src, dst)
		if !ok {
			return nil, fmt.Errorf(
				"%s と %s の間に共通して使えるハッシュがありません。--compare からハッシュを外してください",
				src.Type(), dst.Type())
		}
		c.hashType = ht
	}

	if policy.uses(CompareModTime) && !modTimeUsable(dst) {
		// 更新時刻を保持できないストレージが相手なら、
		// 時刻での比較は成り立たない。黙って無視せず知らせる。
		return nil, fmt.Errorf(
			"%s は最終更新時刻を保持できないため、時刻での比較ができません。"+
				"--compare size または --compare size,hash を指定してください", dst.Type())
	}

	return c, nil
}

// Window は実際に使われる許容幅を返します。
func (c *Comparer) Window() time.Duration { return c.window }

// HashType は比較に使うハッシュの種類を返します。使わない場合は空です。
func (c *Comparer) HashType() storage.HashType { return c.hashType }

// Decide は、コピー元のファイルを転送すべきかを判断します。
// 2つめの戻り値は判断の理由です。表示や記録に使います。
func (c *Comparer) Decide(ctx context.Context, srcInfo storage.FileInfo, dstInfo *storage.FileInfo) (Action, string, error) {
	if dstInfo == nil {
		return ActionCopy, "コピー先にない", nil
	}
	if c.policy.IgnoreExisting {
		return ActionSkip, "コピー先にすでにある", nil
	}

	// 更新時刻をいちばん先に見る。
	//
	// 「コピー先のほうが新しければ上書きしない」という指定は、
	// サイズや内容が違っていても守られるべきものなので、
	// サイズの比較より前に判断する必要がある。
	if c.policy.uses(CompareModTime) {
		if action, reason, decided := c.decideByModTime(srcInfo, *dstInfo); decided {
			return action, reason, nil
		}
	}

	// サイズが違えば転送する。ハッシュを取るより安いので先に見る。
	//
	// 更新時刻が同じなのにサイズが違うのは、転送が途中で
	// 終わっているなど、内容が壊れている兆候でもある。
	if c.policy.uses(CompareSize) && srcInfo.Size != dstInfo.Size {
		return ActionCopy, fmt.Sprintf("サイズが違う（%d と %d）", srcInfo.Size, dstInfo.Size), nil
	}

	if c.policy.uses(CompareHash) {
		same, err := c.sameHash(ctx, srcInfo, *dstInfo)
		if err != nil {
			return ActionCopy, "ハッシュを取得できなかった", err
		}
		if !same {
			return ActionCopy, "ハッシュが違う", nil
		}
		return ActionSkip, "ハッシュが同じ", nil
	}

	// ここまで来たら、比較した項目はすべて同じ。
	return ActionSkip, "同じ", nil
}

// decideByModTime は最終更新時刻で判断します。
// 判断がついた場合は decided が真になります。
func (c *Comparer) decideByModTime(srcInfo, dstInfo storage.FileInfo) (Action, string, bool) {
	// どちらかの時刻が分からなければ、時刻では判断できない。
	if srcInfo.ModTime.IsZero() || dstInfo.ModTime.IsZero() {
		return ActionSkip, "", false
	}

	diff := srcInfo.ModTime.UTC().Sub(dstInfo.ModTime.UTC())

	switch {
	case diff < -c.window:
		// コピー先のほうが新しい。
		//
		// 以前は時刻の差を絶対値で見ていたため、新しいファイルを
		// 古いもので上書きしていた。既定では転送しない。
		if c.policy.Update {
			return ActionSkip, "コピー先のほうが新しい", true
		}
		return ActionCopy, "コピー先のほうが新しいが上書きする", true

	case diff > c.window:
		return ActionCopy, "コピー元のほうが新しい", true
	}

	// 差が許容幅の中。サイズやハッシュの比較へ進む。
	return ActionSkip, "", false
}

// sameHash は両側のハッシュが一致するかを返します。
func (c *Comparer) sameHash(ctx context.Context, srcInfo, dstInfo storage.FileInfo) (bool, error) {
	srcHash, err := storage.GetHash(ctx, c.src, &srcInfo, c.hashType)
	if err != nil {
		return false, err
	}
	dstHash, err := storage.GetHash(ctx, c.dst, &dstInfo, c.hashType)
	if err != nil {
		return false, err
	}
	return strings.EqualFold(srcHash, dstHash), nil
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

	// 少なくとも1秒は見る。
	//
	// FAT や exFAT は2秒刻みなので、その上で動かす場合は
	// --modify-window 2s の指定が必要になる。
	// ファイルシステムの種類を実行時に見分ける手立てがないため、
	// 自動では判断できない。
	if window < time.Second {
		window = time.Second
	}
	return window
}

// commonHash は両側が扱える共通のハッシュを返します。
func commonHash(src, dst storage.Storage) (storage.HashType, bool) {
	sf, df := src.Features(), dst.Features()
	if sf == nil || df == nil {
		return "", false
	}
	return storage.CommonHash(sf.Hashes, df.Hashes)
}

// modTimeUsable は、更新時刻での比較が成り立つかを返します。
func modTimeUsable(dst storage.Storage) bool {
	df := dst.Features()
	if df == nil {
		return true
	}
	// コピー先が更新時刻を保持できないと、転送のたびに時刻がずれて
	// 毎回コピーし直すことになる。
	return df.CanSetModTime
}
