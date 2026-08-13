// Package s3 は S3 互換のオブジェクトストレージを storage.Storage として
// 実装します。Amazon S3 のほか、Cloudflare R2・Backblaze B2・MinIO・
// Wasabi など、同じ口を持つものに使えます。
//
// オブジェクトストレージにはディレクトリという仕組みがありません。
// あるのは名前と中身の対だけで、"写真/2024/a.jpg" のような名前を
// 区切り文字で切って、あたかも階層があるかのように見せています。
// この見せかけをここで作ります。
package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/mt3hr/hbg/storage"
	"golang.org/x/sync/errgroup"
)

// Type はこのバックエンドの種別名です。
const Type = "s3"

// mtimeMeta は書き込み時の更新時刻を入れておく項目です。
//
// オブジェクトストレージが持つ時刻は「書き込まれた時刻」で、
// 元のファイルの更新時刻とは別ものです。同期の判断には元の時刻が
// 要るので、利用者定義の項目として一緒に置いておきます。
// 名前と書式は rclone に合わせてあり、同じ入れ物を両方から使えます。
const mtimeMeta = "mtime"

// md5Meta は元のファイルの MD5 を入れておく項目です。
//
// 分割して送った場合、ETag は MD5 になりません。転送元で分かって
// いるなら、ここに控えておくと内容の照合に使えます。
const md5Meta = "md5chksum"

// listPageSize は一覧が1回に要求する件数です。
const listPageSize = 1000

// headConcurrency は一覧のときに更新時刻を問い合わせる同時数です。
const headConcurrency = 8

// minPartSize は分割送信の1つぶんの下限です。S3 の決まりです。
const minPartSize = 5 * 1024 * 1024

// defaultPartSize は分割送信の1つぶんの既定の大きさです。
const defaultPartSize = 16 * 1024 * 1024

// Storage は S3 互換のオブジェクトストレージです。
type Storage struct {
	name   string
	client *awss3.Client
	bucket string
	// root は入れ物の中での起点です。末尾に "/" は付きません。
	root string

	listMetadata     string
	directoryMarkers bool
	storageClass     string
	partSize         int64
	concurrency      int
}

// New は S3 互換ストレージに接続します。
//
// ここでは通信しません。入れ物があるかどうかは最初の操作で分かります。
func New(ctx context.Context, cfg Config) (*Storage, error) {
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("s3 %s: %w", cfg.Name, err)
	}

	client, err := newClient(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("s3 %s: %w", cfg.Name, err)
	}

	partSize := cfg.UploadPartSizeMiB * 1024 * 1024
	if partSize < minPartSize {
		partSize = defaultPartSize
	}

	return &Storage{
		name:             cfg.Name,
		client:           client,
		bucket:           cfg.Bucket,
		root:             strings.Trim(cleanPath(cfg.Root), "/"),
		listMetadata:     cfg.listMetadata(),
		directoryMarkers: cfg.directoryMarkers(),
		storageClass:     cfg.StorageClass,
		partSize:         partSize,
		concurrency:      cfg.UploadConcurrency,
	}, nil
}

// Type はストレージの種別を返します。
func (s *Storage) Type() string { return Type }

// Name は設定ファイルで付けた名前を返します。
func (s *Storage) Name() string { return s.name }

// Features は S3 にできることを返します。
func (s *Storage) Features() *storage.Features {
	return &storage.Features{
		// 更新時刻は利用者定義の項目に入れるので、精度は落ちない。
		ModTimePrecision: time.Nanosecond,
		CanSetModTime:    false,
		CaseInsensitive:  false,
		Hashes:           storage.HashSet{storage.MD5},
		// 名前の中の "/" が階層なので、親を作る必要はない。
		ImplicitDirs: true,
		// 空のディレクトリは、末尾が "/" の空のオブジェクトで表す。
		EmptyDirs: s.directoryMarkers,
		// 書き込みは完了してはじめて見えるので不可分。
		AtomicPut: true,
	}
}

// Close はストレージを閉じます。
func (s *Storage) Close() error {
	s.client = nil
	return nil
}

// --- 名前とパスの対応 ---

// key はパスをオブジェクトの名前に変換します。
//
// 先頭の "/" は取り除きます。オブジェクトの名前は "/" で始まりません。
func (s *Storage) key(p string) string {
	p = strings.TrimPrefix(cleanPath(p), "/")
	if s.root == "" {
		return p
	}
	if p == "" {
		return s.root
	}
	return s.root + "/" + p
}

// dirPrefix はディレクトリを表す接頭辞を返します。末尾は "/" です。
func (s *Storage) dirPrefix(p string) string {
	k := s.key(p)
	if k == "" {
		return ""
	}
	return k + "/"
}

// pathOf はオブジェクトの名前を hbg のパスに戻します。
func (s *Storage) pathOf(key string) string {
	key = strings.TrimSuffix(key, "/")
	if s.root != "" {
		key = strings.TrimPrefix(strings.TrimPrefix(key, s.root), "/")
	}
	return "/" + key
}

// cleanPath はパスを正規化します。
//
// "\" は区切りとして扱いません。オブジェクトの名前に使える
// ふつうの文字なので、区切りに読み替えると別の名前になってしまいます。
func cleanPath(p string) string {
	if p == "" {
		return "/"
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return path.Clean(p)
}

// --- 一覧 ---

// List はディレクトリの直下を1件ずつ fn に渡します。
//
// 区切り文字を指定して問い合わせると、同じ接頭辞を持つものが
// ひとまとめにされて返ってきます。これをディレクトリとして見せます。
func (s *Storage) List(ctx context.Context, dir string, fn func(storage.FileInfo) error) error {
	prefix := s.dirPrefix(dir)
	base := cleanPath(dir)

	found := false
	paginator := awss3.NewListObjectsV2Paginator(s.client, &awss3.ListObjectsV2Input{
		Bucket:    aws.String(s.bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
		MaxKeys:   aws.Int32(listPageSize),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return s.wrapErr("list", dir, err)
		}

		for _, cp := range page.CommonPrefixes {
			found = true
			name := path.Base(strings.TrimSuffix(aws.ToString(cp.Prefix), "/"))
			if cbErr := fn(storage.FileInfo{
				Path:  path.Join(base, name),
				Name:  name,
				IsDir: true,
				Size:  storage.SizeUnknown,
			}); cbErr != nil {
				return cbErr
			}
		}

		objects, err := s.pageObjects(ctx, page.Contents, prefix)
		if err != nil {
			return s.wrapErr("list", dir, err)
		}
		for _, obj := range objects {
			found = true
			if obj.marker {
				// ディレクトリを表す印そのもの。中身ではない。
				continue
			}
			if err := fn(obj.info(base)); err != nil {
				return err
			}
		}
	}

	if !found && prefix != "" {
		// 何も返ってこない場合、空のディレクトリなのか、
		// そもそも無いのかを区別できない。印を確かめる。
		if err := s.requireDir(ctx, dir); err != nil {
			return err
		}
	}
	return nil
}

// listedObject は一覧で見つかった1件です。
type listedObject struct {
	key     string
	size    int64
	modTime time.Time
	md5     string
	marker  bool
}

func (o listedObject) info(base string) storage.FileInfo {
	name := path.Base(o.key)
	fi := storage.FileInfo{
		Path:    path.Join(base, name),
		Name:    name,
		Size:    o.size,
		ModTime: o.modTime,
	}
	if o.md5 != "" {
		fi.Hashes = map[storage.HashType]string{storage.MD5: o.md5}
	}
	return fi
}

// pageObjects は1ページぶんの中身を整えます。
//
// 一覧の応答には利用者定義の項目が含まれないため、書き込み時の
// 更新時刻を知るには1件ずつ問い合わせる必要があります。
// 件数ぶんの往復が増えるので、まとめて並行に行います。
func (s *Storage) pageObjects(ctx context.Context, contents []s3types.Object, prefix string) ([]listedObject, error) {
	out := make([]listedObject, len(contents))

	for i, obj := range contents {
		key := aws.ToString(obj.Key)
		out[i] = listedObject{
			key:     key,
			size:    aws.ToInt64(obj.Size),
			modTime: aws.ToTime(obj.LastModified),
			md5:     etagMD5(aws.ToString(obj.ETag)),
			marker:  key == prefix || strings.HasSuffix(key, "/"),
		}
	}

	if s.listMetadata != ListMetadataHead {
		return out, nil
	}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(headConcurrency)

	for i := range out {
		if out[i].marker {
			continue
		}
		g.Go(func() error {
			head, err := s.head(gctx, out[i].key)
			if err != nil {
				return err
			}
			if t, ok := metaModTime(head.Metadata); ok {
				out[i].modTime = t
			}
			if md5 := head.Metadata[md5Meta]; md5 != "" {
				out[i].md5 = md5
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return out, nil
}

// requireDir はディレクトリとして存在するかを確かめます。
func (s *Storage) requireDir(ctx context.Context, dir string) error {
	prefix := s.dirPrefix(dir)

	// 印がある、あるいは配下に何かあれば、ディレクトリとして存在する。
	res, err := s.client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
		Bucket:  aws.String(s.bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int32(1),
	})
	if err != nil {
		return s.wrapErr("list", dir, err)
	}
	if len(res.Contents) > 0 {
		return nil
	}

	// 同じ名前のファイルがあるかもしれない。
	if _, err := s.head(ctx, s.key(dir)); err == nil {
		return s.wrapErr("list", dir, storage.ErrNotDir)
	}
	return s.wrapErr("list", dir, storage.ErrNotFound)
}

// Stat は1件のメタデータを返します。
func (s *Storage) Stat(ctx context.Context, p string) (*storage.FileInfo, error) {
	if cleanPath(p) == "/" {
		return &storage.FileInfo{Path: "/", Name: "/", IsDir: true, Size: storage.SizeUnknown}, nil
	}

	head, err := s.head(ctx, s.key(p))
	if err == nil {
		return s.infoFromHead(p, head), nil
	}
	if !isNotFound(err) {
		return nil, s.wrapErr("stat", p, err)
	}

	// ファイルとしては無い。ディレクトリかどうかを確かめる。
	if dirErr := s.requireDir(ctx, p); dirErr != nil {
		return nil, s.wrapErr("stat", p, storage.ErrNotFound)
	}
	return &storage.FileInfo{
		Path:  cleanPath(p),
		Name:  path.Base(cleanPath(p)),
		IsDir: true,
		Size:  storage.SizeUnknown,
	}, nil
}

// head は1件のメタデータを問い合わせます。
func (s *Storage) head(ctx context.Context, key string) (*awss3.HeadObjectOutput, error) {
	return s.client.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
}

// infoFromHead は問い合わせの結果を storage.FileInfo にします。
func (s *Storage) infoFromHead(p string, head *awss3.HeadObjectOutput) *storage.FileInfo {
	cp := cleanPath(p)
	fi := &storage.FileInfo{
		Path:    cp,
		Name:    path.Base(cp),
		Size:    aws.ToInt64(head.ContentLength),
		ModTime: aws.ToTime(head.LastModified),
	}
	if t, ok := metaModTime(head.Metadata); ok {
		fi.ModTime = t
	}
	if md5 := hashOf(head.Metadata, aws.ToString(head.ETag)); md5 != "" {
		fi.Hashes = map[storage.HashType]string{storage.MD5: md5}
	}
	return fi
}

// hashOf は MD5 を求めます。分からない場合は空を返します。
func hashOf(meta map[string]string, etag string) string {
	if md5 := meta[md5Meta]; md5 != "" {
		return md5
	}
	return etagMD5(etag)
}

// etagMD5 は ETag が MD5 として使えるならそれを返します。
//
// 分割して送ったオブジェクトの ETag は MD5 ではなく、
// "-" と分割数が付いた別の値になります。
func etagMD5(etag string) string {
	etag = strings.Trim(etag, `"`)
	if etag == "" || strings.Contains(etag, "-") {
		return ""
	}
	return etag
}

// --- 読み書き ---

// Open はファイルの内容を読む ReadCloser を返します。
func (s *Storage) Open(ctx context.Context, p string) (io.ReadCloser, *storage.FileInfo, error) {
	res, err := s.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(p)),
	})
	if err != nil {
		return nil, nil, s.wrapErr("open", p, err)
	}

	cp := cleanPath(p)
	fi := &storage.FileInfo{
		Path:    cp,
		Name:    path.Base(cp),
		Size:    aws.ToInt64(res.ContentLength),
		ModTime: aws.ToTime(res.LastModified),
	}
	if t, ok := metaModTime(res.Metadata); ok {
		fi.ModTime = t
	}
	if md5 := hashOf(res.Metadata, aws.ToString(res.ETag)); md5 != "" {
		fi.Hashes = map[storage.HashType]string{storage.MD5: md5}
	}

	return res.Body, fi, nil
}

// OpenRange は offset から length バイトを読む ReadCloser を返します。
func (s *Storage) OpenRange(ctx context.Context, p string, offset, length int64) (io.ReadCloser, error) {
	res, err := s.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(p)),
		Range:  aws.String(rangeHeader(offset, length)),
	})
	if err != nil {
		return nil, s.wrapErr("open", p, err)
	}
	return res.Body, nil
}

func rangeHeader(offset, length int64) string {
	if length < 0 {
		return fmt.Sprintf("bytes=%d-", offset)
	}
	return fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
}

// Put はファイルを書き込みます。
//
// 大きいものは自動的に分割して送ります。宣言されたサイズは使わず、
// 読み終わるまでを書き込みます。
func (s *Storage) Put(ctx context.Context, p string, r io.Reader, meta storage.ObjectMeta) (*storage.FileInfo, error) {
	if cleanPath(p) == "/" {
		return nil, s.wrapErr("put", p, errors.New("ルートをファイルとして書き込むことはできません"))
	}

	// feature/s3/manager は「非推奨」と印が付いているが、
	// 後継の feature/s3/transfermanager はまだ v0.x で、
	// 予告なく形が変わりうる。落ち着くまではこちらを使う。
	//nolint:staticcheck // 後継が安定するまで
	uploader := manager.NewUploader(s.client, func(u *manager.Uploader) {
		u.PartSize = s.partSize
		if s.concurrency > 0 {
			u.Concurrency = s.concurrency
		}
	})

	counting := &countingReader{r: r}
	input := &awss3.PutObjectInput{
		Bucket:   aws.String(s.bucket),
		Key:      aws.String(s.key(p)),
		Body:     counting,
		Metadata: s.metadataFor(meta),
	}
	if s.storageClass != "" {
		input.StorageClass = s3types.StorageClass(s.storageClass)
	}
	if meta.MIMEType != "" {
		input.ContentType = aws.String(meta.MIMEType)
	}

	//nolint:staticcheck // 後継の transfermanager が安定するまで
	if _, err := uploader.Upload(ctx, input); err != nil {
		return nil, s.wrapErr("put", p, err)
	}

	cp := cleanPath(p)
	fi := &storage.FileInfo{
		Path:    cp,
		Name:    path.Base(cp),
		Size:    counting.n,
		ModTime: meta.ModTime,
	}
	if md5 := meta.Hashes[storage.MD5]; md5 != "" {
		fi.Hashes = map[storage.HashType]string{storage.MD5: md5}
	}
	return fi, nil
}

// metadataFor は書き込みに添える利用者定義の項目を組み立てます。
func (s *Storage) metadataFor(meta storage.ObjectMeta) map[string]string {
	out := map[string]string{}
	if !meta.ModTime.IsZero() {
		out[mtimeMeta] = formatModTime(meta.ModTime)
	}
	if md5 := meta.Hashes[storage.MD5]; md5 != "" {
		// 分割して送ると ETag が MD5 でなくなるので、控えておく。
		out[md5Meta] = md5
	}
	return out
}

// countingReader は読んだバイト数を数えます。
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// formatModTime は更新時刻を項目に入れる形にします。
// 秒とナノ秒を "." で繋いだ形で、rclone と同じです。
func formatModTime(t time.Time) string {
	u := t.UTC()
	return fmt.Sprintf("%d.%09d", u.Unix(), u.Nanosecond())
}

// metaModTime は項目から更新時刻を読み取ります。
func metaModTime(meta map[string]string) (time.Time, bool) {
	raw := meta[mtimeMeta]
	if raw == "" {
		return time.Time{}, false
	}

	sec, frac, _ := strings.Cut(raw, ".")
	seconds, err := strconv.ParseInt(sec, 10, 64)
	if err != nil {
		return time.Time{}, false
	}

	nanos := int64(0)
	if frac != "" {
		// 桁が足りない場合に備えて9桁に揃える。
		frac = (frac + "000000000")[:9]
		if n, err := strconv.ParseInt(frac, 10, 64); err == nil {
			nanos = n
		}
	}
	return time.Unix(seconds, nanos).UTC(), true
}

// --- ディレクトリと削除 ---

// Mkdir はディレクトリを表す印を書きます。
//
// オブジェクトストレージにディレクトリはないので、中身が入れば
// 階層は勝手にできます。印を書くのは、空のディレクトリを
// 表せるようにするためです。
func (s *Storage) Mkdir(ctx context.Context, dir string) error {
	if !s.directoryMarkers {
		return nil
	}
	prefix := s.dirPrefix(dir)
	if prefix == "" {
		return nil
	}

	_, err := s.client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(prefix),
		Body:          strings.NewReader(""),
		ContentLength: aws.Int64(0),
	})
	return s.wrapErr("mkdir", dir, err)
}

// Remove は1つのファイル、または空のディレクトリを削除します。
func (s *Storage) Remove(ctx context.Context, p string) error {
	if cleanPath(p) == "/" {
		return s.wrapErr("remove", p, errors.New("ルートは削除できません"))
	}

	// まずファイルとして消せるか試す。
	if _, err := s.head(ctx, s.key(p)); err == nil {
		return s.wrapErr("remove", p, s.deleteKey(ctx, s.key(p)))
	} else if !isNotFound(err) {
		return s.wrapErr("remove", p, err)
	}

	// ディレクトリの場合。空でなければ消さない。
	prefix := s.dirPrefix(p)
	res, err := s.client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
		Bucket:  aws.String(s.bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int32(2),
	})
	if err != nil {
		return s.wrapErr("remove", p, err)
	}

	switch {
	case len(res.Contents) == 0:
		return s.wrapErr("remove", p, storage.ErrNotFound)
	case len(res.Contents) > 1 || aws.ToString(res.Contents[0].Key) != prefix:
		return s.wrapErr("remove", p,
			fmt.Errorf("%w: 中身ごと消すには purge を使ってください", storage.ErrNotEmpty))
	}
	return s.wrapErr("remove", p, s.deleteKey(ctx, prefix))
}

func (s *Storage) deleteKey(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &awss3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	return err
}

// Purge はディレクトリを中身ごと削除します。
func (s *Storage) Purge(ctx context.Context, dir string) error {
	if cleanPath(dir) == "/" {
		return s.wrapErr("purge", dir, errors.New("ルートは削除できません"))
	}

	prefix := s.dirPrefix(dir)
	paginator := awss3.NewListObjectsV2Paginator(s.client, &awss3.ListObjectsV2Input{
		Bucket:  aws.String(s.bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int32(listPageSize),
	})

	deleted := 0
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return s.wrapErr("purge", dir, err)
		}
		if len(page.Contents) == 0 {
			continue
		}

		objects := make([]s3types.ObjectIdentifier, 0, len(page.Contents))
		for _, obj := range page.Contents {
			objects = append(objects, s3types.ObjectIdentifier{Key: obj.Key})
		}

		if _, err := s.client.DeleteObjects(ctx, &awss3.DeleteObjectsInput{
			Bucket: aws.String(s.bucket),
			Delete: &s3types.Delete{Objects: objects, Quiet: aws.Bool(true)},
		}); err != nil {
			return s.wrapErr("purge", dir, err)
		}
		deleted += len(objects)
	}

	if deleted == 0 {
		return s.wrapErr("purge", dir, storage.ErrNotFound)
	}
	return nil
}

// --- 付随する機能 ---

// Hash はファイルの MD5 を返します。
func (s *Storage) Hash(ctx context.Context, p string, ht storage.HashType) (string, error) {
	if ht != storage.MD5 {
		return "", fmt.Errorf("%w: s3 が扱えるのは %s だけです（%s を要求されました）",
			storage.ErrUnsupported, storage.MD5, ht)
	}

	head, err := s.head(ctx, s.key(p))
	if err != nil {
		return "", s.wrapErr("hash", p, err)
	}

	md5 := hashOf(head.Metadata, aws.ToString(head.ETag))
	if md5 == "" {
		// 分割して送られたもので、元の MD5 も控えられていない。
		// 求めるには中身を読み直すしかないので、できないと伝える。
		return "", s.wrapErr("hash", p, fmt.Errorf(
			"%w: 分割して書き込まれたため MD5 を取得できません", storage.ErrUnsupported))
	}
	return md5, nil
}

// ServerSideCopy は内容を転送せずにコピーします。
func (s *Storage) ServerSideCopy(ctx context.Context, srcPath, dstPath string) (*storage.FileInfo, error) {
	_, err := s.client.CopyObject(ctx, &awss3.CopyObjectInput{
		Bucket:     aws.String(s.bucket),
		Key:        aws.String(s.key(dstPath)),
		CopySource: aws.String(s.bucket + "/" + s.key(srcPath)),
	})
	if err != nil {
		return nil, s.wrapErr("copy", srcPath, err)
	}

	head, err := s.head(ctx, s.key(dstPath))
	if err != nil {
		return nil, s.wrapErr("copy", dstPath, err)
	}
	return s.infoFromHead(dstPath, head), nil
}

// Move は内容を転送せずに移動・改名します。
func (s *Storage) Move(ctx context.Context, srcPath, dstPath string) error {
	if _, err := s.ServerSideCopy(ctx, srcPath, dstPath); err != nil {
		return err
	}
	return s.wrapErr("move", srcPath, s.deleteKey(ctx, s.key(srcPath)))
}

var (
	_ storage.Storage          = (*Storage)(nil)
	_ storage.Hasher           = (*Storage)(nil)
	_ storage.Purger           = (*Storage)(nil)
	_ storage.Mover            = (*Storage)(nil)
	_ storage.RangeOpener      = (*Storage)(nil)
	_ storage.ServerSideCopier = (*Storage)(nil)
)
