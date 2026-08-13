package s3

import (
	"context"
	"fmt"
	"strconv"

	"github.com/mt3hr/hbg/backend"
	"github.com/mt3hr/hbg/storage"
)

func init() {
	backend.Register(backend.Descriptor{
		Type:    Type,
		Summary: "S3 互換（S3 / R2 / B2 / MinIO / Wasabi）",
		ConfigDoc: `  # - name: s3
  #   type: s3
  #   provider: aws  # aws / r2 / b2 / minio / wasabi / other
  #   bucket: 入れ物の名前
  #   region: ap-northeast-1
  #   endpoint: 接続先（提供元から決まる場合は不要）
  #   account_id: Cloudflare R2 の口座ID
  #   access_key_id: ${AWS_ACCESS_KEY_ID}
  #   secret_access_key: ${AWS_SECRET_ACCESS_KEY}
  #   profile: ~/.aws のどの設定を使うか
  #   force_path_style: false  # MinIO では true
  #   storage_class: STANDARD
  #   list_metadata: head  # head なら一覧のたびに更新時刻を問い合わせる
  #   directory_markers: true
  #   root: 起点にする接頭辞
`,
		New: func(ctx context.Context, name string, params backend.Params) (storage.Storage, error) {
			partSize, err := intParam(params, "upload_part_size_mib")
			if err != nil {
				return nil, fmt.Errorf("s3 %s: %w", name, err)
			}
			concurrency, err := intParam(params, "upload_concurrency")
			if err != nil {
				return nil, fmt.Errorf("s3 %s: %w", name, err)
			}

			cfg := Config{
				Name:              name,
				Provider:          params.Get("provider"),
				Bucket:            params.Get("bucket"),
				Region:            params.Get("region"),
				Endpoint:          params.Get("endpoint"),
				AccountID:         params.Get("account_id"),
				AccessKeyID:       params.Get("access_key_id"),
				SecretAccessKey:   params.Get("secret_access_key"),
				SessionToken:      params.Get("session_token"),
				Profile:           params.Get("profile"),
				ForcePathStyle:    params.Get("force_path_style") == "true",
				StorageClass:      params.Get("storage_class"),
				ListMetadata:      params.Get("list_metadata"),
				UploadPartSizeMiB: int64(partSize),
				UploadConcurrency: concurrency,
				Root:              params.Get("root"),
			}
			if raw := params.Get("directory_markers"); raw != "" {
				markers := raw == "true"
				cfg.DirectoryMarkers = &markers
			}

			return New(ctx, cfg)
		},
	})
}

// intParam は数として指定された設定を読みます。
func intParam(params backend.Params, key string) (int, error) {
	raw := params.Get(key)
	if raw == "" {
		return 0, nil
	}

	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s には数を指定してください（%q が指定されました）", key, raw)
	}
	return n, nil
}
