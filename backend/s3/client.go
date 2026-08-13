package s3

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/mt3hr/hbg/internal/hbghome"
	"github.com/spf13/viper"
)

// 対応している提供元。
//
// 決めているのは、接続先の組み立て方と、AWS 独自の機能を使うかどうかです。
const (
	// ProviderAWS は Amazon S3 です。
	ProviderAWS = "aws"
	// ProviderR2 は Cloudflare R2 です。
	ProviderR2 = "r2"
	// ProviderB2 は Backblaze B2 の S3 互換の口です。
	ProviderB2 = "b2"
	// ProviderMinIO は MinIO です。
	ProviderMinIO = "minio"
	// ProviderWasabi は Wasabi です。
	ProviderWasabi = "wasabi"
	// ProviderOther はそれ以外の S3 互換ストレージです。
	ProviderOther = "other"
)

// 一覧のときに更新時刻をどう求めるか。
const (
	// ListMetadataHead は1件ずつ問い合わせて、書き込み時の更新時刻を求めます。
	ListMetadataHead = "head"
	// ListMetadataNone は一覧が返す時刻（書き込まれた時刻）をそのまま使います。
	ListMetadataNone = "none"
)

// Config は S3 互換ストレージの設定です。
type Config struct {
	// Name は設定ファイルで付けた名前です。
	Name string

	// Provider は提供元です。省略すると aws です。
	Provider string
	// Bucket は入れ物の名前です。
	Bucket string
	// Region は地域です。省略できる提供元もあります。
	Region string
	// Endpoint は接続先です。提供元から決まる場合は省略できます。
	Endpoint string
	// AccountID は Cloudflare R2 の口座IDです。接続先の組み立てに使います。
	AccountID string

	// AccessKeyID と SecretAccessKey は認証情報です。
	// 省略した場合は $HOME/hbg/credentials/s3_<名前>.yaml を読み、
	// それもなければ環境変数や ~/.aws の設定を使います。
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	// Profile は ~/.aws の設定のうち、どれを使うかです。
	Profile string

	// ForcePathStyle が真なら、入れ物の名前をパスに含めます。
	// MinIO のように名前を副ドメインにできない相手で使います。
	ForcePathStyle bool

	// StorageClass は書き込むときの保管の種類です。
	StorageClass string
	// ListMetadata は一覧のときに更新時刻をどう求めるかです。
	// "head"（既定）か "none" を指定します。
	ListMetadata string
	// DirectoryMarkers が偽なら、空のディレクトリを表す印を書きません。
	DirectoryMarkers *bool

	// UploadPartSizeMiB は分割送信の1つぶんの大きさです。0 なら自動です。
	UploadPartSizeMiB int64
	// UploadConcurrency は分割送信の同時数です。0 なら既定値です。
	UploadConcurrency int

	// Root を指定すると、その下を起点として扱います。
	Root string

	// clientOverride は試験のために接続先を差し替えるためのものです。
	clientOverride *awss3.Client
}

func (c Config) provider() string {
	if c.Provider == "" {
		return ProviderAWS
	}
	return c.Provider
}

func (c Config) listMetadata() string {
	if c.ListMetadata == "" {
		return ListMetadataHead
	}
	return c.ListMetadata
}

func (c Config) directoryMarkers() bool {
	if c.DirectoryMarkers == nil {
		return true
	}
	return *c.DirectoryMarkers
}

// validate は接続を試みる前に設定の不足を知らせます。
func (c Config) validate() error {
	if c.Bucket == "" {
		return errors.New("入れ物（bucket）が指定されていません")
	}

	switch c.provider() {
	case ProviderAWS, ProviderR2, ProviderB2, ProviderMinIO, ProviderWasabi, ProviderOther:
	default:
		return fmt.Errorf("provider には %s のいずれかを指定してください（%q が指定されました）",
			strings.Join([]string{ProviderAWS, ProviderR2, ProviderB2, ProviderMinIO, ProviderWasabi, ProviderOther}, ", "),
			c.Provider)
	}

	switch c.listMetadata() {
	case ListMetadataHead, ListMetadataNone:
	default:
		return fmt.Errorf("list_metadata には %q か %q を指定してください（%q が指定されました）",
			ListMetadataHead, ListMetadataNone, c.ListMetadata)
	}

	if c.provider() == ProviderR2 && c.Endpoint == "" && c.AccountID == "" {
		return errors.New("r2 では account_id か endpoint のどちらかが必要です")
	}
	return nil
}

// endpoint は接続先を決めます。空なら SDK の既定に任せます。
func (c Config) endpoint() string {
	if c.Endpoint != "" {
		return c.Endpoint
	}
	if c.provider() == ProviderR2 && c.AccountID != "" {
		return fmt.Sprintf("https://%s.r2.cloudflarestorage.com", c.AccountID)
	}
	return ""
}

// region は地域を決めます。
func (c Config) region() string {
	if c.Region != "" {
		return c.Region
	}
	switch c.provider() {
	case ProviderR2:
		// R2 に地域の概念はないが、署名には何か必要になる。
		return "auto"
	case ProviderMinIO, ProviderOther:
		return "us-east-1"
	}
	return ""
}

// newClient は S3 のクライアントを作ります。
func newClient(ctx context.Context, cfg Config) (*awss3.Client, error) {
	if cfg.clientOverride != nil {
		return cfg.clientOverride, nil
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	opts := []func(*awsconfig.LoadOptions) error{}
	if r := cfg.region(); r != "" {
		opts = append(opts, awsconfig.WithRegion(r))
	}
	if cfg.Profile != "" {
		opts = append(opts, awsconfig.WithSharedConfigProfile(cfg.Profile))
	}

	creds, err := resolveCredentials(cfg)
	if err != nil {
		return nil, err
	}
	if creds != nil {
		opts = append(opts, awsconfig.WithCredentialsProvider(creds))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("認証情報を読めません: %w", err)
	}

	return awss3.NewFromConfig(awsCfg, s3Options(cfg)...), nil
}

// s3Options は S3 クライアントの調整をまとめます。
func s3Options(cfg Config) []func(*awss3.Options) {
	return []func(*awss3.Options){
		func(o *awss3.Options) {
			if ep := cfg.endpoint(); ep != "" {
				o.BaseEndpoint = aws.String(ep)
			}
			o.UsePathStyle = cfg.ForcePathStyle

			if cfg.provider() != ProviderAWS {
				// aws-sdk-go-v2 は 2025年1月の版から、要求に CRC32 の
				// 検査値を既定で付けるようになった。AWS 以外の相手では
				// これが署名の食い違いとして拒否されることがある。
				// 相手が求めたときだけ付ける。
				o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
				o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
			}
		},
	}
}

// resolveCredentials は認証情報を決めます。
//
// 設定に直接書かれていればそれを、なければ
// $HOME/hbg/credentials/s3_<名前>.yaml を読みます。
// どちらもなければ nil を返し、環境変数や ~/.aws に任せます。
func resolveCredentials(cfg Config) (aws.CredentialsProvider, error) {
	if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		return credentials.NewStaticCredentialsProvider(
			cfg.AccessKeyID, cfg.SecretAccessKey, cfg.SessionToken), nil
	}
	if cfg.AccessKeyID != "" || cfg.SecretAccessKey != "" {
		return nil, errors.New("access_key_id と secret_access_key は両方を指定してください")
	}
	if cfg.Profile != "" {
		// ~/.aws の設定に任せる。
		return nil, nil
	}

	file, err := credentialsPath(cfg.Name)
	if err != nil {
		return nil, err
	}
	stored, err := readCredentialsFile(file)
	if err != nil {
		return nil, err
	}
	if stored == nil {
		return nil, nil
	}
	return credentials.NewStaticCredentialsProvider(
		stored.AccessKeyID, stored.SecretAccessKey, stored.SessionToken), nil
}

// credentialsPath は認証情報を置く既定の場所です。
func credentialsPath(name string) (string, error) {
	dir, err := hbghome.CredentialsDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "s3_"+name+".yaml"), nil
}

// storedCredentials は認証情報のファイルの中身です。
type storedCredentials struct {
	AccessKeyID     string `mapstructure:"access_key_id"`
	SecretAccessKey string `mapstructure:"secret_access_key"`
	SessionToken    string `mapstructure:"session_token"`
}

// readCredentialsFile は認証情報のファイルを読みます。
// ファイルがなければ nil を返します。
func readCredentialsFile(file string) (*storedCredentials, error) {
	if _, err := os.Stat(file); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}

	v := viper.New()
	v.SetConfigFile(file)
	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("認証情報 %s を読めません: %w", file, err)
	}

	var stored storedCredentials
	if err := v.Unmarshal(&stored); err != nil {
		return nil, fmt.Errorf("認証情報 %s を解釈できません: %w", file, err)
	}
	if stored.AccessKeyID == "" || stored.SecretAccessKey == "" {
		return nil, fmt.Errorf("認証情報 %s に access_key_id と secret_access_key が必要です", file)
	}
	return &stored, nil
}
