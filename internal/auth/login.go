package auth

// LoginOptions は対話的な認可の進め方を指定します。
//
// バックエンドごとに認可の詳細は違いますが、利用者から見た
// 進め方は共通なので、ここにまとめておきます。
type LoginOptions struct {
	// OpenBrowser が真なら、認可URLを既定のブラウザで自動的に開きます。
	OpenBrowser bool
	// Prompt は認可URLを利用者に伝える関数です。
	// ブラウザを開けない環境でも、この表示から手動で開けます。
	Prompt func(authURL string)
}
