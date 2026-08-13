package hbg

// AuthLoginOptions は対話的な認可の進め方を指定します。
type AuthLoginOptions struct {
	// OpenBrowser が真なら、認可URLを既定のブラウザで自動的に開きます。
	OpenBrowser bool
	// Prompt は認可URLを利用者に伝える関数です。
	// ブラウザを開けない環境でも、この表示から手動で開けます。
	Prompt func(authURL string)
}
