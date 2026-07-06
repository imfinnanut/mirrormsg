package telegrambridge

// Telegram app credentials (apiID / apiHash from https://my.telegram.org).
//
// These are an app IDENTIFIER, not account access — login still requires the
// user's phone number, login code, and 2FA. They are declared here but left
// zero-valued on purpose: the real values live in credentials.local.go, which
// is gitignored and NOT committed. That keeps the credentials out of the public
// source tree.
//
// To build: copy credentials.local.go.example to credentials.local.go and fill
// in your own values. Without that file the vars stay zero and the bridge fails
// fast at startup with a clear message (see run() in bridge_tg.go).
var (
	apiID   int
	apiHash string
)
