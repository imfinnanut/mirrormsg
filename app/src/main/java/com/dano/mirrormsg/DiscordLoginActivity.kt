package com.dano.mirrormsg

import android.app.Activity
import android.content.res.Configuration
import android.graphics.Color
import android.graphics.Typeface
import android.os.Bundle
import android.os.Handler
import android.os.Looper
import android.view.ViewGroup
import android.webkit.CookieManager
import android.webkit.WebView
import android.webkit.WebViewClient
import android.widget.LinearLayout
import android.widget.TextView
import kotlin.concurrent.thread

/**
 * DiscordLoginActivity — native Discord login via an in-app WebView.
 *
 * Discord offers no sanctioned third-party OAuth for reading your own DMs; like
 * mautrix/Beeper, MirrorMsg authenticates with the account's own user token. The
 * QR remote-auth flow exists but its handshake lives in AGPL code we can't ship in
 * a proprietary app, and raw email/password hits Discord's hCaptcha + "authorize
 * this location" gauntlet that we'd have to reimplement.
 *
 * So we show Discord's REAL login page in a WebView: the user signs in with their
 * username/password, and Discord's own page handles the captcha, 2FA, and the
 * "authorize this IP" email — we implement none of that. Once the session exists,
 * we lift the token out of the page's storage, open the gateway, and on READY
 * persist it and finish (MainActivity re-renders on resume, same as Telegram).
 *
 * The token grab uses the well-known iframe trick: Discord deletes the reference to
 * localStorage on the top window to deter theft, but a freshly-created same-origin
 * iframe still exposes it, and the token remains under the "token" key.
 */
class DiscordLoginActivity : Activity() {

    // ---- app palette (identical values to MainActivity.palette()) --------------

    private data class Pal(
        val accent: Int, val bg: Int, val card: Int, val cardBorder: Int,
        val text: Int, val subtext: Int, val headerText: Int, val onAccent: Int
    )

    private fun palette(): Pal {
        val accent = Color.parseColor("#7C4DFF")
        val night = (resources.configuration.uiMode and
            Configuration.UI_MODE_NIGHT_MASK) == Configuration.UI_MODE_NIGHT_YES
        return if (night) Pal(
            accent = accent,
            bg = Color.parseColor("#0E0E10"),
            card = Color.parseColor("#1C1C1F"),
            cardBorder = Color.parseColor("#2A2A2E"),
            text = Color.parseColor("#F2F2F5"),
            subtext = Color.parseColor("#9A9AA2"),
            headerText = Color.WHITE,
            onAccent = Color.WHITE
        ) else Pal(
            accent = accent,
            bg = Color.parseColor("#F5F4F8"),
            card = Color.WHITE,
            cardBorder = Color.parseColor("#E2E0E8"),
            text = Color.parseColor("#1A1A1E"),
            subtext = Color.parseColor("#6B6B73"),
            headerText = Color.WHITE,
            onAccent = Color.WHITE
        )
    }

    private fun dp(v: Int): Int = (v * resources.displayMetrics.density).toInt()

    private fun statusBarHeight(): Int {
        val id = resources.getIdentifier("status_bar_height", "dimen", "android")
        return if (id > 0) resources.getDimensionPixelSize(id) else dp(28)
    }

    // ---- login state -----------------------------------------------------------

    private var webView: WebView? = null
    private val poller = Handler(Looper.getMainLooper())
    @Volatile private var polling = false
    @Volatile private var connecting = false
    // A token we already tried and that failed — don't loop on it. A genuinely new
    // sign-in (different token) is still allowed to retry.
    private var failedToken: String? = null
    private lateinit var statusView: TextView

    // Reads the auth token out of Discord's storage. Discord deletes the top
    // window's localStorage reference, but a new same-origin iframe still exposes
    // it; the token is stored JSON-encoded under "token", so JSON.parse strips the
    // quotes. Returns "" until a session exists.
    private val tokenGrabJs = """
        (function(){
          try {
            var f = document.createElement('iframe');
            document.body.appendChild(f);
            var raw = f.contentWindow.localStorage.getItem('token');
            f.remove();
            if (!raw) return "";
            return JSON.parse(raw);
          } catch (e) { return ""; }
        })();
    """.trimIndent()

    // ---- UI --------------------------------------------------------------------

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        val p = palette()

        val root = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setBackgroundColor(p.bg)
        }

        // Accent header band — runs up behind the status bar so the title clears the
        // clock/icons (same treatment as MainActivity's header).
        val header = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setBackgroundColor(p.accent)
            setPadding(dp(20), statusBarHeight() + dp(6), dp(20), dp(18))
        }
        header.addView(TextView(this).apply {
            text = "Connect Discord"; textSize = 26f; setTextColor(p.headerText)
            setTypeface(typeface, Typeface.BOLD)
        })
        root.addView(header)

        // Thin status line under the header: a reassurance up front, then live
        // sign-in status / errors. Selectable so an error can be copied.
        statusView = TextView(this).apply {
            text = "Sign in to Discord below. Your login stays on this phone \u2014 we " +
                "keep only the session token, to mirror your DMs to the watch."
            textSize = 13f; setTextColor(p.subtext)
            setPadding(dp(16), dp(12), dp(16), dp(12))
            setTextIsSelectable(true)
        }
        root.addView(statusView)

        // Discord's real login page.
        val web = WebView(this)
        CookieManager.getInstance().setAcceptCookie(true)
        CookieManager.getInstance().setAcceptThirdPartyCookies(web, true)
        web.settings.apply {
            javaScriptEnabled = true
            domStorageEnabled = true
            databaseEnabled = true
        }
        web.webViewClient = object : WebViewClient() {
            // Keep Discord's own login flow (captcha, 2FA, authorize-location) inside
            // the WebView rather than kicking out to an external browser.
            override fun shouldOverrideUrlLoading(view: WebView?, url: String?): Boolean = false
        }
        webView = web
        root.addView(web, LinearLayout.LayoutParams(
            ViewGroup.LayoutParams.MATCH_PARENT, 0, 1f))

        setContentView(root)
        web.loadUrl("https://discord.com/login")
        startPolling()
    }

    // ---- token harvest + connect ----------------------------------------------

    private fun startPolling() {
        polling = true
        poller.postDelayed(pollRunnable, 1200L)
    }

    private val pollRunnable = object : Runnable {
        override fun run() {
            if (!polling) return
            if (!connecting) {
                webView?.evaluateJavascript(tokenGrabJs) { res ->
                    val token = cleanToken(res)
                    // A real Discord token is long and dotted; guard against the
                    // empty/"null" polls and the one we already know failed.
                    if (token.length > 20 && token != failedToken && !connecting) {
                        onTokenGrabbed(token)
                    }
                }
            }
            poller.postDelayed(this, 1200L)
        }
    }

    // evaluateJavascript hands back the JS value JSON-encoded (a quoted string, or
    // the literal null / "" ). Strip that back to the bare token.
    private fun cleanToken(js: String?): String {
        if (js.isNullOrBlank()) return ""
        var s = js.trim()
        if (s == "null" || s == "\"\"") return ""
        if (s.length >= 2 && s.startsWith("\"") && s.endsWith("\"")) {
            s = s.substring(1, s.length - 1)
        }
        return s.replace("\\\"", "\"").replace("\\\\", "\\").trim()
    }

    private fun onTokenGrabbed(token: String) {
        connecting = true
        statusView.text = "Signing in\u2026"
        webView?.alpha = 0.35f
        thread(name = "dc-login") {
            val b = DiscordService.bridge()
            val err = b.start(token)
            if (err.contains("dcError")) {
                failWith("Couldn't connect: $err", token)
                return@thread
            }
            // Wait for READY (self populated) up to ~8s. A bad/again-challenged token
            // opens the gateway but never reaches READY, so no-READY = "not accepted".
            var st = b.connectStatus()
            var tries = 0
            while (st != "READY" && tries < 80) {
                Thread.sleep(100); st = b.connectStatus(); tries++
            }
            if (st == "READY") {
                DiscordService.markLoggedIn(
                    this@DiscordLoginActivity, token, b.selfName(), b.selfID()
                )
                LogStore.i("MirrorMsgSpike", "DC login OK: ${b.selfName()}")
                runOnUiThread { polling = false; finish() }
            } else {
                failWith("That sign-in didn't complete. Try again.", token)
            }
        }
    }

    // Tear the half-open session down so a fresh sign-in can start cleanly, remember
    // the failed token so the poller doesn't loop on it, and re-enable the page.
    private fun failWith(msg: String, token: String) {
        try { DiscordService.bridge().close() } catch (_: Throwable) {}
        runOnUiThread {
            failedToken = token
            connecting = false
            statusView.text = msg
            webView?.alpha = 1f
        }
    }

    override fun onBackPressed() {
        val w = webView
        if (w != null && w.canGoBack()) w.goBack() else super.onBackPressed()
    }

    override fun onDestroy() {
        polling = false
        poller.removeCallbacksAndMessages(null)
        webView?.destroy()
        webView = null
        super.onDestroy()
    }
}
