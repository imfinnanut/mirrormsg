package com.dano.mirrormsg

import android.app.Activity
import android.content.res.Configuration
import android.graphics.Color
import android.graphics.Typeface
import android.graphics.drawable.GradientDrawable
import android.os.Bundle
import android.os.Handler
import android.os.Looper
import android.view.View
import android.view.ViewGroup
import android.webkit.CookieManager
import android.webkit.WebStorage
import android.webkit.WebView
import android.webkit.WebViewClient
import android.widget.Button
import android.widget.LinearLayout
import android.widget.ScrollView
import android.widget.TextView
import android.widget.Toast
import org.json.JSONObject

/**
 * Native Instagram login: sign in to instagram.com in a WebView, then hand its
 * session cookies to the bridge, which checks them by loading the inbox the way
 * the website does. The WebView keeps its own user agent (Instagram's login page
 * is served to it as to any phone browser). Part of the native-Instagram drop-out
 * set (see InstagramService).
 */
class InstagramLoginActivity : Activity() {

    private enum class Stage { WEB, CHECKING, BLOCKED }

    private data class Pal(
        val header: Int, val headerText: Int, val bg: Int, val card: Int, val cardBorder: Int,
        val text: Int, val subtext: Int, val button: Int, val onButton: Int
    )

    private lateinit var pal: Pal
    private lateinit var root: LinearLayout
    private lateinit var signInHint: TextView
    private lateinit var scroll: ScrollView
    private lateinit var status: TextView
    private lateinit var retryBtn: Button
    private var web: WebView? = null

    private val handler = Handler(Looper.getMainLooper())
    @Volatile private var stage = Stage.WEB
    @Volatile private var busy = false
    private var retryAction: (() -> Unit)? = null
    private val errorRed = Color.parseColor("#E5484D")

    private val cookiePoll = object : Runnable {
        override fun run() {
            if (stage != Stage.WEB) return
            checkForSession()
            handler.postDelayed(this, 1_000L)
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        Mirror.appContext = applicationContext
        pal = palette()

        root = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setBackgroundColor(pal.bg)
            layoutParams = ViewGroup.LayoutParams(
                ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.MATCH_PARENT
            )
        }
        val header = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setBackgroundColor(pal.header)
            setPadding(dp(20), statusBarHeight() + dp(6), dp(20), dp(14))
        }
        header.addView(TextView(this).apply {
            text = "Connect Instagram"
            textSize = 26f
            setTextColor(pal.headerText)
            setTypeface(typeface, Typeface.BOLD)
        })
        signInHint = TextView(this).apply {
            text = "Sign in with your Instagram username and password."
            textSize = 13f
            setTextColor(pal.headerText)
            alpha = 0.8f
            setPadding(0, dp(6), 0, 0)
        }
        header.addView(signInHint)
        root.addView(header)

        scroll = ScrollView(this).apply {
            layoutParams = LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, 0, 1f)
            visibility = View.GONE
        }
        val content = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setPadding(dp(16), dp(14), dp(16), dp(20))
        }
        val card = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            background = rounded(pal.card, 14, 1, pal.cardBorder)
            setPadding(dp(16), dp(16), dp(16), dp(16))
        }
        status = TextView(this).apply {
            textSize = 15f
            setTextColor(pal.text)
            setTextIsSelectable(true)
        }
        retryBtn = Button(this).apply {
            text = "Try again"
            isAllCaps = false
            textSize = 16f
            setTextColor(pal.onButton)
            setPadding(0, dp(14), 0, dp(14))
            background = rounded(pal.button, 12)
            stateListAnimator = null
            visibility = View.GONE
            setOnClickListener { retryAction?.invoke() }
        }
        card.addView(status, stack(0))
        card.addView(retryBtn, stack(16))
        content.addView(card)
        scroll.addView(content)
        root.addView(scroll)
        setContentView(root)

        showWeb()
    }

    override fun onDestroy() {
        handler.removeCallbacksAndMessages(null)
        destroyWeb()
        super.onDestroy()
    }

    private fun showWeb() {
        stage = Stage.WEB
        scroll.visibility = View.GONE
        signInHint.visibility = View.VISIBLE
        val wv = WebView(this).apply {
            layoutParams = LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, 0, 1f)
        }
        val cm = CookieManager.getInstance()
        cm.setAcceptCookie(true)
        cm.setAcceptThirdPartyCookies(wv, true)
        wv.settings.javaScriptEnabled = true
        wv.settings.domStorageEnabled = true
        if (SESSION_COOKIE !in cookieNames(igCookies())) {
            clearIgCookies()
            WebStorage.getInstance().deleteOrigin(COOKIE_URL)
        }
        LogStore.i(TAG, "IGLogin: WebView UA ${wv.settings.userAgentString}")
        wv.webViewClient = object : WebViewClient() {
            override fun onPageFinished(view: WebView?, url: String?) {
                checkForSession()
            }
        }
        root.addView(wv)
        web = wv
        wv.loadUrl(LOGIN_URL)
        handler.removeCallbacks(cookiePoll)
        handler.postDelayed(cookiePoll, 1_000L)
    }

    private fun destroyWeb() {
        val wv = web ?: return
        web = null
        try {
            root.removeView(wv)
            wv.stopLoading()
            wv.destroy()
        } catch (_: Throwable) {}
    }

    private fun igCookies(): String = CookieManager.getInstance().getCookie(COOKIE_URL) ?: ""

    private fun cookieNames(raw: String): Set<String> =
        raw.split(";").mapNotNull { part ->
            val eq = part.indexOf('=')
            if (eq <= 0) null
            else {
                val k = part.substring(0, eq).trim()
                val v = part.substring(eq + 1).trim()
                if (k.isNotEmpty() && v.isNotEmpty()) k else null
            }
        }.toSet()

    private fun bridgeCookieHeader(raw: String): String {
        val kept = ArrayList<String>()
        val skipped = ArrayList<String>()
        for (part in raw.split(";")) {
            val eq = part.indexOf('=')
            if (eq <= 0) continue
            val name = part.substring(0, eq).trim()
            val value = part.substring(eq + 1).trim()
            if (name.isEmpty() || value.isEmpty()) continue
            if (validCookieName(name) && validCookieValue(value)) kept.add("$name=$value") else skipped.add(name)
        }
        LogStore.i(TAG, "IGLogin: passing ${kept.size} cookies to the bridge (${kept.joinToString(", ") { it.substringBefore('=') }})" +
            if (skipped.isEmpty()) "" else ", left out unreadable: ${skipped.joinToString(", ")}")
        return kept.joinToString("; ")
    }

    private fun validCookieName(n: String): Boolean =
        n.all { it.code in 0x21..0x7E && it !in "()<>@,;:\\\"/[]?={}" }

    private fun validCookieValue(v: String): Boolean {
        val inner = if (v.length > 1 && v.first() == '"' && v.last() == '"') v.substring(1, v.length - 1) else v
        return inner.all { it.code in 0x20..0x7E && it != '"' && it != ';' && it != '\\' }
    }

    private fun checkForSession() {
        if (stage != Stage.WEB || busy) return
        val raw = igCookies()
        val names = cookieNames(raw)
        if (REQUIRED_COOKIES.any { it !in names }) return
        CookieManager.getInstance().flush()
        LogStore.i(TAG, "IGLogin: instagram.com session found (${names.size} cookies), checking the inbox")
        handler.removeCallbacks(cookiePoll)
        destroyWeb()
        beginLogin(raw)
    }

    private fun beginLogin(cookieHeader: String) {
        stage = Stage.CHECKING
        signInHint.visibility = View.GONE
        scroll.visibility = View.VISIBLE
        retryBtn.visibility = View.GONE
        setStatus("Signed in to Instagram. Opening your inbox…", false)
        busy = true
        Thread({
            var result: String? = null
            var error: String? = null
            try {
                val b = InstagramService.bridgeOrNull() ?: throw IllegalStateException("the Instagram bridge isn't available")
                result = b.login(bridgeCookieHeader(cookieHeader))
                val ok = try { JSONObject(result).optBoolean("ok", false) } catch (_: Throwable) { false }
                if (ok) InstagramService.onLoginSucceeded()
            } catch (t: Throwable) {
                error = t.message ?: "unknown error"
                LogStore.w(TAG, "IGLogin: login failed: $error")
            }
            runOnUiThread { if (!isFinishing && !isDestroyed) onLoginResult(result, error) }
        }, "ig-login").start()
    }

    private fun onLoginResult(result: String?, error: String?) {
        busy = false
        if (result == null) {
            showBlocked("The sign-in didn't go through: $error") { retryWithSession() }
            return
        }
        val o = try { JSONObject(result) } catch (_: Throwable) { JSONObject() }
        if (o.optBoolean("ok", false)) {
            val user = o.optString("username")
            LogStore.i(TAG, "IGLogin: inbox loaded, connected")
            clearIgCookies()
            Toast.makeText(this, if (user.isNotEmpty()) "Instagram connected as @$user" else "Instagram connected",
                Toast.LENGTH_SHORT).show()
            finish()
            return
        }
        val kind = o.optString("kind")
        LogStore.w(TAG, "IGLogin: Instagram refused the session ($kind)")
        val msg = o.optString("error").ifEmpty { "Instagram didn't accept the sign-in." }
        when (kind) {
            // Waiting on something in Instagram's own app: the session itself is fine.
            "checkpoint", "consent", "rate_limited", "unknown" -> showBlocked(msg) { retryWithSession() }
            else -> showBlocked(msg) { startOver() }
        }
    }

    // Retry with the session the WebView already holds; sign in again if it's gone.
    private fun retryWithSession() {
        val raw = igCookies()
        if (REQUIRED_COOKIES.all { it in cookieNames(raw) }) beginLogin(raw) else startOver()
    }

    private fun startOver() {
        clearIgCookies()
        WebStorage.getInstance().deleteOrigin(COOKIE_URL)
        showWeb()
    }

    private fun showBlocked(msg: String, retry: (() -> Unit)?) {
        stage = Stage.BLOCKED
        retryAction = retry
        retryBtn.visibility = if (retry != null) View.VISIBLE else View.GONE
        setStatus(msg, true)
    }

    private fun clearIgCookies() {
        val cm = CookieManager.getInstance()
        for (name in cookieNames(igCookies())) {
            cm.setCookie(COOKIE_URL, "$name=; Max-Age=0; Path=/")
            cm.setCookie(COOKIE_URL, "$name=; Max-Age=0; Path=/; Domain=.instagram.com")
        }
        cm.flush()
    }

    private fun setStatus(msg: String, error: Boolean) {
        status.setTextColor(if (error) errorRed else pal.text)
        status.text = msg
    }

    private fun palette(): Pal {
        val night = (resources.configuration.uiMode and
            Configuration.UI_MODE_NIGHT_MASK) == Configuration.UI_MODE_NIGHT_YES
        return if (night) Pal(
            header = Color.parseColor("#833AB4"),
            headerText = Color.WHITE,
            bg = Color.parseColor("#0E0E10"),
            card = Color.parseColor("#1C1C1F"),
            cardBorder = Color.parseColor("#2A2A2E"),
            text = Color.parseColor("#F2F2F5"),
            subtext = Color.parseColor("#9A9AA2"),
            button = Color.parseColor("#C13584"),
            onButton = Color.WHITE
        ) else Pal(
            header = Color.parseColor("#833AB4"),
            headerText = Color.WHITE,
            bg = Color.parseColor("#F5F4F8"),
            card = Color.WHITE,
            cardBorder = Color.parseColor("#E2E0E8"),
            text = Color.parseColor("#1A1A1E"),
            subtext = Color.parseColor("#6B6B73"),
            button = Color.parseColor("#C13584"),
            onButton = Color.WHITE
        )
    }

    private fun stack(topGap: Int) = LinearLayout.LayoutParams(
        ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.WRAP_CONTENT
    ).apply { if (topGap > 0) topMargin = dp(topGap) }

    private fun dp(v: Int): Int = (v * resources.displayMetrics.density).toInt()

    private fun statusBarHeight(): Int {
        val id = resources.getIdentifier("status_bar_height", "dimen", "android")
        return if (id > 0) resources.getDimensionPixelSize(id) else dp(28)
    }

    private fun rounded(fill: Int, radius: Int, stroke: Int = 0, strokeColor: Int = 0) =
        GradientDrawable().apply {
            setColor(fill)
            cornerRadius = dp(radius).toFloat()
            if (stroke > 0) setStroke(dp(stroke), strokeColor)
        }

    companion object {
        private const val TAG = "MirrorMsgSpike"
        private const val LOGIN_URL = "https://www.instagram.com/accounts/login/"
        private const val COOKIE_URL = "https://www.instagram.com"
        private const val SESSION_COOKIE = "sessionid"
        private val REQUIRED_COOKIES = listOf("sessionid", "csrftoken", "ds_user_id")
    }
}
