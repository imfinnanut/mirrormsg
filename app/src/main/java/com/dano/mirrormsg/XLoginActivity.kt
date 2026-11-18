package com.dano.mirrormsg

import android.app.Activity
import android.content.res.Configuration
import android.graphics.Color
import android.graphics.Typeface
import android.graphics.drawable.GradientDrawable
import android.os.Bundle
import android.os.Handler
import android.os.Looper
import android.text.InputType
import android.view.View
import android.view.ViewGroup
import android.webkit.CookieManager
import android.webkit.WebStorage
import android.webkit.WebView
import android.webkit.WebViewClient
import android.widget.Button
import android.widget.EditText
import android.widget.LinearLayout
import android.widget.ScrollView
import android.widget.TextView
import android.widget.Toast
import org.json.JSONObject

/**
 * Native X login: sign in to x.com in a WebView, hand the session cookies to the
 * bridge, then unlock X Chat with the user's own X Chat passcode. A passcode is
 * only ever submitted when the user taps the button — X limits wrong attempts.
 * Part of the native-X drop-out set (see XService).
 */
class XLoginActivity : Activity() {

    private enum class Stage { WEB, CHECKING, PASSCODE, BLOCKED }

    private data class Pal(
        val header: Int, val headerText: Int, val bg: Int, val card: Int, val cardBorder: Int,
        val text: Int, val subtext: Int, val button: Int, val onButton: Int
    )

    private lateinit var pal: Pal
    private lateinit var root: LinearLayout
    private lateinit var signInHint: TextView
    private lateinit var scroll: ScrollView
    private lateinit var status: TextView
    private lateinit var who: TextView
    private lateinit var pinField: EditText
    private lateinit var actionBtn: Button
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
            text = "Connect X"
            textSize = 26f
            setTextColor(pal.headerText)
            setTypeface(typeface, Typeface.BOLD)
        })
        signInHint = TextView(this).apply {
            text = "Sign in with your X username or email and password. Google and Apple sign-in don't work here."
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
        status = TextView(this).apply {
            textSize = 13f
            setTextColor(pal.subtext)
            setPadding(dp(4), 0, dp(4), dp(14))
            setTextIsSelectable(true)
        }
        content.addView(status)

        val card = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            background = rounded(pal.card, 14, 1, pal.cardBorder)
            setPadding(dp(16), dp(16), dp(16), dp(16))
        }
        who = TextView(this).apply {
            textSize = 16f
            setTextColor(pal.text)
            setTypeface(typeface, Typeface.BOLD)
            visibility = View.GONE
        }
        pinField = EditText(this).apply {
            hint = "X Chat passcode"
            inputType = InputType.TYPE_CLASS_NUMBER or InputType.TYPE_NUMBER_VARIATION_PASSWORD
            setTextColor(pal.text)
            setHintTextColor(pal.subtext)
            textSize = 16f
            setPadding(dp(14), dp(14), dp(14), dp(14))
            background = rounded(pal.bg, 10, 1, pal.cardBorder)
            visibility = View.GONE
        }
        actionBtn = Button(this).apply {
            text = "Unlock X Chat"
            isAllCaps = false
            textSize = 16f
            setTextColor(pal.onButton)
            setPadding(0, dp(14), 0, dp(14))
            background = rounded(pal.button, 12)
            stateListAnimator = null
            visibility = View.GONE
            setOnClickListener { submitPasscode() }
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
        card.addView(who, stack(0))
        card.addView(pinField, stack(10))
        card.addView(actionBtn, stack(16))
        card.addView(retryBtn, stack(10))
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
        if ("auth_token" !in cookieNames(xCookies())) {
            clearXCookies()
            WebStorage.getInstance().deleteOrigin(COOKIE_URL)
        }
        LogStore.i(TAG, "XLogin: WebView UA ${wv.settings.userAgentString}")
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

    private fun xCookies(): String = CookieManager.getInstance().getCookie(COOKIE_URL) ?: ""

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
        LogStore.i(TAG, "XLogin: passing ${kept.size} cookies to the bridge (${kept.joinToString(", ") { it.substringBefore('=') }})" +
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
        val raw = xCookies()
        val names = cookieNames(raw)
        if ("auth_token" !in names || "ct0" !in names) return
        CookieManager.getInstance().flush()
        LogStore.i(TAG, "XLogin: x.com session found (${names.size} cookies), checking X Chat")
        handler.removeCallbacks(cookiePoll)
        destroyWeb()
        beginLogin(raw)
    }

    private fun beginLogin(cookieHeader: String) {
        stage = Stage.CHECKING
        signInHint.visibility = View.GONE
        scroll.visibility = View.VISIBLE
        who.visibility = View.GONE
        pinField.visibility = View.GONE
        actionBtn.visibility = View.GONE
        retryBtn.visibility = View.GONE
        setStatus("Signed in to X. Checking X Chat…", false)
        busy = true
        Thread({
            var result: String? = null
            var error: String? = null
            try {
                val b = XService.bridgeOrNull() ?: throw IllegalStateException("the X bridge isn't available")
                result = b.beginLogin(bridgeCookieHeader(cookieHeader))
            } catch (t: Throwable) {
                error = t.message ?: "unknown error"
                LogStore.w(TAG, "XLogin: beginLogin failed: $error")
            }
            runOnUiThread { if (!isFinishing && !isDestroyed) onBegin(result, error) }
        }, "x-login").start()
    }

    private fun onBegin(result: String?, error: String?) {
        busy = false
        if (result == null) {
            showBlocked("The sign-in didn't go through: $error\n\nTap Try again to sign in fresh.") {
                clearXCookies()
                showWeb()
            }
            return
        }
        val o = try { JSONObject(result) } catch (_: Throwable) { JSONObject() }
        val handle = o.optString("screenName")
        val name = o.optString("name")
        who.text = when {
            name.isNotEmpty() && handle.isNotEmpty() -> "$name  @$handle"
            handle.isNotEmpty() -> "@$handle"
            else -> name
        }
        who.visibility = if (who.text.isNullOrEmpty()) View.GONE else View.VISIBLE
        LogStore.i(TAG, "XLogin: passcode state ${o.optString("passcode")}")
        when (o.optString("passcode")) {
            "recover" -> showPasscode()
            "missing" -> showBlocked(
                "This account has no X Chat passcode yet, so its chats can't be unlocked. " +
                    "In the X app, open Chat and set up a passcode, then tap Try again."
            ) { beginLogin(xCookies()) }
            else -> showBlocked(
                "Couldn't check X Chat: ${o.optString("detail").ifEmpty { "unknown error" }}"
            ) { beginLogin(xCookies()) }
        }
    }

    private fun showPasscode() {
        stage = Stage.PASSCODE
        retryBtn.visibility = View.GONE
        pinField.visibility = View.VISIBLE
        pinField.isEnabled = true
        actionBtn.visibility = View.VISIBLE
        setButtonEnabled(true)
        setStatus(
            "Enter the X Chat passcode you set up in the X app. " +
                "X allows only a limited number of wrong attempts, so type it carefully.",
            false
        )
        pinField.requestFocus()
    }

    private fun submitPasscode() {
        if (busy || stage != Stage.PASSCODE) return
        val pin = pinField.text.toString().trim()
        if (pin.isEmpty()) {
            setStatus("Enter your X Chat passcode.", true)
            return
        }
        busy = true
        setButtonEnabled(false)
        pinField.isEnabled = false
        setStatus("Unlocking X Chat…", false)
        Thread({
            var result: String? = null
            var error: String? = null
            try {
                val b = XService.bridgeOrNull() ?: throw IllegalStateException("the X bridge isn't available")
                result = b.submitPasscode(pin)
                val unlocked = try { JSONObject(result).optBoolean("ok", false) } catch (_: Throwable) { false }
                if (unlocked) XService.onLoginSucceeded()
            } catch (t: Throwable) {
                error = t.message ?: "unknown error"
            }
            runOnUiThread { if (!isFinishing && !isDestroyed) onPasscodeResult(result, error) }
        }, "x-passcode").start()
    }

    private fun onPasscodeResult(result: String?, error: String?) {
        busy = false
        pinField.isEnabled = true
        if (result == null) {
            LogStore.w(TAG, "XLogin: submitPasscode failed: $error")
            setButtonEnabled(true)
            setStatus("Couldn't unlock X Chat: $error", true)
            return
        }
        val o = try { JSONObject(result) } catch (_: Throwable) { JSONObject() }
        if (o.optBoolean("ok", false)) {
            LogStore.i(TAG, "XLogin: X Chat unlocked")
            clearXCookies()
            Toast.makeText(this, "X connected", Toast.LENGTH_SHORT).show()
            finish()
            return
        }
        pinField.text.clear()
        when {
            o.optBoolean("locked", false) -> {
                LogStore.w(TAG, "XLogin: passcode attempts used up")
                showBlocked(
                    "No attempts left. X has locked this passcode. Reset your X Chat passcode " +
                        "in the X app, then log in here again.", null
                )
            }
            o.has("guessesLeft") -> {
                val left = o.optInt("guessesLeft", 0)
                LogStore.w(TAG, "XLogin: wrong passcode, $left attempts left")
                setButtonEnabled(true)
                setStatus("Wrong passcode. $left ${if (left == 1) "attempt" else "attempts"} left.", true)
            }
            else -> {
                setButtonEnabled(true)
                setStatus(o.optString("error").ifEmpty { "Couldn't unlock X Chat." }, true)
            }
        }
    }

    private fun showBlocked(msg: String, retry: (() -> Unit)?) {
        stage = Stage.BLOCKED
        pinField.visibility = View.GONE
        actionBtn.visibility = View.GONE
        retryAction = retry
        retryBtn.visibility = if (retry != null) View.VISIBLE else View.GONE
        setStatus(msg, true)
    }

    private fun clearXCookies() {
        val cm = CookieManager.getInstance()
        for (name in cookieNames(xCookies())) {
            cm.setCookie(COOKIE_URL, "$name=; Max-Age=0; Path=/")
            cm.setCookie(COOKIE_URL, "$name=; Max-Age=0; Path=/; Domain=.x.com")
        }
        cm.flush()
    }

    private fun setStatus(msg: String, error: Boolean) {
        status.setTextColor(if (error) errorRed else pal.subtext)
        status.text = msg
    }

    private fun setButtonEnabled(on: Boolean) {
        actionBtn.isEnabled = on
        actionBtn.alpha = if (on) 1f else 0.5f
    }

    private fun palette(): Pal {
        val night = (resources.configuration.uiMode and
            Configuration.UI_MODE_NIGHT_MASK) == Configuration.UI_MODE_NIGHT_YES
        return if (night) Pal(
            header = Color.parseColor("#000000"),
            headerText = Color.WHITE,
            bg = Color.parseColor("#0E0E10"),
            card = Color.parseColor("#1C1C1F"),
            cardBorder = Color.parseColor("#2A2A2E"),
            text = Color.parseColor("#F2F2F5"),
            subtext = Color.parseColor("#9A9AA2"),
            button = Color.parseColor("#EFF3F4"),
            onButton = Color.parseColor("#0F1419")
        ) else Pal(
            header = Color.parseColor("#000000"),
            headerText = Color.WHITE,
            bg = Color.parseColor("#F5F4F8"),
            card = Color.WHITE,
            cardBorder = Color.parseColor("#E2E0E8"),
            text = Color.parseColor("#1A1A1E"),
            subtext = Color.parseColor("#6B6B73"),
            button = Color.parseColor("#0F1419"),
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
        private const val LOGIN_URL = "https://x.com/i/flow/login"
        private const val COOKIE_URL = "https://x.com"
    }
}
