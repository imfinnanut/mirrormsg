package com.dano.mirrormsg

import android.app.Activity
import android.content.res.Configuration
import android.graphics.Color
import android.graphics.Typeface
import android.graphics.drawable.GradientDrawable
import android.os.Bundle
import android.text.InputType
import android.view.View
import android.view.ViewGroup
import android.widget.Button
import android.widget.EditText
import android.widget.LinearLayout
import android.widget.ScrollView
import android.widget.TextView
import android.widget.Toast

/**
 * TelegramLoginActivity — native Telegram login form, walking the telegrambridge
 * auth steps: phone -> code -> (2FA password if set). NOT a WebView cookie harvest;
 * this is the real MTProto auth flow via gotd/td.
 *
 * Each bridge call (sendCode / signIn / submitPassword) blocks on the gotd run loop
 * for up to ~30s, so every one runs OFF the main thread and posts its result back
 * with runOnUiThread — calling them on the UI thread would ANR (same reason
 * FbLoginActivity threads its connect()).
 *
 * UI matches the app's login idiom (see DiscordLoginActivity): a violet accent
 * header band, a status line, and carded rounded inputs, dark/light aware. Only the
 * presentation is styled here — the auth flow, bridge calls, and result handling are
 * unchanged.
 *
 * Part of the native-Telegram drop-out set: remove TelegramService.kt + this file +
 * the manifest <activity> entry + the ServiceRegistry candidate together.
 */
class TelegramLoginActivity : Activity() {

    private enum class Stage { PHONE, CODE, PASSWORD }

    // ---- theme (shared idiom with DiscordLoginActivity) ------------------------

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

    private fun rounded(fill: Int, radius: Int, stroke: Int = 0, strokeColor: Int = 0) =
        GradientDrawable().apply {
            setColor(fill)
            cornerRadius = dp(radius).toFloat()
            if (stroke > 0) setStroke(dp(stroke), strokeColor)
        }

    // ---- views / state ---------------------------------------------------------

    private lateinit var status: TextView
    private lateinit var phoneField: EditText
    private lateinit var codeField: EditText
    private lateinit var passwordField: EditText
    private lateinit var actionBtn: Button
    private lateinit var pal: Pal

    @Volatile private var stage = Stage.PHONE
    @Volatile private var busy = false
    private var enteredPhone = ""

    private val errorRed = Color.parseColor("#E5484D")

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        val p = palette()
        pal = p

        val root = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setBackgroundColor(p.bg)
            layoutParams = ViewGroup.LayoutParams(
                ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.MATCH_PARENT
            )
        }

        // Accent header band — runs up behind the status bar so the title clears the
        // clock/icons (same treatment as MainActivity / DiscordLoginActivity).
        val header = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setBackgroundColor(p.accent)
            setPadding(dp(20), statusBarHeight() + dp(6), dp(20), dp(18))
        }
        header.addView(TextView(this).apply {
            text = "Connect Telegram"; textSize = 26f; setTextColor(p.headerText)
            setTypeface(typeface, Typeface.BOLD)
        })
        root.addView(header)

        val scroll = ScrollView(this).apply {
            layoutParams = LinearLayout.LayoutParams(
                ViewGroup.LayoutParams.MATCH_PARENT, 0, 1f
            )
        }
        val content = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setPadding(dp(16), dp(14), dp(16), dp(20))
        }

        // Status / instruction line under the header. Selectable so an error can be
        // copied. Neutral (subtext) for guidance, red on failure.
        status = TextView(this).apply {
            text = "Enter your phone number in international format, including the country code."
            textSize = 13f; setTextColor(p.subtext)
            setPadding(dp(4), 0, dp(4), dp(14))
            setTextIsSelectable(true)
        }
        content.addView(status)

        // Fields + action live in a single rounded card.
        val card = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            background = rounded(p.card, 14, 1, p.cardBorder)
            setPadding(dp(16), dp(16), dp(16), dp(16))
        }

        fun styleInput(e: EditText) {
            e.setTextColor(p.text)
            e.setHintTextColor(p.subtext)
            e.textSize = 16f
            e.setPadding(dp(14), dp(14), dp(14), dp(14))
            e.background = rounded(p.bg, 10, 1, p.cardBorder)
        }

        fun stackParams(topGap: Int) = LinearLayout.LayoutParams(
            ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.WRAP_CONTENT
        ).apply { if (topGap > 0) topMargin = dp(topGap) }

        phoneField = EditText(this).apply {
            hint = "+1 555 123 4567"
            inputType = InputType.TYPE_CLASS_PHONE
        }.also(::styleInput)

        codeField = EditText(this).apply {
            hint = "Login code"
            inputType = InputType.TYPE_CLASS_NUMBER
            visibility = View.GONE
        }.also(::styleInput)

        passwordField = EditText(this).apply {
            hint = "Two-step (2FA) password"
            inputType = InputType.TYPE_CLASS_TEXT or InputType.TYPE_TEXT_VARIATION_PASSWORD
            visibility = View.GONE
        }.also(::styleInput)

        actionBtn = Button(this).apply {
            text = "Send code"
            isAllCaps = false
            textSize = 16f
            setTextColor(p.onAccent)
            setPadding(0, dp(14), 0, dp(14))
            background = rounded(p.accent, 12)
            stateListAnimator = null   // flat, no elevation shadow over our own bg
            setOnClickListener { onAction() }
        }

        card.addView(phoneField, stackParams(0))
        card.addView(codeField, stackParams(10))
        card.addView(passwordField, stackParams(10))
        card.addView(actionBtn, stackParams(16))

        content.addView(card)
        scroll.addView(content)
        root.addView(scroll)
        setContentView(root)
    }

    private fun onAction() {
        if (busy) return
        when (stage) {
            Stage.PHONE -> {
                val phone = phoneField.text.toString().trim().replace(" ", "")
                if (phone.isEmpty()) { showError("Enter a phone number."); return }
                enteredPhone = phone
                runStep("Sending code…") { TelegramService.bridge().sendCode(phone) }
            }
            Stage.CODE -> {
                val code = codeField.text.toString().trim()
                if (code.isEmpty()) { showError("Enter the code from your Telegram app."); return }
                runStep("Signing in…") { TelegramService.bridge().signIn(code) }
            }
            Stage.PASSWORD -> {
                val pw = passwordField.text.toString()
                if (pw.isEmpty()) { showError("Enter your two-step password."); return }
                runStep("Checking password…") { TelegramService.bridge().submitPassword(pw) }
            }
        }
    }

    /** Run a blocking bridge call off the main thread; route its result to onResult. */
    private fun runStep(busyMsg: String, work: () -> String) {
        busy = true
        actionBtn.isEnabled = false
        actionBtn.alpha = 0.5f
        status.setTextColor(pal.subtext)
        status.text = busyMsg
        Thread {
            val result = try {
                work()
            } catch (e: Throwable) {
                LogStore.e(TAG, "TG login step failed: ${e.message}", e)
                "error: ${e.message}"
            }
            runOnUiThread { onResult(result) }
        }.start()
    }

    private fun onResult(result: String) {
        busy = false
        actionBtn.isEnabled = true
        actionBtn.alpha = 1f
        when {
            result == "code-sent" -> {
                stage = Stage.CODE
                phoneField.isEnabled = false
                phoneField.alpha = 0.6f
                codeField.visibility = View.VISIBLE
                codeField.requestFocus()
                actionBtn.text = "Sign in"
                status.setTextColor(pal.subtext)
                status.text = "We sent a code to your Telegram app. Enter it above."
            }
            result == "password-needed" -> {
                stage = Stage.PASSWORD
                codeField.isEnabled = false
                codeField.alpha = 0.6f
                passwordField.visibility = View.VISIBLE
                passwordField.requestFocus()
                actionBtn.text = "Submit password"
                status.setTextColor(pal.subtext)
                status.text = "This account has two-step verification. Enter your password."
            }
            result == "authorized" -> {
                TelegramService.markLoggedIn(applicationContext, enteredPhone)
                LogStore.i(TAG, "TG login: authorized")
                Toast.makeText(this, "Telegram connected", Toast.LENGTH_SHORT).show()
                finish()
            }
            else -> {
                showError(if (result.isBlank()) "Login failed. Try again." else result)
                LogStore.w(TAG, "TG login result: $result")
            }
        }
    }

    /** Inline validation / failure text in the status line, in the error color. */
    private fun showError(msg: String) {
        status.setTextColor(errorRed)
        status.text = msg
    }

    companion object {
        private const val TAG = "MirrorMsgSpike"
    }
}
