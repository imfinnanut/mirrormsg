package com.dano.mirrormsg

import android.app.Activity
import android.content.res.Configuration
import android.graphics.Bitmap
import android.graphics.Color
import android.graphics.Typeface
import android.graphics.drawable.GradientDrawable
import android.os.Bundle
import android.os.Handler
import android.os.Looper
import android.text.InputType
import android.view.Gravity
import android.view.View
import android.view.ViewGroup
import android.widget.Button
import android.widget.EditText
import android.widget.FrameLayout
import android.widget.ImageView
import android.widget.LinearLayout
import android.widget.ScrollView
import android.widget.TextView
import com.google.zxing.BarcodeFormat
import com.google.zxing.EncodeHintType
import com.google.zxing.qrcode.QRCodeWriter
import org.json.JSONObject
import kotlin.concurrent.thread

/**
 * WhatsApp linked-device pairing screen. Two ways to link:
 *   1. Phone number (default): enter your number, tap Get code, get an 8-char code,
 *      then in WhatsApp -> Linked devices -> Link a device -> "Link with phone
 *      number instead" -> type the code.
 *   2. QR (fallback): tap "Scan QR instead" and scan from the same screen.
 *
 * UI matches the app's login idiom (see TelegramLoginActivity / DiscordLoginActivity):
 * a violet accent header band, a status line, and carded rounded elements, dark/light
 * aware. Only the presentation is styled here — the pairing flow, bridge calls, and
 * event handling are unchanged. The QR card stays white on purpose: a QR needs a
 * white quiet-zone and black modules to scan.
 *
 * Borrows WhatsAppService's single shared bridge (never builds its own).
 * Launch: WhatsAppService.startLogin(), or:
 *   adb shell am start -n com.dano.mirrormsg/.WhatsAppPairActivity
 */
class WhatsAppPairActivity : Activity() {

    private lateinit var root: LinearLayout
    private lateinit var status: TextView
    private lateinit var numberField: EditText
    private lateinit var getCodeBtn: Button
    private lateinit var codeView: TextView
    private lateinit var steps: TextView
    private lateinit var toggleBtn: Button
    private lateinit var qrCard: FrameLayout
    private lateinit var qrView: ImageView
    private lateinit var log: TextView
    private lateinit var pal: Pal

    private var wa: WhatsAppService? = null
    @Volatile private var flowReady = false
    @Volatile private var latestQr: String? = null
    private var qrMode = false

    private val main = Handler(Looper.getMainLooper())

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(buildUi())

        val svc = ServiceRegistry.byId(this, "whatsapp") as? WhatsAppService
        if (svc == null) {
            setStatus("WhatsApp service not registered in this build")
            return
        }
        wa = svc

        if (svc.connectedNow() || svc.pairedNow()) {
            setStatus("Already linked \u2713  \u2014 log out first to re-link")
        } else {
            setStatus("Connecting\u2026")
        }

        svc.setPairingListener { type, json ->
            LogStore.i(TAG, "WA-PAIR $type $json")
            main.post { handleEvent(type, json) }
        }

        thread(name = "wa-pair") { svc.beginPairing() }
        main.postDelayed({ markReady() }, 2500)
    }

    override fun onDestroy() {
        super.onDestroy()
        wa?.setPairingListener(null)
    }

    private fun markReady() {
        if (flowReady) return
        flowReady = true
        setPrimaryEnabled(true)
        if (!qrMode && codeView.visibility != View.VISIBLE) {
            setStatus("Enter your number and tap Get code")
        }
    }

    private fun handleEvent(eventType: String, jsonPayload: String) {
        when (eventType) {
            "QR" -> {
                latestQr = try { JSONObject(jsonPayload).optString("code") } catch (_: Exception) { "" }
                markReady()
                if (qrMode) latestQr?.let { if (it.isNotEmpty()) renderQr(it) }
            }
            "PAIRED" -> setStatus("Linked \u2713  \u2014 connecting\u2026")
            "CONNECTED" -> {
                qrView.setImageBitmap(null)
                onLinked()
            }
            "PAIR_TIMEOUT" -> setError("Code expired \u2014 tap Get code for a new one")
            "PAIR_ERROR"   -> setError("Pairing error \u2014 try again")
            "LOGGED_OUT"   -> setError("Logged out \u2014 session cleared")
            else -> appendLog("$eventType $jsonPayload")
        }
    }

    private fun onLinked() {
        setStatus("Connected \u2713")
        numberField.visibility = View.GONE
        getCodeBtn.visibility = View.GONE
        codeView.visibility = View.GONE
        steps.visibility = View.GONE
        toggleBtn.visibility = View.GONE
        qrCard.visibility = View.GONE
        codeView.text = ""
    }

    private fun onGetCode() {
        val number = numberField.text.toString().trim()
        if (number.replace(Regex("\\D"), "").length < 7) {
            setError("Enter your full number with country code (with the leading +).")
            return
        }
        setPrimaryEnabled(false)
        setStatus("Requesting code\u2026")
        thread(name = "wa-paircode") {
            try {
                val code = wa?.requestPairCode(number) ?: ""
                main.post { showCode(code) }
            } catch (e: Throwable) {
                LogStore.e(TAG, "requestPairCode failed", e)
                main.post {
                    setError("Couldn't get a code: ${e.message}")
                    setPrimaryEnabled(true)
                }
            }
        }
    }

    private fun showCode(code: String) {
        if (code.isEmpty()) {
            setError("Empty code returned \u2014 try again")
            setPrimaryEnabled(true)
            return
        }
        val pretty = if (code.length == 8) code.substring(0, 4) + "-" + code.substring(4) else code
        codeView.text = pretty
        codeView.visibility = View.VISIBLE
        steps.text = STEPS_CODE
        steps.visibility = View.VISIBLE
        setStatus("Enter this code in WhatsApp on your phone")
        setPrimaryEnabled(true)
        getCodeBtn.text = "Get a new code"
    }

    private fun onToggle() {
        qrMode = !qrMode
        if (qrMode) {
            toggleBtn.text = "Use phone number instead"
            numberField.visibility = View.GONE
            getCodeBtn.visibility = View.GONE
            codeView.visibility = View.GONE
            steps.text = STEPS_QR
            steps.visibility = View.VISIBLE
            qrCard.visibility = View.VISIBLE
            val q = latestQr
            if (q != null && q.isNotEmpty()) renderQr(q) else setStatus("Waiting for QR\u2026")
        } else {
            toggleBtn.text = "Scan QR instead"
            qrCard.visibility = View.GONE
            qrView.setImageBitmap(null)
            numberField.visibility = View.VISIBLE
            getCodeBtn.visibility = View.VISIBLE
            steps.text = STEPS_CODE
            steps.visibility = if (codeView.visibility == View.VISIBLE) View.VISIBLE else View.GONE
        }
    }

    private fun renderQr(text: String) {
        try {
            val size = 720
            val hints = mapOf(EncodeHintType.MARGIN to 1)
            val matrix = QRCodeWriter().encode(text, BarcodeFormat.QR_CODE, size, size, hints)
            val w = matrix.width
            val h = matrix.height
            val pixels = IntArray(w * h)
            for (y in 0 until h) {
                val off = y * w
                for (x in 0 until w) {
                    pixels[off + x] = if (matrix.get(x, y)) Color.BLACK else Color.WHITE
                }
            }
            val bmp = Bitmap.createBitmap(w, h, Bitmap.Config.ARGB_8888)
            bmp.setPixels(pixels, 0, w, 0, 0, w, h)
            qrView.setImageBitmap(bmp)
        } catch (e: Exception) {
            appendLog("QR render failed: ${e.message}")
        }
    }

    // ---- theme (shared idiom with Telegram / Discord login) -----------------

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

    private fun statusBarHeight(): Int {
        val id = resources.getIdentifier("status_bar_height", "dimen", "android")
        return if (id > 0) resources.getDimensionPixelSize(id) else dp(28)
    }

    // ---- UI construction ----------------------------------------------------

    private fun buildUi(): ViewGroup {
        val p = palette()
        pal = p

        root = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setBackgroundColor(p.bg)
            layoutParams = ViewGroup.LayoutParams(
                ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.MATCH_PARENT
            )
        }

        // Accent header band — runs up behind the status bar so the title clears the
        // clock/icons (same treatment as the Telegram / Discord login screens).
        val header = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setBackgroundColor(p.accent)
            setPadding(dp(20), statusBarHeight() + dp(6), dp(20), dp(18))
        }
        header.addView(TextView(this).apply {
            text = "Connect WhatsApp"; textSize = 26f; setTextColor(p.headerText)
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

        status = TextView(this).apply {
            textSize = 13f
            setTextColor(p.subtext)
            setPadding(dp(4), 0, dp(4), dp(14))
            setTextIsSelectable(true)
        }

        numberField = EditText(this).apply {
            hint = "Country code + number (e.g. +1 555 123 4567)"
            inputType = InputType.TYPE_CLASS_PHONE
            setTextColor(p.text)
            setHintTextColor(p.subtext)
            textSize = 16f
            background = rounded(p.bg, 12, 1, p.cardBorder)
            setPadding(dp(14), dp(14), dp(14), dp(14))
        }

        getCodeBtn = Button(this).apply {
            text = "Get code"
            isAllCaps = false
            textSize = 16f
            setTextColor(p.onAccent)
            setTypeface(typeface, Typeface.BOLD)
            background = rounded(p.accent, 12)
            stateListAnimator = null
            setPadding(0, dp(14), 0, dp(14))
            setOnClickListener { onGetCode() }
        }

        codeView = TextView(this).apply {
            textSize = 32f
            setTypeface(Typeface.MONOSPACE, Typeface.BOLD)
            setTextColor(p.accent)
            setTextIsSelectable(true)
            gravity = Gravity.CENTER
            background = rounded(p.card, 12, 1, p.cardBorder)
            setPadding(dp(16), dp(20), dp(16), dp(20))
            visibility = View.GONE
        }

        steps = TextView(this).apply {
            text = STEPS_CODE
            textSize = 13f
            setTextColor(p.subtext)
            setLineSpacing(dp(2).toFloat(), 1f)
            setPadding(dp(2), dp(12), dp(2), dp(8))
            visibility = View.GONE
        }

        toggleBtn = Button(this).apply {
            text = "Scan QR instead"
            isAllCaps = false
            textSize = 15f
            setTextColor(p.text)
            background = roundedStroke(p.cardBorder, 12, 1)
            stateListAnimator = null
            setPadding(0, dp(12), 0, dp(12))
            setOnClickListener { onToggle() }
        }

        qrView = ImageView(this).apply {
            layoutParams = FrameLayout.LayoutParams(dp(240), dp(240)).apply {
                gravity = Gravity.CENTER
            }
        }
        qrCard = FrameLayout(this).apply {
            background = rounded(Color.WHITE, 16)
            setPadding(dp(14), dp(14), dp(14), dp(14))
            addView(qrView)
            visibility = View.GONE
        }

        log = TextView(this).apply {
            textSize = 10f
            setTextColor(p.subtext)
            alpha = 0.6f
            setTextIsSelectable(true)
            setPadding(dp(2), dp(14), dp(2), 0)
        }

        content.addView(status)
        content.addView(numberField, stackParams(0))
        content.addView(getCodeBtn, stackParams(12))
        content.addView(codeView, stackParams(18))
        content.addView(steps, stackParams(0))
        content.addView(toggleBtn, stackParams(20))
        content.addView(qrCard, LinearLayout.LayoutParams(
            ViewGroup.LayoutParams.WRAP_CONTENT, ViewGroup.LayoutParams.WRAP_CONTENT
        ).apply { topMargin = dp(20); gravity = Gravity.CENTER_HORIZONTAL })
        content.addView(log, stackParams(0))

        scroll.addView(content)
        root.addView(scroll)
        return root
    }

    private fun stackParams(topGap: Int) = LinearLayout.LayoutParams(
        ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.WRAP_CONTENT
    ).apply { if (topGap > 0) topMargin = dp(topGap) }

    private fun rounded(fill: Int, radiusDp: Int, strokeDp: Int = 0, strokeColor: Int = 0): GradientDrawable =
        GradientDrawable().apply {
            setColor(fill)
            cornerRadius = dp(radiusDp).toFloat()
            if (strokeDp > 0) setStroke(dp(strokeDp), strokeColor)
        }

    private fun roundedStroke(strokeColor: Int, radiusDp: Int, strokeDp: Int): GradientDrawable =
        GradientDrawable().apply {
            setColor(Color.TRANSPARENT)
            cornerRadius = dp(radiusDp).toFloat()
            setStroke(dp(strokeDp), strokeColor)
        }

    private fun setPrimaryEnabled(en: Boolean) {
        getCodeBtn.isEnabled = en
        getCodeBtn.alpha = if (en) 1f else 0.4f
    }

    private fun setStatus(s: String) {
        status.setTextColor(pal.subtext)
        status.text = s
        appendLog("\u2022 $s")
    }

    private fun setError(s: String) {
        status.setTextColor(ERROR_RED)
        status.text = s
        appendLog("\u2022 $s")
    }

    private fun appendLog(s: String) { log.append(s + "\n") }
    private fun dp(v: Int): Int = (v * resources.displayMetrics.density).toInt()

    companion object {
        private const val TAG = "MirrorMsgSpike"

        private val ERROR_RED = Color.parseColor("#E5484D")

        private const val STEPS_CODE =
            "On your phone: WhatsApp \u2192 Settings \u2192 Linked devices \u2192 " +
            "Link a device \u2192 \"Link with phone number instead\" \u2192 enter this code."
        private const val STEPS_QR =
            "On your phone: WhatsApp \u2192 Settings \u2192 Linked devices \u2192 " +
            "Link a device \u2192 scan this code."
    }
}
