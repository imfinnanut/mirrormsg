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
import android.view.Gravity
import android.view.View
import android.view.ViewGroup
import android.widget.Button
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
 * Signal linked-device pairing. Signal only links by having the PRIMARY phone
 * scan a QR, and this phone IS the primary — so the watch is the second screen:
 * the bridge posts the code into a "MirrorMsg · Signal" picture message, the
 * watch pops it like any incoming picture, and the phone's Signal scans the
 * watch. The on-phone QR stays available as a fallback for scanning from another
 * device. Borrows SignalService's single shared bridge.
 */
class SignalPairActivity : Activity() {

    private lateinit var root: LinearLayout
    private lateinit var status: TextView
    private lateinit var stepsCard: LinearLayout
    private lateinit var sendBtn: Button
    private lateinit var fallbackBtn: Button
    private lateinit var qrCard: FrameLayout
    private lateinit var qrView: ImageView
    private lateinit var doneNote: TextView
    private lateinit var log: TextView
    private lateinit var pal: Pal

    private var sig: SignalService? = null
    @Volatile private var latestQr: String? = null
    private var showPhoneQr = false

    private val main = Handler(Looper.getMainLooper())

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(buildUi())

        val svc = ServiceRegistry.byId(this, "signalnative") as? SignalService
        if (svc == null) {
            setError("Signal service not registered in this build")
            return
        }
        sig = svc

        svc.setPairingListener { type, json ->
            if (type == "PAIR_ERROR") LogStore.i(TAG, "SIG-PAIR $type $json") else if (type != "QR") LogStore.i(TAG, "SIG-PAIR $type")
            main.post { handleEvent(type, json) }
        }

        when {
            svc.connectedNow() || svc.pairedNow() -> {
                onLinked(connected = svc.connectedNow())
                setStatus(if (svc.connectedNow()) "Connected ✓" else "Already linked ✓  — connecting…")
                thread(name = "sig-pair") { svc.beginPairing() }
            }
            svc.linkingNow() -> {
                setStatus("A code is already on your watch — open the picture and scan it with Signal.")
                sendBtn.text = "Send a fresh code to my watch"
            }
            else -> setStatus("Read the three steps, then tap the button.")
        }
    }

    override fun onDestroy() {
        super.onDestroy()
        sig?.setPairingListener(null)
    }

    private fun handleEvent(eventType: String, jsonPayload: String) {
        when (eventType) {
            "QR" -> {
                latestQr = try { JSONObject(jsonPayload).optString("code") } catch (_: Exception) { "" }
                if (showPhoneQr) latestQr?.let { if (it.isNotEmpty()) renderQr(it) }
                sendBtn.isEnabled = true
                sendBtn.alpha = 1f
                sendBtn.text = "Send a fresh code to my watch"
                setStatus("Code sent to your watch ✓  Open the picture there, then scan it with Signal on this phone.")
            }
            "PAIRED" -> {
                onLinked(connected = false)
                setStatus("Linked ✓  — connecting…")
            }
            "CONNECTED" -> {
                onLinked(connected = true)
                setStatus("Connected ✓")
            }
            "PAIR_ERROR" -> {
                val msg = try { JSONObject(jsonPayload).optString("error") } catch (_: Exception) { "" }
                setError(if (msg.isNotEmpty()) msg else "Linking failed — try again")
                qrView.setImageBitmap(null)
                sendBtn.isEnabled = true
                sendBtn.alpha = 1f
                sendBtn.text = "Start over"
            }
            "DISCONNECTED" -> appendLog("disconnected $jsonPayload")
            "LOGGED_OUT" -> {
                setError("Signal unlinked this device — log out in MirrorMsg and link again")
                sendBtn.visibility = View.GONE
            }
            else -> appendLog("$eventType $jsonPayload")
        }
    }

    private fun onLinked(connected: Boolean) {
        stepsCard.visibility = View.GONE
        sendBtn.visibility = View.GONE
        fallbackBtn.visibility = View.GONE
        qrCard.visibility = View.GONE
        qrView.setImageBitmap(null)
        doneNote.visibility = View.VISIBLE
        doneNote.text = if (connected)
            "Signal chats show up on the watch as messages arrive. Pick Signal in the watch's account switcher (hold Up)."
        else
            "Finishing up…"
    }

    private fun onSend() {
        val svc = sig ?: return
        sendBtn.isEnabled = false
        sendBtn.alpha = 0.4f
        setStatus("Getting a code from Signal…")
        thread(name = "sig-pair") {
            svc.cancelPairing()
            svc.beginPairing()
        }
    }

    private fun onToggleFallback() {
        showPhoneQr = !showPhoneQr
        if (showPhoneQr) {
            fallbackBtn.text = "Hide the code"
            qrCard.visibility = View.VISIBLE
            val q = latestQr
            if (q != null && q.isNotEmpty()) renderQr(q) else appendLog("no code yet — tap Send first")
        } else {
            fallbackBtn.text = "Scanning from another device? Show the code here"
            qrCard.visibility = View.GONE
            qrView.setImageBitmap(null)
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

    // ---- theme (shared idiom with the other login screens) ------------------

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

        val header = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setBackgroundColor(p.accent)
            setPadding(dp(20), statusBarHeight() + dp(6), dp(20), dp(18))
        }
        header.addView(TextView(this).apply {
            text = "Connect Signal"; textSize = 26f; setTextColor(p.headerText)
            setTypeface(typeface, Typeface.BOLD)
        })
        header.addView(TextView(this).apply {
            text = "Your watch shows the code. Your phone's Signal scans it."
            textSize = 14f; setTextColor(p.headerText); alpha = 0.9f
            setPadding(0, dp(4), 0, 0)
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
            textSize = 14f
            setTextColor(p.subtext)
            setPadding(dp(4), 0, dp(4), dp(12))
            setTextIsSelectable(true)
        }

        stepsCard = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            background = rounded(p.card, 14, 1, p.cardBorder)
            setPadding(dp(16), dp(10), dp(16), dp(6))
        }
        addStep(stepsCard, p, "1", "Wear your Pebble and open MirrorMsg on it.")
        addStep(stepsCard, p, "2",
            "Tap the button below. The code lands on the watch as a picture in a " +
            "“MirrorMsg · Signal” chat — open the picture so it fills the screen.")
        addStep(stepsCard, p, "3",
            "On THIS phone: Signal → Settings → Linked devices → Link new device, " +
            "then point the camera at the watch. The code renews every 75 seconds: if Signal " +
            "says it is invalid, back out of the picture on the watch and open it again.")

        sendBtn = Button(this).apply {
            text = "Send code to my watch"
            isAllCaps = false
            textSize = 17f
            setTextColor(p.onAccent)
            setTypeface(typeface, Typeface.BOLD)
            background = rounded(p.accent, 12)
            stateListAnimator = null
            setPadding(0, dp(16), 0, dp(16))
            setOnClickListener { onSend() }
        }

        fallbackBtn = Button(this).apply {
            text = "Scanning from another device? Show the code here"
            isAllCaps = false
            textSize = 13f
            setTextColor(p.subtext)
            background = roundedStroke(p.cardBorder, 12, 1)
            stateListAnimator = null
            setPadding(dp(8), dp(10), dp(8), dp(10))
            setOnClickListener { onToggleFallback() }
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

        doneNote = TextView(this).apply {
            textSize = 15f
            setTextColor(p.text)
            setLineSpacing(dp(2).toFloat(), 1f)
            background = rounded(p.card, 14, 1, p.cardBorder)
            setPadding(dp(16), dp(14), dp(16), dp(14))
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
        content.addView(stepsCard, stackParams(0))
        content.addView(sendBtn, stackParams(16))
        content.addView(doneNote, stackParams(0))
        content.addView(fallbackBtn, stackParams(22))
        content.addView(qrCard, LinearLayout.LayoutParams(
            ViewGroup.LayoutParams.WRAP_CONTENT, ViewGroup.LayoutParams.WRAP_CONTENT
        ).apply { topMargin = dp(14); gravity = Gravity.CENTER_HORIZONTAL })
        content.addView(log, stackParams(0))

        scroll.addView(content)
        root.addView(scroll)
        return root
    }

    private fun addStep(parent: LinearLayout, p: Pal, number: String, text: String) {
        val row = LinearLayout(this).apply {
            orientation = LinearLayout.HORIZONTAL
            setPadding(0, dp(8), 0, dp(8))
        }
        row.addView(TextView(this).apply {
            this.text = number
            textSize = 15f
            setTextColor(p.onAccent)
            setTypeface(typeface, Typeface.BOLD)
            gravity = Gravity.CENTER
            background = GradientDrawable().apply { shape = GradientDrawable.OVAL; setColor(p.accent) }
            layoutParams = LinearLayout.LayoutParams(dp(30), dp(30)).apply { rightMargin = dp(12) }
        })
        row.addView(TextView(this).apply {
            this.text = text
            textSize = 15f
            setTextColor(p.text)
            setLineSpacing(dp(2).toFloat(), 1f)
            layoutParams = LinearLayout.LayoutParams(0, ViewGroup.LayoutParams.WRAP_CONTENT, 1f)
        })
        parent.addView(row)
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

    private fun setStatus(s: String) {
        status.setTextColor(pal.subtext)
        status.text = s
        appendLog("• $s")
    }

    private fun setError(s: String) {
        status.setTextColor(ERROR_RED)
        status.text = s
        appendLog("• $s")
    }

    private fun appendLog(s: String) { log.append(s + "\n") }
    private fun dp(v: Int): Int = (v * resources.displayMetrics.density).toInt()

    companion object {
        private const val TAG = "MirrorMsgSpike"
        private val ERROR_RED = Color.parseColor("#E5484D")
    }
}
