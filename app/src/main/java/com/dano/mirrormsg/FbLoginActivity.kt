package com.dano.mirrormsg

import android.app.Activity
import android.os.Bundle
import android.webkit.CookieManager
import android.webkit.WebView
import android.webkit.WebViewClient
import android.widget.Toast
import org.json.JSONObject

/**
 * FbLoginActivity — Facebook/Messenger cookie-harvest login.
 *
 * Loads the Messenger login page in a WebView, lets the user sign in, then
 * harvests the session cookies and hands them to the Facebook bridge.
 *
 * ── THE HTTPONLY QUESTION ────────────────────────────────────────────────────
 * Required FB cookies: xs, c_user, datr. **xs is HttpOnly** — the same class of
 * problem Google's __Secure-3PSID hit. CookieManager *may or may not* expose it.
 * On the harvest we print a VERDICT line: is `xs` visible? If yes, this WebView
 * is the real login. If no, FB needs the GeckoView/extension path like Google.
 *
 * ── DETECTION + MANUAL FALLBACK ──────────────────────────────────────────────
 * Auto-detect waits for the c_user cookie (set once logged in), harvests the
 * cookies, hands them to the bridge, and finishes the activity automatically.
 * Every page load logs whether c_user is present yet, so the log shows progress.
 *
 * ── DROP-OUT PAIRING ─────────────────────────────────────────────────────────
 * References FacebookMessengerService (imports the FB AAR). Part of the FB
 * drop-out set: remove the combined AAR's FB package + FacebookMessengerService.kt
 * + this file + the manifest <activity> entry together.
 *
 * PRIVACY: xs + c_user = full Messenger account access. Real session secrets.
 * Never paste these logs anywhere without scrubbing.
 */
class FbLoginActivity : Activity() {

    companion object {
        private const val TAG = "MirrorMsgSpike"
        // facebook.com/login is far more WebView-tolerant than messenger.com,
        // which redirects embedded browsers to an app-download promo. Facebook
        // login sets the SAME c_user/xs/datr cookies (shared across FB domains),
        // so harvesting still works — we read cookies across all FB domains below.
        private const val LOGIN_URL = "https://www.facebook.com/login/"
        // Desktop Chrome UA — stops FB/Messenger from punting the embedded
        // browser to a "get the app" page.
        private const val DESKTOP_UA =
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
            "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
        private const val SIGNED_IN_MARKER = "c_user="
    }

    @Volatile private var handled = false
    private lateinit var cm: CookieManager

    private val cookieUrls = listOf(
        "https://www.messenger.com",
        "https://messenger.com",
        "https://www.facebook.com",
        "https://facebook.com"
    )

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        val wv = WebView(this)
        setContentView(wv)

        cm = CookieManager.getInstance()
        cm.setAcceptCookie(true)
        cm.setAcceptThirdPartyCookies(wv, true)

        wv.settings.javaScriptEnabled = true
        wv.settings.domStorageEnabled = true
        // Desktop UA so FB serves the real login form, not the app-promo page.
        wv.settings.userAgentString = DESKTOP_UA

        wv.webViewClient = object : WebViewClient() {
            override fun onPageFinished(view: WebView?, url: String?) {
                cm.flush()
                val signedIn = cookieUrls.any {
                    cm.getCookie(it)?.contains(SIGNED_IN_MARKER) == true
                }
                // Live progress: shows whether login is detected yet.
                LogStore.i(TAG, "FbLogin page done: $url (c_user present: $signedIn)")
                if (handled) return
                if (signedIn) {
                    cm.flush()
                    completeLogin(cm)
                }
            }
        }

        wv.loadUrl(LOGIN_URL)
    }

    /**
     * Harvest cookies across FB domains into one map, print the HttpOnly verdict,
     * and (if xs is present) hand to the bridge.
     */
    private fun completeLogin(cm: CookieManager) {
        if (handled) return

        val merged = HashMap<String, String>()
        for (u in cookieUrls) {
            val raw = cm.getCookie(u) ?: continue
            for (pair in raw.split(";")) {
                val eq = pair.indexOf('=')
                if (eq <= 0) continue
                val k = pair.substring(0, eq).trim()
                val v = pair.substring(eq + 1).trim()
                if (k.isNotEmpty() && v.isNotEmpty()) merged[k] = v
            }
        }

        val hasCUser = merged.containsKey("c_user")
        val hasXs = merged.containsKey("xs")
        val hasDatr = merged.containsKey("datr")

        // Log only the NAMES present, never values (values are secrets).
        LogStore.i(TAG, ">>> FB cookie harvest: c_user=$hasCUser xs=$hasXs datr=$hasDatr " +
            "(total ${merged.size}) <<<")
        LogStore.i(TAG, ">>> xs (HttpOnly) visible to CookieManager? $hasXs <<<")

        if (!hasCUser) {
            // Not actually logged in yet — don't consume the attempt. Tell the
            // user to finish login, then press the button.
            LogStore.i(TAG, "FbLogin: no c_user yet — not logged in. Finish login, then press Done.")
            Toast.makeText(
                this,
                "Not logged in yet — sign in to Messenger, then tap \"Done\".",
                Toast.LENGTH_LONG
            ).show()
            return
        }

        if (!hasXs) {
            // Same wall as Google's __Secure-3PSID — xs is HttpOnly-hidden.
            LogStore.e(TAG, "FbLogin: xs not visible to CookieManager — WebView " +
                "harvest insufficient, need GeckoView/extension path (like Google).")
            Toast.makeText(
                this,
                "Facebook login needs extension-based capture (xs is HttpOnly). See logs.",
                Toast.LENGTH_LONG
            ).show()
            return
        }

        // xs IS visible — harvest is sufficient. Hand to the bridge.
        handled = true
        handToBridge(merged)
    }

    private fun handToBridge(cookies: Map<String, String>) {
        val json = JSONObject(cookies as Map<*, *>).toString()
        try {
            val service = ServiceRegistry.byId(this, "facebook")
            if (service is FacebookMessengerService) {
                val err = service.completeCookieLogin(json)
                if (err == null) {
                    LogStore.i(TAG, "FbLogin: cookies accepted, session saved, connecting.")
                    Toast.makeText(this, "Facebook connected.", Toast.LENGTH_SHORT).show()
                    finish()
                } else {
                    handled = false // allow retry
                    LogStore.e(TAG, "FbLogin: bridge rejected cookies: $err")
                    Toast.makeText(this, "Facebook login failed: $err", Toast.LENGTH_LONG).show()
                }
            } else {
                handled = false
                LogStore.e(TAG, "FbLogin: FacebookMessengerService not available.")
                Toast.makeText(this, "Facebook service not available.", Toast.LENGTH_LONG).show()
            }
        } catch (e: Throwable) {
            handled = false
            LogStore.e(TAG, "FbLogin: handoff failed", e)
            Toast.makeText(this, "Facebook login error: ${e.message}", Toast.LENGTH_LONG).show()
        }
    }
}
