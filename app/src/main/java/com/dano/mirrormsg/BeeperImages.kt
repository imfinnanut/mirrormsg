package com.dano.mirrormsg

import android.app.Notification
import android.content.Context
import android.graphics.Bitmap
import android.graphics.BitmapFactory
import android.os.Bundle
import android.service.notification.StatusBarNotification
import android.util.Base64
import androidx.core.app.NotificationCompat
import org.json.JSONObject
import java.io.File
import java.io.FileOutputStream

/**
 * Shared media layer for the Beeper-backed networks (Discord, Google Voice, …).
 *
 * WHY THIS EXISTS — Beeper's content provider exposes images only as `mxc://`
 * Matrix handles that no on-device caller can resolve to bytes (verified: no
 * provider media endpoint, files sandboxed in the Beeper app, no localhost
 * server, the original CDN URL stripped by the bridge). The ONE place the actual
 * decoded pixels are handed to us is the system notification Beeper posts for a
 * new message. So we capture the bitmap there, via a NotificationListenerService,
 * and cache it ourselves.
 *
 * This object is network-agnostic on purpose: a Beeper notification is a Beeper
 * notification regardless of which network it came from, so every account shares
 * this one capture/cache/serve path rather than reimplementing it per adapter.
 *
 * THREE JOBS:
 *  - CAPTURE [onBeeperNotification]: pull the image out of Beeper's MessagingStyle
 *    notification and cache it, keyed by the *per-message* sender + timestamp
 *    (not the notification envelope, which updates as new messages arrive).
 *  - MATCH [mediaContentFor]: given a provider message (sender + timestamp), find
 *    the cached image that belongs to it and emit the `mediaContent` marker the
 *    watch uses to request it. The cache key travels as mediaID (opaque).
 *  - SERVE [scaleForWatch]: on REQ_IMAGE, load the cached PNG and run it through
 *    the Go scaler (borrowed off [Mirror.bridge] — the scale/pack is pure pixel
 *    math, no Google session) to produce the watch's {w,h,pixels,fmt,palette}.
 *
 * Images are caught LIVE: we only have the picture for a message that notified
 * while the listener was connected; history/backfill and muted-chat images are
 * unreachable by design.
 *
 * Cache lives at filesDir/[DIR]/<key>.png with a <key>.json metadata sidecar,
 * bounded to [MAX_CACHED] most-recent images (oldest evicted).
 */
object BeeperImages {
    private const val TAG = "MirrorMsgSpike"   // exportable-log tag; messages prefixed "Beeper-Img:"
    private const val DIR = "beeper_notif_images"
    private const val MAX_CACHED = 60

    // How close (ms) a cached image's timestamp must be to a message's timestamp
    // to be considered the same event. The notification's per-message timestamp
    // and the provider row's timestamp are the same clock, so this is generous
    // slack, not a real gap. Nearest-match wins among candidates.
    private const val MATCH_WINDOW_MS = 15_000L

    private fun dir(ctx: Context): File =
        File(ctx.filesDir, DIR).apply { if (!exists()) mkdirs() }

    // A captured image plus the identity we key/match it by.
    private data class Captured(val bitmap: Bitmap, val sender: String, val tsMs: Long)

    // ====================================================================
    // CAPTURE
    // ====================================================================

    /**
     * Entry point called by [BeeperNotificationListenerService] for every
     * notification posted by the Beeper app. Extracts an image if the
     * notification carries one and caches it; no-op otherwise. Safe to call on a
     * background thread (it does file I/O).
     */
    fun onBeeperNotification(ctx: Context, sbn: StatusBarNotification) {
        try {
            val n = sbn.notification ?: return
            val ex = n.extras ?: return
            val title = ex.getCharSequence(Notification.EXTRA_TITLE)?.toString()?.trim() ?: ""

            val cap = extractCaptured(ctx, n, ex, title, sbn.postTime)
            if (cap == null) {
                logNoImageDiag(n, ex, title)
                return
            }
            val key = makeKey(cap.sender, cap.tsMs)
            val saved = save(ctx, key, cap)
            if (saved != null) {
                LogStore.i(TAG, "Beeper-Img: cached ${cap.bitmap.width}x${cap.bitmap.height} " +
                    "sender=${cap.sender.take(20)} ts=${cap.tsMs} key=$key")
            }
            prune(ctx)
        } catch (e: Exception) {
            LogStore.e(TAG, "Beeper-Img: onBeeperNotification failed", e)
        }
    }

    // Pull a real bitmap + its message identity out of the notification. Prefer
    // the MessagingStyle per-message image (which carries the message's OWN
    // sender + timestamp — the robust match key) over BigPictureStyle (whose only
    // identity is the notification envelope). The image data URI is a content://
    // the OS grants this listener permission to open — unlike the unresolvable
    // mxc:// in the content provider. These are already-decoded images; we never
    // touch mxc:// here.
    @Suppress("DEPRECATION")
    private fun extractCaptured(
        ctx: Context, n: Notification, ex: Bundle, title: String, postTime: Long
    ): Captured? {
        // First: MessagingStyle per-message image, newest image message. Keyed by
        // that message's own sender + timestamp.
        try {
            val ms = NotificationCompat.MessagingStyle.extractMessagingStyleFromNotification(n)
            val imgMsg = ms?.messages?.lastOrNull {
                it.dataUri != null && (it.dataMimeType?.startsWith("image/") == true)
            }
            val uri = imgMsg?.dataUri
            if (uri != null) {
                ctx.contentResolver.openInputStream(uri)?.use { ins ->
                    val bmp = BitmapFactory.decodeStream(ins)
                    if (bmp != null) {
                        val sender = imgMsg.person?.name?.toString()?.trim()
                            ?.ifEmpty { title } ?: title
                        val ts = if (imgMsg.timestamp > 0L) imgMsg.timestamp else postTime
                        return Captured(bmp, sender, ts)
                    }
                }
            }
        } catch (e: Exception) {
            LogStore.w(TAG, "Beeper-Img: MessagingStyle image read failed: ${e.message}")
        }

        // Fallback: BigPictureStyle / expanded large icon, keyed by the envelope.
        val big = (ex.getParcelable(Notification.EXTRA_PICTURE) as? Bitmap)
            ?: (ex.getParcelable(Notification.EXTRA_LARGE_ICON_BIG) as? Bitmap)
        if (big != null) {
            val whenTs = if (n.`when` > 0L) n.`when` else postTime
            return Captured(big, title, whenTs)
        }
        return null
    }

    // When no image is found, log how the notification is actually structured so
    // we can see exactly where (and whether) Beeper attaches the image: the
    // template/style, the per-message mime types + whether they carry a uri.
    // Turns a bare "no image" into something actionable. TEMP-ish — can be
    // trimmed once capture is proven.
    @Suppress("DEPRECATION")
    private fun logNoImageDiag(n: Notification, ex: Bundle, title: String) {
        try {
            val template = ex.getString(Notification.EXTRA_TEMPLATE) ?: "?"
            val msgs = ex.getParcelableArray(Notification.EXTRA_MESSAGES)
            val mimes = StringBuilder()
            if (msgs != null) {
                for (m in msgs) {
                    val b = m as? Bundle ?: continue
                    val mime = b.getString("type")
                    val hasUri = b.get("uri") != null
                    mimes.append("[mime=$mime uri=$hasUri]")
                }
            }
            LogStore.i(TAG, "Beeper-Img: NO-IMG title=${title.take(20)} " +
                "template=${template.substringAfterLast('.')} " +
                "msgCount=${msgs?.size ?: 0} msgData=$mimes")
        } catch (e: Exception) {
            LogStore.w(TAG, "Beeper-Img: NO-IMG diag failed: ${e.message}")
        }
    }

    // Key = normalized sender + message timestamp. The timestamp makes it unique
    // per message; the sender scopes the match so two chats with near-simultaneous
    // images don't collide.
    private fun makeKey(sender: String, tsMs: Long): String {
        val safe = normalize(sender).take(24)
        return if (safe.isEmpty()) "img_$tsMs" else "${safe}_$tsMs"
    }

    private fun normalize(s: String): String =
        s.lowercase().replace(Regex("[^a-z0-9]"), "")

    // Write the image + sidecar. PREFER-LARGEST: Beeper often posts a notification
    // twice (a first low-res render, then the full image) under the same key —
    // last-write-wins would clobber the good one with the small one, so we only
    // overwrite when the new bitmap has at least as many pixels as what's cached.
    private fun save(ctx: Context, key: String, cap: Captured): File? {
        return try {
            val png = File(dir(ctx), "$key.png")
            val sidecar = File(dir(ctx), "$key.json")
            val newPixels = cap.bitmap.width * cap.bitmap.height
            if (png.exists() && sidecar.exists()) {
                val existing = runCatching {
                    val j = JSONObject(sidecar.readText()); j.optInt("w") * j.optInt("h")
                }.getOrDefault(0)
                if (existing >= newPixels) {
                    return png  // keep the larger/equal cached version
                }
            }
            FileOutputStream(png).use { cap.bitmap.compress(Bitmap.CompressFormat.PNG, 100, it) }
            val meta = JSONObject()
                .put("sender", cap.sender)
                .put("ts", cap.tsMs)
                .put("w", cap.bitmap.width)
                .put("h", cap.bitmap.height)
            sidecar.writeText(meta.toString())
            png
        } catch (e: Exception) {
            LogStore.e(TAG, "Beeper-Img: save failed", e)
            null
        }
    }

    // Bound the cache: keep the MAX_CACHED most-recently-written images, drop the
    // rest (png + sidecar together). The image lifecycle shadows the message
    // lifecycle — old images age out the same way old messages do.
    private fun prune(ctx: Context) {
        try {
            val pngs = dir(ctx).listFiles { f -> f.name.endsWith(".png") }?.toMutableList()
                ?: return
            if (pngs.size <= MAX_CACHED) return
            pngs.sortBy { it.lastModified() }
            val toRemove = pngs.size - MAX_CACHED
            for (i in 0 until toRemove) {
                val png = pngs[i]
                val base = png.name.removeSuffix(".png")
                png.delete()
                File(dir(ctx), "$base.json").delete()
            }
        } catch (_: Exception) {}
    }

    // ====================================================================
    // MATCH
    // ====================================================================

    /**
     * Find the cached image belonging to a provider message and return the
     * `mediaContent` marker to attach to that message's JSON, or null if none.
     * Match = same (normalized) sender AND timestamp within [MATCH_WINDOW_MS];
     * nearest timestamp wins among candidates.
     *
     * The returned object mirrors the native Google MMS shape closely enough for
     * the watch: the cache key rides as `mediaID` (opaque — it comes back to us
     * unchanged in [scaleForWatch] via downloadImage), and dimensions are the
     * real captured size.
     */
    fun mediaContentFor(ctx: Context, sender: String, tsMillis: Long): JSONObject? {
        val wantSender = normalize(sender)
        if (wantSender.isEmpty()) return null  // our own sent msgs aren't captured
        return try {
            val sidecars = dir(ctx).listFiles { f -> f.name.endsWith(".json") } ?: return null
            var bestKey: String? = null
            var bestW = 0
            var bestH = 0
            var bestDelta = Long.MAX_VALUE
            for (sc in sidecars) {
                val j = runCatching { JSONObject(sc.readText()) }.getOrNull() ?: continue
                val sSender = normalize(j.optString("sender"))
                if (sSender.isEmpty()) continue
                val nameMatch = sSender == wantSender ||
                    sSender.contains(wantSender) || wantSender.contains(sSender)
                if (!nameMatch) continue
                val sTs = j.optLong("ts")
                val delta = kotlin.math.abs(sTs - tsMillis)
                if (delta <= MATCH_WINDOW_MS && delta < bestDelta) {
                    bestDelta = delta
                    bestKey = sc.name.removeSuffix(".json")
                    bestW = j.optInt("w")
                    bestH = j.optInt("h")
                }
            }
            val key = bestKey ?: return null
            JSONObject()
                .put("format", "IMAGE_PNG")
                .put("mediaID", key)
                .put("dimensions", JSONObject()
                    .put("width", bestW.toString())
                    .put("height", bestH.toString()))
        } catch (e: Exception) {
            LogStore.w(TAG, "Beeper-Img: mediaContentFor failed: ${e.message}")
            null
        }
    }

    // ====================================================================
    // SERVE
    // ====================================================================

    /**
     * Load the cached PNG for [key] and scale/pack it into the watch's bitmap
     * format, returning the {w,h,pixels,fmt,palette} JSON the watch consumes.
     *
     * The scale + 4bpp-palette pack is done by the Go mirrorbridge's
     * ProcessInlineImageForWatch, which is pure pixel math (no session/client
     * touched), so we borrow the always-present Google bridge instance
     * [Mirror.bridge] purely as a stateless scaler. [key] arrives here as the
     * mediaID we stashed in [mediaContentFor]; maxW/maxH/maxBytes/quality come
     * straight from the watch's REQ_IMAGE and are honored by the scaler.
     */
    fun scaleForWatch(
        ctx: Context, key: String, maxW: Int, maxH: Int, maxBytes: Int, quality: Int
    ): String {
        val png = File(dir(ctx), "$key.png")
        if (!png.exists()) return errStr("no cached image for key")
        val bridge = Mirror.bridge ?: return errStr("scaler unavailable")
        return try {
            val b64 = Base64.encodeToString(png.readBytes(), Base64.NO_WRAP)
            // gomobile lowercases the first letter of the exported Go method
            // Bridge.ProcessInlineImageForWatch -> processInlineImageForWatch,
            // and Go int params bind as Java/Kotlin Long (matches how
            // GoogleMessagesService calls the identical method).
            bridge.processInlineImageForWatch(
                b64, maxW.toLong(), maxH.toLong(), maxBytes.toLong(), quality.toLong()
            )
        } catch (e: Exception) {
            LogStore.e(TAG, "Beeper-Img: scaleForWatch failed for $key", e)
            errStr("scale failed: ${e.message}")
        }
    }

    private fun errStr(msg: String) = "{\"error\":\"$msg\"}"

    // Number of images currently cached (for diagnostics / settings display).
    fun cachedCount(ctx: Context): Int =
        try { dir(ctx).listFiles { f -> f.name.endsWith(".png") }?.size ?: 0 }
        catch (e: Exception) { 0 }
}
