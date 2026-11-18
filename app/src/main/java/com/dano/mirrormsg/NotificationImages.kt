package com.dano.mirrormsg

import android.graphics.Bitmap
import android.graphics.BitmapFactory
import android.util.Base64
import org.json.JSONObject

/**
 * Produces an Android [Bitmap] for a message's image so it can ride along in the
 * notification (BigPictureStyle), which is how PebbleOS renders pictures in its
 * own notifications.
 *
 * WHY THIS EXISTS: PebbleOS now shows images attached to Android notifications.
 * Without this, MirrorMsg's notifications are strictly WORSE than the ones the
 * Pebble app produces on its own — the user would see the picture if they let
 * Pebble handle the notification directly, and only "[Image]" if they let us.
 * That's a reason to turn MirrorMsg off, so the picture has to be there.
 *
 * TWO SOURCES, deliberately different in quality:
 *  - INLINE (MMS, and RCS's INCOMING_COMPLETE stage): the JPEG/PNG bytes are
 *    already in the message JSON as base64. Decoded directly — full colour, no
 *    network, near-instant. This is the good path.
 *  - DOWNLOADED (RCS media behind a mediaID): the only bridge call that fetches
 *    it is the watch encoder, which returns Pebble GColor8 pixels rather than an
 *    ordinary image. We reconstruct a Bitmap from those. It costs a round-trip
 *    and comes back 64-colour, so it looks posterized in the PHONE shade — but on
 *    the watch it is exactly right, because the Pebble screen is that colour space
 *    anyway. (A bridge function returning real JPEG bytes would fix the phone-side
 *    fidelity; that needs a Go change, deliberately not done here.)
 *
 * Requesting 8bpp (quality != 1) rather than the palettized 4bpp form matters:
 * PebbleOS re-quantizes whatever we hand it with its own median-cut + dithering,
 * and feeding an already-dithered 16-colour image into a second ditherer
 * compounds the noise. A clean 64-colour image gives its encoder good input.
 */
object NotificationImages {

    private const val TAG = "NotifImg"

    /** Longest edge we ask for. Matches PebbleOS's own imaging limit (300px), so
     *  we never spend time on pixels the watch would throw away. Also keeps the
     *  bitmap far below the ~1MB notification IPC ceiling. */
    private const val MAX_DIM = 300

    /** Below this on the long edge an image is a preview thumbnail, not the
     *  picture — Android would stretch it across the notification and it would
     *  read as a blur. Worth a network round-trip to get the real thing. */
    private const val MIN_USABLE_DIM = 160

    /** A message's image, in whichever form the source gave us. */
    data class Ref(val mediaId: String, val keyB64: String, val inlineB64: String) {
        val usable: Boolean get() = mediaId.isNotEmpty() || inlineB64.isNotEmpty()
    }

    /**
     * Pull the first still image's media reference out of a message JSON. Mirrors
     * LiveRouter.messageHasImage's rules exactly — same traversal, same video
     * exclusion — so "we said it has an image" and "we can fetch that image"
     * never disagree.
     */
    fun refFor(m: JSONObject): Ref? {
        val infos = m.optJSONArray("messageInfo") ?: return null
        var mediaId = ""
        var keyB64 = ""
        var inlineB64 = ""
        for (j in 0 until infos.length()) {
            val media = infos.optJSONObject(j)?.optJSONObject("mediaContent") ?: continue
            // Video is not a still image and can't be decoded — skip, matching
            // LiveRouter.messageHasImage and the thread parser.
            val fmt = media.optString("format")
            val mime = media.optString("mimeType")
            if (fmt.startsWith("VIDEO") || mime.startsWith("video/")) continue
            if (mediaId.isEmpty()) {
                mediaId = media.optString("mediaID")
                keyB64 = media.optString("decryptionKey")
            }
            // CRITICAL, and the reason notification images were stuck on a 63x47
            // blur: many RCS image messages carry NO plain mediaID and expose the
            // downloadable reference only under thumbnailMediaID — which despite
            // the name points at the FULL-RES media. Reading just "mediaID" found
            // nothing and fell through to the tiny inline preview. This mirrors
            // the thread parser's extraction exactly; keep the two in step.
            if (mediaId.isEmpty()) {
                mediaId = media.optString("thumbnailMediaID")
                keyB64 = media.optString("thumbnailDecryptionKey")
            }
            if (inlineB64.isEmpty()) inlineB64 = media.optString("mediaData")
            // DIAGNOSTIC: when no downloadable reference is found, report exactly
            // which fields mediaContent DOES carry at notification time. The live
            // event arrives earlier in an RCS message's life than the thread parse
            // does, so the media reference may simply not exist yet — and guessing
            // at field names has already cost two round trips.
            if (mediaId.isEmpty()) {
                val keys = mutableListOf<String>()
                val it = media.keys()
                while (it.hasNext()) keys.add(it.next())
                // "dimensions" is the source photo's real size. If it's much larger
                // than the inline preview, this stage carries a placeholder and the
                // full media reference is still coming — worth waiting for.
                LogStore.i(TAG, "mediaContent has no id; fields=${keys.joinToString(",")} dimensions=${media.opt("dimensions")} size=${media.opt("size")} name=${media.optString("mediaName")}")
            }
        }
        val ref = Ref(mediaId, keyB64, inlineB64)
        return if (ref.usable) ref else null
    }

    /**
     * Fetch and decode [ref] into a Bitmap, or null if it can't be produced. Does
     * network and image work — call it off the main thread. Never throws: a
     * notification without its picture is a far better outcome than no
     * notification, so every failure path returns null and lets the caller post
     * text-only.
     */
    fun bitmapFor(ref: Ref, ownerServiceId: String, ctx: android.content.Context): Bitmap? {
        try {
            // Inline bytes are a FALLBACK, not the preference. Google Messages'
            // mediaData often carries a tiny preview rather than the picture —
            // measured at 63x47 on a real RCS photo — and Android stretches that
            // across the whole notification as a grey smear. So decode it, but
            // only accept it if it's actually big enough to look like something;
            // otherwise prefer the (slower, network) download of the real media.
            var inline: Bitmap? = null
            if (ref.inlineB64.isNotEmpty()) {
                inline = decodeInline(ref.inlineB64)
                if (inline != null && maxOf(inline.width, inline.height) >= MIN_USABLE_DIM) {
                    LogStore.i(TAG, "Notification image from inline data (${inline.width}x${inline.height})")
                    return inline
                }
                if (inline != null) {
                    LogStore.i(TAG, "Inline image is only ${inline.width}x${inline.height} — fetching full media instead")
                }
            }
            // No media to download: a small inline preview beats nothing at all.
            if (ref.mediaId.isEmpty()) {
                LogStore.i(TAG, "No downloadable media ref at notify time — keeping the ${inline?.width}x${inline?.height} preview")
                return inline
            }
            val svc = ServiceRegistry.byId(ctx, ownerServiceId)
            if (svc == null) {
                LogStore.w(TAG, "No service '$ownerServiceId' to download media — using inline")
                return inline
            }
            // quality != 1 -> the encoder skips palettizing and returns 8bpp
            // GColor8 (see above). maxBytes = 0 selects plain bounding-box fit.
            val t0 = System.currentTimeMillis()
            LogStore.i(TAG, "Downloading full media for notification (id=${ref.mediaId.take(24)}, key=${ref.keyB64.isNotEmpty()})")
            val json = svc.downloadImage(ref.mediaId, ref.keyB64, MAX_DIM, MAX_DIM, 0, 0)
            LogStore.i(TAG, "Media download returned ${json.length} chars in ${System.currentTimeMillis() - t0}ms")
            if (json.isEmpty() || !json.trimStart().startsWith("{")) {
                LogStore.w(TAG, "Notification image fetch returned non-JSON (${json.length} chars)")
                return inline
            }
            val o = JSONObject(json)
            if (!o.has("w")) {
                LogStore.w(TAG, "Notification image fetch failed: ${o.optString("error", "no w")}")
                return inline
            }
            val bmp = fromGColor8(o)
            if (bmp != null) {
                LogStore.i(TAG, "Notification image from bridge (${bmp.width}x${bmp.height})")
                return bmp
            }
            return inline
        } catch (e: Throwable) {
            LogStore.w(TAG, "Notification image unavailable: ${e.message}")
            return null
        }
    }

    /** Decode base64 JPEG/PNG bytes, downscaled with inSampleSize so a large
     *  camera photo never allocates full size just to be shrunk. */
    private fun decodeInline(b64: String): Bitmap? {
        return try {
            val bytes = Base64.decode(b64, Base64.DEFAULT)
            val bounds = BitmapFactory.Options().apply { inJustDecodeBounds = true }
            BitmapFactory.decodeByteArray(bytes, 0, bytes.size, bounds)
            var sample = 1
            var longest = maxOf(bounds.outWidth, bounds.outHeight)
            while (longest / sample > MAX_DIM * 2) sample *= 2
            val opts = BitmapFactory.Options().apply { inSampleSize = sample }
            BitmapFactory.decodeByteArray(bytes, 0, bytes.size, opts)
        } catch (e: Throwable) {
            LogStore.w(TAG, "Inline image decode failed: ${e.message}")
            null
        }
    }

    /**
     * Rebuild a Bitmap from the encoder's 8bpp result: one GColor8 byte per pixel,
     * tightly packed (the 8bpp path emits no row padding), 2 bits per channel
     * expanded back to 0/85/170/255.
     */
    private fun fromGColor8(o: JSONObject): Bitmap? {
        val w = o.optInt("w"); val h = o.optInt("h")
        if (w <= 0 || h <= 0) return null
        if (o.optInt("fmt", 0) != 0) {
            // Palettized came back unexpectedly; rebuilding it would need the
            // palette + bit unpacking. Not worth a second code path — skip.
            LogStore.w(TAG, "Notification image came back palettized (fmt=1) — skipping")
            return null
        }
        val px = Base64.decode(o.optString("pixels"), Base64.DEFAULT)
        if (px.size < w * h) {
            LogStore.w(TAG, "Notification image short: ${px.size} bytes for ${w}x$h")
            return null
        }
        val argb = IntArray(w * h)
        for (i in 0 until w * h) {
            val v = px[i].toInt()
            val r = ((v shr 4) and 3) * 85
            val g = ((v shr 2) and 3) * 85
            val b = (v and 3) * 85
            argb[i] = (0xFF shl 24) or (r shl 16) or (g shl 8) or b
        }
        return Bitmap.createBitmap(argb, w, h, Bitmap.Config.ARGB_8888)
    }
}
