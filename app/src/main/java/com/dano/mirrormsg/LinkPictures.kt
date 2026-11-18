package com.dano.mirrormsg

import android.content.Context
import android.graphics.Bitmap
import android.graphics.BitmapFactory
import android.media.MediaMetadataRetriever
import java.io.File

/**
 * The picture behind a link in a message, for its Pebble notification (step 109):
 * a post's photo or a video's poster from X, Reddit, Imgur, Instagram, Facebook and
 * YouTube (LinkMedia), a picture file linked directly, a GIF picker's link
 * (MediaUrls). The same lookups the watch's chat uses, so one answer serves both.
 */
object LinkPictures {
    private const val TAG = "MirrorMsgSpike"
    private const val MAX_SIDE = 480          // longest side handed to the notification

    /** Does [text] hold a link this could find a picture behind? No network. */
    fun has(text: String): Boolean =
        text.isNotEmpty() && (LinkMedia.find(text) != null || MediaUrls.gifLink(text) != null)

    /** The picture behind [text]'s first such link, or null. Blocking; network. */
    suspend fun forText(ctx: Context, text: String, limitMs: Long): Bitmap? {
        val link = LinkMedia.find(text)
        if (link != null) {
            val m = link.direct ?: LinkMedia.await(ctx, link.url, limitMs)
            if (m == null) { LogStore.i(TAG, "Link picture: no answer in time"); return null }
            return when (m.kind) {
                LinkMedia.Kind.NONE -> null
                LinkMedia.Kind.VIDEO ->
                    if (m.poster.isNotEmpty()) picture(ctx, m.poster) else videoFrame(m.url)
                else ->
                    picture(ctx, m.poster.ifEmpty { m.url }) ?: if (m.poster.isNotEmpty()) picture(ctx, m.url) else null
            }
        }
        val gif = MediaUrls.gifLink(text) ?: return null
        return picture(ctx, gif)
    }

    /** [url] downloaded and decoded: a picture, or a clip's first frame. */
    private fun picture(ctx: Context, url: String): Bitmap? {
        if (url.isEmpty()) return null
        val f = File(ctx.cacheDir, "notifpic_${System.nanoTime()}.tmp")
        return try {
            if (MediaUrls.downloadToFile(url, f.path) <= 0L) return null
            decodeFile(f) ?: fileFrame(f)
        } catch (e: Throwable) {
            LogStore.w(TAG, "Link picture failed: ${e.javaClass.simpleName}")
            null
        } finally {
            f.delete()
        }
    }

    private fun decodeFile(f: File): Bitmap? {
        val o = BitmapFactory.Options().apply { inJustDecodeBounds = true }
        BitmapFactory.decodeFile(f.path, o)
        if (o.outWidth <= 0 || o.outHeight <= 0) return null
        var sample = 1
        while (maxOf(o.outWidth, o.outHeight) / (sample * 2) >= MAX_SIDE) sample *= 2
        val b = BitmapFactory.decodeFile(f.path, BitmapFactory.Options().apply { inSampleSize = sample })
            ?: return null
        return fit(b)
    }

    private fun fileFrame(f: File): Bitmap? {
        val r = MediaMetadataRetriever()
        return try {
            r.setDataSource(f.path)
            r.getFrameAtTime(0)?.let { fit(it) }
        } catch (e: Throwable) {
            null
        } finally {
            try { r.release() } catch (_: Throwable) {}
        }
    }

    /** A video with no poster: its first frame, read from the network without downloading it all. */
    private fun videoFrame(url: String): Bitmap? {
        if (url.isEmpty()) return null
        val r = MediaMetadataRetriever()
        return try {
            r.setDataSource(LinkMedia.current(url).substringBefore(LinkMedia.DASH_AUDIO), HashMap<String, String>())
            r.getFrameAtTime(0)?.let { fit(it) }
        } catch (e: Throwable) {
            LogStore.w(TAG, "Link video frame failed: ${e.javaClass.simpleName}")
            null
        } finally {
            try { r.release() } catch (_: Throwable) {}
        }
    }

    private fun fit(b: Bitmap): Bitmap {
        val big = maxOf(b.width, b.height)
        if (big <= MAX_SIDE) return b
        val k = MAX_SIDE.toFloat() / big
        val out = Bitmap.createScaledBitmap(b, maxOf(1, (b.width * k).toInt()), maxOf(1, (b.height * k).toInt()), true)
        if (out !== b) b.recycle()
        return out
    }
}
