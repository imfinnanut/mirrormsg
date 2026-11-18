package com.dano.mirrormsg

import android.content.Context
import android.graphics.Bitmap
import android.media.MediaMetadataRetriever
import fi.mirrormsg.mediaroute.Mediaroute
import java.io.ByteArrayOutputStream
import java.io.File

/** Short mp4 clips (a GIF picker's, a network's GIF): fetched once, shown for the watch. */
object Clips {
    private const val TAG = "Clips"
    internal const val CLIP_KEEP = 8

    /** True when these bytes are an ISO media file (mp4/m4v/mov). */
    fun looksLikeMp4(b: ByteArray): Boolean =
        b.size > 12 && b[4] == 'f'.code.toByte() && b[5] == 't'.code.toByte() &&
        b[6] == 'y'.code.toByte() && b[7] == 'p'.code.toByte()

    /**
     * The clip encoded for the watch: the same {"kind","data"} envelope the bridges
     * return, or "" when it can't be decoded. [maxEdge] bounds the decoded frames'
     * longer side (0 = the detail view's).
     */
    fun encodeForWatch(
        ctx: Context?, key: String, bytes: ByteArray,
        maxW: Int, maxH: Int, maxBytes: Int, quality: Int,
        fpsFloor: Int, gifBytes: Int, startRung: Int, maxEdge: Int = 0
    ): String =
        Media.engine.encodeClip(ctx, key, bytes, maxW, maxH, maxBytes, quality, fpsFloor, gifBytes, startRung, maxEdge)
            ?: stillForWatch(ctx, bytes, maxW, maxH, maxBytes, quality)

    // OPTION_CLOSEST, not OPTION_CLOSEST_SYNC: sync snaps to the nearest KEYFRAME,
    // and a short picker clip often has exactly one — so every sample came back as
    // frame zero and the "animation" was the same picture N times over.
    /** A still of the clip's first frame, as a {"kind":"image",...} envelope. */
    fun stillForWatch(
        ctx: Context?, bytes: ByteArray, maxW: Int, maxH: Int, maxBytes: Int, quality: Int
    ): String {
        var tmp: File? = null
        val mmr = MediaMetadataRetriever()
        return try {
            tmp = File.createTempFile("clip", ".mp4", ctx?.cacheDir)
            tmp.writeBytes(bytes)
            mmr.setDataSource(tmp.absolutePath)
            still(mmr, maxW, maxH, maxBytes, quality)
        } catch (e: Throwable) {
            LogStore.w(TAG, "still failed: ${e.message}")
            ""
        } finally {
            runCatching { mmr.release() }
            runCatching { tmp?.delete() }
        }
    }

    /**
     * The bytes of a network's own clip (a WhatsApp GIF, a Telegram animation),
     * fetched once through that network's download and kept on disk: one clip is
     * asked for several times over — the row preview, the full picture, and again
     * whenever the watch turns the first attempt down.
     */
    fun clipBytes(ctx: Context?, id: String, download: (File) -> Boolean): ByteArray? {
        val base = ctx?.cacheDir ?: return null
        val dir = File(base, "clips")
        if (!dir.exists()) dir.mkdirs()
        val name = sha1(id) + ".mp4"
        val f = File(dir, name)
        if (f.isFile && f.length() > 0) {
            f.setLastModified(System.currentTimeMillis())
            return runCatching { f.readBytes() }.getOrNull()
        }
        val part = File(dir, "$name.part")
        val ok = try {
            download(part)
        } catch (e: Throwable) {
            LogStore.w(TAG, "clip download failed: ${e.message}")
            false
        }
        if (!ok || !part.isFile || part.length() == 0L) {
            runCatching { part.delete() }
            return null
        }
        part.renameTo(f)
        dir.listFiles { x -> x.name.endsWith(".mp4") }
            ?.sortedByDescending { it.lastModified() }
            ?.drop(CLIP_KEEP)
            ?.forEach { runCatching { it.delete() } }
        LogStore.i(TAG, "clip fetched: ${f.length() / 1024} KB")
        return runCatching { f.readBytes() }.getOrNull()
    }

    internal fun sha1(s: String): String =
        java.security.MessageDigest.getInstance("SHA-1").digest(s.toByteArray())
            .joinToString("") { "%02x".format(it) }

    /** First frame as a still. */
    internal fun still(
        mmr: MediaMetadataRetriever, maxW: Int, maxH: Int, maxBytes: Int, quality: Int
    ): String = try {
        val bm = mmr.getFrameAtTime(0L, MediaMetadataRetriever.OPTION_CLOSEST_SYNC)
        if (bm == null) "" else {
            val jpg = ByteArrayOutputStream()
            bm.compress(Bitmap.CompressFormat.JPEG, 85, jpg)
            bm.recycle()
            Mediaroute.encode(
                jpg.toByteArray(), maxW.toLong(), maxH.toLong(), maxBytes.toLong(),
                quality.toLong(), 0L, 0L, 0L, 0L
            )
        }
    } catch (e: Throwable) {
        LogStore.w(TAG, "still fallback failed: ${e.message}")
        ""
    }
}
