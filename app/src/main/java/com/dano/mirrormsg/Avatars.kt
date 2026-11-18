package com.dano.mirrormsg

import android.content.Context
import android.graphics.Bitmap
import android.graphics.BitmapFactory
import android.graphics.Canvas
import android.graphics.Color
import android.graphics.Paint
import android.graphics.PorterDuff
import android.graphics.PorterDuffXfermode
import android.graphics.Rect
import android.graphics.RectF
import android.graphics.Typeface
import android.net.Uri
import android.util.Base64
import android.util.LruCache
import org.json.JSONObject
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.IOException
import java.net.HttpURLConnection
import java.net.URL
import java.security.MessageDigest
import java.util.concurrent.ConcurrentHashMap

/**
 * Contact photos (step 90): the picture each chat shows in the watch's chat list (cut
 * to the avatar's circle, inside a ring of the chat's colour) and on its notification
 * (the big round picture, with MirrorMsg's icon as the badge).
 *
 * A network names a chat's photo ([MessagingService.avatarId]) and fetches it
 * ([MessagingService.avatarBytes]); the rest is here:
 *  - One square copy per photo on the phone ([SRC_PX] px PNG in filesDir/avatars):
 *    fresh for 3 days (a stale one is still shown while a new one is fetched), "no
 *    photo" remembered for a day, a failed fetch tried again after 2 minutes.
 *  - The watch's copy for a list size ([watchRecord]): dd x dd GColor8 pixels through
 *    the watch's own still encoder (what Dan approved in the mock), the corners
 *    outside the circle clear.
 *  - The notification's round icon, or with no photo the chat's letter on its colour:
 *    the watch's letter avatar, phone-sized ([roundIcon], [letterIcon]).
 *  - The picture of a picture message for the notification's conversation view, as
 *    a content URI through the app's FileProvider ([pictureUri]).
 *
 * Chats are learnt from the list rows packed for the watch ([rowKey]): a row's key
 * (the record's sixth field, FNV-1a of the chat and its photo) is what the watch asks
 * for, so a changed photo is a new key and the watch asks again.
 */
object Avatars {
    private const val TAG = "MirrorMsgSpike"
    const val SRC_PX = 192
    const val DEFAULT_COLOR = "#7E57C2"
    private const val FRESH_MS = 3 * 24 * 3600_000L
    private const val NONE_MS = 24 * 3600_000L
    private const val RETRY_MS = 2 * 60_000L
    private const val MAX_BYTES = 4 shl 20
    private const val MAX_FILES = 400

    /** A chat as the watch's list last showed it. */
    class Chat(val serviceId: String, val convoId: String, val avatarId: String?,
               val name: String, val color: String, val group: Boolean)

    private val byKey = ConcurrentHashMap<Long, Chat>()
    private val byConvo = ConcurrentHashMap<String, Chat>()
    // A chat's notification photo: its id, or "" = none (asked of the network).
    private val notifIds = ConcurrentHashMap<String, String>()
    private val mem = LruCache<String, Bitmap>(24)            // photo id -> square copy
    private val watchMem = LruCache<String, ByteArray>(64)    // "id@dd" -> watch pixels
    private val failedAt = ConcurrentHashMap<String, Long>()
    private val locks = ConcurrentHashMap<String, Any>()

    // ---- Chats ------------------------------------------------------------------

    /**
     * A row of the chat list being packed for the watch: remember the chat and name
     * its photo. Returns the key for the record's sixth field, 8 hex digits, or ""
     * when the chat has no photo (the watch keeps the letter and asks for nothing).
     */
    fun rowKey(svc: MessagingService?, row: JSONObject, color: String): String {
        val convoId = row.optString("conversationID")
        if (svc == null || convoId.isEmpty()) return ""
        val id = try { svc.avatarId(convoId, row) } catch (e: Throwable) { null }?.takeIf { it.isNotEmpty() }
        val chat = Chat(svc.id, convoId, id, row.optString("name"),
                        color.ifEmpty { DEFAULT_COLOR }, isGroup(row))
        byConvo[convoId] = chat
        if (id == null) return ""
        notifIds[convoId] = id
        var key = fnv1a("$convoId\n$id")
        if (key == 0L) key = 1L
        byKey[key] = chat
        return String.format("%08x", key)
    }

    private fun isGroup(row: JSONObject): Boolean {
        if (row.optBoolean("isGroupChat")) return true
        val parts = row.optJSONArray("participants") ?: return false
        var others = 0
        for (i in 0 until parts.length())
            if (parts.optJSONObject(i)?.optBoolean("isMe", false) == false) others++
        return others > 1
    }

    fun chat(convoId: String): Chat? = byConvo[convoId]

    // Keys the watch asked for before a list packed their rows here (the phone had just
    // started), with the size asked for: answered once a list does ([takeWaiting]).
    private val waiting = ConcurrentHashMap<Long, Int>()

    /** Whether the phone knows [key]: a list packed its row since the phone started. */
    fun knows(key: Long): Boolean = byKey.containsKey(key)

    /** Hold [key], asked for at [dd] px, until a list packs its row. */
    fun waitFor(key: Long, dd: Int) {
        if (waiting.size >= 64) waiting.clear()   // keys no list will pack (the watch asks again)
        waiting[key] = dd
    }

    /** The held keys a list has packed since, by size; they're no longer held. */
    fun takeWaiting(): Map<Int, List<Long>> {
        if (waiting.isEmpty()) return emptyMap()
        val out = HashMap<Int, MutableList<Long>>()
        for (k in waiting.keys.toList()) {
            if (!byKey.containsKey(k)) continue
            val dd = waiting.remove(k) ?: continue
            out.getOrPut(dd) { ArrayList() }.add(k)
        }
        return out
    }

    /** The photo id of a chat's notifications if known (null: none, or not asked yet). */
    fun notifId(convoId: String): String? = notifIds[convoId]?.ifEmpty { null }

    /** Whether the network was asked (or the list told) about this chat's notification photo. */
    fun notifIdKnown(convoId: String): Boolean = notifIds.containsKey(convoId)

    /** Ask the network for a chat's notification photo (blocking: off the main thread). */
    fun lookupNotifId(svc: MessagingService, convoId: String): String? {
        val id = try { svc.avatarId(convoId, null) } catch (e: Throwable) { return null }?.takeIf { it.isNotEmpty() }
        notifIds[convoId] = id ?: ""
        return id
    }

    // ---- Photos ---------------------------------------------------------------------

    /** Photo [id]'s square copy if it's on the phone (memory or disk, fresh or not). No network. */
    fun cached(ctx: Context, id: String): Bitmap? {
        mem.get(id)?.let { return it }
        val f = pngFile(ctx, id)
        if (!f.exists()) return null
        val b = try { BitmapFactory.decodeFile(f.path) } catch (e: Throwable) { null } ?: return null
        mem.put(id, b)
        return b
    }

    /**
     * Photo [id]'s square copy, fetched through [svc] when it's missing or stale
     * (blocking: off the main thread; one fetch per photo at a time). Null when the
     * chat has no photo, or none could be had and none was kept.
     */
    fun load(ctx: Context, svc: MessagingService, id: String): Bitmap? {
        val now = System.currentTimeMillis()
        if (fresh(ctx, id, now)) return cached(ctx, id)
        if (noneFresh(ctx, id, now)) return null
        if (now - (failedAt[id] ?: 0L) < RETRY_MS) return cached(ctx, id)
        synchronized(locks.getOrPut(id) { Any() }) {
            val t = System.currentTimeMillis()
            if (fresh(ctx, id, t)) return cached(ctx, id)
            if (noneFresh(ctx, id, t)) return null
            val bytes = try {
                svc.avatarBytes(id)
            } catch (e: Throwable) {
                failedAt[id] = t
                LogStore.w(TAG, "Contact photo: ${svc.id} couldn't fetch one (${e.message})")
                return cached(ctx, id)
            }
            if (bytes == null || bytes.isEmpty()) {
                markNone(ctx, id)
                return null
            }
            val sq = square(bytes)
            if (sq == null) {
                LogStore.w(TAG, "Contact photo: ${svc.id} sent ${bytes.size} bytes that aren't a picture")
                markNone(ctx, id)
                return null
            }
            mem.put(id, sq)
            save(ctx, id, sq)
            watchMem.evictAll()   // a photo replaced: its watch copies go (cheap to make again)
            LogStore.i(TAG, "Contact photo: ${svc.id}, ${bytes.size / 1024} KB, kept")
            return sq
        }
    }

    // A picture (any size, any format Android reads) cut to its centre square at
    // SRC_PX, transparency flattened onto white (a server icon's clear corners).
    private fun square(bytes: ByteArray): Bitmap? {
        val o = BitmapFactory.Options().apply { inJustDecodeBounds = true }
        BitmapFactory.decodeByteArray(bytes, 0, bytes.size, o)
        if (o.outWidth <= 0 || o.outHeight <= 0) return null
        var sample = 1
        while (minOf(o.outWidth, o.outHeight) / (sample * 2) >= SRC_PX) sample *= 2
        val src = BitmapFactory.decodeByteArray(bytes, 0, bytes.size,
            BitmapFactory.Options().apply { inSampleSize = sample }) ?: return null
        val side = minOf(src.width, src.height)
        val out = Bitmap.createBitmap(SRC_PX, SRC_PX, Bitmap.Config.ARGB_8888)
        val c = Canvas(out)
        c.drawColor(Color.WHITE)
        val l = (src.width - side) / 2
        val t = (src.height - side) / 2
        c.drawBitmap(src, Rect(l, t, l + side, t + side), Rect(0, 0, SRC_PX, SRC_PX),
                     Paint(Paint.FILTER_BITMAP_FLAG))
        return out
    }

    // ---- The watch -------------------------------------------------------------------

    /**
     * The answer to the watch's request for [key] at diameter [dd] (KEY_AV_DATA record):
     * [u32 key][u8 dd][u8 kind] + for kind 8 (a photo) dd*dd GColor8 pixels, the corners
     * outside the circle clear (0x00); kind 0 = no photo (the network said so). [fetch]
     * false answers only from what's on the phone, a stale copy included; true fetches
     * what's missing or stale ([needsFetch]). Null when there's no answer to give yet:
     * nothing on the phone, a fetch that failed (the watch asks again), or a key the
     * phone doesn't know (no list has packed its row since it started: [waitFor] it).
     */
    fun watchRecord(ctx: Context, key: Long, dd: Int, fetch: Boolean): ByteArray? {
        val chat = byKey[key] ?: return null
        val id = chat.avatarId ?: return record(key, dd, null)
        if (!fetch) watchMem.get("$id@$dd")?.let { return record(key, dd, it) }
        if (noneFresh(ctx, id, System.currentTimeMillis())) return record(key, dd, null)
        val sq = (if (!fetch) cached(ctx, id)
                  else ServiceRegistry.byId(ctx, chat.serviceId)?.let { load(ctx, it, id) })
            ?: return if (noneFresh(ctx, id, System.currentTimeMillis())) record(key, dd, null) else null
        val px = watchPixels(sq, dd) ?: return record(key, dd, null)
        watchMem.put("$id@$dd", px)
        return record(key, dd, px)
    }

    /** Whether [key]'s photo should be fetched: missing or stale, not "none", not just failed. */
    fun needsFetch(ctx: Context, key: Long): Boolean {
        val id = byKey[key]?.avatarId ?: return false
        val now = System.currentTimeMillis()
        return !fresh(ctx, id, now) && !noneFresh(ctx, id, now) && now - (failedAt[id] ?: 0L) >= RETRY_MS
    }

    private fun record(key: Long, dd: Int, px: ByteArray?): ByteArray {
        val r = ByteArray(6 + (px?.size ?: 0))
        for (i in 0 until 4) r[i] = (key ushr (8 * i)).toByte()
        r[4] = dd.toByte()
        r[5] = if (px != null) 8 else 0
        px?.copyInto(r, 6)
        return r
    }

    // dd x dd for the watch through its still encoder at full colour (bilinear scale,
    // GColor8 pixels: the look of the mock Dan picked), the corners clear. A copy made
    // here if the encoder can't (the same quantizing, Android's scaling).
    fun watchPixels(sq: Bitmap, dd: Int): ByteArray? {
        if (dd < 8 || dd > 64) return null
        var px: ByteArray? = null
        try {
            val png = ByteArrayOutputStream().also { sq.compress(Bitmap.CompressFormat.PNG, 100, it) }.toByteArray()
            val js = fi.mirrormsg.mirrorbridge.Mirrorbridge.encodeImageForWatch(
                Base64.encodeToString(png, Base64.NO_WRAP), dd.toLong(), dd.toLong(), 0L, 0L)
            val o = JSONObject(js)
            if (o.optInt("w") == dd && o.optInt("h") == dd && o.optInt("depth") == 8) {
                val p = Base64.decode(o.optString("pixels"), Base64.DEFAULT)
                if (p.size == dd * dd) px = p
            }
        } catch (e: Throwable) {
            LogStore.w(TAG, "Contact photo: the encoder failed (${e.message}), scaling here")
        }
        if (px == null) {
            val s = Bitmap.createScaledBitmap(sq, dd, dd, true)
            val argb = IntArray(dd * dd)
            s.getPixels(argb, 0, dd, 0, 0, dd, dd)
            px = ByteArray(dd * dd) { i ->
                val c = argb[i]
                (((c ushr 22) and 3 shl 4) or ((c ushr 14) and 3 shl 2) or ((c ushr 6) and 3)).toByte()
            }
        }
        circle(px, dd)
        return px
    }

    /** Clear (0x00) outside the circle the watch draws (radius cr + 1/2), opaque inside. */
    fun circle(px: ByteArray, dd: Int) {
        val cr = (dd - 1) / 2
        val lim = cr * cr + cr
        for (y in 0 until dd) for (x in 0 until dd) {
            val dx = x - cr
            val dy = y - cr
            val i = y * dd + x
            px[i] = if (dx * dx + dy * dy > lim) 0 else (px[i].toInt() or 0xC0).toByte()
        }
    }

    // ---- Notifications ---------------------------------------------------------------

    /** The photo cut round, [px] square. */
    fun roundIcon(sq: Bitmap, px: Int): Bitmap {
        val out = Bitmap.createBitmap(px, px, Bitmap.Config.ARGB_8888)
        val c = Canvas(out)
        val p = Paint(Paint.ANTI_ALIAS_FLAG or Paint.FILTER_BITMAP_FLAG)
        c.drawOval(RectF(0f, 0f, px.toFloat(), px.toFloat()), p)
        p.xfermode = PorterDuffXfermode(PorterDuff.Mode.SRC_IN)
        c.drawBitmap(sq, null, Rect(0, 0, px, px), p)
        return out
    }

    /**
     * No photo: the watch's letter avatar, [px] square: the chat's colour in a circle,
     * the first letter or digit of [name] (else '#'), white on dark colours and dark
     * grey on light ones (the watch's luma cut, 140).
     */
    fun letterIcon(name: String, color: String, px: Int): Bitmap {
        val rgb = try { Color.parseColor(color) } catch (e: Throwable) { Color.parseColor(DEFAULT_COLOR) }
        val out = Bitmap.createBitmap(px, px, Bitmap.Config.ARGB_8888)
        val c = Canvas(out)
        val p = Paint(Paint.ANTI_ALIAS_FLAG)
        p.color = rgb or (0xFF shl 24)
        c.drawOval(RectF(0f, 0f, px.toFloat(), px.toFloat()), p)
        val ch = name.firstOrNull { it in 'A'..'Z' || it in 'a'..'z' || it in '0'..'9' }?.uppercaseChar() ?: '#'
        val lum = (Color.red(rgb) * 299 + Color.green(rgb) * 587 + Color.blue(rgb) * 114) / 1000
        p.color = if (lum < 140) Color.WHITE else Color.rgb(64, 64, 64)
        p.typeface = Typeface.DEFAULT_BOLD
        p.textAlign = Paint.Align.CENTER
        p.textSize = px * 0.5f
        val fm = p.fontMetrics
        c.drawText(ch.toString(), px / 2f, px / 2f - (fm.ascent + fm.descent) / 2f, p)
        return out
    }

    /**
     * A picture message's picture as a content URI for the notification's conversation
     * view (the system reads it while the notification shows), through the app's
     * FileProvider (filesDir is shared as "logs"). Null when it can't be made.
     */
    fun pictureUri(ctx: Context, convoId: String, picture: Bitmap, whenMs: Long): Uri? = try {
        val dir = File(ctx.filesDir, "notif").apply { mkdirs() }
        val f = File(dir, "${sha1(convoId).take(16)}-$whenMs.jpg")
        if (!f.exists()) f.outputStream().use { picture.compress(Bitmap.CompressFormat.JPEG, 85, it) }
        dir.listFiles()?.sortedByDescending { it.lastModified() }?.drop(24)?.forEach { it.delete() }
        androidx.core.content.FileProvider.getUriForFile(ctx, "${ctx.packageName}.fileprovider", f)
    } catch (e: Throwable) {
        LogStore.w(TAG, "Notification picture: no URI (${e.message})")
        null
    }

    // ---- Fetching and the disk ----------------------------------------------------

    /** GET [url] (https), at most 4 MB: the body; null on 404 / 410 (nothing there); throws otherwise. */
    fun httpGet(url: String): ByteArray? {
        if (!url.startsWith("https://")) return null
        val c = URL(url).openConnection() as HttpURLConnection
        try {
            c.connectTimeout = 10_000
            c.readTimeout = 15_000
            c.setRequestProperty("User-Agent", "MirrorMsg")
            val code = c.responseCode
            if (code == 404 || code == 410) return null
            if (code !in 200..299) throw IOException("HTTP $code")
            c.inputStream.use { ins ->
                val out = ByteArrayOutputStream()
                val buf = ByteArray(16384)
                while (true) {
                    val n = ins.read(buf)
                    if (n < 0) break
                    out.write(buf, 0, n)
                    if (out.size() > MAX_BYTES) throw IOException("over 4 MB")
                }
                return out.toByteArray()
            }
        } finally {
            c.disconnect()
        }
    }

    private fun dir(ctx: Context) = File(ctx.filesDir, "avatars").apply { mkdirs() }
    private fun pngFile(ctx: Context, id: String) = File(dir(ctx), sha1(id) + ".png")
    private fun noneFile(ctx: Context, id: String) = File(dir(ctx), sha1(id) + ".none")

    private fun fresh(ctx: Context, id: String, now: Long): Boolean {
        val f = pngFile(ctx, id)
        return f.exists() && now - f.lastModified() < FRESH_MS
    }

    private fun noneFresh(ctx: Context, id: String, now: Long): Boolean {
        val f = noneFile(ctx, id)
        return f.exists() && now - f.lastModified() < NONE_MS
    }

    private fun markNone(ctx: Context, id: String) {
        try {
            noneFile(ctx, id).writeText("")
            pngFile(ctx, id).delete()
        } catch (_: Throwable) {}
        mem.remove(id)
    }

    // Written beside it and renamed over it, so a reader never sees half a file.
    private fun save(ctx: Context, id: String, sq: Bitmap) {
        try {
            val f = pngFile(ctx, id)
            val tmp = File(f.path + ".tmp")
            tmp.outputStream().use { sq.compress(Bitmap.CompressFormat.PNG, 100, it) }
            if (!tmp.renameTo(f)) { tmp.delete(); return }
            noneFile(ctx, id).delete()
            val all = dir(ctx).listFiles() ?: return
            if (all.size > MAX_FILES) all.sortedBy { it.lastModified() }.take(all.size - MAX_FILES).forEach { it.delete() }
        } catch (e: Throwable) {
            LogStore.w(TAG, "Contact photo: not kept (${e.message})")
        }
    }

    private fun sha1(s: String): String =
        MessageDigest.getInstance("SHA-1").digest(s.toByteArray()).joinToString("") { "%02x".format(it) }

    private fun fnv1a(s: String): Long {
        var h = 0x811C9DC5L
        for (b in s.toByteArray(Charsets.UTF_8)) {
            h = h xor (b.toLong() and 0xFF)
            h = (h * 0x01000193L) and 0xFFFFFFFFL
        }
        return h
    }
}
