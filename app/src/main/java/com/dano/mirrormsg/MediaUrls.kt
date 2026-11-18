package com.dano.mirrormsg

import android.content.Context
import fi.mirrormsg.mediaroute.Mediaroute
import java.io.File
import java.net.HttpURLConnection
import java.net.URL
import java.security.MessageDigest
import java.util.concurrent.ConcurrentHashMap

/**
 * Media that arrives as a LINK rather than as an attachment.
 *
 * A GIF picked from a picker is almost never uploaded: Discord's picker sends a
 * klipy.com/gifs/... link, others send tenor.com/view/... or giphy.com/gifs/... ,
 * and a pasted link can end in .gif outright. Every real client renders those as
 * the GIF, so MirrorMsg does too. Nothing here is network-specific.
 *
 * A direct .gif link is downloaded as is. A share PAGE is resolved first by
 * reading the page's preview tags — the same og:image / og:video tags Discord
 * reads when it unfurls the link — which turns the page into the file it shows.
 *
 * The bytes then go to the same Go encoder the bridges use (mediaroute.Encode),
 * so a linked GIF walks the identical path as an uploaded one: same animation
 * ladder, same thumbnail rules, same watch-side retries.
 *
 * Downloads are cached on disk because one GIF is fetched several times over —
 * the chat-row preview, the full animation, and again at a smaller rung whenever
 * the watch refuses the first attempt.
 */
object MediaUrls {
    private const val TAG = "MediaUrls"
    private const val MAX_BYTES = 24L * 1024 * 1024     // refuse absurd files outright
    private const val CACHE_KEEP = 16                   // most recent downloads kept
    private const val CONNECT_MS = 15000
    private const val READ_MS = 30000
    private const val HTML_SNIFF = 192 * 1024           // preview tags live near the top

    private val GIF_RE = Regex(
        """https?://[^\s<>"'\\\]\[)(}{]+\.gif(?:[?#][^\s<>"'\\\]\[)(}{]*)?""",
        RegexOption.IGNORE_CASE
    )
    private val VIDEO_RE = Regex(
        """https?://[^\s<>"'\\\]\[)(}{]+\.(?:mp4|m4v|webm|mov)(?:[?#][^\s<>"'\\\]\[)(}{]*)?""",
        RegexOption.IGNORE_CASE
    )
    private val URL_RE = Regex(
        """https?://[^\s<>"'\\\]\[)(}{]+""",
        RegexOption.IGNORE_CASE
    )

    /** Hosts whose links are a GIF by definition — the pickers people actually use. */
    private val GIF_PAGE_HOSTS = setOf(
        "tenor.com", "media.tenor.com", "c.tenor.com",
        "klipy.com", "media.klipy.com",
        "giphy.com", "media.giphy.com", "i.giphy.com",
        "gfycat.com"
    )

    private val PREVIEW_A = Regex(
        """<meta[^>]+(?:property|name)=["'](?:og:image(?::secure_url)?|og:video(?::secure_url)?|twitter:image)["'][^>]+content=["']([^"']+)["']""",
        RegexOption.IGNORE_CASE
    )
    private val PREVIEW_B = Regex(
        """<meta[^>]+content=["']([^"']+)["'][^>]+(?:property|name)=["'](?:og:image(?::secure_url)?|og:video(?::secure_url)?|twitter:image)["']""",
        RegexOption.IGNORE_CASE
    )

    private val resolvedCache = ConcurrentHashMap<String, String>()

    /** A file by its extension, or a host that only serves files: no page to read. */
    private val FILE_EXT = setOf("gif", "jpg", "jpeg", "png", "webp", "mp4", "m4v", "webm", "mov")
    private val FILE_HOSTS = setOf(
        "pbs.twimg.com", "video.twimg.com", "i.redd.it", "v.redd.it", "preview.redd.it",
        "external-preview.redd.it", "share.redd.it", "i.imgur.com"
    )
    private fun isFile(url: String): Boolean = try {
        val u = URL(url)
        val h = u.host.lowercase()
        h in FILE_HOSTS || h.endsWith(".cdninstagram.com") || h.endsWith(".fbcdn.net") ||
            u.path.substringAfterLast('/').substringAfterLast('.', "").lowercase() in FILE_EXT
    } catch (e: Throwable) {
        false
    }

    /**
     * Discord serves other people's media through a signed proxy:
     *   images-ext-1.discordapp.net/external/<sig>/https/static.klipy.com/x.mp4
     * Those signatures expire, so a message from a fortnight ago still holds a url
     * that now 403s, while resending the same GIF works — which is exactly the
     * failure this unwraps. The original address is right there in the path, and
     * the source itself doesn't expire.
     */
    fun unwrapProxy(url: String): String {
        if (!url.contains("discordapp.net", ignoreCase = true)) return url
        val i = url.indexOf("/external/", ignoreCase = true)
        if (i < 0) return url
        val afterSig = url.substring(i + "/external/".length).substringAfter('/', "")
        val scheme = afterSig.substringBefore('/', "")
        if (!scheme.equals("http", true) && !scheme.equals("https", true)) return url
        val rest = afterSig.substringAfter('/', "")
        if (rest.isEmpty()) return url
        return scheme.lowercase() + "://" + rest.substringBefore('?')
    }

    fun isHttp(s: String): Boolean =
        s.startsWith("http://", ignoreCase = true) || s.startsWith("https://", ignoreCase = true)

    /** The first .gif link in [text], or null. */
    fun gifUrl(text: String): String? = GIF_RE.find(text)?.value

    /** The first video link in [text], or null. */
    fun videoUrl(text: String): String? = VIDEO_RE.find(text)?.value

    /**
     * The first link in [text] that is a GIF or a GIF share page, or null. The page
     * is NOT fetched here — that waits until the watch asks for the picture — so
     * this stays cheap enough to run while packing a whole thread.
     */
    fun gifLink(text: String): String? {
        for (m in URL_RE.findAll(text)) {
            val u = m.value.trimEnd('.', ',', ';', ')', ']', '>')
            val parsed = try {
                URL(u)
            } catch (e: Throwable) {
                continue
            }
            // X / Reddit / Imgur links (their GIFs included) are LinkMedia's: it knows
            // what each post holds and labels it as a link.
            if (LinkMedia.handles(parsed.host)) continue
            if (u.substringBefore('?').endsWith(".gif", ignoreCase = true)) return u
            val host = parsed.host.lowercase().removePrefix("www.")
            if (host in GIF_PAGE_HOSTS) return u
            if (parsed.path.lowercase().contains("/gif")) return u
        }
        return null
    }

    /** [text] with [url] removed, tidied — what's left is the caption, if any. */
    fun stripUrl(text: String, url: String): String =
        text.replace(url, " ").replace(Regex("""\s+"""), " ").trim()

    /** True when the message is nothing but the link (so the media IS the message). */
    fun isBareUrl(text: String, url: String): Boolean = stripUrl(text, url).isEmpty()

    /**
     * Turn a share-page link into the file it shows, by reading the page's preview
     * tags. Returns [url] unchanged when it already points at a file, or when the
     * page says nothing useful.
     */
    fun resolve(url: String): String {
        if (isFile(url)) return url
        if (unwrapProxy(url) != url) return url      // a proxied file, not a share page
        resolvedCache[url]?.let { return it }
        var conn: HttpURLConnection? = null
        val found = try {
            conn = open(url, "text/html,application/xhtml+xml,*/*;q=0.8")
            if (conn.responseCode !in 200..299) {
                LogStore.w(TAG, "HTTP ${conn.responseCode} resolving ${host(url)}")
                null
            } else {
                val head = conn.inputStream.use { ins ->
                    val buf = ByteArray(HTML_SNIFF)
                    var n = 0
                    while (n < buf.size) {
                        val r = ins.read(buf, n, buf.size - n)
                        if (r <= 0) break
                        n += r
                    }
                    String(buf, 0, n, Charsets.UTF_8)
                }
                val hits = (PREVIEW_A.findAll(head).map { it.groupValues[1] } +
                            PREVIEW_B.findAll(head).map { it.groupValues[1] })
                    .map { it.replace("&amp;", "&") }
                    .filter { isHttp(it) }
                    .toList()
                hits.firstOrNull { it.substringBefore('?').endsWith(".gif", ignoreCase = true) }
                    ?: hits.firstOrNull { VIDEO_RE.matches(it) }
                    ?: hits.firstOrNull()
            }
        } catch (e: Throwable) {
            LogStore.w(TAG, "resolve failed for $url: ${e.message}")
            null
        } finally {
            conn?.disconnect()
        }
        if (found == null) return url
        LogStore.i(TAG, "resolved ${host(url)} -> ${host(found)}${if (found.substringBefore('?').endsWith(".gif", true)) " (.gif)" else ""}")
        resolvedCache[url] = found
        return found
    }

    /**
     * Download [url] (resolving a share page first) and encode it for the watch,
     * returning the same {"kind":"gif"|"image","data":{...}} envelope the bridges
     * return, or "" so the caller falls back to its still-image path.
     */
    fun encodeForWatch(
        ctx: Context?, url: String,
        maxW: Int, maxH: Int, maxBytes: Int, quality: Int,
        fpsFloor: Int, gifBytes: Int, startRung: Int, clipEdge: Int = 0
    ): String {
        val target = resolve(url)
        val raw = fetch(ctx, target) ?: return ""
        // Pickers serve their "GIFs" as short mp4s. A clip that loops like a GIF is
        // decoded into frames and takes the animation path; a long one stays a video.
        // clipEdge (0 = the detail view's) bounds the size such a clip is decoded at.
        if (Clips.looksLikeMp4(raw))
            return if (clipEdge > 0)
                Clips.encodeForWatch(ctx, target, raw, maxW, maxH, maxBytes, quality,
                    fpsFloor, gifBytes, startRung, clipEdge)
            else
                Clips.encodeForWatch(ctx, target, raw, maxW, maxH, maxBytes, quality,
                    fpsFloor, gifBytes, startRung)
        return try {
            Mediaroute.encode(
                decodable(raw), maxW.toLong(), maxH.toLong(), maxBytes.toLong(), quality.toLong(),
                fpsFloor.toLong(), 0L, gifBytes.toLong(), startRung.toLong()
            )
        } catch (e: Throwable) {
            LogStore.w(TAG, "encode failed for ${host(url)}: ${e.message}")
            ""
        }
    }

    /**
     * A STILL of the media at [url]: a {"kind":"image",...} envelope even when the
     * file is animated. The still-image path needs {w,h,pixels}; handing it an
     * animation (which the encoder returns for any animated input) is what made it
     * throw "No value for w".
     */
    fun encodeStillForWatch(
        ctx: Context?, url: String, maxW: Int, maxH: Int, maxBytes: Int, quality: Int
    ): String {
        val raw = fetch(ctx, resolve(url)) ?: return ""
        return stillFromBytes(ctx, raw, maxW, maxH, maxBytes, quality)
    }

    /** First frame of a GIF / clip, or the picture itself, encoded as a still. */
    fun stillFromBytes(
        ctx: Context?, raw: ByteArray, maxW: Int, maxH: Int, maxBytes: Int, quality: Int
    ): String {
        if (Clips.looksLikeMp4(raw)) return Clips.stillForWatch(ctx, raw, maxW, maxH, maxBytes, quality)
        return try {
            val isGif = raw.size > 6 && raw[0] == 'G'.code.toByte() && raw[1] == 'I'.code.toByte() &&
                raw[2] == 'F'.code.toByte()
            val src = if (isGif) {
                val bm = android.graphics.BitmapFactory.decodeByteArray(raw, 0, raw.size)
                if (bm == null) raw else {
                    val out = java.io.ByteArrayOutputStream()
                    bm.compress(android.graphics.Bitmap.CompressFormat.PNG, 100, out)
                    bm.recycle()
                    out.toByteArray()
                }
            } else decodable(raw)
            Mediaroute.encode(
                src, maxW.toLong(), maxH.toLong(), maxBytes.toLong(), quality.toLong(),
                0L, 0L, 0L, 0L
            )
        } catch (e: Throwable) {
            LogStore.w(TAG, "still encode failed: ${e.message}")
            ""
        }
    }

    /** WebP: a RIFF container whose form type is WEBP. */
    private fun isWebp(b: ByteArray): Boolean =
        b.size > 12 && b[0] == 'R'.code.toByte() && b[1] == 'I'.code.toByte() && b[2] == 'F'.code.toByte() &&
            b[3] == 'F'.code.toByte() && b[8] == 'W'.code.toByte() && b[9] == 'E'.code.toByte() &&
            b[10] == 'B'.code.toByte() && b[11] == 'P'.code.toByte()

    /**
     * The Go encoder reads GIF, JPEG and PNG only. A WebP picture (Reddit's preview
     * cards, .webp links, sites that serve WebP to anything accepting image/...) is
     * decoded by Android and handed over as PNG; anything else passes unchanged.
     */
    private fun decodable(raw: ByteArray): ByteArray {
        if (!isWebp(raw)) return raw
        val bm = android.graphics.BitmapFactory.decodeByteArray(raw, 0, raw.size) ?: return raw
        val out = java.io.ByteArrayOutputStream()
        bm.compress(android.graphics.Bitmap.CompressFormat.PNG, 100, out)
        bm.recycle()
        return out.toByteArray()
    }

    /**
     * Download [url] straight to [path] (video streaming). Returns bytes, or -1. A
     * link's file behind a signed address (Instagram's lapse within days) comes from
     * its current address, and one refused is renewed once (see LinkMedia.current).
     */
    fun downloadToFile(url: String, path: String): Long = try {
        if (url.contains(LinkMedia.DASH_AUDIO)) downloadPieces(url, path) else downloadOne(url, path)
    } catch (e: Throwable) {
        LogStore.w(TAG, "download failed for ${host(url)}: ${e.message}")
        -1L
    }

    /**
     * A video Facebook serves in pieces (LinkMedia.dashPair): its picture track and its
     * sound track downloaded side by side, then joined into one MP4 at [path]. Without
     * the sound (it didn't come, or won't join) the picture alone, silent.
     */
    private fun downloadPieces(url: String, path: String): Long {
        val now = LinkMedia.current(url)
        val vUrl = now.substringBefore(LinkMedia.DASH_AUDIO)
        val aUrl = java.net.URLDecoder.decode(now.substringAfter(LinkMedia.DASH_AUDIO, ""), "UTF-8")
        val vPath = "$path.v"
        val aPath = "$path.a"
        var aBytes = -1L
        val t = Thread { aBytes = try { if (aUrl.isNotEmpty()) saveTo(aUrl, aPath).second else -1L } catch (e: Throwable) { -1L } }
        t.start()
        var v = saveTo(vUrl, vPath)
        t.join(60_000)
        if (v.second < 0 && (v.first == 403 || v.first == 410)) {
            // Lapsed: the link read again, both pieces from the new addresses.
            val again = LinkMedia.current(url, refresh = true)
            if (again != now) {
                File(vPath).delete(); File(aPath).delete()
                return downloadPieces(again, path)
            }
        }
        if (v.second < 0) { File(vPath).delete(); File(aPath).delete(); return -1L }
        val joined = aBytes > 0 && joinTracks(vPath, aPath, path)
        if (!joined) {
            File(path).delete()
            if (!File(vPath).renameTo(File(path))) { File(vPath).delete(); File(aPath).delete(); return -1L }
            LogStore.w(TAG, "video in pieces: " + (if (aBytes > 0) "the sound wouldn't join" else "no sound track came") + "; silent")
        } else {
            LogStore.i(TAG, "video in pieces: picture ${v.second / 1024} KB + sound ${aBytes / 1024} KB joined")
        }
        File(vPath).delete(); File(aPath).delete()
        return File(path).length().takeIf { it > 0 } ?: -1L
    }

    /** The first video track of [vPath] and the first audio track of [aPath] into one MP4 at [out]. */
    private fun joinTracks(vPath: String, aPath: String, out: String): Boolean {
        val ex = ArrayList<android.media.MediaExtractor>()
        var mux: android.media.MediaMuxer? = null
        var started = false
        return try {
            File(out).delete()
            val m = android.media.MediaMuxer(out, android.media.MediaMuxer.OutputFormat.MUXER_OUTPUT_MPEG_4)
            mux = m
            val tracks = ArrayList<Pair<android.media.MediaExtractor, Int>>()
            for ((p, kind) in listOf(vPath to "video/", aPath to "audio/")) {
                val e = android.media.MediaExtractor()
                ex.add(e)
                e.setDataSource(p)
                val k = (0 until e.trackCount).firstOrNull {
                    e.getTrackFormat(it).getString(android.media.MediaFormat.KEY_MIME)?.startsWith(kind) == true
                } ?: return false
                e.selectTrack(k)
                tracks.add(e to m.addTrack(e.getTrackFormat(k)))
            }
            m.start()
            started = true
            val buf = java.nio.ByteBuffer.allocate(4 * 1024 * 1024)
            val info = android.media.MediaCodec.BufferInfo()
            for ((e, ti) in tracks) {
                while (true) {
                    buf.clear()
                    val n = e.readSampleData(buf, 0)
                    if (n < 0) break
                    info.offset = 0
                    info.size = n
                    info.presentationTimeUs = e.sampleTime
                    info.flags = if ((e.sampleFlags and android.media.MediaExtractor.SAMPLE_FLAG_SYNC) != 0)
                        android.media.MediaCodec.BUFFER_FLAG_KEY_FRAME else 0
                    m.writeSampleData(ti, buf, info)
                    e.advance()
                }
            }
            m.stop()
            started = false
            true
        } catch (e: Throwable) {
            LogStore.w(TAG, "joining a video's pieces failed: ${e.javaClass.simpleName}")
            false
        } finally {
            if (started) runCatching { mux?.stop() }
            runCatching { mux?.release() }
            ex.forEach { runCatching { it.release() } }
        }
    }

    private fun downloadOne(url: String, path: String): Long = try {
        val from = LinkMedia.current(url)
        val r = saveTo(unwrapProxy(resolve(from)), path)
        if (r.second < 0 && (r.first == 403 || r.first == 410)) {
            val again = LinkMedia.current(url, refresh = true)
            if (again != from) saveTo(unwrapProxy(resolve(again)), path).second else r.second
        } else r.second
    } catch (e: Throwable) {
        LogStore.w(TAG, "download failed for ${host(url)}: ${e.message}")
        -1L
    }

    /** [url] into the file at [path]: (HTTP status, 0 without one; bytes written, or -1). */
    private fun saveTo(url: String, path: String): Pair<Int, Long> {
        var conn: HttpURLConnection? = null
        var code = 0
        return try {
            conn = open(url, "video/*,*/*;q=0.8")
            code = conn.responseCode
            if (code !in 200..299) {
                LogStore.w(TAG, "HTTP $code for ${host(url)}")
                return code to -1L
            }
            val out = File(path)
            out.parentFile?.mkdirs()
            var total = 0L
            conn.inputStream.use { ins ->
                out.outputStream().use { os ->
                    val buf = ByteArray(64 * 1024)
                    while (true) {
                        val n = ins.read(buf)
                        if (n <= 0) break
                        total += n
                        if (total > MAX_BYTES) {
                            LogStore.w(TAG, "aborting ${host(url)} — over ${MAX_BYTES / (1024 * 1024)} MB")
                            return code to -1L
                        }
                        os.write(buf, 0, n)
                    }
                }
            }
            LogStore.i(TAG, "downloaded ${total / 1024} KB from ${host(url)}")
            code to total
        } catch (e: Throwable) {
            LogStore.w(TAG, "download failed for ${host(url)}: ${e.message}")
            code to -1L
        } finally {
            conn?.disconnect()
        }
    }

    // ---- internals -------------------------------------------------------------

    private fun open(url: String, accept: String): HttpURLConnection {
        val c = URL(url).openConnection() as HttpURLConnection
        c.connectTimeout = CONNECT_MS
        c.readTimeout = READ_MS
        c.instanceFollowRedirects = true
        c.setRequestProperty("User-Agent", "Mozilla/5.0 (Android) MirrorMsg")
        c.setRequestProperty("Accept", accept)
        return c
    }

    private fun host(url: String): String = try {
        URL(url).host
    } catch (e: Throwable) {
        url.take(40)
    }

    private fun cacheDir(ctx: Context?): File? {
        val base = ctx?.cacheDir ?: return null
        val d = File(base, "urlmedia")
        if (!d.exists()) d.mkdirs()
        return if (d.isDirectory) d else null
    }

    private fun keyFor(url: String): String =
        MessageDigest.getInstance("SHA-1").digest(url.toByteArray())
            .joinToString("") { "%02x".format(it) }

    private fun trimCache(dir: File) {
        val files = dir.listFiles()?.sortedByDescending { it.lastModified() } ?: return
        if (files.size <= CACHE_KEEP) return
        files.drop(CACHE_KEEP).forEach { runCatching { it.delete() } }
    }

    private fun fetch(ctx: Context?, url: String): ByteArray? {
        val direct = unwrapProxy(url)
        if (direct != url) {
            val viaSource = fetchOne(ctx, direct)
            if (viaSource != null) return viaSource
            LogStore.i(TAG, "source fetch failed, trying Discord's proxy copy")
        }
        return fetchOne(ctx, url)
    }

    private fun fetchOne(ctx: Context?, url: String): ByteArray? {
        val hit = cacheDir(ctx)?.let { File(it, keyFor(url)) }
        if (hit != null && hit.isFile && hit.length() > 0) {
            hit.setLastModified(System.currentTimeMillis())
            return try {
                hit.readBytes()
            } catch (e: Throwable) {
                null
            }
        }
        // A link's file behind a signed address (Instagram's lapse within days) comes
        // from its current address, and one refused is renewed once. It's kept under
        // the address it was asked for.
        val from = try { LinkMedia.current(url) } catch (e: Throwable) { url }
        var got = pull(from)
        if (got.second == null && (got.first == 403 || got.first == 410)) {
            val again = try { LinkMedia.current(url, refresh = true) } catch (e: Throwable) { from }
            if (again != from) got = pull(again)
        }
        val bytes = got.second
        if (bytes != null && bytes.isNotEmpty()) {
            LogStore.i(TAG, "fetched ${bytes.size / 1024} KB from ${host(url)}")
            if (hit != null) {
                runCatching { hit.writeBytes(bytes) }
                runCatching { hit.parentFile?.let { trimCache(it) } }
            }
        }
        return bytes
    }

    /** (HTTP status, 0 without one; the file, or null when it didn't come) from [url]. */
    private fun pull(url: String): Pair<Int, ByteArray?> {
        var conn: HttpURLConnection? = null
        var code = 0
        return try {
            conn = open(url, "image/gif,image/*,video/*;q=0.8,*/*;q=0.5")
            code = conn.responseCode
            if (code !in 200..299) {
                LogStore.w(TAG, "HTTP $code for ${host(url)}")
                return code to null
            }
            val acc = java.io.ByteArrayOutputStream()
            val buf = ByteArray(64 * 1024)
            conn.inputStream.use { ins ->
                while (true) {
                    val n = ins.read(buf)
                    if (n <= 0) break
                    acc.write(buf, 0, n)
                    if (acc.size() > MAX_BYTES) {
                        LogStore.w(TAG, "aborting ${host(url)} — over ${MAX_BYTES / (1024 * 1024)} MB")
                        return code to null
                    }
                }
            }
            code to acc.toByteArray()
        } catch (e: Throwable) {
            LogStore.w(TAG, "fetch failed for ${host(url)}: ${e.message}")
            code to null
        } finally {
            conn?.disconnect()
        }
    }
}
