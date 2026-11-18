package com.dano.mirrormsg

import android.content.Context
import android.util.Log
import java.io.File
import java.text.SimpleDateFormat
import java.util.Collections
import java.util.Date
import java.util.Locale

/**
 * In-app log capture. Mirrors app log lines into a bounded ring buffer and a
 * file so they can be viewed, copied, and shared from inside the app without
 * needing adb / logcat.
 *
 * Use LogStore.i(tag, msg) etc. instead of (or alongside) android.util.Log.
 * Each call also forwards to android.util.Log so logcat still works. Every line
 * is scrubbed of private data first (see scrub()), for logcat and file alike.
 */
object LogStore {
    private const val MAX_LINES = 2000
    private val lines = Collections.synchronizedList(ArrayList<String>(MAX_LINES))
    private val fmt = SimpleDateFormat("MM-dd HH:mm:ss.SSS", Locale.US)

    @Volatile private var logFile: File? = null

    fun init(ctx: Context) {
        if (logFile == null) {
            val f = File(ctx.filesDir, "mirrormsg_log.txt")
            // One-time wipe: logs written before the scrub below may hold message
            // text, names and numbers. The marker makes it happen once per install.
            try {
                val marker = File(ctx.filesDir, "mirrormsg_log.scrubbed")
                if (!marker.exists()) { f.writeText(""); marker.writeText("1") }
            } catch (_: Exception) {}
            logFile = f
        }
    }

    // ---- Private-data scrub -------------------------------------------------
    //
    // Every line is scrubbed before it reaches logcat, the ring buffer or the
    // file, so a shared log carries structure (ids, counts, states, errors) but
    // no message text, names, numbers or logins. Two passes:
    //  1. JSON string values under the keys below are replaced by "[N chars]".
    //     Hand-written scanner, not a regex: message JSON can be tens of KB and
    //     Java's regex engine overflows its stack on long alternations. Handles
    //     plain JSON and JSON escaped inside another JSON string (\"key\":\"v\").
    //  2. Phone numbers, emails, WhatsApp / Messenger JIDs, Matrix user ids,
    //     Discord tokens and bearer tokens anywhere in the line.

    private val SECRET_KEYS = hashSetOf(
        "text", "content", "body", "snippet", "caption", "title", "name",
        "fullname", "firstname", "lastname", "sendername", "threadname", "displayname",
        "username", "nickname", "globalname", "replytotext", "replytosender",
        "replytosnippet", "formattednumber", "number", "phone", "phonenumber", "email",
        "pushname", "groupname", "message", "description", "url", "token", "accesstoken",
        "refreshtoken", "authorization", "cookie", "cookies", "xs", "c_user", "datr",
        "fr", "sessionid", "csrftoken", "ds_user_id", "auth_token", "ct0", "password",
        "session", "pairingcode", "code", "qr"
    )

    private val PHONE_INTL = Regex("""\+\d{7,15}""")
    private val PHONE_US1 = Regex("""\(\d{3}\)\s?\d{3}-\d{4}""")
    private val PHONE_US2 = Regex("""\b\d{3}-\d{3}-\d{4}\b""")
    private val JID = Regex("""[\d-]{5,}(?::\d+)?@(?:s\.whatsapp\.net|c\.us|lid|g\.us|msgr)\b""")
    // Bounded runs and no repeated groups: linear-ish on long lines, no deep recursion.
    private val EMAIL = Regex("""[A-Za-z0-9._%+-]{1,64}@[A-Za-z0-9.-]{1,253}\.[A-Za-z]{2,24}""")
    private val MXID = Regex("""@[A-Za-z0-9._=/+-]{1,255}:[A-Za-z0-9.-]{1,253}""")
    private val DISCORD_TOKEN = Regex("""[A-Za-z0-9_-]{23,28}\.[A-Za-z0-9_-]{6,7}\.[A-Za-z0-9_-]{27,40}""")
    private val MFA_TOKEN = Regex("""mfa\.[A-Za-z0-9_-]{60,}""")
    private val BEARER = Regex("""(?i)bearer\s+[A-Za-z0-9._~+/=-]{10,}""")

    /** The line with private values removed. Never throws. */
    fun scrub(msg: String): String {
        if (msg.isEmpty()) return msg
        return try {
            var s = if (msg.indexOf('"') >= 0) scrubJson(msg) else msg
            if (s.indexOf('@') >= 0) {
                s = JID.replace(s, "[jid]")
                s = EMAIL.replace(s, "[email]")
                s = MXID.replace(s, "[user]")
            }
            if (s.indexOf('.') >= 0) {
                s = DISCORD_TOKEN.replace(s, "[token]")
                s = MFA_TOKEN.replace(s, "[token]")
            }
            if (s.contains("earer", ignoreCase = true)) s = BEARER.replace(s, "Bearer [token]")
            if (s.any { it.isDigit() }) {
                s = PHONE_INTL.replace(s, "[phone]")
                s = PHONE_US1.replace(s, "[phone]")
                s = PHONE_US2.replace(s, "[phone]")
            }
            s
        } catch (_: Throwable) {
            "[log line dropped: scrub failed, ${msg.length} chars]"
        }
    }

    private fun backslashesBefore(s: String, i: Int): Int {
        var c = 0; var j = i - 1
        while (j >= 0 && s[j] == '\\') { c++; j-- }
        return c
    }

    /** Nesting level of a quote preceded by c backslashes: 0->0, 1->1, 3->2, 7->3; else -1. */
    private fun levelOf(c: Int): Int {
        var n = c; var lvl = 0
        while (n > 0) { if (n % 2 == 0) return -1; n = (n - 1) / 2; lvl++ }
        return lvl
    }

    /**
     * Is a quote preceded by c backslashes the end of a string at nesting level
     * lvl? Peel one escape layer per level; a quote that ends a shallower string
     * also counts as an end (stop redacting there, never run past it).
     */
    private fun closes(c0: Int, lvl: Int): Boolean {
        var c = c0
        repeat(lvl) {
            if (c % 2 == 0) return true
            c = (c - 1) / 2
        }
        return c % 2 == 0
    }

    private fun scrubJson(s: String): String {
        val out = StringBuilder(s.length)
        var i = 0
        val n = s.length
        var copied = 0
        while (i < n) {
            if (s[i] != '"') { i++; continue }
            val c = backslashesBefore(s, i)
            val lvl = levelOf(c)
            if (lvl < 0) { i++; continue }
            // Key: identifier chars, then the same escape run and a quote.
            var k = i + 1
            while (k < n && (s[k].isLetterOrDigit() || s[k] == '_')) k++
            if (k == i + 1 || k + c >= n) { i++; continue }
            var ok = true
            for (b in 0 until c) if (s[k + b] != '\\') { ok = false; break }
            if (!ok || s[k + c] != '"') { i++; continue }
            val key = s.substring(i + 1, k)
            var v = k + c + 1
            while (v < n && s[v] == ' ') v++
            if (v >= n || s[v] != ':') { i = k + c + 1; continue }
            v++
            while (v < n && s[v] == ' ') v++
            // Value must open with the same escape run + quote.
            if (v + c >= n) { i = v; continue }
            ok = true
            for (b in 0 until c) if (s[v + b] != '\\') { ok = false; break }
            if (!ok || s[v + c] != '"') { i = v; continue }
            val start = v + c + 1
            if (key.lowercase(Locale.US) !in SECRET_KEYS) { i = start; continue }
            // Find the closing quote of this value.
            var e = start
            var end = -1
            while (e < n) {
                if (s[e] == '"' && closes(backslashesBefore(s, e), lvl)) { end = e; break }
                e++
            }
            val stop = if (end < 0) n else end - c   // value text ends before the escape run
            val len = (stop - start).coerceAtLeast(0)
            out.append(s, copied, start)
            if (len > 0) out.append('[').append(len).append(" chars]")
            copied = if (end < 0) n else stop
            i = if (end < 0) n else end + 1
        }
        if (copied == 0) return s
        out.append(s, copied, n)
        return out.toString()
    }

    private fun add(level: String, tag: String, msg: String) {
        val line = "${fmt.format(Date())} $level/$tag: $msg"
        synchronized(lines) {
            lines.add(line)
            while (lines.size > MAX_LINES) lines.removeAt(0)
        }
        // Append to file (best-effort)
        try { logFile?.appendText(line + "\n") } catch (_: Exception) {}
    }

    fun i(tag: String, msg: String) { val s = scrub(msg); Log.i(tag, s); add("I", tag, s) }
    fun w(tag: String, msg: String) { val s = scrub(msg); Log.w(tag, s); add("W", tag, s) }
    fun e(tag: String, msg: String, t: Throwable? = null) {
        val s = scrub(msg + (t?.let { " :: ${it.message}" } ?: ""))
        Log.e(tag, s)
        // Stack frames carry no private data; the exception message (already
        // folded into s above) can, so it is left out of the trace line.
        if (t != null) {
            val frames = t.stackTrace.take(12).joinToString("\n") { "    at $it" }
            Log.e(tag, "${t.javaClass.name}\n$frames")
        }
        add("E", tag, s)
    }
    fun d(tag: String, msg: String) { val s = scrub(msg); Log.d(tag, s); add("D", tag, s) }

    /** Full log as a single string (oldest first). */
    fun dump(): String = synchronized(lines) { lines.joinToString("\n") }

    /** Clear in-memory and file logs. */
    fun clear() {
        synchronized(lines) { lines.clear() }
        try { logFile?.writeText("") } catch (_: Exception) {}
    }

    /** The backing file (for share intent). Writes current buffer first. */
    fun fileForShare(ctx: Context): File {
        init(ctx)
        val f = logFile!!
        try { f.writeText(dump()) } catch (_: Exception) {}
        return f
    }

    // ---- Heap reporting -----------------------------------------------------

    fun logHeap(reason: String = "periodic") {
        val rt = Runtime.getRuntime()
        val usedMb = (rt.totalMemory() - rt.freeMemory()) / (1024.0 * 1024.0)
        val maxMb = rt.maxMemory() / (1024.0 * 1024.0)
        val nativeMb = android.os.Debug.getNativeHeapAllocatedSize() / (1024.0 * 1024.0)
        i("Heap", String.format(Locale.US,
            "[%s] JVM used %.1f / %.0f MB  |  native %.1f MB",
            reason, usedMb, maxMb, nativeMb))
    }
}
