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
 * Each call also forwards to android.util.Log so logcat still works.
 */
object LogStore {
    private const val MAX_LINES = 2000
    private val lines = Collections.synchronizedList(ArrayList<String>(MAX_LINES))
    private val fmt = SimpleDateFormat("MM-dd HH:mm:ss.SSS", Locale.US)

    @Volatile private var logFile: File? = null

    fun init(ctx: Context) {
        if (logFile == null) {
            logFile = File(ctx.filesDir, "mirrormsg_log.txt")
        }
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

    fun i(tag: String, msg: String) { Log.i(tag, msg); add("I", tag, msg) }
    fun w(tag: String, msg: String) { Log.w(tag, msg); add("W", tag, msg) }
    fun e(tag: String, msg: String, t: Throwable? = null) {
        Log.e(tag, msg, t)
        add("E", tag, msg + (t?.let { " :: ${it.message}" } ?: ""))
    }
    fun d(tag: String, msg: String) { Log.d(tag, msg); add("D", tag, msg) }

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
