package com.dano.mirrormsg

import android.content.Context
import android.util.Log
import org.json.JSONArray
import org.json.JSONObject
import java.io.File

/**
 * Persistent, deduplicated per-conversation message store.
 *
 * The past never changes: once a message is stored it's permanent. New
 * messages are merged in either via live MESSAGE events (background) or via
 * on-open catch-up fetch. Messages are keyed by messageID, sorted by
 * timestamp, and capped at MAX_PER_CONVO to bound file size.
 *
 * Files live at: filesDir/threads/<convoID>.json
 * Each file is a JSON array of raw message objects (same shape as the
 * "messages" array from fetchMessages and the MESSAGE event payload).
 */
object MessageStore {
    private const val TAG = "MirrorMsgSpike"
    private const val MAX_PER_CONVO = 200

    private fun dir(ctx: Context): File =
        File(ctx.filesDir, "threads").apply { if (!exists()) mkdirs() }

    private fun fileFor(ctx: Context, convoId: String): File {
        // Sanitize convoID for filename (it can contain / etc.)
        val safe = convoId.replace(Regex("[^A-Za-z0-9_-]"), "_")
        return File(dir(ctx), "$safe.json")
    }

    /** Load all stored messages for a conversation as a JSON array string. */
    @Synchronized
    fun load(ctx: Context, convoId: String): JSONArray {
        val f = fileFor(ctx, convoId)
        if (!f.exists()) return JSONArray()
        return try {
            JSONArray(f.readText())
        } catch (e: Exception) {
            Log.e(TAG, "MessageStore.load failed for $convoId", e)
            JSONArray()
        }
    }

    /** Returns the stored messages wrapped as {"messages":[...]} for streaming. */
    @Synchronized
    fun loadAsThreadJson(ctx: Context, convoId: String): String {
        val arr = load(ctx, convoId)
        // Messages can be stored out of chronological order (Facebook delivers
        // history/realtime in bursts that are newest-first within each batch and
        // across batches). Sort ascending by timestamp so the thread reads
        // oldest→newest top to bottom on the watch. No-op for already-ordered data.
        val items = ArrayList<JSONObject>(arr.length())
        for (i in 0 until arr.length()) items.add(arr.getJSONObject(i))
        items.sortBy { it.optString("timestamp").toLongOrNull() ?: 0L }
        val sorted = JSONArray()
        for (m in items) sorted.put(m)
        val obj = JSONObject()
        obj.put("messages", sorted)
        return obj.toString()
    }

    /** The newest stored message timestamp (microseconds), or 0 if empty. */
    @Synchronized
    fun newestTimestamp(ctx: Context, convoId: String): Long {
        val arr = load(ctx, convoId)
        var newest = 0L
        for (i in 0 until arr.length()) {
            val ts = arr.getJSONObject(i).optString("timestamp").toLongOrNull() ?: 0L
            if (ts > newest) newest = ts
        }
        return newest
    }

    /** The messageID of the newest stored message (by timestamp), or "" if the
     *  conversation has none. Used to mark a thread read UP THROUGH its latest
     *  message when the user enters it on the watch. Mirrors [version]'s
     *  newest-by-timestamp selection. */
    @Synchronized
    fun newestMessageId(ctx: Context, convoId: String): String {
        val arr = load(ctx, convoId)
        var newestTs = -1L
        var newestId = ""
        for (i in 0 until arr.length()) {
            val m = arr.getJSONObject(i)
            val ts = m.optString("timestamp").toLongOrNull() ?: 0L
            if (ts >= newestTs) { newestTs = ts; newestId = msgId(m) }
        }
        return newestId
    }

    /** Preview of the newest stored message for a conversation: its text (media
     *  rendered as a tag), the sending direction, and the timestamp (microseconds).
     *  Used to patch a convo-list row whose bridge-provided preview lags behind a
     *  just-seen message (notably an OUTGOING send, which some bridges are slow to
     *  reflect in their conversation list). Returns null when the store is empty. */
    @Synchronized
    fun newestSnippet(ctx: Context, convoId: String): Snippet? {
        val arr = load(ctx, convoId)
        if (arr.length() == 0) return null
        var newestTs = -1L
        var newest: JSONObject? = null
        for (i in 0 until arr.length()) {
            val m = arr.getJSONObject(i)
            val ts = m.optString("timestamp").toLongOrNull() ?: 0L
            if (ts >= newestTs) { newestTs = ts; newest = m }
        }
        val m = newest ?: return null
        var text = msgText(m)
        if (text.isEmpty() && (hasMediaID(m) || hasInlineMedia(m))) text = "[Image]"
        // Direction: mirror LiveRouter's tolerant isMe read (some outgoing rows
        // carry no senderParticipant, so fall back to fromMe / OUTGOING status).
        val sp = m.optJSONObject("senderParticipant")
        val status = m.optJSONObject("messageStatus")?.optString("status") ?: ""
        val isMe = (sp?.optBoolean("isMe", false) ?: false) ||
                   m.optBoolean("isMe", false) ||
                   m.optBoolean("fromMe", false) ||
                   status.startsWith("OUTGOING")
        return Snippet(text, isMe, newestTs)
    }

    /** Newest-message preview for a conversation. [timestampMicros] matches the
     *  store's microsecond timestamps so callers can compare recency against a
     *  bridge list row. */
    data class Snippet(val text: String, val isMe: Boolean, val timestampMicros: Long)

    /**
     * A stable version token for a conversation: "<count>:<newestMessageID>".
     * Unlike a bare timestamp this doesn't drift when a message's status
     * updates, so the watch cache check only invalidates on genuinely new
     * messages.
     */
    @Synchronized
    fun version(ctx: Context, convoId: String): String {
        val arr = load(ctx, convoId)
        if (arr.length() == 0) return "0:none"
        // Find newest by timestamp, take its id.
        var newestTs = -1L
        var newestId = "none"
        for (i in 0 until arr.length()) {
            val m = arr.getJSONObject(i)
            val ts = m.optString("timestamp").toLongOrNull() ?: 0L
            if (ts >= newestTs) { newestTs = ts; newestId = msgId(m) }
        }
        return "${arr.length()}:$newestId"
    }

    /** True if any messageInfo entry carries a mediaContent.mediaID (the handle
     *  needed to download the full image). */
    // Set by merge(): number of messages that were new OR meaningfully updated
    // (gained media/content). Lets callers re-stream on status progressions
    // even when no brand-new messageID was added.
    @Volatile var lastMergeChanged = 0
        private set
    @Volatile var lastMergeAdded = 0
        private set

    /** True if `now` carries media/content that `before` lacked. */
    private fun isRicher(now: JSONObject, before: JSONObject): Boolean {
        val nowMedia = hasMediaID(now) || hasInlineMedia(now)
        val beforeMedia = hasMediaID(before) || hasInlineMedia(before)
        if (nowMedia && !beforeMedia) return true
        // Gained text content where there was none.
        if (msgText(now).isNotEmpty() && msgText(before).isEmpty()) return true
        return false
    }

    private fun hasInlineMedia(m: JSONObject): Boolean {
        val infos = m.optJSONArray("messageInfo") ?: return false
        for (j in 0 until infos.length()) {
            val media = infos.optJSONObject(j)?.optJSONObject("mediaContent") ?: continue
            if (media.optString("mediaData").isNotEmpty()) return true
        }
        return false
    }

    private fun msgText(m: JSONObject): String {
        val infos = m.optJSONArray("messageInfo") ?: return ""
        for (j in 0 until infos.length()) {
            val mc = infos.optJSONObject(j)?.optJSONObject("messageContent") ?: continue
            val c = mc.optString("content")
            if (c.isNotEmpty()) return c
        }
        return ""
    }

    private fun hasMediaID(m: JSONObject): Boolean {
        val infos = m.optJSONArray("messageInfo") ?: return false
        for (j in 0 until infos.length()) {
            val media = infos.optJSONObject(j)?.optJSONObject("mediaContent") ?: continue
            if (media.optString("mediaID").isNotEmpty()) return true
        }
        return false
    }

    private fun msgId(m: JSONObject): String =
        m.optString("messageID").ifEmpty {
            m.optJSONObject("anotherMessageID")?.optString("messageID") ?: ""
        }

    /**
     * Merge a list of messages (JSONArray of message objects) into storage.
     * Dedupes by messageID, sorts by timestamp, caps at MAX_PER_CONVO.
     * Returns the number of NEW messages added.
     */
    @Synchronized
    fun merge(ctx: Context, convoId: String, incoming: JSONArray): Int {
        val existing = load(ctx, convoId)
        val byId = LinkedHashMap<String, JSONObject>()
        for (i in 0 until existing.length()) {
            val m = existing.getJSONObject(i)
            val id = msgId(m)
            if (id.isNotEmpty()) byId[id] = m
        }
        var added = 0
        var changed = 0
        for (i in 0 until incoming.length()) {
            val m = incoming.getJSONObject(i)
            val id = msgId(m)
            if (id.isEmpty()) continue
            if (!byId.containsKey(id)) {
                added++
                changed++
                byId[id] = m
            } else {
                // Same message arriving again (status progression). Normally the
                // newer copy wins, BUT an image arrives in stages: an inline
                // thumbnail first, then the full copy carrying a mediaID. Don't
                // let a later media-less / mediaID-less update clobber a stored
                // copy that already has the real downloadable mediaID.
                val old = byId[id]!!
                if (hasMediaID(old) && !hasMediaID(m)) {
                    // keep the richer stored copy
                } else {
                    // If this update adds media/content the stored copy lacked
                    // (e.g. an image's later stage bringing mediaID/mediaData),
                    // count it as a change so the watch gets re-streamed.
                    if (isRicher(m, old)) changed++
                    byId[id] = m
                }
            }
        }
        lastMergeChanged = changed
        lastMergeAdded = added
        // Sort by timestamp ascending
        val sorted = byId.values.sortedBy {
            it.optString("timestamp").toLongOrNull() ?: 0L
        }
        // Cap to most recent MAX_PER_CONVO
        val capped = if (sorted.size > MAX_PER_CONVO)
            sorted.subList(sorted.size - MAX_PER_CONVO, sorted.size) else sorted
        val out = JSONArray()
        for (m in capped) out.put(m)
        try {
            fileFor(ctx, convoId).writeText(out.toString())
        } catch (e: Exception) {
            Log.e(TAG, "MessageStore.merge write failed for $convoId", e)
        }
        return added
    }

    /** Merge a single message object (from a live MESSAGE event). */
    @Synchronized
    fun mergeOne(ctx: Context, convoId: String, message: JSONObject): Int {
        val arr = JSONArray()
        arr.put(message)
        return merge(ctx, convoId, arr)
    }
}
