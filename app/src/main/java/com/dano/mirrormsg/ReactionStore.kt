package com.dano.mirrormsg

/**
 * ReactionStore — a source-agnostic store of message reactions.
 *
 * Reactions are a generic core capability, deliberately decoupled from any one
 * network: this store knows nothing about Facebook, Beeper, Discord, etc. Every
 * source maps its own reaction events onto a single entry point, [ingest], using
 * opaque string identities. Adding a new native network later (e.g. a native
 * Discord) requires NO change here — that network just calls [ingest].
 *
 * Model: messageId -> (actorKey -> emoji)
 *  - messageId: the per-network message id (FB "mid.$...", a Matrix event id, a
 *    Discord snowflake — all just strings, assumed globally unique like convoIds).
 *  - actorKey:  an opaque per-network identity for WHO reacted. Used only so one
 *    person's add -> change -> remove collapse to a single slot; never displayed.
 *  - emoji:     the reaction as raw unicode. Substitution for the Pebble font
 *    happens on READ (see [summaryFor]) so the emoji-substitution toggle applies
 *    retroactively without rebuilding the store.
 *
 * The watch reads one value per message via [summaryFor], which yields the
 * field-7 wire string the chat renderer packs.
 */
object ReactionStore {

    // messageId -> ordered (actorKey -> emoji). Outer map is insertion-ordered so
    // the oldest reacted-message can be evicted first when the persisted set grows
    // past MAX_MESSAGES.
    private val byMessage = LinkedHashMap<String, LinkedHashMap<String, String>>()

    // (thread|tsSec) -> the message ids seen at that send-second. A Messenger DM
    // that's now E2EE stores its messages under the numeric whatsmeow id but its
    // reactions arrive keyed to the cookie mid (or vice-versa); the message id and
    // the reaction id then live in different id-spaces and never match directly.
    // Both id-spaces share the same (thread, send-second), so recording every
    // message id under its stamp lets a lookup by a stored row's stamp find the
    // OTHER id the reaction is actually keyed to. Purely additive — a direct id
    // match is always tried first. NOT persisted: it is rebuilt from the message
    // re-sync every launch (cacheMessage re-notes every message on connect).
    private val aliasByStamp = HashMap<String, LinkedHashSet<String>>()

    // (thread|tsSec) -> the AUTHORITATIVE current reaction set for the message at
    // that send-second, as last reported by a server snapshot. Presence of a key
    // means "the server has spoken for this message" — even an empty value means
    // "no reactions now." When present it WINS over byMessage/alias at pack time,
    // so removals, changes, id-space duplicates, and stale optimistic self-reacts
    // are all superseded by ground truth. Only FB feeds this today (via op:snapshot);
    // networks that don't send snapshots simply never populate it and keep using the
    // per-id merge path. Persisted so the corrected state survives a restart.
    private val authByStamp = HashMap<String, LinkedHashMap<String, String>>()
    private val lock = Any()

    // Max distinct emoji shown in a badge; the total count still reflects all.
    private const val MAX_DISTINCT = 3

    // --- Persistence ---------------------------------------------------------
    // Reactions live for the life of a message, not a session: a bridge only
    // re-feeds old reactions in whatever id-space it syncs (often one that no
    // longer matches the stored message), and a watch-sent reaction never echoes
    // back at all. So the reaction set is persisted to disk and reloaded on start,
    // exactly like the messaging app's own local store — surviving force-close for
    // every account at once (this store is source-agnostic). Only byMessage is
    // persisted; the alias index rebuilds from the message re-sync.
    private const val FILE = "reactions.json"
    private const val MAX_MESSAGES = 4000
    private var loaded = false
    private val saveExec = java.util.concurrent.Executors.newSingleThreadExecutor()
    @Volatile private var savePending = false

    /** Load the persisted reactions once, as soon as an app context exists. */
    private fun ensureLoaded() {
        if (loaded) return
        synchronized(lock) {
            if (loaded) return
            val ctx = Mirror.appContext ?: return   // no context yet — retry next call
            loaded = true
            try {
                val f = java.io.File(ctx.filesDir, FILE)
                if (!f.exists()) return
                val root = org.json.JSONObject(f.readText())
                val msgs = root.optJSONObject("m") ?: return
                val it = msgs.keys()
                while (it.hasNext()) {
                    val mid = it.next()
                    val actors = msgs.optJSONObject(mid) ?: continue
                    val inner = LinkedHashMap<String, String>()
                    val ai = actors.keys()
                    while (ai.hasNext()) {
                        val ak = ai.next()
                        val e = actors.optString(ak)
                        if (e.isNotEmpty()) inner[ak] = e
                    }
                    if (inner.isNotEmpty()) byMessage[mid] = inner
                }
                val auth = root.optJSONObject("a")
                if (auth != null) {
                    val si = auth.keys()
                    while (si.hasNext()) {
                        val stamp = si.next()
                        val actors = auth.optJSONObject(stamp) ?: continue
                        val inner = LinkedHashMap<String, String>()
                        val ai2 = actors.keys()
                        while (ai2.hasNext()) {
                            val ak = ai2.next()
                            val e = actors.optString(ak)
                            if (e.isNotEmpty()) inner[ak] = e
                        }
                        // An authoritative empty set is still meaningful (present key
                        // = server said "no reactions"), so keep even-if-empty.
                        authByStamp[stamp] = inner
                    }
                }
            } catch (_: Exception) {
            }
        }
    }

    /** Coalesced background save — a burst of reaction updates writes once. */
    private fun scheduleSave() {
        if (savePending) return
        savePending = true
        saveExec.execute {
            try { Thread.sleep(800) } catch (_: InterruptedException) {}
            savePending = false
            persistNow()
        }
    }

    private fun persistNow() {
        val snapshot = synchronized(lock) {
            val root = org.json.JSONObject()
            val m = org.json.JSONObject()
            for ((mid, actors) in byMessage) {
                val a = org.json.JSONObject()
                for ((ak, e) in actors) a.put(ak, e)
                m.put(mid, a)
            }
            root.put("m", m)
            val a2 = org.json.JSONObject()
            for ((stamp, actors) in authByStamp) {
                val a = org.json.JSONObject()
                for ((ak, e) in actors) a.put(ak, e)
                a2.put(stamp, a)
            }
            root.put("a", a2)
            root.toString()
        }
        try {
            val ctx = Mirror.appContext ?: return
            val tmp = java.io.File(ctx.filesDir, "$FILE.tmp")
            val dst = java.io.File(ctx.filesDir, FILE)
            tmp.writeText(snapshot)
            if (!tmp.renameTo(dst)) { dst.delete(); tmp.renameTo(dst) }
        } catch (_: Exception) {
        }
    }

    /** Evict oldest reacted-messages until at most MAX_MESSAGES remain. Caller holds lock. */
    private fun trimLocked() {
        while (byMessage.size > MAX_MESSAGES) {
            val eldest = byMessage.keys.iterator().next()
            byMessage.remove(eldest)
        }
    }

    /**
     * Record (or clear) one actor's reaction on a message. A null/blank [emoji]
     * means the actor removed their reaction. This single call is the entire
     * integration surface every source uses.
     */
    fun ingest(messageId: String, actorKey: String, emoji: String?): Boolean =
        if (emoji.isNullOrEmpty()) remove(messageId, actorKey)
        else set(messageId, actorKey, emoji)

    fun set(messageId: String, actorKey: String, emoji: String): Boolean {
        if (messageId.isEmpty() || actorKey.isEmpty() || emoji.isEmpty()) return false
        ensureLoaded()
        synchronized(lock) {
            val prev = byMessage.getOrPut(messageId) { LinkedHashMap() }.put(actorKey, emoji)
            if (prev == emoji) return false
            trimLocked()
        }
        scheduleSave()
        return true
    }

    fun remove(messageId: String, actorKey: String): Boolean {
        if (messageId.isEmpty() || actorKey.isEmpty()) return false
        ensureLoaded()
        val had = synchronized(lock) {
            val m = byMessage[messageId] ?: return false
            val h = m.remove(actorKey) != null
            if (m.isEmpty()) byMessage.remove(messageId)
            h
        }
        if (had) scheduleSave()
        return had
    }

    /**
     * Replace a message's ENTIRE reaction set with [newReactions] (actorKey ->
     * emoji). For sources that deliver the full current set per update (e.g. Google
     * Messages embeds all reactions on every message), this resolves adds, changes,
     * and removals in one shot — including removal-to-empty, which clears the entry.
     * Returns true if the stored state actually changed (so callers can re-push
     * only when needed). Empty-emoji entries are ignored.
     */
    fun replaceAll(messageId: String, newReactions: Map<String, String>): Boolean {
        if (messageId.isEmpty()) return false
        ensureLoaded()
        val changed = synchronized(lock) {
            val cleaned = newReactions.filterValues { it.isNotEmpty() }
            val existing = byMessage[messageId]
            when {
                cleaned.isEmpty() -> {
                    if (existing.isNullOrEmpty()) return false
                    byMessage.remove(messageId)
                    true
                }
                existing != null && existing.size == cleaned.size &&
                    existing.all { cleaned[it.key] == it.value } -> false // identical
                else -> {
                    byMessage[messageId] = LinkedHashMap(cleaned)
                    trimLocked()
                    true
                }
            }
        }
        if (changed) scheduleSave()
        return changed
    }

    /** True if the message currently has any reactions. */
    fun has(messageId: String): Boolean {
        ensureLoaded()
        return synchronized(lock) { byMessage[messageId]?.isNotEmpty() == true }
    }

    /**
     * The field-7 value for a message: up to [MAX_DISTINCT] distinct emoji (each
     * mapped through EmojiSub.reactionGlyph), concatenated as a raw glyph run with
     * NO separator, then " <total>" (a space then the count) when there is more
     * than one reaction in total. Empty string when none.
     *
     * Wire shape (agreed with the watch): the watch draws the glyph run as one
     * string and peels the count off the trailing " <number>". Examples: "❤" ·
     * "❤ 4" · "❤😢 5" · "❤😆👍 7" (3-distinct cap).
     */
    fun summaryFor(messageId: String): String {
        ensureLoaded()
        val emojis = synchronized(lock) {
            byMessage[messageId]?.values?.toList() ?: return ""
        }
        return summarizeEmojis(emojis)
    }

    /** The field-7 glyph run + count for a list of raw reaction emoji. */
    private fun summarizeEmojis(emojis: List<String>): String {
        if (emojis.isEmpty()) return ""
        val distinct = LinkedHashSet<String>()
        for (e in emojis) {
            distinct.add(EmojiSub.reactionGlyph(e))
            if (distinct.size >= MAX_DISTINCT) break
        }
        val total = emojis.size
        val glyphs = distinct.joinToString("")
        return if (total > 1) "$glyphs $total" else glyphs
    }

    /**
     * Apply a standard bridge REACTION event — the per-actor add/remove shape emitted
     * by the FB and WhatsApp bridges: {op:"upsert"|"delete", msg, actor, emoji}. An
     * "upsert" with an empty emoji is treated as a removal. Unknown ops (FB's
     * diagnostic summary/v2upsert/deleteV2) are ignored. This is the single call a
     * whatsmeow-style bridge's event sink needs to wire reactions in.
     */
    fun handleBridgeEvent(jsonPayload: String) {
        try {
            val e = org.json.JSONObject(jsonPayload)
            val msg = e.optString("msg")
            if (msg.isEmpty()) return
            val actor = e.opt("actor")?.toString() ?: return
            when (e.optString("op")) {
                "upsert" -> ingest(msg, actor, e.optString("emoji"))
                "delete" -> remove(msg, actor)
            }
        } catch (_: Exception) {
        }
    }

    /**
     * Replace a message's reactions from an aggregate emoji -> count map, for sources
     * that expose reactions as tallies WITHOUT per-person attribution (e.g. a Content
     * Provider that only says "❤ x3, 😂 x1"). Synthesizes [count] stable internal
     * keys per emoji so the existing actor-keyed store and summaryFor work unchanged —
     * the badge shows the distinct emoji and the correct total. Returns whether the
     * stored state changed. Pass an empty map to clear.
     */
    fun setCounts(messageId: String, counts: Map<String, Int>): Boolean {
        val synthetic = LinkedHashMap<String, String>()
        for ((emoji, n) in counts) {
            if (emoji.isEmpty() || n <= 0) continue
            for (i in 0 until n) synthetic["c:$emoji#$i"] = emoji
        }
        return replaceAll(messageId, synthetic)
    }

    /**
     * Normalize a timestamp to whole SECONDS regardless of whether it arrives in
     * seconds, milliseconds, or microseconds. The store keeps message timestamps in
     * microseconds while live bridge events carry milliseconds; keying the alias on a
     * fixed divisor would put the two sides on different granularities and never
     * match. Reducing by /1000 until the value is in the epoch-seconds range
     * (~1.7e9) makes both sides land on the same second.
     */
    private fun toSec(ts: Long): Long {
        var x = ts
        while (x > 100_000_000_000L) x /= 1000L
        return x
    }

    private fun stampKey(thread: String, ts: Long) = "$thread|${toSec(ts)}"

    /**
     * Record that [messageId] exists in thread [thread] at time [ts] (any of s/ms/us),
     * so a reaction keyed to the same message's OTHER id-space can be matched by
     * stamp. Called for every message; a cheap map put. No-op on blanks.
     */
    fun noteAlias(thread: String, ts: Long, messageId: String) {
        if (thread.isEmpty() || ts <= 0L || messageId.isEmpty()) return
        synchronized(lock) {
            aliasByStamp.getOrPut(stampKey(thread, ts)) { LinkedHashSet() }.add(messageId)
        }
    }

    /**
     * Apply an AUTHORITATIVE server snapshot: the complete current actor->emoji set
     * for the message at ([thread], [ts]). Replaces whatever was there — pruning
     * removed/changed reactions and superseding id-space duplicates and optimistic
     * self-reacts — so the badge matches the server. An empty [set] records "no
     * reactions now" (still authoritative). Returns whether the stored state changed.
     * Ignored when [ts] is unknown (0): the per-actor merge path covers that case.
     */
    fun applySnapshot(thread: String, ts: Long, set: Map<String, String>): Boolean {
        if (thread.isEmpty() || ts <= 0L) return false
        ensureLoaded()
        val cleaned = LinkedHashMap<String, String>()
        for ((a, e) in set) if (a.isNotEmpty() && e.isNotEmpty()) cleaned[a] = e
        val key = stampKey(thread, ts)
        val changed = synchronized(lock) {
            val existing = authByStamp[key]
            if (existing != null && existing.size == cleaned.size &&
                existing.all { cleaned[it.key] == it.value }
            ) false
            else {
                authByStamp[key] = cleaned
                true
            }
        }
        if (changed) scheduleSave()
        return changed
    }

    /**
     * Like [has], but if the id has no reactions directly, also checks any other id
     * recorded at the same (thread, second) — so a numeric-keyed stored row finds a
     * reaction that arrived under its cookie mid twin (or the reverse). An
     * authoritative snapshot for the stamp, if present, wins over everything.
     * [ts] may be s/ms/us.
     */
    fun hasStamped(messageId: String, thread: String, ts: Long): Boolean {
        ensureLoaded()
        val key = stampKey(thread, ts)
        synchronized(lock) { authByStamp[key] }?.let { return it.isNotEmpty() }
        if (has(messageId)) return true
        val aliases = synchronized(lock) {
            aliasByStamp[key]?.toList() ?: return false
        }
        for (a in aliases) if (a != messageId && has(a)) return true
        return false
    }

    /**
     * Like [summaryFor], but authoritative-aware: a server snapshot for this
     * (thread, second) wins over everything (even an empty one → no badge). Absent a
     * snapshot, tries the direct id, then any id recorded at the same stamp — the
     * cross-id-space bridge for E2EE Messenger DMs. [ts] may be s/ms/us.
     */
    fun summaryForStamped(messageId: String, thread: String, ts: Long): String {
        ensureLoaded()
        val key = stampKey(thread, ts)
        synchronized(lock) { authByStamp[key]?.values?.toList() }?.let { return summarizeEmojis(it) }
        val direct = summaryFor(messageId)
        if (direct.isNotEmpty()) return direct
        val aliases = synchronized(lock) {
            aliasByStamp[key]?.toList() ?: return ""
        }
        for (a in aliases) {
            if (a == messageId) continue
            val s = summaryFor(a)
            if (s.isNotEmpty()) return s
        }
        return ""
    }

    /**
     * DIAGNOSTIC ONLY: alias-map size + the normalized stamp computed for [ts] in
     * [thread] and whether an alias exists at that key. Reveals whether the alias is
     * populated and whether the two id-spaces land on the same second.
     */
    fun aliasDebug(thread: String, ts: Long): String = synchronized(lock) {
        val k = stampKey(thread, ts)
        "${aliasByStamp.size}|st=${toSec(ts)}|k=${aliasByStamp.containsKey(k)}"
    }

    /** Drop all stored reactions (e.g. on logout / full resync). */
    fun clear() {
        synchronized(lock) { byMessage.clear(); aliasByStamp.clear(); authByStamp.clear() }
        scheduleSave()
    }

    /**
     * DIAGNOSTIC ONLY: total keyed messages + the last-12 chars of up to [n] keys,
     * so a log line can compare the store's keys against a thread's message ids and
     * reveal a keying mismatch (reactions present but keyed to a different id form).
     */
    fun debugKeys(n: Int = 6): String = synchronized(lock) {
        val ks = byMessage.keys.take(n).joinToString(",") { it.takeLast(12) }
        "${byMessage.size}{$ks}"
    }
}
