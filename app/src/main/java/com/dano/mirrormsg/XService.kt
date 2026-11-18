package com.dano.mirrormsg

import android.app.Activity
import android.content.Context
import android.content.Intent
import android.content.SharedPreferences
import android.util.Base64
import fi.mirrormsg.mediaroute.Mediaroute
import kotlinx.coroutines.launch
import org.json.JSONArray
import org.json.JSONObject
import java.io.File
import java.security.SecureRandom
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import fi.mirrormsg.xbridge.Bridge as XBridge
import fi.mirrormsg.xbridge.EventSink as XEventSink
import fi.mirrormsg.xbridge.Xbridge

/**
 * Native X (Twitter) over the xbridge package of the combined AAR: chats under
 * "Direct Messages", and the Following timeline as one chat ([FEED_ID]) where
 * each post is a message, Like likes it, a reply answers it and its replies
 * open as a room.
 * Id "xnative" (Beeper's X twin owns "x"); conflictGroup "x" keeps one active.
 * Drop-out set: this file, XLoginActivity.kt, its manifest entry, the
 * ServiceRegistry candidate, and fi.mirrormsg/xbridge in the gomobile bind.
 */
class XService : MessagingService, ReplySource, ReactionRemover {

    override val id = ID
    override val displayName = "X"
    override val conflictGroup = "x"
    override val avatarPalette = listOf("#000000", "#555555", "#222222", "#333333")

    override val supportsReplyTo = true
    override val supportsReactionSend = true
    override val supportsMarkRead = true
    override val hierarchical = true

    init {
        // The watch shows the Following timeline newest on top (held in memory, so every launch).
        try { Mirror.setFeed(FEED_ID) } catch (t: Throwable) { LogStore.w(TAG, "X: feed mode not set: ${t.message}") }
    }

    override fun isLoggedIn(): Boolean =
        try { bridgeOrNull()?.isLoggedIn ?: false } catch (_: Throwable) { false }

    override fun selfActorKey(): String? = selfId().ifEmpty { null }

    override fun accountLabel(): String {
        if (sessionExpired) return "Signed out by X. Log out, then log in again."
        val a = account()
        val handle = a.optString("screenName")
        return if (handle.isNotEmpty()) "@$handle" else a.optString("name")
    }

    override fun connectIfPossible(): Boolean {
        val b = bridgeOrNull() ?: return false
        if (!b.isLoggedIn) return false
        replaceFlatRootCache()
        startConnect(b)
        return true
    }

    override fun disconnect() {
        stopConnect()
        try { bridge?.disconnect() } catch (_: Throwable) {}
    }

    override fun logout(context: Context) {
        stopConnect()
        try { bridgeOrNull()?.logout() } catch (e: Throwable) { LogStore.e(TAG, "X logout failed", e) }
        sessionExpired = false
        clearConvos(context)
        clearFeed(context)
        LogStore.i(TAG, "X logged out")
    }

    override fun startLogin(activity: Activity) {
        activity.startActivity(Intent(activity, XLoginActivity::class.java))
    }

    override fun listConversations(count: Int): String {
        ensureConvosLoaded()
        val arr = JSONArray()
        convos.values.sortedByDescending { it.optLong("_ts", 0L) }.take(count).forEach { arr.put(it) }
        return JSONObject().put("conversations", arr).toString()
    }

    // The account's top level: the chats bucket, then the Following timeline.
    override fun listContext(count: Int, context: List<String>): String =
        when (context.lastOrNull()) {
            null -> rootJson()
            DMS -> listConversations(count)
            else -> EMPTY_CONVERSATIONS
        }

    override fun forceOpenNavContext(convoId: String): List<String> =
        if (convoId == FEED_ID) emptyList() else listOf(DMS)

    override fun threadLabel(convoId: String): String =
        if (convoId == FEED_ID) "$displayName\nFollowing" else displayName

    override fun fetchMessages(conversationId: String, count: Long, cursor: String): String {
        if (conversationId == FEED_ID) return fetchFeed(count, cursor)
        val b = bridgeOrNull() ?: return EMPTY_MESSAGES
        val raw = try {
            b.fetchMessages(conversationId, count, cursor)
        } catch (e: Throwable) {
            LogStore.w(TAG, "X fetchMessages failed: ${e.message}")
            return EMPTY_MESSAGES
        }
        val obj = try { JSONObject(raw) } catch (_: Throwable) { return EMPTY_MESSAGES }
        val next = obj.optString("cursor", "")
        val keepDeeper = cursor.isEmpty() && Mirror.backfillCursor.containsKey(conversationId)
        if (!keepDeeper) {
            if (next.isNotEmpty() && next != cursor) {
                Mirror.backfillCursor[conversationId] = next
                Mirror.backfillExhausted[conversationId] = false
            } else {
                Mirror.backfillExhausted[conversationId] = true
            }
        }
        val flat = obj.optJSONArray("messages") ?: JSONArray()
        val out = JSONArray()
        for (i in 0 until flat.length()) {
            val f = flat.optJSONObject(i) ?: continue
            if (f.optString("messageId").isEmpty()) continue
            out.put(toStoreMessage(f, live = false))
        }
        return JSONObject().put("messages", out).toString()
    }

    override fun sendMessage(conversationId: String, text: String, replyToken: String): String =
        if (conversationId == FEED_ID) errJson("Posting isn't supported from the watch. Open a post to reply to it.")
        else send(conversationId, text, "")

    override fun sendReplyTo(
        conversationId: String, targetMessageId: String,
        targetFromMe: Boolean, text: String
    ): String {
        if (conversationId == FEED_ID) return replyToPost(targetMessageId, text)
        if (!isSequenceId(targetMessageId)) return errJson("that message can't take a reply yet")
        return send(conversationId, text, targetMessageId)
    }

    override fun sendReaction(
        conversationId: String, targetMessageId: String,
        targetFromMe: Boolean, emoji: String
    ): String {
        val e = emoji.trim()
        if (conversationId == FEED_ID) return if (e.isEmpty()) unlikePost(targetMessageId) else likePost(targetMessageId)
        if (e.isEmpty()) return errJson("X reactions can't be removed from the watch")
        if (!isSequenceId(targetMessageId)) return errJson("that message can't take a reaction yet")
        val b = bridgeOrNull() ?: return errJson("X is not connected")
        return try {
            b.sendReaction(conversationId, targetMessageId, e)
        } catch (t: Throwable) {
            LogStore.e(TAG, "X reaction failed", t)
            errJson(t.message ?: "reaction failed")
        }
    }

    // In the feed a second Like is an unlike; in a chat it takes back the user's emoji.
    override fun removeReaction(
        conversationId: String, targetMessageId: String,
        targetFromMe: Boolean, emoji: String
    ): String {
        if (conversationId == FEED_ID) return unlikePost(targetMessageId)
        val e = emoji.trim()
        if (e.isEmpty()) return errJson("no reaction to take back")
        if (!isSequenceId(targetMessageId)) return errJson("that message can't take a reaction yet")
        val b = bridgeOrNull() ?: return errJson("X is not connected")
        return try {
            b.removeReaction(conversationId, targetMessageId, e)
        } catch (t: Throwable) {
            LogStore.e(TAG, "X reaction removal failed", t)
            errJson(t.message ?: "reaction removal failed")
        }
    }

    // A room: one page of a post's (or a reply's) direct replies, in X's order.
    override fun fetchReplies(convoId: String, messageId: String, cursor: String): String {
        if (convoId != FEED_ID) return EMPTY_MESSAGES
        val postId = postIdOf(messageId)
        if (postId.isEmpty()) {
            LogStore.w(TAG, "X replies: not a post id")
            return EMPTY_MESSAGES
        }
        val b = bridgeOrNull() ?: run {
            LogStore.w(TAG, "X replies: X is not connected")
            return EMPTY_MESSAGES
        }
        val obj = try {
            JSONObject(b.tweetReplyPage(postId, cursor))
        } catch (t: Throwable) {
            LogStore.w(TAG, "X replies failed: ${t.message}")
            return EMPTY_MESSAGES
        }
        val rows = obj.optJSONArray("messages") ?: JSONArray()
        val out = JSONArray()
        for (i in 0 until rows.length()) {
            val r = rows.optJSONObject(i) ?: continue
            if (r.optString("messageId").isEmpty()) continue
            out.put(feedStoreMessage(r))
        }
        val res = JSONObject().put("messages", out)
        obj.optString("cursor").takeIf { it.isNotEmpty() }?.let { res.put("cursor", it) }
        return res.toString()
    }

    override fun markRead(conversationId: String, messageId: String): String {
        if (conversationId == FEED_ID) return "{}"
        val b = bridgeOrNull() ?: return errJson("X is not connected")
        return try {
            b.markRead(conversationId, messageId)
        } catch (t: Throwable) {
            LogStore.w(TAG, "X markRead failed: ${t.message}")
            errJson(t.message ?: "mark read failed")
        }
    }

    override fun replyTokenFor(conversationId: String): String = ""

    // The feed is always behind when opened: show it once it's caught up.
    override fun catchUpBeforeShowing(conversationId: String): Boolean = conversationId == FEED_ID

    // A post opened on the watch: its text, then its top replies, fetched now.
    override fun detailText(conversationId: String, messageId: String, fullText: String): String {
        if (conversationId != FEED_ID) return fullText
        val postId = postIdOf(messageId)
        if (postId.isEmpty()) return fullText
        val replies = topReplies(postId) ?: return fullText
        return withReplies(fullText, replies)
    }

    override fun downloadImage(
        mediaId: String, decryptionKeyB64: String,
        maxW: Int, maxH: Int, maxBytes: Int, quality: Int
    ): String {
        if (mediaId.startsWith(POSTER_ID) || mediaId.startsWith(GIF_ID)) {
            val bytes = clipBytes(rawId(mediaId)) ?: return errJson("media download failed")
            val env = MediaUrls.stillFromBytes(Mirror.appContext, bytes, maxW, maxH, maxBytes, quality)
            return (try { JSONObject(env).optJSONObject("data")?.toString() } catch (_: Throwable) { null })
                ?: errJson("media encode failed")
        }
        val b = bridgeOrNull() ?: return errJson("X is not connected")
        return try {
            b.downloadImageForWatch(mediaId, maxW.toLong(), maxH.toLong(), maxBytes.toLong(), quality.toLong())
        } catch (t: Throwable) {
            LogStore.e(TAG, "X downloadImage failed", t)
            errJson(t.message ?: "image download failed")
        }
    }

    override fun downloadMediaForWatch(
        mediaId: String, decryptionKeyB64: String,
        maxW: Int, maxH: Int, maxBytes: Int, quality: Int,
        fpsFloor: Int, maxFrames: Int, gifBytes: Int, startRung: Int
    ): String {
        if (mediaId.startsWith(GIF_ID)) {
            val bytes = clipBytes(rawId(mediaId)) ?: return ""
            return try {
                if (Clips.looksLikeMp4(bytes)) {
                    Clips.encodeForWatch(Mirror.appContext, mediaId, bytes,
                        maxW, maxH, maxBytes, quality, fpsFloor, gifBytes, startRung)
                } else {
                    Mediaroute.encode(bytes, maxW.toLong(), maxH.toLong(), maxBytes.toLong(), quality.toLong(),
                        fpsFloor.toLong(), maxFrames.toLong(), gifBytes.toLong(), startRung.toLong())
                }
            } catch (t: Throwable) {
                LogStore.w(TAG, "X GIF encode failed: ${t.message}")
                ""
            }
        }
        if (mediaId.startsWith(POSTER_ID) || mediaId.startsWith(VIDEO_ID)) return ""
        val b = bridgeOrNull() ?: return ""
        return try {
            b.downloadMediaForWatch(
                mediaId, maxW.toLong(), maxH.toLong(), maxBytes.toLong(), quality.toLong(),
                fpsFloor.toLong(), maxFrames.toLong(), gifBytes.toLong(), startRung.toLong()
            )
        } catch (t: Throwable) {
            LogStore.w(TAG, "X downloadMediaForWatch failed: ${t.message}")
            ""
        }
    }

    override fun downloadMediaToFile(mediaId: String, decryptionKeyB64: String, path: String): Long {
        val id = rawId(mediaId)
        cachedClip(id)?.let { cached ->
            try {
                return cached.copyTo(File(path), overwrite = true).length()
            } catch (_: Throwable) {}
        }
        val b = bridgeOrNull() ?: return -1L
        return try {
            b.downloadMediaToFile(id, path)
        } catch (t: Throwable) {
            LogStore.w(TAG, "X media file failed: ${t.message}")
            -1L
        }
    }

    // A chat's picture: the other person's photo, or a group's own (encrypted) one.
    override fun avatarId(conversationId: String, row: JSONObject?): String? {
        if (conversationId == FEED_ID || conversationId == DMS) return null
        val c = row ?: run { ensureConvosLoaded(); convos[conversationId] } ?: return null
        val url = c.optString("_avatar")
        if (url.isEmpty()) return null
        return if (c.optBoolean("_group", false)) "$GROUP_AVATAR$conversationId|$url" else url
    }

    override fun avatarBytes(avatarId: String): ByteArray? {
        if (!avatarId.startsWith(GROUP_AVATAR)) return Avatars.httpGet(avatarId)
        val rest = avatarId.removePrefix(GROUP_AVATAR)
        val url = rest.substringAfter('|', "")
        if (url.isEmpty()) return null
        val b = bridgeOrNull() ?: throw IllegalStateException("X is not connected")
        return b.groupAvatar(rest.substringBefore('|'), url)?.takeIf { it.isNotEmpty() }
    }

    override fun processInlineImage(
        mediaDataB64: String,
        maxW: Int, maxH: Int, maxBytes: Int, quality: Int
    ): String = errJson("x has no inline images")

    private fun send(conversationId: String, text: String, replyTo: String): String {
        val b = bridgeOrNull() ?: return errJson("X is not connected")
        val result = try {
            if (replyTo.isEmpty()) b.sendMessage(conversationId, text)
            else b.sendReply(conversationId, replyTo, text)
        } catch (t: Throwable) {
            LogStore.e(TAG, "X send failed", t)
            return errJson(t.message ?: "send failed")
        }
        val sentId = (try { JSONObject(result).optString("messageId") } catch (_: Throwable) { "" })
            .ifEmpty { "xsent-" + System.currentTimeMillis() }
        echoSent(conversationId, sentId, text, replyTo)
        return JSONObject().put("messageId", sentId).toString()
    }

    companion object {
        const val ID = "xnative"
        const val FEED_ID = "xf:following"
        private const val DMS = "dms"
        private const val TAG = "MirrorMsgSpike"
        private const val DATA_DIR = "xbridge"
        private const val CONVO_FILE = "x_convos.json"
        private const val EMPTY_MESSAGES = "{\"messages\":[]}"
        private const val EMPTY_CONVERSATIONS = "{\"conversations\":[]}"
        private const val FEED_PREFS = "xnative_feed"
        private const val SEAL_PREFS = "xnative_seal"
        private const val SEAL_KEY = "key"
        private const val FEED_FLOOR = "floor"
        private const val FEED_PREVIEW = "preview"
        private const val FEED_HEAD = "head"
        private const val ROOT_V2 = "root_v2"
        private const val FEED_ALBUMS = "albums"
        private const val FEED_ROOMS = "rooms"
        private const val FEED_PAGE = 40L
        private const val REPLIES_SHOWN = 5L
        private const val REPLIES_TTL_MS = 3 * 60_000L
        private const val DETAIL_MAX = 900
        private const val REPLY_LINE_MAX = 160
        private const val REPLIES_MAX = 520
        private const val VIDEO_ID = "xvid:"
        private const val GIF_ID = "xgif:"
        private const val POSTER_ID = "xposter:"
        private const val GROUP_AVATAR = "xg|"
        private const val POSTER_MAX_BYTES = 20L shl 20
        private val PLACEHOLDER_NAMES = setOf("X chat", "Group chat")

        private val lock = Any()
        @Volatile private var bridge: XBridge? = null
        @Volatile private var initFailureLogged = false
        @Volatile private var sessionExpired = false
        @Volatile private var connectGen = 0
        private var connectThread: Thread? = null
        private var connectThreadGen = -1

        private val convos = ConcurrentHashMap<String, JSONObject>()
        private val feedFetchLock = Any()   // one fetch at a time, held across the network
        private val feedStateLock = Any()   // the stored run's bookkeeping, never held across it
        private val feedGen = AtomicInteger()
        private val replyCache = ConcurrentHashMap<String, Pair<Long, List<Pair<String, String>>>>()
        @Volatile private var convosLoaded = false
        private val recentLive = ConcurrentHashMap<String, Long>()
        private val saveQueued = AtomicBoolean(false)
        @Volatile private var refreshFuture: ScheduledFuture<*>? = null
        private val timer: ScheduledExecutorService by lazy {
            Executors.newSingleThreadScheduledExecutor { r -> Thread(r, "x-service").apply { isDaemon = true } }
        }

        private val sink = object : XEventSink {
            override fun onEvent(eventType: String, jsonPayload: String) {
                try {
                    when (eventType) {
                        "MESSAGE" -> onMessage(JSONObject(jsonPayload))
                        "CONVERSATION" -> onConversation(JSONObject(jsonPayload))
                        "REACTION" -> onReaction(JSONObject(jsonPayload))
                        "READY" -> {
                            sessionExpired = false
                            LogStore.i(TAG, "X ready")
                        }
                        "LOGGED_OUT" -> {
                            sessionExpired = true
                            LogStore.w(TAG, "X rejected the saved session: " +
                                JSONObject(jsonPayload).optString("reason").take(200))
                        }
                        "XDIAG" -> LogStore.i(TAG, "X: " + JSONObject(jsonPayload).optString("d"))
                        "XLOG" -> LogStore.w(TAG, "X lib: " + JSONObject(jsonPayload).optString("z"))
                    }
                } catch (t: Throwable) {
                    LogStore.e(TAG, "X event $eventType failed", t)
                }
            }
        }

        fun bridgeOrNull(): XBridge? {
            bridge?.let { return it }
            val ctx = Mirror.appContext ?: return null
            return synchronized(lock) {
                bridge ?: try {
                    Xbridge.newBridge(File(ctx.filesDir, DATA_DIR).absolutePath, sealKey(ctx), sink).also { bridge = it }
                } catch (t: Throwable) {
                    if (!initFailureLogged) {
                        initFailureLogged = true
                        LogStore.e(TAG, "X bridge init failed", t)
                    }
                    null
                }
            }
        }

        // The bridge seals the X login files with this key, kept in the Keystore vault.
        private fun sealKey(ctx: Context): String {
            val prefs = ctx.getSharedPreferences(SEAL_PREFS, Context.MODE_PRIVATE)
            Vault.getString(prefs, SEAL_KEY).let { if (it.isNotEmpty()) return it }
            if (prefs.contains(SEAL_KEY)) LogStore.w(TAG, "X: login key unreadable; log in to X again")
            val fresh = Base64.encodeToString(ByteArray(32).also { SecureRandom().nextBytes(it) }, Base64.NO_WRAP)
            Vault.putString(prefs, SEAL_KEY, fresh)
            if (Vault.getString(prefs, SEAL_KEY) == fresh) return fresh
            LogStore.w(TAG, "X: login key couldn't be kept; X logins stay unsealed")
            return ""
        }

        fun onLoginSucceeded() {
            sessionExpired = false
            val b = bridgeOrNull() ?: return
            LogStore.i(TAG, "X login complete, connecting")
            startConnect(b)
        }

        private fun startConnect(b: XBridge) {
            synchronized(lock) {
                val prev = connectThread
                if (prev != null && prev.isAlive && connectThreadGen == connectGen) return
                val gen = connectGen
                connectThreadGen = gen
                connectThread = Thread({ runConnect(b, gen, prev) }, "x-connect").apply {
                    isDaemon = true
                    start()
                }
            }
        }

        private fun stopConnect() {
            synchronized(lock) {
                connectGen++
                connectThread?.interrupt()
            }
        }

        private fun runConnect(b: XBridge, gen: Int, prev: Thread?) {
            if (prev != null && prev.isAlive) {
                try { prev.join(30_000L) } catch (_: InterruptedException) { return }
            }
            var waitMs = 15_000L
            while (gen == connectGen && !sessionExpired) {
                if (!b.isLoggedIn) return
                try {
                    b.connect()
                    if (gen == connectGen) LogStore.i(TAG, "X connected")
                    return
                } catch (t: Throwable) {
                    if (gen != connectGen || sessionExpired) return
                    try { b.disconnect() } catch (_: Throwable) {}
                    LogStore.w(TAG, "X connect failed (${t.message}); retrying in ${waitMs / 1000}s")
                }
                try { Thread.sleep(waitMs) } catch (_: InterruptedException) { return }
                waitMs = minOf(waitMs * 2, 300_000L)
            }
        }

        private fun account(): JSONObject =
            try { JSONObject(bridgeOrNull()?.accountJSON() ?: "{}") } catch (_: Throwable) { JSONObject() }

        private fun selfId(): String = account().optString("userId")

        private fun isSequenceId(s: String): Boolean = s.isNotEmpty() && s.all { it in '0'..'9' }

        private fun rawId(mediaId: String): String =
            mediaId.removePrefix(VIDEO_ID).removePrefix(GIF_ID).removePrefix(POSTER_ID)

        private fun clipBytes(id: String): ByteArray? {
            val b = bridgeOrNull() ?: return null
            return Clips.clipBytes(Mirror.appContext, id) { f ->
                try {
                    b.downloadMediaToFile(id, f.path) > 0
                } catch (t: Throwable) {
                    LogStore.w(TAG, "X media download failed: ${t.message}")
                    false
                }
            }
        }

        private fun cachedClip(id: String): File? {
            val ctx = Mirror.appContext ?: return null
            val name = java.security.MessageDigest.getInstance("SHA-1").digest(id.toByteArray())
                .joinToString("") { "%02x".format(it) } + ".mp4"
            val f = File(File(ctx.cacheDir, "clips"), name)
            return if (f.isFile && f.length() > 0) f else null
        }

        private fun onMessage(f: JSONObject) {
            val ctx = Mirror.appContext ?: return
            val thread = f.optString("threadKey")
            if (thread.isEmpty() || f.optString("messageId").isEmpty()) return
            val live = f.optBoolean("live", false)
            val msg = toStoreMessage(f, live)
            if (live) {
                recentLive[thread] = System.currentTimeMillis()
                LiveRouter.ingestLiveMessage(ID, msg.toString())
            } else {
                MessageStore.merge(ctx, thread, JSONArray().put(msg))
                if (Mirror.currentOpenConvoId == thread) PebbleListenerService.onLiveMessage(thread)
            }
        }

        private fun toStoreMessage(f: JSONObject, live: Boolean): JSONObject {
            val thread = f.optString("threadKey")
            val isMe = f.optBoolean("isMe", false)
            val tsMs = f.optLong("timestamp", 0L).takeIf { it > 0L }
                ?: if (live) System.currentTimeMillis() else 0L
            val name = if (isMe) "" else f.optString("senderName")
            val info = JSONObject().put("messageContent", JSONObject().put("content", f.optString("text")))
            val messageInfo = JSONArray().put(info)
            fun addPart(part: JSONObject) {
                if (info.has("mediaContent")) messageInfo.put(JSONObject().put("mediaContent", part))
                else info.put("mediaContent", part)
            }
            val items = f.optJSONArray("media") ?: JSONArray()
            for (i in 0 until items.length()) {
                val item = items.optJSONObject(i) ?: continue
                val id = item.optString("id")
                if (id.isEmpty()) continue
                when (item.optString("kind")) {
                    "photo" -> addPart(JSONObject().put("format", "IMAGE")
                        .put("mimeType", item.optString("mime").ifEmpty { "image/jpeg" }).put("mediaID", id))
                    "gif" -> addPart(JSONObject().put("format", "IMAGE").put("mimeType", "image/gif").put("mediaID", GIF_ID + id))
                    "video" -> {
                        if (f.optString("text").isBlank()) info.getJSONObject("messageContent").put("content", "[Video]")
                        addPart(JSONObject().put("format", "VIDEO").put("mimeType", "video/mp4").put("mediaID", VIDEO_ID + id)
                            .apply { if (item.optLong("bytes", 0L) <= POSTER_MAX_BYTES) put("posterMediaID", POSTER_ID + id) })
                    }
                }
            }
            val audioId = f.optString("audioId")
            if (audioId.isNotEmpty()) {
                addPart(JSONObject().put("format", "AUDIO")
                    .put("mimeType", f.optString("audioMime").ifEmpty { "audio/mp4" }).put("mediaID", audioId))
            }
            val m = JSONObject()
                .put("conversationID", thread)
                .put("messageID", f.optString("messageId"))
                .put("timestamp", (tsMs * 1000L).toString())
                .put("senderParticipant", JSONObject().put("isMe", isMe).put("firstName", name).put("fullName", name))
                .put("messageInfo", messageInfo)
                .put("isMe", isMe)
                .put("fromMe", isMe)
            if (isMe) m.put("messageStatus", JSONObject().put("status", "OUTGOING_DELIVERED"))
            val replyId = f.optString("replyToId")
            if (replyId.isNotEmpty() || f.optString("replyToText").isNotEmpty()) {
                m.put("replyTo", JSONObject()
                    .put("id", replyId)
                    .put("sender", f.optString("replyToSender"))
                    .put("text", f.optString("replyToText"))
                    .put("isMe", f.optBoolean("replyToIsMe", false)))
            }
            if (live) groupNameFor(thread)?.let { m.put("groupName", it) }
            return m
        }

        private fun groupNameFor(thread: String): String? {
            ensureConvosLoaded()
            val c = convos[thread] ?: return null
            if (!c.optBoolean("_group", false)) return null
            return c.optString("name").takeIf { it.isNotEmpty() }
        }

        private fun echoSent(thread: String, sentId: String, text: String, replyTo: String) {
            try {
                val f = JSONObject()
                    .put("threadKey", thread)
                    .put("messageId", sentId)
                    .put("text", text)
                    .put("timestamp", System.currentTimeMillis())
                    .put("isMe", true)
                if (replyTo.isNotEmpty()) f.put("replyToId", replyTo)
                LiveRouter.ingestLiveMessage(ID, toStoreMessage(f, live = false).toString())
                touchConvo(thread, text)
            } catch (t: Throwable) {
                LogStore.e(TAG, "X sent-message echo failed", t)
            }
        }

        private fun touchConvo(thread: String, snippet: String) {
            ensureConvosLoaded()
            val prev = convos[thread] ?: return
            val c = JSONObject(prev.toString())
                .put("_ts", System.currentTimeMillis())
                .put("latestMessage", JSONObject().put("displayContent", snippet))
            convos[thread] = c
            scheduleSave()
        }

        private fun onConversation(e: JSONObject) {
            val thread = e.optString("threadKey")
            if (thread.isEmpty()) return
            ensureConvosLoaded()
            val prev = convos[thread]
            val ts = maxOf(e.optLong("timestamp", 0L), prev?.optLong("_ts", 0L) ?: 0L)
            val snippet = e.optString("snippet").ifEmpty {
                prev?.optJSONObject("latestMessage")?.optString("displayContent") ?: ""
            }
            val avatar = if (e.has("avatarUrl")) e.optString("avatarUrl") else prev?.optString("_avatar").orEmpty()
            val prevName = prev?.optString("name").orEmpty()
            val newName = e.optString("threadName")
            val name = if ((newName.isEmpty() || newName in PLACEHOLDER_NAMES) && prevName.isNotEmpty()) prevName
                else newName.ifEmpty { "X chat" }
            val c = JSONObject()
                .put("conversationID", thread)
                .put("name", name)
                .put("type", "X")
                .put("unread", false)
                .put("_group", e.optBoolean("group", false))
                .put("_ts", ts)
                .put("latestMessage", JSONObject().put("displayContent", snippet))
            if (avatar.isNotEmpty()) c.put("_avatar", avatar)
            if (prev != null && prev.toString() == c.toString()) return
            convos[thread] = c
            scheduleSave()
            val liveAt = recentLive[thread] ?: 0L
            if (System.currentTimeMillis() - liveAt > 3_000L) scheduleListRefresh()
        }

        private fun onReaction(e: JSONObject) {
            val msg = e.optString("msg")
            val actor = e.optString("actor")
            if (msg.isEmpty() || actor.isEmpty()) return
            var changed = when (e.optString("op")) {
                "upsert" -> ReactionStore.ingest(msg, actor, e.optString("emoji"))
                "delete" -> ReactionStore.remove(msg, actor)
                else -> false
            }
            if (actor == selfId() && ReactionStore.remove(msg, "self")) changed = true
            if (!changed) return
            val thread = e.optString("thread")
            if (thread.isNotEmpty()) PebbleListenerService.onLiveReaction(thread, msg)
        }

        // ---- The Following feed ------------------------------------------------

        // Before the feed, X's top level was its chat list, and the copy of it
        // cached for the watch would be served on the next open. Replace it once
        // (the cache key is Mirror.convoCacheKey() as it reads with X active).
        private fun replaceFlatRootCache() {
            val ctx = Mirror.appContext ?: return
            val prefs = feedPrefs(ctx)
            if (prefs.getBoolean(ROOT_V2, false)) return
            try {
                Mirror.putCachedConvoJson(rootJson(), "$ID|0|")
                prefs.edit().putBoolean(ROOT_V2, true).apply()
            } catch (t: Throwable) {
                LogStore.w(TAG, "X root list cache update failed: ${t.message}")
            }
        }

        private fun rootJson(): String {
            val dms = JSONObject()
                .put("conversationID", DMS)
                .put("name", "Direct Messages")
                .put("navKind", "ENTER")
                .put("navContext", DMS)
            val feed = JSONObject()
                .put("conversationID", FEED_ID)
                .put("name", "Following")
                .put("type", "X")
                .put("unread", false)
                .put("latestMessage", JSONObject().put("displayContent", feedPreview()))
            return JSONObject().put("conversations", JSONArray().put(dms).put(feed)).toString()
        }

        private fun feedPrefs(ctx: Context): SharedPreferences =
            ctx.getSharedPreferences(FEED_PREFS, Context.MODE_PRIVATE)

        private fun feedPreview(): String {
            val ctx = Mirror.appContext ?: return ""
            return feedPrefs(ctx).getString(FEED_PREVIEW, "") ?: ""
        }

        /**
         * One page of the timeline as store messages. The stored feed is kept one
         * unbroken run of posts so scrolling up never skips any: a newest page that
         * doesn't reach back to where the last one started (the head) replaces what
         * is stored. The cursor for the posts just below the stored run (the floor)
         * and the head persist; an older page always continues from the floor.
         * One fetch at a time, so an open and a scroll-back can't interleave; a
         * logout during one discards it.
         */
        private fun fetchFeed(count: Long, cursor: String): String {
            synchronized(feedFetchLock) { return fetchFeedLocked(count, cursor) }
        }

        private fun fetchFeedLocked(count: Long, cursor: String): String {
            val ctx = Mirror.appContext ?: return EMPTY_MESSAGES
            val prefs = feedPrefs(ctx)
            val gen = feedGen.get()
            // Posts stored before albums (a row per picture) or rooms (no counts): start over once.
            if (!prefs.getBoolean(FEED_ALBUMS, false) || !prefs.getBoolean(FEED_ROOMS, false)) synchronized(feedStateLock) {
                MessageStore.clear(ctx, FEED_ID)
                prefs.edit().remove(FEED_FLOOR).remove(FEED_HEAD)
                    .putBoolean(FEED_ALBUMS, true).putBoolean(FEED_ROOMS, true).apply()
            }
            val older = cursor.isNotEmpty()
            var floor = prefs.getString(FEED_FLOOR, "") ?: ""
            fun publishFloor() {
                if (floor.isNotEmpty()) {
                    Mirror.backfillCursor[FEED_ID] = floor
                    Mirror.backfillExhausted[FEED_ID] = false
                } else {
                    Mirror.backfillCursor.remove(FEED_ID)
                    Mirror.backfillExhausted[FEED_ID] = true
                }
            }
            if (older && floor.isEmpty()) {
                publishFloor()
                return EMPTY_MESSAGES
            }
            // An older page can repeat a post already shown further down (another
            // repost of it): it stays where it is.
            val known = if (older) storedFeedIds(ctx) else emptySet()
            fun newRows(arr: JSONArray?): Int {
                if (arr == null) return 0
                var n = 0
                for (i in 0 until arr.length()) {
                    val id = arr.optJSONObject(i)?.optString("messageId").orEmpty()
                    if (id.isNotEmpty() && id !in known) n++
                }
                return n
            }
            val b = bridgeOrNull()
            var request = if (older) floor else ""
            var page: JSONObject? = null
            var pageRequest = request
            val started = System.currentTimeMillis()
            if (b != null) for (attempt in 0 until 3) {
                val raw = try {
                    b.fetchFeed(request, count.coerceIn(1L, FEED_PAGE))
                } catch (t: Throwable) {
                    LogStore.w(TAG, "X feed failed: ${t.message}")
                    null
                } ?: break
                val obj = try { JSONObject(raw) } catch (_: Throwable) { null } ?: break
                page = obj
                pageRequest = request
                val next = obj.optString("cursor", "")
                val shown = newRows(obj.optJSONArray("messages"))
                // A page with nothing new to show (ads, unreadable or repeated posts):
                // go on below it.
                if (!older || shown > 0 || next.isEmpty() || next == request) break
                if (System.currentTimeMillis() - started > 8_000L) break
                request = next
            }
            if (page == null) {
                if (gen == feedGen.get()) publishFloor()
                return EMPTY_MESSAGES
            }
            val rows = page.optJSONArray("messages") ?: JSONArray()
            val next = page.optString("cursor", "")
            synchronized(feedStateLock) {
                if (gen != feedGen.get()) return EMPTY_MESSAGES
                if (older) {
                    floor = if (next == pageRequest) "" else next
                } else if (rows.length() > 0) {
                    val head = prefs.getLong(FEED_HEAD, 0L)
                    val oldest = page.optLong("oldestMs", 0L)
                    val newest = page.optLong("newestMs", 0L)
                    val storedAny = MessageStore.load(ctx, FEED_ID).length() > 0
                    if (storedAny && !(head > 0L && oldest in 1L..head)) {
                        MessageStore.clear(ctx, FEED_ID)
                        floor = next
                        LogStore.i(TAG, "X feed: the newest posts don't reach the stored ones; starting over")
                    } else if (!storedAny || floor.isEmpty()) {
                        floor = next
                    }
                    if (newest > 0L) prefs.edit().putLong(FEED_HEAD, newest).apply()
                }
                prefs.edit().putString(FEED_FLOOR, floor).apply()
                publishFloor()
            }

            val self = selfId()
            val out = JSONArray()
            var newestText: JSONObject? = null
            for (i in 0 until rows.length()) {
                val r = rows.optJSONObject(i) ?: continue
                val id = r.optString("messageId")
                if (id.isEmpty() || id in known) continue
                out.put(feedStoreMessage(r))
                if (r.optString("text").isNotEmpty()) newestText = r
                // Likes are the post's count now, not a reaction of the user's.
                if (self.isNotEmpty()) ReactionStore.remove(id, self)
            }
            if (!older) newestText?.let { r ->
                val who = if (r.optBoolean("isMe", false)) "You" else r.optString("senderName")
                prefs.edit().putString(FEED_PREVIEW, clip("$who: ${r.optString("text")}", 80)).apply()
            }
            return JSONObject().put("messages", out).toString()
        }

        private fun storedFeedIds(ctx: Context): Set<String> {
            val arr = MessageStore.load(ctx, FEED_ID)
            val out = HashSet<String>(arr.length() * 2)
            for (i in 0 until arr.length()) arr.optJSONObject(i)?.optString("messageID")?.let { if (it.isNotEmpty()) out.add(it) }
            return out
        }

        // A post is one message (id = the post id): its words, then each of its
        // pictures and clips as a part (an album when there are several). Media are
        // public URLs the watch path downloads.
        private fun feedStoreMessage(r: JSONObject): JSONObject {
            val isMe = r.optBoolean("isMe", false)
            val name = if (isMe) "" else r.optString("senderName")
            val messageInfo = JSONArray().put(JSONObject().put("messageContent", JSONObject().put("content", r.optString("text"))))
            val media = r.optJSONArray("media") ?: JSONArray()
            for (i in 0 until media.length()) {
                val item = media.optJSONObject(i) ?: continue
                val url = item.optString("url")
                if (url.isEmpty()) continue
                val part = when (item.optString("kind")) {
                    "photo" -> JSONObject().put("format", "IMAGE").put("mimeType", "image/jpeg").put("mediaID", url)
                    "gif" -> JSONObject().put("format", "IMAGE").put("mimeType", "image/gif").put("mediaID", url)
                    "video" -> JSONObject().put("format", "VIDEO").put("mimeType", "video/mp4").put("mediaID", url)
                        .apply { item.optString("poster").takeIf { it.isNotEmpty() }?.let { put("posterMediaID", it) } }
                    else -> null
                } ?: continue
                messageInfo.put(JSONObject().put("mediaContent", part))
            }
            val m = JSONObject()
                .put("conversationID", FEED_ID)
                .put("messageID", r.optString("messageId"))
                .put("timestamp", r.optLong("tsUs", 0L).toString())
                .put("senderParticipant", JSONObject().put("isMe", isMe).put("firstName", name).put("fullName", name))
                .put("messageInfo", messageInfo)
                .put("isMe", isMe)
                .put("fromMe", isMe)
            r.optString("fullText").takeIf { it.isNotEmpty() }?.let { m.put("fullText", it) }
            if (r.has("replyCount")) m.put("replyCount", r.optLong("replyCount", 0L))
            r.optLong("likeCount", 0L).takeIf { it > 0L }?.let { m.put("likeCount", it) }
            m.put("liked", r.optBoolean("liked", false))
            if (isMe) m.put("messageStatus", JSONObject().put("status", "OUTGOING_DELIVERED"))
            val replyId = r.optString("replyToId")
            if (replyId.isNotEmpty()) {
                m.put("replyTo", JSONObject()
                    .put("id", replyId)
                    .put("sender", r.optString("replyToSender"))
                    .put("text", r.optString("replyToText"))
                    .put("isMe", r.optBoolean("replyToIsMe", false)))
            }
            return m
        }

        private fun postIdOf(messageId: String): String {
            val id = messageId.substringBefore('#')
            return if (isSequenceId(id)) id else ""
        }

        private fun likePost(target: String): String {
            val postId = postIdOf(target)
            if (postId.isEmpty()) return errJson("that post can't take a like")
            val b = bridgeOrNull() ?: return errJson("X is not connected")
            return try {
                b.likeTweet(postId)
                JSONObject().put("messageId", target).toString()
            } catch (t: Throwable) {
                LogStore.w(TAG, "X like failed: ${t.message}")
                errJson(t.message ?: "like failed")
            }
        }

        private fun unlikePost(target: String): String {
            val postId = postIdOf(target)
            if (postId.isEmpty()) return errJson("that post can't take an unlike")
            val b = bridgeOrNull() ?: return errJson("X is not connected")
            return try {
                b.unlikeTweet(postId)
                JSONObject().put("messageId", target).toString()
            } catch (t: Throwable) {
                LogStore.w(TAG, "X unlike failed: ${t.message}")
                errJson(t.message ?: "unlike failed")
            }
        }

        private fun replyToPost(target: String, text: String): String {
            val postId = postIdOf(target)
            if (postId.isEmpty()) return errJson("that post can't take a reply")
            val b = bridgeOrNull() ?: return errJson("X is not connected")
            val result = try {
                b.replyToTweet(postId, text)
            } catch (t: Throwable) {
                LogStore.w(TAG, "X reply to a post failed: ${t.message}")
                return errJson(t.message ?: "reply failed")
            }
            val sentId = (try { JSONObject(result).optString("messageId") } catch (_: Throwable) { "" })
                .ifEmpty { "xfsent-" + System.currentTimeMillis() }
            val ctx = Mirror.appContext
            if (ctx != null) {
                try {
                    val r = JSONObject()
                        .put("messageId", sentId)
                        .put("isMe", true)
                        .put("text", text)
                        .put("fullText", text)
                        .put("tsUs", System.currentTimeMillis() * 1000L)
                        .put("replyToId", postId)
                        .put("replyCount", 0L)
                    MessageStore.merge(ctx, FEED_ID, JSONArray().put(feedStoreMessage(r)))
                } catch (t: Throwable) {
                    LogStore.w(TAG, "X reply echo failed: ${t.message}")
                }
            }
            return JSONObject().put("messageId", sentId).toString()
        }

        private fun topReplies(postId: String): List<Pair<String, String>>? {
            val now = System.currentTimeMillis()
            replyCache[postId]?.let { if (now - it.first < REPLIES_TTL_MS) return it.second }
            val b = bridgeOrNull() ?: return null
            val raw = try {
                b.tweetReplies(postId, REPLIES_SHOWN)
            } catch (t: Throwable) {
                LogStore.w(TAG, "X replies failed: ${t.message}")
                return null
            }
            val arr = try { JSONObject(raw).optJSONArray("replies") } catch (_: Throwable) { null } ?: return null
            val out = ArrayList<Pair<String, String>>()
            for (i in 0 until arr.length()) {
                val o = arr.optJSONObject(i) ?: continue
                val name = o.optString("name").ifEmpty { "@" + o.optString("handle") }
                out.add(name to o.optString("text"))
            }
            if (replyCache.size > 50) replyCache.clear()
            replyCache[postId] = now to out
            return out
        }

        private fun withReplies(base: String, replies: List<Pair<String, String>>): String {
            val section = if (replies.isEmpty()) "\n\nNo replies yet." else buildString {
                append("\n\nTop replies:")
                for ((name, text) in replies) {
                    val line = "\n" + clip("$name: ${text.replace('\n', ' ')}", REPLY_LINE_MAX)
                    if (length + line.length > REPLIES_MAX) break
                    append(line)
                }
            }
            return clip(base, DETAIL_MAX - section.length) + section
        }

        private fun clip(s: String, max: Int): String {
            if (s.length <= max) return s
            var end = (max - 1).coerceAtLeast(0)
            if (end > 0 && Character.isHighSurrogate(s[end - 1])) end--
            return s.substring(0, end).trimEnd() + "\u2026"
        }

        private fun clearFeed(ctx: Context) {
            synchronized(feedStateLock) {
                feedGen.incrementAndGet()
                feedPrefs(ctx).edit().clear().apply()
                MessageStore.clear(ctx, FEED_ID)
                Mirror.backfillCursor.remove(FEED_ID)
                replyCache.clear()
            }
        }

        private fun scheduleListRefresh() {
            synchronized(lock) {
                refreshFuture?.cancel(false)
                refreshFuture = timer.schedule(Runnable { refreshWatchList() }, 1_500L, TimeUnit.MILLISECONDS)
            }
        }

        private fun refreshWatchList() {
            try {
                if (!Mirror.watchAppOpen || Mirror.activeServiceId != ID || Mirror.navStack.toList() != listOf(DMS)) return
                val svc = PebbleListenerService.instance ?: return
                Mirror.invalidateConvoList()
                svc.scope.launch { svc.refreshConversationList() }
            } catch (t: Throwable) {
                LogStore.w(TAG, "X list refresh failed: ${t.message}")
            }
        }

        private fun ensureConvosLoaded() {
            if (convosLoaded) return
            val ctx = Mirror.appContext ?: return
            synchronized(lock) {
                if (convosLoaded) return
                try {
                    val f = File(ctx.filesDir, CONVO_FILE)
                    if (f.exists()) {
                        val arr = JSONArray(f.readText())
                        for (i in 0 until arr.length()) {
                            val c = arr.optJSONObject(i) ?: continue
                            val key = c.optString("conversationID")
                            if (key.isNotEmpty()) convos.putIfAbsent(key, c)
                        }
                    }
                } catch (t: Throwable) {
                    LogStore.w(TAG, "X conversation cache unreadable: ${t.message}")
                }
                convosLoaded = true
            }
        }

        private fun scheduleSave() {
            if (!saveQueued.compareAndSet(false, true)) return
            timer.schedule(Runnable {
                saveQueued.set(false)
                writeConvos()
            }, 2_000L, TimeUnit.MILLISECONDS)
        }

        private fun writeConvos() {
            val ctx = Mirror.appContext ?: return
            try {
                val arr = JSONArray()
                convos.values.forEach { arr.put(it) }
                val dst = File(ctx.filesDir, CONVO_FILE)
                val tmp = File(ctx.filesDir, "$CONVO_FILE.tmp")
                tmp.writeText(arr.toString())
                if (!tmp.renameTo(dst)) {
                    dst.delete()
                    tmp.renameTo(dst)
                }
            } catch (t: Throwable) {
                LogStore.w(TAG, "X conversation cache write failed: ${t.message}")
            }
        }

        private fun clearConvos(ctx: Context) {
            synchronized(lock) {
                convos.clear()
                recentLive.clear()
                convosLoaded = true
                File(ctx.filesDir, CONVO_FILE).delete()
            }
        }

        private fun errJson(msg: String): String = JSONObject().put("error", msg).toString()
    }
}
