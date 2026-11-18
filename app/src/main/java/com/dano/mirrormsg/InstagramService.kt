package com.dano.mirrormsg

import android.app.Activity
import android.content.Context
import android.content.Intent
import android.content.SharedPreferences
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
import fi.mirrormsg.igbridge.Bridge as IgBridge
import fi.mirrormsg.igbridge.EventSink as IgEventSink
import fi.mirrormsg.igbridge.Igbridge

/**
 * Native Instagram over the igbridge package of the combined AAR (instameow,
 * mautrix-meta's Instagram client, vendored). The account's top level holds
 * "Direct Messages", which drills into the chats, and the Following feed as one
 * chat ([FEED_ID]) where each post is its pictures above its caption, a reaction
 * likes it and a reply comments on it. Id "instagram" (Beeper's Instagram twin is
 * "igbeeper"); conflictGroup "instagram" keeps one of the two active.
 * Drop-out set: this file, InstagramLoginActivity.kt, its manifest entry, the
 * ServiceRegistry candidate, and fi.mirrormsg/igbridge in the gomobile bind.
 */
class InstagramService : MessagingService, ReplySource, ReactionRemover {

    override val id = ID
    override val displayName = "Instagram"
    override val conflictGroup = "instagram"
    // Instagram gradient family, as Beeper Instagram uses. All under ~140 luma.
    override val avatarPalette = listOf("#833AB4", "#C13584", "#E1306C")

    override val supportsReplyTo = true
    override val supportsReactionSend = true
    override val supportsMarkRead = true
    override val hierarchical = true

    init {
        // The watch shows the Following feed newest on top (held in memory, so every launch).
        try { Mirror.setFeed(FEED_ID) } catch (t: Throwable) { LogStore.w(TAG, "Instagram: feed mode not set: ${t.message}") }
    }

    override fun isLoggedIn(): Boolean =
        try { bridgeOrNull()?.isLoggedIn ?: false } catch (_: Throwable) { false }

    override fun selfActorKey(): String? = selfId().ifEmpty { null }

    override fun accountLabel(): String {
        if (sessionExpired) {
            return when (expiredKind) {
                "checkpoint", "consent" ->
                    "Instagram wants you to confirm it's you. Do that in the Instagram app, then log out and in here."
                "suspended" -> "Instagram reports this account as suspended."
                else -> "Signed out by Instagram. Log out, then log in again."
            }
        }
        val a = account()
        val user = a.optString("username")
        return if (user.isNotEmpty()) "@$user" else a.optString("name")
    }

    override fun connectIfPossible(): Boolean {
        val b = bridgeOrNull() ?: return false
        if (!b.isLoggedIn) return false
        replaceStaleRootCache()
        startConnect(b)
        return true
    }

    override fun disconnect() {
        stopConnect()
        try { bridge?.disconnect() } catch (_: Throwable) {}
    }

    override fun logout(context: Context) {
        stopConnect()
        try { bridgeOrNull()?.logout() } catch (e: Throwable) { LogStore.e(TAG, "Instagram logout failed", e) }
        sessionExpired = false
        expiredKind = ""
        caughtUp.clear()
        clearConvos(context)
        clearFeed(context)
        LogStore.i(TAG, "Instagram logged out")
    }

    override fun startLogin(activity: Activity) {
        activity.startActivity(Intent(activity, InstagramLoginActivity::class.java))
    }

    override fun listConversations(count: Int): String {
        ensureConvosLoaded()
        val arr = JSONArray()
        convos.values.sortedByDescending { it.optLong("_ts", 0L) }.take(count).forEach { arr.put(it) }
        return JSONObject().put("conversations", arr).toString()
    }

    // The account's top level: the chats bucket, then the Following feed.
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

    // A chat's picture (a DM: the other person's; a group: its own image), named by
    // the picture's file so the id changes with it. The top rows (Direct Messages,
    // Following) keep their letters. Cheap: from the chat's row or the stored chats;
    // for a notification that comes before its chat's row (a new chat), from what
    // the bridge holds. No network either way.
    override fun avatarId(conversationId: String, row: JSONObject?): String? {
        if (!conversationId.startsWith(DM_PREFIX)) return null
        val c = row ?: run { ensureConvosLoaded(); convos[conversationId] }
        var key = c?.optString("_avatar").orEmpty()
        if (key.isEmpty() && row == null) {
            key = try { bridgeOrNull()?.chatPictureKey(conversationId).orEmpty() } catch (t: Throwable) { "" }
        }
        return if (key.isEmpty()) null else "$AVATAR_PREFIX$conversationId|$key"
    }

    // The bytes, from the bridge (it reads a lapsed address again from the chat).
    // null: no picture; an exception: not now (the phone asks again later).
    override fun avatarBytes(avatarId: String): ByteArray? {
        if (!avatarId.startsWith(AVATAR_PREFIX)) return null
        val thread = avatarId.removePrefix(AVATAR_PREFIX).substringBeforeLast('|')
        if (!thread.startsWith(DM_PREFIX)) return null
        val b = bridgeOrNull() ?: throw IllegalStateException("Instagram is not connected")
        val bytes = b.chatPicture(thread)
        return if (bytes == null || bytes.isEmpty()) null else bytes
    }

    /**
     * Instagram serves 20 messages a page. Once a chat has caught up since the
     * session started (READY), a newest fetch reads one page: live events keep it
     * current. The first newest fetch of a chat with stored messages reads [count]
     * (bridging what arrived while disconnected); one with none reads a page, so a
     * first open is quick. Older pages always read [count].
     */
    override fun fetchMessages(conversationId: String, count: Long, cursor: String): String {
        if (conversationId == FEED_ID) return fetchFeed(count, cursor)
        val b = bridgeOrNull() ?: return EMPTY_MESSAGES
        val want = if (cursor.isEmpty()) newestCount(conversationId, count) else count
        val raw = try {
            b.fetchMessages(conversationId, want, cursor)
        } catch (e: Throwable) {
            LogStore.w(TAG, "Instagram fetchMessages failed: ${e.message}")
            return EMPTY_MESSAGES
        }
        val obj = try { JSONObject(raw) } catch (_: Throwable) { return EMPTY_MESSAGES }
        val flat = obj.optJSONArray("messages") ?: JSONArray()
        val next = obj.optString("cursor", "")
        // Nothing, yet not the end: the bridge isn't connected. Leave the paging state alone.
        if (flat.length() == 0 && next.isEmpty() && !obj.optBoolean("exhausted", false)) return EMPTY_MESSAGES
        if (cursor.isEmpty()) caughtUp.add(conversationId)
        val keepDeeper = cursor.isEmpty() && Mirror.backfillCursor.containsKey(conversationId)
        if (!keepDeeper) {
            if (next.isNotEmpty() && next != cursor) {
                Mirror.backfillCursor[conversationId] = next
                Mirror.backfillExhausted[conversationId] = false
            } else {
                Mirror.backfillExhausted[conversationId] = true
            }
        }
        val out = JSONArray()
        val reacted = ArrayList<String>()
        for (i in 0 until flat.length()) {
            val f = flat.optJSONObject(i) ?: continue
            val mid = f.optString("messageId")
            if (mid.isEmpty()) continue
            out.put(toStoreMessage(f, live = false))
            f.optJSONArray("reactions")?.let { if (applyReactions(mid, it)) reacted.add(mid) }
        }
        if (reacted.isNotEmpty() && Mirror.currentOpenConvoId == conversationId) {
            for (mid in reacted) PebbleListenerService.onLiveReaction(conversationId, mid)
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
        if (conversationId == FEED_ID) return commentOn(targetMessageId, text)
        if (isOwnEcho(targetMessageId)) return errJson("that message can't take a reply yet")
        return send(conversationId, text, targetMessageId)
    }

    // In the feed the phone sends only Like here (❤), to a post or a comment.
    override fun sendReaction(
        conversationId: String, targetMessageId: String,
        targetFromMe: Boolean, emoji: String
    ): String {
        val e = emoji.trim()
        if (conversationId == FEED_ID) return setLike(targetMessageId, e.isNotEmpty())
        if (e.isEmpty()) return errJson("Instagram reactions can't be removed this way")
        if (isOwnEcho(targetMessageId)) return errJson("that message can't take a reaction yet")
        val b = bridgeOrNull() ?: return errJson("Instagram is not connected")
        return try {
            b.sendReaction(conversationId, targetMessageId, e)
        } catch (t: Throwable) {
            LogStore.e(TAG, "Instagram reaction failed", t)
            errJson(t.message ?: "reaction failed")
        }
    }

    // A reaction sent again: in the feed an unlike, in a chat the user's reaction off.
    override fun removeReaction(
        conversationId: String, targetMessageId: String,
        targetFromMe: Boolean, emoji: String
    ): String {
        if (conversationId == FEED_ID) return setLike(targetMessageId, false)
        if (isOwnEcho(targetMessageId)) return errJson("that message can't take a reaction back yet")
        val b = bridgeOrNull() ?: return errJson("Instagram is not connected")
        return try {
            b.removeReaction(conversationId, targetMessageId, emoji.trim())
        } catch (t: Throwable) {
            LogStore.w(TAG, "Instagram reaction removal failed: ${t.message}")
            errJson(t.message ?: "reaction removal failed")
        }
    }

    // A post's room: its comments; a comment's room: the replies in its thread
    // (those open no room of their own: Instagram threads one level deep).
    override fun fetchReplies(convoId: String, messageId: String, cursor: String): String {
        if (convoId != FEED_ID) return EMPTY_MESSAGES
        val post = postIdOf(messageId)
        val ref = if (post.isEmpty()) commentRef(messageId) else null
        if (post.isEmpty() && (ref == null || ref.root != ref.id)) return EMPTY_MESSAGES
        val postId = ref?.post ?: post
        val b = bridgeOrNull() ?: return EMPTY_MESSAGES
        val raw = try {
            b.postComments(postId, ref?.id.orEmpty(), cursor)
        } catch (t: Throwable) {
            LogStore.w(TAG, "Instagram comments failed: ${t.message}")
            return EMPTY_MESSAGES
        }
        val obj = try { JSONObject(raw) } catch (_: Throwable) { null } ?: return EMPTY_MESSAGES
        val arr = obj.optJSONArray("comments") ?: JSONArray()
        val out = JSONArray()
        for (i in 0 until arr.length()) {
            val c = arr.optJSONObject(i) ?: continue
            val id = c.optString("id")
            if (id.isEmpty()) continue
            val root = ref?.id ?: id
            val m = commentMessage(c, commentId(postId, id, root))
            if (ref != null) m.remove("replyCount") // a reply in a thread opens none
            out.put(m)
            noteAuthor(id, c.optString("user"), c.optBoolean("isMe", false))
        }
        return JSONObject().put("messages", out).put("cursor", obj.optString("cursor", "")).toString()
    }

    override fun markRead(conversationId: String, messageId: String): String {
        if (conversationId == FEED_ID) return "{}"
        val b = bridgeOrNull() ?: return errJson("Instagram is not connected")
        return try {
            b.markRead(conversationId, if (isOwnEcho(messageId)) "" else messageId)
        } catch (t: Throwable) {
            LogStore.w(TAG, "Instagram markRead failed: ${t.message}")
            errJson(t.message ?: "mark read failed")
        }
    }

    override fun replyTokenFor(conversationId: String): String = ""

    // The feed is always behind when opened: show it once it's caught up.
    override fun catchUpBeforeShowing(conversationId: String): Boolean = conversationId == FEED_ID

    // A post or a comment opened on the watch: its words (the comments are in its room).
    override fun detailText(conversationId: String, messageId: String, fullText: String): String = fullText

    override fun downloadImage(
        mediaId: String, decryptionKeyB64: String,
        maxW: Int, maxH: Int, maxBytes: Int, quality: Int
    ): String {
        if (mediaId.startsWith(GIF_ID)) {
            val bytes = clipBytes(rawId(mediaId)) ?: return errJson("media download failed")
            val env = MediaUrls.stillFromBytes(Mirror.appContext, bytes, maxW, maxH, maxBytes, quality)
            return (try { JSONObject(env).optJSONObject("data")?.toString() } catch (_: Throwable) { null })
                ?: errJson("media encode failed")
        }
        val b = bridgeOrNull() ?: return errJson("Instagram is not connected")
        return try {
            b.downloadImageForWatch(rawId(mediaId), maxW.toLong(), maxH.toLong(), maxBytes.toLong(), quality.toLong())
        } catch (t: Throwable) {
            LogStore.e(TAG, "Instagram downloadImage failed", t)
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
                LogStore.w(TAG, "Instagram GIF encode failed: ${t.message}")
                ""
            }
        }
        if (mediaId.startsWith(VIDEO_ID)) return ""
        val b = bridgeOrNull() ?: return ""
        return try {
            b.downloadMediaForWatch(
                mediaId, maxW.toLong(), maxH.toLong(), maxBytes.toLong(), quality.toLong(),
                fpsFloor.toLong(), maxFrames.toLong(), gifBytes.toLong(), startRung.toLong()
            )
        } catch (t: Throwable) {
            LogStore.w(TAG, "Instagram downloadMediaForWatch failed: ${t.message}")
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
            LogStore.w(TAG, "Instagram media file failed: ${t.message}")
            -1L
        }
    }

    override fun processInlineImage(
        mediaDataB64: String,
        maxW: Int, maxH: Int, maxBytes: Int, quality: Int
    ): String = errJson("instagram has no inline images")

    private fun send(conversationId: String, text: String, replyTo: String): String {
        val b = bridgeOrNull() ?: return errJson("Instagram is not connected")
        val result = try {
            if (replyTo.isEmpty()) b.sendMessage(conversationId, text)
            else b.sendReply(conversationId, replyTo, text)
        } catch (t: Throwable) {
            LogStore.e(TAG, "Instagram send failed", t)
            return errJson(t.message ?: "send failed")
        }
        val sentId = (try { JSONObject(result).optString("messageId") } catch (_: Throwable) { "" })
            .ifEmpty { SENT_PREFIX + System.currentTimeMillis() }
        echoSent(conversationId, sentId, text, replyTo)
        return JSONObject().put("messageId", sentId).toString()
    }

    companion object {
        const val ID = "instagram"
        const val FEED_ID = "igf:following"
        private const val DM_PREFIX = "ig:"
        private const val AVATAR_PREFIX = "igpic|"
        private const val DMS = "dms"
        private const val TAG = "MirrorMsgSpike"
        private const val DATA_DIR = "igbridge"
        private const val SEAL_PREFS = "instagram_native_seal"
        private const val SEAL_KEY = "seal_key"
        private const val CONVO_FILE = "ig_convos.json"
        private const val ROOT_V2 = "root_v2"
        private const val FEED_PREFS = "instagram_native_feed"
        private const val FEED_FLOOR = "floor"
        private const val FEED_PREVIEW = "preview"
        private const val FEED_HEAD = "head"
        // Set once step 2's stored feed (a post as several messages) is gone.
        private const val FEED_POSTS = "posts_v1"
        private const val FEED_PAGE = 40L
        private const val COMMENT_TEXT_MAX = 280
        private const val COMMENT_FULL_MAX = 1000
        private const val COMMENT_AUTHORS_MAX = 2000
        // A comment's id in the feed: "igc:<post>:<comment>:<thread>", the thread
        // being the comment Instagram threads its replies under (itself for one
        // on the post).
        private const val COMMENT_ID_PREFIX = "igc:"
        // An Instagram like is a heart (the watch font draws it).
        private const val EMPTY_MESSAGES = "{\"messages\":[]}"
        private const val EMPTY_CONVERSATIONS = "{\"conversations\":[]}"
        private const val NEWEST_PAGE = 20L
        private const val VIDEO_ID = "igvid:"
        private const val GIF_ID = "iggif:"
        private const val SENT_PREFIX = "igsent-"
        private val PLACEHOLDER_NAMES = setOf("Instagram chat", "Group chat")

        private val lock = Any()
        @Volatile private var bridge: IgBridge? = null
        @Volatile private var initFailureLogged = false
        @Volatile private var sessionExpired = false
        @Volatile private var expiredKind = ""
        @Volatile private var connectGen = 0
        private var connectThread: Thread? = null
        private var connectThreadGen = -1

        private val convos = ConcurrentHashMap<String, JSONObject>()
        private val feedFetchLock = Any()   // one fetch at a time, held across the network
        private val feedStateLock = Any()   // the stored run's bookkeeping, never held across it
        private val feedGen = AtomicInteger()
        // A comment's author, for "@name" on a reply to it; "" for the user's own.
        private val commentAuthors = ConcurrentHashMap<String, String>()
        @Volatile private var convosLoaded = false
        private val recentLive = ConcurrentHashMap<String, Long>()
        private val caughtUp: MutableSet<String> = ConcurrentHashMap.newKeySet()
        private val saveQueued = AtomicBoolean(false)
        @Volatile private var refreshFuture: ScheduledFuture<*>? = null
        private val timer: ScheduledExecutorService by lazy {
            Executors.newSingleThreadScheduledExecutor { r -> Thread(r, "ig-service").apply { isDaemon = true } }
        }

        private val sink = object : IgEventSink {
            override fun onEvent(eventType: String, jsonPayload: String) {
                try {
                    when (eventType) {
                        "MESSAGE" -> onMessage(JSONObject(jsonPayload))
                        "CONVERSATION" -> onConversation(JSONObject(jsonPayload))
                        "REACTION" -> onReaction(JSONObject(jsonPayload))
                        "READY" -> {
                            sessionExpired = false
                            expiredKind = ""
                            // A new session may have missed events: each chat's next
                            // newest fetch reads deeper again.
                            caughtUp.clear()
                            LogStore.i(TAG, "Instagram ready")
                        }
                        "LOGGED_OUT" -> {
                            val o = JSONObject(jsonPayload)
                            sessionExpired = true
                            expiredKind = o.optString("kind")
                            LogStore.w(TAG, "Instagram rejected the saved session (${expiredKind.ifEmpty { "?" }}): " +
                                o.optString("reason").take(200))
                        }
                        "IGDIAG" -> LogStore.i(TAG, "Instagram: " + JSONObject(jsonPayload).optString("d"))
                        "IGLOG" -> LogStore.w(TAG, "Instagram lib: " + JSONObject(jsonPayload).optString("z"))
                    }
                } catch (t: Throwable) {
                    LogStore.e(TAG, "Instagram event $eventType failed", t)
                }
            }
        }

        fun bridgeOrNull(): IgBridge? {
            bridge?.let { return it }
            val ctx = Mirror.appContext ?: return null
            return synchronized(lock) {
                bridge ?: try {
                    Igbridge.newSealedBridge(File(ctx.filesDir, DATA_DIR).absolutePath, sealKey(ctx), sink).also { bridge = it }
                } catch (t: Throwable) {
                    if (!initFailureLogged) {
                        initFailureLogged = true
                        LogStore.e(TAG, "Instagram bridge init failed", t)
                    }
                    null
                }
            }
        }

        // The key the bridge seals the login's files with (cookies, page state): 32
        // random bytes, made once and kept in the Vault (Android's Keystore).
        private fun sealKey(ctx: Context): String {
            val prefs = ctx.getSharedPreferences(SEAL_PREFS, Context.MODE_PRIVATE)
            val have = Vault.getString(prefs, SEAL_KEY)
            if (have.length == 64) return have
            val raw = ByteArray(32).also { SecureRandom().nextBytes(it) }
            val hex = raw.joinToString("") { "%02x".format(it) }
            Vault.putString(prefs, SEAL_KEY, hex)
            return hex
        }

        fun onLoginSucceeded() {
            sessionExpired = false
            expiredKind = ""
            caughtUp.clear()
            // A new login starts with an empty feed (the bridge forgets its side too).
            Mirror.appContext?.let { clearFeed(it) }
            val b = bridgeOrNull() ?: return
            LogStore.i(TAG, "Instagram login complete, connecting")
            replaceStaleRootCache()
            startConnect(b)
        }

        private fun startConnect(b: IgBridge) {
            synchronized(lock) {
                val prev = connectThread
                if (prev != null && prev.isAlive && connectThreadGen == connectGen) return
                val gen = connectGen
                connectThreadGen = gen
                connectThread = Thread({ runConnect(b, gen, prev) }, "ig-connect").apply {
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

        private fun runConnect(b: IgBridge, gen: Int, prev: Thread?) {
            if (prev != null && prev.isAlive) {
                try { prev.join(30_000L) } catch (_: InterruptedException) { return }
            }
            var waitMs = 15_000L
            while (gen == connectGen && !sessionExpired) {
                if (!b.isLoggedIn) return
                try {
                    b.connect()
                    if (gen == connectGen) LogStore.i(TAG, "Instagram connected")
                    return
                } catch (t: Throwable) {
                    if (gen != connectGen || sessionExpired) return
                    try { b.disconnect() } catch (_: Throwable) {}
                    LogStore.w(TAG, "Instagram connect failed (${t.message}); retrying in ${waitMs / 1000}s")
                }
                try { Thread.sleep(waitMs) } catch (_: InterruptedException) { return }
                waitMs = minOf(waitMs * 2, 300_000L)
            }
        }

        private fun account(): JSONObject =
            try { JSONObject(bridgeOrNull()?.accountJSON() ?: "{}") } catch (_: Throwable) { JSONObject() }

        // Instagram's messaging id for this account: the actor its reactions carry.
        private fun selfId(): String = account().optString("fbid").takeIf { it != "0" } ?: ""

        private fun isOwnEcho(id: String): Boolean = id.startsWith(SENT_PREFIX)

        private fun rawId(mediaId: String): String = mediaId.removePrefix(VIDEO_ID).removePrefix(GIF_ID)

        private fun newestCount(conversationId: String, count: Long): Long {
            if (conversationId in caughtUp) return NEWEST_PAGE
            val ctx = Mirror.appContext ?: return NEWEST_PAGE
            return if (MessageStore.load(ctx, conversationId).length() == 0) NEWEST_PAGE else count
        }

        private fun clipBytes(id: String): ByteArray? {
            val b = bridgeOrNull() ?: return null
            return Clips.clipBytes(Mirror.appContext, id) { f ->
                try {
                    b.downloadMediaToFile(id, f.path) > 0
                } catch (t: Throwable) {
                    LogStore.w(TAG, "Instagram media download failed: ${t.message}")
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
            // Photos and videos sent together: every one a part, in order.
            val media = f.optJSONArray("media")
            var album = false
            if (media != null) {
                val parts = ArrayList<JSONObject>()
                for (i in 0 until media.length()) {
                    val item = media.optJSONObject(i) ?: continue
                    val id = item.optString("id")
                    if (id.isNotEmpty()) parts.add(mediaPart(item.optString("kind"), id, item.optString("mime"), item.optString("posterId")))
                }
                putParts(info, messageInfo, parts, f.optString("text"))
                album = parts.size > 1
            }
            val mediaId = f.optString("mediaId")
            if (mediaId.isNotEmpty()) {
                val photo = JSONObject()
                    .put("format", "IMAGE")
                    .put("mimeType", f.optString("imageMime").ifEmpty { "image/jpeg" })
                    .put("mediaID", mediaId)
                if (info.has("mediaContent")) messageInfo.put(JSONObject().put("mediaContent", photo))
                else info.put("mediaContent", photo)
            }
            val clipId = f.optString("clipId")
            if (clipId.isNotEmpty()) {
                val clip = if (f.optBoolean("clipGif", false)) {
                    JSONObject().put("format", "IMAGE").put("mimeType", "image/gif").put("mediaID", GIF_ID + clipId)
                } else {
                    if (f.optString("text").isBlank()) info.getJSONObject("messageContent").put("content", "[Video]")
                    JSONObject().put("format", "VIDEO").put("mimeType", "video/mp4").put("mediaID", VIDEO_ID + clipId)
                        .apply { f.optString("posterId").takeIf { it.isNotEmpty() }?.let { put("posterMediaID", it) } }
                }
                if (info.has("mediaContent")) messageInfo.put(JSONObject().put("mediaContent", clip))
                else info.put("mediaContent", clip)
            }
            val audioId = f.optString("audioId")
            if (audioId.isNotEmpty()) {
                val audio = JSONObject().put("format", "AUDIO")
                    .put("mimeType", f.optString("audioMime").ifEmpty { "audio/mp4" }).put("mediaID", audioId)
                if (info.has("mediaContent")) messageInfo.put(JSONObject().put("mediaContent", audio))
                else info.put("mediaContent", audio)
            }
            val m = JSONObject()
                .put("conversationID", thread)
                .put("messageID", f.optString("messageId"))
                .put("timestamp", (tsMs * 1000L).toString())
                .put("senderParticipant", JSONObject().put("isMe", isMe).put("firstName", name).put("fullName", name))
                .put("messageInfo", messageInfo)
                .put("isMe", isMe)
                .put("fromMe", isMe)
            // An album is this one message: another sent right after it stays apart.
            if (album) m.put("albumID", "igmsg:" + f.optString("messageId"))
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

        // A photo or video as a message part: the bridge's id (a video behind
        // VIDEO_ID, with its picture).
        private fun mediaPart(kind: String, id: String, mime: String, posterId: String): JSONObject =
            if (kind == "video") {
                JSONObject().put("format", "VIDEO").put("mimeType", "video/mp4").put("mediaID", VIDEO_ID + id)
                    .apply { if (posterId.isNotEmpty()) put("posterMediaID", posterId) }
            } else {
                JSONObject().put("format", "IMAGE").put("mimeType", mime.ifEmpty { "image/jpeg" }).put("mediaID", id)
            }

        // The parts in order: the first beside the words, each other one a part of
        // its own (the phone shows two or more as an album). A lone video without
        // words reads "[Video]", which the phone takes as no caption.
        private fun putParts(info: JSONObject, messageInfo: JSONArray, parts: List<JSONObject>, text: String) {
            for (p in parts) {
                if (!info.has("mediaContent")) info.put("mediaContent", p)
                else messageInfo.put(JSONObject().put("mediaContent", p))
            }
            if (parts.size == 1 && parts[0].optString("format") == "VIDEO" && text.isBlank()) {
                info.optJSONObject("messageContent")?.put("content", "[Video]")
            }
        }

        // A history page carries each message's whole reaction set: replace what's
        // stored, so a reaction removed while disconnected goes too.
        private fun applyReactions(messageId: String, arr: JSONArray): Boolean {
            val set = LinkedHashMap<String, String>()
            for (i in 0 until arr.length()) {
                val r = arr.optJSONObject(i) ?: continue
                val actor = r.optString("actor")
                val emoji = r.optString("emoji")
                if (actor.isNotEmpty() && emoji.isNotEmpty()) set[actor] = emoji
            }
            return ReactionStore.replaceAll(messageId, set)
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
                LogStore.e(TAG, "Instagram sent-message echo failed", t)
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
            val prevName = prev?.optString("name").orEmpty()
            val newName = e.optString("threadName")
            val name = if ((newName.isEmpty() || newName in PLACEHOLDER_NAMES) && prevName.isNotEmpty()) prevName
                else newName.ifEmpty { "Instagram chat" }
            // The chat's picture: its key ("" none); an event that leaves it out (the
            // chat isn't known yet) keeps the last one.
            val avatar = if (e.has("avatar")) e.optString("avatar") else prev?.optString("_avatar").orEmpty()
            val c = JSONObject()
                .put("conversationID", thread)
                .put("name", name)
                .put("type", "INSTAGRAM")
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

        private fun rootJson(): String {
            val dms = JSONObject()
                .put("conversationID", DMS)
                .put("name", "Direct Messages")
                .put("navKind", "ENTER")
                .put("navContext", DMS)
            val feed = JSONObject()
                .put("conversationID", FEED_ID)
                .put("name", "Following")
                .put("type", "INSTAGRAM")
                .put("unread", false)
                .put("latestMessage", JSONObject().put("displayContent", feedPreview()))
            return JSONObject().put("conversations", JSONArray().put(dms).put(feed)).toString()
        }

        // An older root (a July 2026 build's flat chat list, then step 1's root
        // without the feed, or the last account's feed preview) may still sit in
        // the watch's root cache (key: Mirror.convoCacheKey() as it reads with
        // Instagram active). Replace it once with this root; the flag lives with
        // the feed's prefs, so a logout clears it.
        private fun replaceStaleRootCache() {
            val ctx = Mirror.appContext ?: return
            val prefs = feedPrefs(ctx)
            if (prefs.getBoolean(ROOT_V2, false)) return
            try {
                Mirror.putCachedConvoJson(rootJson(), "$ID|0|")
                prefs.edit().putBoolean(ROOT_V2, true).apply()
            } catch (t: Throwable) {
                LogStore.w(TAG, "Instagram root list cache update failed: ${t.message}")
            }
        }

        // ---- The Following feed ------------------------------------------------

        private fun feedPrefs(ctx: Context): SharedPreferences =
            ctx.getSharedPreferences(FEED_PREFS, Context.MODE_PRIVATE)

        private fun feedPreview(): String {
            val ctx = Mirror.appContext ?: return ""
            return feedPrefs(ctx).getString(FEED_PREVIEW, "") ?: ""
        }

        /**
         * One page of the feed as store messages. The stored feed is kept one
         * unbroken run of posts so scrolling up never skips any: a newest page that
         * doesn't reach back to where the last one started (the head) replaces what
         * is stored. The cursor for the posts just below the stored run (the floor)
         * and the head persist; an older page always continues from the floor.
         * An open returns its newest page at once: when that's shorter than the
         * watch's window, the phone's core fills it in below from the floor (the
         * backfill cursor), in the background. A post is one message (its photos
         * and videos an album, its caption a row after it).
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
            val older = cursor.isNotEmpty()
            // Step 2 stored a post as several messages (its pictures, its caption): a
            // post is one message now. Those go at the next open that reaches
            // Instagram; nothing is added below them before that.
            if (older && !prefs.getBoolean(FEED_POSTS, false)) return EMPTY_MESSAGES
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
            // The run's posts and its lowest row. A page below the run can repeat a
            // post already shown further down: it stays where it is. And its rows
            // all sit below the run's lowest: one that wouldn't (newer posts
            // answered for a cursor) never lands mid-run, renumbering rows the
            // watch holds.
            val known = HashSet<String>()
            var bottom = 0L
            fun note(m: JSONObject) {
                val id = m.optString("messageID")
                if (id.isNotEmpty()) known.add(id)
                val ts = m.optString("timestamp").toLongOrNull() ?: 0L
                if (ts > 0L && (bottom == 0L || ts < bottom)) bottom = ts
            }
            fun noteRows(arr: JSONArray) {
                for (i in 0 until arr.length()) note(arr.optJSONObject(i) ?: continue)
            }
            if (older) noteRows(MessageStore.load(ctx, FEED_ID))
            // below: the run's lowest row time this page must stay under (0: none).
            fun fits(r: JSONObject?, below: Long): Boolean {
                val row = r ?: return false
                val id = row.optString("messageId")
                if (id.isEmpty() || id in known) return false
                return !(below > 0L && row.optLong("tsUs", 0L) >= below)
            }
            val ceiling = if (older) bottom else 0L
            fun newRows(arr: JSONArray?): Int {
                if (arr == null) return 0
                var n = 0
                for (i in 0 until arr.length()) if (fits(arr.optJSONObject(i), ceiling)) n++
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
                    LogStore.w(TAG, "Instagram feed failed: ${t.message}")
                    null
                } ?: break
                val obj = try { JSONObject(raw) } catch (_: Throwable) { null } ?: break
                page = obj
                pageRequest = request
                val next = obj.optString("cursor", "")
                val shown = newRows(obj.optJSONArray("messages"))
                // A page with nothing new to show (ads, suggested or repeated posts):
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
                if (!older && !prefs.getBoolean(FEED_POSTS, false)) {
                    forgetStoredFeed(ctx)
                    prefs.edit().remove(FEED_HEAD).remove(FEED_PREVIEW).putBoolean(FEED_POSTS, true).apply()
                    floor = ""
                    LogStore.i(TAG, "Instagram feed: step 2's stored posts cleared (a post is one message now)")
                }
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
                        LogStore.i(TAG, "Instagram feed: the newest posts don't reach the stored ones; starting over")
                    } else if (!storedAny || floor.isEmpty()) {
                        floor = next
                    }
                    if (newest > 0L) prefs.edit().putLong(FEED_HEAD, newest).apply()
                }
                prefs.edit().putString(FEED_FLOOR, floor).apply()
                publishFloor()
            }

            val out = JSONArray()
            val ids = ArrayList<String>()
            var newestPost: JSONObject? = null // rows come oldest first
            for (i in 0 until rows.length()) {
                val r = rows.optJSONObject(i) ?: continue
                if (!fits(r, ceiling)) continue
                out.put(feedStoreMessage(r))
                ids.add(r.optString("messageId"))
                newestPost = r
            }
            // A logout while this was read: nothing of it is kept.
            if (gen != feedGen.get()) return EMPTY_MESSAGES
            // Likes are the post's own count now: the hearts once kept as the
            // user's reaction go.
            val self = selfId()
            if (self.isNotEmpty()) for (id in ids) ReactionStore.remove(id, self)
            // The list's line: the newest post's words, else what it holds ("[3 photos]").
            if (!older) newestPost?.let { r ->
                val who = if (r.optBoolean("isMe", false)) "You" else r.optString("senderName")
                val words = r.optString("text").ifBlank { r.optString("summary") }.replace('\n', ' ')
                if (words.isNotBlank()) prefs.edit().putString(FEED_PREVIEW, clip("$who: $words", 80)).apply()
            }
            return JSONObject().put("messages", out).toString()
        }

        // A post is one message: its caption and every photo and video as parts, in
        // the post's order (the phone shows two or more as an album, the caption as
        // a row after it). Its own albumID keeps two posts by one person, side by
        // side, from becoming one album. Media ids are the bridge's: it resolves a
        // current link at download time. Also a comment's echo (words only).
        private fun feedStoreMessage(r: JSONObject): JSONObject {
            val isMe = r.optBoolean("isMe", false)
            val name = if (isMe) "" else r.optString("senderName")
            val text = r.optString("text")
            val info = JSONObject().put("messageContent", JSONObject().put("content", text))
            val messageInfo = JSONArray().put(info)
            val parts = ArrayList<JSONObject>()
            val items = r.optJSONArray("items")
            if (items != null) for (i in 0 until items.length()) {
                val item = items.optJSONObject(i) ?: continue
                val id = item.optString("mediaId")
                if (id.isNotEmpty()) parts.add(mediaPart(item.optString("kind"), id, "image/jpeg", item.optString("posterId")))
            }
            putParts(info, messageInfo, parts, text)
            val m = JSONObject()
                .put("conversationID", FEED_ID)
                .put("messageID", r.optString("messageId"))
                .put("timestamp", r.optLong("tsUs", 0L).toString())
                .put("senderParticipant", JSONObject().put("isMe", isMe).put("firstName", name).put("fullName", name))
                .put("messageInfo", messageInfo)
                .put("isMe", isMe)
                .put("fromMe", isMe)
            val postId = r.optString("postId")
            if (postId.isNotEmpty() && parts.isNotEmpty()) m.put("albumID", "igpost:$postId")
            r.optString("fullText").takeIf { it.isNotEmpty() }?.let { m.put("fullText", it) }
            if (isMe) m.put("messageStatus", JSONObject().put("status", "OUTGOING_DELIVERED"))
            putCounts(m, r)
            // Instagram shows a post's picture above its caption.
            m.put("captionBelow", true)
            return m
        }

        // A post's or a comment's likes (❤ N, purple when the user's is among them)
        // and its comments (the arrow into its room); 0 left out.
        private fun putCounts(m: JSONObject, r: JSONObject) {
            val likes = r.optLong("likeCount", 0L)
            if (likes > 0L) m.put("likeCount", likes)
            if (r.optBoolean("liked", false)) m.put("liked", true)
            val replies = r.optLong("replyCount", 0L)
            if (replies > 0L) m.put("replyCount", replies)
        }

        // A comment as a message of the feed: its words, its author, its counts.
        private fun commentMessage(c: JSONObject, messageId: String): JSONObject {
            val isMe = c.optBoolean("isMe", false)
            val name = if (isMe) "" else c.optString("user")
            val words = c.optString("text")
            val m = JSONObject()
                .put("conversationID", FEED_ID)
                .put("messageID", messageId)
                .put("timestamp", c.optLong("tsUs", 0L).toString())
                .put("senderParticipant", JSONObject().put("isMe", isMe).put("firstName", name).put("fullName", name))
                .put("messageInfo", JSONArray().put(JSONObject().put("messageContent",
                    JSONObject().put("content", clip(words, COMMENT_TEXT_MAX)))))
                .put("isMe", isMe)
                .put("fromMe", isMe)
            if (words.length > COMMENT_TEXT_MAX) m.put("fullText", clip(words, COMMENT_FULL_MAX))
            if (isMe) m.put("messageStatus", JSONObject().put("status", "OUTGOING_DELIVERED"))
            putCounts(m, c)
            return m
        }

        private class CommentRef(val post: String, val id: String, val root: String)

        private fun commentId(post: String, id: String, root: String): String =
            "$COMMENT_ID_PREFIX$post:$id:$root"

        private fun commentRef(messageId: String): CommentRef? {
            if (!messageId.startsWith(COMMENT_ID_PREFIX)) return null
            val p = messageId.removePrefix(COMMENT_ID_PREFIX).split(':')
            if (p.size != 3 || p.any { it.isEmpty() || !it.all { ch -> ch in '0'..'9' } }) return null
            return CommentRef(p[0], p[1], p[2])
        }

        private fun noteAuthor(id: String, user: String, isMe: Boolean) {
            if (commentAuthors.size > COMMENT_AUTHORS_MAX) commentAuthors.clear()
            commentAuthors[id] = if (isMe) "" else user
        }

        private fun postIdOf(messageId: String): String {
            val id = messageId.substringBefore('#')
            return if (id.isNotEmpty() && id.all { it in '0'..'9' }) id else ""
        }

        // Like or unlike a post or a comment. The phone moves the count itself.
        private fun setLike(target: String, like: Boolean): String {
            val post = postIdOf(target)
            val ref = if (post.isEmpty()) commentRef(target) else null
            if (post.isEmpty() && ref == null) return errJson("that can't take a like")
            val b = bridgeOrNull() ?: return errJson("Instagram is not connected")
            return try {
                b.setLike(ref?.post ?: post, ref?.id.orEmpty(), like)
                JSONObject().put("messageId", target).toString()
            } catch (t: Throwable) {
                LogStore.w(TAG, "Instagram ${if (like) "like" else "unlike"} failed: ${t.message}")
                errJson(t.message ?: "like failed")
            }
        }

        // A reply on the watch: a public comment on a post, or a reply in a
        // comment's thread, starting with "@<its author>" as Instagram's own does.
        private fun commentOn(target: String, text: String): String {
            val post = postIdOf(target)
            val ref = if (post.isEmpty()) commentRef(target) else null
            if (post.isEmpty() && ref == null) return errJson("that can't take a comment")
            var words = text.trim()
            if (ref != null) {
                val author = commentAuthors[ref.id].orEmpty()
                if (author.isNotEmpty() && !words.startsWith("@$author")) words = "@$author $words"
            }
            val postId = ref?.post ?: post
            val b = bridgeOrNull() ?: return errJson("Instagram is not connected")
            val result = try {
                b.commentOnPost(postId, ref?.root.orEmpty(), words)
            } catch (t: Throwable) {
                LogStore.w(TAG, "Instagram comment failed: ${t.message}")
                return errJson(t.message ?: "comment failed")
            }
            val newId = try { JSONObject(result).optString("messageId") } catch (_: Throwable) { "" }
            if (newId.isEmpty()) return errJson("Instagram didn't confirm the comment. Check the post before sending it again.")
            noteAuthor(newId, "", true)
            return JSONObject().put("messageId", commentId(postId, newId, ref?.root ?: newId)).toString()
        }

        private fun clip(s: String, max: Int): String {
            if (s.length <= max) return s
            var end = (max - 1).coerceAtLeast(0)
            if (end > 0 && Character.isHighSurrogate(s[end - 1])) end--
            return s.substring(0, end).trimEnd() + "…"
        }

        private fun clearFeed(ctx: Context) {
            synchronized(feedStateLock) {
                feedGen.incrementAndGet()
                feedPrefs(ctx).edit().clear().apply()
                forgetStoredFeed(ctx)
                commentAuthors.clear()
            }
        }

        // The stored feed and the account's hearts on its rows (by row: at a logout
        // the account's id is already gone). Under feedStateLock.
        private fun forgetStoredFeed(ctx: Context) {
            val stored = MessageStore.load(ctx, FEED_ID)
            for (i in 0 until stored.length()) {
                val id = stored.optJSONObject(i)?.optString("messageID").orEmpty()
                if (id.isNotEmpty()) ReactionStore.replaceAll(id, emptyMap())
            }
            MessageStore.clear(ctx, FEED_ID)
            Mirror.backfillCursor.remove(FEED_ID)
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
                LogStore.w(TAG, "Instagram list refresh failed: ${t.message}")
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
                    LogStore.w(TAG, "Instagram conversation cache unreadable: ${t.message}")
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
                LogStore.w(TAG, "Instagram conversation cache write failed: ${t.message}")
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

// A post's top comments as fetched for the watch's detail view.
