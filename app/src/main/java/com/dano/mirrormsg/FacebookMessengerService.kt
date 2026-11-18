package com.dano.mirrormsg

import android.app.Activity
import android.content.Context
import fi.mirrormsg.fbmessagebridge.Bridge as FbBridge
import fi.mirrormsg.fbmessagebridge.EventSink as FbEventSink
import fi.mirrormsg.fbmessagebridge.Fbmessagebridge

/**
 * FacebookMessengerService is the Facebook/Messenger implementation of
 * [MessagingService], wrapping the messagix/fbmessagebridge AAR.
 *
 * MODULARITY — this file and fbmessagebridge.aar are a MATCHED DROP-OUT PAIR:
 *  - This file imports fi.mirrormsg.fbmessagebridge.* (the AAR's classes), so it
 *    cannot compile unless the AAR is present. That's correct and intended: the
 *    adapter and its transport always ship together or not at all.
 *  - The CORE never imports this class. ServiceRegistry finds it only by the
 *    string "com.dano.mirrormsg.FacebookMessengerService" via reflection. So
 *    removing this file + the AAR (and the conditional gradle line that links
 *    the AAR) leaves the core fully intact — Facebook just shows as Locked.
 *  - To pull Facebook out (e.g. to avoid the AGPL obligations of messagix):
 *    delete this file + app/libs/fbmessagebridge.aar. Nothing else changes.
 *
 * STATUS — phase 1 (non-E2EE), matching bridge_fb.go. The bridge handles
 * non-encrypted Messenger chats; modern E2EE DMs are a documented phase 2 in the
 * Go bridge. Methods here are thin wrappers over that bridge, shaped to the same
 * interface the core already drives for Google.
 *
 * Discovered by [ServiceRegistry] via its no-arg constructor.
 */
open class FacebookMessengerService : MessagingService, ReactionRemover {

    override val id = "facebook"
    override val displayName = "Facebook Messenger"
    override val conflictGroup = "facebook"   // mutual-exclusion with Beeper Facebook (id "fbmessenger")

    // Facebook brand-blue family. The conversation's stable shade is picked by
    // the shared selector in MessagingService.avatarColorFor(). All under ~140
    // luma so the watch draws white initials.
    override val avatarPalette = listOf("#1877F2", "#0866FF", "#1B74E4", "#3B5998")

    // ---- Per-platform seams -------------------------------------------------
    // Instagram is native DMs over the SAME messagix bridge — one Meta backend,
    // so the entire conversation/message/reaction/image pipeline below is shared.
    // Only these members differ; InstagramService subclasses this and overrides
    // them. Kept together here so the platform surface is one obvious block.
    //
    // platform      -> the messagix platform token handed to the Go bridge.
    // prefsPrefix   -> SharedPreferences namespace: "<prefix>_session"/"_label".
    // logTag        -> LogStore tag (so IG lines read "InstagramService").
    // labelPrefix   -> the account label shown for this network ("FB: <id>").
    // cookieMarker  -> the cookie name that marks a raw harvested-cookie JSON
    //                  (vs an exported session blob) in connectIfPossible.
    // usesE2EE      -> whether the Facebook-only E2EE path may arm. Instagram
    //                  has no E2EE DMs (Meta removed the option May 2026), so it
    //                  leaves this false and no whatsmeow path ever starts.
    // loginActivityClass -> the cookie-harvest WebView Activity for this network.
    protected open val platform: String = "facebook"
    protected open val prefsPrefix: String = "fb"
    protected open val logTag: String = "FacebookMessengerService"
    protected open val labelPrefix: String = "FB"
    protected open val cookieMarker: String = "c_user"
    protected open val usesE2EE: Boolean = true
    protected open val e2eeDbName: String = "fbe2ee.db"
    protected open val loginActivityClass: Class<*> = FbLoginActivity::class.java

    private val ctx: Context get() = Mirror.appContext!!

    // One bridge instance for this network's session. Kept here (not in Mirror)
    // so the Google and Facebook bridges stay independent — Mirror.bridge is
    // Google's; this is Facebook's. The framework keeps them separate on purpose.
    @Volatile private var fbBridge: FbBridge? = null

    // The bridge a connect was started for. A second connectIfPossible() for the
    // same bridge (e.g. boot plus app start) used to start a second connect, which
    // re-ran the page load during the first one's sync and then failed with
    // "socket is already open". Cleared when a connect fails, on disconnect and on
    // logout, so a real retry still goes through.
    @Volatile private var connectStartedFor: FbBridge? = null

    // The logged-in user's fbid, captured from the READY event (and, as a fallback,
    // the first isMe message). A reaction the user sends from the watch is stored
    // optimistically under this key so a later change/removal from the Messenger app
    // — which arrives keyed to this same fbid — reconciles it. Null until known.
    @Volatile private var selfFbid: String? = null

    override fun selfActorKey(): String? = selfFbid

    // Live conversation cache. FB conversations arrive PUSH-style as CONVERSATION
    // events (unlike Google's pull-based listConversations), so we accumulate them
    // here keyed by threadKey (newest snippet/timestamp wins) and serve the watch
    // from this cache via listConversations(). LinkedHashMap preserves arrival
    // order; we re-sort by timestamp when serving.
    private val convoCache = java.util.Collections.synchronizedMap(
        LinkedHashMap<String, org.json.JSONObject>()
    )

    // Live per-thread message cache. FB messages arrive push-style as MESSAGE
    // events (the bridge's on-demand history pull is phase 2), so we accumulate
    // them here keyed by threadKey -> ordered list of messages (in the same JSON
    // shape the watch expects from Google), and serve the chat view from this
    // cache via fetchMessages(). Capped per thread to avoid unbounded growth.
    private val msgCache = java.util.Collections.synchronizedMap(
        LinkedHashMap<String, MutableList<org.json.JSONObject>>()
    )
    private val seenMsgIds = java.util.Collections.synchronizedSet(HashSet<String>())
    private val junkThreads = java.util.Collections.synchronizedSet(HashSet<String>())
    private val MSG_CACHE_PER_THREAD = 60

    // The event sink that forwards Facebook bridge events into the same handling
    // path the app uses. For now it logs + stores; wiring it into the live
    // notification/watch path is done where the core consumes service events.
    private val sink = object : FbEventSink {
        override fun onEvent(eventType: String, jsonPayload: String) {
            LogStore.i(logTag, "FB EVENT $eventType ${logSafe(jsonPayload)}")
            when (eventType) {
                "READY" -> {
                    try {
                        val o = org.json.JSONObject(jsonPayload)
                        val fbid = o.optLong("fbid", 0L)
                        if (fbid != 0L) {
                            setFbLabel("$labelPrefix: $fbid")
                            selfFbid = fbid.toString()
                        }
                    } catch (_: Exception) {}
                }
                "CONVERSATION" -> cacheConversation(jsonPayload)
                "MESSAGE" -> cacheMessage(jsonPayload)
                "UNSEND" -> handleUnsend(jsonPayload)
                "REACTION" -> handleReaction(jsonPayload)
            }
        }
    }

    // Map the Go bridge's REACTION events onto the source-agnostic ReactionStore.
    // The bridge emits per-actor "upsert" (actor + emoji; an empty emoji means the
    // actor cleared it) and "delete" (actor) for cookie threads. Other ops
    // (summary, v2upsert, deleteV2) are diagnostics and ignored here. Actor ids are
    // stringified to match the store's opaque actorKey contract.
    private fun handleReaction(jsonPayload: String) {
        try {
            val e = org.json.JSONObject(jsonPayload)
            val msg = e.optString("msg")
            if (msg.isEmpty()) return
            var changed = false
            when (e.optString("op")) {
                "upsert" -> {
                    val actor = e.opt("actor")?.toString() ?: return
                    changed = ReactionStore.ingest(msg, actor, e.optString("emoji"))
                    // If this is our own reaction, drop any optimistic "self" entry —
                    // the authoritative fbid-keyed one now represents us (also cleans
                    // legacy "self" entries left by older watch-sent reactions).
                    if (actor == selfFbid && ReactionStore.remove(msg, "self")) changed = true
                }
                "delete" -> {
                    val actor = e.opt("actor")?.toString() ?: return
                    changed = ReactionStore.remove(msg, actor)
                    if (actor == selfFbid && ReactionStore.remove(msg, "self")) changed = true
                }
                "snapshot" -> {
                    // Authoritative full set for the message: {actor:emoji}, keyed by
                    // send-second so it reconciles across the cookie/numeric id-spaces.
                    val thread = e.opt("thread")?.toString() ?: ""
                    val tsSec = e.optLong("tsSec", 0L)
                    val set = LinkedHashMap<String, String>()
                    e.optJSONObject("set")?.let { s ->
                        val it = s.keys()
                        while (it.hasNext()) {
                            val k = it.next()
                            val v = s.optString(k)
                            if (v.isNotEmpty()) set[k] = v
                        }
                    }
                    changed = ReactionStore.applySnapshot(thread, tsSec, set)
                }
                // summary / v2upsert / deleteV2 -> diagnostics, ignore
            }
            // A reaction on the thread the watch is CURRENTLY showing won't appear
            // until that thread is re-streamed. Nudge the same debounced live-thread
            // push messages already use, so an in-view reaction (or a change/removal)
            // shows on the wrist without backing out and re-entering. Gated on an
            // actual change so idle re-syncs (which re-fire every reaction) don't
            // re-push; the debounce collapses a sync's reaction burst into one push.
            if (changed) {
                val thread = e.opt("thread")?.toString()
                if (!thread.isNullOrEmpty()) PebbleListenerService.onLiveReaction(thread)
            }
        } catch (ex: Exception) {
            LogStore.e(logTag, "handleReaction failed", ex)
        }
    }

    // Translate an FB CONVERSATION event ({threadKey, threadName, snippet,
    // timestamp}) into the conversation shape the watch-push pipeline expects
    // (conversationID, name, latestMessage.displayContent, type, unread), and
    // cache it by threadKey. Newer events overwrite older for the same thread.
    private fun cacheConversation(jsonPayload: String) {
        try {
            val e = org.json.JSONObject(jsonPayload)
            val threadKey = e.opt("threadKey")?.toString() ?: return
            val name = e.optString("threadName").ifEmpty { "(no name)" }
            val snippet = e.optString("snippet")
            val ts = e.optLong("timestamp", 0L)

            // Filter junk threads so the list matches the real Messenger inbox:
            //  - Marketplace seller chats: name carries a " · <product>" separator
            //    (e.g. "Noah · Polk Audio XT70", "Oscar · bHaptics ...").
            //  - Meta-AI / bot threads: the thread's own key appears as a sender,
            //    or the name mentions Meta AI. These are the "glasses spam" rows.
            if (isJunkThread(threadKey, name, snippet)) {
                junkThreads.add(threadKey)
                return
            }

            val convo = org.json.JSONObject()
            convo.put("conversationID", threadKey)
            convo.put("name", name)
            convo.put("type", "FB")          // distinct from Google's "RCS"/"SMS"
            convo.put("unread", false)
            convo.put("_ts", ts)             // internal sort key
            convo.put("latestMessage", org.json.JSONObject().put("displayContent", snippet))
            convoCache[threadKey] = convo

            // NOTE: we no longer seed a synthetic snippet message into msgCache.
            // Real messages now arrive through emitTableDeltas (LSUpsertMessage),
            // so the seed only created duplicates (snippet + real copy of the
            // same latest message, with guessed vs. real isMe) and is removed.
            // The conversation-list snippet under the title still comes from
            // convoCache above, so the list view is unaffected.
        } catch (e: Exception) {
            LogStore.e(logTag, "cacheConversation failed", e)
        }
    }

    // Identifies threads that shouldn't appear in the watch's conversation list:
    // marketplace seller chats and Meta-AI/automated-bot threads. These come down
    // in the same sync as real chats but aren't part of the normal inbox.
    private fun isJunkThread(threadKey: String, name: String, snippet: String): Boolean {
        // Marketplace: thread names are "<Seller> · <Product listing>". The
        // " · " (space–middledot–space) separator is the reliable marker.
        if (name.contains(" \u00B7 ")) return true
        // Meta AI assistant + automated bot threads: the thread's own numeric key
        // shows up as the sender id (1:1 with a "page"/bot), or the name/snippet
        // mentions Meta AI. The glasses-ad thread is exactly this shape.
        if (name.contains("Meta AI", ignoreCase = true)) return true
        if (snippet.contains("Meta AI", ignoreCase = true)) return true
        if (snippet.contains("AI glasses", ignoreCase = true)) return true
        return false
    }

    // Stores the conversation snippet as the thread's latest cached message
    // (keyed so it won't duplicate a real MESSAGE event for the same text+ts).
    private fun seedSnippetMessage(threadKey: String, threadName: String, snippet: String, ts: Long) {
        if (snippet.isBlank()) return
        try {
            var text = snippet
            var isMe = false
            // "You: ..." => outgoing. "Name: ..." => incoming with a sender label.
            if (text.startsWith("You: ")) {
                isMe = true
                text = text.removePrefix("You: ")
            } else {
                val colon = text.indexOf(": ")
                // Only strip a leading "Sender: " if it looks like a name prefix
                // (short, no URL) — avoids mangling snippets that contain colons.
                if (colon in 1..24 && !text.substring(0, colon).contains("http")) {
                    text = text.substring(colon + 2)
                }
            }
            val senderName = if (isMe) "" else
                (threadName.takeIf { it != "(no name)" } ?: "")
            val sp = org.json.JSONObject().put("isMe", isMe).put("firstName", senderName)
            val content = org.json.JSONObject().put("content", text)
            val info = org.json.JSONObject().put("messageContent", content)
            // MessageStore dedups/keys by "messageID" and SKIPS any message with
            // an empty id — so the seed MUST carry a stable id or it never stores.
            // Synthesize one from thread+timestamp (stable, so re-seeds dedup).
            val seedId = "seed_${threadKey}_$ts"
            val msg = org.json.JSONObject()
                .put("messageID", seedId)
                .put("senderParticipant", sp)
                .put("timestamp", ts.toString())
                .put("messageInfo", org.json.JSONArray().put(info))

            synchronized(msgCache) {
                val list = msgCache.getOrPut(threadKey) { mutableListOf() }
                // Don't duplicate if we already have a message at this timestamp.
                val dupKey = msgKey(msg)
                if (list.none { msgKey(it) == dupKey }) {
                    list.add(msg)
                    while (list.size > MSG_CACHE_PER_THREAD) list.removeAt(0)
                    LogStore.i(logTag, "SEED thread=$threadKey now=${list.size} text=${text.length} chars")
                }
            }
        } catch (e: Exception) {
            LogStore.e(logTag, "seedSnippetMessage failed", e)
        }
    }

    // Translate a flat FB MESSAGE event ({threadKey, messageId, senderId, text,
    // timestamp, isMe}) into the nested message shape the watch's thread streamer
    // expects (senderParticipant.{firstName,isMe}, timestamp, messageInfo[].
    // messageContent.content) and append it to that thread's cache.
    // Maps a flat FB message ({threadKey, messageId, senderId, text, timestamp,
    // isMe}) into the nested shape the watch's thread streamer expects.
    private fun mapFlatMessage(e: org.json.JSONObject): org.json.JSONObject {
        val threadKey = e.opt("threadKey")?.toString() ?: ""
        val isMe = e.optBoolean("isMe", false)
        val text = e.optString("text")
        val ts = e.optLong("timestamp", 0L)
        // FB flat messages carry "messageId" (lowercase); the watch store keys on
        // "messageID" and skips empty ids, so map it across (fall back to
        // thread+ts so it's never empty).
        val mid = e.opt("messageId")?.toString()?.takeIf { it.isNotEmpty() && it != "null" }
            ?: "fb_${threadKey}_$ts"
        // Sender label. For incoming messages prefer the bridge-resolved sender
        // name (set per-message from the FB contact store) — this is what makes
        // GROUP messages show the actual person who sent them, not the thread
        // name. Fall back to the thread name (correct for 1:1 DMs, named for the
        // other person) when the bridge couldn't resolve a contact. Outgoing is
        // unused (isMe).
        val senderName = if (isMe) "" else
            (e.optString("senderName").takeIf { it.isNotEmpty() }
                ?: convoCache[threadKey]?.optString("name")?.takeIf { it != "(no name)" }
                ?: "")
        val sp = org.json.JSONObject().put("isMe", isMe).put("firstName", senderName)
        // An unsent message keeps its place but shows Messenger's line instead of
        // its content (the bridge sends it with no text, media or quote).
        val unsent = e.optBoolean("unsent", false)
        val shown = if (unsent) unsentLine(isMe, e.optString("senderName")) else text
        val content = org.json.JSONObject().put("content", shown)
        val info = org.json.JSONObject().put("messageContent", content)
        // An album: several pictures/videos, listed by the bridge in "media" in
        // order, each with its own id (the message id for the first, id#i after),
        // which the bridge's downloads look up. Each becomes a part of its own: the
        // first in this entry, the rest in entries of their own. A lone picture or
        // video comes as the single markers below instead.
        val parts = if (unsent) null else e.optJSONArray("media")
        val album = parts != null && parts.length() > 0
        // If the bridge tagged this message as carrying an image, add mediaContent
        // so the watch knows to request it. mediaID = the FB messageId; the bridge's
        // downloadImage looks the CDN URL up by it.
        val imageMime = if (unsent || album) "" else e.optString("imageMime", "")
        if (imageMime.isNotEmpty()) {
            val media = org.json.JSONObject()
                .put("format", "IMAGE")
                .put("mimeType", imageMime)
                .put("mediaID", mid)
            info.put("mediaContent", media)
        }
        val messageInfo = org.json.JSONArray().put(info)
        if (album && parts != null) {
            for (i in 0 until parts.length()) {
                val p = parts.optJSONObject(i) ?: continue
                val id = p.optString("id", "")
                if (id.isEmpty()) continue
                val part = org.json.JSONObject()
                if (p.optString("kind") == "video") {
                    part.put("format", "VIDEO")
                        .put("mimeType", p.optString("mime", "video/mp4"))
                        .put("mediaID", id)
                        .put("gifLike", p.optBoolean("gifLike", false))
                    val poster = p.optString("posterMediaID", "")
                    if (poster.isNotEmpty()) part.put("posterMediaID", poster)
                } else {
                    part.put("format", "IMAGE")
                        .put("mimeType", p.optString("mime", "image/jpeg"))
                        .put("mediaID", id)
                }
                if (info.has("mediaContent")) {
                    messageInfo.put(org.json.JSONObject().put("mediaContent", part))
                } else {
                    info.put("mediaContent", part)
                }
            }
        }
        // Video: mediaID = the FB messageId; the bridge's downloadMediaToFile looks
        // the video up by it. Poster is either inline bytes (E2EE) or a key the
        // bridge's image download serves (cookie threads). A message that also has
        // a photo carries the video as a second messageInfo entry.
        val videoMime = if (unsent || album) "" else e.optString("videoMime", "")
        if (videoMime.isNotEmpty()) {
            val video = org.json.JSONObject()
                .put("format", "VIDEO")
                .put("mimeType", videoMime)
                .put("mediaID", mid)
                .put("gifLike", e.optBoolean("gifLike", false))
            e.optString("posterData", "").takeIf { it.isNotEmpty() }?.let { video.put("posterData", it) }
            e.optString("posterMediaID", "").takeIf { it.isNotEmpty() }?.let { video.put("posterMediaID", it) }
            if (info.has("mediaContent")) {
                messageInfo.put(org.json.JSONObject().put("mediaContent", video))
            } else {
                info.put("mediaContent", video)
            }
        }
        // Voice message / audio file: mediaID = the FB messageId; the bridge's
        // downloadMediaToFile serves the file, from which the phone draws the voice
        // card. Only when there's no picture or video (they take its place).
        val audioMime = if (unsent || album) "" else e.optString("audioMime", "")
        if (audioMime.isNotEmpty() && !info.has("mediaContent")) {
            info.put("mediaContent", org.json.JSONObject()
                .put("format", "AUDIO")
                .put("mimeType", audioMime)
                .put("mediaID", mid))
        }
        val out = org.json.JSONObject()
            .put("messageID", mid)
            .put("senderParticipant", sp)
            .put("timestamp", ts.toString())
            .put("messageInfo", messageInfo)
        // Pictures/videos sent as separate messages of one album share its id; the
        // phone groups them by it.
        e.optString("albumID", "").takeIf { it.isNotEmpty() && !unsent }?.let { out.put("albumID", it) }
        // Quoted-reply context: the bridge attaches replyTo* when this message is
        // a reply (FB hands us the quoted sender + text directly). Carry it through
        // as a "replyTo" object; the watch-packer trims and renders it.
        val replyToText = e.optString("replyToText", "")
        val replyToSender = e.optString("replyToSender", "")
        if (!unsent && (replyToText.isNotEmpty() || e.optString("replyToId", "").isNotEmpty())) {
            out.put("replyTo", org.json.JSONObject()
                .put("sender", replyToSender)
                .put("text", replyToText)
                .put("id", e.optString("replyToId", ""))
                .put("isMe", e.optBoolean("replyToIsMe", false)))
        }
        return out
    }

    // What an unsent message reads as, in Messenger's words.
    private fun unsentLine(isMe: Boolean, name: String): String = when {
        isMe -> "You deleted a message"
        name.isNotEmpty() -> "$name deleted a message"
        else -> "Message deleted"
    }

    // Put [msg] in the thread's live cache in place of any copy with the same id.
    private fun replaceInCache(threadKey: String, msg: org.json.JSONObject) {
        val id = msg.optString("messageID")
        synchronized(msgCache) {
            val list = msgCache.getOrPut(threadKey) { mutableListOf() }
            list.removeAll { it.optString("messageID") == id }
            list.add(msg)
            while (list.size > MSG_CACHE_PER_THREAD) list.removeAt(0)
        }
    }

    // Store an unsent message's replacement and refresh the watch, silently: an
    // unsend never notifies. The store keeps a stored photo/video copy over one
    // without media (images arrive in stages), so an unsent picture stays until
    // the store can overwrite it outright; that's logged.
    private fun storeUnsent(threadKey: String, msg: org.json.JSONObject, hadMedia: Boolean) {
        val ctx = Mirror.appContext ?: return
        replaceInCache(threadKey, msg)
        MessageStore.merge(ctx, threadKey, org.json.JSONArray().put(msg))
        LogStore.i(logTag, "UNSEND ${msg.optString("messageID")} in $threadKey" +
            if (hadMedia) " (stored copy has media: kept until the store can overwrite it)" else "")
        PebbleListenerService.onLiveMessage(threadKey)
    }

    private fun hasMedia(m: org.json.JSONObject): Boolean {
        val infos = m.optJSONArray("messageInfo") ?: return false
        for (i in 0 until infos.length()) {
            if (infos.optJSONObject(i)?.optJSONObject("mediaContent")?.optString("mediaID")?.isNotEmpty() == true) return true
        }
        return false
    }

    // A message removed by id only (cookie LSDeleteMessage, or an E2EE revoke):
    // the stored copy is replaced by one that keeps its sender, time and place and
    // reads "… deleted a message". Nothing stored under that id: nothing to do.
    private fun handleUnsend(jsonPayload: String) {
        try {
            val e = org.json.JSONObject(jsonPayload)
            val threadKey = e.opt("threadKey")?.toString() ?: return
            val id = e.optString("messageId")
            if (id.isEmpty()) return
            val ctx = Mirror.appContext ?: return
            var stored: org.json.JSONObject? = null
            val arr = MessageStore.load(ctx, threadKey)
            for (i in 0 until arr.length()) {
                val m = arr.optJSONObject(i) ?: continue
                if (m.optString("messageID") == id) { stored = m; break }
            }
            if (stored == null) stored = synchronized(msgCache) {
                msgCache[threadKey]?.firstOrNull { it.optString("messageID") == id }
            }
            val found: org.json.JSONObject = stored ?: run {
                LogStore.i(logTag, "UNSEND $id in $threadKey: not stored, nothing to replace")
                return
            }
            val sp = found.optJSONObject("senderParticipant")
            val line = unsentLine(sp?.optBoolean("isMe", false) ?: false, sp?.optString("firstName") ?: "")
            val tomb = org.json.JSONObject(found.toString())
            tomb.remove("replyTo")
            tomb.put("messageInfo", org.json.JSONArray().put(
                org.json.JSONObject().put("messageContent", org.json.JSONObject().put("content", line))))
            storeUnsent(threadKey, tomb, hasMedia(found))
        } catch (ex: Exception) {
            LogStore.e(logTag, "handleUnsend failed", ex)
        }
    }

    private fun cacheMessage(jsonPayload: String) {
        try {
            val e = org.json.JSONObject(jsonPayload)
            val threadKey = e.opt("threadKey")?.toString() ?: return
            // Skip messages for threads we've filtered out (marketplace/bot spam).
            if (junkThreads.contains(threadKey)) return
            val msgId = e.opt("messageId")?.toString() ?: ""
            // Record a (thread, send-second) -> id alias so a reaction that arrives
            // keyed to this message's OTHER id-space (E2EE DMs store messages under
            // the numeric whatsmeow id but their reactions arrive under the cookie
            // mid) can still be matched at pack time. Cheap; done on first sight.
            run {
                val tsA = e.optLong("timestamp", 0L)
                if (tsA > 0L && msgId.isNotEmpty()) ReactionStore.noteAlias(threadKey, tsA, msgId)
            }
            // Fallback capture of the logged-in fbid: the first message we send
            // ourselves reveals it, in case the READY event's fbid was missed.
            if (selfFbid == null && e.optBoolean("isMe", false)) {
                val sid = e.optLong("senderId", 0L)
                if (sid != 0L) selfFbid = sid.toString()
            }
            // An unsent message replaces whatever copy is held, so it skips the
            // dedup below, and it never notifies.
            if (e.optBoolean("unsent", false)) {
                if (msgId.isNotEmpty()) seenMsgIds.add("$threadKey/$msgId")
                val ctx = Mirror.appContext ?: return
                val msg = mapFlatMessage(e)
                var hadMedia = false
                val arr = MessageStore.load(ctx, threadKey)
                for (i in 0 until arr.length()) {
                    val m = arr.optJSONObject(i) ?: continue
                    if (m.optString("messageID") == msg.optString("messageID")) { hadMedia = hasMedia(m); break }
                }
                storeUnsent(threadKey, msg, hadMedia)
                return
            }
            // Dedup: the same message can arrive on initial sync and via realtime.
            if (msgId.isNotEmpty()) {
                val key = "$threadKey/$msgId"
                if (!seenMsgIds.add(key)) return
            }
            val msg = mapFlatMessage(e)
            synchronized(msgCache) {
                val list = msgCache.getOrPut(threadKey) { mutableListOf() }
                list.add(msg)
                // Cap oldest-out to bound memory.
                while (list.size > MSG_CACHE_PER_THREAD) list.removeAt(0)
            }
            // Persist into the shared MessageStore (which the watch-push reads
            // from) and surface the message on the watch. "live" distinguishes a
            // realtime arrival from the inbox/history replay the bridge re-emits
            // on every (re)connect.
            val ctx = Mirror.appContext ?: return
            val live = e.optBoolean("live", false)
            if (live) {
                // Realtime: route through LiveRouter — the same notify entry
                // Google uses. It stores the message, pushes the thread to the
                // watch, AND raises the wrist notification, all gated by its own
                // recency/self-sent checks (so our own sent messages and any
                // stale re-delivery won't buzz). LiveRouter keys on
                // conversationID, which mapFlatMessage doesn't emit, so add it.
                msg.put("conversationID", threadKey)
                LiveRouter.ingestLiveMessage("facebook", msg.toString())
            } else {
                // Inbox/history replay: store + refresh the watch view silently so
                // a launch or reconnect doesn't notify for every active thread.
                val arr = org.json.JSONArray().put(msg)
                MessageStore.merge(ctx, threadKey, arr)
                PebbleListenerService.onLiveMessage(threadKey)
            }
        } catch (e: Exception) {
            LogStore.e(logTag, "cacheMessage failed", e)
        }
    }

    private fun prefs() = ctx.getSharedPreferences("mirrormsg", Context.MODE_PRIVATE)

    // ---- Identity & state --------------------------------------------------

    override fun isLoggedIn(): Boolean =
        (prefs().getString("${prefsPrefix}_session", "") ?: "").isNotEmpty()

    override fun accountLabel(): String =
        prefs().getString("${prefsPrefix}_label", "") ?: ""

    private fun setFbLabel(label: String) {
        prefs().edit().putString("${prefsPrefix}_label", label).apply()
    }

    // ---- Lifecycle ---------------------------------------------------------

    /**
     * Arms the Milestone-0 Facebook E2EE de-risk spike. The bridge auto-attempts
     * RegisterE2EE + an E2EE socket connect once its messagix socket reaches
     * Ready (the only moment the crypto-auth-token and our fbid are both
     * available), persisting the whatsmeow device in app-private fbe2ee.db.
     * Registration happens ONCE — subsequent connects reuse the saved device.
     * All progress is emitted as "FBE2EE" log lines; a received family-DM message
     * logs chat/sender/timestamp only (never content). This is proof-of-life
     * instrumentation, not wired to the watch.
     */
    // Kill-switch for the Facebook E2EE socket. Set false to run native FB with
    // NO E2EE socket — used to A/B whether E2EE contributes to code-24 drops on
    // network flaps versus messagix tripping 24 against its own ghost connection.
    // Pure Kotlin: flipping this needs only installDebug, no AAR rebuild.
    private val e2eeEnabled = true

    private fun armE2EE(b: FbBridge) {
        if (!usesE2EE) return   // Instagram has no E2EE DMs — never arm.
        if (!e2eeEnabled) {
            LogStore.i(logTag, "FB E2EE disabled (kill-switch) — baseline run, no E2EE socket")
            return
        }
        try {
            val path = java.io.File(ctx.filesDir, e2eeDbName).absolutePath
            b.setE2EEStorePath(path)
            LogStore.i(logTag, "FB E2EE spike armed -> $path")
        } catch (e: Throwable) {
            LogStore.e(logTag, "armE2EE failed: ${e.message}", e)
        }
    }

    override fun connectIfPossible(): Boolean {
        val saved = Vault.getString(prefs(), "${prefsPrefix}_session")
        if (saved.isEmpty()) return false
        return try {
            val b = fbBridge ?: run {
                val looksLikeCookies = saved.contains("\"$cookieMarker\"")
                val nb = if (looksLikeCookies) {
                    Fbmessagebridge.newBridgeForPlatform("", platform, sink).also { it.setCookies(saved) }
                } else {
                    Fbmessagebridge.newBridgeForPlatform(saved, platform, sink)
                }
                fbBridge = nb
                nb
            }
            if (!b.isLoggedIn) {
                LogStore.i(logTag, "FB restore: bridge reports not logged in")
                return false
            }
            if (connectStartedFor === b) {
                LogStore.i(logTag, "FB connect already started for this session; not starting another")
                return true
            }
            connectStartedFor = b
            // connect() opens the realtime socket and BLOCKS running the read
            // loop, so it must run off this thread (same as completeCookieLogin).
            // Events arrive via the sink; errors surface there + in LogStore.
            Thread {
                try {
                    armE2EE(b)
                    b.connect()
                    val session = b.exportSession()
                    if (!session.isNullOrEmpty()) {
                        Vault.putString(prefs(), "${prefsPrefix}_session", session)
                        LogStore.i(logTag, "FB session re-exported after restore connect")
                    }
                } catch (e: Throwable) {
                    if (connectStartedFor === b) connectStartedFor = null
                    LogStore.e(logTag, "FB restore connect (bg) failed: ${e.message}", e)
                }
            }.start()
            true
        } catch (e: Throwable) {
            LogStore.e(logTag, "FB connectIfPossible failed: ${e.message}", e)
            fbBridge = null
            false
        }
    }

    override fun disconnect() {
        connectStartedFor = null
        try { fbBridge?.disconnect() } catch (_: Exception) {}
    }

    override fun logout(context: Context) {
        context.getSharedPreferences("mirrormsg", Context.MODE_PRIVATE).edit()
            .remove("${prefsPrefix}_session").remove("${prefsPrefix}_label").apply()
        connectStartedFor = null
        try { fbBridge?.disconnect() } catch (_: Exception) {}
        fbBridge = null
    }

    // ---- Login -------------------------------------------------------------

    override fun startLogin(activity: Activity) {
        // Facebook login = harvest messenger.com/facebook.com cookies in a
        // WebView, then hand them to the bridge. Launch the harvest Activity;
        // it calls completeCookieLogin() below once it has the cookies.
        LogStore.i(logTag, "startLogin: launching FB cookie harvest")
        activity.startActivity(android.content.Intent(activity, loginActivityClass))
    }

    /**
     * Completes login from harvested cookies (called by FbLoginActivity). Runs
     * the exact bridge sequence: newBridge -> setCookies -> exportSession ->
     * persist -> connect. Returns null on success, or an error string the login
     * UI can surface. Kept here so all FB session state lives in one place.
     */
    fun completeCookieLogin(cookiesJson: String): String? {
        return try {
            val b = fbBridge ?: Fbmessagebridge.newBridgeForPlatform("", platform, sink).also { fbBridge = it }
            // setCookies validates required cookies (xs, c_user, datr) and throws
            // if any are missing. If we get past this line, the harvest was good.
            b.setCookies(cookiesJson)
            // Persist the harvested cookies immediately as the session seed, so a
            // restart can re-load them. (exportSession/DumpState is empty until the
            // client has connected + synced, so we do NOT gate login on it here —
            // that was the "empty session after setCookies" false failure.)
            Vault.putString(prefs(), "${prefsPrefix}_session", cookiesJson)
            // connect() opens the realtime socket and can block, so run it off the
            // caller's (UI) thread. After it connects, export the REAL session
            // state and upgrade the saved seed to it. Errors surface via the sink
            // (SOCKET_ERROR / LOGGED_OUT) and LogStore.
            connectStartedFor = b
            Thread {
                try {
                    armE2EE(b)
                    b.connect()
                    val session = b.exportSession()
                    if (!session.isNullOrEmpty()) {
                        Vault.putString(prefs(), "${prefsPrefix}_session", session)
                        LogStore.i(logTag, "FB session exported + saved after connect")
                    }
                } catch (e: Throwable) {
                    if (connectStartedFor === b) connectStartedFor = null
                    LogStore.e(logTag, "FB connect (post-login) failed", e)
                }
            }.start()
            null
        } catch (e: Throwable) {
            LogStore.e(logTag, "completeCookieLogin failed", e)
            fbBridge = null
            e.message ?: "fb login failed"
        }
    }

    // ---- Universal data surface --------------------------------------------

    override fun listConversations(count: Int): String =
        try {
            // FB conversations arrive push-style (CONVERSATION events -> convoCache),
            // so we serve the watch from the cache rather than a pull call. Sort by
            // timestamp desc and cap at count.
            val all = synchronized(convoCache) { convoCache.values.toList() }
            val sorted = all.sortedByDescending { it.optLong("_ts", 0L) }
            val arr = org.json.JSONArray()
            for (c in sorted.take(count)) arr.put(c)
            org.json.JSONObject().put("conversations", arr).toString()
        } catch (e: Throwable) { errJson(e) }

    override fun fetchMessages(conversationId: String, count: Long, cursor: String): String =
        try {
            // Real history pull from the bridge (ExecuteTasks/FetchMessagesTask),
            // mapped into the watch's nested shape, MERGED with any live messages
            // already cached for this thread (deduped), sorted oldest-first.
            val merged = LinkedHashMap<String, org.json.JSONObject>()

            // 1) Live cache (messages that streamed in since connect).
            synchronized(msgCache) { msgCache[conversationId]?.toList() ?: emptyList() }
                .forEach { m -> merged[msgKey(m)] = m }

            // 2) Bridge history fetch (blocking socket call). Flat rows -> nested.
            if (fbBridge == null) {
                LogStore.w(logTag, "FETCH: fbBridge is NULL for $conversationId (cache=${merged.size})")
            }
            val raw = fbBridge?.fetchMessages(conversationId, count, cursor)
            // CURSOR-DIAG (temporary): log the raw response's TOP-LEVEL SHAPE — key
            // names, and the value of any key that ISN'T the bulk "messages" array — so
            // we can read FB's pagination cursor field (if any) off a live fetch, the
            // same way we did for Google. Remove once FB backfill is wired.
            if (!raw.isNullOrEmpty()) try {
                val o = org.json.JSONObject(raw)
                val sb = StringBuilder("FB-CURSOR-DIAG keys=[")
                val it = o.keys()
                while (it.hasNext()) {
                    val k = it.next()
                    sb.append(k)
                    if (k != "messages") {
                        val vs = o.get(k).toString()
                        sb.append("=").append(if (vs.length > 160) vs.take(160) + "…" else vs)
                    }
                    if (it.hasNext()) sb.append(", ")
                }
                sb.append("]")
                LogStore.i(logTag, sb.toString())
            } catch (_: Exception) { /* diag only */ }
            LogStore.i(logTag,
                "FETCH convo=$conversationId bridge=${if (fbBridge==null) "null" else "ok"} " +
                "rawLen=${raw?.length ?: -1} cache=${merged.size}")
            if (!raw.isNullOrEmpty()) {
                val obj = org.json.JSONObject(raw)
                // Capture the bridge's backfill cursor + exhaustion so the generic
                // backfill in PebbleListenerService can page older history (it keys
                // off Mirror.backfillCursor) and stop cleanly at the true top of the
                // thread. FB's cursor shape is {referenceMessageId, referenceTimestampMs};
                // the bridge takes it back verbatim as the next-page anchor. Same seam
                // Google uses — only the cursor's internal fields differ.
                val cur = obj.optJSONObject("cursor")
                if (cur != null && cur.optString("referenceMessageId").isNotEmpty()) {
                    Mirror.backfillCursor[conversationId] = cur.toString()
                    Mirror.backfillExhausted[conversationId] = false
                } else {
                    // No cursor -> either the top of the thread, or a thread we can't
                    // page (E2EE / no anchor). Honor the bridge's explicit exhausted flag.
                    Mirror.backfillExhausted[conversationId] = obj.optBoolean("exhausted", false)
                }
                val arr = obj.optJSONArray("messages") ?: org.json.JSONArray()
                for (i in 0 until arr.length()) {
                    val flat = arr.getJSONObject(i)
                    // Cache+dedup so future opens are instant and live events don't double up.
                    val mid = flat.opt("messageId")?.toString() ?: ""
                    if (mid.isNotEmpty()) seenMsgIds.add("$conversationId/$mid")
                    val mapped = mapFlatMessage(flat)
                    // An unsent copy replaces the original, which has the same id
                    // but a different text key.
                    if (flat.optBoolean("unsent", false)) {
                        val uid = mapped.optString("messageID")
                        merged.values.removeAll { it.optString("messageID") == uid }
                    }
                    merged[msgKey(mapped)] = mapped
                }
            }

            val sorted = merged.values.sortedBy { it.optString("timestamp").toLongOrNull() ?: 0L }
            // Refresh the thread cache with the merged set so subsequent opens are instant.
            synchronized(msgCache) { msgCache[conversationId] = sorted.toMutableList() }
            val out = org.json.JSONArray()
            for (m in sorted) out.put(m)
            org.json.JSONObject().put("messages", out).toString()
        } catch (e: Throwable) { errJson(e) }

    // Stable per-message key for dedup/merge: timestamp + first 12 chars of text.
    private fun msgKey(m: org.json.JSONObject): String {
        val ts = m.optString("timestamp")
        val txt = m.optJSONArray("messageInfo")?.optJSONObject(0)
            ?.optJSONObject("messageContent")?.optString("content")?.take(12) ?: ""
        val me = m.optJSONObject("senderParticipant")?.optBoolean("isMe", false) ?: false
        return "$ts|$me|$txt"
    }

    override fun sendMessage(conversationId: String, text: String, replyToken: String): String {
        // Facebook needs no SIM routing, so replyToken is ignored. The FB bridge
        // send is a simple 2-arg (threadKey, text); a hard failure throws.
        val result = try {
            fbBridge?.sendMessage(conversationId, text) ?: return errStr("no bridge")
        } catch (e: Throwable) {
            return errJson(e)
        }
        optimisticInsertSent(conversationId, result, text)
        return result
    }

    // Optimistically surface a message we just sent. whatsmeow does NOT reliably
    // loop our OWN E2EE sends back as an event, so the store never learns of them
    // and the watch keeps showing only history. Insert through LiveRouter — the
    // exact path a received message takes: it stores the message, pushes the thread
    // to the watch, and (because isMe) won't raise a notification. We use the real
    // messageId from the send response so that if a late echo ever does arrive, the
    // store dedups on the id instead of showing the message twice.
    private fun optimisticInsertSent(conversationId: String, result: String, text: String, replyToId: String = "") {
        try {
            val sentId = org.json.JSONObject(result).optString("messageId")
            if (sentId.isNotEmpty()) {
                val flat = org.json.JSONObject()
                    .put("threadKey", conversationId)
                    .put("messageId", sentId)
                    .put("text", text)
                    .put("timestamp", System.currentTimeMillis())
                    .put("isMe", true)
                // For a reply, tag the quoted message id; the watch-packer resolves
                // the quoted sender/text from the stored thread, so our own reply
                // shows "You replied to …" without us re-sending that context.
                if (replyToId.isNotEmpty()) flat.put("replyToId", replyToId)
                val mapped = mapFlatMessage(flat).put("conversationID", conversationId)
                LiveRouter.ingestLiveMessage("facebook", mapped.toString())
            }
        } catch (e: Throwable) {
            LogStore.e(logTag, "optimistic echo of sent message failed", e)
        }
    }

    // ---- Targeted send (opt-in): FB supports both on E2EE threads -----------
    // These work on encrypted (Messenger-over-WhatsApp) threads — e.g. a 1:1
    // chat. On cookie threads the bridge returns a phase-2 error and the caller
    // falls back to a normal send (which also can't write cookie threads yet, so
    // no regression).

    override val supportsReplyTo: Boolean get() = true
    override val supportsReactionSend: Boolean get() = true

    // Messenger supports read receipts (via a watermark timestamp), so opt into
    // the mark-read seam — drives both the notification action and browse-to-read.
    override val supportsMarkRead: Boolean get() = true

    override fun sendReplyTo(
        conversationId: String, targetMessageId: String,
        targetFromMe: Boolean, text: String
    ): String {
        val result = try {
            fbBridge?.sendReply(conversationId, targetMessageId, targetFromMe, text) ?: return errStr("no bridge")
        } catch (e: Throwable) {
            return errJson(e)
        }
        // Surface our reply like any sent message, tagging the quoted message so
        // the watch shows "You replied to …". The quote linkage itself is FB-side.
        optimisticInsertSent(conversationId, result, text, targetMessageId)
        return result
    }

    override fun sendReaction(
        conversationId: String, targetMessageId: String,
        targetFromMe: Boolean, emoji: String
    ): String {
        // Our own reaction echoes back through the ReactionMessage capture path
        // (actor = us), so the badge updates on its own — no optimistic insert.
        return try {
            fbBridge?.sendReaction(conversationId, targetMessageId, targetFromMe, emoji)
                ?: errStr("no bridge")
        } catch (e: Throwable) {
            errJson(e)
        }
    }

    // The same emoji sent again from the watch takes it back. Messenger removes
    // the user's reaction with an empty one, on both paths (cookie task and
    // encrypted), as upstream does; the emoji itself isn't needed. The removal
    // echoes back as a REACTION delete, which finds nothing left to drop.
    override fun removeReaction(
        conversationId: String, targetMessageId: String,
        targetFromMe: Boolean, emoji: String
    ): String = sendReaction(conversationId, targetMessageId, targetFromMe, "")

    // Mark the whole thread read on Facebook. Messenger marks read by a watermark
    // timestamp, not a message id, so the bridge ignores messageId and uses the
    // thread's newest activity — the caller's "mark up through this" intent maps to
    // "mark the thread read" here. Same shape as sendReaction.
    override fun markRead(conversationId: String, messageId: String): String {
        return try {
            fbBridge?.markThreadRead(conversationId, messageId)
                ?: errStr("no bridge")
        } catch (e: Throwable) {
            errJson(e)
        }
    }

    override fun replyTokenFor(conversationId: String): String = ""  // FB has no routing

    override fun downloadImage(
        mediaId: String, decryptionKeyB64: String,
        maxW: Int, maxH: Int, maxBytes: Int, quality: Int
    ): String =
        try {
            // FB ignores decryptionKeyB64 — the bridge holds the CDN URL keyed by
            // the FB messageId (= mediaId) and downloads it directly.
            fbBridge?.downloadImageForWatch(
                mediaId, maxW.toLong(), maxH.toLong(), maxBytes.toLong(), quality.toLong()
            ) ?: errStr("fb bridge unavailable")
        } catch (e: Throwable) {
            LogStore.e(logTag, "fb downloadImage failed", e)
            errJson(e)
        }

    // FB images are never inline — the message references a CDN URL, so they always
    // go through downloadImage above. (Kept for the interface.)
    override fun processInlineImage(
        mediaDataB64: String,
        maxW: Int, maxH: Int, maxBytes: Int, quality: Int
    ): String = errStr("fb has no inline images")

    // ---- Unified media (still OR animation, decided from the bytes) ---------
    // Same two fetch paths downloadImage uses (E2EE decrypt, or signed CDN URL);
    // the bridge hands the bytes to mediaroute instead of straight to the still
    // encoder, so an animated GIF comes back as frames. Nothing had to be
    // unblocked upstream — FB already treats AttachmentTypeAnimatedImage as an
    // image, so those bytes were always arriving, just being encoded flat.
    //
    // Returns "" on any failure: the caller reads empty as "fall back to the
    // still path", which is the right outcome for every problem here.
    override fun downloadMediaForWatch(
        mediaId: String, decryptionKeyB64: String,
        maxW: Int, maxH: Int, maxBytes: Int, quality: Int,
        fpsFloor: Int, maxFrames: Int, gifBytes: Int, startRung: Int
    ): String =
        try {
            fbBridge?.downloadMediaForWatch(
                mediaId, maxW.toLong(), maxH.toLong(), maxBytes.toLong(), quality.toLong(),
                fpsFloor.toLong(), maxFrames.toLong(), gifBytes.toLong(), startRung.toLong()
            ) ?: ""
        } catch (e: Throwable) {
            LogStore.w(logTag, "fb downloadMediaForWatch failed: ${e.message}")
            ""
        }

    // mediaId is the FB messageId; the bridge finds the video or voice message
    // (E2EE transport or cookie playable URL) by it and writes the whole playable
    // file to path.
    override fun downloadMediaToFile(mediaId: String, decryptionKeyB64: String, path: String): Long =
        try {
            fbBridge?.downloadMediaToFile(mediaId, path) ?: -1L
        } catch (e: Throwable) {
            LogStore.e(logTag, "fb downloadMediaToFile failed", e)
            -1L
        }

    // No inline media on FB, so no inline animation path either.
    override fun processInlineMediaForWatch(
        mediaDataB64: String,
        maxW: Int, maxH: Int, maxBytes: Int, quality: Int,
        fpsFloor: Int, maxFrames: Int, gifBytes: Int, startRung: Int
    ): String = ""

    // ---- Chat photos --------------------------------------------------------
    // A 1:1 chat (encrypted ones too) shows the other person's profile picture;
    // a group, its own picture if it was given one (else its letter stays). The
    // bridge harvests the addresses from every sync. The id is
    // "fb|<threadKey>|<the picture's file name>", so it changes when the picture
    // does; the address itself is signed and lapses, so the bridge keeps the
    // current one and avatarBytes fetches through it. The list (row) and
    // notification (null) cases both just read what the bridge already holds:
    // no network here.
    override fun avatarId(conversationId: String, row: org.json.JSONObject?): String? {
        val key = try {
            fbBridge?.avatarKey(conversationId)
        } catch (_: Throwable) {
            null
        }
        return if (key.isNullOrEmpty()) null else "fb|$key"
    }

    // Null only when there's no picture to have (gone, unusable, or the person
    // has none); anything that may work later throws (not connected, timeout,
    // an error page, an address that lapsed), so the phone tries again in 2
    // minutes instead of showing the letter for a day. The bridge's errors never
    // carry the address.
    override fun avatarBytes(avatarId: String): ByteArray? {
        if (!avatarId.startsWith("fb|")) return null
        val threadKey = avatarId.removePrefix("fb|").substringBefore('|')
        val b = fbBridge ?: throw IllegalStateException("Messenger isn't connected")
        return b.downloadAvatar(threadKey)?.takeIf { it.isNotEmpty() }
    }

    // ---- Facebook link pages ------------------------------------------------
    // A facebook.com / fb.watch page read as the logged-in session, for the
    // phone's link previews: {"status","url","body"} or {"error"} from the
    // bridge (which paces reads and refuses other hosts). Empty when the account
    // isn't logged in or the bridge isn't up. Blocking: call off the main thread.
    fun fetchPage(url: String, maxBytes: Long): String {
        if (!isLoggedIn()) return ""
        val b = fbBridge ?: return ""
        return try {
            b.fetchPage(url, maxBytes) ?: ""
        } catch (e: Throwable) {
            errJson(e)
        }
    }

    // ---- helpers -----------------------------------------------------------

    // An event payload as logged: LogStore scrubs names, text and addresses by
    // key; this also drops what its keys don't cover: an encrypted video's
    // poster picture (base64) and the account's own id.
    private val posterRe = Regex("\"posterData\":\"[^\"]*\"")
    private val fbidRe = Regex("\"fbid\":\\d+")
    private fun logSafe(payload: String): String = payload
        .replace(posterRe) { "\"posterData\":\"[${it.value.length - 15} chars]\"" }
        .replace(fbidRe, "\"fbid\":\"[id]\"")

    private fun empty(key: String) = "{\"$key\":[]}"
    private fun errStr(msg: String) = "{\"error\":\"$msg\"}"
    private fun errJson(e: Throwable) = "{\"error\":${org.json.JSONObject.quote(e.message ?: "fb error")}}"
}
