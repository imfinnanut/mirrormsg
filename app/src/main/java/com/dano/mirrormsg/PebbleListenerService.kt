package com.dano.mirrormsg

import android.util.Base64
import android.util.Log
import io.rebble.pebblekit2.client.BasePebbleListenerService
import io.rebble.pebblekit2.common.model.PebbleDictionary
import io.rebble.pebblekit2.common.model.PebbleDictionaryItem
import io.rebble.pebblekit2.common.model.ReceiveResult
import io.rebble.pebblekit2.common.model.WatchIdentifier
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.json.JSONArray
import org.json.JSONObject
import java.util.UUID

class PebbleListenerService : BasePebbleListenerService() {

    // Serialize image streaming: only ONE image transfer in flight at a time so
    // chunks never interleave (interleaving corrupted the watch bitmap AND
    // leaked heap). A newer request supersedes the latest pending one.
    private val imgMutex = Mutex()
    // Track in-flight image-stream jobs so a PRIORITY request (user opened the
    // enhanced view) can cancel everything else and stream the wanted image now.
    private val imgJobs = java.util.Collections.synchronizedList(mutableListOf<kotlinx.coroutines.Job>())
    private fun cancelAllImageJobs() {
        synchronized(imgJobs) {
            imgJobs.forEach { it.cancel() }
            imgJobs.clear()
        }
        LogStore.i(TAG, "Cancelled all in-flight image jobs (priority request)")
    }
    @Volatile private var latestImgRequest = -1

    // Idempotency guard for KEY_MARK_THREAD_READ. The watch fires that key on
    // EVERY appearance of a thread (including reply-overlay close onto an already-
    // visible thread), so we get duplicates by design. markRead is itself a no-op
    // on an already-read thread, but this skips the redundant network round-trip:
    // remember the last "convoId:newestMsgId" we marked and ignore an identical
    // repeat. Cleared implicitly whenever a genuinely newer message arrives (the
    // newestMsgId changes), so a new message in an open thread still re-marks.
    @Volatile private var lastMarkedRead: String? = null

    internal val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    companion object {
        private const val TAG = "MirrorMsgSpike"
        private const val FETCH_COUNT = 45L   // per-thread history depth (bumped 50% from 30)
        // Full conversation-list depth we build + page through. The watch shows
        // Schema.CONVO_PAGE_SIZE per page; this is how many total we keep indexed
        // (so ~CONVO_CAP/CONVO_PAGE_SIZE pages). Was 50 (5 pages) — raised to 20
        // pages. Cheap: only the current page is streamed; this just caps how deep
        // the index (and each list fetch) goes.
        private const val CONVO_CAP = 200
        // Leading spaces before the ENTER "drill-in" arrow (watch text is left-
        // aligned, so padding pushes it rightward). Tuning reference: ~30 rode the
        // RIGHT edge; ~22 was the original; 15 sat MIDDLE. 29 = on the edge with a
        // one-space gap.
        private const val ENTER_ARROW_PAD = 29
        // Max age of a stored message allowed to override a bridge's convo-list
        // preview (see convoSnippet). Generous enough to cover "sent today, list
        // still shows yesterday"; bounded so a stale store entry can't mask a
        // genuinely newer bridge preview indefinitely. 24h.
        private const val SNIPPET_STORE_MAX_AGE_MS = 24L * 60 * 60 * 1000
        private const val MSG_TEXT_MAX = 135
        private const val FULL_TEXT_MAX = 900   // cap for on-demand full text (one AppMessage)
        private const val SENDER_MAX = 22
        // Quoted-reply line caps (field 8): keep enough of the quoted message to
        // recognize it; a long quote is cut with "…" and the user scrolls up to
        // the original for the rest.
        // Fixed-width watch buffers (bytes, leaving room for the null): the bubble
        // sender and the reply target are sender[16]/target[16]; the reply snippet is
        // snippet[24]. We pre-fit to these so the watch never has to silently hard-clamp.
        private const val WATCH_NAME_MAX_BYTES = 15
        private const val WATCH_SNIPPET_MAX_BYTES = 23
        // The watch inbox is a hard 4096-byte AppMessage dict and KEY_THREAD_BLOB
        // is a single un-chunked tuple, so blob + other tuples + framing must fit.
        // Keep the blob comfortably under, leaving ~300B for the other tuples.
        private const val BLOB_BYTE_BUDGET = 3800
        private const val TEXT_FRAME_DELAY = 50L
        private const val IMG_CHUNK_DELAY = 45L

        // Live reference to the running service so the activity's live-message
        // handler can push thread updates without re-implementing streaming.
        @Volatile var instance: PebbleListenerService? = null

        /** Called from MainActivity when a live MESSAGE arrives for a convo. */
        fun onLiveMessage(convoId: String) = scheduleThreadPush(convoId, reactionOnly = false)

        /**
         * Called when only a REACTION changed on a convo (no new message). Same
         * debounced re-push as onLiveMessage, but flags the push reaction-only so the
         * watch repaints the badge in place instead of scrolling to the newest row.
         */
        fun onLiveReaction(convoId: String) = scheduleThreadPush(convoId, reactionOnly = true)

        private fun scheduleThreadPush(convoId: String, reactionOnly: Boolean) {
            val svc = instance ?: return
            // History/realtime messages arrive in bursts (20-88 at once). Pushing
            // the whole thread to the watch on EVERY message floods it with dozens
            // of partial re-renders and scrambles the view. Debounce: restart a
            // short timer on each message and only push once it goes quiet.
            synchronized(debounceJobs) {
                // The coalesced push stays reaction-only ONLY if every contributor in
                // this window was reaction-only; a single real message in the burst
                // makes it a normal push (jump to newest).
                val prev = pendingReactionOnly[convoId]
                pendingReactionOnly[convoId] = (prev ?: true) && reactionOnly
                debounceJobs.remove(convoId)?.cancel()
                debounceJobs[convoId] = svc.scope.launch {
                    delay(DEBOUNCE_MS)
                    val ro = synchronized(debounceJobs) {
                        debounceJobs.remove(convoId)
                        pendingReactionOnly.remove(convoId) ?: reactionOnly
                    }
                    if (Mirror.currentOpenConvoId == convoId) {
                        svc.pushThreadUpdate(convoId, reactionOnly = ro)
                    }
                    svc.refreshConversationList()
                }
            }
        }

        private const val DEBOUNCE_MS = 400L
        private val debounceJobs =
            java.util.Collections.synchronizedMap(HashMap<String, kotlinx.coroutines.Job>())
        // Per-convo: is the pending debounced push reaction-only? Guarded by the
        // debounceJobs monitor (only touched inside synchronized(debounceJobs)).
        private val pendingReactionOnly = HashMap<String, Boolean>()
    }

    override fun onCreate() {
        super.onCreate()
        instance = this
        // The service can start without MainActivity ever being created (after a
        // reboot, or an OS-triggered service restart). MainActivity is the only
        // other place that loads settings, so without this the persisted
        // activeServiceId never loads and the watch is served the wrong account's
        // list on open. Ensure appContext + settings are loaded here too.
        if (Mirror.appContext == null) Mirror.appContext = applicationContext
        Mirror.loadSettings(applicationContext)
        LogStore.i(TAG, "Service onCreate: activeServiceId=${Mirror.activeServiceId}")
    }

    override fun onDestroy() {
        if (instance === this) instance = null
        super.onDestroy()
    }

    /** Re-stream the stored thread to the watch (used for live updates). */
    suspend fun pushThreadUpdate(convoId: String, openView: Int = Schema.VIEW_CHAT, pnRow: Int = -1, reactionOnly: Boolean = false) {
        try {
            val json = MessageStore.loadAsThreadJson(this, convoId)
            // A reaction/badge re-push holds the reader's current page; a live NEW
            // message (or reply refresh) resets to page 0 (newest) — the v1 "jump to
            // newest on new message" behavior.
            val page = if (reactionOnly) Mirror.currentMsgPage else 0
            streamMessages(convoId, json, openView, pnRow, reactionOnly, msgPage = page)
            LogStore.i(TAG, "Live thread update pushed for convo $convoId (view=$openView${if (reactionOnly) " reactOnly" else ""})")
        } catch (e: Exception) {
            LogStore.e(TAG, "pushThreadUpdate failed", e)
        }
    }

    /**
     * The multi-account pseudo-notification sequence. A PN makes its OWNING
     * account active, always: switch activeServiceId to the owner, refresh that
     * account's conversation list + switcher rail (so the watch shows the owner's
     * list, not whatever was active before), then push the enhanced frame for the
     * convo — carrying its row index in the just-pushed list (KEY_PN_ROW) so the
     * watch sets s_open_row to match. Ordering matters: list refresh BEFORE the
     * enhanced frame, so the row index is valid against the list the watch holds.
     *
     * This also fixes cross-account image serving: once the owner is active,
     * streamImage's Mirror.activeService() resolves to the owning adapter, so a
     * Beeper image is served by Beeper instead of by whatever account was active.
     */
    suspend fun pushForceOpen(convoId: String, openView: Int, ownerServiceId: String) {
        try {
            Mirror.currentMsgPage = 0   // force-open lands on the newest page
            // 1) Switch active account to the owner (persisted; flips the multiplexer).
            if (Mirror.activeServiceId != ownerServiceId) {
                Mirror.activeServiceId = ownerServiceId
                Mirror.activeTgFolder = 0   // force-open targets the owner's main context
                Mirror.activeDiscordGuild = ""   // ...and Discord's base (DMs) context
                LogStore.i(TAG, "pushForceOpen: active account -> $ownerServiceId (was different)")
            }
            // 2) Refresh that account's list, but stream the PAGE that contains the
            //    target so the watch syncs to it (KEY_CONVO_PAGE) and KEY_PN_ROW can
            //    stay page-relative. Inline (not repushConversations) so we stream the
            //    target's page directly instead of page 0 then jumping.
            val svc = Mirror.activeService()
            if (svc == null) {
                LogStore.w(TAG, "pushForceOpen: no active service")
                return
            }
            // Set the nav context so the streamed list CONTAINS the target and BACK
            // climbs coherently. Each account decides its own home: a DM drops into
            // "dms"; a Discord server channel drops into that server's channel list so
            // back-out lands there (not DMs). Because the streamed list is that context,
            // the target row also resolves — setting s_open_row on server channels.
            Mirror.navStack.clear()
            Mirror.navStack.addAll(svc.forceOpenNavContext(convoId))
            reportServiceList()
            val json = svc.listContext(CONVO_CAP, Mirror.navStack.toList())
            // Cache only the root list (see refreshConversationList); a drilled context
            // must not later be served as the account's root.
            if (Mirror.navStack.isEmpty()) Mirror.putCachedConvoJson(json)
            val total = rebuildConvoIndex(json)
            // 3) Resolve the convo's ABSOLUTE row, split into page + page-relative row.
            val abs = Mirror.convoOrder.indexOf(convoId)
            val page  = if (abs >= 0) abs / Schema.CONVO_PAGE_SIZE else 0
            val pnRow = if (abs >= 0) abs % Schema.CONVO_PAGE_SIZE else -1
            if (abs < 0)
                LogStore.w(TAG, "pushForceOpen: convo $convoId not in owner's list (row=-1); s_open_row not set")
            streamConvoPage(json, page, total)
            // 4) Push the enhanced frame with the page-relative row index.
            pushThreadUpdate(convoId, openView, pnRow)
            LogStore.i(TAG, "pushForceOpen: convo $convoId owner=$ownerServiceId abs=$abs page=$page pnRow=$pnRow view=$openView")
        } catch (e: Exception) {
            LogStore.e(TAG, "pushForceOpen failed", e)
        }
    }

    /** Re-fetch the conversation list and re-stream it so the watch's list
     *  updates live (new message bumps that convo to the top). */
    suspend fun refreshConversationList() {
        try {
            val svc = Mirror.activeService() ?: return
            val json = svc.listContext(CONVO_CAP, Mirror.navStack.toList())
            // Only the account root is cached (the fast open path); a drilled context
            // must not later be served as root.
            if (Mirror.navStack.isEmpty()) Mirror.putCachedConvoJson(json)
            streamConvos(json)
            LogStore.i(TAG, "Live conversation list refresh pushed")
        } catch (e: Exception) {
            LogStore.e(TAG, "refreshConversationList failed", e)
        }
    }

    override suspend fun onMessageReceived(
        watchappUUID: UUID,
        data: PebbleDictionary,
        watch: WatchIdentifier
    ): ReceiveResult {
        if (watchappUUID != Schema.WATCH_UUID) return ReceiveResult.Nack

        val replyText = (data[Schema.KEY_REPLY_TEXT] as? PebbleDictionaryItem.Text)?.value
        if (replyText != null) {
            val row = (data[Schema.KEY_REQ_OPEN] as? PebbleDictionaryItem.Int32)?.value
                ?: return ReceiveResult.Nack
            // Present (>=0) only when the reply came from the DETAIL view: it's the
            // target message's index for a reply-to / reaction. Absent => -1 => a
            // normal chat-view reply.
            val targetIndex = (data[Schema.KEY_REPLY_TO_INDEX] as? PebbleDictionaryItem.Int32)?.value ?: -1
            LogStore.i(TAG, "Reply received for row $row (target=$targetIndex): \"$replyText\"")
            scope.launch { sendReply(row, replyText, targetIndex) }
            return ReceiveResult.Ack
        }

        // Diagnostic string from the watch (image stride/recv etc.) -> logcat.
        val diag = (data[Schema.KEY_DIAG] as? PebbleDictionaryItem.Text)?.value
        if (diag != null) {
            LogStore.i(TAG, "WATCH-DIAG: $diag")
            return ReceiveResult.Ack
        }

        // Account switcher: watch asked to switch to service index N (the index
        // into the same logged-in-service list we reported via KEY_SERVICES).
        // Flip the active service and re-push that service's conversations.
        val reqService = (data[Schema.KEY_REQ_SERVICE] as? PebbleDictionaryItem.Int32)?.value
        if (reqService != null) {
            // Map the selected index against the exact rail we last sent, so a
            // Telegram folder sub-account resolves to (tgnative, folderId) and a
            // real account resolves to (id, 0).
            val entry = Mirror.lastSwitcherEntries.getOrNull(reqService)
            if (entry != null &&
                (entry.serviceId != Mirror.activeServiceId ||
                    entry.folderId != Mirror.activeTgFolder ||
                    entry.guildId != Mirror.activeDiscordGuild)) {
                LogStore.i(TAG, "Watch switched account -> [${entry.label.trim()}] (idx $reqService svc=${entry.serviceId} folder=${entry.folderId} guild=${entry.guildId})")
                Mirror.activeServiceId = entry.serviceId
                Mirror.activeTgFolder = entry.folderId
                Mirror.activeDiscordGuild = entry.guildId
                scope.launch { repushConversations() }
            } else {
                LogStore.i(TAG, "Watch service switch ignored (idx $reqService)")
            }
            return ReceiveResult.Ack
        }

        // Pagination: watch tapped a page arrow and wants a specific 0-based page.
        // Re-stream that page from the cached list (falling back to a fresh fetch);
        // streamConvos rebuilds the full index so absolute-index opens stay correct.
        val reqPage = (data[Schema.KEY_REQ_CONVO_PAGE] as? PebbleDictionaryItem.Int32)?.value
        if (reqPage != null) {
            LogStore.i(TAG, "Watch requested convo page $reqPage")
            scope.launch {
                // At root, the cached list is valid; inside a drilled context the
                // cache is the root list, so fetch the current context fresh.
                val json = (if (Mirror.navStack.isEmpty()) Mirror.getCachedConvoJson() else null)
                    ?: Mirror.activeService()?.listContext(CONVO_CAP, Mirror.navStack.toList())
                if (json != null) streamConvos(json, reqPage)
                else LogStore.w(TAG, "No convo list available to page")
            }
            return ReceiveResult.Ack
        }

        // Hierarchical drill/climb: n >= 0 enters the current list's absolute entry n
        // (a server/bucket); -1 pops one level. Re-streams the new context's page 0.
        val reqEnter = (data[Schema.KEY_REQ_ENTER] as? PebbleDictionaryItem.Int32)?.value
        if (reqEnter != null) {
            LogStore.i(TAG, "Watch nav request: entry $reqEnter")
            scope.launch { navigateContext(reqEnter) }
            return ReceiveResult.Ack
        }

        // Thread pagination: the watch asks for a different message page of the OPEN
        // thread (up-arrow => older/page+1, down-arrow => newer/page-1). Re-stream that
        // page; streamMessages sets Mirror.currentMsgPage so subsequent row-indexed
        // requests (reply/react target, image, full-text) resolve against it.
        val reqMsgPage = (data[Schema.KEY_REQ_MSG_PAGE] as? PebbleDictionaryItem.Int32)?.value
        if (reqMsgPage != null && reqMsgPage >= 0) {
            val convoId = Mirror.currentOpenConvoId
            if (convoId != null) {
                scope.launch {
                    streamMessages(convoId,
                        MessageStore.loadAsThreadJson(this@PebbleListenerService, convoId),
                        msgPage = reqMsgPage)
                }
            }
            return ReceiveResult.Ack
        }

        // On-demand FULL TEXT: watch opened a truncated message in enhanced view
        // and wants the complete text (which didn't fit the thread blob).
        val reqFullText = (data[Schema.KEY_REQ_FULL_TEXT] as? PebbleDictionaryItem.Int32)?.value
        if (reqFullText != null) {
            val convoId = Mirror.currentOpenConvoId
            if (convoId != null) {
                scope.launch { sendFullText(convoId, reqFullText) }
            }
            return ReceiveResult.Ack
        }

        // On-demand image request: watch parked on / entered image at msg index.
        val reqImg = (data[Schema.KEY_REQ_IMAGE] as? PebbleDictionaryItem.Int32)?.value
        if (reqImg != null) {
            val convoId = Mirror.currentOpenConvoId
            val full = ((data[Schema.KEY_REQ_IMG_FULL] as? PebbleDictionaryItem.Int32)?.value ?: 0) == 1
            val budget = (data[Schema.KEY_REQ_IMG_BUDGET] as? PebbleDictionaryItem.Int32)?.value ?: 0
            val budgetMax = (data[Schema.KEY_REQ_IMG_BUDGET_MAX] as? PebbleDictionaryItem.Int32)?.value ?: budget
            // All images are 16-color (4bpp) now; the bridge maps quality=1 to a
            // 4bpp palettized encode. (Full-color/8bpp support was removed.)
            val quality = 1
            val priority = ((data[Schema.KEY_REQ_IMG_PRIORITY] as? PebbleDictionaryItem.Int32)?.value ?: 0) == 1
            if (convoId != null) {
                // PRIORITY (user opened enhanced view): cancel every other image
                // job so this one isn't stuck behind thumbnail downloads/streams.
                if (priority) cancelAllImageJobs()
                // ALWAYS honor the request with a stream. The watch serializes
                // transfers and tracks in-flight state expecting an IMG_END for
                // every request it sends; dropping one here would leave the watch
                // stuck waiting forever. The imgMutex still prevents two streams
                // from overlapping on the wire.
                LogStore.i(TAG, "Watch requested image msg $reqImg (convo $convoId, full=$full, budget=$budget, max=$budgetMax, q=$quality, prio=$priority)")
                val job = scope.launch { streamImageByIndex(convoId, reqImg, full, budget, budgetMax, quality) }
                imgJobs.add(job)
                job.invokeOnCompletion { imgJobs.remove(job) }
            }
            return ReceiveResult.Ack
        }

        // Mark-thread-read: the watch fires this whenever a chat thread becomes the
        // visible top of screen — manual nav, a force-open, OR backing out of a PN's
        // detail view into the thread beneath (a case KEY_REQ_OPEN does NOT cover).
        // Contract: receive it => mark that convo read. Fire-and-forget + duplicated,
        // so the op is idempotent and guarded against redundant network calls.
        val markRow = (data[Schema.KEY_MARK_THREAD_READ] as? PebbleDictionaryItem.Int32)?.value
        if (markRow != null) {
            // ENTER rows (buckets/servers) aren't threads — never mark those.
            if (Mirror.navKinds.getOrNull(markRow) == 1) return ReceiveResult.Ack
            val convoId = Mirror.convoOrder.getOrNull(markRow)
            if (convoId != null) {
                // The watch shows the ACTIVE account's list, so the convo at this
                // index belongs to the active service (same basis openThread uses).
                val svc = Mirror.activeService()
                if (svc != null && svc.supportsMarkRead) {
                    scope.launch {
                        try {
                            val newestId = MessageStore.newestMessageId(
                                this@PebbleListenerService, convoId)
                            if (newestId.isEmpty()) return@launch
                            val token = "$convoId:$newestId"
                            if (token == lastMarkedRead) return@launch  // idempotent skip
                            val res = svc.markRead(convoId, newestId)
                            lastMarkedRead = token
                            LogStore.i(TAG, "Thread-read: markRead($convoId) -> $res")
                        } catch (e: Exception) {
                            LogStore.e(TAG, "KEY_MARK_THREAD_READ handling failed", e)
                        }
                    }
                }
            }
            return ReceiveResult.Ack
        }

        val row = (data[Schema.KEY_REQ_OPEN] as? PebbleDictionaryItem.Int32)?.value
            ?: return ReceiveResult.Nack
        LogStore.i(TAG, "Watch requested thread for row $row")
        scope.launch { openThread(row) }
        return ReceiveResult.Ack
    }

    // ---- Send reply --------------------------------------------------------

    /** Best-effort refresh after a reply was sent OUTSIDE the service (e.g. the
     *  notification quick-reply, which sends via Mirror.bridge directly). Re-pulls
     *  the thread so the sent message lands in the store and reaches the watch.
     *  Safe to call from any context; no-op failures are logged, not thrown. */
    fun refreshAfterReply(convoId: String) {
        scope.launch {
            try {
                val svc = Mirror.activeService() ?: return@launch
                delay(800)
                val json = svc.fetchMessages(convoId, FETCH_COUNT, "")
                val msgs = JSONObject(json).optJSONArray("messages") ?: JSONArray()
                MessageStore.merge(this@PebbleListenerService, convoId, msgs)
                Mirror.invalidateConvoList()
                refreshConversationList()
                if (Mirror.currentOpenConvoId == convoId) {
                    streamMessages(convoId, MessageStore.loadAsThreadJson(this@PebbleListenerService, convoId))
                }
            } catch (e: Exception) {
                LogStore.e(TAG, "refreshAfterReply failed", e)
            }
        }
    }

    // Re-stream the currently-open thread IN PLACE (same call shape as the post-reply
    // refresh above, so it never changes the watch's current view), so state that
    // lands out-of-band — a reaction ingested into ReactionStore, with no new message
    // to ride on — surfaces without the user re-opening the thread. No-op unless
    // convoId is the thread the watch is showing.
    fun refreshOpenThread(convoId: String) {
        scope.launch {
            try {
                if (Mirror.currentOpenConvoId != convoId) return@launch
                streamMessages(
                    convoId,
                    MessageStore.loadAsThreadJson(this@PebbleListenerService, convoId)
                )
            } catch (e: Exception) {
                LogStore.e(TAG, "refreshOpenThread failed", e)
            }
        }
    }

    private suspend fun sendReply(row: Int, text: String, targetIndex: Int = -1) {
        try {
            val convoId = Mirror.currentOpenConvoId
                ?: Mirror.convoOrder.getOrNull(row)
                ?: run { LogStore.w(TAG, "No open convo for reply"); return }
            val route = Mirror.currentOpenRoute
                ?: Mirror.simRoutes.getOrNull(row)
                ?: Mirror.SimRoute("2", 2L, 2L)
            // Detail-view reply: targeted at a specific message (reply-to / react),
            // gated on the owning service's opt-in capability. If sendTargeted
            // handles it, we're done; otherwise (chat-view reply, unsupported
            // service, or a failed targeted send) fall through to a normal message —
            // so non-opted services keep working exactly as before.
            if (targetIndex >= 0 && sendTargeted(convoId, targetIndex, text)) return
            sendReplyInternal(convoId, route, text)
        } catch (e: Exception) {
            LogStore.e(TAG, "sendReply failed", e)
        }
    }

    // Detail-view targeted send. Resolves the target message by its index in the
    // open thread (same index machinery as image/full-text requests), then routes:
    //   emoji-only + service supports reaction -> send a reaction
    //   text       + service supports reply-to -> send a quoted reply
    //   otherwise                              -> false (caller sends a plain message)
    // Returns true only when a targeted send actually succeeded; false means "fall
    // back to a normal message," which covers non-opted services, threads the bridge
    // can't target (e.g. FB cookie), and any send error.
    private suspend fun sendTargeted(convoId: String, targetIndex: Int, text: String): Boolean {
        val svc = Mirror.activeService() ?: return false
        val trimmed = buildTrimmed(MessageStore.loadAsThreadJson(this, convoId))
        if (targetIndex >= trimmed.size) {
            LogStore.w(TAG, "Reply target index $targetIndex out of range (${trimmed.size}); plain send")
            return false
        }
        val mm = trimmed[targetIndex]
        if (mm.messageId.isEmpty()) {
            LogStore.w(TAG, "Reply target $targetIndex has no message id; plain send")
            return false
        }
        val targetFromMe = mm.incoming == 0   // incoming: 1 = from them, 0 = from me
        val emojiOnly = isEmojiOnly(text)
        val result: String = when {
            emojiOnly && svc.supportsReactionSend ->
                svc.sendReaction(convoId, mm.messageId, targetFromMe, text)
            !emojiOnly && svc.supportsReplyTo ->
                svc.sendReplyTo(convoId, mm.messageId, targetFromMe, text)
            else -> return false // service hasn't opted in -> normal message
        }
        if (!sendSucceeded(result)) {
            LogStore.w(TAG, "Targeted send failed; falling back to plain message: $result")
            return false
        }
        LogStore.i(TAG, "Sent ${if (emojiOnly) "reaction" else "reply-to"} to msg ${mm.messageId.takeLast(8)} in $convoId")
        if (emojiOnly) {
            // A watch-sent reaction never echoes back through the bridge (FB only
            // loops back reactions left in its own app), so apply it optimistically
            // right now. ReactionStore persists it, so it also survives a restart.
            // Keyed under the stored message's own id → matches at pack time with no
            // alias needed. "self" collapses repeat self-reactions to one slot.
            // Key the optimistic reaction under this network's own actor id when it
            // knows it (e.g. the FB fbid), so a later change/removal from the native
            // app — which carries that same actor — reconciles it. Falls back to a
            // synthetic key for networks that don't expose a self id.
            val actorKey = Mirror.activeService()?.selfActorKey() ?: "self"
            ReactionStore.ingest(mm.messageId, actorKey, text.trim())
            if (Mirror.currentOpenConvoId == convoId)
                streamMessages(convoId, MessageStore.loadAsThreadJson(this, convoId),
                    reactionOnly = true, msgPage = Mirror.currentMsgPage)
        } else {
            // A reply optimistically inserts itself in the service; re-push the open
            // thread after a beat so the watch reflects it.
            if (Mirror.currentOpenConvoId == convoId) {
                delay(700)
                streamMessages(convoId, MessageStore.loadAsThreadJson(this, convoId))
            }
        }
        return true
    }

    // A bridge send response is JSON; success carries a non-empty messageId, an
    // error carries an "error" field (errJson/errStr). Used to decide fall-back.
    private fun sendSucceeded(result: String): Boolean = try {
        JSONObject(result).optString("messageId").isNotEmpty()
    } catch (_: Exception) {
        false
    }

    // True when [text] is purely emoji (one or more emoji and nothing else) — the
    // detail-view signal to react instead of reply. Joiners, variation selectors,
    // and skin-tone modifiers are allowed; any ordinary character disqualifies it.
    private fun isEmojiOnly(text: String): Boolean {
        val t = text.trim()
        if (t.isEmpty()) return false
        var i = 0
        var sawEmoji = false
        while (i < t.length) {
            val cp = t.codePointAt(i)
            i += Character.charCount(cp)
            when {
                cp == 0x200D || cp == 0xFE0F || cp == 0xFE0E -> {}           // ZWJ / variation selectors
                cp in 0x1F3FB..0x1F3FF -> {}                                  // skin-tone modifiers
                isEmojiCodePoint(cp) -> sawEmoji = true
                else -> return false
            }
        }
        return sawEmoji
    }

    private fun isEmojiCodePoint(cp: Int): Boolean =
        cp in 0x1F300..0x1FAFF ||  // pictographs, supplemental, symbols & pictographs extended-A
        cp in 0x1F000..0x1F0FF ||  // mahjong/dominoes/cards
        cp in 0x2600..0x27BF ||    // misc symbols + dingbats (incl. ❤ U+2764, ☺, ✌, ✨)
        cp in 0x2B00..0x2BFF ||    // stars and misc symbols/arrows
        cp in 0x2300..0x23FF ||    // misc technical (⌚ ⏰ ⏳ …)
        cp in 0x1F1E6..0x1F1FF ||  // regional indicators (flags)
        cp == 0x2640 || cp == 0x2642 || cp == 0x2695 // gender/medical signs used in ZWJ sequences

    private suspend fun sendReplyInternal(convoId: String, route: Mirror.SimRoute, text: String) {
        try {
            val svc = Mirror.activeService() ?: run { LogStore.w(TAG, "No service for reply"); return }
            LogStore.i(TAG, "Sending reply to convo $convoId: \"$text\"")
            // Encode the SIM route as the opaque reply token the service expects.
            // (Google decodes it back into SIM routing; other services ignore it.)
            val replyToken = JSONObject().apply {
                put("p", route.participantID); put("t", route.simTwo); put("n", route.simNumber)
            }.toString()
            val result = svc.sendMessage(convoId, text, replyToken)
            LogStore.i(TAG, "Reply sent. Response: $result")

            // Re-fetch after a short delay so the sent message lands in the store
            // and gets pushed to the watch.
            delay(800)
            try {
                val json = svc.fetchMessages(convoId, FETCH_COUNT, "")
                val msgs = JSONObject(json).optJSONArray("messages") ?: JSONArray()
                MessageStore.merge(this, convoId, msgs)
                if (Mirror.currentOpenConvoId == convoId) {
                    streamMessages(convoId, MessageStore.loadAsThreadJson(this, convoId))
                }
            } catch (e: Exception) {
                LogStore.e(TAG, "post-send refresh failed", e)
            }
        } catch (e: Exception) {
            LogStore.e(TAG, "sendReply failed", e)
        }
    }

    // ---- Hierarchical navigation -------------------------------------------

    /** Drill into the current context's entry [entry] (>= 0), or pop one level (-1),
     *  then re-stream the new context as a list (page 0). [entry] is absolute (same
     *  indexing as KEY_REQ_OPEN), resolved against the current context's entries. */
    private suspend fun navigateContext(entry: Int) {
        try {
            if (entry < 0) {
                if (Mirror.navStack.isNotEmpty())
                    Mirror.navStack.removeAt(Mirror.navStack.size - 1)
                else {
                    LogStore.w(TAG, "REQ_ENTER pop at root — ignoring")
                    return
                }
            } else {
                val token = Mirror.navContexts.getOrNull(entry).orEmpty()
                if (Mirror.navKinds.getOrNull(entry) == 1 && token.isNotEmpty()) {
                    Mirror.navStack.add(token)
                } else {
                    LogStore.w(TAG, "REQ_ENTER on non-ENTER row $entry — ignoring")
                    return
                }
            }
            val svc = Mirror.activeService() ?: return
            val json = svc.listContext(CONVO_CAP, Mirror.navStack.toList())
            // Drilled context is never cached as root (see refreshConversationList).
            streamConvos(json, 0)
            LogStore.i(TAG, "Nav context -> [${Mirror.navStack.joinToString("/")}]")
        } catch (e: Exception) {
            LogStore.e(TAG, "navigateContext failed", e)
        }
    }

    // ---- Open thread: store-first, then catch-up ---------------------------

    private suspend fun openThread(row: Int) {
        // Defensive: if the watch opens an ENTER row (bucket/server), drill instead of
        // trying to open a non-existent thread. Normally the watch sends KEY_REQ_ENTER
        // for these, so this only catches a mismatch.
        if (Mirror.navKinds.getOrNull(row) == 1) {
            LogStore.w(TAG, "openThread on ENTER row $row — drilling instead")
            navigateContext(row)
            return
        }
        val convoId = Mirror.convoOrder.getOrNull(row)
            ?: run { LogStore.w(TAG, "No convo at row $row"); return }
        Mirror.currentOpenConvoId = convoId
        Mirror.currentOpenRoute = Mirror.simRoutes.getOrNull(row)
        Mirror.currentMsgPage = 0   // a fresh open always starts on the newest page

        // 1. Serve stored messages instantly if we have them. The watch no longer
        //    caches thread text (no version handshake), so we always send the full
        //    blob — the re-stream is near-instant and the open animation covers it.
        val stored = MessageStore.loadAsThreadJson(this, convoId)
        val hasStored = (JSONObject(stored).optJSONArray("messages")?.length() ?: 0) > 0
        if (hasStored) {
            LogStore.i(TAG, "Serving stored thread for convo $convoId")
            streamMessages(convoId, stored)
        }

        // 2. Catch-up fetch and re-push only if changed.
        try {
            val svc = Mirror.activeService() ?: run { LogStore.w(TAG, "No service"); return }
            LogStore.i(TAG, "openThread: activeService=${svc.id} activeServiceId=${Mirror.activeServiceId} convo=$convoId")
            val json = svc.fetchMessages(convoId, FETCH_COUNT, "")
            val msgs = JSONObject(json).optJSONArray("messages") ?: JSONArray()
            val added = MessageStore.merge(this, convoId, msgs)
            LogStore.i(TAG, "Catch-up added $added new messages for $convoId")
            if (added > 0 || !hasStored) {
                if (Mirror.currentOpenConvoId == convoId) {
                    streamMessages(convoId, MessageStore.loadAsThreadJson(this, convoId))
                }
            }
        } catch (e: Exception) {
            LogStore.e(TAG, "Catch-up fetch failed", e)
        }
    }

    /**
     * Watch-send guard. Entitlement moved off the phone entirely — it now lives in
     * the watchapp's PKJS layer, which decides whether to render — so the phone
     * mirrors unconditionally and this always returns true. Kept as the single seam
     * every outbound emitter already calls, so if gating is ever needed again it's a
     * one-line change here rather than across all five call sites.
     */
    private fun watchSendAllowed(): Boolean = true

    // ---- Shared message-row model + parser ---------------------------------

    data class MsgRow(
        val ts: Long, val text: String, val incoming: Int,
        val sender: String, val mediaID: String, val decryptionKeyB64: String,
        val mediaDataB64: String,
        // For a combined image+caption that we SPLIT into two chat rows, `link`
        // is the partner row's index (image<->caption). -1 for normal messages.
        var link: Int = -1,
        // The FULL untruncated text. `text` is the (possibly truncated) preview
        // sent in the thread blob; the watch fetches `fullText` on demand only
        // when a message was actually truncated. Empty == not truncated.
        val fullText: String = "",
        // The source message id (FB mid.$.../whatsmeow id/Matrix id/etc.), used to
        // look up reactions in ReactionStore at pack time. Empty for synthetic rows
        // (e.g. the caption half of a split image+caption) so they carry no badge.
        val messageId: String = "",
        // Quoted-reply context when this message replies to another, kept as TWO
        // discrete tokens so the watch gets the original sender's NAME cleanly
        // (not parsed back out of the snippet) and can style it. Both "" when the
        // message isn't a reply. Live on the primary/image row; the caption half
        // carries none, mirroring messageId/reactions.
        val replyTitle: String = "",
        val replySnippet: String = ""
    )

    // Pull the (sender name, text, isMe) of a stored message — used to resolve the
    // quoted message a reply points at, when the source only handed us its id.
    // Reduce a display name to just the first name (first whitespace-delimited
    // token) for in-thread use — the name above a bubble and the reply target.
    // The conversation list still shows full names; this only affects the thread
    // view. Blank in -> blank out; a single-token name is returned unchanged.
    // Stable uint32 token identifying a chat, so the watch can key its thumbnail
    // cache by (token, row) rather than row alone — killing the cross-chat bleed
    // where two chats' same-index images shared a cache slot. FNV-1a over the
    // convoId's UTF-8 bytes: deterministic per chat, good spread. Sent as Int32;
    // the watch reads the same 32 bits as uint32.
    private fun threadToken(convoId: String, page: Int): Int {
        var h = 0x811C9DC5.toInt()               // FNV offset basis
        for (b in convoId.toByteArray(Charsets.UTF_8)) {
            h = (h xor (b.toInt() and 0xFF)) * 0x01000193   // xor byte, FNV prime (wraps at 32-bit)
        }
        // Fold the page in so each (chat, page) gets a distinct, opaque token — the
        // watch's (token, row) thumbnail cache then keeps a separate, coexisting set
        // per page with zero watch-side change.
        h = (h xor page) * 0x01000193
        return h
    }

    private fun firstNameOnly(name: String): String {
        val t = name.trim()
        val sp = t.indexOf(' ')
        return if (sp <= 0) t else t.substring(0, sp)
    }

    // Pre-fit a string to at most maxBytes UTF-8 bytes for a fixed-width watch
    // buffer, appending an ellipsis if it had to be cut — so the truncation reads
    // cleanly ("Christopher…") instead of the watch's silent hard clamp. Never
    // splits a multibyte character, so accented names / emoji can't overrun.
    private fun fitField(s: String, maxBytes: Int): String {
        if (s.toByteArray(Charsets.UTF_8).size <= maxBytes) return s
        val ell = "…"
        val budget = maxBytes - ell.toByteArray(Charsets.UTF_8).size
        if (budget <= 0) return ""
        val sb = StringBuilder()
        var used = 0
        var i = 0
        while (i < s.length) {
            val cp = s.codePointAt(i)
            val n = Character.charCount(cp)
            val piece = s.substring(i, i + n)
            val pieceBytes = piece.toByteArray(Charsets.UTF_8).size
            if (used + pieceBytes > budget) break
            sb.append(piece)
            used += pieceBytes
            i += n
        }
        return sb.toString() + ell
    }

    private fun quotedSummary(mm: org.json.JSONObject): Triple<String, String, Boolean> {
        val sp = mm.optJSONObject("senderParticipant")
        val isMe = sp?.optBoolean("isMe", false) ?: false
        val name = (sp?.optString("firstName")?.takeIf { it.isNotEmpty() }
            ?: sp?.optString("fullName")?.takeIf { it.isNotEmpty() } ?: "")
        var text = ""
        mm.optJSONArray("messageInfo")?.let { infos ->
            for (j in 0 until infos.length()) {
                val c = infos.getJSONObject(j).optJSONObject("messageContent")?.optString("content")
                if (!c.isNullOrEmpty()) { text = c; break }
            }
        }
        return Triple(name, text, isMe)
    }

    // Build a reply snippet preview of a quoted message, tagged by media type: a
    // leading "[Link]"/"[Image]"/"[Video]" token, then as much of the caption/text
    // as the snippet budget allows (buildReplyTitle does the final flatten + cap).
    // The bridge marks bare links as "[Link]" and FB videos as "[video]"; images
    // carry an image mediaContent. Plain text returns unchanged (no token).
    private fun quotedSnippetText(q: org.json.JSONObject): String {
        var text = ""
        var hasImage = false
        q.optJSONArray("messageInfo")?.let { infos ->
            for (j in 0 until infos.length()) {
                val info = infos.getJSONObject(j)
                if (text.isEmpty()) {
                    val c = info.optJSONObject("messageContent")?.optString("content")
                    if (!c.isNullOrEmpty()) text = c
                }
                val mc = info.optJSONObject("mediaContent")
                if (mc != null) {
                    val fmt = mc.optString("format").uppercase()
                    val mime = mc.optString("mimeType").lowercase()
                    if (fmt.contains("IMAGE") || mime.startsWith("image/")) hasImage = true
                }
            }
        }
        val trimmed = text.trimStart()
        val lower = trimmed.lowercase()
        return when {
            lower.startsWith("[link]") -> {
                val rest = trimmed.drop(6).trim { it == '\n' || it == '\r' || it == ' ' }
                if (rest.isEmpty()) "[Link]" else "[Link] $rest"
            }
            lower == "[video]" -> "[Video]"
            hasImage -> if (text.isEmpty()) "[Image]" else "[Image] $text"
            else -> text
        }
    }

    // Compose the quoted-reply tokens for the watch from the resolved quoted
    // message (name/text/isMe) plus who sent THIS message. Returns (target, snippet):
    //   target — ONLY the person/pronoun being replied to, with is-me resolved to a
    //            pronoun phone-side. The watch glues "<replier> replied to <target>",
    //            where <replier> is "You" for its own messages or the sender (field 1)
    //            for incoming. So we emit just the tail token:
    //              me -> Kendra       => "Kendra"   (watch: "You replied to Kendra")
    //              me -> my own msg   => "yourself" (watch: "You replied to yourself")
    //              Kendra -> me       => "you"      (watch: "Kendra replied to you")
    //              Kendra -> Nicole   => "Nicole"   (watch: "Kendra replied to Nicole")
    //   snippet — a trimmed preview of the quoted text, enough to recognize it.
    // Both "" when there's nothing to show. (The watch still gets field 0 to know
    // if THIS message is mine, for bubble side/color — that's separate from this.)
    private fun buildReplyTitle(
        quotedName: String, quotedText: String, quotedIsMe: Boolean,
        replierName: String, replierIsMe: Boolean
    ): Pair<String, String> {
        if (quotedName.isEmpty() && quotedText.isEmpty() && !quotedIsMe) return "" to ""
        // Field 7 now carries ONLY the reply target — the person/pronoun being
        // replied to. The watch composes the visible "<replier> replied to <target>"
        // itself, using "You" for its own messages or the message's sender (field 1)
        // for incoming. We keep all identity logic here: is-me -> pronoun, otherwise
        // the resolved name. (replierName is retained in the signature but no longer
        // glued in — the watch derives the replier from the sender field directly,
        // so the two can never diverge.)
        val target = when {
            replierIsMe && quotedIsMe -> "yourself"  // I replied to my own message
            quotedIsMe -> "you"                       // someone replied to me
            else -> fitField(EmojiSub.processText(firstNameOnly(quotedName)), WATCH_NAME_MAX_BYTES).ifBlank { "someone" }
        }
        var snippet = EmojiSub.processText(quotedText.replace("\n", " ").replace("\r", " ").trim())
        snippet = fitField(snippet, WATCH_SNIPPET_MAX_BYTES)
        return target to snippet
    }

    /** Parse + sort + trim a stored thread JSON into the exact row list the
     *  watch indexes against (so msg indices match between thread + image). */
    private fun buildSorted(json: String): List<MsgRow> {
        val arr = JSONObject(json).optJSONArray("messages") ?: JSONArray()
        // Index messages by id so a reply can resolve its quoted message's
        // sender/text/isMe from the stored thread when the source only gave us the
        // quoted id (E2EE arrivals, and our own optimistically-inserted replies).
        val byId = HashMap<String, JSONObject>()
        for (i in 0 until arr.length()) {
            val mm = arr.getJSONObject(i)
            val id = mm.optString("messageID")
            if (id.isNotEmpty()) byId[id] = mm
        }
        val list = ArrayList<MsgRow>()
        for (i in 0 until arr.length()) {
            val m = arr.getJSONObject(i)
            val msgId = m.optString("messageID")
            val sp = m.optJSONObject("senderParticipant")
            val isMe = sp?.optBoolean("isMe", false) ?: false
            // Never leave the sender blank, or the watch shows no name at all for
            // that message. Prefer firstName, fall back to fullName, then the
            // formatted/raw number — so a contact's name always appears at least
            // once in the thread. In-thread we show just the first name (the convo
            // list keeps the full name); number fallbacks are left whole.
            val sender = fitField(
                (sp?.optString("firstName")?.takeIf { it.isNotEmpty() }?.let { firstNameOnly(it) }
                    ?: sp?.optString("fullName")?.takeIf { it.isNotEmpty() }?.let { firstNameOnly(it) }
                    ?: sp?.optString("formattedNumber")?.takeIf { it.isNotEmpty() }
                    ?: sp?.optJSONObject("ID")?.optString("number")
                    ?: ""),
                WATCH_NAME_MAX_BYTES
            )
            // Quoted-reply source: FB-style "replyTo" {id,sender,text,isMe}, or
            // Google RCS's native protojson "replyMessage" {messageID}. Either way
            // the quoted sender/text/isMe is resolved from the stored thread by id.
            val replyObj = m.optJSONObject("replyTo")
            val gReplyId = m.optJSONObject("replyMessage")?.optString("messageID") ?: ""
            val (replyTitle, replySnippet) =
                if (replyObj == null && gReplyId.isEmpty()) "" to ""
                else {
                    var qName = replyObj?.optString("sender") ?: ""
                    var qText = replyObj?.optString("text") ?: ""
                    var qIsMe = replyObj?.optBoolean("isMe", false) ?: false
                    val qId = replyObj?.optString("id")?.takeIf { it.isNotEmpty() } ?: gReplyId
                    // Fill anything the source didn't give us from the quoted message
                    // in this thread (the store is authoritative for direction). For
                    // the quoted TEXT, prefer the store's type-aware display ("[Link]
                    // …", "[Image] …", "[Video]") over the source's raw reply snippet,
                    // so a reply to a link/image/etc. shows the media token first.
                    if (qId.isNotEmpty()) byId[qId]?.let { q ->
                        val (sn, _, me) = quotedSummary(q)
                        val disp = quotedSnippetText(q)
                        if (qName.isEmpty()) qName = sn
                        if (disp.isNotEmpty()) qText = disp
                        qIsMe = me
                    }
                    buildReplyTitle(qName, qText, qIsMe, sender, isMe)
                }
            val ts = m.optString("timestamp").toLongOrNull() ?: 0L
            var text = ""; var mediaID = ""; var decryptionKeyB64 = ""
            var mediaDataB64 = ""
            val infos = m.optJSONArray("messageInfo")
            var sawMediaPart = false
            var sawVideoPart = false
            if (infos != null) {
                for (j in 0 until infos.length()) {
                    val info = infos.getJSONObject(j)
                    val mc = info.optJSONObject("messageContent")
                    val media = info.optJSONObject("mediaContent")
                    if (mc != null) {
                        val c = mc.optString("content")
                        if (c.isNotEmpty()) { text = c }
                    }
                    if (media != null) {
                        // Video is NOT a still image: the Pebble image pipeline
                        // (and the Go decoder it streams to) only handles JPEG/PNG,
                        // so feeding it MP4 bytes throws "image: unknown format" and
                        // hangs the transfer. Detect video by format/mimeType and
                        // surface it as a "[Video]" placeholder instead of trying
                        // to download + decode it as a picture.
                        val fmt = media.optString("format")
                        val mime = media.optString("mimeType")
                        if (fmt.startsWith("VIDEO") || mime.startsWith("video/")) {
                            sawVideoPart = true
                            continue
                        }
                        sawMediaPart = true
                        if (mediaID.isEmpty()) {
                            mediaID = media.optString("mediaID")
                            decryptionKeyB64 = media.optString("decryptionKey")
                        }
                        // Some image messages expose the downloadable reference
                        // only under thumbnailMediaID/thumbnailDecryptionKey (no
                        // plain mediaID), even though it points to the full-res
                        // media. Fall back to it so these images download at full
                        // resolution instead of stranding on the inline preview.
                        if (mediaID.isEmpty()) {
                            mediaID = media.optString("thumbnailMediaID")
                            decryptionKeyB64 = media.optString("thumbnailDecryptionKey")
                        }
                        if (mediaDataB64.isEmpty())
                            mediaDataB64 = media.optString("mediaData")
                    }
                }
            }
            val isImageMsg = mediaID.isNotEmpty() || mediaDataB64.isNotEmpty() || sawMediaPart
            if (isImageMsg) {
                LogStore.i(TAG, "IMGPARSE v3: mediaID=${if (mediaID.isEmpty()) "<none>" else "present(${mediaID.length})"} inline=${mediaDataB64.length} isMe=$isMe")
            }
            // A video-only message (no still image, no text) shows a placeholder
            // so it doesn't fall through to "[Image]" and trigger a doomed decode.
            if (text.isEmpty() && !isImageMsg) text = if (sawVideoPart) "[Video]" else "[Image]"
            val inc = if (isMe) 0 else 1
            val snd = sender.take(SENDER_MAX)
            if (isImageMsg && text.isNotEmpty() && text != "[Image]") {
                // Combined image+caption: SPLIT into two chat rows (image, then
                // caption text). They share the SAME ts so they stay adjacent;
                // links are wired after the final sort/trim below. Both keep the
                // media+caption pairing info so the enhanced view can recombine.
                val cFull = if (text.length > MSG_TEXT_MAX) text else ""
                list.add(MsgRow(ts, "[Image]", inc, snd, mediaID, decryptionKeyB64, mediaDataB64, messageId = msgId, replyTitle = replyTitle, replySnippet = replySnippet))
                list.add(MsgRow(ts, text.take(MSG_TEXT_MAX), inc, snd, "", "", "", -2, cFull))
                // link = -2 marks "this is the caption half"; resolved below.
            } else {
                val tFull = if (text.length > MSG_TEXT_MAX) text else ""
                list.add(MsgRow(ts, text.take(MSG_TEXT_MAX), inc, snd,
                    mediaID, decryptionKeyB64, mediaDataB64, -1, tFull, msgId, replyTitle, replySnippet))
            }
        }
        // Stable sort: a split pair (same ts, image added before caption) keeps
        // its image-then-caption order. NO cap here — the full history (up to the
        // store's MAX_PER_CONVO) is returned so pageWindow() can slice any page.
        list.sortBy { it.ts }
        return list
    }

    // Resolve image<->caption split-pair links WITHIN a page window (indices are
    // page-relative, which is what the watch sees). A caption row was marked
    // link=-2; its image partner is the row right before it. If the image half
    // landed on the previous page (split across a page boundary), demote the
    // orphan caption to a normal text row (link=-1).
    private fun resolveSplitLinks(rows: MutableList<MsgRow>) {
        for (k in rows.indices) {
            if (rows[k].link == -2) {
                if (k > 0 && (rows[k - 1].mediaID.isNotEmpty() || rows[k - 1].mediaDataB64.isNotEmpty())) {
                    rows[k].link = k - 1
                    rows[k - 1].link = k
                } else {
                    rows[k].link = -1   // orphaned caption -> plain text
                }
            }
        }
    }

    // The window of messages for `page` (0 = newest MSG_PAGE_SIZE, 1 = next older,
    // ...), oldest→newest within the page, plus whether OLDER messages exist before
    // it (drives KEY_MSG_MORE_OLDER). Split-pair links are resolved on the window so
    // they stay page-relative. A page past the start of history returns empty.
    private fun pageWindow(json: String, page: Int): Pair<List<MsgRow>, Boolean> {
        val full = buildSorted(json)
        val size = full.size
        val endExcl = size - Schema.MSG_PAGE_SIZE * page       // page 0 ends at newest
        if (endExcl <= 0) return emptyList<MsgRow>() to (size > 0)
        val start = maxOf(0, endExcl - Schema.MSG_PAGE_SIZE)
        val window = ArrayList(full.subList(start, endExcl))   // copy refs; full is discarded
        resolveSplitLinks(window)
        return window to (start > 0)
    }

    // Current-page window for the OPEN thread. Every row-indexed caller (reply/react
    // target, image, full-text) indexes this by the watch's plain row 0..N-1, so the
    // page offset is applied here once — the window IS the offset.
    private fun buildTrimmed(json: String): List<MsgRow> =
        pageWindow(json, Mirror.currentMsgPage).first

    /** On-demand: stream ONE image (by its watch msg index) for a convo. */
    // Send the FULL untruncated text of one message (by its watch index) back to
    // the watch's enhanced view. Only called for messages the watch knows were
    // truncated, so this fires rarely and never touches the chat-list path.
    private suspend fun sendFullText(convoId: String, msgIndex: Int) {
        if (!watchSendAllowed()) return
        try {
            val json = MessageStore.loadAsThreadJson(this, convoId)
            val trimmed = buildTrimmed(json)
            if (msgIndex < 0 || msgIndex >= trimmed.size) return
            val full = trimmed[msgIndex].fullText
            if (full.isEmpty()) return   // not actually truncated; nothing to send
            // The watch caps its full-text buffer; keep it bounded to a sane size.
            val payload = EmojiSub.processText(full).take(FULL_TEXT_MAX)
            val sender = Mirror.sender(this)
            sender.sendDataToPebble(Schema.WATCH_UUID, mapOf(
                Schema.KEY_FULL_TEXT_IDX to PebbleDictionaryItem.Int32(msgIndex),
                Schema.KEY_FULL_TEXT     to PebbleDictionaryItem.Text(payload)
            ), watches = null)
            LogStore.i(TAG, "Sent full text for msg $msgIndex (${payload.length} chars)")
        } catch (e: Exception) {
            LogStore.e(TAG, "sendFullText failed for msg $msgIndex", e)
        }
    }

    suspend fun streamImageByIndex(convoId: String, msgIndex: Int, full: Boolean = false, budget: Int = 0, budgetMax: Int = 0, quality: Int = 0) {
        try {
            val json = MessageStore.loadAsThreadJson(this, convoId)
            val trimmed = buildTrimmed(json)
            if (msgIndex < 0 || msgIndex >= trimmed.size) {
                LogStore.w(TAG, "Image request index $msgIndex out of range (${trimmed.size})")
                return
            }
            val mm = trimmed[msgIndex]
            if (mm.mediaID.isEmpty() && mm.mediaDataB64.isEmpty()) {
                LogStore.w(TAG, "Image request msg $msgIndex has no media")
                return
            }
            streamImage(convoId, msgIndex, mm.mediaID, mm.decryptionKeyB64,
                mm.mediaDataB64, mm.incoming, mm.sender, full, budget, budgetMax, quality)
        } catch (e: Exception) {
            LogStore.e(TAG, "streamImageByIndex failed", e)
        }
    }

    // ---- Stream parsed messages to watch -----------------------------------

    private suspend fun streamMessages(convoId: String, json: String,
                                       openView: Int = Schema.VIEW_CHAT, pnRow: Int = -1,
                                       reactionOnly: Boolean = false, msgPage: Int = 0) {
        if (!watchSendAllowed()) return
        try {
            // This push defines the open thread's current page; row-indexed requests
            // (reply/react target, image, full-text) resolve against it via buildTrimmed.
            Mirror.currentMsgPage = msgPage
            // Google embeds the reaction set on each message but only the live path
            // parsed it, so a thread's PRE-EXISTING reactions never populated the
            // store. Hydrate from the thread here so they render on load/reopen and
            // after a restart. Gated to Google; inert for other networks anyway
            // (they don't embed a "reactions" field) but this avoids the per-msg scan.
            if (Mirror.activeServiceId == "google") LiveRouter.hydrateGoogleReactions(json)
            else if (Mirror.activeServiceId == "tgnative") LiveRouter.hydrateTelegramReactions(json)
            else if (Mirror.activeServiceId == "discord") LiveRouter.hydrateDiscordReactions(json)
            val (trimmed, moreOlder) = pageWindow(json, msgPage)
            val count = trimmed.size
            val sender = Mirror.sender(this)
            LogStore.i(TAG, "Streaming $count messages (packed) to watch for convo $convoId")

            // Pack the whole thread into a single string:
            //   each message: incoming \x1f sender \x1f text \x1f tsSeconds
            //   messages separated by \x1e
            // Images become "[Image]" text and their indices are listed separately.
            val US = "\u001f"  // unit separator (field)
            val RS = "\u001e"  // record separator (message)

            // Pack one message row. The reply title (field 8) and quoted snippet
            // (field 9) can each be emptied under byte pressure without disturbing
            // any message index, since they're trailing fields and every message is
            // still present.
            fun packRow(mm: MsgRow, withTitle: Boolean, withSnippet: Boolean): String {
                val isImg = mm.mediaID.isNotEmpty() || mm.mediaDataB64.isNotEmpty()
                val textForBlob = if (isImg) "[Image]" else EmojiSub.processText(mm.text)
                val tsSec = if (mm.ts > 0) mm.ts / 1_000_000L else 0L
                val title = if (withTitle) mm.replyTitle.replace(RS, " ").replace(US, " ") else ""
                val snippet = if (withSnippet) mm.replySnippet.replace(RS, " ").replace(US, " ") else ""
                return StringBuilder()
                    .append(mm.incoming).append(US)
                    .append(EmojiSub.processText(mm.sender)).append(US)
                    .append(textForBlob.replace(RS, " ").replace(US, " ")).append(US)
                    .append(tsSec).append(US)
                    .append(mm.link).append(US)                                // 5th: split-pair partner index
                    .append(if (mm.fullText.isNotEmpty()) 1 else 0).append(US)  // 6th: truncated flag
                    .append(ReactionStore.summaryForStamped(mm.messageId, convoId, mm.ts)).append(US)  // 7th: reaction summary ("" if none)
                    .append(title).append(US)                                  // 8th: reply title ("" if not a reply / dropped)
                    .append(snippet)                                            // 9th: quoted snippet ("" if not a reply / dropped)
                    .toString()
            }
            // DIAG (reactions): one line per FB/IG thread stream — the store's key
            // count + a few key tails, how many thread rows matched a reaction, and
            // the first row's id tail. Reveals on-device whether reactions that
            // arrived are keyed to the same message ids the thread carries.
            if (Mirror.activeServiceId == "facebook" || Mirror.activeServiceId == "instagram") {
                val d = StringBuilder("REACT-DIAG store=").append(ReactionStore.debugKeys())
                var matched = 0
                for (i in 0 until count) {
                    val row = trimmed[i]
                    val id = row.messageId
                    if (ReactionStore.hasStamped(id, convoId, row.ts)) {
                        matched++
                        d.append(" [$i ..").append(id.takeLast(12)).append('=')
                            .append(ReactionStore.summaryForStamped(id, convoId, row.ts)).append(']')
                    }
                }
                d.append(" matched=").append(matched).append('/').append(count)
                    .append(" row0=..").append(if (count > 0) trimmed[0].messageId.takeLast(12) else "-")
                    .append(" alias=").append(ReactionStore.aliasDebug(convoId, if (count > 0) trimmed[0].ts else 0L))
                LogStore.i(TAG, d.toString())
            }
            fun assemble(withTitle: Boolean, withSnippet: Boolean): Pair<String, List<Int>> {
                val b = StringBuilder()
                val imgs = ArrayList<Int>()
                for (i in 0 until count) {
                    val mm = trimmed[i]
                    if (i > 0) b.append(RS)
                    if (mm.mediaID.isNotEmpty() || mm.mediaDataB64.isNotEmpty()) imgs.add(i)
                    b.append(packRow(mm, withTitle, withSnippet))
                }
                return b.toString() to imgs
            }
            fun bytesOf(s: String) = s.toByteArray(Charsets.UTF_8).size

            // Fit the blob to the watch's 4096 dict: full first, then drop quoted
            // snippets (field 9), then reply titles (field 8) — snippets before
            // titles, never the reverse. All 25 messages stay so the watch's
            // image/full-text/reply-target indices (which re-derive from the full
            // set) remain valid. If even the base blob (no reply fields) is over,
            // it's a pre-existing message-count problem, logged below.
            var withTitle = true; var withSnippet = true
            var (blob, imgIndices) = assemble(withTitle, withSnippet)
            if (bytesOf(blob) > BLOB_BYTE_BUDGET) {
                withSnippet = false
                assemble(withTitle, withSnippet).let { blob = it.first; imgIndices = it.second }
            }
            if (bytesOf(blob) > BLOB_BYTE_BUDGET) {
                withTitle = false
                assemble(withTitle, withSnippet).let { blob = it.first; imgIndices = it.second }
            }
            if (!withTitle || !withSnippet)
                LogStore.w(TAG, "Thread blob near 4096: titles=$withTitle snippets=$withSnippet (${bytesOf(blob)}B)")
            if (bytesOf(blob) > BLOB_BYTE_BUDGET)
                LogStore.w(TAG, "Thread blob STILL over budget at ${bytesOf(blob)}B with $count msgs — base content exceeds inbox")

            val frame = HashMap<UInt, PebbleDictionaryItem>()
            frame[Schema.KEY_OP]          = PebbleDictionaryItem.Int32(Schema.OP_MSG)
            frame[Schema.KEY_COUNT]       = PebbleDictionaryItem.Int32(count)
            frame[Schema.KEY_THREAD_BLOB] = PebbleDictionaryItem.Text(blob)
            if (imgIndices.isNotEmpty())
                frame[Schema.KEY_IMG_INDICES] = PebbleDictionaryItem.Text(imgIndices.joinToString(","))
            // Tell the watch which view to land on (chat thread vs enhanced).
            frame[Schema.KEY_OPEN_VIEW] = PebbleDictionaryItem.Int32(openView)
            // Message-view pagination: which page this is (the watch syncs its page
            // authoritatively from this) and whether older messages exist before it.
            frame[Schema.KEY_MSG_PAGE] = PebbleDictionaryItem.Int32(msgPage)
            if (moreOlder)
                frame[Schema.KEY_MSG_MORE_OLDER] = PebbleDictionaryItem.Int32(1)
            // On a force-open (PN), tell the watch the convo's row index in the
            // just-pushed list so it sets s_open_row to match — needed for reply
            // routing fallback AND thread-cache key correctness (cache_save_current
            // keys by row; a stale row poisons the cache on PN-exit).
            if (pnRow >= 0)
                frame[Schema.KEY_PN_ROW] = PebbleDictionaryItem.Int32(pnRow)
            // Chat+page identity so the watch keys its thumbnail cache by (token, row)
            // and each page keeps its own coexisting thumbnail set.
            frame[Schema.KEY_THREAD_TOKEN] = PebbleDictionaryItem.Int32(threadToken(convoId, msgPage))
            // Detail-view banner: network name, or "network\nserver#channel" for
            // drillable accounts (Discord channels). Resolved from the owning service —
            // a force-open sets activeService to the owner before streaming, and a
            // browse-open is already on the owner's list, so activeService is correct.
            Mirror.activeService()?.let {
                frame[Schema.KEY_THREAD_LABEL] = PebbleDictionaryItem.Text(it.threadLabel(convoId))
            }
            // Reaction/badge-only update: tell the watch to keep its scroll position
            // (repaint the pin in place) instead of jumping to the newest row.
            if (reactionOnly)
                frame[Schema.KEY_REACTION_ONLY] = PebbleDictionaryItem.Int32(1)
            sender.sendDataToPebble(Schema.WATCH_UUID, frame, watches = null)
            LogStore.i(TAG, "Sent packed thread (${blob.length} chars, ${imgIndices.size} images)")

            // On-demand image model: auto-load only the LATEST image now; the
            // rest show the photo-icon placeholder and load when the watch
            // parks on / enters them (KEY_REQ_IMAGE). One image in memory.
            // SKIP when landing on the ENHANCED view (force-open / hybrid image):
            // the watch jumps straight there and requests the FULL image itself.
            // Auto-streaming a thumbnail here collides with that full request
            // (same msg index) — the thumbnail wins the slot and the full image
            // gets dropped, so the enhanced view never shows the picture.
            //
            // This preload is a TRACKED job (added to imgJobs) with its settle
            // delay INSIDE the job, so a PRIORITY request — the user opening this
            // same image at full size — cancels it via cancelAllImageJobs(),
            // including during the delay window, before any stale small START is
            // sent. (Previously it was an untracked launch that priority couldn't
            // reach, so its preview START raced the full transfer.)
            if (openView != Schema.VIEW_ENHANCED) {
                val latestImg = imgIndices.maxOrNull()
                if (latestImg != null) {
                    val mm = trimmed[latestImg]
                    val job = scope.launch {
                        delay(200)
                        if (!isActive) return@launch
                        streamImage(convoId, latestImg, mm.mediaID, mm.decryptionKeyB64,
                            mm.mediaDataB64, mm.incoming, mm.sender)
                    }
                    imgJobs.add(job)
                    job.invokeOnCompletion { imgJobs.remove(job) }
                }
            }
        } catch (e: Exception) {
            LogStore.e(TAG, "streamMessages failed", e)
        }
    }

    // ---- Stream image to watch ---------------------------------------------

    private suspend fun streamImage(
        convoId: String,
        msgIdx: Int, mediaID: String, decryptionKeyB64: String,
        mediaDataB64: String, incoming: Int, senderName: String,
        full: Boolean = false, budget: Int = 0, budgetMax: Int = 0, quality: Int = 0
    ) = imgMutex.withLock {
        if (!watchSendAllowed()) return@withLock
        // Serialize ALL image transfers: only one image streams to the watch at
        // a time. Two concurrent streams interleave their chunks on the wire,
        // producing overlapping IMG_STARTs that corrupt the watch's single
        // receive bitmap (recv inflated, garbled/black image).
        try {
            val svc = Mirror.activeService() ?: return
            // caller passes quality=1; the bridge encodes 4bpp palettized.
            val q = 1
            // Enhanced (full): width cap + heap byte-budget (height unlimited).
            // Chat (small): classic small box, no budget.
            val maxW = if (full) Schema.IMG_MAX_W else Schema.IMG_SMALL_W
            val maxH = if (full) 0 else Schema.IMG_SMALL_H
            // Budgets come straight from the watch (already heap-floored there).
            // Do NOT inflate them — the watch may have intentionally kept them
            // small to avoid a crash. Just guard against a zero/garbage value.
            val curBudget = if (budget > 200) budget else 200
            val maxBudget = if (budgetMax > curBudget) budgetMax else curBudget
            // Scale at the LARGER (cache-freed) budget. If the result fits within
            // the current budget, no wipe is needed; if it's bigger, the watch
            // must free its cache (wipe=1) to make room.
            val maxBytes = if (full) maxBudget else 0
            val resultJson = if (mediaID.isNotEmpty()) {
                LogStore.i(TAG, "Downloading image for msg $msgIdx, mediaID=$mediaID (full=$full w=$maxW cur=$curBudget max=$maxBudget)")
                svc.downloadImage(
                    mediaID, decryptionKeyB64, maxW, maxH, maxBytes, q)
            } else {
                LogStore.i(TAG, "No mediaID; using INLINE image for msg $msgIdx (${mediaDataB64.length} b64 chars)")
                svc.processInlineImage(
                    mediaDataB64, maxW, maxH, maxBytes, q)
            }
            // DIAGNOSTIC: capture the raw scaler return BEFORE parsing, so an
            // empty/null/non-JSON return is visible too (JSONObject() would
            // otherwise throw before we could log what came back).
            if (resultJson.isEmpty() || !resultJson.trimStart().startsWith("{")) {
                LogStore.e(TAG, "IMG SCALER returned non-JSON for msg $msgIdx — len=${resultJson.length} raw='${resultJson.take(200)}'")
            }
            val result = JSONObject(resultJson)
            // DIAGNOSTIC: the scaler is expected to return {w,h,pixels,fmt,palette}.
            // When it instead returns an error object (or empty), the getInt("w")
            // below throws a cryptic "No value for w". Surface what the bridge
            // actually returned, and the error field if present, so the real cause
            // (download/decode/decrypt failure vs. account issue) is visible in the
            // exported log. Truncated so base64 pixel data can't flood the log.
            if (!result.has("w")) {
                val errField = result.optString("error", "")
                val preview = if (resultJson.length > 300) resultJson.substring(0, 300) + "…(${resultJson.length} chars)" else resultJson
                LogStore.e(TAG, "IMG SCALER returned no 'w' for msg $msgIdx — error='${errField}' raw=$preview")
            }
            val w = result.getInt("w"); val h = result.getInt("h")
            val pixels = Base64.decode(result.getString("pixels"), Base64.DEFAULT)
            val imgFormat = result.optInt("fmt", 0)   // 0=8bpp, 1=palettized
            // Packed bit depth for palettized images: 1/2/4. Absent => 4 (legacy).
            // Only meaningful (and only sent) when fmt==1; the 8bpp path reports 8
            // here but goes down the watch's non-palettized route, so we never tag
            // a palettized frame with depth 8.
            val imgDepth = result.optInt("depth", 4)
            val paletteB64 = result.optString("palette", "")
            val palette = if (paletteB64.isNotEmpty())
                Base64.decode(paletteB64, Base64.DEFAULT) else ByteArray(0)
            // NOTE: the wipe decision moves BELOW, after compression — for a
            // compressed image the resident cost on the watch is the COMPRESSED
            // size, not pixels.size, so comparing raw size here would trigger
            // needless cache wipes (and mislabel the log). We log the raw size now
            // and decide wipe against the real resident size once it's known.
            LogStore.i(TAG, "Image for msg $msgIdx: ${w}x${h}, ${pixels.size} raw bytes (fmt=$imgFormat)")
            val sender = Mirror.sender(this)

            // Phase 2 (heap engine): full-view palettized frames only. Row-compress
            // a TIGHT-packed copy of the stream; ship compressed only if it's
            // genuinely smaller than the raw (aligned) stream — otherwise raw wins
            // and takes the untouched phase-1 native path (never worse than off).
            var encoding = 0
            var sendStream = pixels          // default: raw (word-aligned) stream
            var restartIndex: ByteArray? = null
            if (Mirror.imgCompress && full && imgFormat == 1) {
                val tight = tightRows(pixels, w, imgDepth)   // strip word-align padding
                if (tight != null) {
                    val rc = rowCompress(tight.first, tight.second, h)
                    // Heap comparison: compressed resident (comp+index) vs raw bitmap
                    // (pixels.size). Only switch if it actually saves heap.
                    if (rc.compressed.size + rc.index.size < pixels.size) {
                        encoding = 1
                        sendStream = rc.compressed
                        restartIndex = rc.index
                        LogStore.i(TAG, "COMPRESS msg $msgIdx: raw ${pixels.size} -> " +
                            "${rc.compressed.size} bytes +${rc.index.size} idx " +
                            "(${100 - rc.compressed.size * 100 / maxOf(1, pixels.size)}% heap saved)")
                    } else {
                        LogStore.i(TAG, "COMPRESS msg $msgIdx: raw wins " +
                            "(${pixels.size} <= ${rc.compressed.size + rc.index.size}) — sending raw")
                    }
                }
            }

            // Wipe the watch's caches only when the image's ACTUAL RESIDENT size
            // won't fit the current (no-wipe) budget. For a compressed image the
            // resident cost is the compressed blob (+ index), NOT the raw decoded
            // size — so a 13KB-raw image that compresses to 3.4KB no longer triggers
            // a needless cache purge when 3.4KB fits the current headroom. Only when
            // even the resident size overflows the current budget do we ask the watch
            // to free its thumbnail caches. Cross-chat collisions are keyed on the
            // watch via (KEY_THREAD_TOKEN,row), so this is purely a heap-fit decision.
            val residentSize = if (encoding == 1) sendStream.size + (restartIndex?.size ?: 0)
                               else pixels.size
            val wipeCache = full && residentSize > curBudget
            LogStore.i(TAG, "msg $msgIdx resident=$residentSize (enc=$encoding) vs curBudget=$curBudget -> wipe=$wipeCache")

            sender.sendDataToPebble(Schema.WATCH_UUID, mapOf(
                Schema.KEY_IMG_START      to PebbleDictionaryItem.Int32(msgIdx),
                Schema.KEY_IDX            to PebbleDictionaryItem.Int32(w),
                Schema.KEY_COUNT          to PebbleDictionaryItem.Int32(h),
                Schema.KEY_MSG_IN         to PebbleDictionaryItem.Int32(incoming),
                Schema.KEY_MSG_SENDER     to PebbleDictionaryItem.Text(senderName),
                // KEY_TYPE = decoded (raw packed) size. For RAW this is the bitmap
                // reserve; for COMPRESSED the watch reserves KEY_IMG_ENC_BYTES instead
                // and uses this only as the expand-to size / row-count sanity.
                Schema.KEY_TYPE           to PebbleDictionaryItem.Int32(pixels.size),
                Schema.KEY_IMG_WIPE_CACHE to PebbleDictionaryItem.Int32(if (wipeCache) 1 else 0),
                // LOAD-BEARING — do NOT drop as "redundant with the thread frame".
                // The watch reads this into a PER-TRANSFER token and stamps the
                // thumbnail with it, so an image that completes after a chat switch
                // is still keyed to its own chat. Without it, a mid-transfer switch
                // re-bleeds. Must stay on every IMG_START.
                Schema.KEY_THREAD_TOKEN   to PebbleDictionaryItem.Int32(threadToken(convoId, Mirror.currentMsgPage)),
                Schema.KEY_IMG_FORMAT     to PebbleDictionaryItem.Int32(imgFormat),
                Schema.KEY_IMG_ENCODING   to PebbleDictionaryItem.Int32(encoding),
                Schema.KEY_REQ_IMG_FULL   to PebbleDictionaryItem.Int32(if (full) 1 else 0)
            ) + (if (imgFormat == 1 && palette.isNotEmpty())
                    mapOf(
                        Schema.KEY_IMG_PALETTE to PebbleDictionaryItem.Bytes(palette),
                        Schema.KEY_IMG_DEPTH   to PebbleDictionaryItem.Int32(imgDepth)
                    )
                 else emptyMap())
              + (if (encoding == 1)
                    mapOf(
                        Schema.KEY_IMG_ENC_BYTES   to PebbleDictionaryItem.Int32(sendStream.size),
                        Schema.KEY_IMG_RESTART_IDX to PebbleDictionaryItem.Bytes(restartIndex!!)
                    )
                 else emptyMap()),
            watches = null)
            delay(150)

            // Transport is identical for raw and compressed: fixed seq*380 chunks
            // over the chosen stream. For compressed, the watch reassembles the
            // compressed blob then decodes on draw; a dropped chunk damages only the
            // rows spanning that gap (bounded by the restart index).
            var seq = 0
            var offset = 0
            while (offset < sendStream.size) {
                val end = minOf(offset + Schema.IMG_CHUNK_SIZE, sendStream.size)
                sender.sendDataToPebble(Schema.WATCH_UUID, mapOf(
                    Schema.KEY_IMG_SEQ  to PebbleDictionaryItem.Int32(seq),
                    Schema.KEY_IMG_DATA to PebbleDictionaryItem.Bytes(sendStream.copyOfRange(offset, end))
                ), watches = null)
                delay(IMG_CHUNK_DELAY)
                offset = end; seq++
            }
            sender.sendDataToPebble(Schema.WATCH_UUID, mapOf(
                Schema.KEY_IMG_END to PebbleDictionaryItem.Int32(msgIdx)
            ), watches = null)
            LogStore.i(TAG, "Image stream complete for msg $msgIdx ($seq chunks)")
        } catch (e: Exception) {
            LogStore.e(TAG, "streamImage failed for msg $msgIdx", e)
        }
    }

    // ---- Phase 2: heap-engine row compression encoder ---------------------
    // Result of compressing one image: the concatenated per-row payloads and the
    // uint16-LE restart index (compressed offset of every Nth row).
    private class RowComp(val compressed: ByteArray, val index: ByteArray)

    /**
     * Strip word-align padding from the encoder's aligned stream down to TIGHT rows
     * (the layout the watch decodes for compressed frames: ceil(w*depth/8) bytes/
     * row, no 4-byte padding). Returns (tightBytes, tightRowStride) or null if the
     * geometry doesn't line up (in which case the caller falls back to raw).
     */
    private fun tightRows(aligned: ByteArray, w: Int, depth: Int): Pair<ByteArray, Int>? {
        if (depth != 1 && depth != 2 && depth != 4) return null
        val ppb = 8 / depth
        val tightRow = (w + ppb - 1) / ppb
        val alignedRow = (tightRow + 3) and 3.inv()
        if (alignedRow == 0 || aligned.size % alignedRow != 0) return null
        val h = aligned.size / alignedRow
        if (tightRow == alignedRow) return Pair(aligned, tightRow) // already tight
        val out = ByteArray(tightRow * h)
        for (y in 0 until h) {
            System.arraycopy(aligned, y * alignedRow, out, y * tightRow, tightRow)
        }
        return Pair(out, tightRow)
    }

    private val RESTART_N = 8
    private val REPEAT_ROW = 0x00.toByte()

    /**
     * Compress a tight-packed stream row-by-row (see the Phase-2 comment in Mirror's
     * Schema). Each row is independent PackBits; a row byte-identical to the one
     * above becomes a single 0x00 "repeat previous row" token — EXCEPT restart rows
     * (every RESTART_N-th), which are always real so a seek lands on a decodable row.
     * PackBits count byte: 1..127 => repeat next byte N times; 128..255 => copy the
     * next (256-c) literals. 0x00 is never a PackBits count, so it's free as the
     * whole-row repeat marker.
     */
    private fun rowCompress(tight: ByteArray, rowBytes: Int, h: Int): RowComp {
        val comp = java.io.ByteArrayOutputStream()
        val index = java.io.ByteArrayOutputStream()
        var prevStart = -1  // offset of previous row in `tight`, or -1
        for (y in 0 until h) {
            val start = y * rowBytes
            val isRestart = (y % RESTART_N == 0)
            if (isRestart) {
                val off = comp.size()
                index.write(off and 0xFF); index.write((off ushr 8) and 0xFF)
            }
            val repeat = !isRestart && prevStart >= 0 &&
                rowEquals(tight, start, prevStart, rowBytes)
            if (repeat) {
                comp.write(0x00) // REPEAT_ROW
            } else {
                packBitsRow(tight, start, rowBytes, comp)
            }
            prevStart = start
        }
        return RowComp(comp.toByteArray(), index.toByteArray())
    }

    private fun rowEquals(a: ByteArray, o1: Int, o2: Int, n: Int): Boolean {
        for (i in 0 until n) if (a[o1 + i] != a[o2 + i]) return false
        return true
    }

    /** PackBits-encode one row [start, start+n) into `out`. Never emits a 0x00 count. */
    private fun packBitsRow(row: ByteArray, start: Int, n: Int, out: java.io.ByteArrayOutputStream) {
        var i = 0
        while (i < n) {
            var runLen = 1
            while (i + runLen < n && row[start + i + runLen] == row[start + i] && runLen < 127) runLen++
            if (runLen >= 2) {
                out.write(runLen)                 // 1..127 = repeat
                out.write(row[start + i].toInt() and 0xFF)
                i += runLen
            } else {
                var litLen = 1
                while (i + litLen < n && litLen < 128) {
                    if (i + litLen + 1 < n && row[start + i + litLen] == row[start + i + litLen + 1]) break
                    litLen++
                }
                out.write((256 - litLen) and 0xFF)  // 128..255 = copy litLen literals
                for (k in 0 until litLen) out.write(row[start + i + k].toInt() and 0xFF)
                i += litLen
            }
        }
    }

    // ---- Auto-push conversations on watchapp open --------------------------

    override fun onAppOpened(watchappUUID: UUID, watch: WatchIdentifier) {
        if (watchappUUID != Schema.WATCH_UUID) return
        LogStore.init(applicationContext)
        LogStore.i(TAG, "Watchapp opened — pushing conversations")
        scope.launch { pushConversations() }
    }

    private suspend fun pushConversations() {
        try {
            // AUTHORITATIVE RESYNC on watchapp open. The watch may hold stale state
            // from before the phone's data was wiped (a roster of accounts that no
            // longer exist, a selector pointing at a gone row). Before pushing, make
            // the active account a REAL logged-in one (the persisted id — or the
            // "google" default — may name a wiped account), so we never boot the
            // watch into a dead account with an empty list and broken nav.
            Mirror.ensureActiveLoggedIn(this)
            val svc = Mirror.activeService() ?: run {
                // Nothing logged in (e.g. right after a full wipe). Still send the
                // authoritative (empty) roster so the watch discards its stale
                // account list instead of showing accounts that no longer exist.
                LogStore.w(TAG, "No service for auto-push — reporting empty roster")
                reportServiceList()
                return
            }
            // A fresh watchapp open lands on the account's top level — clear any nav
            // context left from a prior session so the root list (and the cache) apply.
            Mirror.navStack.clear()
            // Pin the service this push is FOR. Both background coroutines below
            // capture `svc` by reference and run after a delay, during which the
            // user can switch accounts on the watch. If that happens, the account
            // switch handler (repushConversations) has already pushed the correct
            // list — so anything these stale coroutines compute is the WRONG
            // service's data (e.g. WhatsApp threads written into / streamed under
            // the now-active Facebook list). Bail out if the active service has
            // changed. We re-check AFTER each fetch too, since listConversations
            // can span the switch.
            val pinnedId = svc.id
            reportServiceList()
            val cachedJson = Mirror.getCachedConvoJson()
            if (cachedJson != null) {
                LogStore.i(TAG, "Serving cached conversation list")
                streamConvos(cachedJson)
                scope.launch {
                    try {
                        val fresh = svc.listContext(CONVO_CAP, Mirror.navStack.toList())
                        if (Mirror.activeServiceId == pinnedId) Mirror.putCachedConvoJson(fresh)
                    } catch (e: Exception) { LogStore.e(TAG, "Convo background refresh failed", e) }
                }
                // Late async data can land AFTER the cache serve above: an account
                // whose threads load asynchronously (notably FB's E2EE thread
                // restore, which only completes once FB finishes connecting ~1s
                // after the watchapp opens) isn't in the cache yet, so the first
                // list the watch sees is missing it. Re-check shortly and re-stream
                // only if the list actually changed, so a freshly-restored thread
                // (e.g. the wife's E2EE chat) appears without a manual reopen.
                scope.launch {
                    try {
                        delay(1500)
                        if (Mirror.activeServiceId != pinnedId) return@launch
                        val refreshed = svc.listContext(CONVO_CAP, Mirror.navStack.toList())
                        if (Mirror.activeServiceId != pinnedId) return@launch
                        if (refreshed != Mirror.getCachedConvoJson()) {
                            Mirror.putCachedConvoJson(refreshed)
                            streamConvos(refreshed)
                            LogStore.i(TAG, "Late convo restore — re-streamed list to watch")
                        }
                    } catch (e: Exception) { LogStore.e(TAG, "Late convo refresh failed", e) }
                }
            } else {
                val json = svc.listContext(CONVO_CAP, Mirror.navStack.toList())
                Mirror.putCachedConvoJson(json)
                streamConvos(json)
            }
        } catch (e: Exception) {
            LogStore.e(TAG, "pushConversations failed", e)
        }
    }

    /**
     * Forces a fresh conversation push for the CURRENTLY ACTIVE service, bypassing
     * the cache. Used after an account switch (KEY_REQ_SERVICE) so the watch
     * immediately shows the newly-selected network's threads instead of the stale
     * cached list. Also re-reports the service list + active index so the
     * switcher rail reflects the new selection.
     */
    private suspend fun repushConversations() {
        try {
            val svc = Mirror.activeService() ?: run { LogStore.w(TAG, "No active service for repush"); return }
            // Switching accounts lands on the new account's top level.
            Mirror.navStack.clear()
            reportServiceList()
            val json = svc.listContext(CONVO_CAP, Mirror.navStack.toList())
            Mirror.putCachedConvoJson(json)
            streamConvos(json)
        } catch (e: Exception) {
            LogStore.e(TAG, "repushConversations failed", e)
        }
    }

    // Resolve Beeper's shared base class WITHOUT a compile-time reference to it.
    // This file is core, and the core never imports a concrete network class —
    // that's what lets ServiceRegistry drop a network in/out by reflection and
    // still build (see ServiceRegistry's design note). A hard `is BeeperServiceBase`
    // here would break that: deleting Beeper's files would fail to compile this
    // file. So we look the base class up reflectively, the same way the registry
    // loads adapters. If it's absent, every service reports as native, the mask
    // is 0, and the watch draws no "Beeper" divider — exactly the additive
    // fallback the watch expects.
    private val beeperBaseClass: Class<*>? by lazy {
        try { Class.forName("com.dano.mirrormsg.BeeperServiceBase") }
        catch (_: Throwable) { null }
    }
    private fun isBeeperService(svc: MessagingService): Boolean =
        beeperBaseClass?.isInstance(svc) ?: false

    /**
     * Reports logged-in networks + active index to the watch (KEY_SERVICES +
     * KEY_ACTIVE_SERVICE) so the account-switcher rail shows the right options.
     * Sent only when 2+ accounts are connected (no switcher for a single one).
     */
    private suspend fun reportServiceList() {
        if (!watchSendAllowed()) return
        try {
            // Augmented rail = logged-in services + each enabled Telegram folder as
            // an indented sub-account. Built via Mirror so this emitter, the
            // MainActivity emitter, and the KEY_REQ_SERVICE selector all agree on
            // the exact label/index order. lastSwitcherEntries records what the
            // watch is showing so a selected index maps back to a service+folder.
            val entries = Mirror.switcherEntries(this)
            // ALWAYS report the authoritative roster, even at 0 or 1 account. The
            // old `if (entries.size < 2) return` suppressed the push for a single
            // account "because no switcher UI is needed" — but that left the watch
            // showing a STALE roster after the phone's data is wiped (e.g. 3
            // accounts drop to 1, but the watch never hears the count shrank, so
            // its switcher indices no longer match the phone and nav/switch break).
            // The watch adopts whatever roster we send as authoritative: 1 label =
            // single account (no switcher chrome), 0 labels = nothing logged in.
            Mirror.lastSwitcherEntries = entries
            val labels = entries.joinToString("\u001e") { it.label }
            val activeIdx = entries.indexOfFirst {
                it.serviceId == Mirror.activeServiceId &&
                    it.folderId == Mirror.activeTgFolder &&
                    it.guildId == Mirror.activeDiscordGuild
            }.coerceAtLeast(0)
            val serviceFlags = Mirror.switcherFlags(this, entries)
            val sender = Mirror.sender(this)
            val frame = HashMap<UInt, PebbleDictionaryItem>()
            frame[Schema.KEY_OP]             = PebbleDictionaryItem.Int32(Schema.OP_CONVO)
            frame[Schema.KEY_SERVICES]       = PebbleDictionaryItem.Text(labels)
            frame[Schema.KEY_ACTIVE_SERVICE] = PebbleDictionaryItem.Int32(activeIdx)
            frame[Schema.KEY_SERVICE_FLAGS]  = PebbleDictionaryItem.Int32(serviceFlags)
            // 1 if the active service supports reaction-send (watch uses this to
            // keep the chat put on a reaction vs jump to bottom on a message).
            val activeSvcId = entries.getOrNull(activeIdx)?.serviceId ?: Mirror.activeServiceId
            frame[Schema.KEY_ACTIVE_CAN_REACT] = PebbleDictionaryItem.Int32(
                if (ServiceRegistry.byId(this, activeSvcId)?.supportsReactionSend == true) 1 else 0)
            sender.sendDataToPebble(Schema.WATCH_UUID, frame, watches = null)
            LogStore.i(TAG, "Reported services to watch: [$labels] active=$activeIdx flags=$serviceFlags")
        } catch (e: Exception) {
            LogStore.e(TAG, "reportServiceList failed", e)
        }
    }

    // Signature of the last switcher we pushed (logged-in ids + active id + Beeper
    // mask). refreshSwitcher() compares against this so it only re-sends when the
    // switcher actually changed.
    @Volatile private var lastSwitcherSig: String = ""

    /**
     * Background-safe re-push of the account switcher — but only if it actually
     * changed since the last push. The Beeper poll calls this when an account's
     * logged-in status may have flipped in the background (the user logged in/out
     * of a network inside Beeper). Nothing else re-reports the switcher in that
     * case, so without this the watch shows a stale rail: a newly-present account
     * missing, or a ghost account that dead-ends when selected.
     *
     * Safe to call liberally (e.g. every poll cycle) — the signature diff drops
     * no-op calls so it won't spam the watch. Runs on the service scope, needs no
     * Activity: call via PebbleListenerService.instance?.refreshSwitcher().
     *
     * Note: like reportServiceList(), this only pushes a rail for 2+ logged-in
     * accounts; a drop to a single account doesn't actively collapse an already-
     * shown rail (pre-existing behaviour of the 2+ gate, unchanged here).
     */
    fun refreshSwitcher() {
        scope.launch {
            try {
                val entries = Mirror.switcherEntries(this@PebbleListenerService)
                val sig = entries.joinToString("\u001e") { it.serviceId + ":" + it.folderId + ":" + it.guildId } +
                          "|" + Mirror.activeServiceId + "|" + Mirror.activeTgFolder + "|" + Mirror.activeDiscordGuild
                if (sig == lastSwitcherSig) return@launch
                lastSwitcherSig = sig
                reportServiceList()   // rebuilds + sends KEY_SERVICES / ACTIVE / FLAGS
            } catch (e: Exception) {
                LogStore.e(TAG, "refreshSwitcher failed", e)
            }
        }
    }

    // Build the FULL conversation index (convoOrder + simRoutes + convoTargets) from
    // the list JSON, capped at CONVO_CAP, so absolute indices (KEY_REQ_OPEN and the
    // force-open row) resolve across every page — the watch holds only one page, but
    // the phone indexes the whole list. Returns the total. Streaming is separate.
    private fun rebuildConvoIndex(json: String): Int {
        val arr = JSONObject(json).getJSONArray("conversations")
        val total = minOf(arr.length(), CONVO_CAP)
        Mirror.convoOrder.clear()
        Mirror.simRoutes.clear()
        Mirror.navKinds.clear()
        Mirror.navContexts.clear()
        for (i in 0 until total) {
            val c = arr.getJSONObject(i)
            val convoId = c.optString("conversationID")
            Mirror.convoOrder.add(convoId)
            // Navigation kind: ENTER (bucket/server/folder) vs OPEN (a real chat).
            // Absent navKind => OPEN, so flat services need no change.
            val isEnter = c.optString("navKind") == "ENTER"
            Mirror.navKinds.add(if (isEnter) 1 else 0)
            Mirror.navContexts.add(if (isEnter) c.optString("navContext") else "")
            // Deep-link target: protocol (top-level "type") + the other participant's
            // number, so "Open on phone"/banner can jump straight to the thread.
            Mirror.convoTargets[convoId] = parseConvoTarget(c)
            val simPayload = c.optJSONObject("simCard")
                ?.optJSONObject("SIMData")?.optJSONObject("SIMPayload")
            val simTwo    = (simPayload?.optInt("two", 2) ?: 2).toLong()
            val simNumber = (simPayload?.optInt("SIMNumber", 2) ?: 2).toLong()
            Mirror.simRoutes.add(Mirror.SimRoute(
                c.optString("defaultOutgoingID", "2"), simTwo, simNumber))
        }
        return total
    }

    // Stream ONE page (Schema.CONVO_PAGE_SIZE convos) to the watch. KEY_IDX/KEY_COUNT
    // are page-relative (0..pageLen-1); KEY_CONVO_PAGE tells the watch which page this
    // is (it syncs to it, authoritative), and KEY_CONVO_MORE flags whether further
    // pages exist. `total` is the full list size from rebuildConvoIndex. The page is
    // clamped to what exists, so an out-of-range request lands on the last page.
    private suspend fun streamConvoPage(json: String, page: Int, total: Int) {
        if (!watchSendAllowed()) return
        val arr = JSONObject(json).getJSONArray("conversations")
        val sender = Mirror.sender(this@PebbleListenerService)
        // Streamed convos belong to the active service (all push paths read
        // activeService().listConversations()); resolve once for the brand palette.
        val ownerSvc = Mirror.activeService()
        val pageCount = if (total <= 0) 1 else (total + Schema.CONVO_PAGE_SIZE - 1) / Schema.CONVO_PAGE_SIZE
        val pg = page.coerceIn(0, pageCount - 1)
        val start = pg * Schema.CONVO_PAGE_SIZE
        val end = minOf(start + Schema.CONVO_PAGE_SIZE, total)
        val pageLen = (end - start).coerceAtLeast(0)
        val hasMore = end < total
        val hasParent = Mirror.navStack.isNotEmpty()
        for (rel in 0 until pageLen) {
            val abs = start + rel
            val c = arr.getJSONObject(abs)
            val cf = HashMap<UInt, PebbleDictionaryItem>()
            cf[Schema.KEY_OP]         = PebbleDictionaryItem.Int32(Schema.OP_CONVO)
            cf[Schema.KEY_IDX]        = PebbleDictionaryItem.Int32(rel)      // page-relative
            cf[Schema.KEY_COUNT]      = PebbleDictionaryItem.Int32(pageLen)  // this page's count
            cf[Schema.KEY_CONVO_PAGE] = PebbleDictionaryItem.Int32(pg)
            cf[Schema.KEY_CONVO_MORE] = PebbleDictionaryItem.Int32(if (hasMore) 1 else 0)
            // Whether this list has a parent (BACK climbs vs leaves) and whether this
            // row is an ENTER (drill) vs OPEN (chat). Both drive the nav model.
            cf[Schema.KEY_LIST_HAS_PARENT] = PebbleDictionaryItem.Int32(if (hasParent) 1 else 0)
            cf[Schema.KEY_ENTRY_KIND] = PebbleDictionaryItem.Int32(Mirror.navKinds.getOrNull(abs) ?: 0)
            cf[Schema.KEY_NAME]       = PebbleDictionaryItem.Text(
                EmojiSub.processText(c.optString("name").ifEmpty { "(no name)" }))
            // ENTER rows (buckets/servers/folders) show a right-arrow as the "drill
            // in" affordance instead of a message preview; chats show their snippet.
            // The watch text is left-aligned, so the arrow is pushed rightward with
            // leading spaces (trailing spaces do nothing). Tune ENTER_ARROW_PAD if it
            // lands short of / past the edge.
            cf[Schema.KEY_SNIPPET]    = PebbleDictionaryItem.Text(
                if (Mirror.navKinds.getOrNull(abs) == 1) "\u2192".padStart(ENTER_ARROW_PAD + 1)
                else EmojiSub.processText(convoSnippet(c)))
            cf[Schema.KEY_COLOR]      = PebbleDictionaryItem.Text(convoColor(c, ownerSvc))
            cf[Schema.KEY_TYPE]       = PebbleDictionaryItem.Int32(
                when (c.optString("type")) {
                    "RCS" -> 4
                    "FB"  -> 5
                    else  -> 1
                })
            cf[Schema.KEY_UNREAD]     = PebbleDictionaryItem.Int32(
                if (c.optBoolean("unread", false)) 1 else 0)
            if (rel == 0) {
                cf[Schema.KEY_NIGHT_MODE] = PebbleDictionaryItem.Int32(if (Mirror.nightMode) 1 else 0)
                // Quick replies packed \x1e-separated, sent once on the first frame.
                cf[Schema.KEY_QUICK_REPLIES] =
                    PebbleDictionaryItem.Text(Mirror.quickReplies.joinToString("\u001e"))
                cf[Schema.KEY_THUMB_LIMIT] = PebbleDictionaryItem.Int32(Mirror.thumbLimit)
                cf[Schema.KEY_HEAP_GUARD] = PebbleDictionaryItem.Int32(Mirror.heapGuard)
                cf[Schema.KEY_ACTIVE_CAN_REACT] =
                    PebbleDictionaryItem.Int32(if (Mirror.activeService()?.supportsReactionSend == true) 1 else 0)
            }
            sender.sendDataToPebble(Schema.WATCH_UUID, cf, watches = null)
            delay(80)
        }
        LogStore.i(TAG, "Streamed convo page $pg ($pageLen of $total, more=$hasMore) to watch")
    }

    // Rebuild the full index and stream a page (default 0). Non-force-open callers
    // (reconnect, live refresh, account switch, page request) use this; force-open
    // computes the target's page from the index before streaming.
    private suspend fun streamConvos(json: String, page: Int = 0) {
        val total = rebuildConvoIndex(json)
        streamConvoPage(json, page, total)
    }

    // Build a protocol-agnostic deep-link target from a conversation JSON object.
    // Protocol = top-level "type" (e.g. "SMS"); address = the OTHER participant's
    // E.164 number (ID.number). For group chats (>1 non-me participant) we leave
    // the address blank so the deep link falls back gracefully (no single thread).
    private fun parseConvoTarget(c: JSONObject): ConvoTarget {
        val protocol = c.optString("type")   // "SMS", and later WhatsApp/etc via bridges
        var number = ""
        var others = 0
        val parts = c.optJSONArray("participants")
        if (parts != null) for (i in 0 until parts.length()) {
            val p = parts.getJSONObject(i)
            if (p.optBoolean("isMe", false)) continue
            others++
            val id = p.optJSONObject("ID")
            val n = id?.optString("number") ?: ""
            if (n.isNotEmpty() && number.isEmpty()) number = n
        }
        // Only deep-link to a specific thread for 1:1 chats; blank address for
        // groups so DeepLink degrades to the inbox instead of the wrong thread.
        val address = if (others == 1) number else ""
        return ConvoTarget.from(protocol, address)
    }

    // The convo-list row preview. Prefer the newest message in our OWN store when
    // it's recent, because a bridge's list preview can lag a just-seen message —
    // notably an OUTGOING send (the phone's native app sends a text; libgm reflects
    // it in live events, which we store, but its conversation-list snippet keeps
    // showing the last INCOMING message until some later sync). The store is fed by
    // those live events + thread catch-up, so it's the authoritative "latest we've
    // seen." Recency-gate it so a stale store entry never masks a genuinely newer
    // bridge preview (e.g. a message that arrived while the service was dead lands
    // in the bridge list but not the store); that edge defers to the bridge and
    // self-heals on the next event. Outgoing gets a "You: " prefix, like a normal
    // messaging app's latest-preview line.
    private fun convoSnippet(c: JSONObject): String {
        val bridgePreview = c.optJSONObject("latestMessage")?.optString("displayContent")?.take(60) ?: ""
        val convoId = c.optString("conversationID")
        if (convoId.isEmpty()) return bridgePreview
        val snip = MessageStore.newestSnippet(this@PebbleListenerService, convoId) ?: return bridgePreview
        // Normalize the store's microsecond timestamp to millis (mirrors LiveRouter's
        // tolerant read) and require it to be recent before overriding the bridge.
        val tsRaw = snip.timestampMicros
        val tsMillis = if (tsRaw > 1_000_000_000_000_000L) tsRaw / 1000 else tsRaw
        val ageMs = System.currentTimeMillis() - tsMillis
        val recent = tsMillis > 0 && ageMs in -120_000..SNIPPET_STORE_MAX_AGE_MS
        if (!recent || snip.text.isEmpty()) return bridgePreview
        val body = if (snip.isMe) "You: ${snip.text}" else snip.text
        return body.take(60)
    }

    // Avatar background color for a conversation row, in priority order:
    //   1) bridge-provided color (e.g. Google darkens per-contact at the source),
    //   2) the owning account's brand palette (the account defines its own colors
    //      in its own file; the shared selector picks a stable per-thread shade),
    //   3) the global default purple.
    private fun convoColor(c: JSONObject, owner: MessagingService?): String {
        c.optString("avatarHexColor").let { if (it.isNotEmpty()) return it }
        val parts = c.optJSONArray("participants")
        if (parts != null) for (i in 0 until parts.length()) {
            val p = parts.getJSONObject(i)
            if (!p.optBoolean("isMe", false)) {
                val col = p.optString("avatarHexColor")
                if (col.isNotEmpty()) return col
            }
        }
        owner?.avatarColorFor(c.optString("conversationID"))
            ?.let { if (it.isNotEmpty()) return it }
        return "#7E57C2"
    }

    override fun onAppClosed(watchappUUID: UUID, watch: WatchIdentifier) {
        if (watchappUUID == Schema.WATCH_UUID) LogStore.i(TAG, "Watchapp closed")
    }
}
