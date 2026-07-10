package com.dano.mirrormsg

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.json.JSONObject

/**
 * Process-lifetime home for incoming-message routing (store + notify + force-open).
 *
 * This logic used to live on MainActivity, which tied it to the UI: it only
 * survived backgrounding by accident (the Google bridge's EventSink retained the
 * activity). That accidental coupling is fine for Google but unsafe for Beeper,
 * whose ContentObserver pumps run on background threads with no activity at all.
 *
 * LiveRouter is a singleton (lives for the whole process) driven by
 * Mirror.appContext, so every network — Google's long-poll and Beeper's per-
 * network observers — calls the SAME pipeline the SAME way, foreground or not.
 *
 * Logic here is a verbatim relocation of the former MainActivity pipeline; only
 * the context source changed (applicationContext -> Mirror.appContext) and the
 * coroutine scope owner. The notify gates (recency, self-sent, held-image
 * staging, video) are unchanged.
 */
object LiveRouter {
    private const val TAG = "MirrorMsgSpike"

    // A live incoming message must be no older than this to wake the watch.
    // Re-synced archived threads re-deliver ancient messages that are "new"
    // to our store but not actually new — this rejects them. 2h: a text older
    // than that isn't a "buzz me now" event for a glanceable watch (you'll see
    // it when you open the thread), and it still rejects months-old re-syncs.
    private const val ROUTE_MAX_AGE_MS = 2 * 60 * 60 * 1000L

    // Message IDs we've already force-opened/notified on, so a later status stage
    // (delivered/read/image-attached) doesn't re-buzz. Bounded so it can't grow
    // without limit.
    private val imageForceOpened = object : LinkedHashSet<String>() {
        override fun add(element: String): Boolean {
            val r = super.add(element)
            while (size > 200) remove(iterator().next())
            return r
        }
    }

    // Message IDs whose first stage we HELD (image still downloading). Only these
    // get later-stage handling, so a normal text's delivered/read status updates
    // never re-notify. Bounded like imageForceOpened.
    private val heldForImage = object : LinkedHashSet<String>() {
        override fun add(element: String): Boolean {
            val r = super.add(element)
            while (size > 200) remove(iterator().next())
            return r
        }
    }

    /**
     * Public live-message ingest seam. The owning network's serviceId is the
     * explicit first param so the force-open path can switch the watch to the
     * RIGHT account (and so image serving via Mirror.activeService() resolves to
     * the owning adapter). Google's internal long-poll calls this with "google";
     * Beeper's per-network pumps (BeeperDiscordService / BeeperGoogleVoiceService)
     * call it with their own registry id ("discord" / "gvoice"), one message at a
     * time in timestamp order. serviceId MUST be the ServiceRegistry id, not a
     * Beeper protocol string.
     *
     * Safe to call from any thread, foreground or backgrounded: it depends only on
     * Mirror.appContext (set in MainActivity.onCreate, process-lifetime), never on
     * a live Activity.
     */
    fun ingestLiveMessage(serviceId: String, jsonPayload: String) =
        handleLiveMessage(jsonPayload, serviceId)

    private fun handleLiveMessage(jsonPayload: String, ownerServiceId: String = Mirror.activeServiceId) {
        val ctx = Mirror.appContext ?: run {
            LogStore.w(TAG, "ingestLiveMessage: no appContext yet — dropping message")
            return
        }
        try {
            val m = JSONObject(jsonPayload)
            val convoId = m.optString("conversationID")
            if (convoId.isEmpty()) return
            // Only store messages that are complete (have content or media).
            val infos = m.optJSONArray("messageInfo")
            if (infos == null || infos.length() == 0) return
            MessageStore.mergeOne(ctx, convoId, m)
            val changed = MessageStore.lastMergeChanged
            val added = MessageStore.lastMergeAdded

            // Google Messages embeds the full current reaction set (RCS tapbacks) on
            // every message update. Apply it to the source-agnostic ReactionStore as
            // a wholesale replace, so adds, changes, and removals all resolve from the
            // single embedded array. If it changed, re-push the thread so the badge
            // updates live — but do NOT fall into the notify path below; a reaction is
            // not a new message and must never buzz the wrist.
            if (ownerServiceId == "google") {
                val rMsgId = m.optString("messageID")
                if (rMsgId.isNotEmpty() && applyGoogleReactions(m, rMsgId)) {
                    LogStore.i(TAG, "Google reactions updated for msg ${rMsgId.takeLast(8)} in convo $convoId")
                    Mirror.invalidateConvoList()
                    PebbleListenerService.onLiveReaction(convoId)
                }
            }

            // Telegram embeds its reaction set (emoji + count) on each message; a
            // reaction change arrives as a message edit carrying the updated set. Apply
            // it as authoritative counts (adds/changes/removals all resolve). Like the
            // Google path: re-push the badge, never fall through to notify.
            if (ownerServiceId == "tgnative") {
                val rMsgId = m.optString("messageID")
                if (rMsgId.isNotEmpty() && m.has("reactions") && applyTelegramReactions(m, rMsgId)) {
                    LogStore.i(TAG, "Telegram reactions updated for msg ${rMsgId.takeLast(8)} in convo $convoId")
                    Mirror.invalidateConvoList()
                    PebbleListenerService.onLiveReaction(convoId)
                }
            }

            if (changed > 0) {
                LogStore.i(TAG, "Live message stored/updated for convo $convoId (added=$added changed=$changed)")
                Mirror.invalidateConvoList()
                // Always re-push the thread so a status progression that added
                // media (image arriving in stages) reaches the watch — even when
                // no brand-new messageID was added.
                PebbleListenerService.onLiveMessage(convoId)

                val sp = m.optJSONObject("senderParticipant")
                // Direction must NOT rely solely on senderParticipant.isMe: some
                // events (e.g. a re-synced archived thread, or certain outgoing
                // rows) arrive with NO senderParticipant block at all, so that
                // read defaults to false and an OUTGOING message gets mistaken for
                // incoming — which force-opened the enhanced view on our OWN sent
                // message. Treat it as ours if ANY of these say so.
                val statusForDir = m.optJSONObject("messageStatus")?.optString("status") ?: ""
                val isMe = (sp?.optBoolean("isMe", false) ?: false) ||
                           m.optBoolean("isMe", false) ||
                           m.optBoolean("fromMe", false) ||
                           statusForDir.startsWith("OUTGOING")
                val msgId = m.optString("messageID").ifEmpty {
                    m.optJSONObject("anotherMessageID")?.optString("messageID") ?: ""
                }
                val hasImage = messageHasImage(m)
                // RCS delivers a media message in stages: the FIRST stage has no
                // mediaContent yet and carries status INCOMING_AUTO_DOWNLOADING
                // (the phone is fetching the picture); a later stage flips to
                // INCOMING_COMPLETE with the image attached. A plain text arrives
                // already COMPLETE. So AUTO_DOWNLOADING is our "this is probably
                // an image" signal — used to hold the notification in hybrid mode
                // so a picture force-opens instead of first flashing a text alert.
                val status = m.optJSONObject("messageStatus")?.optString("status") ?: ""
                val downloadingMedia = status == "INCOMING_AUTO_DOWNLOADING"

                if (!isMe) {
                    // Ensure a deep-link target exists for this convo before we
                    // post the notification (streamConvos may not have run yet).
                    if (Mirror.convoTargets[convoId] == null) {
                        val num = sp?.optJSONObject("ID")?.optString("number") ?: ""
                        Mirror.convoTargets[convoId] =
                            ConvoTarget.from(ConvoTarget.SMS, num)
                    }
                    val sender = sp?.optString("firstName")
                        ?.ifEmpty { sp.optString("fullName") } ?: ""
                    val body = extractMessageText(m)
                    val groupName = m.optString("groupName")   // Discord server; "" for DMs/others
                    val silent = m.optBoolean("silent")         // Discord mute / notification level

                    // Recency gate: "added>0" only means new to OUR store, not
                    // newly sent in the real world. When the phone re-syncs an
                    // archived/old thread it re-delivers ancient messages that
                    // were never stored this session, so they pass added>0 and
                    // would force-open the watch for a "phantom" message that
                    // isn't actually new (and shows nothing new in Messages).
                    // A real incoming message's timestamp is within seconds of
                    // now; require it to be recent before we alert. The field is
                    // microseconds (16 digits) — fall back gracefully if absent.
                    val tsRaw = m.optString("timestamp").toLongOrNull() ?: 0L
                    val tsMillis = if (tsRaw > 1_000_000_000_000_000L) tsRaw / 1000 else tsRaw
                    val ageMs = System.currentTimeMillis() - tsMillis
                    val isRecent = tsMillis > 0 && ageMs in -120_000..ROUTE_MAX_AGE_MS
                    if (added > 0 && !isRecent) {
                        LogStore.i(TAG, "routeIncoming: SKIP stale convo $convoId (ageMs=$ageMs, ts=$tsMillis) — re-sync, not a new message")
                    }

                    // Route ONLY for a genuinely NEW incoming message — not for
                    // status updates of one we already alerted on (else an image's
                    // 2nd/3rd stage would re-buzz/re-open).
                    if (added > 0 && isRecent) {
                        if ((Mirror.notifyMode == Mirror.MODE_HYBRID
                             || Mirror.notifyMode == Mirror.MODE_NOTIFY)
                            && !hasImage && downloadingMedia
                            && msgId.isNotEmpty()) {
                            // Image still downloading: HOLD this first-stage alert.
                            // The completing stage posts/force-opens ONCE with the
                            // full body (so we don't get a bare "[Image]" first and
                            // a second notification when the caption/image lands).
                            heldForImage.add(msgId)
                            LogStore.i(TAG, "Holding notification for downloading media msg $msgId (mode=${Mirror.notifyMode})")
                        } else {
                            if (hasImage && msgId.isNotEmpty()) imageForceOpened.add(msgId)
                            routeIncoming(convoId, sender, body, groupName, silent, hasImage, ownerServiceId, msgId)
                        }
                    } else if (msgId.isNotEmpty() && heldForImage.contains(msgId)
                               && !imageForceOpened.contains(msgId)) {
                        // Later stage of a message whose first stage we HELD (only
                        // these — a normal text's delivered/read updates never enter
                        // here, so they don't re-notify).
                        if (hasImage) {
                            imageForceOpened.add(msgId)   // handled once
                            if (Mirror.notifyMode == Mirror.MODE_HYBRID) {
                                LogStore.i(TAG, "Late image attach -> hybrid force-open for msg $msgId")
                                routeIncoming(convoId, sender, body, groupName, silent, true, ownerServiceId, msgId)
                            } else {
                                // Notify mode: still a notification (routeIncoming's
                                // mode check prevents a force-open), but pass TRUE so
                                // the notification carries hasImage — "Open on watch"
                                // then lands on the enhanced view (straight to the
                                // picture) instead of the chat thread.
                                LogStore.i(TAG, "Late image attach -> notify [Image] for msg $msgId")
                                routeIncoming(convoId, sender, body, groupName, silent, true, ownerServiceId, msgId)
                            }
                        } else if (status == "INCOMING_COMPLETE") {
                            // Completed with no media after all -> notify text once.
                            imageForceOpened.add(msgId)
                            LogStore.i(TAG, "Held media never arrived -> notify text for msg $msgId")
                            routeIncoming(convoId, sender, body, groupName, silent, false, ownerServiceId, msgId)
                        }
                    }
                }
            }
        } catch (e: Exception) {
            LogStore.e(TAG, "handleLiveMessage failed", e)
        }
    }

    // Parse Google's embedded "reactions" array into actorKey(participantID) -> emoji
    // and hand the full set to ReactionStore.replaceAll. Each ReactionEntry is one
    // emoji (data.unicode) shared by a list of participantIDs. An absent/empty array
    // means the message currently has no reactions (replaceAll then clears any prior
    // ones — that's how a removal-to-empty resolves). Returns whether the store
    // changed.
    /**
     * Hydrate [ReactionStore] from the reactions embedded on every message of a
     * Google thread JSON. Google carries the full reaction set on each message,
     * but only the LIVE-message path ([applyGoogleReactions] at ingest) parsed it
     * — so a thread's PRE-EXISTING reactions never reached the store and didn't
     * show on the watch (the in-memory store is also empty after an app restart).
     * Call this on thread load for Google convos, before the thread is built.
     * Gated per-message on the presence of a "reactions" field, so it's inert for
     * networks that don't embed one; callers should still restrict it to Google.
     * Idempotent — each message is a wholesale replace.
     */
    fun hydrateGoogleReactions(threadJson: String) {
        try {
            val msgs = JSONObject(threadJson).optJSONArray("messages") ?: return
            for (i in 0 until msgs.length()) {
                val m = msgs.optJSONObject(i) ?: continue
                if (!m.has("reactions")) continue
                val msgId = m.optString("messageID")
                if (msgId.isNotEmpty()) applyGoogleReactions(m, msgId)
            }
        } catch (e: Exception) {
            LogStore.e(TAG, "hydrateGoogleReactions failed", e)
        }
    }

    private fun applyGoogleReactions(m: JSONObject, msgId: String): Boolean {
        val arr = m.optJSONArray("reactions")
        val map = LinkedHashMap<String, String>()
        if (arr != null) {
            for (i in 0 until arr.length()) {
                val entry = arr.optJSONObject(i) ?: continue
                val emoji = entry.optJSONObject("data")?.optString("unicode") ?: ""
                if (emoji.isEmpty()) continue
                val parts = entry.optJSONArray("participantIDs") ?: continue
                for (j in 0 until parts.length()) {
                    val pid = parts.optString(j)
                    if (pid.isNotEmpty()) map[pid] = emoji
                }
            }
        }
        return ReactionStore.replaceAll(msgId, map)
    }

    /**
     * Hydrate [ReactionStore] from the reactions embedded on each message of a
     * Telegram thread JSON — the mirror of [hydrateGoogleReactions]. Telegram carries
     * the aggregate reaction set (emoji + count, no per-actor attribution) on every
     * message, so [ReactionStore.setCounts] synthesizes count keys. Call on thread
     * load for tgnative convos, before the thread is built. Idempotent.
     */
    fun hydrateTelegramReactions(threadJson: String) {
        try {
            val msgs = JSONObject(threadJson).optJSONArray("messages") ?: return
            for (i in 0 until msgs.length()) {
                val m = msgs.optJSONObject(i) ?: continue
                if (!m.has("reactions")) continue
                val msgId = m.optString("messageID")
                if (msgId.isNotEmpty()) applyTelegramReactions(m, msgId)
            }
        } catch (e: Exception) {
            LogStore.e(TAG, "hydrateTelegramReactions failed", e)
        }
    }

    // Apply one Telegram message's embedded "reactions":[{emoji,count}] as the
    // authoritative count set. An empty (but present) array clears the badge. Returns
    // whether the stored state changed.
    private fun applyTelegramReactions(m: JSONObject, msgId: String): Boolean {
        val arr = m.optJSONArray("reactions") ?: return false
        val counts = LinkedHashMap<String, Int>()
        for (i in 0 until arr.length()) {
            val e = arr.optJSONObject(i) ?: continue
            val emoji = e.optString("emoji")
            val c = e.optInt("count", 0)
            if (emoji.isNotEmpty() && c > 0) counts[emoji] = c
        }
        return ReactionStore.setCounts(msgId, counts)
    }

    /**
     * Hydrate [ReactionStore] from the reactions embedded on each message of a Discord
     * thread JSON — identical wire shape to Telegram (aggregate emoji + count, no
     * per-actor attribution), so it defers to [ReactionStore.setCounts]. Call on thread
     * load for discord convos, before the thread is built. This is the historic +
     * server-sync path: every fetch carries the authoritative reaction snapshot, so a
     * reopen reconciles any live per-actor deltas back to the true counts. Idempotent.
     */
    fun hydrateDiscordReactions(threadJson: String) {
        try {
            val msgs = JSONObject(threadJson).optJSONArray("messages") ?: return
            for (i in 0 until msgs.length()) {
                val m = msgs.optJSONObject(i) ?: continue
                if (!m.has("reactions")) continue
                val msgId = m.optString("messageID")
                if (msgId.isNotEmpty()) applyDiscordReactions(m, msgId)
            }
        } catch (e: Exception) {
            LogStore.e(TAG, "hydrateDiscordReactions failed", e)
        }
    }

    // Apply one Discord message's embedded "reactions":[{emoji,count}] as the
    // authoritative count set (unicode only; the bridge skips custom emoji). An empty
    // (but present) array clears the badge. Returns whether the stored state changed.
    private fun applyDiscordReactions(m: JSONObject, msgId: String): Boolean {
        val arr = m.optJSONArray("reactions") ?: return false
        val counts = LinkedHashMap<String, Int>()
        for (i in 0 until arr.length()) {
            val e = arr.optJSONObject(i) ?: continue
            val emoji = e.optString("emoji")
            val c = e.optInt("count", 0)
            if (emoji.isNotEmpty() && c > 0) counts[emoji] = c
        }
        return ReactionStore.setCounts(msgId, counts)
    }

    /** True if the convo's most-recent message carries a (still) image — the "Open on
     *  watch" view decision: land on the enhanced view for a picture so it shows big
     *  immediately, otherwise the chat view (message in context). */
    fun convoLatestHasImage(convoId: String): Boolean {
        val ctx = Mirror.appContext ?: return false
        return try {
            val msgs = JSONObject(MessageStore.loadAsThreadJson(ctx, convoId))
                .optJSONArray("messages") ?: return false
            if (msgs.length() == 0) return false
            // Thread JSON is oldest-first, so the last entry is the triggering message.
            val last = msgs.optJSONObject(msgs.length() - 1) ?: return false
            messageHasImage(last)
        } catch (_: Exception) { false }
    }

    private fun messageHasImage(m: JSONObject): Boolean {
        val infos = m.optJSONArray("messageInfo") ?: return false
        for (j in 0 until infos.length()) {
            val media = infos.getJSONObject(j).optJSONObject("mediaContent") ?: continue
            // Video isn't a still image — it can't render on the watch and must
            // not trigger Hybrid-mode force-open-to-enhanced-view. Only count
            // actual images here.
            val fmt = media.optString("format")
            val mime = media.optString("mimeType")
            if (fmt.startsWith("VIDEO") || mime.startsWith("video/")) continue
            return true
        }
        return false
    }

    /** Route an incoming message per the selected notify mode. */
    private fun routeIncoming(convoId: String, sender: String, body: String, groupName: String, silent: Boolean, hasImage: Boolean, ownerServiceId: String, msgId: String) {
        val ctx = Mirror.appContext ?: return
        // Per-message silence from the source's own settings (e.g. a muted Discord
        // server/channel). Stored already, browsable on the watch — just no alert.
        if (silent) {
            LogStore.i(TAG, "routeIncoming: message silenced by source settings — no notify/PN (still stored)")
            return
        }
        // Mute: the message is already in the MessageStore (browsable on the watch),
        // but a muted account never alerts — no notification, no force-open/PN.
        if (MuteState.isMuted(ctx, ownerServiceId)) {
            LogStore.i(TAG, "routeIncoming: $ownerServiceId is muted — no notify/PN (message still stored)")
            return
        }
        val mode = Mirror.notifyMode
        // Force open?  Forced mode always; Hybrid only when an image arrived
        // (images can't render in a Pebble notification).
        val forceOpen = when (mode) {
            Mirror.MODE_FORCE  -> true
            Mirror.MODE_HYBRID -> hasImage
            else               -> false      // MODE_NOTIFY
        }
        if (forceOpen) {
            // Forced mode and Hybrid+image both land on the ENHANCED view so the
            // message (or image) is shown big immediately.
            LogStore.i(TAG, "routeIncoming: force-open ENHANCED convo $convoId (mode=$mode, img=$hasImage, owner=$ownerServiceId)")
            forceOpenWatch(convoId, Schema.VIEW_ENHANCED, ownerServiceId)
        } else {
            // Notification (and Hybrid text): normal notification -> chat view on tap.
            LogStore.i(TAG, "routeIncoming: notification convo $convoId (mode=$mode)")
            Notifier.postNewMessage(ctx, convoId, sender, body, ownerServiceId, msgId, groupName)
        }
    }

    /**
     * Immediately force-open the watchapp, landing on the given view. ownerServiceId
     * is the account that OWNS this convo — the force-open switches the watch's
     * active account to it (so the rail + list + image serving all agree), then
     * pushes the thread. This is the multi-account PN contract: a pseudo-notification
     * makes its owning account active, always.
     */
    private fun forceOpenWatch(convoId: String, openView: Int = Schema.VIEW_CHAT,
                               ownerServiceId: String = Mirror.activeServiceId) {
        val ctx = Mirror.appContext ?: return
        CoroutineScope(Dispatchers.IO).launch {
            try {
                val sender = Mirror.sender(ctx)
                Mirror.currentOpenConvoId = convoId
                val launched = sender.startAppOnTheWatch(Schema.WATCH_UUID, watches = null)
                LogStore.i(TAG, "forceOpenWatch startAppOnTheWatch -> $launched (view=$openView, owner=$ownerServiceId)")
                delay(1200)
                // Hand the whole PN sequence to the service: switch active account
                // to the owner, refresh that account's list + rail, then push the
                // enhanced frame carrying the convo's row index (KEY_PN_ROW).
                PebbleListenerService.instance?.pushForceOpen(convoId, openView, ownerServiceId)
            } catch (e: Exception) {
                LogStore.e(TAG, "forceOpenWatch failed", e)
            }
        }
    }

    private fun extractMessageText(m: JSONObject): String {
        val infos = m.optJSONArray("messageInfo") ?: return "New message"
        var text = ""
        var hasImage = false
        // Scan ALL parts (don't early-return): a message can carry both an image
        // and a text caption, and we want to show both.
        for (j in 0 until infos.length()) {
            val info = infos.getJSONObject(j)
            val c = info.optJSONObject("messageContent")?.optString("content") ?: ""
            if (c.isNotEmpty() && text.isEmpty()) text = c.take(120)
            if (info.optJSONObject("mediaContent") != null) hasImage = true
        }
        // First stage of an image hasn't attached the media yet but carries the
        // AUTO_DOWNLOADING status — still label it an image so notify mode shows
        // "[Image]" instead of "New message".
        if (!hasImage) {
            val status = m.optJSONObject("messageStatus")?.optString("status") ?: ""
            if (status == "INCOMING_AUTO_DOWNLOADING") hasImage = true
        }
        return when {
            // Image + caption: "[Image]" on its own line, caption beneath.
            hasImage && text.isNotEmpty() -> "[Image]\n$text"
            hasImage                      -> "[Image]"
            text.isNotEmpty()             -> text
            else                          -> "New message"
        }
    }
}
