package com.dano.mirrormsg

import android.app.Activity
import android.content.ContentValues
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.database.ContentObserver
import android.net.Uri
import android.os.Handler
import android.os.HandlerThread
import org.json.JSONArray
import org.json.JSONObject

/**
 * BeeperServiceBase — shared implementation of [MessagingService] for every
 * Beeper-backed network (Discord, Google Voice, and more to come). It wraps NO
 * bridge AAR: it reads and writes through the Beeper app's on-device Content
 * Provider (authority com.beeper.api), so its only dependency is the Beeper app
 * (com.beeper.android) being installed, logged in, and having the relevant
 * network connected.
 *
 * A concrete account is a TINY subclass that overrides exactly three values —
 * [id], [displayName], [protocol] — and nothing else. All behavior (live
 * polling + observer, image/video/missed-image handling, send, the shared
 * live-ingest seam, conversation/message shaping) lives here, so a change to
 * common Beeper behavior is a single edit that every account inherits.
 *
 * Everything derived from the three values (log prefix, observer-thread name,
 * conversation type tag) is computed here so subclasses stay declaration-only.
 *
 * MODULARITY — the core never imports concrete subclasses; [ServiceRegistry]
 * finds them by class-name string via reflection (each needs a no-arg
 * constructor), so deleting one subclass file removes that network cleanly (it
 * then shows as Locked). There is no AAR to remove.
 *
 * SHAPES — listConversations()/fetchMessages() return the exact JSON the watch
 * pipeline already parses, so the watch renderer and notification gates are
 * untouched. Live updates use a polled ContentObserver on the chats URI, then
 * route each new message through the shared live-ingest seam
 * (LiveRouter.ingestLiveMessage) — the same notify path Google Messages uses,
 * so all three modes work and it fires while backgrounded.
 */
abstract class BeeperServiceBase : MessagingService {

    // Per-account identity — the ONLY things a concrete Beeper network overrides.
    abstract override val id: String           // e.g. "discord", "gvoice"
    abstract override val displayName: String  // e.g. "Discord", "Google Voice"
    protected abstract val protocol: String    // Beeper provider filter, e.g. "discordgo"

    // Everything below is shared. Logs use the core tag (so they reach the
    // exportable in-app log) and are prefixed per-account ("Beeper-<id>:") so a
    // 12-account log is still greppable by network.
    private val TAG = "MirrorMsgSpike"
    private val logPrefix: String get() = "Beeper-$id:"

    companion object {
        // Process-wide: the distinct-protocol diagnostic logs once total, not
        // once per Beeper account.
        @Volatile private var protocolsLogged = false
    }

    private val ctx: Context get() = Mirror.appContext!!

    // ---- Beeper Content Provider constants ---------------------------------
    private val CHATS_URI = "content://com.beeper.api/chats"
    private val MSGS_URI  = "content://com.beeper.api/messages"
    private val BEEPER_PKG = "com.beeper.android"
    private val READ_PERM = "com.beeper.android.permission.READ_PERMISSION"
    private val SEND_PERM = "com.beeper.android.permission.SEND_PERMISSION"

    // Beeper timestamps are MILLISECONDS; Google (the reference renderer) uses
    // MICROSECONDS. Scale ms -> us so Beeper times render alongside the others.
    // Flip to 1L if Beeper message times display wrong on the watch.
    private val TS_SCALE = 1000L

    // Live-update plumbing. The observer fires on ANY chats change (no per-row
    // detail), so we keep the last-seen timestamp per room and only pump rooms
    // whose timestamp advanced past their baseline.
    @Volatile private var observer: ContentObserver? = null
    @Volatile private var observerThread: HandlerThread? = null
    @Volatile private var observerHandler: Handler? = null
    @Volatile private var pollRunnable: Runnable? = null
    // Last isLoggedIn() value reflected on the watch switcher. Used to detect a
    // login/logout transition between poll cycles. null = not yet known.
    @Volatile private var lastLoggedIn: Boolean? = null
    private val lastSeenTs =
        java.util.Collections.synchronizedMap(HashMap<String, Long>())

    // Beeper does NOT reliably notifyChange() the chats observer for live
    // messages (observed: zero onChange callbacks across an active session), so
    // the observer alone leaves the live pump — and therefore PNs — starved. We
    // poll the same diff path on this fixed interval as the dependable backbone;
    // the observer stays registered as a free accelerant for whenever it DOES
    // fire. Both paths call the identical onChatsChanged().
    private val POLL_INTERVAL_MS = 4000L

    // Grace period to let a notification-captured image land before we route a
    // freshly-polled image message. The content-provider poll can beat the
    // notification capture by a few hundred ms; without this we'd prematurely
    // label a perfectly-capturable image "[missed image]". 667ms covers the
    // observed gap with margin; only uncaptured-image messages wait — text and
    // already-captured images route instantly.
    private val IMG_GRACE_MS = 667L

    // ---- Identity & state --------------------------------------------------

    // Logged in == the user has switched Beeper on (master), this account on,
    // AND the network is actually connected in Beeper (has chats). The presence
    // check is what stops a supported-but-unconnected network (e.g. WhatsApp the
    // user hasn't linked) from advertising itself to the watch switcher.
    override fun isLoggedIn(): Boolean =
        BeeperState.isAccountActive(ctx, id) && BeeperState.isPresent(ctx, protocol)

    /** Does the user actually have THIS network connected in Beeper? A UI hint
     *  for greying a supported-but-absent account. Public so the Networks-tab UI
     *  can ask without seeing the protected `protocol`. */
    fun isPresentInBeeper(c: Context): Boolean = BeeperState.isPresent(c, protocol)

    // Mutual-exclusion (native<->Beeper): "active" = master + this account enabled
    // AND the account is actually present in Beeper. Presence matters here because
    // an account that isn't in Beeper can't be fighting the native twin for the
    // upstream (e.g. Meta) connection — so it must not block native login. This is
    // the ONLY caller of isActive (ServiceRegistry.conflictingActive); the watch
    // switcher + routing use isLoggedIn, which is already presence-gated, so this
    // change does not touch them. Tradeoff (Dan's call): if you enable a Beeper
    // account before Beeper has synced it (present=false), it won't pre-block the
    // native twin during that sync window — accepted, because the old enabled-only
    // gate silently blocked native on stale/never-present enabled state, with the
    // account's own switch showing OFF (the upgrade-install bug we just hit).
    override fun isActive(ctx: Context): Boolean =
        BeeperState.isAccountActive(ctx, id) && isPresentInBeeper(ctx)

    override fun accountLabel(): String = "via Beeper"

    // ---- Lifecycle ---------------------------------------------------------

    override fun connectIfPossible(): Boolean {
        // Token diagnostic runs BEFORE the enable gate: it only needs Beeper
        // present (not any account toggled on), and it dumps every protocol the
        // provider reports. This breaks a chicken-and-egg — a wrong-token account
        // reads as "not present", which disables its toggle, which would block the
        // connect that the dump otherwise rode on. Now the real token always
        // surfaces on app start.
        if (BeeperState.isInstalled(ctx) && BeeperState.hasPermissions(ctx)) {
            logDistinctProtocolsOnce()
        }
        if (!BeeperState.isAccountActive(ctx, id)) {
            LogStore.d(TAG, "$logPrefix connect skipped (installed=${BeeperState.isInstalled(ctx)} " +
                "perms=${BeeperState.hasPermissions(ctx)} master=${BeeperState.masterEnabled(ctx)} " +
                "acct=${BeeperState.isAccountEnabled(ctx, id)})")
            return false
        }
        if (observer == null) {
            primeLastSeen()
            registerObserver()
            LogStore.i(TAG, "$logPrefix $displayName connected, observing chats")
        }
        return true
    }

    // Diagnostic: dump the distinct `protocol` values Beeper's provider actually
    // reports (no protocol filter), ONCE per process. This is how we learn the
    // exact protocol token for a network — Beeper reports the LOCAL bridge name,
    // which isn't always the friendly name (Discord = "discordgo"). When adding a
    // network, check this log line for its real token.
    private fun logDistinctProtocolsOnce() {
        if (protocolsLogged) return
        protocolsLogged = true
        val context = Mirror.appContext ?: return
        try {
            val uri = Uri.parse("$CHATS_URI?limit=200")
            context.contentResolver.query(uri, null, null, null, null)?.use { c ->
                val idProto = c.getColumnIndex("protocol")
                val seen = LinkedHashSet<String>()
                while (c.moveToNext()) {
                    val p = if (idProto >= 0) c.getString(idProto) else null
                    if (!p.isNullOrEmpty()) seen.add(p)
                }
                LogStore.i(TAG, "$logPrefix provider protocols seen = ${seen.joinToString()}")
            }
        } catch (e: Exception) {
            LogStore.w(TAG, "$logPrefix protocol dump failed: ${e.message}")
        }
    }

    override fun disconnect() = unregisterObserver()

    override fun logout(context: Context) {
        // Beeper owns the actual session; we just stop observing. The user
        // disconnects this network inside the Beeper app.
        unregisterObserver()
        lastSeenTs.clear()
    }

    // ---- Login (= ensure Beeper + permissions) -----------------------------

    override fun startLogin(activity: Activity) {
        if (!beeperInstalled()) {
            LogStore.w(TAG, "$logPrefix app not installed; opening store listing")
            try {
                activity.startActivity(
                    Intent(Intent.ACTION_VIEW,
                        Uri.parse("https://play.google.com/store/apps/details?id=$BEEPER_PKG")))
            } catch (_: Exception) {}
            return
        }
        val need = listOf(READ_PERM, SEND_PERM).filter {
            activity.checkSelfPermission(it) != PackageManager.PERMISSION_GRANTED
        }
        if (need.isNotEmpty()) {
            LogStore.i(TAG, "$logPrefix requesting permissions ${need.joinToString()}")
            activity.requestPermissions(need.toTypedArray(), 9110)
        } else {
            connectIfPossible()
        }
    }

    // ---- Universal data surface (JSON in/out) ------------------------------

    override fun listConversations(count: Int): String {
        return try {
            val context = Mirror.appContext ?: return errStr("no context")
            val limit = count.coerceIn(1, 200)
            val uri = Uri.parse("$CHATS_URI?protocol=$protocol&limit=$limit")
            val cur = context.contentResolver.query(uri, null, null, null, null)
                ?: return errStr("provider unavailable")
            val rows = ArrayList<JSONObject>()
            cur.use { c ->
                val idRoom   = c.getColumnIndex("roomId")
                val idTitle  = c.getColumnIndex("title")
                val idPrev   = c.getColumnIndex("messagePreview")
                val idUnread = c.getColumnIndex("unreadCount")
                val idTs     = c.getColumnIndex("timestamp")
                while (c.moveToNext()) {
                    val rid = (if (idRoom >= 0) c.getString(idRoom) else null) ?: continue
                    val title = (if (idTitle >= 0) c.getString(idTitle) else null) ?: ""
                    val prev  = (if (idPrev >= 0) c.getString(idPrev) else null) ?: ""
                    val unread = idUnread >= 0 && c.getInt(idUnread) > 0
                    val tsMs = if (idTs >= 0) c.getLong(idTs) else 0L
                    rows.add(
                        JSONObject()
                            .put("conversationID", rid)
                            .put("name", title.ifEmpty { "(no name)" })
                            .put("type", id.uppercase())
                            .put("unread", unread)
                            .put("_ts", tsMs)
                            .put("latestMessage", JSONObject().put("displayContent", prev))
                    )
                }
            }
            val sorted = rows.sortedByDescending { it.optLong("_ts", 0L) }
            val arr = JSONArray()
            for (cnv in sorted) arr.put(cnv)
            JSONObject().put("conversations", arr).toString()
        } catch (e: Throwable) { errJson(e) }
    }

    // Backfill via OFFSET pagination. The Beeper provider ignores every before/id
    // anchor (verified) but honors `offset` on the messages query: offset=N skips the
    // N newest and returns older ones, up to Beeper's retained depth. So the opaque
    // cursor the generic backfill hands back is {"offset":N}:
    //   cursor "" (initial open) -> offset 0, take `count` newest; cursor advances to
    //     the rows returned. Backfill cursor N -> offset N, next older `count`; advance
    //     by rows actually returned (handles the partial batch at the floor cleanly).
    //   0 rows -> past Beeper's floor for this chat -> backfillExhausted.
    // Keyed by the VERBATIM conversationId (must match currentOpenConvoId for the
    // generic lookup). count is the limit (backfill sends BACKFILL_BATCH=80).
    override fun fetchMessages(conversationId: String, count: Long, cursor: String): String {
        return try {
            val offset = if (cursor.isEmpty()) 0 else
                try { JSONObject(cursor).optInt("offset", 0) } catch (_: Exception) { 0 }
            val msgs = fetchRoomMessages(conversationId, count.toInt().coerceIn(1, 500), offset)
            if (msgs.isEmpty()) {
                Mirror.backfillExhausted[conversationId] = true
            } else {
                Mirror.backfillCursor[conversationId] =
                    JSONObject().put("offset", offset + msgs.size).toString()
                Mirror.backfillExhausted[conversationId] = false
            }
            val arr = JSONArray()
            for (m in msgs) arr.put(m)
            JSONObject().put("messages", arr).toString()
        } catch (e: Throwable) { errJson(e) }
    }

    override fun sendMessage(conversationId: String, text: String, replyToken: String): String {
        return try {
            val context = Mirror.appContext ?: return errStr("no context")
            // insert() uses SINGULAR roomId (query uses plural roomIds); roomId +
            // text ride in the URI; an EMPTY (non-null) ContentValues is required
            // or the cross-process marshalling NPEs.
            val uri = Uri.parse(
                "$MSGS_URI?roomId=" + Uri.encode(conversationId) +
                "&text=" + Uri.encode(text))
            val result = context.contentResolver.insert(uri, ContentValues())
            if (result == null) {
                errStr("send failed")
            } else {
                JSONObject()
                    .put("ok", true)
                    .put("messageId", result.getQueryParameter("messageId") ?: "")
                    .toString()
            }
        } catch (e: Throwable) { errJson(e) }
    }


    override fun replyTokenFor(conversationId: String): String = ""  // roomId is the convoId

    // Watch parked on an image row -> REQ_IMAGE -> here. mediaId is the cache
    // key we stashed in mediaContent; load the cached PNG and scale it for the
    // watch. decryptionKeyB64 is unused (our bytes are already local + decoded).
    override fun downloadImage(
        mediaId: String, decryptionKeyB64: String,
        maxW: Int, maxH: Int, maxBytes: Int, quality: Int
    ): String {
        val ctx = Mirror.appContext ?: return errStr("no context")
        return BeeperImages.scaleForWatch(ctx, mediaId, maxW, maxH, maxBytes, quality)
    }

    // Beeper images always travel as a mediaID (cache key), so the watch's
    // call-site selector routes to downloadImage, never here.
    override fun processInlineImage(
        mediaDataB64: String,
        maxW: Int, maxH: Int, maxBytes: Int, quality: Int
    ): String = errStr("beeper uses downloadImage (cache-key path)")

    // ---- Message mapping ---------------------------------------------------

    // Query a room's most-recent messages and map each to the nested watch shape
    // (messageID / senderParticipant.{isMe,firstName} / timestamp / messageInfo[]
    // .messageContent.content), sorted oldest-first. Used by both fetchMessages()
    // and the live observer pump.
    // Set by fetchRoomMessages when a reaction set actually changed during that
    // fetch; read ONLY by the live-pump path to fire onLiveMessage. Reset on each
    // fetch entry. Single-threaded per observer thread (the only live caller); the
    // open-fetch path never reads it.
    @Volatile private var reactionsChangedInFetch = false

    private fun fetchRoomMessages(roomId: String, count: Int, offset: Int = 0): List<JSONObject> {
        val out = ArrayList<JSONObject>()
        reactionsChangedInFetch = false
        val context = Mirror.appContext ?: return out
        val uri = Uri.parse("$MSGS_URI?roomIds=" + Uri.encode(roomId) + "&limit=$count" +
            (if (offset > 0) "&offset=$offset" else ""))
        try {
            val cur = context.contentResolver.query(uri, null, null, null, null) ?: return out
            cur.use { c ->
                val idRoom = c.getColumnIndex("roomId")
                val idOrig = c.getColumnIndex("originalId")
                val idTs   = c.getColumnIndex("timestamp")
                val idMe   = c.getColumnIndex("isSentByMe")
                val idDel  = c.getColumnIndex("isDeleted")
                val idText = c.getColumnIndex("text_content")
                val idName = c.getColumnIndex("displayName")
                val idImg  = c.getColumnIndex("image_path")
                val idVid  = c.getColumnIndex("video_path")
                val idReact = c.getColumnIndex("reactions")
                while (c.moveToNext()) {
                    if (idDel >= 0 && c.getInt(idDel) == 1) continue
                    val rid = (if (idRoom >= 0) c.getString(idRoom) else null) ?: roomId
                    val tsMs = if (idTs >= 0) c.getLong(idTs) else 0L
                    val isMe = idMe >= 0 && c.getInt(idMe) == 1
                    val text = (if (idText >= 0) c.getString(idText) else null) ?: ""
                    val orig = if (idOrig >= 0) c.getString(idOrig) else null
                    // messageID MUST be non-empty (MessageStore skips empty ids).
                    val mid = orig?.takeIf { it.isNotEmpty() } ?: "beeper_${rid}_$tsMs"
                    // Reactions ride on the message row (column "reactions",
                    // comma-separated "emoji|senderId|isSentByMe|order"). This is
                    // the full current set per message WITH per-actor identity, so
                    // we replaceAll wholesale (handles add/change/remove-to-empty).
                    // Keyed by `mid` — byte-identical to the messageID buildMessage
                    // emits — so the badge lookup at pack time aligns. Only touch
                    // the store when the column exists; an empty value clears.
                    if (idReact >= 0) {
                        val raw = c.getString(idReact) ?: ""
                        val rmap = HashMap<String, String>()
                        if (raw.isNotEmpty()) {
                            for (entry in raw.split(',')) {
                                if (entry.isEmpty()) continue
                                val f = entry.split('|')
                                val emoji = f.getOrNull(0) ?: ""
                                val actor = f.getOrNull(1) ?: ""
                                // actorKey is opaque -> key per (person, emoji) so one
                                // person stacking multiple emoji (Discord/Matrix) gets
                                // a distinct slot each instead of collapsing to the
                                // last. Removal still works: replaceAll gets the full
                                // current set, so a vanished pair is simply absent.
                                // Raw unicode straight through — Pebble-font
                                // substitution happens on read in summaryFor.
                                if (emoji.isNotEmpty() && actor.isNotEmpty()) rmap["$actor|$emoji"] = emoji
                            }
                        }
                        // replaceAll returns true only when the stored set actually
                        // changed; the live pump uses this to push without
                        // over-pushing on every chat change. (Open-fetch ignores it —
                        // it already pushes.)
                        if (ReactionStore.replaceAll(mid, rmap)) reactionsChangedInFetch = true
                        if (rmap.isNotEmpty()) LogStore.i(TAG, "$logPrefix reactions msg=$mid n=${rmap.size}")
                    }
                    val sender = if (isMe) "" else
                        ((if (idName >= 0) c.getString(idName) else null) ?: "")
                    val hasImage = idImg >= 0 && !c.getString(idImg).isNullOrEmpty()
                    val hasVideo = idVid >= 0 && !c.getString(idVid).isNullOrEmpty()
                    // Busy Discord channels expose companion rows per message (the
                    // Matrix event split into primary + secondary metadata/edit/
                    // reaction-holder events): same sender + ~same ts, but NO text,
                    // image, or video. They're not renderable messages — the watch
                    // draws an empty body as "[Image]" and their presence shifts the
                    // sender/body pairing. Drop any row with nothing to show. (Reaction
                    // capture already happened above, keyed by mid, so we don't lose
                    // reactions that ride a secondary row — those were merged before
                    // this skip.)
                    if (text.isEmpty() && !hasImage && !hasVideo) continue
                    if (hasImage || hasVideo) {
                        LogStore.i(TAG, "$logPrefix media row img=$hasImage vid=$hasVideo ts=$tsMs")
                    }
                    out.add(buildMessage(roomId, mid, isMe, sender, tsMs, text, hasImage, hasVideo))
                }
            }
        } catch (e: Exception) {
            LogStore.e(TAG, "$logPrefix fetchRoomMessages failed for $roomId", e)
        }
        out.sortBy { it.optString("timestamp").toLongOrNull() ?: 0L }
        return out
    }

    private fun buildMessage(
        convoId: String, messageId: String, isMe: Boolean, sender: String,
        tsMs: Long, text: String, hasImage: Boolean, hasVideo: Boolean
    ): JSONObject {
        val sp = JSONObject().put("isMe", isMe).put("firstName", sender)

        // Only treat as a viewable image if we actually CAPTURED the bitmap from a
        // Beeper notification (matched by sender + timestamp). The mxc:// in the
        // provider row is unresolvable, so a row BEING an image does not mean we
        // have pixels.
        val captured: JSONObject? = if (hasImage) Mirror.appContext?.let { ctx ->
            BeeperImages.mediaContentFor(ctx, sender, tsMs)
        } else null

        // Body text. Media we can't show gets an honest label instead of an empty
        // "New Message", and carries NO mediaContent — so it routes as a plain text
        // notification, never a force-open into an empty enhanced view:
        //   captured image -> real mediaContent attached (force-opens + shows),
        //                     caption kept as the body.
        //   uncaptured img -> "[missed image]" (+ caption), no mediaContent.
        //   video          -> "[video]" (+ caption); we never receive video bytes.
        val body = when {
            hasVideo                     -> tagged("[video]", text)
            hasImage && captured == null -> tagged("[missed image]", text)
            else                         -> text
        }

        val info = JSONObject().put("messageContent", JSONObject().put("content", body))
        if (captured != null) info.put("mediaContent", captured)

        // conversationID is REQUIRED on the live path: handleLiveMessage reads it
        // off each message JSON to route + notify (and early-returns without it).
        // Harmless on the history path. = the Beeper roomId, same as Google embeds
        // its convo id, so every message matches the Google schema the core reads.
        return JSONObject()
            .put("messageID", messageId)
            .put("conversationID", convoId)
            .put("senderParticipant", sp)
            .put("timestamp", (tsMs * TS_SCALE).toString())
            .put("messageInfo", JSONArray().put(info))
    }

    // "[tag] caption" when a caption exists, else just the tag.
    private fun tagged(tag: String, caption: String): String =
        if (caption.isBlank()) tag else "$tag $caption"

    // ---- Live updates ------------------------------------------------------

    // Record current per-room timestamps WITHOUT emitting, so connect doesn't
    // replay all history as "new". Only changes after this baseline fire.
    private fun primeLastSeen() {
        val context = Mirror.appContext ?: return
        try {
            val uri = Uri.parse("$CHATS_URI?protocol=$protocol&limit=200")
            context.contentResolver.query(uri, null, null, null, null)?.use { c ->
                val idRoom = c.getColumnIndex("roomId")
                val idTs   = c.getColumnIndex("timestamp")
                while (c.moveToNext()) {
                    val rid = (if (idRoom >= 0) c.getString(idRoom) else null) ?: continue
                    lastSeenTs[rid] = if (idTs >= 0) c.getLong(idTs) else 0L
                }
            }
        } catch (e: Exception) {
            LogStore.e(TAG, "$logPrefix primeLastSeen failed", e)
        }
    }

    private fun registerObserver() {
        val context = Mirror.appContext ?: return
        val ht = HandlerThread("beeper-$id-observer").also { it.start() }
        observerThread = ht
        val h = Handler(ht.looper)
        observerHandler = h
        val obs = object : ContentObserver(h) {
            override fun onChange(selfChange: Boolean) { onChatsChanged() }
            override fun onChange(selfChange: Boolean, uri: Uri?) { onChatsChanged() }
        }
        observer = obs
        try {
            context.contentResolver.registerContentObserver(
                Uri.parse(CHATS_URI), true, obs)
        } catch (e: Exception) {
            LogStore.e(TAG, "$logPrefix registerContentObserver failed", e)
        }
        startPolling(h)
    }

    // The reliable backbone for live updates: re-run the chats diff every
    // POLL_INTERVAL_MS on the observer's own thread. Re-posts itself at the END
    // of each tick (not a fixed rate), so a slow tick can't pile up. Stopped in
    // unregisterObserver().
    // If this account's logged-in state changed since the last poll cycle — it
    // gained its first chat / was linked in Beeper (false->true), or was unlinked
    // (true->false) — re-push the watch switcher rail so the account appears or
    // disappears without a manual toggle. refreshSwitcher() is background-safe and
    // self-diffing, so an unchanged state is a harmless no-op. It re-pushes the
    // rail only; selecting an account still drives its conversation load as usual.
    private fun checkSwitcherTransition() {
        val now = isLoggedIn()
        if (lastLoggedIn != now) {
            lastLoggedIn = now
            LogStore.i(TAG, "$logPrefix logged-in -> $now; refreshing watch switcher")
            PebbleListenerService.instance?.refreshSwitcher()
        }
    }

    private fun startPolling(h: Handler) {
        LogStore.i(TAG, "$logPrefix polling chats every ${POLL_INTERVAL_MS}ms")
        val r = object : Runnable {
            override fun run() {
                try { onChatsChanged() } catch (e: Exception) {
                    LogStore.e(TAG, "$logPrefix poll tick failed", e)
                }
                try { checkSwitcherTransition() } catch (e: Exception) {
                    LogStore.e(TAG, "$logPrefix switcher transition check failed", e)
                }
                observerHandler?.postDelayed(this, POLL_INTERVAL_MS)
            }
        }
        pollRunnable = r
        h.postDelayed(r, POLL_INTERVAL_MS)
    }

    private fun unregisterObserver() {
        try { observer?.let { ctx.contentResolver.unregisterContentObserver(it) } }
        catch (_: Exception) {}
        observer = null
        pollRunnable?.let { r -> observerHandler?.removeCallbacks(r) }
        pollRunnable = null
        observerHandler = null
        lastLoggedIn = null
        try { observerThread?.quitSafely() } catch (_: Exception) {}
        observerThread = null
    }

    // Runs on the observer's background thread. Re-query this network's chats, and for
    // any room whose timestamp advanced past its baseline, pull its newest
    // messages and push them through the shared store + watch path.
    private fun onChatsChanged() {
        val context = Mirror.appContext ?: return
        try {
            val uri = Uri.parse("$CHATS_URI?protocol=$protocol&limit=100")
            context.contentResolver.query(uri, null, null, null, null)?.use { c ->
                val idRoom = c.getColumnIndex("roomId")
                val idTs   = c.getColumnIndex("timestamp")
                var advanced = 0  // BEEPER-OBS-DIAG
                while (c.moveToNext()) {
                    val rid = (if (idRoom >= 0) c.getString(idRoom) else null) ?: continue
                    val tsMs = if (idTs >= 0) c.getLong(idTs) else 0L
                    val prev = lastSeenTs[rid] ?: 0L
                    if (tsMs > prev) {
                        advanced++  // BEEPER-OBS-DIAG
                        LogStore.i(TAG, "$logPrefix $rid advanced ts=$tsMs prev=$prev — pumping")
                        lastSeenTs[rid] = tsMs
                        pumpRoom(rid, prev)
                    }
                }
                // BEEPER-OBS-DIAG: proves the chats observer actually FIRED (and
                // whether it saw any advance). No line at all when a message arrives
                // = observer never woke = Beeper isn't notifying us for this protocol.
                LogStore.i(TAG, "$logPrefix BEEPER-OBS-DIAG onChatsChanged fired, advanced=$advanced")
            }
        } catch (e: Exception) {
            LogStore.e(TAG, "$logPrefix onChatsChanged failed", e)
        }
    }

    // Route a room's genuinely-new messages through the shared live-ingest seam
    // (LiveRouter.ingestLiveMessage) — one at a time, oldest-first — so each gets
    // the full pipeline: store-merge, convo-list refresh, thread push, the notify
    // gates, and all three modes (notification / hybrid force-open-for-pictures /
    // force-open) for THIS account. Background-safe: LiveRouter is static and
    // app-context-driven, so it fires whether or not MirrorMsg is foregrounded.
    private fun pumpRoom(roomId: String, sinceTsMs: Long) {
        pumpRoomInternal(roomId, sinceTsMs, allowDefer = true)
    }

    private fun pumpRoomInternal(roomId: String, sinceTsMs: Long, allowDefer: Boolean) {
        // Only messages newer than the room's last-seen baseline, so one
        // onChange() doesn't re-notify the last N already-seen messages. Compare
        // in the emitted unit (us = ms * TS_SCALE) and feed oldest -> newest.
        val sinceUs = sinceTsMs * TS_SCALE
        val fresh = fetchRoomMessages(roomId, 10)
            .filter { (it.optString("timestamp").toLongOrNull() ?: 0L) > sinceUs }
            .sortedBy { (it.optString("timestamp").toLongOrNull() ?: 0L) }
        // fetchRoomMessages just set this for ALL fetched rows (incl. old ones whose
        // reaction set changed). Capture before any further work.
        val reactionsChanged = reactionsChangedInFetch
        if (fresh.isEmpty()) {
            // No new messages, but a reaction on an existing message may have
            // changed — push so the badge refreshes live on the open thread. (When
            // there ARE fresh messages, ingesting them re-pushes the thread and the
            // reaction rides along in the pack, so we only push here in the empty
            // case to avoid double-pushing.) Live only fires when something actually
            // re-runs fetchRoomMessages for this room (chats observer), since the
            // messages observer never fires — otherwise it stays appear-on-open.
            if (reactionsChanged) PebbleListenerService.onLiveMessage(roomId)
            return
        }

        // The poll can beat the notification capture by a few hundred ms. If a
        // just-arrived image isn't cached yet, hold the batch one short grace and
        // re-pump — by then the capture has landed and the real image attaches
        // instead of a premature "[missed image]". Each message routes exactly
        // ONCE (we defer the batch rather than ingest-now-and-again), so the wrist
        // never gets a duplicate. The re-pump can't defer again, so a genuinely
        // uncaptured image still routes as "[missed image]" after the grace.
        if (allowDefer && fresh.any { isUncapturedImage(it) }) {
            observerHandler?.postDelayed({
                pumpRoomInternal(roomId, sinceTsMs, allowDefer = false)
            }, IMG_GRACE_MS)
            return
        }

        for (m in fresh) LiveRouter.ingestLiveMessage(id, m.toString())
    }

    // An image row whose bitmap we haven't captured yet: no mediaContent attached
    // and the body carries the missed-image marker. (Video is never deferred — we
    // never receive video bytes, so there's nothing to wait for.)
    private fun isUncapturedImage(m: JSONObject): Boolean {
        val info = m.optJSONArray("messageInfo")?.optJSONObject(0) ?: return false
        if (info.has("mediaContent")) return false
        val content = info.optJSONObject("messageContent")?.optString("content") ?: return false
        return content.startsWith("[missed image]")
    }

    // ---- helpers -----------------------------------------------------------

    // Detection delegates to BeeperState (the single source of truth shared with
    // the Networks-tab UI). startLogin() still uses these + the *_PERM/PKG consts.
    private fun beeperInstalled(): Boolean = BeeperState.isInstalled(ctx)
    private fun hasPerms(): Boolean = BeeperState.hasPermissions(ctx)

    private fun errStr(msg: String) = "{\"error\":\"$msg\"}"
    private fun errJson(e: Throwable) =
        "{\"error\":${JSONObject.quote(e.message ?: "beeper error")}}"
}
