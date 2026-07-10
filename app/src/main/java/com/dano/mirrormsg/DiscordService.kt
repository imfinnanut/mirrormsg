package com.dano.mirrormsg

import android.app.Activity
import android.content.Context
import android.content.Intent
import fi.mirrormsg.discordbridge.Bridge as DcBridge
import fi.mirrormsg.discordbridge.Discordbridge
import fi.mirrormsg.discordbridge.LiveSink

/**
 * DiscordService — native Discord over the gateway (github.com/beeper/discordgo, a
 * BSD-licensed fork), wrapped by the `discordbridge` AAR. Same combined-go/gomobile
 * shape as the other native bridges, and independent of Beeper: a real Discord
 * client session, no second app required.
 *
 * MILESTONE STATE: DMs + opted-in servers. Token login + session persistence,
 * listConversations, fetchMessages, sendMessage, reactions (send + live add/remove
 * + historic view + fetch reconcile), quoted reply-to (send + view), and images
 * (image attachment -> mediaContent -> shared mirrorbridge encoder) are all live.
 * Other media surfaces as a text label ([Video]/[Voice]/[GIF]/[Sticker]/[Link]).
 * Servers stay dark until SetEnabledGuilds names them; DMs are always on.
 *
 * The id is "discord"; Beeper's Discord twin is "dcbeeper". conflictGroup "discord"
 * makes the native and Beeper Discord mutually exclusive, so only one "Discord" is
 * ever active in the switcher (same twin pattern as native "tgnative" vs Beeper
 * "telegram"). Users only ever see the displayName "Discord"; the id is internal.
 *
 * isLoggedIn() must be cheap (it's read during UI render), so it reads a persisted
 * flag set on successful login — never a blocking bridge call. connectIfPossible()
 * is where the gateway actually opens, on a background thread.
 */
class DiscordService : MessagingService {

    override val id = "discord"
    override val displayName = "Discord"
    override val conflictGroup = "discord"

    // Discord blurple family (under ~140 luma -> white initials on the watch).
    override val avatarPalette = listOf("#5865F2", "#4752C4", "#3C45A5", "#5B6EAE")

    override fun isLoggedIn(): Boolean =
        Mirror.appContext?.let { prefs(it).getBoolean(KEY_AUTHED, false) } ?: false

    override fun accountLabel(): String =
        Mirror.appContext?.let { prefs(it).getString(KEY_USERNAME, "") ?: "" } ?: ""

    /**
     * The logged-in Discord user id, read from prefs (cheap, no bridge call). Used
     * so a reaction the user sends from the watch is stored optimistically under the
     * SAME key Discord's own reaction events carry, letting a later change/removal
     * reconcile it (same pattern as Facebook's fbid).
     */
    override fun selfActorKey(): String? =
        Mirror.appContext?.let { prefs(it).getString(KEY_SELFID, "")?.ifEmpty { null } }

    override val supportsReactionSend = true   // gateway MessageReactionAdd/Remove
    override val supportsReplyTo = true         // ChannelMessageSendReply (message reference)

    // Discord marks read via ChannelMessageAck (per-channel high-water mark), so
    // acking the newest message clears the channel's unread + mention badge.
    override val supportsMarkRead = true

    /**
     * Bring Discord online: start the gateway with the persisted token and wait
     * briefly for READY so the first conversation list isn't empty. Blocks
     * (interface contract: safe on a background thread). Deliberately does NOT clear
     * the authed flag on a transient not-ready — a valid token that's momentarily
     * offline is still "logged in"; only an explicit logout clears it.
     */
    override fun connectIfPossible(): Boolean {
        val ctx = Mirror.appContext ?: return false
        val token = prefs(ctx).getString(KEY_TOKEN, "") ?: ""
        if (token.isEmpty()) return false
        val err = bridge().start(token)   // "" if ok / already started
        if (err.contains("dcError")) LogStore.w(TAG, "DC start: ${err.take(160)}")
        var status = bridge().connectStatus()
        var tries = 0
        while (status != "READY" && tries < 30) {   // up to ~3s
            Thread.sleep(100); status = bridge().connectStatus(); tries++
        }
        val ok = status == "READY"
        if (ok) {
            prefs(ctx).edit().putBoolean(KEY_AUTHED, true).apply()
            applyEnabledGuilds(ctx)   // gate live server-channel messages to opted-in servers
        } else LogStore.i(TAG, "DC connectIfPossible: $status")
        return ok
    }

    override fun disconnect() {
        synchronized(lock) {
            dcBridge?.close()
            dcBridge = null
        }
    }

    override fun logout(context: Context) {
        disconnect()
        prefs(context).edit()
            .remove(KEY_AUTHED).remove(KEY_TOKEN).remove(KEY_USERNAME).remove(KEY_SELFID)
            .apply()
        LogStore.i(TAG, "DC logged out")
    }

    override fun startLogin(activity: Activity) {
        activity.startActivity(Intent(activity, DiscordLoginActivity::class.java))
    }

    // ---- Universal data surface --------------------------------------------

    override fun listConversations(count: Int): String {
        // A server sub-account is active -> list that server's channels; otherwise
        // the base Discord account shows DMs only (the "Direct Messages" bucket).
        val guild = Mirror.activeDiscordGuild
        val json = try {
            if (guild.isNotEmpty()) bridge().listGuildChannels(guild)
            else bridge().listConversations(count.toLong())
        } catch (e: Throwable) {
            LogStore.w(TAG, "DC listConversations failed: ${e.message}")
            return "{\"conversations\":[]}"
        }
        return if (json.startsWith("{")) json else "{\"conversations\":[]}"
    }

    // Discord is a hierarchy: root -> "Direct Messages" (bucket) + shown servers ->
    // that server's channels. The watch drills via KEY_REQ_ENTER; the phone tracks
    // the path in Mirror.navStack and asks for each level here.
    override val hierarchical = true

    /**
     * A server-channel force-open lands in that server's channel list (so back-out
     * climbs channel -> server channels -> root); a DM lands in the DMs bucket. The
     * bridge's channel->guild map resolves which. Falls back to DMs when unknown
     * (gateway not ready), which still opens the thread — just backs out to DMs.
     */
    override fun forceOpenNavContext(convoId: String): List<String> {
        val channelId = convoId.removePrefix("discord:")
        val guildId = try { bridge().guildForChannel(channelId) } catch (_: Throwable) { "" }
        return if (guildId.isNotEmpty()) listOf("guild:$guildId") else listOf("dms")
    }

    /**
     * Detail-view banner label. A server channel resolves to "Discord\nServer#channel";
     * a DM (no guild mapping) falls back to just "Discord". Reuses the same bridge-side
     * Server#channel logic that builds the notification group name.
     */
    override fun threadLabel(convoId: String): String {
        val channelId = convoId.removePrefix("discord:")
        val sc = try { bridge().threadLabel(channelId) } catch (_: Throwable) { "" }
        return if (sc.isNotEmpty()) "$displayName\n$sc" else displayName
    }

    override fun listContext(count: Int, context: List<String>): String {
        // Resolve against the CURRENT (deepest) context token; each token is
        // self-identifying, so arbitrary depth (server -> forum -> ...) works.
        val cur = context.lastOrNull()
        return when {
            cur == null -> discordRootJson()          // root: DMs + shown servers
            cur == "dms" -> {                          // DM conversations (OPEN)
                val json = try { bridge().listConversations(count.toLong()) }
                catch (e: Throwable) { LogStore.w(TAG, "DC DMs failed: ${e.message}"); "{\"conversations\":[]}" }
                if (json.startsWith("{")) json else "{\"conversations\":[]}"
            }
            cur.startsWith("guild:") -> {              // a server's channels (forums = ENTER)
                val gid = cur.removePrefix("guild:")
                val json = try { bridge().listGuildChannels(gid) }
                catch (e: Throwable) { LogStore.w(TAG, "DC channels failed: ${e.message}"); "{\"conversations\":[]}" }
                if (json.startsWith("{")) json else "{\"conversations\":[]}"
            }
            cur.startsWith("forum:") -> {              // a forum's posts/threads (OPEN)
                val chId = cur.removePrefix("forum:")
                val json = try { bridge().listForumThreads(chId) }
                catch (e: Throwable) { LogStore.w(TAG, "DC forum threads failed: ${e.message}"); "{\"conversations\":[]}" }
                if (json.startsWith("{")) json else "{\"conversations\":[]}"
            }
            else -> "{\"conversations\":[]}"
        }
    }

    // The Discord root list: an ENTER row for the DMs bucket, then an ENTER row for
    // each server the user has shown (dcserver_enabled_*). Names come from the live
    // guild list once the gateway has READY'd, and are cached so a cold-start query
    // (before the gateway loads guilds) still lists every enabled server — matching
    // Telegram's folders, which its gotd session exposes immediately.
    private fun discordRootJson(): String {
        val ctx = Mirror.appContext ?: return "{\"conversations\":[]}"
        val convos = org.json.JSONArray()
        convos.put(enterEntry("dms", "Direct Msgs"))
        // Enabled servers straight from the live guild list. On a cold start the list
        // is empty until the gateway READYs, so only DMs show until then — we no longer
        // reconstruct the list from a prefs cache (it never worked reliably; the live
        // list repopulates within a few seconds of connecting).
        try {
            val guilds = org.json.JSONObject(guildsJson()).optJSONArray("guilds")
            if (guilds != null) for (i in 0 until guilds.length()) {
                val g = guilds.optJSONObject(i) ?: continue
                val gid = g.optString("id"); if (gid.isEmpty()) continue
                if (!serverEnabled(ctx, gid)) continue
                val name = g.optString("name").ifEmpty { "Server" }
                convos.put(enterEntry("guild:$gid", name))
            }
        } catch (e: Throwable) { LogStore.w(TAG, "DC root build failed: ${e.message}") }
        return org.json.JSONObject().put("conversations", convos).toString()
    }

    private fun enterEntry(context: String, name: String): org.json.JSONObject =
        org.json.JSONObject()
            .put("conversationID", context)   // unused for ENTER, but keeps the shape
            .put("name", name)
            .put("navKind", "ENTER")
            .put("navContext", context)

    override fun fetchMessages(conversationId: String, count: Long, cursor: String): String {
        val json = try {
            bridge().fetchMessages(conversationId, count)
        } catch (e: Throwable) {
            LogStore.w(TAG, "DC fetchMessages failed: ${e.message}")
            return "{\"messages\":[]}"
        }
        return if (json.startsWith("{")) json else "{\"messages\":[]}"
    }

    override fun sendMessage(conversationId: String, text: String, replyToken: String): String {
        val result = try {
            bridge().sendMessage(conversationId, text)
        } catch (e: Throwable) {
            LogStore.e(TAG, "DC send failed", e)
            return "{\"error\":\"${e.message}\"}"
        }
        // The gateway echoes our own send back as a MESSAGE shortly after; insert it
        // optimistically so the thread updates instantly. The real messageID dedups
        // the later echo.
        optimisticEcho(conversationId, text, sentIdOf(result))
        return result
    }

    override fun replyTokenFor(conversationId: String): String = ""   // convoId is the routing

    override fun sendReplyTo(
        conversationId: String, targetMessageId: String,
        targetFromMe: Boolean, text: String
    ): String {
        val result = try {
            bridge().sendReply(conversationId, targetMessageId, text)
        } catch (e: Throwable) {
            LogStore.e(TAG, "DC sendReplyTo failed", e)
            return "{\"error\":\"${e.message}\"}"
        }
        if (!bridgeOk(result)) return "{\"error\":\"reply failed\"}"
        val sentId = sentIdOf(result)
        // Tag the optimistic echo with the reply target so the thread builder composes
        // the "You replied to X" header from the stored quoted message.
        optimisticEcho(conversationId, text, sentId, targetMessageId)
        // Normalize to the shared success shape ({"messageId":...}) that sendTargeted
        // checks; without it the reply is ALSO posted as a plain message.
        return "{\"messageId\":\"${sentId.ifEmpty { targetMessageId }}\"}"
    }

    override fun sendReaction(
        conversationId: String, targetMessageId: String,
        targetFromMe: Boolean, emoji: String
    ): String {
        val result = try {
            // Non-empty emoji => add. Removal needs the specific emoji, which the
            // empty-emoji "clear" contract can't supply on Discord; the shared send
            // path (sendTargeted) only issues adds, so this stays an add.
            bridge().sendReaction(conversationId, targetMessageId, emoji, false)
        } catch (e: Throwable) {
            LogStore.e(TAG, "DC sendReaction failed", e)
            return "{\"error\":\"${e.message}\"}"
        }
        // A reaction creates no new message, and the bridge answers {"ok":true}. Echo
        // the target id as messageId — Telegram's okID(msgID) pattern — so the shared
        // sendTargeted success check passes and the emoji isn't ALSO posted as a message.
        return if (bridgeOk(result)) "{\"messageId\":\"$targetMessageId\"}"
        else "{\"error\":\"reaction rejected\"}"
    }

    // Mark the channel read on Discord up through messageId (ChannelMessageAck).
    // The bridge answers {"ok":true} on success. Errors just log — fire-and-forget.
    override fun markRead(conversationId: String, messageId: String): String {
        return try {
            bridge().markThreadRead(conversationId, messageId)
        } catch (e: Throwable) {
            LogStore.e(TAG, "DC markRead failed", e)
            "{\"error\":\"${e.message}\"}"
        }
    }

    // The Discord bridge answers {"ok":true} on success, {"dcError":...} on failure.
    private fun bridgeOk(result: String): Boolean = try {
        org.json.JSONObject(result).optBoolean("ok", false)
    } catch (_: Throwable) { false }

    private fun sentIdOf(result: String): String =
        try { org.json.JSONObject(result).optString("messageID") } catch (_: Throwable) { "" }

    // Optimistically insert a just-sent message through LiveRouter (the received-
    // message path: stores it, pushes the thread, stays silent because it's ours).
    // Discord timestamps are MICROSECONDS.
    private fun optimisticEcho(conversationId: String, text: String, sentId: String, replyToId: String = "") {
        if (sentId.isEmpty()) return
        try {
            val sp = org.json.JSONObject()
                .put("isMe", true).put("firstName", "").put("fullName", "")
                .put("ID", org.json.JSONObject().put("number", ""))
            val info = org.json.JSONArray().put(
                org.json.JSONObject().put(
                    "messageContent", org.json.JSONObject().put("content", text)
                )
            )
            val msg = org.json.JSONObject()
                .put("conversationID", conversationId)
                .put("messageID", sentId)
                .put("timestamp", (System.currentTimeMillis() * 1000L).toString())
                .put("senderParticipant", sp)
                .put("messageStatus", org.json.JSONObject().put("status", "OUTGOING_DELIVERED"))
                .put("messageInfo", info)
                .put("isMe", true)
                .put("fromMe", true)
            if (replyToId.isNotEmpty()) {
                msg.put("replyTo", org.json.JSONObject().put("id", replyToId))
            }
            LiveRouter.ingestLiveMessage("discord", msg.toString())
        } catch (e: Throwable) {
            LogStore.e(TAG, "DC optimistic echo failed", e)
        }
    }

    // Images: an image attachment surfaces as mediaContent (MediaID = the message id);
    // the bridge cached the CDN url and DownloadImageForWatch fetches + scales it via
    // the shared mirrorbridge encoder, identical to Google/WhatsApp/Telegram.
    override fun downloadImage(
        mediaId: String, decryptionKeyB64: String,
        maxW: Int, maxH: Int, maxBytes: Int, quality: Int
    ): String = try {
        bridge().downloadImageForWatch(
            mediaId, maxW.toLong(), maxH.toLong(), maxBytes.toLong(), quality.toLong()
        )
    } catch (e: Throwable) {
        LogStore.e(TAG, "DC downloadImage failed", e)
        "{\"error\":\"${e.message}\"}"
    }

    override fun processInlineImage(
        mediaDataB64: String,
        maxW: Int, maxH: Int, maxBytes: Int, quality: Int
    ): String = "{\"error\":\"discord has no inline images\"}"

    companion object {
        private const val TAG = "MirrorMsgSpike"
        private const val PREFS = "mirrormsg"
        private const val KEY_AUTHED = "dc_authed"
        private const val KEY_TOKEN = "dc_token"
        private const val KEY_USERNAME = "dc_username"
        private const val KEY_SELFID = "dc_selfid"

        private val lock = Any()
        @Volatile private var dcBridge: DcBridge? = null

        /**
         * The single shared bridge — its gateway session lives process-wide so login
         * and the live pipeline share one connection. Created idle; Start(token)
         * opens the gateway (from connectIfPossible / the login activity).
         */
        fun bridge(): DcBridge {
            dcBridge?.let { return it }
            return synchronized(lock) {
                dcBridge ?: run {
                    Discordbridge.newDiscordBridge().also { b ->
                        // Live events from the gateway dispatcher. MESSAGE routes into
                        // the shared pipeline (store + notify); REACTION is the per-actor
                        // add/remove shape ReactionStore.handleBridgeEvent consumes (then
                        // we re-push the open thread so the badge updates live, never
                        // buzzing the wrist); DIAG surfaces bridge diagnostics.
                        b.setLiveSink(object : LiveSink {
                            override fun onEvent(eventType: String, jsonPayload: String) {
                                when (eventType) {
                                    "MESSAGE" -> LiveRouter.ingestLiveMessage("discord", jsonPayload)
                                    "REACTION" -> {
                                        ReactionStore.handleBridgeEvent(jsonPayload)
                                        val open = Mirror.currentOpenConvoId
                                        if (open != null && Mirror.activeServiceId == "discord") {
                                            PebbleListenerService.instance?.refreshOpenThread(open)
                                        }
                                    }
                                    "DIAG" -> LogStore.i(TAG, jsonPayload)
                                    "GUILDMUTE" -> mainHandler.post {
                                        // Discord pushed a live settings change for
                                        // this guild — repaint its on-screen controls
                                        // (mute + notify level) so the relationship is
                                        // visible in real time.
                                        refreshGuildSettings(jsonPayload)
                                    }
                                    else -> {}
                                }
                            }
                        })
                        dcBridge = b
                    }
                }
            }
        }

        private fun prefs(ctx: Context) =
            ctx.getSharedPreferences(PREFS, Context.MODE_PRIVATE)

        /** Servers (guilds) as JSON, for the Networks-page server panel. */
        fun guildsJson(): String = try { bridge().listGuilds() } catch (e: Throwable) {
            LogStore.w(TAG, "DC guildsJson failed: ${e.message}"); "{\"guilds\":[]}"
        }

        /**
         * Per-server opt-in pref key. Servers default OFF (opt-in) — unlike Telegram
         * folders, which default on — so a fresh login shows DMs only until the user
         * enables specific servers in the Networks tab.
         */
        fun serverEnabledKey(guildId: String) = "dcserver_enabled_$guildId"
        fun serverEnabled(ctx: Context, guildId: String): Boolean =
            prefs(ctx).getBoolean(serverEnabledKey(guildId), false)
        fun setServerEnabled(ctx: Context, guildId: String, on: Boolean) {
            prefs(ctx).edit().putBoolean(serverEnabledKey(guildId), on).apply()
            applyEnabledGuilds(ctx)   // keep the bridge's live-message gate in sync
        }

        /**
         * Discord's REAL per-server mute (one source of truth). Reading reflects what's
         * set in the Discord app; writing PATCHes user_guild_settings so the change
         * shows up in Discord too and the bridge's notification gate honors it. The
         * write blocks on a REST call — call it off the main thread.
         */
        fun serverMutedInDiscord(guildId: String): Boolean =
            try { bridge().isGuildMuted(guildId) } catch (_: Throwable) { false }

        fun setServerMutedInDiscord(guildId: String, muted: Boolean): Boolean =
            try { org.json.JSONObject(bridge().setGuildMute(guildId, muted)).optBoolean("ok", false) }
            catch (_: Throwable) { false }

        /** Discord's real per-server notification level (0=all, 1=@mentions, 2=nothing).
         *  Reading reflects the Discord app; writing PATCHes only message_notifications
         *  so mute and everything else stay intact. gomobile maps Go int to Long. */
        fun serverNotifyLevel(guildId: String): Int =
            try { bridge().guildNotifyLevel(guildId).toInt() } catch (_: Throwable) { -1 }

        fun setServerNotifyLevel(guildId: String, level: Int): Boolean =
            try { org.json.JSONObject(bridge().setGuildNotifyLevel(guildId, level.toLong())).optBoolean("ok", false) }
            catch (_: Throwable) { false }

        /**
         * Live-refresh plumbing for the per-server mute + notify-level buttons. A
         * visible control registers a repaint callback keyed "$guildId#$tag" while it's
         * on screen; a GUILDMUTE push (fired on ANY of that guild's settings changing)
         * invokes every callback for that guild on the main thread, so both controls
         * flip live. Detached controls unregister, so this never holds a stale view.
         */
        private val settingRefreshers = java.util.concurrent.ConcurrentHashMap<String, () -> Unit>()
        private val mainHandler = android.os.Handler(android.os.Looper.getMainLooper())
        fun registerSettingRefresher(key: String, cb: () -> Unit) { settingRefreshers[key] = cb }
        fun unregisterSettingRefresher(key: String) { settingRefreshers.remove(key) }
        fun refreshGuildSettings(guildId: String) {
            settingRefreshers.forEach { (k, cb) -> if (k.substringBefore('#') == guildId) cb() }
        }

        /**
         * CSV of enabled server ids, derived from the per-server prefs over the
         * current server list. Drives the bridge's channelAllowed gate so live
         * messages from non-opted-in servers are dropped.
         */
        fun enabledGuildsCsv(ctx: Context): String {
            val ids = ArrayList<String>()
            try {
                val arr = org.json.JSONObject(guildsJson()).optJSONArray("guilds")
                if (arr != null) for (i in 0 until arr.length()) {
                    val id = arr.optJSONObject(i)?.optString("id").orEmpty()
                    if (id.isNotEmpty() && serverEnabled(ctx, id)) ids.add(id)
                }
            } catch (e: Throwable) { LogStore.w(TAG, "DC enabledGuildsCsv failed: ${e.message}") }
            return ids.joinToString(",")
        }

        /** Push the opted-in server set to the bridge (live-message gate). */
        fun applyEnabledGuilds(ctx: Context) {
            try { bridge().setEnabledGuilds(enabledGuildsCsv(ctx)) }
            catch (e: Throwable) { LogStore.w(TAG, "DC applyEnabledGuilds failed: ${e.message}") }
        }

        /**
         * Called by DiscordLoginActivity once Start(token) reports READY. Persists the
         * token, display name, and self id.
         */
        fun markLoggedIn(ctx: Context, token: String, username: String, selfId: String) {
            prefs(ctx).edit()
                .putBoolean(KEY_AUTHED, true)
                .putString(KEY_TOKEN, token)
                .putString(KEY_USERNAME, username)
                .putString(KEY_SELFID, selfId)
                .apply()
        }
    }
}
