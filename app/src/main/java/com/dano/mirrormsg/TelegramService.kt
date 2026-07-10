package com.dano.mirrormsg

import android.app.Activity
import android.content.Context
import android.content.Intent
import java.io.File
import fi.mirrormsg.telegrambridge.Bridge as TgBridge
import fi.mirrormsg.telegrambridge.Telegrambridge
import fi.mirrormsg.telegrambridge.LiveSink

/**
 * TelegramService — native Telegram DMs over gotd/td (pure-Go MTProto), wrapped by
 * the `telegrambridge` AAR. Same combined-go/gomobile shape as the other native
 * bridges, and independent of Beeper: a real Telegram client session, no second
 * app required.
 *
 * MILESTONE STATE: AUTH ONLY. Login (phone -> code -> 2FA) and session persistence
 * are implemented; listConversations is live; the rest of the data surface (fetchMessages /
 * sendMessage / images) is intentionally stubbed until the read/send milestones.
 *
 * The id is "tgnative", NOT "telegram": Beeper's Telegram twin (BeeperTelegramService)
 * already owns "telegram". conflictGroup "telegram" makes the native and Beeper
 * Telegram mutually exclusive, so only one "Telegram" is ever active in the switcher
 * (same twin pattern as native "whatsapp" vs Beeper "wabeeper"). Users only ever see
 * the displayName "Telegram"; the id is internal.
 *
 * isLoggedIn() must be cheap (it's read during UI render), so it reads a persisted
 * flag set on successful login — never a blocking bridge call. connectIfPossible()
 * is where the actual gotd connect happens, on a background thread.
 */
class TelegramService : MessagingService {

    override val id = "tgnative"
    override val displayName = "Telegram"
    override val conflictGroup = "telegram"

    // Telegram brand blues (under ~140 luma -> white initials on the watch).
    override val avatarPalette = listOf("#0088CC", "#229ED9", "#1C7FC0", "#2775A8")

    override fun isLoggedIn(): Boolean =
        Mirror.appContext?.let { prefs(it).getBoolean(KEY_AUTHED, false) } ?: false

    override fun accountLabel(): String =
        Mirror.appContext?.let { prefs(it).getString(KEY_PHONE, "") ?: "" } ?: ""

    /**
     * Bring Telegram online: creating the bridge starts the gotd run loop and loads
     * any saved session file. Blocks (interface contract: safe on a background
     * thread). Refreshes the persisted auth flag from the real connection result.
     */
    override fun connectIfPossible(): Boolean {
        val status = bridge().connectStatus()
        val ok = status == "authorized"
        Mirror.appContext?.let { prefs(it).edit().putBoolean(KEY_AUTHED, ok).apply() }
        if (!ok) LogStore.i(TAG, "TG connectIfPossible: $status")
        return ok
    }

    override fun disconnect() {
        synchronized(lock) {
            tgBridge?.close()
            tgBridge = null
        }
    }

    override fun logout(context: Context) {
        disconnect()
        prefs(context).edit().remove(KEY_AUTHED).remove(KEY_PHONE).apply()
        File(context.filesDir, SESSION_FILE).delete()
        LogStore.i(TAG, "TG logged out")
    }

    override fun startLogin(activity: Activity) {
        activity.startActivity(Intent(activity, TelegramLoginActivity::class.java))
    }

    // ---- Universal data surface: STUBBED until read/send milestones ---------

    override fun listConversations(count: Int): String {
        // A folder sub-account is active -> show only that folder's chats.
        val folder = Mirror.activeTgFolder
        if (folder > 0) {
            val fjson = try {
                bridge().listFolderConversations(folder.toLong(), count.toLong())
            } catch (e: Throwable) {
                LogStore.w(TAG, "TG folder $folder list failed: ${e.message}")
                return "{\"conversations\":[]}"
            }
            return if (fjson.startsWith("{")) fjson else "{\"conversations\":[]}"
        }
        val json = try {
            bridge().listConversations(count.toLong())
        } catch (e: Throwable) {
            LogStore.w(TAG, "TG listConversations failed: ${e.message}")
            return "{\"conversations\":[]}"
        }
        // TEMP diagnostics (remove once sub-accounts land): the conversation JSON,
        // and a one-shot dump of chat folders (the grouping behind channel sub-accounts).
        LogStore.i(TAG, "TG-CONVOS: ${json.take(1800)}")
        if (!dumpedFolders) {
            dumpedFolders = true
            try {
                val fj = bridge().listFolders()
                LogStore.i(TAG, "TG-SUBACCOUNTS: $fj")
                // Prove getPeerDialogs: resolve the first folder's chats to a full list.
                val arr = org.json.JSONObject(fj).optJSONArray("folders")
                val fid = arr?.optJSONObject(0)?.optInt("id", -1) ?: -1
                if (fid >= 0) LogStore.i(TAG,
                    "TG-FOLDERCONVOS[$fid]: ${bridge().listFolderConversations(fid.toLong(), 15L).take(1200)}")
            } catch (e: Throwable) { LogStore.w(TAG, "TG folder diag failed: ${e.message}") }
        }
        // Bridge returns valid JSON on success; guard against a stray diagnostic.
        return if (json.startsWith("{")) json else "{\"conversations\":[]}"
    }

    override fun fetchMessages(conversationId: String, count: Long, cursor: String): String {
        val json = try {
            bridge().fetchMessages(conversationId, count)
        } catch (e: Throwable) {
            LogStore.w(TAG, "TG fetchMessages failed: ${e.message}")
            return "{\"messages\":[]}"
        }
        // TEMP diagnostic: how many messages did the bridge return, and any tgError?
        val n = try { org.json.JSONObject(json).optJSONArray("messages")?.length() ?: 0 } catch (e: Throwable) { -1 }
        val err = if (json.contains("tgError")) " ${json.take(160)}" else ""
        LogStore.i(TAG, "TG fetch $conversationId -> $n msgs$err")
        return if (json.startsWith("{")) json else "{\"messages\":[]}"
    }

    override fun sendMessage(conversationId: String, text: String, replyToken: String): String {
        val result = try {
            bridge().sendMessage(conversationId, text)
        } catch (e: Throwable) {
            LogStore.e(TAG, "TG send failed", e)
            return "{\"error\":\"${e.message}\"}"
        }
        // gotd doesn't loop our own send back promptly, so optimistically insert it
        // (the received-message path: stores + pushes the thread, stays silent since
        // it's from us). The real messageId dedups any later update echo.
        optimisticEcho(conversationId, text, sentIdOf(result), "")
        return result
    }

    // Native reply-to + emoji reactions from the watch detail view (gotd
    // reply_to / sendReaction). The interface defaults both to false.
    override val supportsReplyTo = true
    override val supportsReactionSend = true

    // Telegram marks read via messages/channels.readHistory (a per-dialog high-water
    // mark), so the newest message clears the thread. Opt into the mark-read seam.
    override val supportsMarkRead = true

    override fun sendReplyTo(
        conversationId: String, targetMessageId: String,
        targetFromMe: Boolean, text: String
    ): String {
        val result = try {
            bridge().sendReply(conversationId, targetMessageId.toLongOrNull() ?: 0L, text)
        } catch (e: Throwable) {
            LogStore.e(TAG, "TG sendReplyTo failed", e)
            return "{\"error\":\"${e.message}\"}"
        }
        // Tag the optimistic echo with replyTo.id so the thread builder composes the
        // "You replied to X" title from the stored quoted message.
        optimisticEcho(conversationId, text, sentIdOf(result), targetMessageId)
        return result
    }

    override fun sendReaction(
        conversationId: String, targetMessageId: String,
        targetFromMe: Boolean, emoji: String
    ): String {
        val result = try {
            bridge().sendReaction(conversationId, targetMessageId.toLongOrNull() ?: 0L, emoji)
        } catch (e: Throwable) {
            LogStore.e(TAG, "TG sendReaction failed", e)
            return "{\"error\":\"${e.message}\"}"
        }
        // The optimistic badge is applied once by the shared send path
        // (PebbleListenerService.sendTargeted) for EVERY network. Doing it again here
        // under a different actor key ("tg:self" vs the shared path's "self")
        // double-counted the reaction on the watch. Telegram's own reaction feedback
        // reconciles the badge to the true count shortly after.
        return result
    }

    override fun replyTokenFor(conversationId: String): String = ""   // convoId is the routing

    // Mark the thread read on Telegram up through messageId. The bridge routes
    // channel/supergroup vs DM/basic-group to the right readHistory RPC and parses
    // the id (Telegram ids are ints). Errors just log — fire-and-forget.
    override fun markRead(conversationId: String, messageId: String): String {
        return try {
            bridge().markThreadRead(conversationId, messageId)
        } catch (e: Throwable) {
            LogStore.e(TAG, "TG markRead failed", e)
            "{\"error\":\"${e.message}\"}"
        }
    }

    private fun sentIdOf(result: String): String =
        try { org.json.JSONObject(result).optString("messageId") } catch (_: Throwable) { "" }

    // Optimistically insert a just-sent message through LiveRouter (the received-
    // message path: stores it, pushes the thread, stays silent because it's ours).
    // replyToId, when set, tags it as a reply. Telegram timestamps are MICROSECONDS.
    private fun optimisticEcho(
        conversationId: String, text: String, sentId: String, replyToId: String
    ) {
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
            LiveRouter.ingestLiveMessage("tgnative", msg.toString())
        } catch (e: Throwable) {
            LogStore.e(TAG, "TG optimistic echo failed", e)
        }
    }

    override fun downloadImage(
        mediaId: String, decryptionKeyB64: String,
        maxW: Int, maxH: Int, maxBytes: Int, quality: Int
    ): String =
        try {
            // Telegram ignores decryptionKeyB64 — the download ref lives in the
            // bridge's photo cache, keyed by mediaId (= the message ID).
            bridge().downloadImageForWatch(
                mediaId, maxW.toLong(), maxH.toLong(), maxBytes.toLong(), quality.toLong()
            )
        } catch (e: Throwable) {
            LogStore.e(TAG, "TG downloadImage failed", e)
            "{\"error\":\"${e.message}\"}"
        }

    // Telegram photos are never inline — they always download via downloadImage.
    override fun processInlineImage(
        mediaDataB64: String,
        maxW: Int, maxH: Int, maxBytes: Int, quality: Int
    ): String = "{\"error\":\"telegram has no inline images\"}"

    companion object {
        private const val TAG = "MirrorMsgSpike"
        private const val PREFS = "mirrormsg"
        private const val KEY_AUTHED = "tg_authed"
        private const val KEY_PHONE = "tg_phone"
        private const val SESSION_FILE = "tg_session.json"

        private val lock = Any()
        @Volatile private var tgBridge: TgBridge? = null
        @Volatile private var dumpedFolders = false  // one-shot TG-FOLDERS diagnostic

        /**
         * The single shared bridge — its gotd run loop lives process-wide, so login
         * and (later) the live pipeline share one connection/session. Lazily created
         * against <filesDir>/tg_session.json.
         */
        fun bridge(): TgBridge {
            tgBridge?.let { return it }
            return synchronized(lock) {
                tgBridge ?: run {
                    val ctx = Mirror.appContext!!
                    val path = File(ctx.filesDir, SESSION_FILE).absolutePath
                    Telegrambridge.newTelegramBridge(path).also { b ->
                        // Live events from the gotd update dispatcher. MESSAGE routes
                        // into the shared pipeline (store + notify); REACTION updates
                        // the badge store; DIAG surfaces bridge diagnostics.
                        b.setLiveSink(object : LiveSink {
                            override fun onEvent(eventType: String, jsonPayload: String) {
                                when (eventType) {
                                    "MESSAGE" -> LiveRouter.ingestLiveMessage("tgnative", jsonPayload)
                                    "REACTION" -> {
                                        ReactionStore.handleBridgeEvent(jsonPayload)
                                        val open = Mirror.currentOpenConvoId
                                        if (open != null && Mirror.activeServiceId == "tgnative") {
                                            PebbleListenerService.instance?.refreshOpenThread(open)
                                        }
                                    }
                                    "DIAG" -> LogStore.i(TAG, jsonPayload)
                                    else -> {}
                                }
                            }
                        })
                        tgBridge = b
                    }
                }
            }
        }

        private fun prefs(ctx: Context) =
            ctx.getSharedPreferences(PREFS, Context.MODE_PRIVATE)

        /** Sub-accounts (Telegram folders) as JSON, for the Networks-page rows. */
        fun foldersJson(): String = try { bridge().listFolders() } catch (e: Throwable) {
            LogStore.w(TAG, "TG foldersJson failed: ${e.message}"); "{\"folders\":[]}"
        }

        /** Called by TelegramLoginActivity once login reports "authorized". */
        fun markLoggedIn(ctx: Context, phone: String) {
            prefs(ctx).edit().putBoolean(KEY_AUTHED, true).putString(KEY_PHONE, phone).apply()
        }
    }
}
