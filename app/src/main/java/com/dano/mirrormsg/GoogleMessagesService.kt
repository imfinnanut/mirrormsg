package com.dano.mirrormsg

import android.app.Activity
import android.content.Context
import org.json.JSONObject

/**
 * GoogleMessagesService is the FIRST implementation of [MessagingService] —
 * Google Messages (RCS/SMS), wrapping the existing libgm/mirrorbridge AAR. It
 * proves the framework pattern: the core talks to this through the interface,
 * exactly as it will talk to Facebook, with nothing Google-privileged.
 *
 * This adapter is intentionally THIN — it delegates to the same Mirror.bridge
 * and the same flows the app already uses, just behind the interface. The one
 * piece of real translation is the reply routing: Google needs SIM info
 * (participantID/simTwo/simNumber) to send, but the interface's [sendMessage]
 * takes only an opaque token. So this adapter encodes the SIM route into that
 * token in [replyTokenFor] and decodes it in [sendMessage] — keeping all
 * SMS/RCS-specific routing inside the Google module, invisible to the core.
 *
 * Discovered by [ServiceRegistry] via its no-arg constructor.
 */
class GoogleMessagesService : MessagingService {

    override val id = "google"
    override val displayName = "Google Messages"

    // Mutual-exclusion group for a Beeper Google-Messages twin (Beeper protocol
    // "gmessages"/"googlechat"): the twin declares conflictGroup = "google" too,
    // and presence-gated isActive() keeps only one of the pair active on the watch
    // at a time. A Beeper-only user has no native GM present, so this never blocks
    // their Beeper twin; a both-configured user gets native as the active side.
    override val conflictGroup = "google"

    // RCS supports both reply-to and tapback reactions, so this adapter opts into
    // the targeted-send capabilities; the watch's detail-view reply routes here.
    override val supportsReplyTo = true
    override val supportsReactionSend = true

    // Google Messages can mark a thread read on the source (libgm read receipt),
    // so the "Mark as read" notification action is offered for this network.
    override val supportsMarkRead = true

    private val ctx: Context get() = Mirror.appContext!!

    // ---- Identity & state --------------------------------------------------

    override fun isLoggedIn(): Boolean {
        // Logged in iff we have a saved session. (The bridge being connected is
        // a stronger condition handled by connectIfPossible.)
        val prefs = ctx.getSharedPreferences("mirrormsg", Context.MODE_PRIVATE)
        return (prefs.getString("session", "") ?: "").isNotEmpty()
    }

    override fun accountLabel(): String = Mirror.accountNumber

    // ---- Lifecycle ---------------------------------------------------------

    override fun connectIfPossible(): Boolean {
        val b = Mirror.bridge ?: return false
        return try {
            if (b.isLoggedIn) { b.connect(); true } else false
        } catch (e: Exception) {
            LogStore.e("GoogleMessagesService", "connect failed", e); false
        }
    }

    override fun disconnect() {
        try { Mirror.bridge?.disconnect() } catch (_: Exception) {}
    }

    override fun logout(context: Context) {
        // Mirror of the existing performLogout: clear session + cached number.
        context.getSharedPreferences("mirrormsg", Context.MODE_PRIVATE)
            .edit().remove("session").apply()
        Mirror.setAccountNumber(context, "")
        try { Mirror.closeSender() } catch (_: Exception) {}
        Mirror.bridge = null
    }

    // ---- Login -------------------------------------------------------------

    override fun startLogin(activity: Activity) {
        // The Google login flow lives in MainActivity (the WebView cookie harvest
        // + Gaia pairing). The interface just needs to trigger it; MainActivity
        // exposes startWebViewLogin() for exactly this. We call it via the
        // activity so the core never needs to know the mechanism.
        (activity as? MainActivity)?.startWebViewLogin()
    }

    // ---- Universal data surface --------------------------------------------

    override fun listConversations(count: Int): String =
        Mirror.bridge?.listConversations(count.toLong()) ?: "{\"conversations\":[]}"

    override fun fetchMessages(conversationId: String, count: Long, cursor: String): String =
        Mirror.bridge?.fetchMessages(conversationId, count, cursor) ?: "{\"messages\":[]}"

    override fun sendMessage(conversationId: String, text: String, replyToken: String): String {
        val route = decodeRoute(replyToken)
        return Mirror.bridge?.sendMessage(
            conversationId, text,
            route.participantID, route.simTwo, route.simNumber
        ) ?: "{\"error\":\"no bridge\"}"
    }

    // Detail-view reply: an RCS reply to targetMessageId. Same SIM routing as a
    // normal send (resolved from the conversation), with the quoted message id
    // handed to the bridge. On a bridge error we return an error JSON so the
    // caller falls back to a plain message rather than dropping the reply.
    override fun sendReplyTo(
        conversationId: String, targetMessageId: String,
        targetFromMe: Boolean, text: String
    ): String {
        val route = routeFor(conversationId)
        return try {
            Mirror.bridge?.sendReply(
                conversationId, text, route.participantID, targetMessageId,
                route.simTwo, route.simNumber
            ) ?: "{\"error\":\"no bridge\"}"
        } catch (e: Throwable) {
            LogStore.e("GoogleMessagesService", "sendReplyTo failed", e)
            JSONObject().put("error", e.message ?: "reply send failed").toString()
        }
    }

    // Detail-view emoji reply: an RCS tapback on targetMessageId. No SIM routing
    // needed. The sent reaction surfaces back via the message's embedded reactions
    // on the next update (same path received reactions use), so no optimistic echo.
    override fun sendReaction(
        conversationId: String, targetMessageId: String,
        targetFromMe: Boolean, emoji: String
    ): String {
        return try {
            Mirror.bridge?.sendReaction(targetMessageId, emoji, false)
                ?: "{\"error\":\"no bridge\"}"
        } catch (e: Throwable) {
            LogStore.e("GoogleMessagesService", "sendReaction failed", e)
            JSONObject().put("error", e.message ?: "reaction send failed").toString()
        }
    }

    // Mark a conversation read on Google Messages itself: libgm sends a read
    // receipt up through messageId, clearing the thread's unread state on the
    // source (not just dismissing the local notification). Errors return an error
    // JSON so the receiver can log without crashing.
    override fun markRead(conversationId: String, messageId: String): String {
        return try {
            Mirror.bridge?.markRead(conversationId, messageId)
                ?: "{\"error\":\"no bridge\"}"
        } catch (e: Throwable) {
            LogStore.e("GoogleMessagesService", "markRead failed", e)
            JSONObject().put("error", e.message ?: "mark-read failed").toString()
        }
    }

    // The SIM route for a conversation (cached per-conversation in Mirror.simRoutes
    // via convoOrder), or a default route when unknown. Shared by replyTokenFor and
    // sendReplyTo so both route identically.
    private fun routeFor(conversationId: String): Mirror.SimRoute {
        val idx = Mirror.convoOrder.indexOf(conversationId)
        return if (idx >= 0 && idx < Mirror.simRoutes.size) Mirror.simRoutes[idx]
        else Mirror.SimRoute("2", 2L, 2L)
    }

    override fun replyTokenFor(conversationId: String): String {
        // Encode this conversation's SIM route as the opaque token sendMessage decodes.
        val route = routeFor(conversationId)
        return JSONObject().apply {
            put("p", route.participantID)
            put("t", route.simTwo)
            put("n", route.simNumber)
        }.toString()
    }

    private fun decodeRoute(token: String): Mirror.SimRoute {
        return try {
            if (token.isEmpty()) Mirror.SimRoute("2", 2L, 2L)
            else {
                val o = JSONObject(token)
                Mirror.SimRoute(
                    o.optString("p", "2"),
                    o.optLong("t", 2L),
                    o.optLong("n", 2L)
                )
            }
        } catch (e: Exception) {
            Mirror.SimRoute("2", 2L, 2L)
        }
    }

    override fun downloadImage(
        mediaId: String, decryptionKeyB64: String,
        maxW: Int, maxH: Int, maxBytes: Int, quality: Int
    ): String =
        Mirror.bridge?.downloadImageForWatch(
            mediaId, decryptionKeyB64,
            maxW.toLong(), maxH.toLong(), maxBytes.toLong(), quality.toLong()
        ) ?: "{\"error\":\"no bridge\"}"

    override fun processInlineImage(
        mediaDataB64: String,
        maxW: Int, maxH: Int, maxBytes: Int, quality: Int
    ): String =
        Mirror.bridge?.processInlineImageForWatch(
            mediaDataB64,
            maxW.toLong(), maxH.toLong(), maxBytes.toLong(), quality.toLong()
        ) ?: "{\"error\":\"no bridge\"}"
}
