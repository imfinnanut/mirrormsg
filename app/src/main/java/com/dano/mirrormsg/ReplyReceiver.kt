package com.dano.mirrormsg

import android.app.RemoteInput
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch

/**
 * Fires when the user sends a reply from a MirrorMsg Pebble notification.
 *
 * Pebble (via the Android Wear notification-action system) surfaces any wearable
 * action that carries a RemoteInput as its "Reply with Text" menu — canned
 * responses + emoji on the watch. When the user picks one, Android invokes this
 * receiver's PendingIntent with the chosen text attached as a RemoteInput extra.
 *
 * IMPORTANT: the send goes straight through Mirror.bridge here rather than the
 * PebbleListenerService. The service is created on-demand by PebbleKit and may
 * not be alive when a notification is answered, so routing through its instance
 * (which was null) silently dropped the reply. Mirror.bridge is always available
 * while connected, exactly like OpenWatchReceiver uses Mirror.sender directly.
 *
 * SINGLE-EMOJI REPLIES BECOME REACTIONS. If the reply is a single emoji and the
 * owning account is a native one that can send reactions (supportsReactionSend —
 * true for Google/Facebook/WhatsApp/Telegram, false for Beeper), we REACT to the
 * latest incoming message in the conversation instead of posting the emoji as a
 * message. This mirrors the watch's emoji-only-as-reaction behavior. Anything
 * else — text, an emoji plus words, multiple emoji, or a non-reaction account —
 * sends as a normal reply. If the reaction is rejected by the network (e.g. an
 * emoji Telegram doesn't allow) or there's no message to react to, we fall back
 * to sending it as a plain message.
 */
class ReplyReceiver : BroadcastReceiver() {
    override fun onReceive(context: Context, intent: Intent) {
        val convoId = intent.getStringExtra(Notifier.EXTRA_CONVO_ID) ?: return
        // The OWNING service was baked into the notification at post time. The
        // reply MUST go through this account, not whatever is active now — else a
        // reply to a DM gets sent via the currently-parked account and leaks into
        // the wrong conversation. Fall back to active service only for legacy
        // notifications posted before this field existed.
        val ownerServiceId = intent.getStringExtra(Notifier.EXTRA_SERVICE_ID)
        val results = RemoteInput.getResultsFromIntent(intent) ?: return
        val text = results.getCharSequence(Notifier.KEY_REPLY_INPUT)?.toString()?.trim()
        if (text.isNullOrEmpty()) return
        val appCtx = context.applicationContext
        Mirror.appContext = appCtx
        LogStore.i("MirrorMsgSpike", "ReplyReceiver: reply to convo $convoId via ${ownerServiceId ?: "active"}: ${text.length} chars")
        // Dismiss the notification now that it's been answered.
        Notifier.cancel(appCtx, convoId)
        val pending = goAsync()
        CoroutineScope(Dispatchers.IO).launch {
            try {
                val svc = (ownerServiceId?.let { ServiceRegistry.byId(appCtx, it) })
                    ?: Mirror.activeService()
                if (svc == null) {
                    LogStore.w("MirrorMsgSpike", "ReplyReceiver: no service for $ownerServiceId — cannot send")
                    return@launch
                }

                // Single-emoji reply on a reaction-capable (native) account -> react
                // to the latest incoming message rather than posting the emoji as a
                // message. Falls through to a normal reply if there's nothing to react
                // to or the network rejects the reaction.
                if (isEmojiOnly(text) && svc.supportsReactionSend) {
                    val targetId = latestIncomingMessageId(appCtx, convoId)
                    if (targetId != null) {
                        val rres = try {
                            svc.sendReaction(convoId, targetId, /*targetFromMe=*/ false, text)
                        } catch (e: Throwable) {
                            LogStore.e("MirrorMsgSpike", "ReplyReceiver: sendReaction threw", e)
                            "{\"error\":\"${e.message}\"}"
                        }
                        if (reactionSucceeded(rres)) {
                            LogStore.i("MirrorMsgSpike",
                                "ReplyReceiver: reacted \"$text\" to msg ${targetId.takeLast(8)} in $convoId via ${svc.id}")
                            // Optimistically reflect the reaction so an open thread on
                            // the watch shows the badge; the network's own feedback
                            // reconciles it to the true count shortly after. Keyed under
                            // this account's self actor when it exposes one, else "self".
                            val actorKey = svc.selfActorKey() ?: "self"
                            ReactionStore.ingest(targetId, actorKey, text)
                            PebbleListenerService.instance?.refreshAfterReply(convoId)
                            return@launch
                        }
                        LogStore.w("MirrorMsgSpike",
                            "ReplyReceiver: reaction rejected, sending as message: $rres")
                        // fall through to plain send
                    } else {
                        LogStore.i("MirrorMsgSpike",
                            "ReplyReceiver: no incoming message to react to — sending \"$text\" as message")
                        // fall through to plain send
                    }
                }

                // Resolve the SIM route from the conversation's position in the
                // OWNING service's convo list — only meaningful when that service
                // is the active one (so convoOrder reflects it); otherwise the
                // route defaults, which non-Google services ignore anyway. Google
                // replies from a notification only occur while Google is active.
                val idx = if (svc.id == Mirror.activeServiceId)
                    Mirror.convoOrder.indexOf(convoId) else -1
                val route = (if (idx >= 0) Mirror.simRoutes.getOrNull(idx) else null)
                    ?: Mirror.SimRoute("2", 2L, 2L)
                LogStore.i("MirrorMsgSpike", "ReplyReceiver sending to convo $convoId via ${svc.id} (route=${route.participantID})")
                val replyToken = org.json.JSONObject().apply {
                    put("p", route.participantID); put("t", route.simTwo); put("n", route.simNumber)
                }.toString()
                val result = svc.sendMessage(convoId, text, replyToken)
                LogStore.i("MirrorMsgSpike", "ReplyReceiver: reply sent. Response: $result")
                // Best-effort: if the service is alive, let it refresh the thread
                // so the sent message shows on the watch. Not required for sending.
                PebbleListenerService.instance?.refreshAfterReply(convoId)
            } catch (e: Exception) {
                LogStore.e("MirrorMsgSpike", "ReplyReceiver failed", e)
            } finally {
                pending.finish()
            }
        }
    }

    /** Most recent INCOMING (not-from-me) message id in the stored thread, or null. */
    private fun latestIncomingMessageId(ctx: Context, convoId: String): String? {
        return try {
            val json = MessageStore.loadAsThreadJson(ctx, convoId)
            val arr = org.json.JSONObject(json).optJSONArray("messages") ?: return null
            var bestId: String? = null
            var bestTs = Long.MIN_VALUE
            for (i in 0 until arr.length()) {
                val m = arr.getJSONObject(i)
                val isMe = m.optJSONObject("senderParticipant")?.optBoolean("isMe", false) ?: false
                if (isMe) continue
                val id = m.optString("messageID")
                if (id.isEmpty()) continue
                val ts = m.optString("timestamp").toLongOrNull() ?: 0L
                if (ts >= bestTs) { bestTs = ts; bestId = id }
            }
            bestId
        } catch (e: Throwable) {
            LogStore.e("MirrorMsgSpike", "ReplyReceiver: latestIncomingMessageId failed", e)
            null
        }
    }

    /** A reaction send succeeded when the bridge response carries a non-empty messageId. */
    private fun reactionSucceeded(result: String): Boolean = try {
        org.json.JSONObject(result).optString("messageId").isNotEmpty()
    } catch (e: Throwable) {
        false
    }

    // Emoji-only detection — kept byte-for-byte in step with the watch reply path
    // (PebbleListenerService.isEmojiOnly) so both treat the same strings as "a
    // single emoji." Allows ZWJ sequences, variation selectors, and skin-tone
    // modifiers; requires at least one real emoji code point and nothing else.
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
}
