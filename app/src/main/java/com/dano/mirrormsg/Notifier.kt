package com.dano.mirrormsg

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.RemoteInput
import android.content.Context
import android.content.Intent
import android.os.Build

/**
 * Posts a normal-importance system notification so the Pebble mirrors it to
 * the watch (Pebble ignores silent/low-importance notifications).
 *
 * IMPORTANT: Pebble surfaces notification ACTIONS via Android Wear notification
 * extensions, NOT via the plain contentIntent. So to get a working "open on
 * phone"-style action that lands on OUR intent, we attach the action through
 * Notification.WearableExtender (and also as a normal action). The contentIntent
 * still handles taps on the phone itself.
 */
object Notifier {
    private const val CHANNEL_ID = "mirrormsg_messages_v5"
    private const val CHANNEL_NAME = "Messages"
    private const val NOTIF_ID = 4201   // single reused id; latest message replaces

    const val EXTRA_CONVO_ID = "com.dano.mirrormsg.CONVO_ID"
    const val EXTRA_SERVICE_ID = "com.dano.mirrormsg.SERVICE_ID"
    const val EXTRA_FROM_NOTIF = "com.dano.mirrormsg.FROM_NOTIF"
    const val KEY_REPLY_INPUT = "com.dano.mirrormsg.REPLY_INPUT"

    // Canned quick-replies offered on the Pebble notification (it also adds its
    // own emoji + dictation). Keep to <=5 short phrases.
    private val QUICK_REPLIES = arrayOf(
        "OK", "Thanks!", "On my way", "Call you later", "👍")

    private fun ensureChannel(ctx: Context) {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            val mgr = ctx.getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
            if (mgr.getNotificationChannel(CHANNEL_ID) == null) {
                val ch = NotificationChannel(CHANNEL_ID, CHANNEL_NAME,
                    NotificationManager.IMPORTANCE_DEFAULT).apply {
                    description = "New message alerts (mirrored to Pebble)"
                }
                mgr.createNotificationChannel(ch)
            }
        }
    }

    // Background-activity-launch (BAL) grant for notification actions that open
    // an Activity. When the Pebble companion app fires one of these actions while
    // it is BACKGROUNDED, Android attributes the activity start to Pebble's
    // background process and blocks it — the action then "only works while Pebble
    // is in the foreground." As the PendingIntent CREATOR, we grant our own
    // background-activity-start privilege to ride along with the send, so the
    // launch is permitted no matter who fires it. API 34+ only; older devices get
    // null (the prior, foreground-only behavior). Returned as an ActivityOptions
    // Bundle for PendingIntent.getActivity(..., options).
    private fun creatorBalOptions(): android.os.Bundle? {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.UPSIDE_DOWN_CAKE) return null
        return android.app.ActivityOptions.makeBasic()
            .setPendingIntentCreatorBackgroundActivityStartMode(
                android.app.ActivityOptions.MODE_BACKGROUND_ACTIVITY_START_ALLOWED)
            .toBundle()
    }

    // Banner tap on the PHONE + Pebble's "Open on phone" action both fire this
    // contentIntent. We point it at the NATIVE app for the conversation's
    // protocol (Google Messages on the right thread for SMS; WhatsApp/etc. later)
    // via the protocol-agnostic DeepLink layer. If no deep link resolves (group
    // chat, missing number, unknown protocol), fall back to opening MirrorMsg.
    private fun activityIntent(ctx: Context, convoId: String, ownerServiceId: String?): PendingIntent {
        // Which native app to open is a property of the OWNING account, not of the
        // stored convo protocol (which was often just tagged SMS). Re-derive it from
        // ownerServiceId; the thread address (phone number for 1:1s) still comes from
        // the stored target.
        val address = Mirror.convoTargets[convoId]?.address ?: ""
        val deepIntent = DeepLink.openForService(ctx, ownerServiceId, address)
        if (deepIntent != null) {
            // Route through OUR trampoline activity rather than launching the
            // native app directly. A background-fired PendingIntent that starts
            // ANOTHER app's activity is blocked by BAL even WITH the creator grant;
            // starting OUR OWN activity is grantable. The trampoline (now foreground)
            // then opens the native app as a normal foreground launch, which BAL
            // always allows, and falls back to MirrorMsg if the app is gone.
            val tramp = Intent(ctx, OpenInAppActivity::class.java).apply {
                flags = Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_NO_ANIMATION
                putExtra(EXTRA_CONVO_ID, convoId)
                putExtra(EXTRA_SERVICE_ID, ownerServiceId)
            }
            return PendingIntent.getActivity(
                ctx, convoId.hashCode(), tramp,
                PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE,
                creatorBalOptions())
        }
        // Fallback: open MirrorMsg itself on this conversation.
        val tapIntent = Intent(ctx, MainActivity::class.java).apply {
            flags = Intent.FLAG_ACTIVITY_NEW_TASK or
                    Intent.FLAG_ACTIVITY_CLEAR_TOP or
                    Intent.FLAG_ACTIVITY_SINGLE_TOP
            putExtra(EXTRA_CONVO_ID, convoId)
            putExtra(EXTRA_FROM_NOTIF, true)
        }
        return PendingIntent.getActivity(
            ctx, convoId.hashCode(), tapIntent,
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE,
            creatorBalOptions())
    }

    // "Open on watch" action -> BROADCAST to OpenWatchReceiver. A receiver runs
    // without foregrounding the app, so it opens the watchapp reliably even when
    // MirrorMsg is backgrounded/killed (the flaky part was the activity launch).
    private fun watchIntent(ctx: Context, convoId: String, ownerServiceId: String): PendingIntent {
        val bcast = Intent(ctx, OpenWatchReceiver::class.java).apply {
            putExtra(EXTRA_CONVO_ID, convoId)
            putExtra(EXTRA_SERVICE_ID, ownerServiceId)
        }
        return PendingIntent.getBroadcast(
            ctx, convoId.hashCode() + 1, bcast,
            // MUTABLE (not IMMUTABLE): EXTRA_SERVICE_ID can change from one
            // message to the next (a convo may notify from different owning
            // accounts). With IMMUTABLE, FLAG_UPDATE_CURRENT does NOT rewrite the
            // extras — they freeze at first creation — so a stale owner could be
            // used for "Open on watch" routing. MUTABLE lets UPDATE_CURRENT
            // refresh them. (Explicit component target, so this is safe.)
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_MUTABLE)
    }

    // "Open in app" action -> launch OUR OWN invisible trampoline activity, which
    // then opens the native-app deep link. Launching another app's activity
    // DIRECTLY from a watch-originated action (whether via getActivity or a
    // receiver's startActivity) is a background-activity-launch and Android blocks
    // it intermittently ("works once, then nothing"). But launching OUR app's
    // activity from a notification action is always permitted; once we're
    // foreground, WE launch Messages as a normal foreground launch, which BAL
    // allows. We only offer the action when a deep-link target exists.
    private fun phoneIntent(ctx: Context, convoId: String, ownerServiceId: String?): PendingIntent? {
        val address = Mirror.convoTargets[convoId]?.address ?: ""
        DeepLink.openForService(ctx, ownerServiceId, address) ?: return null   // bail if dead
        val tramp = Intent(ctx, OpenInAppActivity::class.java).apply {
            flags = Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_NO_ANIMATION
            putExtra(EXTRA_CONVO_ID, convoId)
            putExtra(EXTRA_SERVICE_ID, ownerServiceId)
        }
        return PendingIntent.getActivity(
            ctx, convoId.hashCode() + 2, tramp,
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE,
            creatorBalOptions())
    }

    fun postNewMessage(ctx: Context, convoId: String, sender: String, body: String,
                       ownerServiceId: String, groupName: String = "") {
        ensureChannel(ctx)

        val contentPi = activityIntent(ctx, convoId, ownerServiceId)   // phone banner / "open on phone"
        val watchPi   = watchIntent(ctx, convoId, ownerServiceId)  // "Open on watch" action
        val phonePi   = phoneIntent(ctx, convoId, ownerServiceId)       // "Open in app" deep link
        // Top line: the owning account's network name, then the sender on line two.
        // Uses each adapter's displayName, so every network — native AND Beeper —
        // works with no per-account code. When the message belongs to a group/server
        // (Discord: the guild name), the top line becomes "Network\u2192Server" (e.g.
        // "Discord\u2192Please rewind"); a plain DM keeps "Network:". The Pebble title
        // field wraps, so the network line stands on its own no matter how long it is.
        // Falls back to just the sender if the service can't be resolved.
        val netName = ServiceRegistry.byId(ctx, ownerServiceId)?.displayName ?: ""
        val name = if (sender.isNotEmpty()) sender else "New message"
        val topLine = when {
            netName.isEmpty()      -> ""
            groupName.isNotEmpty() -> "$netName\u2192$groupName"
            else                   -> "$netName:"
        }
        val title = if (topLine.isNotEmpty()) "$topLine\n$name" else name

        // Reply action with a RemoteInput — this is what Pebble surfaces as its
        // "Reply with Text" menu (canned responses + emoji + dictation). The
        // chosen text comes back to ReplyReceiver as a RemoteInput extra.
        // CRITICAL: bake the OWNING serviceId into the reply intent. The reply
        // must send through the account that owns this convo — NOT through
        // whatever account happens to be active when the reply is answered.
        // Without this, a reply to (say) a Beeper DM would go out via the active
        // service (e.g. the Discord group the watch was parked on) and land in
        // the wrong conversation. The destination is a property of the
        // notification, fixed at post time, not of current app state.
        val remoteInput = RemoteInput.Builder(KEY_REPLY_INPUT)
            .setLabel("Reply")
            .setChoices(QUICK_REPLIES)
            .build()
        val replyPi = PendingIntent.getBroadcast(
            ctx, convoId.hashCode() + 3,
            Intent(ctx, ReplyReceiver::class.java)
                .putExtra(EXTRA_CONVO_ID, convoId)
                .putExtra(EXTRA_SERVICE_ID, ownerServiceId),
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_MUTABLE)
        val replyAction = Notification.Action.Builder(
            android.R.drawable.ic_menu_send, "Reply", replyPi)
            .addRemoteInput(remoteInput)
            .build()

        // "Open on watch" routes through a BroadcastReceiver (no activity launch).
        val watchAction = Notification.Action.Builder(
            android.R.drawable.ic_menu_view, "Open on watch", watchPi).build()
        // "Open in app" fires the deep link into Google Messages / native app.
        val phoneAction = if (phonePi != null)
            Notification.Action.Builder(
                android.R.drawable.ic_menu_send, "Open in app", phonePi).build()
            else null

        // Run sender + body through the emoji filter so the Pebble-mirrored
        // notification shows drawable glyphs (no-op when the setting is off).
        val titleOut = EmojiSub.processText(title)
        val bodyOut  = EmojiSub.processText(body)
        val builder = Notification.Builder(ctx, CHANNEL_ID)
            .setSmallIcon(android.R.drawable.ic_dialog_email)
            .setContentTitle(titleOut)
            .setContentText(bodyOut)
            .setStyle(Notification.BigTextStyle().bigText(bodyOut))
            .setContentIntent(contentPi)
            .setAutoCancel(true)
            .setOnlyAlertOnce(true)
            .addAction(replyAction)
            .addAction(watchAction)
        if (phoneAction != null) builder.addAction(phoneAction)

        // Attach the actions as Android Wear actions — this is the channel Pebble
        // actually reads notification actions from. Order = order on the watch.
        // Reply goes FIRST so it's the default highlighted action on the watch.
        val wearableExtender = Notification.WearableExtender()
            .addAction(replyAction)
            .addAction(watchAction)
        if (phoneAction != null) wearableExtender.addAction(phoneAction)
        builder.extend(wearableExtender)

        val mgr = ctx.getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
        mgr.notify(NOTIF_ID, builder.build())
    }

    /** Dismiss the current new-message notification. Used when a message that
     *  was first notified as text turns out to be an image and instead
     *  force-opens the watch — so the stale "new message" banner goes away. */
    fun cancel(ctx: Context) {
        val mgr = ctx.getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
        mgr.cancel(NOTIF_ID)
    }
}
