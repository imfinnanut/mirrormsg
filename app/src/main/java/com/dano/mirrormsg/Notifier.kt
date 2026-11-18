package com.dano.mirrormsg

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Person
import android.app.RemoteInput
import android.content.Context
import android.content.Intent
import android.content.pm.ShortcutInfo
import android.content.pm.ShortcutManager
import android.graphics.drawable.Icon
import android.os.Build
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch

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
    // Two channels exist, but routing no longer switches between them: once
    // LiveRouter has decided a message is a Pebble notification (vs an IAN), it
    // must ALERT — regardless of whether the watchapp happens to be open. The old
    // watchAppOpen -> silent switch assumed "app open = the app delivers richly
    // on-watch", which is only true on the IAN path; in Notification mode it made
    // messages vanish entirely (Rebble drops IMPORTANCE_LOW with no history — a
    // hardware-verified finding, do not re-litigate). The silent channel is kept
    // registered (channel importance is immutable after creation, so the id
    // survives) for a possible future silent phone-shade record on IAN paths.
    private const val CHANNEL_ID = "mirrormsg_messages_v5"          // alerting — the one in use
    private const val CHANNEL_ID_SILENT = "mirrormsg_messages_v5_silent"  // registered, unused
    private const val CHANNEL_NAME = "Messages"
    private const val CHANNEL_NAME_SILENT = "Messages (in-app, silent)"
    // Per-conversation notification id: each chat gets its own shade entry that
    // updates in place, and different chats STACK separately (matching Beeper's
    // native per-chat notifications). Previously a single reused id (4201) meant every
    // new message replaced the last, so a missed message was gone from the shade. Keyed
    // on convoId.hashCode() — same key the reply/tap/watch PendingIntents already use,
    // so a conversation's notification and its actions line up. Offset by a constant to
    // avoid colliding with those PendingIntent request codes (which reuse the same hash
    // +0..+4); notification ids and PendingIntent request codes are separate namespaces,
    // but keeping them distinct avoids confusion.
    private const val NOTIF_ID_BASE = 0x4201
    private fun notifIdFor(convoId: String): Int = NOTIF_ID_BASE xor convoId.hashCode()

    const val EXTRA_CONVO_ID = "com.dano.mirrormsg.CONVO_ID"
    const val EXTRA_SERVICE_ID = "com.dano.mirrormsg.SERVICE_ID"
    const val EXTRA_MESSAGE_ID = "com.dano.mirrormsg.MESSAGE_ID"
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
            if (mgr.getNotificationChannel(CHANNEL_ID_SILENT) == null) {
                val ch = NotificationChannel(CHANNEL_ID_SILENT, CHANNEL_NAME_SILENT,
                    NotificationManager.IMPORTANCE_LOW).apply {
                    description = "Silent record posted while the watchapp is open (no buzz, phone shade only)"
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
            putExtra(EXTRA_SERVICE_ID, ownerServiceId)   // the thread opens on its own account
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

    /** The text worth showing ALONGSIDE a picture: the caption if there is one,
     *  nothing if the body is just the "[Image]" placeholder we synthesize for
     *  image messages. */
    private fun pictureCaption(body: String): String {
        val stripped = body.removePrefix("[Image]").trimStart('\n', ' ')
        return stripped
    }

    fun postNewMessage(ctx: Context, convoId: String, sender: String, body: String,
                       ownerServiceId: String, messageId: String = "", groupName: String = "",
                       picture: android.graphics.Bitmap? = null) {
        ensureChannel(ctx)
        val whenMs = System.currentTimeMillis()

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

        // "Mark as read" -> BROADCAST to MarkReadReceiver, which asks the OWNING
        // service to mark the thread read on the SOURCE network (up through this
        // message) and clears the notification. Only offered when the owning
        // service actually implements it (supportsMarkRead) — no dead button on
        // networks that can't yet propagate a read. Bakes convo + owner + the
        // triggering messageId (MarkRead needs the message, not just the convo).
        val ownerSvc = ServiceRegistry.byId(ctx, ownerServiceId)
        val markReadAction: Notification.Action? =
            if (ownerSvc?.supportsMarkRead == true && messageId.isNotEmpty()) {
                val markReadPi = PendingIntent.getBroadcast(
                    ctx, convoId.hashCode() + 4,
                    Intent(ctx, MarkReadReceiver::class.java)
                        .putExtra(EXTRA_CONVO_ID, convoId)
                        .putExtra(EXTRA_SERVICE_ID, ownerServiceId)
                        .putExtra(EXTRA_MESSAGE_ID, messageId),
                    // MUTABLE for the same reason as the watch intent: the baked
                    // convo/message/owner vary per message and must not freeze.
                    PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_MUTABLE)
                Notification.Action.Builder(
                    android.R.drawable.ic_menu_view, "Mark as read", markReadPi).build()
            } else null

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
        // Only caption a picture when there's something to say: a bare "[Image]"
        // over the image itself is noise (bodyOut is "[Image]\n<caption>" when
        // captioned). The picture IS the message; a caption is real content.
        val textOut = if (picture != null) pictureCaption(bodyOut) else bodyOut
        // ALWAYS the alerting channel. Reaching this function means the routing
        // chose the Pebble-notification behavior — "new message = pebble system
        // notification, nothing can change it" — so it alerts even when the
        // watchapp is open (the notification pops over the app; the app itself
        // stays passive and just repaints an open chat silently).
        val builder = Notification.Builder(ctx, CHANNEL_ID)
            .setSmallIcon(smallIcon(ctx))
            .setWhen(whenMs)
            .setContentTitle(titleOut)
            .setContentText(textOut)
            .setContentIntent(contentPi)
            .setAutoCancel(true)
            .setOnlyAlertOnce(true)
        // Android 11+: a conversation (step 90), so the phone shows the chat's photo
        // (or its letter) as the notification's picture with MirrorMsg's badge, as
        // Discord's and Google Messages' do. Older Androids keep the plain styles.
        val convo = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.R)
            conversation(ctx, convoId, sender, body, ownerServiceId, ownerSvc, groupName, picture,
                         whenMs, titleOut) else null
        if (convo != null) {
            builder.setStyle(convo.style).setShortcutId(convo.shortcutId)
        } else {
            // A picture attached here lands in the notification's EXTRA_PICTURE, which
            // is where PebbleOS reads images from — without it MirrorMsg's
            // notifications would show "[Image]" where a Pebble-native notification
            // shows the actual photo. setLargeIcon puts it in the collapsed form too,
            // and bigLargeIcon(null) stops it being duplicated when expanded.
            if (picture != null) builder.setLargeIcon(picture)
            builder.setStyle(
                if (picture != null)
                    Notification.BigPictureStyle()
                        .bigPicture(picture)
                        .bigLargeIcon(null as android.graphics.Bitmap?)
                        .setSummaryText(textOut)
                else
                    Notification.BigTextStyle().bigText(bodyOut)
            )
        }
        builder.addAction(replyAction)
        if (markReadAction != null) builder.addAction(markReadAction)
        builder.addAction(watchAction)
        if (phoneAction != null) builder.addAction(phoneAction)

        // Attach the actions as Android Wear actions — this is the channel Pebble
        // actually reads notification actions from. Order = order on the watch.
        // Reply goes FIRST so it's the default highlighted action on the watch.
        val wearableExtender = Notification.WearableExtender()
            .addAction(replyAction)
        if (markReadAction != null) wearableExtender.addAction(markReadAction)
        wearableExtender.addAction(watchAction)
        if (phoneAction != null) wearableExtender.addAction(phoneAction)
        builder.extend(wearableExtender)

        val n = builder.build()
        if (convo != null) {
            // The Pebble app reads the title, the text, the big text and the picture
            // straight from these extras, which the conversation style rewrites (to
            // "Chat: Sender" and the newest message): put back exactly what the plain
            // styles gave it, so the watch's notification doesn't change.
            n.extras.putCharSequence(Notification.EXTRA_TITLE, titleOut)
            n.extras.putCharSequence(Notification.EXTRA_TEXT, textOut)
            if (picture != null) n.extras.putParcelable(Notification.EXTRA_PICTURE, picture)
            else n.extras.putCharSequence(Notification.EXTRA_BIG_TEXT, bodyOut)
        }
        val mgr = ctx.getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
        mgr.notify(notifIdFor(convoId), n)

        // No photo on the phone yet for a chat that may have one: fetch it in the
        // background, so the chat's next notification has it. (This one isn't posted
        // again with it: a repeat could reach the watch as a new notification.)
        if (convo != null && convo.fetchPhoto && ownerSvc != null) {
            CoroutineScope(Dispatchers.IO).launch {
                try {
                    val id = Avatars.notifId(convoId) ?: Avatars.lookupNotifId(ownerSvc, convoId) ?: return@launch
                    Avatars.load(ctx, ownerSvc, id)
                } catch (e: Throwable) {
                    LogStore.w("MirrorMsgSpike", "Notification photo: ${e.message}")
                }
            }
        }
    }

    // MirrorMsg's own notification glyph (the speech bubble) as the small icon: the
    // badge on a conversation's picture. The stock envelope if the app has none.
    private fun smallIcon(ctx: Context): Int =
        ctx.resources.getIdentifier("ic_notification", "drawable", ctx.packageName)
            .takeIf { it != 0 } ?: android.R.drawable.ic_dialog_email

    // ---- Conversations (step 90, Android 11+) ---------------------------------------
    // A MessagingStyle notification tied to a long-lived shortcut for the chat is a
    // "conversation": the shade shows the shortcut's picture (the chat's photo, else
    // its letter on its colour: the watch's avatar) with the app's badge, and the
    // chat's name. Shortcuts are re-published only when the name or picture changes.
    private class Convo(val style: Notification.MessagingStyle, val shortcutId: String,
                        val fetchPhoto: Boolean)

    private const val ICON_PX = 144
    private val pushed = java.util.concurrent.ConcurrentHashMap<String, String>()

    // [title] is the notification's own title (network, then sender): it's also the
    // style's conversation title, which the phone doesn't show (the shortcut's name
    // takes its place) but the Pebble app reads as the sender its notification
    // history groups by, so the watch groups them as before (by that title).
    @android.annotation.TargetApi(Build.VERSION_CODES.R)
    private fun conversation(ctx: Context, convoId: String, sender: String, body: String,
                             ownerServiceId: String, svc: MessagingService?, groupName: String,
                             picture: android.graphics.Bitmap?, whenMs: Long,
                             title: CharSequence): Convo? = try {
        val chat = Avatars.chat(convoId)
        val group = groupName.isNotEmpty() || chat?.group == true
        // The chat's name: a server channel's "Server#channel", a group's name, else the person.
        val label = when {
            groupName.isNotEmpty() ->
                (svc?.threadLabel(convoId)?.substringAfter('\n', "") ?: "").ifEmpty { groupName }
            group && !chat?.name.isNullOrEmpty() -> chat!!.name
            sender.isNotEmpty() -> sender
            else -> chat?.name?.ifEmpty { null } ?: "Chat"
        }
        val photoId = Avatars.notifId(convoId)
        val photo = photoId?.let { Avatars.cached(ctx, it) }
        val color = chat?.color ?: svc?.avatarColorFor(convoId) ?: Avatars.DEFAULT_COLOR
        val icon = Icon.createWithBitmap(
            if (photo != null) Avatars.roundIcon(photo, ICON_PX)
            else Avatars.letterIcon(if (group) label else (chat?.name?.ifEmpty { null } ?: label), color, ICON_PX))
        val shortcutId = "mm:$convoId"
        val them = Person.Builder().setName(label).setKey(shortcutId).setIcon(icon).build()
        publishShortcut(ctx, shortcutId, label, icon, them, convoId, ownerServiceId,
                        if (photo != null) "p:$photoId" else "l:$color")
        val from = if (group) Person.Builder().setName(sender.ifEmpty { label })
                                  .setKey("$shortcutId/$sender").build()
                   else them
        val style = Notification.MessagingStyle(Person.Builder().setName("You").setKey("mm:me").build())
            .setGroupConversation(group)
            .setConversationTitle(title)
        if (picture != null) {
            val m = Notification.MessagingStyle.Message("[Image]", whenMs, from)
            Avatars.pictureUri(ctx, convoId, picture, whenMs)?.let { m.setData("image/jpeg", it) }
            style.addMessage(m)
            val cap = pictureCaption(body)
            if (cap.isNotEmpty()) style.addMessage(Notification.MessagingStyle.Message(cap, whenMs, from))
        } else {
            style.addMessage(Notification.MessagingStyle.Message(body, whenMs, from))
        }
        Convo(style, shortcutId, fetchPhoto = photo == null && (photoId != null || !Avatars.notifIdKnown(convoId)))
    } catch (e: Throwable) {
        LogStore.w("MirrorMsgSpike", "Conversation notification not built: ${e.message}")
        null
    }

    // The shortcut opens the chat as tapping its notification does: in the owning
    // account's own app (the trampoline, by the serviceId baked in here).
    @android.annotation.TargetApi(Build.VERSION_CODES.R)
    private fun publishShortcut(ctx: Context, id: String, label: String, icon: Icon,
                                person: Person, convoId: String, ownerServiceId: String, sig: String) {
        if (pushed[id] == "$label|$ownerServiceId|$sig") return
        try {
            val sm = ctx.getSystemService(ShortcutManager::class.java) ?: return
            val intent = Intent(ctx, OpenInAppActivity::class.java).setAction(Intent.ACTION_VIEW)
                .putExtra(EXTRA_CONVO_ID, convoId)
                .putExtra(EXTRA_SERVICE_ID, ownerServiceId)
            sm.pushDynamicShortcut(ShortcutInfo.Builder(ctx, id)
                .setShortLabel(label.take(40).ifEmpty { "Chat" })
                .setLongLived(true)
                .setIntent(intent)
                .setIcon(icon)
                .setPerson(person)
                .setCategories(setOf(ShortcutInfo.SHORTCUT_CATEGORY_CONVERSATION))
                .build())
            pushed[id] = "$label|$ownerServiceId|$sig"
        } catch (e: Throwable) {
            LogStore.w("MirrorMsgSpike", "Conversation shortcut not published: ${e.message}")
        }
    }

    /** Dismiss the current new-message notification. Used when a message that
     *  was first notified as text turns out to be an image and instead
     *  force-opens the watch — so the stale "new message" banner goes away. */
    /** Clear a specific conversation's notification (e.g. on mark-read / open). */
    fun cancel(ctx: Context, convoId: String) {
        val mgr = ctx.getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
        mgr.cancel(notifIdFor(convoId))
    }
}
