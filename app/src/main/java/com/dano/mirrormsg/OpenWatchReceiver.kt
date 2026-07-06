package com.dano.mirrormsg

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch

/**
 * Fires when the user picks the "Open on watch" notification action.
 *
 * Runs WITHOUT launching MainActivity — a BroadcastReceiver executes even when
 * the app UI is backgrounded or closed, so it sidesteps the cold-launch /
 * must-be-foregrounded flakiness of routing through the Activity. It opens the
 * watchapp directly via the always-available Mirror sender and pushes the
 * triggering thread.
 */
class OpenWatchReceiver : BroadcastReceiver() {
    override fun onReceive(context: Context, intent: Intent) {
        val convoId = intent.getStringExtra(Notifier.EXTRA_CONVO_ID) ?: return
        // The OWNING service is baked into the notification. "Open on watch" must
        // switch the watch to that account — not just open the convo on whatever
        // account is currently active — or you land in the right thread sitting on
        // the wrong account: back-out drops you in the wrong list, and a reply
        // sends through the wrong service. Falls back to the active service for
        // legacy notifications posted before this field existed.
        val ownerServiceId = intent.getStringExtra(Notifier.EXTRA_SERVICE_ID)
            ?: Mirror.activeServiceId
        val appCtx = context.applicationContext
        Mirror.appContext = appCtx
        LogStore.i("MirrorMsgSpike", "OpenWatchReceiver fired for convo $convoId (owner=$ownerServiceId)")

        // startAppOnTheWatch is suspend; run on IO. goAsync keeps the receiver
        // alive long enough for the watch to boot and the thread push to send.
        val pending = goAsync()
        CoroutineScope(Dispatchers.IO).launch {
            try {
                // PebbleOS (basalt) crashes if a watchapp is launched while the
                // notification action UI is still on screen. Wait for that UI to
                // dismiss BEFORE launching. (The old activity-routed path worked
                // because the phone-foreground hop introduced this delay for us.)
                delay(1000)
                val sender = Mirror.sender(appCtx)
                Mirror.currentOpenConvoId = convoId
                val launched = sender.startAppOnTheWatch(Schema.WATCH_UUID, watches = null)
                LogStore.i("MirrorMsgSpike", "Receiver startAppOnTheWatch -> $launched")
                delay(1200)
                // Full PN sequence: switch active account to the owner, refresh its
                // list + rail, then open the convo with its row — so the thread,
                // the account, the back-out target, and reply routing all agree.
                // "Open on watch" lands on the chat (message in context); if the
                // triggering message is a picture, land on the enhanced view instead
                // so the image shows big immediately.
                val view = if (LiveRouter.convoLatestHasImage(convoId))
                    Schema.VIEW_ENHANCED else Schema.VIEW_CHAT
                PebbleListenerService.instance?.pushForceOpen(convoId, view, ownerServiceId)
            } catch (e: Exception) {
                LogStore.e("MirrorMsgSpike", "OpenWatchReceiver failed", e)
            } finally {
                pending.finish()
            }
        }
    }
}
