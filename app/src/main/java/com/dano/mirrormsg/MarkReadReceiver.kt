package com.dano.mirrormsg

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch

/**
 * Fires when the user picks the "Mark as read" notification action.
 *
 * Resolves the OWNING service by the serviceId baked into the notification —
 * NEVER the active account (same tagged-destination rule as reply / open-on-
 * watch: inferring the active account once leaked a message into the wrong
 * conversation). It asks that service to mark the thread read on the SOURCE
 * network (up through the triggering message), then clears the local
 * notification. Runs WITHOUT launching MainActivity — a BroadcastReceiver fires
 * even when the UI is closed — and off the main thread, because markRead does a
 * network call (e.g. libgm's read receipt to Google Messages).
 */
class MarkReadReceiver : BroadcastReceiver() {
    override fun onReceive(context: Context, intent: Intent) {
        val convoId = intent.getStringExtra(Notifier.EXTRA_CONVO_ID) ?: return
        val messageId = intent.getStringExtra(Notifier.EXTRA_MESSAGE_ID) ?: ""
        // The OWNING service is baked in at post time. Fall back to the active
        // service only for legacy notifications posted before this field existed.
        val ownerServiceId = intent.getStringExtra(Notifier.EXTRA_SERVICE_ID)
            ?: Mirror.activeServiceId
        val appCtx = context.applicationContext
        Mirror.appContext = appCtx
        LogStore.i("MirrorMsgSpike",
            "MarkReadReceiver fired for convo $convoId msg $messageId (owner=$ownerServiceId)")

        // The user asked to be done with this notification — clear it now. The
        // source-side read receipt runs off-thread below (and shouldn't hold up
        // dismissing the wrist/phone banner).
        Notifier.cancel(appCtx)

        val pending = goAsync()
        CoroutineScope(Dispatchers.IO).launch {
            try {
                val svc = ServiceRegistry.byId(appCtx, ownerServiceId)
                if (svc == null || !svc.supportsMarkRead) {
                    LogStore.w("MirrorMsgSpike",
                        "MarkReadReceiver: $ownerServiceId can't mark-read; notification cleared only")
                } else if (messageId.isEmpty()) {
                    LogStore.w("MirrorMsgSpike",
                        "MarkReadReceiver: no messageId for convo $convoId; notification cleared only")
                } else {
                    val res = svc.markRead(convoId, messageId)
                    LogStore.i("MirrorMsgSpike", "markRead($convoId) -> $res")
                }
            } catch (e: Exception) {
                LogStore.e("MirrorMsgSpike", "MarkReadReceiver failed", e)
            } finally {
                pending.finish()
            }
        }
    }
}
