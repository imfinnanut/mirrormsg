package com.dano.mirrormsg

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent

/**
 * Fires when the user picks the "Open in app" notification action (from the
 * phone shade OR relayed to the Pebble).
 *
 * Why a BroadcastReceiver instead of a getActivity() PendingIntent: launching an
 * activity DIRECTLY from a notification action that originated off-device (the
 * watch) is treated by Android 10+ as a background activity launch and is
 * silently blocked most of the time — which is exactly the "works once, then
 * nothing" behaviour we saw. A notification ACTION that targets a
 * BroadcastReceiver carries a short-lived launch grant tied to the user's tap,
 * so launching the deep-link activity from inside onReceive() is permitted. This
 * is the same pattern the reliable "Open on watch" action uses.
 */
class OpenInAppReceiver : BroadcastReceiver() {
    override fun onReceive(context: Context, intent: Intent) {
        val convoId = intent.getStringExtra(Notifier.EXTRA_CONVO_ID) ?: return
        val appCtx = context.applicationContext
        Mirror.appContext = appCtx
        LogStore.i("MirrorMsgSpike", "OpenInAppReceiver fired for convo $convoId")

        val target = Mirror.convoTargets[convoId]
        if (target == null) {
            LogStore.w("MirrorMsgSpike", "OpenInApp: no deep-link target for convo $convoId")
            return
        }
        val deep = DeepLink.openConversationOrInbox(appCtx, target)
        if (deep == null) {
            LogStore.w("MirrorMsgSpike", "OpenInApp: no resolvable intent for $target")
            return
        }
        // Required when starting an activity from a non-activity context.
        deep.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
        try {
            appCtx.startActivity(deep)
            LogStore.i("MirrorMsgSpike", "OpenInApp: launched ${target.protocol} deep link")
        } catch (e: Exception) {
            LogStore.e("MirrorMsgSpike", "OpenInApp: launch failed", e)
        }
    }
}
