package com.dano.mirrormsg

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent

/**
 * Starts MirrorMsg's anchor service after the events that would otherwise leave
 * the app dead until the user manually opened it:
 *
 *  - BOOT_COMPLETED / QUICKBOOT_POWERON — a reboot. (QUICKBOOT_POWERON is the
 *    Samsung/HTC "fast boot" variant, which some builds send INSTEAD of the
 *    standard broadcast; harmless where unused.)
 *  - MY_PACKAGE_REPLACED — the app was updated. Android force-stops an app on
 *    update, so without this a `gradlew installDebug` or a Play update leaves
 *    MirrorMsg silent until launched.
 *  - ACTION_PACKAGE_UNSTOPPED (Android 15+) — the app was un-force-stopped after
 *    a task-manager kill. Ignored on older builds since it simply never arrives.
 *
 * The service uses the remoteMessaging foreground type, which is NOT among the
 * types Android 15+ forbids starting from a BOOT_COMPLETED receiver (camera,
 * dataSync, mediaPlayback, mediaProjection, microphone, phoneCall) — so this
 * start is legal. MirrorForegroundService.start() swallows any refusal, so a
 * hostile OEM policy can't turn boot into a crash loop.
 */
class BootReceiver : BroadcastReceiver() {

    override fun onReceive(context: Context, intent: Intent) {
        val action = intent.action ?: return
        LogStore.i("BootReceiver", "Received $action — starting anchor service")
        MirrorForegroundService.start(context.applicationContext)
    }
}
