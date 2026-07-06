package com.dano.mirrormsg

import android.app.Activity
import android.content.Intent
import android.os.Bundle

/**
 * Invisible trampoline that reliably opens the owning account's native app.
 *
 * The problem: launching another app's activity directly from a notification
 * action that originated on the watch is a BACKGROUND activity launch, which
 * Android 10+ blocks intermittently ("works the first time, then nothing"). A
 * BroadcastReceiver doing startActivity hits the same wall.
 *
 * The fix: the notification action launches OUR OWN activity (always permitted —
 * it's our app responding to a user tap). Once WE are the foreground activity,
 * launching the target app is a normal FOREGROUND launch, which BAL allows. We
 * render nothing, fire the deep link, and finish immediately.
 *
 * WHICH app is decided by the OWNING serviceId (baked into the intent as
 * EXTRA_SERVICE_ID), so a Telegram notification opens Telegram, a WhatsApp one
 * opens WhatsApp, Beeper-bridged ones open Beeper, etc. If the owning app isn't
 * installed (DeepLink returns null), we fall back to opening MirrorMsg itself on
 * this conversation, so a tap never lands on nothing.
 */
class OpenInAppActivity : Activity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        val convoId = intent?.getStringExtra(Notifier.EXTRA_CONVO_ID)
        val serviceId = intent?.getStringExtra(Notifier.EXTRA_SERVICE_ID)
        Mirror.appContext = applicationContext
        if (convoId == null) { finish(); return }

        val address = Mirror.convoTargets[convoId]?.address ?: ""
        val deep = DeepLink.openForService(applicationContext, serviceId, address)
        if (deep == null) {
            // Owning app not installed/resolvable — open MirrorMsg on this convo.
            LogStore.i("MirrorMsgSpike", "OpenInApp(activity): no native app for ${serviceId ?: "?"} — opening MirrorMsg")
            openSelf(convoId)
            finish(); return
        }
        try {
            // We're foreground now, so this is a permitted foreground launch.
            deep.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
            startActivity(deep)
            LogStore.i("MirrorMsgSpike", "OpenInApp(activity): launched ${serviceId ?: "?"} app")
        } catch (e: Exception) {
            // The app vanished between build and tap (or refused the intent) —
            // don't leave the user on nothing; open MirrorMsg instead.
            LogStore.e("MirrorMsgSpike", "OpenInApp(activity): launch failed, opening MirrorMsg", e)
            openSelf(convoId)
        }
        finish()
    }

    private fun openSelf(convoId: String) {
        try {
            startActivity(Intent(this, MainActivity::class.java).apply {
                flags = Intent.FLAG_ACTIVITY_NEW_TASK or
                        Intent.FLAG_ACTIVITY_CLEAR_TOP or
                        Intent.FLAG_ACTIVITY_SINGLE_TOP
                putExtra(Notifier.EXTRA_CONVO_ID, convoId)
                putExtra(Notifier.EXTRA_FROM_NOTIF, true)
            })
        } catch (e: Exception) {
            LogStore.e("MirrorMsgSpike", "OpenInApp(activity): openSelf failed", e)
        }
    }
}
