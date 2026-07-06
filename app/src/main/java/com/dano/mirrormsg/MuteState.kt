package com.dano.mirrormsg

import android.content.Context

/**
 * Per-account mute state, keyed by ServiceRegistry id (native and Beeper alike).
 *
 * Muting is NOT disconnecting: a muted account stays connected and its messages
 * still flow into the MessageStore, so the user can navigate to it on the watch
 * and browse normally. What mute suppresses is the *alerting* — no notifications
 * and no pseudo-notification force-opens from that account. Useful for a network
 * like Discord where you want some chats available to read but not buzzing your
 * wrist.
 *
 * Enforced in [LiveRouter.routeIncoming] (the single notify/force-open decision
 * point). Defaults to NOT muted.
 */
object MuteState {
    private const val PREFS = "mirrormsg_mute"
    private fun key(serviceId: String) = "mute_$serviceId"
    private fun prefs(ctx: Context) =
        ctx.getSharedPreferences(PREFS, Context.MODE_PRIVATE)

    fun isMuted(ctx: Context, serviceId: String): Boolean =
        prefs(ctx).getBoolean(key(serviceId), false)

    fun setMuted(ctx: Context, serviceId: String, on: Boolean) =
        prefs(ctx).edit().putBoolean(key(serviceId), on).apply()
}
