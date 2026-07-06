package com.dano.mirrormsg

import android.content.Context
import android.content.pm.PackageManager
import android.net.Uri

/**
 * Single source of truth for Beeper detection + enable state, shared by the
 * Networks-tab UI (MainActivity) and the Beeper adapters (BeeperServiceBase).
 *
 * STATE MODEL (what the UI renders):
 *   isInstalled       — is the Beeper app on the device at all
 *   hasPermissions    — has the user granted Beeper READ/SEND to MirrorMsg
 *   masterEnabled     — MirrorMsg's Beeper master switch. Default OFF: Beeper is
 *                       opt-in, never insta-connected.
 *   isAccountEnabled  — per-network switch. Default OFF: a freshly-linked Beeper
 *                       network does NOT auto-mirror to the watch. It appears in
 *                       Networks with its toggle off and requires an explicit
 *                       enable before it goes live.
 *   isPresent(proto)  — does the user actually have this network in their Beeper
 *                       (>=1 chat). UI hint for greying a supported-but-absent
 *                       account; not part of the active gate.
 *
 * An account is ACTIVE — it pumps, fires notifications, and appears in the watch
 * account switcher — iff:
 *   isInstalled && hasPermissions && masterEnabled && isAccountEnabled(id)
 * Turning the master OFF, or a single account OFF, makes that account go dark in
 * MirrorMsg (no watch view, no notifications, absent from the switcher) while
 * Beeper itself is untouched.
 */
object BeeperState {
    const val PKG = "com.beeper.android"
    private const val READ_PERM = "com.beeper.android.permission.READ_PERMISSION"
    private const val SEND_PERM = "com.beeper.android.permission.SEND_PERMISSION"
    private const val CHATS_URI = "content://com.beeper.api/chats"

    private const val PREFS = "mirrormsg_beeper"
    private const val K_MASTER = "master_enabled"
    private fun kAccount(id: String) = "account_enabled_$id"

    private fun prefs(ctx: Context) =
        ctx.getSharedPreferences(PREFS, Context.MODE_PRIVATE)

    // ---- Detection ---------------------------------------------------------

    fun isInstalled(ctx: Context): Boolean = try {
        ctx.packageManager.getPackageInfo(PKG, 0); true
    } catch (e: Exception) { false }

    fun hasPermissions(ctx: Context): Boolean =
        ctx.checkSelfPermission(READ_PERM) == PackageManager.PERMISSION_GRANTED &&
        ctx.checkSelfPermission(SEND_PERM) == PackageManager.PERMISSION_GRANTED

    // Short presence cache: isPresent() now feeds isLoggedIn(), which is a hot
    // path (switcher enumeration, routing), so we don't hit the content provider
    // on every call. TTL is small so a newly-connected network appears within a
    // few seconds.
    private val presenceCache = java.util.concurrent.ConcurrentHashMap<String, Pair<Boolean, Long>>()
    private const val PRESENCE_TTL_MS = 4000L

    /**
     * Does the user actually have this network connected in Beeper? (>= 1 chat.)
     * This is the difference between "the account toggle is on" and "the network
     * is really there": a supported network the user hasn't connected in Beeper
     * has zero chats, so it must NOT count as logged in.
     */
    fun isPresent(ctx: Context, protocol: String): Boolean {
        val now = System.currentTimeMillis()
        presenceCache[protocol]?.let { (was, ts) -> if (now - ts < PRESENCE_TTL_MS) return was }
        val present = try {
            val uri = Uri.parse("$CHATS_URI?protocol=${Uri.encode(protocol)}&limit=1")
            ctx.contentResolver.query(uri, null, null, null, null)?.use { it.count > 0 } ?: false
        } catch (e: Exception) { false }
        presenceCache[protocol] = present to now
        return present
    }

    // ---- Master switch (default OFF — opt-in, never insta-connected) --------

    fun masterEnabled(ctx: Context): Boolean = prefs(ctx).getBoolean(K_MASTER, false)

    fun setMasterEnabled(ctx: Context, on: Boolean) =
        prefs(ctx).edit().putBoolean(K_MASTER, on).apply()

    // ---- Per-account switch (default OFF — explicit opt-in per network) -----

    fun isAccountEnabled(ctx: Context, id: String): Boolean =
        prefs(ctx).getBoolean(kAccount(id), false)

    fun setAccountEnabled(ctx: Context, id: String, on: Boolean) =
        prefs(ctx).edit().putBoolean(kAccount(id), on).apply()

    /**
     * Enable a Beeper account, enforcing native<->Beeper mutual exclusion. If
     * another service in the same conflictGroup is already active (e.g. native
     * Facebook when enabling Beeper Facebook), this does NOT enable and returns
     * that service's display name so the UI can surface
     * "{name} is already active - turn it off first." Returns null on success.
     */
    fun tryEnableAccount(ctx: Context, id: String): String? {
        ServiceRegistry.conflictingActive(ctx, id)?.let { return it.displayName }
        setAccountEnabled(ctx, id, true)
        return null
    }

    // ---- The active gate the adapters and switcher enumeration use ----------

    fun isAccountActive(ctx: Context, id: String): Boolean =
        isInstalled(ctx) && hasPermissions(ctx) &&
        masterEnabled(ctx) && isAccountEnabled(ctx, id)
}
