package com.dano.mirrormsg

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.Context
import android.content.Intent
import android.content.pm.ServiceInfo
import android.os.Build
import android.os.IBinder
import fi.mirrormsg.mirrorbridge.EventSink
import fi.mirrormsg.mirrorbridge.Mirrorbridge
import org.json.JSONObject
import kotlin.concurrent.thread

/**
 * The process-lifetime anchor for MirrorMsg.
 *
 * WHY THIS EXISTS: every network connection (Google's long-poll, Discord's
 * gateway, WhatsApp, Telegram, Facebook, and the Beeper observers) lives inside
 * this app's PROCESS. Before this service, those connections were established
 * only in MainActivity.onCreate and nothing held the process open afterwards, so
 * Android reclaimed it after a few idle hours and notifications silently stopped
 * until the user opened the app again. Battery-optimization exemptions don't fix
 * that: they permit the process to live, they don't give the system a REASON to
 * keep it. A foreground service is that reason.
 *
 * SERVICE TYPE — remoteMessaging, chosen deliberately:
 *  - It is what MirrorMsg literally does: transfer messages from one device
 *    (phone) to another (the Pebble).
 *  - It has NO runtime cap. dataSync (the obvious-looking pick) is limited to 6
 *    hours per 24 on Android 15+, which would reproduce the exact bug on a timer
 *    once the app targets 35+ (we target 36).
 *  - It is NOT on the Android 15 list of types banned from being started by a
 *    BOOT_COMPLETED receiver (camera, dataSync, mediaPlayback, mediaProjection,
 *    microphone, phoneCall are), so BootReceiver can legally start it.
 * Requires FOREGROUND_SERVICE + FOREGROUND_SERVICE_REMOTE_MESSAGING in the
 * manifest (both normal, install-time permissions — no runtime prompt), and the
 * type must be declared in the Play Console's App content page at submission.
 *
 * The persistent notification is not optional — it is the contract for running
 * indefinitely. It's posted on its own MIN-importance channel so it sits silently
 * at the bottom of the shade and never buzzes.
 */
class MirrorForegroundService : Service() {

    companion object {
        private const val TAG = "MirrorFgs"
        private const val CHANNEL_ID = "mirrormsg_running"
        private const val NOTIF_ID = 0x4D4D   // "MM"; distinct from message notifications

        /**
         * Start (or no-op if already running) the anchor service. Safe to call
         * repeatedly — onStartCommand is idempotent. Callers in the background
         * must be exempt from background-FGS-start restrictions; ours are: the
         * boot receiver (BOOT_COMPLETED is an allowed source for this type) and
         * MainActivity (foreground). Failures are logged, never thrown, so a
         * refusal can't crash the app on launch or at boot.
         */
        fun start(ctx: Context) {
            try {
                val i = Intent(ctx, MirrorForegroundService::class.java)
                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                    ctx.startForegroundService(i)
                } else {
                    ctx.startService(i)
                }
            } catch (e: Throwable) {
                LogStore.e(TAG, "startForegroundService refused", e)
            }
        }
    }

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onCreate() {
        super.onCreate()
        // The service may be the FIRST thing to run in this process (boot, or a
        // system restart after a kill), so it — not MainActivity — is responsible
        // for the global init that everything else assumes has happened.
        Mirror.appContext = applicationContext
        Mirror.loadSettings(this)
        LogStore.i(TAG, "Foreground service created")
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        // Post the persistent notification FIRST: Android gives a service started
        // via startForegroundService ~5 seconds to call startForeground() before
        // killing the process with an ANR-style crash. Connection bring-up happens
        // afterwards, off the main thread.
        try {
            ensureChannel()
            val notif = buildNotification()
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
                startForeground(NOTIF_ID, notif,
                    ServiceInfo.FOREGROUND_SERVICE_TYPE_REMOTE_MESSAGING)
            } else {
                startForeground(NOTIF_ID, notif)
            }
        } catch (e: Throwable) {
            // A refusal here (wrong type, missing permission, OEM quirk) must not
            // take the app down — log it and keep the process doing what it can.
            LogStore.e(TAG, "startForeground failed", e)
        }
        MirrorConnections.ensureConnected(this)
        // START_STICKY: if the system kills us under memory pressure, restart the
        // service as soon as there's room (with a null intent — onStartCommand
        // handles that fine, since it needs nothing from the intent).
        return START_STICKY
    }

    /**
     * The user swiped MirrorMsg out of Recents. On most OEM builds — Samsung
     * especially — that tears down the process even though a foreground service
     * is running. Ask the system to bring the service straight back; the restart
     * re-runs onStartCommand and re-establishes anything that was lost.
     */
    override fun onTaskRemoved(rootIntent: Intent?) {
        LogStore.i(TAG, "Task removed — requesting service restart")
        try {
            val restart = Intent(applicationContext, MirrorForegroundService::class.java)
            val pi = PendingIntent.getService(
                this, 1, restart,
                PendingIntent.FLAG_ONE_SHOT or PendingIntent.FLAG_IMMUTABLE)
            val am = getSystemService(Context.ALARM_SERVICE) as android.app.AlarmManager
            am.set(android.app.AlarmManager.RTC, System.currentTimeMillis() + 1000, pi)
        } catch (e: Throwable) {
            LogStore.e(TAG, "Restart-after-swipe scheduling failed", e)
        }
        super.onTaskRemoved(rootIntent)
    }

    override fun onDestroy() {
        LogStore.i(TAG, "Foreground service destroyed")
        super.onDestroy()
    }

    // ---- Notification ------------------------------------------------------

    /**
     * MIN importance: no sound, no vibration, no heads-up — it collapses to the
     * silent section at the bottom of the shade. (Unrelated to the message
     * notifications Pebble mirrors, which stay on their own alerting channel.)
     */
    private fun ensureChannel() {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.O) return
        val nm = getSystemService(NotificationManager::class.java) ?: return
        if (nm.getNotificationChannel(CHANNEL_ID) != null) return
        val ch = NotificationChannel(CHANNEL_ID, "MirrorMsg running",
            NotificationManager.IMPORTANCE_MIN).apply {
            description = "Keeps MirrorMsg connected so messages reach your Pebble."
            setShowBadge(false)
            enableVibration(false)
            enableLights(false)
        }
        nm.createNotificationChannel(ch)
    }

    private fun buildNotification(): Notification {
        val tapPi = PendingIntent.getActivity(
            this, 0, Intent(this, MainActivity::class.java),
            PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT)
        val b = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O)
            Notification.Builder(this, CHANNEL_ID) else @Suppress("DEPRECATION") Notification.Builder(this)
        return b.setContentTitle("MirrorMsg")
            .setContentText("Mirroring messages to your Pebble")
            .setSmallIcon(android.R.drawable.stat_sys_data_bluetooth)
            .setContentIntent(tapPi)
            .setOngoing(true)
            .setShowWhen(false)
            .build()
    }
}

/**
 * Process-lifetime connection bring-up, shared by the foreground service and
 * MainActivity so the two can never race into DUPLICATE connections (two Google
 * long-polls would double-deliver every message). Whoever runs first wins; the
 * other becomes a no-op.
 *
 * Deliberately an object, not a service member: connections outlive any single
 * service instance, and a system-initiated service restart must not tear down or
 * re-create a bridge that's still healthy.
 */
object MirrorConnections {
    private const val TAG = "MirrorConn"

    @Volatile private var bringUpStarted = false

    /**
     * Ensure every logged-in network is connected. Idempotent and cheap to call
     * repeatedly (app launch, service start, service restart).
     *
     * Google is special-cased: its bridge is a Go object that needs an EventSink.
     * When MainActivity is running it supplies its own UI-capable sink (pairing
     * emoji); headless, we install the sink below, which handles the same events
     * minus the UI. Either way the bridge is created ONCE — whichever side gets
     * there first — because Mirror.bridge is the single owner.
     */
    fun ensureConnected(ctx: Context) {
        if (bringUpStarted) {
            LogStore.i(TAG, "ensureConnected: already brought up this process")
            return
        }
        bringUpStarted = true
        thread(name = "mirror-connect") {
            try {
                restoreGoogleIfNeeded(ctx)
                connectRegisteredServices(ctx)
            } catch (e: Throwable) {
                LogStore.e(TAG, "Connection bring-up failed", e)
            }
        }
    }

    /** Google (libgm): restore the saved session into a bridge and connect it. */
    private fun restoreGoogleIfNeeded(ctx: Context) {
        if (Mirror.bridge != null) {
            LogStore.i(TAG, "Google bridge already present — skipping restore")
            return
        }
        val prefs = ctx.getSharedPreferences("mirrormsg", Context.MODE_PRIVATE)
        val saved = prefs.getString("session", "") ?: ""
        if (saved.isEmpty()) {
            LogStore.i(TAG, "No saved Google session — nothing to restore")
            return
        }
        try {
            val b = Mirrorbridge.newBridge(saved, headlessSink(ctx))
            Mirror.bridge = b
            if (b.isLoggedIn) {
                b.connect()
                LogStore.i(TAG, "Restored + connected (google, headless)")
            } else {
                prefs.edit().remove("session").apply()
            }
        } catch (e: Exception) {
            LogStore.e(TAG, "Headless Google restore failed", e)
        }
    }

    /**
     * Every non-Google registered service via its own self-gating
     * connectIfPossible() (a no-op when that network has no saved session).
     * Mirrors MainActivity.connectRegisteredServices so a headless start brings
     * up exactly the same set — including the Beeper ContentObserver/poll, which
     * registers inside connectIfPossible.
     */
    private fun connectRegisteredServices(ctx: Context) {
        try {
            for (svc in ServiceRegistry.available(ctx)) {
                if (svc.id == "google") continue
                try {
                    val ok = svc.connectIfPossible()
                    LogStore.i(TAG, "Connect ${svc.id}: connectIfPossible=$ok")
                } catch (e: Throwable) {
                    LogStore.e(TAG, "Connect ${svc.id} failed", e)
                }
            }
        } catch (e: Throwable) {
            LogStore.e(TAG, "Registered-service connect failed", e)
        }
    }

    /**
     * The no-Activity twin of MainActivity's EventSink: same MESSAGE /
     * CONVERSATION / LOGGED_OUT handling (which is what keeps notifications
     * flowing), minus PAIR_EMOJI's UI — pairing only happens during interactive
     * login, when MainActivity is on screen with its own sink.
     */
    private fun headlessSink(ctx: Context) = object : EventSink {
        override fun onEvent(eventType: String, jsonPayload: String) {
            when (eventType) {
                "MESSAGE" -> {
                    LogStore.i(TAG, "MESSAGE $jsonPayload")
                    LiveRouter.ingestLiveMessage("google", jsonPayload)
                }
                "CONVERSATION" -> {
                    LogStore.i(TAG, "CONVERSATION $jsonPayload")
                    Mirror.invalidateConvoList()
                    if (Mirror.accountNumber.isEmpty()) {
                        try {
                            val num = JSONObject(jsonPayload)
                                .optJSONObject("simCard")
                                ?.optJSONObject("SIMData")
                                ?.optString("formattedPhoneNumber") ?: ""
                            if (num.isNotEmpty()) Mirror.setAccountNumber(ctx, num)
                        } catch (e: Exception) { /* ignore parse miss */ }
                    }
                }
                "LOGGED_OUT" -> {
                    LogStore.e(TAG, "Session died — clearing saved session.")
                    ctx.getSharedPreferences("mirrormsg", Context.MODE_PRIVATE)
                        .edit().remove("session").apply()
                }
                else -> LogStore.d(TAG, "$eventType $jsonPayload")
            }
        }
    }
}
