package com.dano.mirrormsg

import android.content.Context

/**
 * ServiceRegistry discovers the messaging networks that are actually present in
 * this build and exposes them to the core. It is the mechanism that makes
 * networks drop-in/drop-out: each known network is looked up BY CLASS NAME via
 * reflection (Class.forName), wrapped in try/catch. If the class is in the APK,
 * it registers and the network is available; if its files were removed before
 * building, the lookup fails, it's caught, and the network simply never
 * registers — it shows as Locked, and the core is none the wiser.
 *
 * Crucially, this file (and the rest of the core) NEVER imports a concrete
 * network class. The only reference to FacebookMessengerService anywhere in the
 * core is the STRING below. So deleting FacebookMessengerService.kt +
 * fbmessagebridge.aar leaves zero dangling compile-time references — the core
 * still builds, and Facebook is Locked.
 *
 * Tradeoff (accepted deliberately): because the wiring is a class-name string,
 * not a compiler-checked reference, renaming a service class without updating
 * the string here will silently fail to find it at runtime. The class names are
 * kept in one place (CANDIDATES) and well-commented to mitigate that.
 */
object ServiceRegistry {

    /**
     * The fully-qualified class names of every network adapter the app knows
     * about, in display order. A name being listed here does NOT mean the class
     * exists in this build — that's exactly what the reflective load checks. To
     * add a future network (e.g. Signal), add its class name here and ship its
     * files; to drop one, remove its files (leaving the name here is harmless —
     * it just won't resolve).
     *
     * KEEP IN SYNC with the actual class names. If you rename an adapter class,
     * update its entry here too.
     */
    private val CANDIDATES = listOf(
        "com.dano.mirrormsg.GoogleMessagesService",
        "com.dano.mirrormsg.FacebookMessengerService",
        "com.dano.mirrormsg.WhatsAppService",
        "com.dano.mirrormsg.TelegramService",
        // Native Discord (id "discord") + Beeper Discord (id "dcbeeper", below):
        // distinct ids, conflictGroup "discord" keeps only one active at a time.
        "com.dano.mirrormsg.DiscordService",
        // Native Signal (id "signalnative") + Beeper Signal (id "signal", below):
        // distinct ids, conflictGroup "signal" keeps only one active at a time.
        "com.dano.mirrormsg.SignalService",
        // Native X (id "xnative") + Beeper X (id "x", below): distinct ids,
        // conflictGroup "x" keeps only one active at a time.
        "com.dano.mirrormsg.XService",
        // Native Instagram (id "instagram") + Beeper Instagram (id "igbeeper", below):
        // distinct ids, conflictGroup "instagram" keeps only one active at a time.
        "com.dano.mirrormsg.InstagramService",
        "com.dano.mirrormsg.BeeperDiscordService",
        "com.dano.mirrormsg.BeeperXService",
        "com.dano.mirrormsg.BeeperFacebookService",
        "com.dano.mirrormsg.BeeperSignalService",
        "com.dano.mirrormsg.BeeperWhatsAppService",
        "com.dano.mirrormsg.BeeperTelegramService",
        "com.dano.mirrormsg.BeeperiMessageService",
        "com.dano.mirrormsg.BeeperInstagramService",
        "com.dano.mirrormsg.BeeperLinkedInService",
        "com.dano.mirrormsg.BeeperSlackService",
        "com.dano.mirrormsg.BeeperGoogleChatService",
        "com.dano.mirrormsg.BeeperLineService",
        // Native WhatsApp (id "whatsapp") and Beeper WhatsApp (id "wabeeper")
        // coexist like the two Facebook paths: distinct ids, so no double-delivery
        // — a user picks native (no extra app) or Beeper (reuse an existing Beeper
        // link instead of burning a WhatsApp linked-device slot). Both display as
        // "WhatsApp"; the switcher's Beeper category header tells them apart.
        // Future: e.g. another network — add its class name here and ship its file.
    )

    /**
     * Every network the app is *aware* of, whether or not it's present, for the
     * Networks tab. Present ones come from reflection; absent ones are shown as
     * Locked placeholders. (Name + id only — no live object for absent ones.)
     */
    data class KnownNetwork(
        val id: String,
        val displayName: String,
        val service: MessagingService?,  // null = not present in this build (Locked)
    )

    @Volatile private var loaded = false
    private val services = mutableListOf<MessagingService>()

    /** Networks that aren't compiled in, shown as Locked. id -> displayName. */
    private val LOCKED_PLACEHOLDERS = linkedMapOf(
        "facebook" to "Facebook Messenger",
        "discord"  to "Discord",
        "dcbeeper" to "Discord",
        "x"        to "X chats",
        "signal"   to "Signal",
        "whatsapp" to "WhatsApp",
        "fbmessenger" to "Facebook Messenger",
        "wabeeper" to "WhatsApp",
        "tgbeeper" to "Telegram",
        "imessage" to "iMessage",
        "igbeeper" to "Instagram",
        "linkedin" to "LinkedIn",
        "slack" to "Slack",
        "googlechat" to "Google Chat",
        "line" to "LINE",
    )

    /**
     * Reflectively instantiate every candidate adapter that's present. Called
     * once, lazily. Each adapter must have a no-arg constructor (or a companion
     * `instance()` — see the load logic). Failures (class absent, or its bridge
     * AAR absent so the class fails to link) are swallowed: that network is just
     * treated as not installed.
     */
    @Synchronized
    fun ensureLoaded(context: Context) {
        if (loaded) return
        loaded = true
        for (className in CANDIDATES) {
            try {
                val clazz = Class.forName(className)
                // Adapters expose a no-arg constructor. (If a class needs the
                // app Context it can pull Mirror.appContext itself.)
                val instance = clazz.getDeclaredConstructor().newInstance()
                if (instance is MessagingService) {
                    services.add(instance)
                    LogStore.i("ServiceRegistry", "Loaded network: ${instance.id}")
                }
            } catch (e: Throwable) {
                // ClassNotFoundException (files removed), NoClassDefFoundError
                // (bridge AAR absent), or any init failure — treat as absent.
                LogStore.d("ServiceRegistry", "Network not present: $className (${e.javaClass.simpleName})")
            }
        }
    }

    /** The live, present services (those whose files are in this build). */
    fun available(context: Context): List<MessagingService> {
        ensureLoaded(context)
        return services.toList()
    }

    /** The present services that are currently logged in. */
    fun loggedIn(context: Context): List<MessagingService> =
        available(context).filter { it.isLoggedIn() }

    /** Look up a present service by its id, or null. */
    fun byId(context: Context, id: String): MessagingService? =
        available(context).firstOrNull { it.id == id }

    /**
     * Mutual-exclusion check for networks that exist via both a native bridge and
     * a Beeper bridge (the two share a non-null [MessagingService.conflictGroup] —
     * e.g. native "facebook" + Beeper "fbmessenger" both report group "facebook").
     * Letting both run logs two clients into one Meta/WhatsApp account and they
     * fight over the connection, which we've watched happen.
     *
     * If turning on [serviceId] would collide with an already-active service in
     * the same group, returns that other service; otherwise null. "Active" is each
     * service's own [MessagingService.isActive] — native services answer from their
     * session, Beeper services from their enabled state (BeeperState) — so the
     * registry never reaches across the native<->Beeper boundary. isActive() is
     * wrapped defensively: a service that throws while reporting state counts as
     * not-active rather than crashing the check.
     *
     * Call BEFORE starting a login / enabling an account and block (naming the
     * returned service) if non-null. Checking after is too late — once the new one
     * is on, both read active.
     */
    fun conflictingActive(context: Context, serviceId: String): MessagingService? {
        val group = byId(context, serviceId)?.conflictGroup ?: return null
        return available(context).firstOrNull { svc ->
            svc.id != serviceId &&
                svc.conflictGroup == group &&
                runCatching { svc.isActive(context) }.getOrDefault(false)
        }
    }

    /**
     * The full Networks-tab picture: every present service (Connected or
     * available-to-sign-in) plus Locked placeholders for networks not compiled
     * in. Present services suppress their matching placeholder.
     */
    fun knownNetworks(context: Context): List<KnownNetwork> {
        ensureLoaded(context)
        val result = mutableListOf<KnownNetwork>()
        val presentIds = services.map { it.id }.toSet()
        for (svc in services) {
            result.add(KnownNetwork(svc.id, svc.displayName, svc))
        }
        for ((id, name) in LOCKED_PLACEHOLDERS) {
            if (id !in presentIds) result.add(KnownNetwork(id, name, null))
        }
        return result
    }
}
