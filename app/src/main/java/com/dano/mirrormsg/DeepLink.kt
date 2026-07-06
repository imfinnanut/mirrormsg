package com.dano.mirrormsg

import android.content.Context
import android.content.Intent
import android.net.Uri

/**
 * Protocol-agnostic deep-linking.
 *
 * MirrorMsg mirrors conversations from different messaging networks (Google
 * Messages RCS/SMS natively; Facebook Messenger, WhatsApp, Telegram natively;
 * and a range of networks via Beeper). When the user taps a notification (or
 * "Open on phone"), we open the NATIVE app for THAT conversation's owning
 * account — on the exact thread where a network reliably supports it, otherwise
 * just launching the app. If the owning app isn't installed, the caller falls
 * back to opening MirrorMsg itself (we signal that by returning null here).
 *
 * The owning account is authoritative for WHICH app to open — resolved from the
 * serviceId via [openForService] — not the stored convo protocol (which was
 * historically tagged SMS for everything). The address (a phone number for 1:1s)
 * is only used for the thread-level deep links that support it.
 */

/** Where a conversation lives: which network + the address/handle within it. */
data class ConvoTarget(
    val protocol: String,   // e.g. "sms", "whatsapp", "telegram", "facebook", "beeper"
    val address: String     // protocol-specific: phone number, username, room id, ...
) {
    companion object {
        const val SMS = "sms"
        const val WHATSAPP = "whatsapp"
        const val TELEGRAM = "telegram"
        const val FACEBOOK = "facebook"
        const val DISCORD = "discord"
        const val BEEPER = "beeper"
        const val SIGNAL = "signal"
        const val MATRIX = "matrix"

        fun from(protocol: String?, address: String?): ConvoTarget {
            val proto = when (protocol?.lowercase()?.trim()) {
                "whatsapp", "wa", "whatsappgo" -> WHATSAPP
                "telegram", "tg"               -> TELEGRAM
                "facebook", "fb", "messenger"  -> FACEBOOK
                "discord", "discordgo"         -> DISCORD
                "beeper"                       -> BEEPER
                "signal"                       -> SIGNAL
                "matrix"                       -> MATRIX
                "sms", "rcs", "mms", null, ""  -> SMS
                else                           -> SMS   // safe default
            }
            return ConvoTarget(proto, address ?: "")
        }
    }
}

/** Knows how to build an Android intent that opens one protocol's native app. */
interface ProtocolHandler {
    val protocol: String
    fun canHandle(ctx: Context, target: ConvoTarget): Boolean
    /** Intent that opens the native app (on the right thread where possible), or
     *  null if it can't — e.g. the app isn't installed. Caller then falls back. */
    fun intentFor(ctx: Context, target: ConvoTarget): Intent?
}

/** True if [pkg] is installed and launchable (needs a <queries> entry on API 30+). */
private fun launchIntentFor(ctx: Context, pkg: String): Intent? =
    ctx.packageManager.getLaunchIntentForPackage(pkg)?.apply {
        addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
    }

/** Google Messages / default SMS app, via the standard smsto: thread deep link. */
class SmsHandler : ProtocolHandler {
    override val protocol = ConvoTarget.SMS
    override fun canHandle(ctx: Context, target: ConvoTarget) = target.address.isNotBlank()
    override fun intentFor(ctx: Context, target: ConvoTarget): Intent? {
        if (target.address.isBlank()) return null
        // ACTION_SENDTO + smsto:<number> opens the default messaging app
        // (Google Messages) on that contact's thread. SENDTO is exempt from
        // package-visibility filtering, so no <queries> entry is needed.
        return Intent(Intent.ACTION_SENDTO,
            Uri.parse("smsto:" + Uri.encode(target.address))).apply {
            flags = Intent.FLAG_ACTIVITY_NEW_TASK
        }
    }
}

/**
 * WhatsApp. Opens the 1:1 chat via wa.me/<international-digits> when we have a
 * number, else just launches the app. Null when WhatsApp isn't installed.
 */
class WhatsAppHandler : ProtocolHandler {
    override val protocol = ConvoTarget.WHATSAPP
    override fun canHandle(ctx: Context, target: ConvoTarget) = true
    override fun intentFor(ctx: Context, target: ConvoTarget): Intent? {
        val pkg = "com.whatsapp"
        // Not installed/visible -> null so the caller opens MirrorMsg instead.
        val launch = launchIntentFor(ctx, pkg) ?: return null
        val digits = target.address.filter { it.isDigit() }
        if (digits.isNotEmpty()) {
            return Intent(Intent.ACTION_VIEW, Uri.parse("https://wa.me/$digits")).apply {
                setPackage(pkg)
                flags = Intent.FLAG_ACTIVITY_NEW_TASK
            }
        }
        return launch
    }
}

/**
 * Telegram — app-level launch. Our convo id is a numeric user id, and Telegram's
 * thread deep links need a @username (or an access hash we don't reliably hold),
 * so we open the app rather than a specific chat. Null when not installed.
 */
class TelegramHandler : ProtocolHandler {
    override val protocol = ConvoTarget.TELEGRAM
    override fun canHandle(ctx: Context, target: ConvoTarget) = true
    override fun intentFor(ctx: Context, target: ConvoTarget): Intent? =
        launchIntentFor(ctx, "org.telegram.messenger")
}

/** Facebook Messenger — app-level launch (thread deep links are unreliable). */
class FacebookHandler : ProtocolHandler {
    override val protocol = ConvoTarget.FACEBOOK
    override fun canHandle(ctx: Context, target: ConvoTarget) = true
    override fun intentFor(ctx: Context, target: ConvoTarget): Intent? =
        launchIntentFor(ctx, "com.facebook.orca")
}

/**
 * Discord — app-level launch. Our convo id is a channel/DM id, but Discord's
 * chat deep links need the guild context for server channels (which we don't
 * reliably hold), so we open the app rather than a specific chat — same posture
 * as Telegram. Null when Discord isn't installed. (This is native Discord,
 * serviceId "discord"; Beeper's Discord opens the Beeper app instead.)
 */
class DiscordHandler : ProtocolHandler {
    override val protocol = ConvoTarget.DISCORD
    override fun canHandle(ctx: Context, target: ConvoTarget) = true
    override fun intentFor(ctx: Context, target: ConvoTarget): Intent? =
        launchIntentFor(ctx, "com.discord")
}

/**
 * Beeper — app-level launch for every Beeper-bridged network (Discord, Signal,
 * X, Instagram, …). Beeper exposes no public per-room deep link, so we open the
 * app. Null when Beeper isn't installed.
 */
class BeeperHandler : ProtocolHandler {
    override val protocol = ConvoTarget.BEEPER
    override fun canHandle(ctx: Context, target: ConvoTarget) = true
    override fun intentFor(ctx: Context, target: ConvoTarget): Intent? =
        launchIntentFor(ctx, "com.beeper.android")
}

/**
 * Central resolver. Ask it for an intent given a conversation's owning account;
 * it picks the registered handler and falls back gracefully.
 */
object DeepLink {
    private val handlers: List<ProtocolHandler> = listOf(
        SmsHandler(),
        WhatsAppHandler(),
        TelegramHandler(),
        FacebookHandler(),
        DiscordHandler(),
        BeeperHandler(),
    )

    /** Map an owning serviceId to the protocol whose native app should open. */
    fun protocolForService(ctx: Context, serviceId: String?): String = when {
        serviceId == "google"   -> ConvoTarget.SMS
        serviceId == "whatsapp"  -> ConvoTarget.WHATSAPP
        serviceId == "tgnative"  -> ConvoTarget.TELEGRAM
        serviceId == "facebook"  -> ConvoTarget.FACEBOOK
        serviceId == "discord"   -> ConvoTarget.DISCORD
        // Every Beeper-bridged network opens the Beeper app.
        serviceId != null && ServiceRegistry.byId(ctx, serviceId) is BeeperServiceBase -> ConvoTarget.BEEPER
        else -> ConvoTarget.SMS   // legacy / unknown default
    }

    /** Best intent to open the owning account's native app for this conversation,
     *  or null if none applies (caller then opens MirrorMsg itself). */
    fun openForService(ctx: Context, serviceId: String?, address: String): Intent? =
        openConversationOrInbox(ctx, ConvoTarget(protocolForService(ctx, serviceId), address))

    fun openConversation(ctx: Context, target: ConvoTarget): Intent? {
        val h = handlers.firstOrNull { it.protocol == target.protocol && it.canHandle(ctx, target) }
            ?: return null
        return h.intentFor(ctx, target)
    }

    /** Resolve, or for the SMS family fall back to the messaging app's inbox. */
    fun openConversationOrInbox(ctx: Context, target: ConvoTarget): Intent? {
        openConversation(ctx, target)?.let { return it }
        if (target.protocol == ConvoTarget.SMS) {
            return Intent(Intent.ACTION_MAIN).apply {
                addCategory(Intent.CATEGORY_APP_MESSAGING)
                flags = Intent.FLAG_ACTIVITY_NEW_TASK
            }
        }
        return null
    }
}
