package com.dano.mirrormsg

import android.app.Activity
import android.content.Context

/**
 * MessagingService is the contract every messaging network implements. It is the
 * single abstraction the CORE of the app depends on — MainActivity, Mirror,
 * PebbleListenerService, ReplyReceiver talk to *this*, never to a concrete
 * network class or a bridge AAR.
 *
 * That decoupling is what makes networks drop-in/drop-out modular: a network is
 * a class implementing this interface plus its own bridge AAR. The core never
 * names Google or Facebook directly, so adding or removing a network's files
 * never touches (or breaks) the core. Discovery happens at runtime via
 * [ServiceRegistry], which finds implementations by reflection — present class
 * → network available; absent class → network simply isn't there.
 *
 * GoogleMessagesService is the first implementation (wrapping the existing
 * libgm/mirrorbridge AAR). FacebookMessengerService is the second (wrapping the
 * messagix/fbmessagebridge AAR), and is a true drop-out pair with its AAR.
 *
 * Design notes:
 *  - The data methods ([listConversations], [fetchMessages], [sendMessage],
 *    [downloadImage], [processInlineImage]) are the UNIVERSAL read/send surface.
 *    Their return shapes are JSON strings, exactly as the bridges already
 *    produce, so the existing parsing in PebbleListenerService is unchanged.
 *  - [sendMessage] takes an OPAQUE replyToken string rather than network-specific
 *    routing args. Google packs SIM routing (participantID/simTwo/simNumber) into
 *    it and unpacks it inside its adapter; Facebook ignores it. The core and the
 *    watch only ever echo the token back — they never interpret it. One signature
 *    serves every network.
 *  - [startLogin] is per-network: Google opens the cookie WebView + Gaia pairing;
 *    Facebook opens its own cookie harvest. The core just calls startLogin and
 *    lets the service do whatever that network needs.
 */
interface MessagingService {

    // ---- Identity & state --------------------------------------------------

    /** Stable lowercase id, e.g. "google", "facebook". Used as the service tag. */
    val id: String

    /** Human-facing name shown in the Networks tab, e.g. "Google Messages". */
    val displayName: String

    /** True if this network has a valid, usable session right now. */
    fun isLoggedIn(): Boolean

    /**
     * Mutual-exclusion key for networks that exist via BOTH a native bridge and a
     * Beeper bridge. Such twins declare the SAME non-null group on both sides
     * (e.g. native Facebook and Beeper Facebook both return "facebook");
     * [ServiceRegistry.conflictingActive] then forbids turning both on at once,
     * since two clients on one account fight over the connection. Default null
     * means "no twin, no conflict." Keyed on this stable string, never on
     * [displayName], so a cosmetic rename can't silently break the safety rule.
     */
    val conflictGroup: String? get() = null

    /**
     * Whether this service counts as "on" for [conflictGroup] mutual exclusion.
     * Defaults to [isLoggedIn] — a native service is active when it holds a
     * session. Beeper services override this to read their enabled state
     * (BeeperState) instead, so enabling a Beeper account blocks its native twin
     * immediately, before any sync. Callers wrap this defensively.
     */
    fun isActive(ctx: Context): Boolean = isLoggedIn()

    /**
     * The reaction actorKey representing THIS user on this network, if known — e.g.
     * the logged-in Facebook fbid. Used so a reaction the user sends from the watch
     * is stored optimistically under the SAME key the network's own reaction events
     * carry, letting a later change/removal from the native app reconcile it. Null
     * (the default) means "unknown" and callers fall back to a synthetic key.
     */
    fun selfActorKey(): String? = null

    /**
     * Short label for the account subtitle (phone number for Google, the FB
     * display name/handle for Facebook). Empty string if unknown yet.
     */
    fun accountLabel(): String

    // ---- Avatar color (per-account brand family) ---------------------------

    /**
     * This account's avatar background palette as "#RRGGBB" hex strings — the
     * account's brand-color family (Facebook blues, Discord blurples, X blacks,
     * ...). The watch fills each conversation's avatar circle with one of these
     * and auto-picks a readable letter color, so ONLY the background is defined
     * here.
     *
     * Declared in the ACCOUNT'S OWN file (the single per-account part), so a
     * network's colors travel with it — drop the network's file and its colors
     * go too, with nothing left behind in shared code. Empty (the default) means
     * "no brand palette": the pipeline then uses any bridge-provided color (e.g.
     * Google's per-contact colors, darkened at the source) or the global default.
     * Keep entries under ~140 luma if you want white initials (the branded look).
     */
    val avatarPalette: List<String> get() = emptyList()

    /**
     * Pick a STABLE avatar color for one conversation out of [avatarPalette], so
     * a given thread always gets the same shade while different threads in the
     * account still vary. This is the SHARED selection engine — every network
     * reuses it; networks define the palette, never this. Returns null when the
     * palette is empty (the pipeline then defers to bridge color / default).
     */
    fun avatarColorFor(conversationKey: String): String? {
        val pal = avatarPalette
        if (pal.isEmpty()) return null
        val h = conversationKey.hashCode().toLong() and 0xFFFFFFFFL
        return pal[(h % pal.size).toInt()]
    }

    // ---- Lifecycle ---------------------------------------------------------

    /**
     * Restore any saved session and bring the network online (open the realtime
     * channel / long-poll). Safe to call on a background thread; may block.
     * No-op if already connected. Returns true if connected/usable afterward.
     */
    fun connectIfPossible(): Boolean

    /** Tear down the live connection (but keep the saved session). */
    fun disconnect()

    /** Clear the saved session/cookies and fully sign out of this network. */
    fun logout(context: Context)

    // ---- Login (per-network) ----------------------------------------------

    /**
     * Begin this network's login flow. Each network does its own thing —
     * Google opens the cookie WebView and Gaia pairing; Facebook opens its own
     * cookie harvest. The core just calls this; it does not know the mechanism.
     */
    fun startLogin(activity: Activity)

    // ---- Universal data surface (JSON in/out, as the bridges produce) ------

    /** Inbox conversations as JSON (same shape the bridge returns). */
    fun listConversations(count: Int): String

    /**
     * True if this service navigates a hierarchy (buckets/servers/folders) rather
     * than a flat inbox. Flat services (the default) stream one OPEN list; the watch
     * treats every entry as a chat. Hierarchical services override [listContext].
     */
    val hierarchical: Boolean get() = false

    /**
     * Conversations for a navigation context — the list the watch should show for the
     * current drill path. [context] is the path from the account root (empty = root).
     * Default: flat — the account's inbox, ignoring context. Hierarchical services
     * override to return a mix of ENTER entries (buckets/servers: JSON field
     * "navKind":"ENTER" + "navContext":"<token>") and OPEN chats (no navKind).
     */
    fun listContext(count: Int, context: List<String>): String = listConversations(count)

    /**
     * The nav-context stack a force-open (notification / "Open on watch") should land
     * in, so the streamed list CONTAINS the target and BACK climbs coherently. Default:
     * a hierarchical account drops into its "dms" bucket; a flat account stays at root.
     * A network with deeper structure (e.g. Discord servers) overrides this to return
     * the target's real home — its server's channel list — so back-out lands there.
     */
    fun forceOpenNavContext(convoId: String): List<String> =
        if (hierarchical) listOf("dms") else emptyList()

    /**
     * Routing-context label for the watch's thread detail-view banner. Default: just
     * [displayName] (the network name). Drillable networks (Discord servers/channels)
     * override to return "<network>\n<server>#<channel>"; the watch treats the presence
     * of a '\n' as the two-line-banner signal, so no separate "is drillable" flag is
     * needed. Flat accounts (DMs, Google, WhatsApp, Telegram DMs) keep the single line.
     */
    fun threadLabel(convoId: String): String = displayName

    /** Messages for a conversation as JSON. cursor "" = most recent. */
    fun fetchMessages(conversationId: String, count: Long, cursor: String): String

    /**
     * Send a text message. [replyToken] is opaque routing the service itself
     * produced (e.g. via [replyTokenFor]) and knows how to interpret; the core
     * passes it back untouched. Returns the bridge's JSON response.
     */
    fun sendMessage(conversationId: String, text: String, replyToken: String): String

    /**
     * Produce the opaque reply token for a conversation, to be stored and later
     * handed back to [sendMessage]. Google encodes SIM routing here; Facebook
     * can return "" since it needs no routing. Keeps all network-specific reply
     * routing inside the adapter.
     */
    fun replyTokenFor(conversationId: String): String

    // ---- Targeted send: reply-to & reaction (OPT-IN) -----------------------

    /**
     * Whether this service can send a REPLY targeted at a specific message (a
     * quoted reply), rather than a plain message to the conversation. Default
     * false — a service opts in only if its bridge can do it. When false, a
     * detail-view reply falls back to [sendMessage], so the network keeps working
     * exactly as before. This is the whole "opt-in" mechanism: capability lives
     * with the service, the core gates behavior on it.
     */
    val supportsReplyTo: Boolean get() = false

    /**
     * Whether this service can send a REACTION to a specific message. Default
     * false — opt in only if supported. When false, an emoji-only detail-view
     * reply falls back to a normal [sendMessage] containing the emoji.
     */
    val supportsReactionSend: Boolean get() = false

    /**
     * Send a quoted reply to [targetMessageId] within [conversationId]. Only
     * called when [supportsReplyTo] is true. [targetFromMe] is whether the message
     * being replied to was sent by us (some networks fold it into the target's
     * addressing). Returns the bridge's JSON response. Default throws — declaring
     * support without implementing is a bug.
     */
    fun sendReplyTo(
        conversationId: String, targetMessageId: String,
        targetFromMe: Boolean, text: String
    ): String = throw UnsupportedOperationException("$id does not support reply-to")

    /**
     * Send reaction [emoji] to [targetMessageId] within [conversationId] (an empty
     * emoji removes it). Only called when [supportsReactionSend] is true.
     * [targetFromMe] is whether the target message was sent by us. Returns the
     * bridge's JSON response. Default throws.
     */
    fun sendReaction(
        conversationId: String, targetMessageId: String,
        targetFromMe: Boolean, emoji: String
    ): String = throw UnsupportedOperationException("$id does not support reaction send")

    /** Download+scale a media image for the watch. Returns the bridge's JSON. */
    fun downloadImage(
        mediaId: String, decryptionKeyB64: String,
        maxW: Int, maxH: Int, maxBytes: Int, quality: Int
    ): String

    /** Process an inline (already-embedded) image for the watch. Returns JSON. */
    fun processInlineImage(
        mediaDataB64: String,
        maxW: Int, maxH: Int, maxBytes: Int, quality: Int
    ): String
}
