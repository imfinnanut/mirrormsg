package com.dano.mirrormsg

/**
 * iMessage via Beeper. Requires the user to bridge iMessage into their Beeper
 * account with a self-hosted iMessage bridge (e.g. corten-matrix / mautrix-imessage
 * registered to Beeper); once bridged, iMessage chats flow through the same
 * com.beeper.api content provider as every other Beeper network, so no special
 * handling is needed here. All behavior lives in [BeeperServiceBase]; this class
 * only declares identity + palette.
 *
 * `protocol` = "imessagego" — CONFIRMED from the provider's distinct-protocol dump
 * (a tester running a self-hosted bridge into Beeper), not guessed.
 *
 * Note: this is a power-user network — it depends on the user running their own
 * iMessage bridge on a Mac. Like all Beeper networks it's read-rich/write-thin
 * (read messages + reactions; send plain text only; no reply-to / reaction-send).
 *
 * Discovered by [ServiceRegistry] via its no-arg constructor.
 */
class BeeperiMessageService : BeeperServiceBase() {
    override val id = "imessage"
    override val displayName = "iMessage"
    override val protocol = "imessagego"

    // Avatar circle colors (iMessage blue family). All under ~140 luma -> white
    // initials.
    override val avatarPalette = listOf("#0B93F6", "#0A7CE0", "#0866C2")
}
