package com.dano.mirrormsg

/**
 * Facebook Messenger via Beeper. Distinct from the native FacebookMessengerService
 * (id "facebook", the paused cookie-bridge): this Beeper path reads through the
 * on-device content provider and so reaches the E2EE DMs the native bridge can't.
 * They coexist as two separate accounts (native = "Facebook Messenger", this =
 * "Messenger"); the user enables whichever they want.
 *
 * `protocol` = "facebookgo", confirmed from the provider's distinct-protocol dump
 * (the LOCAL bridge name, like Discord's "discordgo" — not "meta"/"facebook").
 * Beeper reports a network-specific token, so this does NOT pull in Instagram.
 *
 * All behavior lives in [BeeperServiceBase]; this class only declares identity +
 * palette. Discovered by [ServiceRegistry] via its no-arg constructor.
 */
class BeeperFacebookService : BeeperServiceBase() {
    override val id = "fbmessenger"
    override val displayName = "Facebook Messenger"
    override val protocol = "facebookgo"
    override val conflictGroup = "facebook"   // mutual-exclusion with native FacebookMessengerService

    // Avatar circle colors (Messenger blue family). All under ~140 luma -> white
    // initials.
    override val avatarPalette = listOf("#0084FF", "#006AFF", "#0A5DC2")
}
