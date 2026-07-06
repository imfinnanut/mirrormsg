package com.dano.mirrormsg

/**
 * Signal via Beeper. Signal is DMs + groups only (no channels / server
 * hierarchy), so it's a plain single-account network. All behavior lives in
 * [BeeperServiceBase]; this class only declares identity + palette.
 *
 * `protocol` = "signal" — CONFIRMED from the provider's distinct-protocol dump
 * (just "signal", not "signalgo"/"signalmeow"; the suffix is inconsistent across
 * bridges, so the token is always read from the dump rather than guessed).
 *
 * Discovered by [ServiceRegistry] via its no-arg constructor.
 */
class BeeperSignalService : BeeperServiceBase() {
    override val id = "signal"
    override val displayName = "Signal"
    override val protocol = "signal"

    // Avatar circle colors (Signal blue family). All under ~140 luma -> white
    // initials.
    override val avatarPalette = listOf("#3A76F0", "#2C5FCC", "#1F4BA8")
}
