package com.dano.mirrormsg

/**
 * LINE via Beeper. No native twin, so no conflictGroup.
 *
 * `protocol` = "line" — UNCONFIRMED (not in any dump yet). Once linked, read the
 * "provider protocols seen = …" line and correct if it differs. Presence gate hides
 * it until the token matches, so a wrong guess is harmless.
 *
 * Discovered by [ServiceRegistry] via its no-arg constructor.
 */
class BeeperLineService : BeeperServiceBase() {
    override val id = "line"
    override val displayName = "LINE"
    override val protocol = "line"

    // LINE green family. All under ~140 luma -> white initials.
    override val avatarPalette = listOf("#06C755", "#05A648", "#048A3C")
}
