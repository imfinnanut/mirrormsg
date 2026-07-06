package com.dano.mirrormsg

/**
 * LinkedIn messaging via Beeper. No native twin, so no conflictGroup. All behavior
 * lives in [BeeperServiceBase]; this class only declares identity + palette.
 *
 * `protocol` = "linkedin" — CONFIRMED from a provider dump.
 *
 * Discovered by [ServiceRegistry] via its no-arg constructor.
 */
class BeeperLinkedInService : BeeperServiceBase() {
    override val id = "linkedin"
    override val displayName = "LinkedIn"
    override val protocol = "linkedin"

    // LinkedIn blue family. All under ~140 luma -> white initials.
    override val avatarPalette = listOf("#0A66C2", "#0959A8", "#08487F")
}
