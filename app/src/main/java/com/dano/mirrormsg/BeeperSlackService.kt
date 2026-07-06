package com.dano.mirrormsg

/**
 * Slack via Beeper. No native twin, so no conflictGroup.
 *
 * `protocol` = "slackgo" — UNCONFIRMED (not present in any dump yet; Beeper's Slack
 * bridge is Go-based, hence the guess). BeeperServiceBase logs "provider protocols
 * seen = …" on launch; once Slack is linked, read that line and correct this one
 * value if it differs. The presence gate keeps Slack hidden until the token matches,
 * so a wrong guess is harmless.
 *
 * Discovered by [ServiceRegistry] via its no-arg constructor.
 */
class BeeperSlackService : BeeperServiceBase() {
    override val id = "slack"
    override val displayName = "Slack"
    override val protocol = "slackgo"

    // Slack aubergine family. All under ~140 luma -> white initials.
    override val avatarPalette = listOf("#4A154B", "#611F69", "#3D0E3F")
}
