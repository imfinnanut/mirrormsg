package com.dano.mirrormsg

/**
 * Google Chat via Beeper. No native twin, so no conflictGroup. Distinct from Google
 * Messages — this is Google Chat (Workspace/Hangouts successor), protocol
 * "googlechat", not the SMS/RCS gmessages bridge.
 *
 * `protocol` = "googlechat" — UNCONFIRMED (not in any dump yet). Once linked, read
 * the "provider protocols seen = …" line and correct if it differs. Presence gate
 * hides it until the token matches, so a wrong guess is harmless.
 *
 * Discovered by [ServiceRegistry] via its no-arg constructor.
 */
class BeeperGoogleChatService : BeeperServiceBase() {
    override val id = "googlechat"
    override val displayName = "Google Chat"
    override val protocol = "googlechat"

    // Google green family. All under ~140 luma -> white initials.
    override val avatarPalette = listOf("#1E8E3E", "#188038", "#0D652D")
}
