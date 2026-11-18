package com.dano.mirrormsg

/**
 * X (formerly Twitter) DMs via Beeper. All behavior lives in [BeeperServiceBase];
 * this class only declares identity.
 *
 * `protocol` is the value Beeper's content provider reports in the chats
 * `protocol` column — the LOCAL bridge name, which isn't always the friendly name
 * (Discord is "discordgo", not "discord"). "twitter" is the documented
 * mautrix/twitter identifier; BeeperServiceBase logs the live distinct-protocol
 * list on connect ("provider protocols seen = …"), so if X chats don't appear,
 * read that log line for the exact token and update this one value.
 *
 * Coexists with native X (XService, id "xnative") via distinct ids + a shared
 * conflictGroup "x", so only one of the two runs against an X account.
 *
 * Discovered by [ServiceRegistry] via its no-arg constructor. Deleting this one
 * file removes X cleanly (it then shows as Locked).
 */
class BeeperXService : BeeperServiceBase() {
    override val id = "x"
    override val displayName = "X chats"
    override val protocol = "twitter"
    override val conflictGroup = "x"   // mutual-exclusion with native XService

    // Avatar circle colors (X black / dark-grey family). The watch outlines dark
    // avatars with a light ring in night mode, so the blacks stay visible.
    override val avatarPalette = listOf("#000000", "#555555", "#222222", "#333333")
}
