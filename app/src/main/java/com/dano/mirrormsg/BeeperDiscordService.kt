package com.dano.mirrormsg

/**
 * Discord via Beeper. v1 is "flat Discord": every Discord room Beeper exposes
 * (server channels AND DMs) is surfaced as one conversation under a single
 * "Discord" switcher entry — the guild id isn't exposed by the provider, so
 * there's no sub-grouping. All behavior lives in [BeeperServiceBase]; this class
 * only declares identity.
 *
 * Coexists with native Discord via distinct ids + a shared conflictGroup, matching
 * the other twins (fbmessenger/wabeeper/igbeeper/tgbeeper): Beeper id "dcbeeper",
 * shared conflictGroup "discord" for mutual exclusion — both bridge the same Discord
 * account and would fight if run together.
 * (Native Discord adds real server->channel grouping the provider can't give us
 * here; this flat Beeper path is the reach fallback.)
 *
 * Discovered by [ServiceRegistry] via its no-arg constructor. Deleting this one
 * file removes Discord cleanly (it then shows as Locked).
 */
class BeeperDiscordService : BeeperServiceBase() {
    override val id = "dcbeeper"
    override val displayName = "Discord"
    override val protocol = "discordgo"
    override val conflictGroup = "discord"   // mutual-exclusion with native Discord

    // Avatar circle colors (Discord blurple family). The shared avatarColorFor()
    // selector spreads these across threads; all under ~140 luma -> white initials.
    override val avatarPalette = listOf("#5865F2", "#4752C4", "#3C3F8F")
}
