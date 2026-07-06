package com.dano.mirrormsg

/**
 * Telegram via Beeper. Coexists with native TelegramService via distinct ids +
 * a shared conflictGroup, matching the other twins (fbmessenger/wabeeper/igbeeper):
 * Beeper id "tgbeeper", native id "tgnative", both conflictGroup "telegram" so
 * mutual exclusion stops both running against one Telegram account. Displays as
 * "Telegram" (Beeper category header disambiguates on the watch).
 *
 * Beeper surfaces Telegram's DMs/groups/channels in one flat list, so it's a plain
 * single-account network (no channels-mode). All behavior lives in
 * [BeeperServiceBase]; this class only declares identity + palette.
 *
 * `protocol` = "telegram" — best guess (plain name, like "signal"/"whatsapp";
 * the "go" suffix only appears on "discordgo"/"facebookgo"). UNCONFIRMED: Telegram
 * wasn't connected in the last provider dump. BeeperServiceBase logs "provider
 * protocols seen = …" on launch (now regardless of enable state); once Telegram
 * is linked, read that line and correct this one value if it differs. Until then
 * the presence gate keeps Telegram hidden, so a wrong guess is harmless.
 *
 * Discovered by [ServiceRegistry] via its no-arg constructor.
 */
class BeeperTelegramService : BeeperServiceBase() {
    override val id = "tgbeeper"
    override val displayName = "Telegram"
    override val protocol = "telegram"
    override val conflictGroup = "telegram"   // mutual-exclusion with native TelegramService

    // Avatar circle colors (Telegram blue family). All under ~140 luma -> white
    // initials.
    override val avatarPalette = listOf("#229ED9", "#0088CC", "#1C7CB0")
}
