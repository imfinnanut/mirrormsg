package com.dano.mirrormsg

/**
 * Instagram DMs via Beeper. Has a native twin (native Instagram, conflictGroup
 * "instagram"), so this shares that conflictGroup for mutual exclusion — both
 * bridge the same Meta account and would fight if run together. Distinct id
 * ("igbeeper") so the two coexist without double-delivery, same pattern as the
 * two Facebook/WhatsApp paths. Displays as "Instagram" (Beeper category header
 * disambiguates on the watch).
 *
 * `protocol` = "instagramgo" — CONFIRMED from a provider dump.
 *
 * Discovered by [ServiceRegistry] via its no-arg constructor.
 */
class BeeperInstagramService : BeeperServiceBase() {
    override val id = "igbeeper"
    override val displayName = "Instagram"
    override val protocol = "instagramgo"
    override val conflictGroup = "instagram"   // mutual-exclusion with native Instagram

    // Instagram gradient family (purple -> magenta -> pink). All under ~140 luma.
    override val avatarPalette = listOf("#833AB4", "#C13584", "#E1306C")
}
