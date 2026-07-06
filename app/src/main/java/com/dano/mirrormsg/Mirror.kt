package com.dano.mirrormsg

import android.content.Context
import fi.mirrormsg.mirrorbridge.Bridge
import io.rebble.pebblekit2.client.DefaultPebbleSender
import io.rebble.pebblekit2.client.PebbleSender
import java.util.Collections
import java.util.UUID

object Mirror {
    @Volatile var bridge: Bridge? = null

    // Application context for storage access from EventSink / service.
    @Volatile var appContext: Context? = null

    val convoOrder: MutableList<String> = Collections.synchronizedList(mutableListOf())
    data class SimRoute(val participantID: String, val simTwo: Long, val simNumber: Long)
    val simRoutes: MutableList<SimRoute> = Collections.synchronizedList(mutableListOf())

    // Hierarchical navigation. The active account's current context path (empty =
    // the account's top level / root). For a hierarchical service (Discord: root ->
    // "dms"/"guild:<id>" -> chats) each drill pushes a token, BACK pops. Flat
    // services never populate it. navKinds/navContexts run PARALLEL to convoOrder:
    // navKinds[i] is 0 (OPEN, a real chat -> convoOrder[i]) or 1 (ENTER, a
    // bucket/server -> navContexts[i] is the token to push). Rebuilt every stream.
    val navStack: MutableList<String> = Collections.synchronizedList(mutableListOf())
    val navKinds: MutableList<Int> = Collections.synchronizedList(mutableListOf())
    val navContexts: MutableList<String> = Collections.synchronizedList(mutableListOf())

    // Per-conversation deep-link target (protocol + address), so "Open on phone"
    // / banner tap can jump into the native app on the right thread. Keyed by
    // conversationID. Protocol-agnostic (see DeepLink.kt).
    val convoTargets: MutableMap<String, ConvoTarget> = Collections.synchronizedMap(HashMap())

    @Volatile var currentOpenConvoId: String? = null
    @Volatile var currentOpenRoute: SimRoute? = null
    // Which message page the open thread is currently showing (0 = newest). The
    // watch stays ignorant of paging and always addresses rows 0..MSG_PAGE_SIZE-1;
    // this is the offset the phone adds (page*MSG_PAGE_SIZE) when resolving a
    // row-indexed request. Reset to 0 on every fresh open / live new message.
    @Volatile var currentMsgPage: Int = 0

    // ---- Settings -----------------------------------------------------------
    @Volatile var nightMode: Boolean = false
    // When on, outbound text is run through EmojiSub: emoji the Pebble font can't
    // draw are swapped for a close supported glyph (unknown ones pass through).
    // Mirrored into EmojiSub.enabled so the filter can gate itself statically.
    @Volatile var emojiSubstitution: Boolean = false
    // The logged-in account's own phone number (from simCard.SIMData), shown as
    // the account subtitle. Empty until the first conversation list is parsed.
    @Volatile var accountNumber: String = ""
    // The id of the currently-active messaging service (which network's
    // conversations the watch is showing). Persisted so a switch survives app
    // restarts — otherwise the app reverts to google on relaunch while the watch
    // still shows the previously-selected network's cached list.
    @Volatile private var _activeServiceId: String = "google"
    var activeServiceId: String
        get() = _activeServiceId
        set(value) {
            _activeServiceId = value
            appContext?.getSharedPreferences("mirrormsg", Context.MODE_PRIVATE)
                ?.edit()?.putString("activeServiceId", value)?.apply()
        }

    // Active Telegram sub-account (chat folder). 0 = main Telegram; >0 = a folder
    // id, filtering the Telegram conversation list to that folder's chats. Set
    // alongside activeServiceId by the watch account switch; persisted so a relaunch
    // restores the selected sub-account.
    @Volatile private var _activeTgFolder: Int = 0
    var activeTgFolder: Int
        get() = _activeTgFolder
        set(value) {
            _activeTgFolder = value
            appContext?.getSharedPreferences("mirrormsg", Context.MODE_PRIVATE)
                ?.edit()?.putInt("activeTgFolder", value)?.apply()
        }

    // Active Discord server sub-account (guild). "" = base Discord (DMs only); a
    // non-empty guild id filters the Discord conversation list to that server's
    // channels. Parallels activeTgFolder — Discord servers ride the same switcher
    // rails as Telegram folders, but keyed by snowflake string (too big for Int).
    @Volatile private var _activeDiscordGuild: String = ""
    var activeDiscordGuild: String
        get() = _activeDiscordGuild
        set(value) {
            _activeDiscordGuild = value
            appContext?.getSharedPreferences("mirrormsg", Context.MODE_PRIVATE)
                ?.edit()?.putString("activeDiscordGuild", value)?.apply()
        }

    /**
     * The currently-active messaging service, resolved through the registry.
     * This is the multiplexer entry point: all watch-bound data calls
     * (list/fetch/send/image) go through here, so switching accounts is just a
     * matter of flipping activeServiceId — the watch pipeline is unchanged.
     */
    fun activeService(): MessagingService? =
        appContext?.let { ServiceRegistry.byId(it, activeServiceId) }

    // ---- Watch account switcher entries ----------------------------------
    // The rail the watch shows = every logged-in service, plus (right after
    // Telegram) each ENABLED chat folder as an indented sub-account. Centralized
    // here so both rail emitters (PebbleListenerService.reportServiceList and
    // MainActivity.pushServiceListToWatch) and the watch->phone selection handler
    // agree on the exact labels + index order.
    // folderId is a Telegram chat-folder id (0 = none). guildId is a Discord
    // server (guild) snowflake ("" = none / base DMs) — separate because Discord
    // ids exceed Int32. A given entry uses at most one of the two.
    data class SwitcherEntry(
        val label: String,
        val serviceId: String,
        val folderId: Int,
        val guildId: String = "",
    )

    // The entries behind the rail last sent to the watch, so KEY_REQ_SERVICE (an
    // index) resolves to the right service/folder. Set by whichever emitter last
    // pushed the rail.
    @Volatile var lastSwitcherEntries: List<SwitcherEntry> = emptyList()

    fun switcherEntries(ctx: Context): List<SwitcherEntry> {
        val entries = ArrayList<SwitcherEntry>()
        for (svc in ServiceRegistry.loggedIn(ctx)) {
            // Rail labels are shortened for the narrow watch switcher, independent of
            // svc.displayName (which the Network tab + notifications keep in full).
            val railLabel = when (svc.id) {
                "google"   -> "Google Msgs"
                "facebook" -> "Facebook Msgr"
                else       -> svc.displayName
            }
            entries.add(SwitcherEntry(railLabel, svc.id, 0))
            // Telegram folders and Discord servers no longer explode into the rail —
            // both live in the paginated nav-context list (root: "Direct Messages" +
            // folders/servers). Their show/hide filters (tgsub_enabled_* /
            // dcserver_enabled_*) now gate that root list instead of the rail, so each
            // hierarchical account is a single rail service.
        }
        return entries
    }

    // Beeper grouping bitmask over the entries (bit i set if entry i is a
    // Beeper-backed real account; folder sub-accounts are native Telegram, so 0).
    fun switcherFlags(ctx: Context, entries: List<SwitcherEntry>): Int {
        var flags = 0
        entries.forEachIndexed { i, e ->
            if (i < 32 && e.folderId == 0 && ServiceRegistry.byId(ctx, e.serviceId) is BeeperServiceBase) {
                flags = flags or (1 shl i)
            }
        }
        return flags
    }

    // How incoming messages alert/open on the watch:
    //   1 = Notification (post notification, user taps "Open on watch")
    //   2 = Force open    (immediately force-open the watchapp to the thread)
    //   3 = Hybrid        (text -> mode 1; message with image -> mode 2)
    const val MODE_NOTIFY = 1
    const val MODE_FORCE  = 2
    const val MODE_HYBRID = 3
    @Volatile var notifyMode: Int = MODE_NOTIFY

    // How many chat-view thumbnails the watch caches (0-8). Default 4.
    @Volatile var thumbLimit: Int = 4

    // Image memory reserve (bytes): the minimum free heap the watch keeps when
    // allocating an image bitmap. Before each image alloc it dumps its convo/
    // thumbnail caches if needed to stay above this, and shows a placeholder
    // rather than crashing if the image still won't fit. (Replaces the old
    // entry-time cache-dump check, which couldn't catch a mid-session image
    // load driving the heap to near-zero.) The watch hard-clamps the effective
    // floor to a 600-byte minimum, so 0 just means minimal protection.
    // Wire key/type unchanged (KEY_HEAP_GUARD, int bytes). Default 800 B.
    @Volatile var heapGuard: Int = 800

    fun loadSettings(ctx: Context) {
        val prefs = ctx.getSharedPreferences("mirrormsg", Context.MODE_PRIVATE)
        nightMode = prefs.getBoolean("nightMode", false)
        emojiSubstitution = prefs.getBoolean("emojiSubstitution", false)
        EmojiSub.enabled = emojiSubstitution
        notifyMode = prefs.getInt("notifyMode", MODE_NOTIFY)
        thumbLimit = prefs.getInt("thumbLimit", 4).coerceIn(0, 8)
        heapGuard = prefs.getInt("heapGuard", 800)
        accountNumber = prefs.getString("accountNumber", "") ?: ""
        _activeServiceId = prefs.getString("activeServiceId", "google") ?: "google"
        _activeTgFolder = prefs.getInt("activeTgFolder", 0)
        _activeDiscordGuild = prefs.getString("activeDiscordGuild", "") ?: ""
        loadQuickReplies(ctx)
    }

    fun setThumbLimit(ctx: Context, n: Int) {
        thumbLimit = n.coerceIn(0, 8)
        ctx.getSharedPreferences("mirrormsg", Context.MODE_PRIVATE)
            .edit().putInt("thumbLimit", thumbLimit).apply()
    }

    fun setHeapGuard(ctx: Context, bytes: Int) {
        heapGuard = bytes.coerceIn(0, 6000)
        ctx.getSharedPreferences("mirrormsg", Context.MODE_PRIVATE)
            .edit().putInt("heapGuard", heapGuard).apply()
    }

    fun setNotifyMode(ctx: Context, mode: Int) {
        notifyMode = mode
        ctx.getSharedPreferences("mirrormsg", Context.MODE_PRIVATE)
            .edit().putInt("notifyMode", mode).apply()
    }

    // Quick replies (canned messages). Defaults mirror Pebble's stock set.
    val DEFAULT_QUICK_REPLIES = listOf("Ok", "Yes", "No", "Call me", "Call you later")
    @Volatile var quickReplies: List<String> = DEFAULT_QUICK_REPLIES

    fun loadQuickReplies(ctx: Context) {
        val raw = ctx.getSharedPreferences("mirrormsg", Context.MODE_PRIVATE)
            .getString("quickReplies", null)
        quickReplies = if (raw.isNullOrEmpty()) DEFAULT_QUICK_REPLIES
                       else raw.split("\u001e").filter { it.isNotBlank() }
    }

    fun setQuickReplies(ctx: Context, list: List<String>) {
        val clean = list.map { it.trim() }.filter { it.isNotEmpty() }.take(12)
        quickReplies = if (clean.isEmpty()) DEFAULT_QUICK_REPLIES else clean
        ctx.getSharedPreferences("mirrormsg", Context.MODE_PRIVATE)
            .edit().putString("quickReplies", quickReplies.joinToString("\u001e")).apply()
    }

    fun setNightMode(ctx: Context, enabled: Boolean) {
        nightMode = enabled
        ctx.getSharedPreferences("mirrormsg", Context.MODE_PRIVATE)
            .edit().putBoolean("nightMode", enabled).apply()
    }

    fun setEmojiSubstitution(ctx: Context, enabled: Boolean) {
        emojiSubstitution = enabled
        EmojiSub.enabled = enabled
        ctx.getSharedPreferences("mirrormsg", Context.MODE_PRIVATE)
            .edit().putBoolean("emojiSubstitution", enabled).apply()
    }

    fun setAccountNumber(ctx: Context, number: String) {
        accountNumber = number
        ctx.getSharedPreferences("mirrormsg", Context.MODE_PRIVATE)
            .edit().putString("accountNumber", number).apply()
    }

    // ---- Conversation list (persistent, no expiry) -------------------------
    // The list is cached to disk and never purged on a timer. It's refreshed
    // only when the bridge reports new activity (MESSAGE/CONVERSATION events)
    // or when an explicit fetch returns a different list.
    @Volatile private var convoListMem: String? = null

    private fun convoListFile(ctx: Context) =
        java.io.File(ctx.filesDir, "convo_list.json")

    /** Returns the cached conversation list (memory, then disk). Never expires. */
    fun getCachedConvoJson(): String? {
        convoListMem?.let { return it }
        val ctx = appContext ?: return null
        val f = convoListFile(ctx)
        if (!f.exists()) return null
        return try {
            val s = f.readText()
            convoListMem = s
            s
        } catch (e: Exception) { null }
    }

    /** Store the conversation list to memory and disk. */
    fun putCachedConvoJson(json: String) {
        convoListMem = json
        val ctx = appContext ?: return
        try { convoListFile(ctx).writeText(json) } catch (_: Exception) {}
    }

    /** Force the next list request to fetch fresh (call on new-activity events). */
    fun invalidateConvoList() { convoListMem = null }

    private var _sender: PebbleSender? = null
    fun sender(ctx: Context): PebbleSender =
        _sender ?: DefaultPebbleSender(ctx.applicationContext).also { _sender = it }
    fun closeSender() {
        try { _sender?.close() } catch (_: Exception) {}
        _sender = null
    }
}

object Schema {
    val KEY_OP: UInt         = 0u
    val KEY_IDX: UInt        = 1u
    val KEY_COUNT: UInt      = 2u
    val KEY_NAME: UInt       = 3u
    val KEY_SNIPPET: UInt    = 4u
    val KEY_COLOR: UInt      = 5u
    val KEY_TYPE: UInt       = 6u
    val KEY_UNREAD: UInt     = 7u
    val KEY_MSG_TEXT: UInt   = 8u
    val KEY_MSG_IN: UInt     = 9u
    val KEY_MSG_SENDER: UInt = 10u
    val KEY_REQ_OPEN: UInt    = 20u
    val KEY_REPLY_TEXT: UInt  = 21u
    // 22 (KEY_HAVE_VERSION), 23 (KEY_NO_CHANGE), 25 (KEY_THREAD_VER) RETIRED —
    // the watch dropped its thread-text cache, so there's no version handshake;
    // the full blob is always sent. Numbers reserved, do not reuse.
    val KEY_THREAD_BLOB: UInt = 24u   // phone->watch: entire thread text packed into one string
    val KEY_IMG_INDICES: UInt = 26u   // phone->watch: comma-list of message indices that are images
    val KEY_NIGHT_MODE: UInt  = 27u   // phone->watch: 1=dark theme, 0=light
    val KEY_QUICK_REPLIES: UInt = 28u // phone->watch: canned replies, \x1e-separated
    val KEY_OPEN_VIEW: UInt   = 29u   // phone->watch (OP_MSG): 0=chat view, 1=enhanced view
    val KEY_REQ_IMAGE: UInt   = 40u   // watch->phone: stream the image for this msg index
    val KEY_REQ_IMG_FULL: UInt = 41u  // watch->phone: 0=small (chat preview), 1=full (enhanced)
    val KEY_REQ_IMG_BUDGET: UInt = 42u // watch->phone: free-heap byte budget for a full image
    val KEY_DIAG: UInt = 43u           // watch->phone: diagnostic text string (logged on phone)
    val KEY_REQ_IMG_BUDGET_MAX: UInt = 44u // watch->phone: budget if it frees its convo cache
    val KEY_IMG_WIPE_CACHE: UInt = 45u // phone->watch (IMG_START): 1 = free cache before building
    // 46 (KEY_CACHE_LIMIT) RETIRED — watch no longer caches thread text. Reserved, do not reuse.
    val KEY_REQ_IMG_QUALITY: UInt = 47u // watch->phone: 0=full color (8bpp), 1=16-color (4bpp)
    val KEY_IMG_FORMAT: UInt = 48u     // phone->watch (IMG_START): 0=8bpp, 1=4bpp palette
    val KEY_IMG_PALETTE: UInt = 49u    // phone->watch (IMG_START): palette bytes (GColor8) for 4bpp
    val KEY_THUMB_LIMIT: UInt = 50u    // phone->watch (convo frame): how many thumbnails to cache (0-6)
    val KEY_REQ_IMG_PRIORITY: UInt = 51u  // watch->phone: abort other image work, stream this now
    val KEY_REQ_FULL_TEXT: UInt = 52u     // watch->phone: send full untruncated text for this msg index
    val KEY_FULL_TEXT: UInt     = 53u     // phone->watch: the full text payload
    val KEY_FULL_TEXT_IDX: UInt = 54u     // phone->watch: which msg index the full text is for
    val KEY_SERVICES: UInt      = 55u     // phone->watch: \x1e-separated service labels
    val KEY_ACTIVE_SERVICE: UInt = 56u    // phone->watch: index of the active service
    val KEY_HEAP_GUARD: UInt = 57u        // phone->watch (convo frame): low-heap byte floor (0=off)
    val KEY_REQ_SERVICE: UInt   = 57u     // watch->phone: switch to service index N
    val KEY_PN_ROW: UInt = 58u            // phone->watch (OP_MSG force-open): row index of the PN's
                                          // convo in the just-pushed list, so the watch sets s_open_row
                                          // to match (reply routing fallback + thread-cache key correctness)
    val KEY_SERVICE_FLAGS: UInt = 59u     // phone->watch: int32 bitmask sent alongside KEY_SERVICES in
                                          // the service-report frame. bit i set => the i-th service in
                                          // the KEY_SERVICES list is a Beeper-backed account. Watch
                                          // groups natives first, then a "Beeper" divider, then the
                                          // flagged ones. Absent => watch treats every service as
                                          // native and draws no divider (additive; old builds unaffected).

    val KEY_REPLY_TO_INDEX: UInt = 60u // watch->phone: when replying from the DETAIL
                                       // view, the target message's index in the open
                                       // thread (the reply-to / react target). Sent
                                       // alongside KEY_REPLY_TEXT. Absent or -1 => a
                                       // normal chat-view reply (plain message to the
                                       // conversation). The phone routes by the owning
                                       // service's opt-in capability; unsupported => it
                                       // falls back to a plain message (additive — old
                                       // watch builds that never send this are unaffected).
    val KEY_ACTIVE_CAN_REACT: UInt = 61u // phone->watch: 1 if the currently active
                                       // service supports reaction-send, else 0. Lets
                                       // the watch predict whether a single-emoji send
                                       // from the detail view will become a reaction
                                       // (stay scrolled) vs a message (jump to bottom).
                                       // Absent => watch defaults to 0 (message).
    val KEY_THREAD_TOKEN: UInt = 62u   // phone->watch (thread + image frames): a stable
                                       // uint32 hash of the convoId. The watch keys its
                                       // thumbnail cache by (token, row) so same-index
                                       // images in different chats don't collide. Absent
                                       // => watch uses token 0 (index-only, legacy).

    val KEY_REACTION_ONLY: UInt = 63u  // phone->watch (OP_MSG): 1 => this thread re-push
                                       // is a reaction/badge-only update; the watch keeps
                                       // the current scroll position (s_react_keep_pos)
                                       // instead of snapping to the newest row. Absent/0
                                       // => normal push, jumps to newest as before.

    // ---- Convo-list pagination (watch pages the full list, CONVO_PAGE_SIZE per
    // page, one page in watch memory). Must match main.c's KEY_* values. ----
    val KEY_CONVO_MORE: UInt      = 64u // phone->watch (OP_CONVO): 1 if conversations
                                        // exist beyond the page being sent (drives the
                                        // watch's "next page" row), else 0/absent.
    val KEY_REQ_CONVO_PAGE: UInt  = 65u // watch->phone: the 0-based page the watch wants;
                                        // phone re-streams that page into indices 0..N-1.
    val KEY_CONVO_PAGE: UInt      = 66u // phone->watch (OP_CONVO): which 0-based page this
                                        // push represents. Sent on EVERY convo push; the
                                        // watch treats it as authoritative and syncs its
                                        // page, so a page-0 refresh snaps a paged-forward
                                        // watch back and KEY_REQ_OPEN offsets stay correct.

    // ---- Hierarchical navigation (nav-context lists: DMs bucket / servers /
    // channels for Discord, and later Telegram folders). The watch is depth-agnostic:
    // each entry is OPEN or ENTER, and BACK climbs when the list has a parent. Must
    // match main.c's KEY_* values. ----
    val KEY_REQ_ENTER: UInt       = 67u // watch->phone: drill into the current list's
                                        // absolute entry n (same page math as
                                        // KEY_REQ_OPEN); -1 => pop up one level. Either
                                        // way the phone re-streams the new context page 0.
    val KEY_LIST_HAS_PARENT: UInt = 68u // phone->watch (OP_CONVO): 1 if the list being
                                        // sent is NOT an account's top level, so the
                                        // watch's BACK climbs a level instead of leaving.
    val KEY_ENTRY_KIND: UInt      = 69u // phone->watch (OP_CONVO, per entry): 0/absent =
                                        // OPEN (a real chat/channel -> KEY_REQ_OPEN),
                                        // 1 = ENTER (a bucket/server/folder -> the watch
                                        // sends KEY_REQ_ENTER to drill in).

    val KEY_THREAD_LABEL: UInt    = 70u // phone->watch (OP_MSG): detail-view banner label.
                                        // "<network>", or "<network>\n<server>#<channel>"
                                        // for drillable accounts (Discord channels). The
                                        // watch draws two lines iff a '\n' is present.

    // ---- Message-view (thread) pagination. The watch holds one page (MSG_PAGE_SIZE)
    // as plain rows 0..N-1 and stays ignorant of paging; the PHONE owns the offset
    // (page*MSG_PAGE_SIZE) when resolving any row-indexed request (reply/react target,
    // image, full-text). Page 0 = newest page; higher pages are older. Mirrors the
    // convo-list pagination (KEY_REQ_CONVO_PAGE / KEY_CONVO_PAGE / KEY_CONVO_MORE).
    val KEY_REQ_MSG_PAGE: UInt    = 71u // watch->phone: the 0-based message page to
                                        // (re)stream for the OPEN thread (up = older).
    val KEY_MSG_PAGE: UInt        = 72u // phone->watch (OP_MSG): which page this push is;
                                        // the watch syncs its page authoritatively from it.
    val KEY_MSG_MORE_OLDER: UInt  = 73u // phone->watch (OP_MSG): 1 if older messages exist
                                        // before this page (drives the watch's up-arrow).

    val KEY_IMG_START: UInt  = 30u
    val KEY_IMG_SEQ: UInt    = 31u
    val KEY_IMG_DATA: UInt   = 32u
    val KEY_IMG_END: UInt    = 33u

    const val OP_CONVO  = 1
    const val OP_MSG    = 2
    const val OP_IMG    = 3

    // Conversations sent per watch page. MUST match main.c's MAX_CONVOS (the watch
    // holds exactly one page in memory). KEY_IDX/KEY_COUNT on a convo push are
    // page-relative (0..CONVO_PAGE_SIZE-1); KEY_REQ_OPEN from the watch is absolute
    // (page*CONVO_PAGE_SIZE + row).
    const val CONVO_PAGE_SIZE = 10

    // Messages per thread page — the watch holds exactly one page and addresses it
    // as rows 0..MSG_PAGE_SIZE-1; the phone adds page*MSG_PAGE_SIZE to reach the real
    // message. Single source of truth: a future "messages per page" setting flips
    // this one value (watch + phone must agree on the active size). MUST match the
    // watch's per-page cap.
    const val MSG_PAGE_SIZE = 20

    // Open-view targets (KEY_OPEN_VIEW).
    const val VIEW_CHAT     = 0   // land on the message thread (chat view)
    const val VIEW_ENHANCED = 1   // land on the enhanced single-message view

    val WATCH_UUID: UUID = UUID.fromString("4fe41bb2-f27f-41d0-9bf0-e3b8e206193f")

    const val IMG_MAX_W = 144          // full size (enhanced view) - edge to edge
    const val IMG_MAX_H = 111
    const val IMG_SMALL_W = 80         // chat-view preview size
    const val IMG_SMALL_H = 74
    const val IMG_CHUNK_SIZE = 380     // bigger chunks = fewer round-trips
    const val IMG_BUDGET_FLOOR = 8000  // min bytes for a full image if watch reports too little
}
