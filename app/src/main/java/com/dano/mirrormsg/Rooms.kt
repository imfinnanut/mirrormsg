package com.dano.mirrormsg

import org.json.JSONArray
import org.json.JSONObject
import java.util.concurrent.ConcurrentHashMap

/**
 * A network whose feed posts have replies (X, Instagram, Reddit) implements this on its
 * MessagingService, alongside setting "replyCount" on the posts it returns (step 99).
 */
interface ReplySource {
    /**
     * One page of the replies to post [messageId] of feed chat [convoId], in the order
     * the network shows them, as {"messages":[...], "cursor":"..."}: each reply a message
     * in the shape fetchMessages returns ("replyCount" set when it has replies of its own,
     * "likeCount" its likes), "cursor" the next page's ("" or absent: no more). [cursor]
     * "" asks for the first page. Blocking; called off the main thread.
     */
    fun fetchReplies(convoId: String, messageId: String, cursor: String): String
}

/**
 * A feed network's own actions on a post or a reply (step 100): a like, a vote up or
 * down. The watch shows them at the top of its reply menu, in the feed and its rooms;
 * one picked goes at once to the message selected there (or open in the detail view).
 *
 * A network whose feed takes likes needs none of this: in a feed of a service that
 * supportsReactionSend, the menu has Like, sent as sendReaction(feed, post, fromMe, "❤")
 * (step 101: a heart), and pressed again on a post the user likes, an unlike through
 * ReactionRemover.removeReaction (where the service has it). Implement this for the votes
 * (Reddit's).
 */
interface FeedActions {
    /**
     * The actions on feed chat [convoId]'s posts, in the menu's order: at most 3 of
     * LIKE, UPVOTE, DOWNVOTE (empty: none). Asked for on every frame the watch gets of
     * the feed: a list kept, not a lookup.
     */
    fun feedActions(convoId: String): List<Int>

    /**
     * Vote [action] (UPVOTE, DOWNVOTE; a LIKE never comes here) on message [messageId]
     * (a post or a reply, its "messageID") of feed chat [convoId]: the same vote again
     * takes it back (Dan, step 101: "a second one unsends"), the other one changes it.
     * {"messageId":"…"} when done, {"error":"…"} when not. Blocking; called off the main
     * thread.
     */
    fun sendFeedAction(convoId: String, messageId: String, targetFromMe: Boolean, action: Int): String

    companion object {
        // The watch's ACT_* (mm_shared.h): the same numbers.
        const val LIKE = 1
        const val UPVOTE = 2
        const val DOWNVOTE = 3
    }
}

/**
 * Reply rooms (step 99): a feed's post and its replies, which the watch shows as a feed
 * of their own, the post at the top and the replies below it in the network's order.
 * Selecting a post with replies opens its room; Back goes back up the chain, to the post
 * it came from. Rooms live in memory, for as long as the chain is open on the watch:
 * anything else opening there (Mirror.currentOpenConvoId) ends it.
 *
 * A room has a convoId of its own (never a network's), so the open-thread machinery
 * (spans, pictures, full text, replies) serves it as it serves a chat; [netConvo] gives
 * the network's chat, for calls into the network.
 */
object Rooms {
    private const val PREFIX = "\u0001room:"

    class Room(
        val id: String,          // its own convoId
        val feed: String,        // the network's chat it's in
        val postId: String,      // the post at its top
        val below: String,       // what was open under it: the feed, or the room before
        val anchor: Int,         // the post's row there when it was opened
        parent: JSONObject
    ) {
        private val top: JSONObject = JSONObject(parent.toString()).put("roomTop", true)
        private val replies = ArrayList<JSONObject>()
        private val held = HashSet<String>()
        /** Changes with what it holds (a parse made before is stale). */
        @Volatile var version: Long = System.nanoTime()
            private set

        /**
         * As a stored thread's JSON: "ordered" (its order is the network's, not by time),
         * oldest-first like a store's, so the watch's numbering (newest first) puts the
         * post at the top and the replies under it.
         */
        @Synchronized fun json(): String {
            val arr = JSONArray()
            for (k in replies.indices.reversed()) arr.put(replies[k])
            arr.put(top)
            return JSONObject().put("ordered", true).put("messages", arr).toString()
        }

        /** The next page of replies, under those held (any already held left out): how many it added. */
        @Synchronized fun append(page: JSONArray): Int {
            var n = 0
            for (i in 0 until page.length()) {
                val m = page.optJSONObject(i) ?: continue
                val mid = m.optString("messageID")
                if (mid.isNotEmpty() && !held.add(mid)) continue
                replies.add(m)
                n++
            }
            if (n > 0) version = System.nanoTime()
            return n
        }

        /** A reply just sent to the post: first under it. */
        @Synchronized fun sent(m: JSONObject) {
            val mid = m.optString("messageID")
            if (mid.isNotEmpty() && !held.add(mid)) return
            replies.add(0, m)
            version = System.nanoTime()
        }

        /** The post or a reply here, by its id. */
        @Synchronized fun message(mid: String): JSONObject? =
            if (top.optString("messageID") == mid) top
            else replies.firstOrNull { it.optString("messageID") == mid }

        /** Message [mid] here liked by the user, or no longer (from the watch, step 101). */
        @Synchronized fun setLiked(mid: String, liked: Boolean) {
            val m = message(mid) ?: return
            if (applyLiked(m, liked)) version = System.nanoTime()
        }
    }

    private val rooms = ConcurrentHashMap<String, Room>()
    private val chain = ArrayList<String>()
    private var opened = 0L   // (every room's id its own: a fetch still running for one left can't reach the next)

    fun isRoom(id: String?): Boolean = id != null && id.startsWith(PREFIX)
    fun get(id: String?): Room? = if (id == null) null else rooms[id]
    /** How many rooms deep the watch is (0: the feed). */
    fun depth(): Int = synchronized(chain) { chain.size }
    /** The network's chat for [id] (a room's feed; anything else is its own). */
    fun netConvo(id: String): String = rooms[id]?.feed ?: id

    /** A room for post [postId] of [feed], opened from [below] (where the post is row [anchor]). */
    fun open(feed: String, postId: String, below: String, anchor: Int, parent: JSONObject): Room =
        synchronized(chain) {
            val id = PREFIX + (++opened) + ":" + feed + "/" + postId
            val r = Room(id, feed, postId, below, anchor, parent)
            rooms[id] = r
            chain.add(id)
            r
        }

    /** Message [mid] liked by the user, or no longer, in every room open that holds it (step 101). */
    fun setLiked(mid: String, liked: Boolean) {
        for (r in rooms.values) r.setLiked(mid, liked)
    }

    /**
     * Message JSON [m] just liked by the user ([liked]), or unliked: "likeCount" one more or
     * one less, and "liked" with it where the network sends one (not where it already says
     * so). Whether it changed.
     */
    fun applyLiked(m: JSONObject, liked: Boolean): Boolean {
        if (m.has("liked")) {
            if (m.optBoolean("liked") == liked) return false
            m.put("liked", liked)
        }
        m.put("likeCount", maxOf(0L, m.optLong("likeCount", 0L) + if (liked) 1L else -1L))
        return true
    }

    /** Back out of the deepest room: the room left (null: none open). */
    fun pop(): Room? = synchronized(chain) {
        if (chain.isEmpty()) null else rooms.remove(chain.removeAt(chain.size - 1))?.also { forget(it.id) }
    }

    /** The chain ends (the feed, another chat, or nothing open on the watch). */
    fun leave() = synchronized(chain) {
        if (chain.isEmpty()) return@synchronized
        for (id in chain) forget(id)
        chain.clear()
        rooms.clear()
    }

    private fun forget(id: String) {
        Mirror.backfillCursor.remove(id)
        Mirror.backfillExhausted.remove(id)
        Mirror.backfillQuietUntil.remove(id)
        Mirror.backfillCooldownUntil.remove(id)
    }
}
