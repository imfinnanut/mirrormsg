package com.dano.mirrormsg

import android.content.Context
import io.rebble.pebblekit2.common.model.PebbleDictionary
import io.rebble.pebblekit2.common.model.PebbleDictionaryItem
import kotlinx.coroutines.launch
import org.json.JSONObject
import java.io.File

/** A picture request as streamImage made it, kept so it can be made again. */
data class MediaRequest(
    val convoId: String, val msgIdx: Int, val mediaID: String,
    val keyB64: String, val inlineB64: String,
    val maxW: Int, val maxH: Int, val maxBytes: Int, val q: Int, val dispW: Int,
    val thumb: Boolean = false, val item: Int = -1
)

/** Media handling beyond still pictures. The defaults handle none of it. */
interface MediaEngine {
    val playsVoice: Boolean get() = false
    val playsVideo: Boolean get() = false

    /** A watch message this handles; true when it was one. */
    fun onWatchMessage(svc: PebbleListenerService, data: PebbleDictionary, reqHash: Int): Boolean {
        if (data[Schema.KEY_VID_REQ] != null) {
            svc.scope.launch {
                svc.sendImgFrame(Mirror.sender(svc),
                    mapOf(Schema.KEY_VID_START to PebbleDictionaryItem.Bytes(ByteArray(13))))
            }
            return true
        }
        return data[Schema.KEY_VID_STATUS] != null || data[Schema.KEY_VID_STOP] != null ||
            data[Schema.KEY_GIF_ABORT] != null
    }

    fun clipLoops(ctx: Context, id: String): Boolean = false

    /** The clip [bytes] encoded for the watch, or null for its first frame. */
    fun encodeClip(ctx: Context?, key: String, bytes: ByteArray, maxW: Int, maxH: Int, maxBytes: Int,
                   quality: Int, fpsFloor: Int, gifBytes: Int, startRung: Int, maxEdge: Int): String? = null

    fun voiceCard(ctx: Context, id: String, maxW: Int, full: Boolean, fetch: (File) -> Boolean): ByteArray? = null

    fun noteRequest(req: MediaRequest) {}

    /** Stream [data] for [req]; false leaves it to the still path. */
    suspend fun stream(svc: PebbleListenerService, req: MediaRequest, data: JSONObject,
                       heap: Int, msgId: String): Boolean = false

    fun zoomEnded(svc: PebbleListenerService) {}

    /** A zoomed view of [mm]; false leaves it to the still path. */
    suspend fun zoom(svc: PebbleListenerService, msvc: MessagingService, convoId: String, idx: Int,
                     mm: PebbleListenerService.MsgRow, seq: Int, x0: Int, y0: Int, s: Int,
                     pw: Int, ph: Int, vw: Int, vh: Int, room: Int, item: Int): Boolean = false
}

object Media {
    val engine: MediaEngine get() = MediaEngines.engine
}
