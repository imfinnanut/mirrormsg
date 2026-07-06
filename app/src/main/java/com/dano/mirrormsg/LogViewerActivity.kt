package com.dano.mirrormsg

import android.app.Activity
import android.content.ClipData
import android.content.ClipboardManager
import android.content.Context
import android.content.Intent
import android.os.Bundle
import android.text.method.ScrollingMovementMethod
import android.widget.Button
import android.widget.LinearLayout
import android.widget.ScrollView
import android.widget.TextView
import android.widget.Toast
import androidx.core.content.FileProvider

/**
 * Simple full-screen log viewer with Copy and Share actions.
 * Launched from MainActivity's "View Logs" button.
 */
class LogViewerActivity : Activity() {

    private lateinit var logText: TextView

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        LogStore.init(this)

        val root = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            // Top inset = real status-bar height + a small gap, so the log text
            // never tucks under the status bar (the fixed 48px wasn't enough on
            // tall-status-bar devices like the Fold).
            setPadding(24, statusBarHeight() + 24, 24, 24)
        }

        // Scrollable log text — takes all available space above the buttons.
        val scroll = ScrollView(this)
        logText = TextView(this).apply {
            textSize = 10f
            setTextIsSelectable(true)
            movementMethod = ScrollingMovementMethod()
            typeface = android.graphics.Typeface.MONOSPACE
        }
        scroll.addView(logText)
        // weight = 1 so the scroll view fills the screen, pushing buttons down.
        root.addView(scroll, LinearLayout.LayoutParams(
            LinearLayout.LayoutParams.MATCH_PARENT, 0, 1f))

        // Button row pinned at the bottom.
        val buttonRow = LinearLayout(this).apply {
            orientation = LinearLayout.HORIZONTAL
            setPadding(0, 16, 0, 0)
        }
        val copyBtn = Button(this).apply {
            text = "Copy All"
            setOnClickListener { copyAll() }
        }
        val shareBtn = Button(this).apply {
            text = "Share"
            setOnClickListener { shareLog() }
        }
        val refreshBtn = Button(this).apply {
            text = "Refresh"
            setOnClickListener { loadLog() }
        }
        val clearBtn = Button(this).apply {
            text = "Clear"
            setOnClickListener { LogStore.clear(); loadLog() }
        }
        // Equal-width buttons across the bottom.
        val bp = LinearLayout.LayoutParams(0,
            LinearLayout.LayoutParams.WRAP_CONTENT, 1f)
        buttonRow.addView(copyBtn, bp)
        buttonRow.addView(shareBtn, LinearLayout.LayoutParams(bp))
        buttonRow.addView(refreshBtn, LinearLayout.LayoutParams(bp))
        buttonRow.addView(clearBtn, LinearLayout.LayoutParams(bp))
        root.addView(buttonRow)

        setContentView(root)
        loadLog()
    }

    /** Status-bar height in px (so the top view can clear it), with a safe fallback. */
    private fun statusBarHeight(): Int {
        val id = resources.getIdentifier("status_bar_height", "dimen", "android")
        return if (id > 0) resources.getDimensionPixelSize(id)
               else (24 * resources.displayMetrics.density).toInt()
    }

    private fun loadLog() {
        val dump = LogStore.dump()
        logText.text = if (dump.isEmpty()) "(no logs yet)" else dump
    }

    private fun copyAll() {
        val cm = getSystemService(Context.CLIPBOARD_SERVICE) as ClipboardManager
        cm.setPrimaryClip(ClipData.newPlainText("MirrorMsg logs", LogStore.dump()))
        Toast.makeText(this, "Logs copied to clipboard", Toast.LENGTH_SHORT).show()
    }

    private fun shareLog() {
        try {
            val file = LogStore.fileForShare(this)
            val uri = FileProvider.getUriForFile(
                this, "$packageName.fileprovider", file)
            val intent = Intent(Intent.ACTION_SEND).apply {
                type = "text/plain"
                putExtra(Intent.EXTRA_STREAM, uri)
                putExtra(Intent.EXTRA_SUBJECT, "MirrorMsg logs")
                addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION)
            }
            startActivity(Intent.createChooser(intent, "Share logs"))
        } catch (e: Exception) {
            Toast.makeText(this, "Share failed: ${e.message}", Toast.LENGTH_LONG).show()
        }
    }

    override fun onResume() {
        super.onResume()
        loadLog()
    }
}
