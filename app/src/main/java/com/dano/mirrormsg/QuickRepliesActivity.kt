package com.dano.mirrormsg

import android.app.Activity
import android.content.res.Configuration
import android.graphics.Color
import android.graphics.drawable.GradientDrawable
import android.os.Bundle
import android.text.InputType
import android.view.Gravity
import android.view.ViewGroup
import android.widget.Button
import android.widget.EditText
import android.widget.LinearLayout
import android.widget.ScrollView
import android.widget.TextView
import kotlinx.coroutines.launch

/**
 * Editor for the canned "quick reply" messages. These sync to the watch and
 * appear in the enhanced view's reply menu. Mirrors the Pebble canned-message
 * editor: a list of editable rows with add/remove, plus Save.
 */
class QuickRepliesActivity : Activity() {

    private val rows = ArrayList<EditText>()
    private lateinit var rowsContainer: LinearLayout

    private fun dp(v: Int) = (v * resources.displayMetrics.density).toInt()
    private val accent = Color.parseColor("#7C4DFF")
    private fun night() = (resources.configuration.uiMode and
        Configuration.UI_MODE_NIGHT_MASK) == Configuration.UI_MODE_NIGHT_YES

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        Mirror.loadQuickReplies(this)

        val bg = if (night()) Color.parseColor("#0E0E10") else Color.parseColor("#F5F4F8")
        val card = if (night()) Color.parseColor("#1C1C1F") else Color.WHITE
        val border = if (night()) Color.parseColor("#2A2A2E") else Color.parseColor("#E2E0E8")
        val text = if (night()) Color.parseColor("#F2F2F5") else Color.parseColor("#1A1A1E")
        val sub = if (night()) Color.parseColor("#9A9AA2") else Color.parseColor("#6B6B73")

        val root = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setBackgroundColor(bg)
        }

        // Header band.
        val sb = resources.getIdentifier("status_bar_height", "dimen", "android")
            .let { if (it > 0) resources.getDimensionPixelSize(it) else dp(28) }
        root.addView(LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setBackgroundColor(accent)
            setPadding(dp(20), sb + dp(12), dp(20), dp(16))
            addView(TextView(this@QuickRepliesActivity).apply {
                this.text = "Quick replies"; textSize = 22f; setTextColor(Color.WHITE)
                setTypeface(typeface, android.graphics.Typeface.BOLD)
            })
        })

        val scroller = ScrollView(this).apply {
            layoutParams = LinearLayout.LayoutParams(
                ViewGroup.LayoutParams.MATCH_PARENT, 0, 1f)
        }
        val content = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setPadding(dp(16), dp(16), dp(16), dp(16))
        }
        content.addView(TextView(this).apply {
            this.text = "These appear in the reply menu on the watch. They sync next time the watch connects."
            textSize = 13f; setTextColor(sub); setPadding(dp(4), 0, dp(4), dp(12))
        })

        rowsContainer = LinearLayout(this).apply { orientation = LinearLayout.VERTICAL }
        content.addView(rowsContainer)

        for (r in Mirror.quickReplies) addRow(r, card, border, text)

        // Add reply button.
        content.addView(Button(this).apply {
            this.text = "+ Add reply"; isAllCaps = false
            setTextColor(Color.WHITE); setTextColor(accent)
            background = roundedDrawable(card, border, dp(10))
            setTextColor(text)
            setPadding(dp(16), dp(12), dp(16), dp(12))
            (layoutParams as? LinearLayout.LayoutParams) ?: run {
                layoutParams = LinearLayout.LayoutParams(
                    ViewGroup.LayoutParams.MATCH_PARENT,
                    ViewGroup.LayoutParams.WRAP_CONTENT).apply { topMargin = dp(8) }
            }
            setOnClickListener { addRow("", card, border, text) }
        })

        scroller.addView(content)
        root.addView(scroller)

        // Save bar.
        root.addView(Button(this).apply {
            this.text = "Save"; isAllCaps = false; textSize = 16f
            setTextColor(Color.WHITE)
            background = roundedDrawable(accent, accent, dp(12))
            setPadding(dp(16), dp(14), dp(16), dp(14))
            (layoutParams as? LinearLayout.LayoutParams)
            setOnClickListener { saveAndFinish() }
        }.also {
            val lp = LinearLayout.LayoutParams(
                ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.WRAP_CONTENT)
            lp.setMargins(dp(16), dp(8), dp(16), dp(16))
            it.layoutParams = lp
        })

        setContentView(root)
    }

    private fun roundedDrawable(fill: Int, stroke: Int, radius: Int) =
        GradientDrawable().apply {
            setColor(fill); cornerRadius = radius.toFloat()
            setStroke(dp(1), stroke)
        }

    private fun addRow(value: String, card: Int, border: Int, textColor: Int) {
        val row = LinearLayout(this).apply {
            orientation = LinearLayout.HORIZONTAL
            gravity = Gravity.CENTER_VERTICAL
            (layoutParams as? LinearLayout.LayoutParams)
        }
        val lp = LinearLayout.LayoutParams(
            ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.WRAP_CONTENT)
        lp.bottomMargin = dp(8)
        row.layoutParams = lp

        val edit = EditText(this).apply {
            setText(value)
            setTextColor(textColor)
            textSize = 16f
            inputType = InputType.TYPE_CLASS_TEXT or InputType.TYPE_TEXT_FLAG_CAP_SENTENCES
            background = roundedDrawable(card, border, dp(10))
            setPadding(dp(14), dp(14), dp(14), dp(14))
        }
        rows.add(edit)

        val del = Button(this).apply {
            text = "\u2715"; isAllCaps = false
            setTextColor(textColor)
            background = null
            setOnClickListener {
                rows.remove(edit)
                rowsContainer.removeView(row)
            }
        }
        row.addView(edit, LinearLayout.LayoutParams(0,
            ViewGroup.LayoutParams.WRAP_CONTENT, 1f))
        row.addView(del)
        rowsContainer.addView(row)
    }

    private fun saveAndFinish() {
        val list = rows.map { it.text.toString().trim() }.filter { it.isNotEmpty() }
        Mirror.setQuickReplies(this, list)
        // Push the updated list to the watch immediately.
        try {
            PebbleListenerService.instance?.scope?.launch {
                PebbleListenerService.instance?.refreshConversationList()
            }
        } catch (_: Exception) {}
        finish()
    }
}
