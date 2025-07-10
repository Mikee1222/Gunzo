
# === Notification Target ===
GROUP_CHAT_ID = -1002123456789  # ID της ομαδικής συνομιλίας για ειδοποιήσεις live

# === Greek day names constant ===
DAYS = ["Δευτέρα", "Τρίτη", "Τετάρτη", "Πέμπτη", "Παρασκευή", "Σάββατο", "Κυριακή"]

# === Predefined user ID mappings ===
KNOWN_USERS = {
    "tsaqiris": 1673725703,
    "mikekrp": 6431210056,
    "evi_nikolaidou": 6700819251,
    "kostasmavridis": 1234567890,
    "maraggos": 2099171835,
}

# === Chatter name → Telegram handle mappings ===
CHATTER_HANDLES = {
    "Αναστάσης": "@Anastasiss12",
    "Ηλίας": "@elias_drag",
    "Καραπάντσος": "@mikekrp",
    "Κούζου": "@Kouzounias",
    "Μακρο": None,        # TODO: συμπλήρωσε handle
    "Μαραγγός": "@Maraggos",
    "Νίκος": "@nikospapadop",   
    "Πετρίδης": "@Bull056",
    "Riggers": "@riggersss",
    "Βασίλης": "@basileiou",
}

import logging
import re
import asyncio
import sqlite3
import os
import sys
from datetime import datetime, timedelta, time
import pytz
from telegram import (
    InlineKeyboardButton, InlineKeyboardMarkup,
    Update, MessageEntity, error as tg_error
)

# If handle_makeprogram_day is in another module, import it:
# from your_module import handle_makeprogram_day
from telegram.helpers import escape_markdown


from telegram.ext import (
    Application, CallbackQueryHandler,
    CommandHandler, ContextTypes, MessageHandler, filters
)
import requests
import csv
import io
import nest_asyncio
nest_asyncio.apply()
from telegram.error import RetryAfter

from telegram.error import RetryAfter

async def safe_send(bot, chat_id, text, **kwargs):
    try:
        return await bot.send_message(chat_id=chat_id, text=text, **kwargs)
    except RetryAfter as e:
        import asyncio
        await asyncio.sleep(e.retry_after + 1)
        return await bot.send_message(chat_id=chat_id, text=text, **kwargs)

async def safe_edit(query, text, **kwargs):
    try:
        return await query.message.edit_text(text, **kwargs)
    except Exception as e:
        logging.error(f"Failed to edit message: {e}")

# === COMMANDS HANDLERS ===

# --- /getid command handler ---
async def get_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await update.message.reply_text(f"🆔 Το Telegram ID σου είναι: `{user.id}`", parse_mode="Markdown")

# --- /register command handler ---
async def handle_register(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    context.application.bot_data['notify_chat_id'] = chat_id
    await update.message.reply_text(f"✅ Αυτή η chat ({chat_id}) καταγράφηκε για ειδοποιήσεις startup/shutdown.")


# --- /start confirmation for Evi ---
async def handle_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Preliminary confirmation menu for Evi
    keyboard = [
        [InlineKeyboardButton("Μακαρονια με κιμα", callback_data="evi_pasta")],
        [InlineKeyboardButton("Γαριδες σαγανακι", callback_data="evi_shrimp")],
        [InlineKeyboardButton("Σντιστελ", callback_data="evi_schnitzel")],
        [InlineKeyboardButton("Πατατες", callback_data="evi_fries")],
        [InlineKeyboardButton("McDonalds", callback_data="evi_mcd")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        "🛡️ Επιβεβαίωσε ότι είσαι η Εύη επιλέγοντας ένα από τα παρακάτω:",
        reply_markup=reply_markup
    )

# === CONFIG ===
TOKEN    = "7140433953:AAEOEfkdFM3rkTu-eYn_S9aI3fY_EszkfT8"  # βάλ’ το δικό σου token αν θες
TZ       = pytz.timezone("Europe/Athens")
DB_FILE  = "bot.db"
# Google Sheets API configuration
SHEETS_API_KEY = "AIzaSyDBbGSp2ndjAVXLgGa_fs_GTn6EuFvtIno"
SPREADSHEET_ID = "YOUR_SPREADSHEET_ID"  # replace with your spreadsheet ID
SHEET_RANGE    = "Sheet1!A1:Z"          # adjust sheet name/range as needed

# — Shift (βάρδια) μοντέλα —
SHIFT_MODELS = [
    "Lydia",
    "Miss Frost",
    "Lina",
    "Frika",
    "Iris",
    "Electra",
    "Nina",
    "Eirini",
    "Marilia",
    "Areti",
    "Silia",
    "Iwanna",
    "Elvina",
    "Stefania",
    "Elena",
    "Natalia",
    "Sabrina",
    "Barbie",
    "Antwnia",
    "Κωνσταντίνα Mummy",
    "Gavriela",
    "Χριστίνα"
]

# — Mistake subsystem μοντέλα —
MISTAKE_MODELS = [
    "Lydia", "Miss Frost", "Lina", "Frika",
    "Iris", "Electra", "Nina", "Roxana"
]

# — Logging —
logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger()

# — DB init —
conn = sqlite3.connect(DB_FILE, check_same_thread=False)
c    = conn.cursor()
c.execute("""
CREATE TABLE IF NOT EXISTS shifts (
  user_id TEXT PRIMARY KEY,
  models  TEXT,
  mode    TEXT,
  start_time TEXT
)
""")
conn.commit()

USER_BREAK_USED = {}  # uid -> total break minutes used in current shift
MAX_BREAK_MINUTES = 45

#
# — In-memory state —
user_names        = {}   # uid -> username
user_status       = {}   # uid -> set(models)
user_mode         = {}   # uid -> "on"/"off"
USER_BREAK = {}
on_times          = {}   # uid -> datetime

mistake_status    = {}
mistake_mode      = {}
mistake_on_times  = {}

message_owner     = {}   # (chat_id, msg_id) -> uid
give_target       = {}   # (chat_id, msg_id) -> target_username
give_selected     = {}   # (chat_id, msg_id) -> set(models)

# Live subsystem state
LIVE_SELECTED = {}  # (chat_id, message_id) -> set(models)
LIVE_MODE = {}      # (chat_id, message_id) -> "on" or "off"

# Break subsystem
break_timers          = {}  # uid -> datetime end
break_active          = set()
custom_break_requests = {}  # uid -> chat_id
# Track users who have been notified for late break
break_notified        = set()
break_group_chat_ids  = {}  # uid -> group_chat_id

# Give approval flow
confirm_flow     = {}  # msg_id -> (giver, target, models_str)
recipient_confirm_flow = {}  # mid -> (giver_username, recipient_id, models_str, chat_id)
ALLOWED_APPROVERS = {"mikekrp", "tsaqiris"}

# Predefined users mapping
PREDEFINED_USERS = {
    "Tsaqiris": 6431210056,
    "mikekrp": 123456789,
    "Evi_Nikolaidou": 234567890,
    "kostasmavridis": 345678901
}

removed_map = {}  # uid -> set(models removed during OFF)

# Pending acknowledgments for shift reminders (ack_id -> (chat_id, message_id, model, chatter_handle))
pending_acks = {}


def save_shift(uid: int):
    mods = ",".join(user_status.get(uid, []))
    mode = user_mode.get(uid, "")
    st   = on_times.get(uid)
    iso  = st.astimezone(TZ).isoformat() if st else ""
    c.execute("""
      INSERT INTO shifts(user_id,models,mode,start_time)
      VALUES(?,?,?,?)
      ON CONFLICT(user_id) DO UPDATE SET
        models=excluded.models,
        mode=excluded.mode,
        start_time=excluded.start_time
    """, (str(uid), mods, mode, iso))
    conn.commit()


# --- Google Sheets helper ---
def fetch_sheet_values():
    # Fetch CSV export from published Google Sheet
    csv_url = (
        "https://docs.google.com/spreadsheets/d/e/2PACX-1vSsNJH6TE4kadbjVOlUqRLRqT7hWZ4EvTTmwE2l-IAElU_imO9Q8ZFo1quptxeoN0DQyct90jtTVrf9/pub?gid=0&single=true&output=csv"
    )
    resp = requests.get(csv_url)
    resp.raise_for_status()
    # decode raw bytes as UTF-8 to prevent mojibake
    data = resp.content.decode('utf-8')
    reader = csv.reader(io.StringIO(data))
    return list(reader)


def build_keyboard(models_list, sel_set):
    kb, row = [], []
    for i,m in enumerate(models_list):
        txt = f"✅ {m}" if m in sel_set else m
        row.append(InlineKeyboardButton(txt, callback_data=m))
        if (i+1) % 3 == 0:
            kb.append(row); row = []
    if row:
        kb.append(row)
    kb.append([InlineKeyboardButton("✅ OK", callback_data="OK")])
    return InlineKeyboardMarkup(kb)


async def common_button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    query = q
    # Ensure uid is always set from the Telegram user
    uid = update.effective_user.id
    if not query:
        return

    # Restrict inline button presses to the original invoking user
    key = (query.message.chat.id, query.message.message_id)
    owner_uid = message_owner.get(key)
    if owner_uid is not None and uid != owner_uid:
        return await query.answer("❌ Δεν έχεις δικαίωμα να επιλέξεις αυτό το πλήκτρο.", show_alert=True)

    sel = query.data
    if not sel:
        await query.message.reply_text("Σφάλμα: Δεν βρέθηκε ημέρα.")
        return
    await query.answer()

    # STEP 2 - SELECT TYPE (new)
    sel = query.data if query else ""
    if sel.startswith("mp_type_") and context.user_data.get("makeprog_step") == 2:
        type_selected = sel.replace("mp_type_", "")
        context.user_data["makeprog_type"] = type_selected
        context.user_data["makeprog_step"] = 3

        days = ["Δευτέρα", "Τρίτη", "Τετάρτη", "Πέμπτη", "Παρασκευή", "Σάββατο", "Κυριακή"]
        buttons = [[InlineKeyboardButton(day, callback_data=f"mp_day_{day}")] for day in days]
        await query.edit_message_text("Επέλεξε ημέρα:", reply_markup=InlineKeyboardMarkup(buttons))
        return

    # STEP 3 - SELECT DAY
    if not sel:
        await query.message.reply_text("Σφάλμα: Δεν βρέθηκε ημέρα.")
        return
    if sel.startswith("mp_day_"):
        day = sel.replace("mp_day_", "")
        context.user_data["makeprog_day"] = day
        context.user_data["makeprog_step"] = 4

        buttons = [
            [InlineKeyboardButton("08:00", callback_data="mp_start_08:00"),
             InlineKeyboardButton("09:00", callback_data="mp_start_09:00")],
            [InlineKeyboardButton("10:00", callback_data="mp_start_10:00"),
             InlineKeyboardButton("11:00", callback_data="mp_start_11:00")],
            [InlineKeyboardButton("12:00", callback_data="mp_start_12:00"),
             InlineKeyboardButton("Ρεπό", callback_data="mp_off")],
        ]
        await query.edit_message_text(f"Επέλεξες: {day}\nΤώρα επίλεξε ώρα έναρξης:", reply_markup=InlineKeyboardMarkup(buttons))
        return
    # --- Shift reminder acknowledgment handler ---
    if q.data.startswith("ack_"):
        await q.answer()
        # Extract notification key and remove pending ack
        msg_id = q.message.message_id
        ack_data = pending_acks.pop(msg_id, None)
        if not ack_data:
            return
        user_id, group_chat_id, ack_model, handle = ack_data
        # Notify the user privately
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=f"✅ Ευχαριστούμε που επιβεβαίωσες την υπενθύμιση για το μοντέλο {ack_model}."
            )
        except Exception as e:
            logger.error(f"Error sending DM ack confirmation: {e}")
        # Ειδοποίηση στο group για το callback "ΕΙΔΑ" (live notifications)
        username = q.from_user.username or "user"
        model = ack_model
        try:
            await context.application.bot.send_message(
                chat_id=GROUP_CHAT_ID,
                text=f"✅ Ο χρήστης @{username} είδε την ειδοποίηση για το μοντέλο {model}."
            )
        except Exception as e:
            logger.error(f"Error sending live notification to group: {e}")
        # Notify the registered group chat (legacy, keep for backwards compat)
        notify_chat_id = context.application.bot_data.get('notify_chat_id')
        if notify_chat_id:
            try:
                await context.bot.send_message(
                    chat_id=notify_chat_id,
                    text=f"✅ Ο @{q.from_user.username} είδε την υπενθύμιση για το μοντέλο {ack_model}."
                )
            except Exception as e:
                logger.error(f"Error notifying group on ack: {e}")

        # Notify each admin privately
        for admin in ALLOWED_APPROVERS:
            admin_id = KNOWN_USERS.get(admin)
            if admin_id:
                try:
                    await context.bot.send_message(
                        chat_id=admin_id,
                        text=f"🔔 Ο @{q.from_user.username} επιβεβαίωσε την υπενθύμιση για το μοντέλο {ack_model}."
                    )
                except Exception as e:
                    logger.error(f"Error notifying admin {admin} on ack: {e}")

        return
    # Only the original invoking user may interact with this command's inline buttons
    key = (q.message.chat.id, q.message.message_id)
    owner_uid = message_owner.get(key)
    if owner_uid is not None and uid != owner_uid:
        return await q.answer("❌ Τι κάνεις εκεί; δεν είναι δικό σου command.", show_alert=True)
    sel = q.data
    chat = q.message.chat
    # --- Restart Bot via inline button ---
    if q.data == "restart_bot":
        # Restrict to admins only (check Telegram admin status)
        chat_member = await context.bot.get_chat_member(chat_id=update.effective_chat.id, user_id=update.effective_user.id)
        if chat_member.status not in ["administrator", "creator"]:
            await update.callback_query.answer("🚫 Μόνο οι admins μπορούν να κάνουν επανεκκίνηση του bot.", show_alert=True)
            return
        # Optionally, also restrict to ALLOWED_APPROVERS usernames
        if q.from_user.username not in ALLOWED_APPROVERS:
            return await q.answer("❌ Δεν έχετε δικαίωμα.", show_alert=True)
        await q.message.delete()
        sent = await q.message.chat.send_message("♻️ Επανεκκίνηση bot...")
        await sent.delete()
        os.execv(sys.executable, [sys.executable] + sys.argv)
    # --- Start confirmation for Evi ---
    if q.data.startswith("evi_"):
        try:
            await q.answer()
        except tg_error.TelegramError:
            pass
        choice = q.data.split("_", 1)[1]
        # Only accept shrimp ("Γαρίδες σαγανάκι")
        if choice != "shrimp":
            # Show a simple alert on wrong choice
            return await q.answer("❌ Λανθασμένη επιλογή.", show_alert=True)
        # Fun welcome message for Evi
        await q.message.delete()
        await context.bot.send_message(
            chat_id=q.message.chat.id,
            text="🦐 Γεια σου Εύη! Δεν ήταν και τόσο δύσκολο, ε; !euh για τις εντολές."
        )
        return
    try:
        await q.answer()
    except tg_error.TelegramError:
        pass

    # --- Live subsystem handling ---
    lm_key = (q.message.chat.id, q.message.message_id)
    if lm_key in LIVE_MODE:
        mode = LIVE_MODE[lm_key]
        selset = LIVE_SELECTED[lm_key]
        # If the selection is a model with the prefix "live_model_" or "liveoff_model_", extract only the model name
        if sel.startswith("live_model_"):
            model_name = sel.replace("live_model_", "")
            # Notify the group chat as soon as a model is selected live
            try:
                await context.bot.send_message(
                    chat_id=GROUP_CHAT_ID,
                    text=f"🎥 Το μοντέλο <b>{model_name}</b> κάνει live!",
                    parse_mode="HTML"
                )
            except tg_error.BadRequest as e:
                logger.error(f"Live notify failed for group {GROUP_CHAT_ID}: {e}")
            # Μόλις επιλεχθεί ένα μοντέλο, στείλε προσωπικό μήνυμα στον chatter του μοντέλου με κουμπί "Το είδα" και σταμάτα εδώ
            # Βρες τον chatter που έχει αυτό το μοντέλο (είτε σε ON είτε σε OFF)
            owner = None
            for uid_, mods in user_status.items():
                if model_name in mods:
                    owner = uid_
                    break
            if owner:
                text = f"🎥 Το μοντέλο <b>{model_name}</b> κάνει live!"
                try:
                    ack_callback = f"ack_live_on_{model_name}_{datetime.now(TZ).date().isoformat()}"
                    keyboard = InlineKeyboardMarkup([[
                        InlineKeyboardButton("👍 Το είδα", callback_data=ack_callback)
                    ]])
                    msg = await context.bot.send_message(
                        chat_id=owner,
                        text=text,
                        parse_mode="HTML",
                        reply_markup=keyboard
                    )
                    handle = f"@{user_names.get(owner, '')}"
                    group_chat_id = context.application.bot_data.get('notify_chat_id')
                    pending_acks[msg.message_id] = (owner, group_chat_id, model_name, handle)
                    asyncio.create_task(check_ack(context.application, owner, msg.message_id, model_name, handle))
                except Exception:
                    pass
            await q.message.delete()
            # Send confirmation in this chat that the model is live
            await context.bot.send_message(
                chat_id=q.message.chat.id,
                text=f"✅ Καταγράφηκε live για το μοντέλο {model_name}."
            )
            # Δεν επαναπροβάλουμε το inline keyboard με όλα τα μοντέλα
            return
        elif sel.startswith("liveoff_model_"):
            model_name = sel.replace("liveoff_model_", "")
            # Notify the chatter privately with an acknowledgment button
            owner = None
            for uid_, mods in user_status.items():
                if model_name in mods:
                    owner = uid_
                    break
            if owner:
                try:
                    text = f"✖️ Το μοντέλο <b>{model_name}</b> σταμάτησε το live."
                    ack_callback = f"ack_live_off_{model_name}_{datetime.now(TZ).date().isoformat()}"
                    keyboard = InlineKeyboardMarkup([[
                        InlineKeyboardButton("👍 Το είδα", callback_data=ack_callback)
                    ]])
                    msg = await context.bot.send_message(
                        chat_id=owner,
                        text=text,
                        parse_mode="HTML",
                        reply_markup=keyboard
                    )
                    handle = f"@{user_names.get(owner, '')}"
                    group_chat_id = context.application.bot_data.get('notify_chat_id')
                    pending_acks[msg.message_id] = (owner, group_chat_id, model_name, handle)
                    asyncio.create_task(check_ack(context.application, owner, msg.message_id, model_name, handle))
                except Exception:
                    pass
            # Do not delete the inline message here; wait for OK confirmation
            return
        elif sel.startswith("liveoff_model_"):
            model_name = sel.replace("liveoff_model_", "")
            # Μόλις επιλεχθεί ένα μοντέλο, στείλε προσωπικό μήνυμα στον chatter του μοντέλου με κουμπί "Το είδα" και σταμάτα εδώ
            owner = None
            for uid_, mods in user_status.items():
                if model_name in mods:
                    owner = uid_
                    break
            if owner:
                text = f"✖️ Το μοντέλο <b>{model_name}</b> σταμάτησε το live."
                try:
                    ack_callback = f"ack_live_off_{model_name}_{datetime.now(TZ).date().isoformat()}"
                    keyboard = InlineKeyboardMarkup([[
                        InlineKeyboardButton("👍 Το είδα", callback_data=ack_callback)
                    ]])
                    msg = await context.bot.send_message(
                        chat_id=owner,
                        text=text,
                        parse_mode="HTML",
                        reply_markup=keyboard
                    )
                    handle = f"@{user_names.get(owner, '')}"
                    group_chat_id = context.application.bot_data.get('notify_chat_id')
                    pending_acks[msg.message_id] = (owner, group_chat_id, model_name, handle)
                    asyncio.create_task(check_ack(context.application, owner, msg.message_id, model_name, handle))
                except Exception:
                    pass
            await q.message.delete()
            # Notify the group chat that the model stopped live
            try:
                await context.bot.send_message(
                    chat_id=GROUP_CHAT_ID,
                    text=f"✖️ Το μοντέλο <b>{model_name}</b> σταμάτησε το live.",
                    parse_mode="HTML"
                )
            except tg_error.BadRequest as e:
                logger.error(f"Live notify failed for group {GROUP_CHAT_ID}: {e}")
            # Δεν επαναπροβάλουμε το inline keyboard με όλα τα μοντέλα
            return
        # Only show the inline keyboard for model selection on first call, not after confirmation
        if sel != "OK":
            selset.symmetric_difference_update({sel})
            try:
                return await q.message.edit_reply_markup(reply_markup=build_keyboard(SHIFT_MODELS, selset))
            except Exception:
                return
        # OK pressed: notify chatter(s)
        await q.message.delete()
        # Send DM and group notifications for each selected model
        for model in selset:
            # Find the owner of the model
            owner = None
            for uid_, mods in user_status.items():
                if model in mods:
                    owner = uid_
                    break
            # Prepare notification text
            notif_text = (
                f"🎥 Το μοντέλο <b>{model}</b> κάνει live!"
                if mode == "on"
                else f"✖️ Το μοντέλο <b>{model}</b> σταμάτησε το live."
            )
            # Send private DM to the owner if found
            if owner:
                try:
                    await context.bot.send_message(
                        chat_id=owner,
                        text=notif_text,
                        parse_mode="HTML"
                    )
                except Exception:
                    pass
            # Send notification to the main group chat
            try:
                await context.bot.send_message(
                    chat_id=GROUP_CHAT_ID,
                    text=notif_text,
                    parse_mode="HTML"
                )
            except Exception:
                pass
        # Instead of showing the model selection menu again, just send a confirmation message
        if selset:
            await context.bot.send_message(
                chat_id=chat.id,
                text=f"✅ Καταγράφηκε το live για το/τα μοντέλο/α: {', '.join(selset)}"
            )
        else:
            await context.bot.send_message(
                chat_id=chat.id,
                text="Δεν επιλέχθηκε κανένα μοντέλο για live."
            )
        # cleanup
        del LIVE_MODE[lm_key]
        del LIVE_SELECTED[lm_key]
        return

    # --- Mistake subsystem ---
    if mistake_mode.get(uid) in ("on", "off"):
        selset = mistake_status.setdefault(uid, set())
        if sel != "OK":
            if mistake_mode[uid] == "on":
                selset.symmetric_difference_update({sel})
            else:
                selset.discard(sel)
            # Determine which models to show: all on mistake ON, only active on mistake OFF
            if mistake_mode[uid] == "on":
                keyboard_models = MISTAKE_MODELS
            else:
                keyboard_models = sorted(selset)
            try:
                return await q.message.edit_reply_markup(reply_markup=build_keyboard(keyboard_models, selset))
            except RetryAfter as e:
                logger.warning(f"Flood control on edit_reply_markup: retry after {e.retry_after}s")
                return
            except Exception as e:
                logger.error(f"Error editing reply_markup: {e}")
                return
        # OK finalize
        await q.message.delete()
        now = datetime.now(TZ)
        st = mistake_on_times.get(uid)
        dur = ""
        if st:
            d = now - st
            h, m = divmod(int(d.total_seconds()), 3600)[0], divmod(int(d.total_seconds()) % 3600, 60)[0]
            dur = f"{h}h {m}m"
        selset = mistake_status.get(uid, set())
        if mistake_mode[uid] == "off":
            if not selset:
                txt = (
                    f"🔴 Mistake OFF by @{user_names[uid]}\n"
                    f"🕒 {now.strftime('%H:%M')}   ⏱ Duration: {dur}\n"
                    "🚩 Τελείωσε η mistake βάρδιά του!"
                )
            else:
                txt = (
                    f"🔴 Mistake OFF by @{user_names[uid]}\n"
                    f"🕒 {now.strftime('%H:%M')}   ⏱ Duration: {dur}\n"
                    f"Models: {', '.join(sorted(selset)) or 'κανένα'}"
                )
            mistake_mode.pop(uid, None)
            try:
                await context.bot.send_message(chat.id, txt)
            except RetryAfter as e:
                logger.warning(f"Flood control on send_message: retry after {e.retry_after}s")
            except Exception as e:
                logger.error(f"Error sending message: {e}")
            return
        else:
            txt = (
                f"✅ Mistake ON by @{user_names[uid]}\n"
                f"🕒 {now.strftime('%H:%M')}   ⏱ Duration: {dur}\n"
                f"Models: {', '.join(sorted(selset)) or 'κανένα'}"
            )
            try:
                await context.bot.send_message(chat.id, txt)
            except RetryAfter as e:
                logger.warning(f"Flood control on send_message: retry after {e.retry_after}s")
            except Exception as e:
                logger.error(f"Error sending message: {e}")
            return

    # --- Acceptgive recipient confirmation (move up for early return) ---
    sel = q.data
    if sel.startswith("acceptgive_"):
        uid = q.from_user.id
        chat = q.message.chat
        mid = int(sel.split("_")[1])
        data = recipient_confirm_flow.pop(mid, None)
        if not data:
            return await context.bot.send_message(chat.id, "❌ Δεν βρέθηκε η πληροφορία της απόδοσης.")
        giver, tid, models, original_chat = data
        if uid != tid:
            return await q.answer("❌ Δεν είσαι ο παραλήπτης.", show_alert=True)

        user_names[tid] = q.from_user.username or f"id_{tid}"
        user_mode[tid] = "on"
        user_status.setdefault(tid, set()).update(models.split(", "))
        on_times[tid] = datetime.now(TZ)
        save_shift(tid)
        context.application.bot_data.setdefault("previous_models_map", {})[tid] = set(models.split(", "))

        # αφαιρέσει από giver
        giver_uid = next((k for k,v in user_names.items() if v == giver), None)
        if giver_uid:
            user_status.setdefault(giver_uid, set()).difference_update(models.split(", "))
            if not user_status[giver_uid]:
                user_mode[giver_uid] = "off"
                on_times.pop(giver_uid, None)
            removed_map[giver_uid] = set(models.split(", "))
            save_shift(giver_uid)

        now = datetime.now(TZ)
        dur = "μόλις ξεκίνησε την βάρδια"
        added_info = f"\n➕ Νέα: {', '.join(sorted(models.split(', ')))}"
        shift_text = (
            f"🔛 Shift ON by @{user_names[tid]}\n"
            f"🕒 {now.strftime('%H:%M')}   ⏱ Duration: {dur}\n"
            f"Models: {', '.join(models.split(', '))}{added_info}"
        )
        await context.bot.send_message(chat_id=original_chat, text=shift_text)

        if giver_uid:
            remaining_models = user_status.get(giver_uid, set())
            removed_models = ", ".join(models.split(", "))
            dur = ""
            st = on_times.get(giver_uid)
            if st:
                d = now - st
                h, m = divmod(int(d.total_seconds()), 3600)[0], divmod(int(d.total_seconds()) % 3600, 60)[0]
                dur = f"{h}h {m}m"
            if not remaining_models:
                off_txt = (
                    f"🔴 Shift OFF by @{giver}\n"
                    f"🕒 {now.strftime('%H:%M')}   ⏱ Duration: {dur}\n"
                    f"🎁Έδωσε : {removed_models}\n"
                    "🚩 Τελείωσε την βάρδιά του!"
                )
            else:
                off_txt = (
                    f"🔴 Shift OFF by @{giver}\n"
                    f"🕒 {now.strftime('%H:%M')}   ⏱ Duration: {dur}\n"
                    f"🎁Έδωσε : {removed_models}\n"
                    f"✅ Παραμένει σε: {', '.join(remaining_models)}"
                )
            await context.bot.send_message(original_chat, off_txt)

        return await context.bot.send_message(chat_id=tid, text=f"✅ Έκανες αποδοχή για: {models}")

    user_mode.setdefault(uid, "off")

    # --- Break buttons ---
    if sel.startswith("break_"):
        _, val = sel.split("_",1)
        owner_id = message_owner.get((chat.id, q.message.message_id))
        if owner_id is not None and owner_id != uid:
            try:
                return await q.answer("❌ Δεν έχεις δικαίωμα να το επιλέξεις.", show_alert=True)
            except tg_error.TelegramError:
                return
        # --- Validate break time before starting a predefined break ---
        if val.isdigit():
            requested = int(val)
            used = USER_BREAK_USED.get(uid, 0)
            remaining = MAX_BREAK_MINUTES - used
            if requested > remaining:
                try:
                    await q.answer(f"❌ Δεν έχεις διαθέσιμα {requested} λεπτά.\n📏 Υπόλοιπο: {remaining}ʼ.", show_alert=True)
                except tg_error.TelegramError:
                    pass
                await context.bot.send_message(
                    chat_id=chat.id,
                    text=f"❌ Δεν μπορείς να πάρεις {requested}ʼ διάλειμμα, σου απομένουν μόνο {remaining}ʼ."
                )
                return
        if val=="cancel":
            break_timers.pop(uid, None); break_active.discard(uid)
            return await q.message.edit_text("❌ Το διάλειμμά σου ακυρώθηκε.")
        if val=="custom":
            custom_break_requests[uid] = chat.id
            res = await q.message.edit_text("⏱️ Πληκτρολόγησε πόσα λεπτά θέλεις για διάλειμμα:")
            message_owner[(q.message.chat.id, q.message.message_id)] = uid
            used = USER_BREAK_USED.get(uid, 0)
            rem = MAX_BREAK_MINUTES - used
            await context.bot.send_message(chat_id=chat.id, text=f"📏 Σου απομένουν {rem} λεπτά διαλείμματος.")
            return res
        minutes = int(val)
        context.user_data['break_duration'] = minutes
        context.user_data['break_start_time'] = datetime.now(TZ)
        end = datetime.now(TZ) + timedelta(minutes=minutes)
        break_timers[uid] = end
        break_group_chat_ids[uid] = chat.id
        break_active.add(uid)
        # Schedule break end notification
        context.job_queue.run_once(
            end_break,
            when=timedelta(minutes=minutes),
            data={"uid": uid}
        )
        return await q.message.edit_text(f"☕ Διάλειμμα {minutes}ʼ ξεκίνησε! Θα σε υπενθυμίσω.")

    # --- Custom break text handler elsewhere ---

    # --- Give subsystem ---
    key = (chat.id, q.message.message_id)
    if key in give_target:
        if sel!="OK":
            selset = give_selected[key]
            selset.symmetric_difference_update({sel})
            # Only show user's current models, not all models
            return await q.message.edit_reply_markup(reply_markup=build_keyboard(user_status.get(uid, set()), selset))
        # OK -> approval request
        selset = give_selected.pop(key, set())
        target = give_target.pop(key)
        giver = q.from_user.username
        models = ", ".join(selset) or "κανένα"
        await q.message.delete()
        from telegram.helpers import escape_markdown
        escaped_models = escape_markdown(models, version=2)
        escaped_target = escape_markdown(target, version=2)
        cm = await context.bot.send_message(
            chat_id=chat.id,
            text=f"🔔 Πατήστε ✅ για να επιβεβαιώσετε μοντέλα *{escaped_models}* προς {escaped_target}:",
            parse_mode="MarkdownV2"
        )
        confirm_flow[cm.message_id] = (giver, target, models)
        markup = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Επιβεβαίωση", callback_data=f"confirm_{cm.message_id}"),
            InlineKeyboardButton("❌ Απόρριψη",     callback_data=f"reject_{cm.message_id}")
        ]])
        return await cm.edit_reply_markup(reply_markup=markup)

    # --- Confirmation callbacks ---
    if sel.startswith(("confirm_","reject_")):
        # Only allow specific admins to confirm or reject gives
        approver = q.from_user.username
        action, mid = sel.split("_",1)
        if approver not in ALLOWED_APPROVERS:
            # alert the user
            await q.answer("❌ Δεν είσαι admin, τι κάνεις εκεί;", show_alert=True)
            # notify all admins about the unauthorized press
            for admin_username in ALLOWED_APPROVERS:
                admin_id = KNOWN_USERS.get(admin_username)
                if admin_id:
                    await context.bot.send_message(
                        chat_id=admin_id,
                        text=f"⚠️ Ο @{approver} προσπάθησε να {action} στο give."
                    )
            return
        mid = int(mid)
        values = confirm_flow.pop(mid, None)
        if values is None:
            return await context.bot.send_message(chat.id, "❌ Δεν βρέθηκε η πληροφορία της απόδοσης.")
        giver, target, models = values
        try:
            # Lookup recipient ID by lowercase username key
            username_key = target.lstrip("@").lower()
            tid = KNOWN_USERS.get(username_key)
            if tid is not None:
                class DummyUser:
                    def __init__(self, id, username): self.id, self.username = id, username
                user_obj = DummyUser(tid, username_key)
            else:
                full_target = f"@{target}" if not target.startswith("@") else target
                try:
                    chat_member = await context.bot.get_chat_member(chat.id, full_target)
                    user_obj = chat_member.user
                    tid = user_obj.id
                except tg_error.TelegramError:
                    return await context.bot.send_message(chat.id, f"❌ Δεν βρέθηκε ο χρήστης {target}.")
        except tg_error.TelegramError:
            return await context.bot.send_message(chat.id, f"❌ Δεν βρέθηκε ο χρήστης {target}.")
        await q.message.delete()
        recipient_id = user_obj.id  # Ensure recipient_id assigned before use and user_obj is correct
        if action == "confirm":
            # Βήμα 1: Admin approved → τώρα ζητάμε και του recipient
            recipient_confirm_flow[mid] = (giver, recipient_id, models, chat.id)
            await context.bot.send_message(
                chat_id=recipient_id,
                text=f"🎁 Ο @{giver} θέλει να σου μεταβιβάσει μοντέλα: {models}.\nΠατήστε αποδοχή:",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("✅ Αποδοχή", callback_data=f"acceptgive_{mid}")
                ]])
            )
            # Notify group chat that admins have approved and we're waiting on the recipient
            await context.bot.send_message(
                chat_id=chat.id,
                text=f"🔔 Οι admins αποδέχτηκαν το αίτημά σου @{giver} και περιμένουμε από τον @{target} να πατήσει Αποδοχή για να γίνει το give."
            )
            return
        else:
            return await context.bot.send_message(chat.id, f"❌ Απορρίφθηκε η απόδοση σε {target}.")


    # --- Shift subsystem ---
    # --- Make Program: handle mp_type_dayoff callback ---
    # Implements: set shift_type=dayoff, confirmed True, step=5, show keyboard
    if sel == "mp_type_dayoff" and context.user_data.get("makeprog_step") == 2:
        day = context.user_data.get('current_day')
        if not day:
            return
        context.user_data.setdefault('program', {})
        context.user_data['program'][day] = {
            'shift_type': 'dayoff',
            'hours': '—',
            'confirmed': True
        }
        context.user_data['makeprog_step'] = 5
        prog = context.user_data['program']
        keyboard = [
            [InlineKeyboardButton("🆗 Τέλος, στείλτο", callback_data="mp_send")],
            [InlineKeyboardButton("🔍 Προεπισκόπηση", callback_data="mp_preview")],
            [InlineKeyboardButton("❌ Ακύρωση", callback_data="mp_cancel")]
        ]
        for i, d in enumerate(DAYS):
            label = f"🟢 {d}" if d in prog and prog[d].get("confirmed") else d
            keyboard.append([InlineKeyboardButton(label, callback_data=f"mp_day_{i}")])
        return await safe_send(context.bot, q.message.chat_id, "📅 Θες να συνεχίσεις;", reply_markup=InlineKeyboardMarkup(keyboard))

    # --- Make Program: handle mp_day_X callback ---
    elif sel.startswith("mp_day_"):
        if context.user_data.get("makeprog_step") != 3:
            return
        day = sel.replace("mp_day_", "")
        context.user_data["current_day"] = day
        await handle_makeprogram_day(update, context)
        return
    selset = user_status.setdefault(uid, set())
    if user_mode[uid] == "on":
        if sel != "OK":
            taken_models = {model for u, mods in user_status.items() if u != uid and user_mode.get(u) == "on" for model in mods}
            if sel in taken_models:
                try:
                    return await q.answer("❌ Το μοντέλο είναι ήδη σε χρήση από άλλον χρήστη.", show_alert=True)
                except tg_error.TelegramError:
                    return
            if sel in selset:
                try:
                    return await q.answer("❌ Δεν μπορείς να αφαιρέσεις μοντέλα ενώ είσαι ON. Χρησιμοποίησε /off.", show_alert=True)
                except tg_error.TelegramError:
                    return
            selset.add(sel)
            available_models = [m for m in SHIFT_MODELS if m not in taken_models]
            new_markup = build_keyboard(available_models, selset)
            # Compare button texts manually to avoid "Message is not modified"
            old_buttons = q.message.reply_markup.inline_keyboard
            new_buttons = new_markup.inline_keyboard
            if [[btn.text for btn in row] for row in old_buttons] == [[btn.text for btn in row] for row in new_buttons]:
                return
            try:
                return await q.message.edit_reply_markup(reply_markup=new_markup)
            except RetryAfter as e:
                logger.warning(f"Flood control on edit_reply_markup: retry after {e.retry_after}s")
                return
            except Exception as e:
                logger.error(f"Error editing reply_markup: {e}")
                return
        elif sel == "OK":
            # Check if at least one model is selected before confirming shift ON
            if not selset:
                return await context.bot.send_message(chat.id, "❌ Πρέπει να επιλέξεις τουλάχιστον ένα μοντέλο για να ξεκινήσεις βάρδια.")
            await q.message.delete()
            now = datetime.now(TZ)
            st = on_times.get(uid)
            # Duration logic
            dur = ""
            if st:
                d = now - st
                h = divmod(int(d.total_seconds()), 3600)[0]
                m = divmod(int(d.total_seconds()) % 3600, 60)[0]
                if h == 0 and m == 0:
                    dur = "μόλις ξεκίνησε την βάρδια"
                else:
                    dur = f"{h}h {m}m"
            # Use exactly the models the user selected, without filtering out "taken" sets
            selset = set(user_status.get(uid, set()))
            # Read previous_models as-is
            previous_models = set(context.application.bot_data.get("previous_models_map", {}).get(uid, set()))
            added_models = selset - previous_models
            context.application.bot_data.setdefault("previous_models_map", {})[uid] = selset.copy()
            added_info = ""
            if added_models:
                added_info = f"\n➕ Νέα: {', '.join(sorted(added_models))}"
            else:
                added_info = ""
            txt = (
                f"🔛 Shift ON by @{user_names[uid]}\n"
                f"🕒 {now.strftime('%H:%M')}   ⏱ Duration: {dur}\n"
                f"Models: {', '.join(selset) or 'Δεν επελεξε models'}{added_info}"
            )
            try:
                await context.bot.send_message(chat.id, txt)
            except RetryAfter as e:
                logger.warning(f"Flood control on send_message: retry after {e.retry_after}s")
            except Exception as e:
                logger.error(f"Error sending message: {e}")
            save_shift(uid)
            return
    else:  # off mode
        if sel != "OK":
            # Track removed models
            if sel in selset:
                selset.remove(sel)
                removed_map.setdefault(uid, set()).add(sel)
            elif sel in removed_map.get(uid, set()):
                selset.add(sel)
                removed_map[uid].discard(sel)
            else:
                try:
                    return await q.answer("❌ Δεν είσαι σε αυτό.", show_alert=True)
                except tg_error.TelegramError:
                    return
            # Only edit reply markup if changed
            new_markup = build_keyboard(sorted(selset), selset)
            if q.message.reply_markup == new_markup:
                return  # Avoid "Message is not modified" error
            return await q.message.edit_reply_markup(reply_markup=new_markup)
        # OK finalize
        await q.message.delete()
        now = datetime.now(TZ)
        st = on_times.get(uid)
        dur = ""
        if st:
            d = now - st
            h, m = divmod(int(d.total_seconds()), 3600)[0], divmod(int(d.total_seconds()) % 3600, 60)[0]
            dur = f"{h}h {m}m"
        # Restore previous simpler /off message behavior
        if not selset:
            txt = (
                f"🔴 Shift OFF by @{user_names[uid]}\n"
                f"🕒 {now.strftime('%H:%M')}   ⏱ Duration: {dur}\n"
                "🚩 Τελείωσε την βάρδιά του!"
            )
        else:
            removed_models = removed_map.get(uid, set())
            txt = (
                f"🔴 Shift OFF by @{user_names[uid]}\n"
                f"🕒 {now.strftime('%H:%M')}   ⏱ Duration: {dur}\n"
                f"Models: {', '.join(selset)}\n"
                f"🗑 Αφαίρεσε: {', '.join(sorted(removed_models)) or 'καμία'}"
            )
        await context.bot.send_message(chat.id, txt)
        save_shift(uid)
        if user_mode[uid] == "off" and not selset:
            on_times.pop(uid, None)
        return


# === --- === DAY PROGRAM INTERACTIVE KEYBOARD LOGIC === --- ===

# --- ΕΝΑΡΞΗ /makeprogram ---
async def mp_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Start interactive day program entry for 7 days
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    uid = update.effective_user.id
    context.user_data["mp_days"] = [None] * 7
    context.user_data["mp_stage"] = 0
    await send_mp_day_prompt(update.effective_chat.id, context, 0, context)

# Alias for backward compatibility
handle_makeprogram_start = mp_start


#
# Νέα υλοποίηση handle_makeprogram_day
#
DAYS = {
    "mp_day_mon": "Δευτέρα",
    "mp_day_tue": "Τρίτη",
    "mp_day_wed": "Τετάρτη",
    "mp_day_thu": "Πέμπτη",
    "mp_day_fri": "Παρασκευή",
    "mp_day_sat": "Σάββατο",
    "mp_day_sun": "Κυριακή",
}

async def handle_makeprogram_day(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    sel = query.data

    if sel not in DAYS:
        await safe_edit(query.message, "Σφάλμα: Δεν βρέθηκε ημέρα.")
        return

    day = DAYS[sel]
    context.user_data.setdefault("program", {})
    if day in context.user_data["program"]:
        await safe_edit(query.message, f"📅 Έχεις ήδη καταχωρήσει την {day}.")
        return

    context.user_data["current_day"] = day
    context.user_data["program"][day] = {}

    keyboard = [
        [InlineKeyboardButton("🌞 Πρωινή", callback_data="mp_shift_morning")],
        [InlineKeyboardButton("🌆 Απογευματινή", callback_data="mp_shift_evening")],
        [InlineKeyboardButton("🛌 Ρεπό", callback_data="mp_shift_dayoff")],
        [InlineKeyboardButton("🔙 Πίσω", callback_data="mp_back_to_days")],
    ]
    await safe_edit(query.message, f"📆 Επιλογή βάρδιας για *{day}*:", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")




# --- safe_edit helper as requested ---
async def safe_edit(message, text, **kwargs):
    try:
        return await message.edit_text(text, **kwargs)
    except Exception as e:
        print(f"[safe_edit] Error: {e}")
        try:
            return await message.reply_text(text, **kwargs)
        except Exception as err:
            print(f"[safe_edit fallback] Failed: {err}")


async def send_mp_day_prompt(chat_id, context, day_index, ctx):
    DAYS = ["Δευτέρα", "Τρίτη", "Τετάρτη", "Πέμπτη", "Παρασκευή", "Σάββατο", "Κυριακή"]
    day_name = DAYS[day_index]
    keyboard = [
        [InlineKeyboardButton("✅ Επιβεβαίωση", callback_data=f"mp_end_{day_index}")],
        [InlineKeyboardButton("🛏️ Day OFF", callback_data=f"mp_type_dayoff_{day_index}")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await ctx.bot.send_message(chat_id=chat_id, text=f"🔹 Εισάγετε πρόγραμμα για {day_name}:", reply_markup=reply_markup)


async def handle_mp_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    uid = q.from_user.id
    data = q.data
    # Handle mp_end_ and mp_type_dayoff_
    if data.startswith("mp_end_") or data.startswith("mp_type_dayoff_"):
        if data.startswith("mp_end_"):
            day_index = int(data.split("_")[-1])
            context.user_data["mp_days"][day_index] = "Επιβεβαιώθηκε"
        elif data.startswith("mp_type_dayoff_"):
            day_index = int(data.split("_")[-1])
            context.user_data["mp_days"][day_index] = "Day OFF"
        # Advance to next day or finish
        next_day = day_index + 1
        if next_day < 7:
            await q.answer()
            await q.message.edit_text(f"✅ Καταχωρήθηκε για {DAYS[day_index]}. Προχωράμε στην επόμενη ημέρα...")
            await send_mp_day_prompt(q.message.chat.id, context, next_day, context)
        else:
            # All 7 days done, show final keyboard (must be interactive)
            await q.answer()
            # Build summary text and interactive keyboard
            summary_lines = []
            for idx, val in enumerate(context.user_data["mp_days"]):
                day = DAYS[idx]
                v = val or "–"
                summary_lines.append(f"{day}: {v}")
            summary = "\n".join(summary_lines)
            # Build keyboard: OK, Preview, all 7 days (with 🟢 if confirmed)
            kb = []
            kb.append([InlineKeyboardButton("🆗 Τέλος, στείλτο", callback_data="mp_submit")])
            kb.append([InlineKeyboardButton("🔍 Προεπισκόπηση", callback_data="mp_preview")])
            days_btns = []
            for idx, val in enumerate(context.user_data["mp_days"]):
                btn_text = f"{DAYS[idx]}"
                if val:
                    btn_text = f"🟢 {btn_text}"
                days_btns.append(InlineKeyboardButton(btn_text, callback_data=f"mp_edit_{idx}"))
            # 7 days, group as one row or two
            kb.append(days_btns)
            reply_markup = InlineKeyboardMarkup(kb)
            await q.message.edit_text(
                text=f"✅ Επιβεβαίωση προγράμματος:\n\n{summary}\n\nΕπίλεξε αν θες αλλαγή ή στείλτο:",
                reply_markup=reply_markup
            )
        return


async def handle_custom_break_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid not in custom_break_requests:
        return
    text = update.message.text.strip()
    try:
        minutes = int(text)
        if minutes <= 0 or minutes > 45:
            raise ValueError
    except ValueError:
        return await update.message.reply_text("❌ Δώσε αριθμό λεπτών από 1 έως 45.")
    used = USER_BREAK_USED.get(uid, 0)
    remaining = MAX_BREAK_MINUTES - used
    if minutes > remaining:
        return await update.message.reply_text(f"❌ Δεν έχεις διαθέσιμα {minutes} λεπτά.\n📏 Υπόλοιπο: {remaining}ʼ.")
    context.user_data['break_start_time'] = datetime.now(TZ)
    end = datetime.now(TZ)+timedelta(minutes=minutes)
    break_timers[uid] = end
    break_active.add(uid)
    break_group_chat_ids[uid] = custom_break_requests[uid]
    await update.message.reply_text(f"☕ Διάλειμμα {minutes}ʼ ξεκίνησε! Θα σε υπενθυμίσω.")
    del custom_break_requests[uid]


# === COMMANDS HANDLERS ===


async def handle_on(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u, uid = update.effective_user, update.effective_user.id
    print(f"[DEBUG] User {uid} triggered /on")
    # Reset breaks only if the user was not on any model before (i.e. completely off)
    if not user_status.get(uid):
        USER_BREAK[uid] = 45
        USER_BREAK_USED[uid] = 0
    user_names[uid] = u.username
    user_mode[uid]  = "on"
    on_times[uid]   = datetime.now(TZ)
    user_status.setdefault(uid, set())
    # Ensure the user_status entry exists before copying previous_models
    user_status.setdefault(uid, set())
    previous_models = user_status[uid].copy()
    context.application.bot_data.setdefault("previous_models_map", {})[uid] = previous_models
    save_shift(uid)

    # ✅ Μοντέλα σε χρήση από άλλους
    taken_models = {model for uid_, models in user_status.items() if user_mode.get(uid_) == "on" and uid_ != uid for model in models}
    available_models = [m for m in SHIFT_MODELS if m not in taken_models]

    try: await update.message.delete()
    except: pass
    msg = await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=f"🔛 *Shift ON!* Επέλεξε μοντέλα:",
        reply_markup=build_keyboard(available_models, user_status.get(uid, set())),
        parse_mode="Markdown"
    )
    message_owner[(msg.chat.id, msg.message_id)] = uid

# --- /onall handler ---
async def handle_onall(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u, uid = update.effective_user, update.effective_user.id
    user_names[uid] = u.username
    user_mode[uid]  = "on"
    on_times[uid]   = datetime.now(TZ)
    # assign all shift models
    user_status[uid] = set(SHIFT_MODELS)
    # reset break usage
    USER_BREAK_USED[uid] = 0
    # record previous models map
    context.application.bot_data.setdefault("previous_models_map", {})[uid] = set(SHIFT_MODELS)
    save_shift(uid)
    # build the Shift ON message
    now = datetime.now(TZ)
    dur = "μόλις ξεκίνησε την βάρδια"
    models_text = ", ".join(SHIFT_MODELS)
    txt = (
        f"🔛 Shift ON ALL by @{user_names[uid]}\n"
        f"🕒 {now.strftime('%H:%M')}   ⏱ Duration: {dur}\n"
        f"Models: {models_text}"
    )
    await context.bot.send_message(chat_id=update.effective_chat.id, text=txt)

async def handle_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u, uid = update.effective_user, update.effective_user.id
    user_names[uid] = u.username
    user_mode[uid]  = "off"
    removed_map[uid] = set()
    save_shift(uid)
    if not user_status.get(uid, set()):
        return await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="❌ Δεν έχεις ενεργά μοντέλα για να κάνεις OFF. Πρώτα χρησιμοποίησε /on."
        )
    # Refactored check as per instructions
    if user_mode[uid] == "off" and not user_status.get(uid, set()):
        USER_BREAK_USED.pop(uid, None)
    try: await update.message.delete()
    except: pass
    msg = await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="🔴 *Shift OFF!* Αφαίρεσε μοντέλα:",
        reply_markup=build_keyboard(sorted(user_status.get(uid,set())), user_status.get(uid,set())),
        parse_mode="Markdown"
    )
    message_owner[(msg.chat.id, msg.message_id)] = uid

# --- /offall handler ---
async def handle_offall(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    user_names[uid] = update.effective_user.username
    user_mode[uid] = "off"
    user_status[uid] = set()
    removed_map[uid] = set()
    save_shift(uid)

    now = datetime.now(TZ)
    st = on_times.get(uid)
    dur = ""
    if st:
        d = now - st
        h, m = divmod(int(d.total_seconds()), 3600)[0], divmod(int(d.total_seconds()) % 3600, 60)[0]
        dur = f"{h}h {m}m"
    on_times.pop(uid, None)

    txt = (
        f"🔴 Shift OFF by @{user_names[uid]}\n"
        f"🕒 {now.strftime('%H:%M')}   ⏱ Duration: {dur}\n"
        "🚩 Τελείωσε την βάρδιά του!"
    )
    await context.bot.send_message(update.effective_chat.id, txt)

async def handle_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    sel = user_status.get(uid,set())
    if not sel:
        return await context.bot.send_message(update.effective_chat.id, "❌ Δεν έχεις ενεργά μοντέλα.")
    st = on_times.get(uid)
    dur = ""
    if st:
        d = datetime.now(TZ)-st; h,m = divmod(int(d.total_seconds()),3600)[0], divmod(int(d.total_seconds())%3600,60)[0]
        dur = f"\n⏱ {h}h {m}m"
    await context.bot.send_message(update.effective_chat.id, f"📋 Models: {', '.join(sel)}{dur}")

async def handle_give(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # target via reply or mention
    if update.message.reply_to_message:
        tu = update.message.reply_to_message.from_user
        target = tu.username
    else:
        ents = [e for e in update.message.entities if e.type==MessageEntity.MENTION]
        if not ents:
            return await update.message.reply_text("❌ Κάνε reply ή @mention.")
        m = ents[0]; target = update.message.text[m.offset:m.offset+m.length]
        target = target.lstrip('@')

    username = target.lstrip('@').lower()

    recipient_id = KNOWN_USERS.get(username)
    if not recipient_id:
        await context.bot.send_message(chat_id=update.effective_chat.id,
                                       text=f"❌ Δεν βρέθηκε ο χρήστης @{username}.")
        return

    # Filter models: only allow giving models the user owns
    uid = update.effective_user.id
    owned_models = user_status.get(uid, set())
    if not owned_models:
        return await update.message.reply_text("❌ Δεν έχεις μοντέλα για να δώσεις.")
    # Re-filter models after pressing OK (when called via callback_query)
    if update.callback_query:
        uid = update.effective_user.id
        owned_models = user_status.get(uid, set())
        return await update.callback_query.message.edit_reply_markup(
            reply_markup=build_keyboard(sorted(owned_models), set())
        )
    from_u = update.effective_user.username
    msg = await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=f"🎁 Ο @{from_u} θέλει να δώσει μοντέλο σε @{username}. Επιλέξτε:",
        reply_markup=build_keyboard(sorted(owned_models), set())
    )
    give_target[(msg.chat.id,msg.message_id)] = username
    give_selected[(msg.chat.id,msg.message_id)] = set()
    message_owner[(msg.chat.id,msg.message_id)] = update.effective_user.id

async def handle_mistake_on(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u,uid = update.effective_user, update.effective_user.id
    user_names[uid] = u.username
    mistake_mode[uid] = "on"
    mistake_on_times[uid] = datetime.now(TZ)
    mistake_status.setdefault(uid,set())
    try: await update.message.delete()
    except: pass
    msg = await context.bot.send_message(
        update.effective_chat.id,
        text="🎯 *Mistake ON!* Επέλεξε μοντέλα:",
        reply_markup=build_keyboard(MISTAKE_MODELS, mistake_status[uid]),
        parse_mode="Markdown"
    )
    message_owner[(msg.chat.id,msg.message_id)] = uid

async def handle_mistake_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u,uid = update.effective_user, update.effective_user.id
    user_names[uid] = u.username
    mistake_mode[uid] = "off"
    # Check if user has any active mistake models before proceeding
    if not mistake_status.get(uid):
        return await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="❌ Δεν έχεις ενεργά Mistake μοντέλα για να κάνεις OFF."
        )
    try: await update.message.delete()
    except: pass
    active_models = sorted(mistake_status.get(uid, set()))
    msg = await context.bot.send_message(
        update.effective_chat.id,
        text="🔴 *Mistake OFF!* Αφαίρεσε μοντέλα:",
        reply_markup=build_keyboard(active_models, mistake_status.get(uid, set())),
        parse_mode="Markdown"
    )
    message_owner[(msg.chat.id, msg.message_id)] = uid

async def handle_mistake_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    sel = mistake_status.get(uid,set())
    if not sel:
        return await context.bot.send_message(update.effective_chat.id, "❌ Δεν έχεις Mistake.")
    st = mistake_on_times.get(uid)
    dur = ""
    if st:
        d = datetime.now(TZ)-st; h,m = divmod(int(d.total_seconds()),3600)[0], divmod(int(d.total_seconds())%3600,60)[0]
        dur = f"\n⏱ {h}h {m}m"
    await context.bot.send_message(update.effective_chat.id, f"📋 Mistake: {', '.join(sel)}{dur}")

async def handle_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    # Determine image path
    script_dir = os.path.dirname(os.path.realpath(__file__))
    local_path = os.path.join(script_dir, 'gunzoagency.png')
    bundled_path = '/mnt/data/gunzoagency.png'
    image_path = local_path if os.path.isfile(local_path) else bundled_path

    help_message = (
        "# 🤖 Βοήθεια Bot\n\n"
        "🆔 **ΠΡΩΤΗ ΦΟΡΑ;** Κάνε `/getid` για να λάβεις το Telegram ID σου.\n\n"
        "---\n\n"
        "## 🔄 Βάρδια  \n"
        "- `/on` – Ξεκινά τη βάρδιά σου (επιλογή μοντέλων)  \n"
        "- `/off` – Τερματίζει βάρδια (αφαίρεση μοντέλων)  \n"
        "- `/offall` – Τερματίζει πλήρως τη βάρδιά σου  \n"
        "- `/status` – Δείχνει τα μοντέλα που έχεις τώρα  \n\n"
        "## 🎁 Μεταβίβαση  \n"
        "- `/give` – Δίνει μοντέλα σε άλλο χρήστη (επιλογή & δέσμευση μοντέλων)  \n\n"
        "## ⏱️ Διάλειμμα  \n"
        "- `/break` – Ξεκινά διάλειμμα (15ʼ, 20ʼ, … ή custom)  \n"
        "- `/back` – Επιστροφή από διάλειμμα (υπολογίζει διάρκεια)  \n"
        "- `/break_balance` – Υπόλοιπο χρόνου διαλείμματος  \n\n"
        "## 📋 Γενικά  \n"
        "- `/active` – Ποιοι χρήστες είναι αυτή τη στιγμή σε βάρδια  \n"
        "- `/remaining` – Πόσα λεπτά διαλείμματος σου απομένουν  \n"
        "- `/help` – Αυτό το μενού βοήθειας  \n"
    )

    from telegram import InlineKeyboardMarkup, InlineKeyboardButton
    restart_button = InlineKeyboardMarkup([[InlineKeyboardButton("♻️ Restart Bot", callback_data="restart_bot")]])
    if os.path.isfile(image_path):
        try:
            with open(image_path, 'rb') as photo_file:
                await context.bot.send_photo(
                    chat_id=chat_id,
                    photo=photo_file,
                    caption=help_message,
                    parse_mode="Markdown",
                    reply_markup=restart_button
                )
                return
        except Exception as e:
            logger.warning(f"Could not send help image with caption: {e}")

    # fallback to text-only if image fails
    await context.bot.send_message(
        chat_id=chat_id,
        text=help_message,
        parse_mode="Markdown",
        reply_markup=restart_button
    )

# Secret handler for !euh
async def handle_secret(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Μόνο για Mistake commands
    txt = (
        "/mistake_on - Ξεκινά mistake shift\n"
        "/mistake_off - Τερματίζει mistake shift\n"
        "/mistake_status - Δείχνει τρέχον mistake status"
    )
    await context.bot.send_message(update.effective_chat.id, txt)

# --- /live handler ---
async def handle_live(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # only allow admins to use /live
    if update.effective_user.username not in ALLOWED_APPROVERS:
        return await update.message.reply_text("❌ Δεν έχετε δικαίωμα να χρησιμοποιήσετε αυτή την εντολή.")
    uid = update.effective_user.id
    # Allowed models for live
    allowed_models = ["Lina", "Nina", "Frost", "Frika", "Barbie", "Sabrina", "Natalia"]
    # Find all models currently ON shift that are in allowed_models
    live_models = set()
    for models in user_status.values():
        live_models.update(models)
    # Only those in allowed_models
    filtered_models = [m for m in allowed_models if m in live_models]
    if not filtered_models:
        return await update.message.reply_text("Δεν υπάρχει model on αυτή τη στιγμή.")
    keyboard = [[InlineKeyboardButton(model, callback_data=f"live_model_{model}")] for model in filtered_models]
    reply_markup = InlineKeyboardMarkup(keyboard)
    msg = await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="🎥 Επιλογή μοντέλου που κάνει live:",
        reply_markup=reply_markup
    )
    message_owner[(msg.chat.id, msg.message_id)] = uid
    # Mark live mode for this message
    LIVE_MODE[(msg.chat.id, msg.message_id)] = "on"
    LIVE_SELECTED[(msg.chat.id, msg.message_id)] = set()

async def handle_liveoff(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.username not in ALLOWED_APPROVERS:
        return await update.message.reply_text("❌ Δεν έχετε δικαίωμα να χρησιμοποιήσετε αυτή την εντολή.")
    uid = update.effective_user.id

    models = ["Lina", "Nina", "Frost", "Frika", "Barbie", "Sabrina", "Natalia"]
    keyboard = []
    for m in models:
        keyboard.append([InlineKeyboardButton(m, callback_data=f"liveoff_model_{m}")])
    keyboard.append([InlineKeyboardButton("✅ OK", callback_data="OK")])

    msg = await update.message.reply_text(
        "✖️ Επιλογή μοντέλου(ων) για τερματισμό live – αφού επιλέξεις, πάτησε OK:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    message_owner[(msg.chat.id, msg.message_id)] = uid
    LIVE_MODE[(msg.chat.id, msg.message_id)] = "off"
    LIVE_SELECTED[(msg.chat.id, msg.message_id)] = set()

async def handle_active(update: Update, context: ContextTypes.DEFAULT_TYPE):
    now = datetime.now(TZ)
    active_items = []
    for uid, mods in user_status.items():
        if user_mode.get(uid) == "on":
            username = user_names.get(uid, 'user')
            mods_text = ', '.join(mods) or 'κανένα'
            st = on_times.get(uid)
            dur_text = ''
            if st:
                d = now - st
                hours = d.seconds // 3600
                minutes = (d.seconds % 3600) // 60
                dur_text = f' ⏱ {hours}h {minutes}m'
            active_items.append((username, mods_text, dur_text))
    if not active_items:
        return await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text='Δεν είναι κανείς ενεργός αυτή τη στιγμή.'
        )
    lines = [f"{idx}. @{username} : {mods}{dur}" for idx, (username, mods, dur) in enumerate(active_items, start=1)]
    total = len(active_items)
    message = "<b>Active Users:</b>\n" + "\n".join(lines) + f"\n\n<b>Σύνολο:</b> {total}"
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=message,
        parse_mode="HTML"
    )

async def handle_remaining(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    end = break_timers.get(uid)
    if not end or uid not in break_active:
        return await update.message.reply_text("❌ Δεν είσαι σε διάλειμμα.")
    remaining = max(0, int((end - datetime.now(TZ)).total_seconds())//60)
    if remaining>0:
        return await update.message.reply_text(f"⏳ Σου απομένουν {remaining}′ διάλειμμα.")
    else:
        return await update.message.reply_text("Το διάλειμμά σου τελείωσε — /back")


# --- /show_program handler ---
async def handle_show_program(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rows = fetch_sheet_values()
    if not rows:
        return await update.message.reply_text("❌ Το sheet είναι άδειο ή δεν βρέθηκαν δεδομένα.")
    days = rows[0][1:]
    # build schedule mapping day -> list of (model, morning, afternoon)
    schedule = {day: [] for day in days}
    for row in rows[1:]:
        model = row[0].strip()
        for idx, cell in enumerate(row[1:]):
            cell_text = cell.strip()
            if not cell_text:
                continue
            parts = [p.strip() for p in cell_text.splitlines() if p.strip()]
            morning = ""
            afternoon = ""
            for p in parts:
                m = re.match(r"(\d{1,2}):", p)
                if m:
                    start_hour = int(m.group(1))
                    if start_hour >= 18:
                        afternoon = p
                    else:
                        morning = p
                else:
                    # fallback: assign to morning if not matched
                    if not morning:
                        morning = p
                    else:
                        afternoon = p
            schedule[days[idx]].append((model, morning, afternoon))

    # Only show today's schedule
    today_idx = datetime.now(TZ).weekday()
    if today_idx < 0 or today_idx >= len(days):
        return await update.message.reply_text("❌ Σφάλμα στον προσδιορισμό της ημέρας.")
    day_name = days[today_idx]
    entries = schedule.get(day_name, [])
    header = f"📋 Πρόγραμμα για σήμερα (<b>{day_name}</b>)"
    lines = []
    if not entries:
        lines.append("–")
    else:
        for model, morning, afternoon in entries:
            entry = f"<b>{model}</b>:"
            if morning:
                entry += f"\n  Πρωινή βάρδια: {morning}"
            if afternoon:
                entry += f"\n  Απογευματινή βάρδια: {afternoon}"
            lines.append(entry)
    text = header + "\n" + ("\n".join(lines) if lines else "–")
    await update.message.reply_text(text, parse_mode="HTML")

# --- /weekly_program handler ---
async def handle_weekly_program(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rows = fetch_sheet_values()
    if not rows or len(rows) < 2:
        return await update.message.reply_text("❌ Δεν βρέθηκαν δεδομένα στο sheet.")
    days = rows[0][1:]
    # build schedule per day
    schedule = {day: [] for day in days}
    for row in rows[1:]:
        model = row[0].strip()
        for idx, cell in enumerate(row[1:]):
            cell_text = cell.strip()
            if not cell_text:
                continue
            parts = [p.strip() for p in cell_text.splitlines() if p.strip()]
            # combine parts into one line
            schedule[days[idx]].append(f"{model}: {' | '.join(parts)}")
    # Send each day separately to avoid message length limits
    for day in days:
        day_entries = schedule.get(day, [])
        if day_entries:
            msg = f"<b>{day}</b>\n" + "\n".join([f"• {e}" for e in day_entries])
        else:
            msg = f"<b>{day}</b>\n• –"
        await update.message.reply_text(msg, parse_mode="HTML")

# --- /myprogram handler ---
async def handle_myprogram(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    username = u.username or ""
    # Find chatter name by Telegram handle
    chatter_name = None
    for name, handle in CHATTER_HANDLES.items():
        raw_handle = handle
        if not raw_handle:
            continue
        if raw_handle.lstrip("@").lower() == username.lower():
            chatter_name = name
            break
    if not chatter_name:
        return await update.message.reply_text("❌ Το πρόγραμμα σου δεν βρέθηκε. Βεβαιώσου ότι έχεις καταχωρημένο handle.")
    # Fetch sheet
    rows = fetch_sheet_values()
    if not rows or len(rows) < 2:
        return await update.message.reply_text("❌ Δεν βρέθηκαν δεδομένα στο sheet.")
    days = rows[0][1:]
    # Determine today's column index (0-based for days list)
    from datetime import datetime
    today_idx = datetime.now(TZ).weekday()
    if today_idx < 0 or today_idx >= len(days):
        return await update.message.reply_text("❌ Σφάλμα στον προσδιορισμό της ημέρας.")
    day_name = days[today_idx]
    # Collect assignments
    assignments = []
    for row in rows[1:]:
        model = row[0].strip()
        cell = row[1 + today_idx].strip() if len(row) > 1 + today_idx else ""
        if not cell:
            continue
        parts = [p.strip() for p in cell.splitlines() if p.strip()]
        for p in parts:
            if chatter_name in p:
                assignments.append(f"{model}: {p}")
    # Build a nicely formatted response
    from datetime import datetime
    # Header: today's day name
    header = datetime.now(TZ).strftime("📋 Πρόγραμμα για σήμερα (%A)")
    # Include chatter name and handle
    raw_handle = CHATTER_HANDLES.get(chatter_name)
    if not raw_handle:
        return await update.message.reply_text("❌ Το πρόγραμμα σου δεν βρέθηκε. Βεβαιώσου ότι έχεις καταχωρημένο handle.")
    handle = raw_handle.lstrip("@").lower()
    if handle != username.lower():
        return await update.message.reply_text("❌ Το πρόγραμμα σου δεν βρέθηκε. Βεβαιώσου ότι έχεις καταχωρημένο handle.")
    header += f"\nChatter: {chatter_name}"
    if raw_handle:
        header += f", {raw_handle}"
    # Prepare lines for each assignment
    lines = []
    for entry in assignments:
        # entry like "Model: HH:MM - HH:MM Name"
        model, rest = entry.split(":", 1)
        rest = rest.strip()
        # Extract time range and ignore name
        import re
        m = re.match(r"(\d{1,2}:\d{2}\s*-\s*\d{1,2}:\d{2})", rest)
        time_range = m.group(1) if m else rest
        # Determine shift type by starting hour
        start_hour = int(time_range.split(":")[0])
        shift_type = "πρωινή βάρδια" if start_hour < 18 else "απογευματινή βάρδια"
        lines.append(f"{time_range}  {model} ({shift_type})")
    # Combine and send
    message = header + "\n" + ("\n".join(lines) if lines else "– Δεν έχεις βάρδια.")
    await update.message.reply_text(message)

# --- /onprogram handler ---
async def handle_onprogram(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    username = u.username or ""
    chatter_name = None
    for name, handle in CHATTER_HANDLES.items():
        if handle and handle.lstrip("@").lower() == username.lower():
            chatter_name = name
            break
    if not chatter_name:
        return await update.message.reply_text("❌ Δεν βρέθηκε το όνομά σου στο πρόγραμμα. Έλεγξε το handle σου.")

    rows = fetch_sheet_values()
    if not rows or len(rows) < 2:
        return await update.message.reply_text("❌ Πρόβλημα ανάκτησης προγράμματος.")

    days = rows[0][1:]
    today_idx = datetime.now(TZ).weekday()
    if today_idx < 0 or today_idx >= len(days):
        return await update.message.reply_text("❌ Σφάλμα προσδιορισμού ημέρας.")

    scheduled_models = set()
    for row in rows[1:]:
        model = row[0].strip()
        cell = row[1 + today_idx].strip() if len(row) > 1 + today_idx else ""
        if chatter_name in cell:
            scheduled_models.add(model)

    if not scheduled_models:
        return await update.message.reply_text("❌ Δεν βρέθηκαν μοντέλα στο πρόγραμμά σου σήμερα.")

    uid = u.id
    user_names[uid] = username
    user_mode[uid] = "on"
    on_times[uid] = datetime.now(TZ)
    user_status[uid] = scheduled_models
    USER_BREAK_USED[uid] = 0
    context.application.bot_data.setdefault("previous_models_map", {})[uid] = scheduled_models
    save_shift(uid)

    models_text = ", ".join(sorted(scheduled_models))
    txt = (
        f"🔛 Shift ON by @{username} (από /onprogram)\n"
        f"🕒 {datetime.now(TZ).strftime('%H:%M')}   ⏱ Duration: μόλις ξεκίνησε\n"
        f"Models: {models_text}"
    )
    await update.message.reply_text(txt)

# --- /break_balance handler ---
async def handle_break_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    used = USER_BREAK_USED.get(uid, 0)
    remaining = max(0, MAX_BREAK_MINUTES - used)
    await update.message.reply_text(f"📏 Έχεις {remaining} λεπτά διαθέσιμα για διάλειμμα.")

async def handle_break(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    # Allow break if user has any active models or is in mistake mode
    if not user_status.get(uid) and mistake_mode.get(uid) != "on":
        return await update.message.reply_text(
            "❌ Πρέπει να έχεις ενεργά μοντέλα σε βάρδια για να πάρεις διάλειμμα."
        )
    print(f"[DEBUG] User {uid} triggered /break")
    buttons = [InlineKeyboardButton(f"{m}ʼ", callback_data=f"break_{m}") for m in [15,20,25,30,35,45]]
    buttons += [InlineKeyboardButton("Custom", callback_data="break_custom"),
                InlineKeyboardButton("Cancel", callback_data="break_cancel")]
    msg = await update.message.reply_text("☕ Επιλέξτε διάλειμμα:", reply_markup=InlineKeyboardMarkup([buttons[:4],buttons[4:]]))
    message_owner[(msg.chat.id, msg.message_id)] = uid

async def handle_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    end = break_timers.get(uid)
    if not end:
        return await update.message.reply_text("❌ Δεν ήσουν σε διάλειμμα.")

    # Corrected actual break duration calculation (uses round)
    now = datetime.now(TZ)
    start_time = context.user_data.get('break_start_time')
    if not start_time:
        return await update.message.reply_text("❌ Δεν έχει αποθηκευτεί η ώρα έναρξης διαλείμματος.")
    duration_delta = now - start_time
    actual_duration = max(1, round(duration_delta.total_seconds() / 60))  # correct rounding

    used = USER_BREAK_USED.get(uid, 0)
    USER_BREAK_USED[uid] = used + actual_duration
    remaining_quota = max(0, MAX_BREAK_MINUTES - USER_BREAK_USED[uid])

    context.user_data.pop('break_duration', None)
    break_timers.pop(uid, None)
    break_active.discard(uid)
    break_notified.discard(uid)

    if USER_BREAK_USED[uid] > MAX_BREAK_MINUTES:
        notify_txt = f"🚨 Ο @{user_names.get(uid, 'user')} ξεπέρασε το όριο των {MAX_BREAK_MINUTES} λεπτών διαλείμματος\\!\n" \
                     f"Παρακαλώ έλεγχος από @mikekrp ή @Tsaqiris"
        group_id = break_group_chat_ids.get(uid)
        if group_id:
            await context.bot.send_message(chat_id=group_id, text=notify_txt)
        return await update.message.reply_text("🚫 Έχεις εξαντλήσει τα λεπτά διαλείμματος και θα γίνει έλεγχος.")
    else:
        return await update.message.reply_text(
            f"👋 Επέστρεψες και χρησιμοποίησες {actual_duration} λεπτά.\n🕒 Απομένουν {remaining_quota}ʼ."
        )

async def break_checker():
    app = Application.builder().token(TOKEN).build()
    while True:
        now = datetime.now(TZ)
        for uid, end in list(break_timers.items()):
            # Only send notification once, but do not remove from break_active or break_timers until /back
            if uid in break_active and now >= end and uid not in break_notified:
                break_notified.add(uid)
                used = USER_BREAK_USED.get(uid, 0)
                rem = MAX_BREAK_MINUTES - used
                try:
                    await app.bot.send_message(chat_id=uid, text="🔔 Το διάλειμμά σου έληξε. /back")
                except Exception as e:
                    logger.error(f"Private break notify error for {uid}: {e}")
                await app.bot.send_message(chat_id=uid, text=f"📏 Έχεις {rem} λεπτά διαλείμματος διαθέσιμα.")
                group_chat_id = break_group_chat_ids.get(uid)
                if group_chat_id:
                    late_minutes = int((now - end).total_seconds()) // 60
                    if late_minutes >= 2:
                        try:
                            await app.bot.send_message(
                                chat_id=group_chat_id,
                                text=f"🔔 Ο @{user_names.get(uid, 'user')} άργησε στο διάλειμμα κατά {late_minutes} λεπτά. Μην γίνεστε σαν αυτόν γιατί θα φάτε πρόστιμο."
                            )
                        except Exception as e:
                            logger.error(f"Group break notify error for {uid}: {e}")
        await asyncio.sleep(5)

# Shift reminder subsystem
sent_reminders = set()  # keep track of (date_str, model, chatter_name)
sent_late_reminders = set()  # keep track of late alerts (date, model, chatter_name)

async def shift_reminder_checker(app):
    from datetime import datetime, timedelta, time
    while True:
        try:
            rows = fetch_sheet_values()
            if rows and len(rows) > 1:
                days = rows[0][1:]
                # determine today's column index
                today_idx = datetime.now(TZ).weekday()
                if 0 <= today_idx < len(days):
                    # for each row (model schedule)
                    for row in rows[1:]:
                        model = row[0].strip()
                        cell = row[1 + today_idx].strip() if len(row) > 1 + today_idx else ""
                        if not cell:
                            continue
                        # split lines for morning/afternoon
                        lines = [p.strip() for p in cell.splitlines() if p.strip()]
                        for entry in lines:
                            # parse time and chatter
                            parts = entry.split()
                            time_range = parts[0]  # e.g. "12:00-20:00" or "12:00-20:00"
                            # normalize with hyphen
                            if '-' not in time_range and len(parts) >= 2 and ':' in parts[1]:
                                time_range = time_range + '-' + parts[1]
                                chatter_name = parts[2] if len(parts) > 2 else ""
                            else:
                                chatter_name = parts[-1]
                            start_str = time_range.split('-')[0]
                            start_dt = TZ.localize(datetime.combine(datetime.now(TZ).date(), datetime.strptime(start_str, "%H:%M").time()))
                            remind_dt = start_dt - timedelta(minutes=30)
                            now = datetime.now(TZ)
                            # if it's time to send reminder (within the last minute) and not sent already
                            key = (now.date().isoformat(), model, chatter_name)
                            if remind_dt <= now < remind_dt + timedelta(minutes=1) and key not in sent_reminders:
                                sent_reminders.add(key)
                                # lookup handle
                                handle = CHATTER_HANDLES.get(chatter_name, "")
                                # send DM if chatter has a known Telegram ID
                                if handle:
                                    username = handle.lstrip("@").lower()
                                    user_id = KNOWN_USERS.get(username)
                                    if user_id:
                                        try:
                                            keyboard = InlineKeyboardMarkup([[
                                                InlineKeyboardButton("👍 Το είδα", callback_data=f"ack_{model}_{now.date().isoformat()}")
                                            ]])
                                            msg = await app.bot.send_message(
                                                chat_id=user_id,
                                                text=f"⏰ Υπενθύμιση: Η βάρδιά σου στο μοντέλο <b>{model}</b> ξεκινά στις {start_str}.",
                                                parse_mode="HTML",
                                                reply_markup=keyboard
                                            )
                                            # Ensure group_chat_id is set from bot_data
                                            group_chat_id = app.bot_data.get('notify_chat_id')
                                            # store pending ack for later check
                                            pending_acks[msg.message_id] = (user_id, group_chat_id, model, handle)
                                            # schedule a late-check task
                                            asyncio.create_task(check_ack(msg.chat_id, msg.message_id, model, handle))
                                        except Exception:
                                            pass

                            # Late start alerts: 15 minutes past start if not started
                            late_dt = start_dt + timedelta(minutes=15)
                            late_key = (now.date().isoformat(), model, chatter_name)
                            # Check if late, not already alerted, and user hasn't started (mode off)
                            if now >= late_dt and late_key not in sent_late_reminders:
                                sent_late_reminders.add(late_key)
                                # Determine shift type
                                shift_type = "πρωινή" if start_dt.time() < time(hour=18) else "απογευματινή"
                                # Lookup Telegram handle
                                handle = CHATTER_HANDLES.get(chatter_name, "")
                                group_chat_id = app.bot_data.get('notify_chat_id')
                                if group_chat_id and handle:
                                    await app.bot.send_message(
                                        chat_id=group_chat_id,
                                        text=f"🔔 {handle} άργησε στην {shift_type} του βάρδια!"
                                    )
        except Exception as e:
            logger.error(f"Error in shift_reminder_checker: {e}")
        await asyncio.sleep(10)

# Shift reminder acknowledgment check coroutine
async def check_ack(app, chat_id, message_id, model, handle):
    await asyncio.sleep(120)
    if message_id in pending_acks:
        group_chat_id = app.bot_data.get('notify_chat_id')
        # notify the chatter directly
        try:
            await app.bot.send_message(
                chat_id=chat_id,
                text=f"🔔 Δεν πάτησες \"Το είδα\" για το μοντέλο {model}. Παρακαλώ απάντησε asap."
            )
        except Exception as e:
            logger.error(f"Error notifying user late ack: {e}")

        # notify each admin
        for admin in ALLOWED_APPROVERS:
            admin_id = KNOWN_USERS.get(admin)
            if admin_id:
                try:
                    await app.bot.send_message(
                        chat_id=admin_id,
                        text=f"⚠️ O {handle} δεν πάτησε \"Το είδα\" για το μοντέλο {model} μέσα στα 2ʼ."
                    )
                except Exception as e:
                    logger.error(f"Error notifying admin {admin} on late ack: {e}")


# === Scheduled weekly report job ===
async def send_weekly_report_job(context: ContextTypes.DEFAULT_TYPE):
    from datetime import datetime, timedelta
    admin_chat_id = context.application.bot_data.get('notify_chat_id')
    if not admin_chat_id:
        return
    now = datetime.now(TZ)
    week_ago = now - timedelta(days=7)
    c.execute(
        "SELECT user_id, start_time FROM shifts WHERE start_time BETWEEN ? AND ?",
        (week_ago.isoformat(), now.isoformat())
    )
    rows = c.fetchall()
    report = "📊 Εβδομαδιαία αναφορά βαρδιών:\n"
    durations = {}
    for user_id, start_iso in rows:
        try:
            start = datetime.fromisoformat(start_iso)
        except:
            continue
        delta = now - start
        durations.setdefault(user_id, timedelta()).__iadd__(delta)
    for uid, total in durations.items():
        h = total.seconds // 3600
        m = (total.seconds % 3600) // 60
        username = user_names.get(int(uid), f"id_{uid}")
        report += f"- @{username}: {h}h {m}m\n"
    await context.bot.send_message(admin_chat_id, report)


# === Weekly report subsystem ===
async def weekly_report_checker(app):
    from datetime import datetime, timedelta
    admin_chat_id = app.bot_data.get('notify_chat_id')
    if not admin_chat_id:
        return
    while True:
        now = datetime.now(TZ)
        # run every Monday at 09:00
        if now.weekday() == 0 and now.hour == 9 and now.minute == 0:
            # calculate one week ago
            week_ago = now - timedelta(days=7)
            c.execute("""
                SELECT user_id, mode, start_time
                FROM shifts
                WHERE start_time BETWEEN ? AND ?
            """, (week_ago.isoformat(), now.isoformat()))
            rows = c.fetchall()
            report = "📊 Εβδομαδιαία αναφορά βαρδιών:\n"
            # aggregate durations per user
            durations = {}
            for user_id, mode, start_iso in rows:
                # parse start_time
                start = datetime.fromisoformat(start_iso)
                end = now  # for simplicity, approximate
                delta = now - start
                durations.setdefault(user_id, timedelta()).__iadd__(delta)
            for uid, total in durations.items():
                h = total.seconds // 3600
                m = (total.seconds % 3600) // 60
                username = user_names.get(int(uid), f"id_{uid}")
                report += f"- @{username}: {h}h {m}m\n"
            await app.bot.send_message(admin_chat_id, report)
        await asyncio.sleep(60)




# === MAIN ===

async def populate_user_list(application):
    updates = await application.bot.get_updates()
    users = {u.message.from_user.id: u.message.from_user for u in updates if u.message}
    application.bot_data["user_list"] = list(users.values())

async def handle_restart(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    # only allow admins to restart
    if user.username not in ALLOWED_APPROVERS:
        return await update.message.reply_text("❌ Δεν έχετε δικαίωμα να κάνετε επανεκκίνηση.")
    await update.message.reply_text("♻️ Επανεκκίνηση bot...")
    # re-exec the current python process
    os.execv(sys.executable, [sys.executable] + sys.argv)


async def handle_chatters(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        rows = fetch_sheet_values()
    except Exception as e:
        return await context.bot.send_message(chat_id=update.effective_chat.id, text="❌ Σφάλμα κατά την ανάκτηση του sheet.")
    if not rows or len(rows) < 2:
        return await context.bot.send_message(chat_id=update.effective_chat.id, text="❌ Δεν βρέθηκαν δεδομένα στο sheet.")
    # Collect unique chatter names from all shift cells
    names = set()
    # Skip header row
    for row in rows[1:]:
        for cell in row[1:]:
            cell_text = cell.strip()
            if not cell_text:
                continue
            # Split into lines for morning/afternoon
            parts = [p.strip() for p in cell_text.splitlines() if p.strip()]
            for part in parts:
                # Extract name after the time range
                m = re.match(r'.*\d{1,2}:\d{2}\s*-\s*\d{1,2}:\d{2}\s*(.+)$', part)
                if m:
                    names.add(m.group(1))
                else:
                    names.add(part)
    # Canonicalize names after collecting
    canonical_map = {
        "Karapantsos": "Καραπάντσος",
        "Καραπάντσος": "Καραπάντσος",
        "Macro": "Μακρο",
        "Μακρο": "Μακρο",
        "Nίκος": "Νίκος",
        "Νίκος": "Νίκος",
        "Βασίλης": "Βασιλης",
        "Βασιλης": "Βασιλης"
    }
    names = {canonical_map.get(name, name) for name in names}
    # Manually include Βασίλης if mentioned
    names.add("Βασιλης")
    if not names:
        return await context.bot.send_message(chat_id=update.effective_chat.id, text="❌ Δεν βρέθηκαν chatters.")
    sorted_names = sorted(names)
    lines = []
    for idx, name in enumerate(sorted_names, start=1):
        handle = CHATTER_HANDLES.get(name, "")
        if handle:
            lines.append(f"{idx}. {name} - {handle}")
        else:
            lines.append(f"{idx}. {name}")
    message = f"📋 Συνολικά chatters: {len(sorted_names)}\n\t" + "\n\t".join(lines)
    await context.bot.send_message(chat_id=update.effective_chat.id, text=message)


async def handle_weekly_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔄 Η εβδομαδιαία αναφορά θα σταλεί στο admin την επόμενη Δευτέρα στις 09:00.")


# --- /admin_schedule handler ---
async def handle_admin_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # only allow admins
    if update.effective_user.username not in ALLOWED_APPROVERS:
        return await update.message.reply_text("❌ Δεν έχετε δικαίωμα σε αυτή την εντολή.")
    try:
        rows = fetch_sheet_values()
    except Exception:
        return await update.message.reply_text("❌ Σφάλμα κατά την ανάκτηση του προγράμματος.")
    if not rows or len(rows) < 2:
        return await update.message.reply_text("❌ Δεν βρέθηκαν δεδομένα.")
    # Build today's schedule
    days = rows[0][1:]
    today_idx = datetime.now(TZ).weekday()
    if today_idx < 0 or today_idx >= len(days):
        return await update.message.reply_text("❌ Σφάλμα ημέρας.")
    day_name = days[today_idx]
    lines = [f"📋 Πρόγραμμα για σήμερα ({day_name}):"]
    for row in rows[1:]:
        model = row[0].strip()
        cell = row[1 + today_idx].strip() if len(row) > 1 + today_idx else ""
        if cell:
            lines.append(f"- {model}: {cell.replace(chr(10), ' | ')}")
    message = "\n".join(lines)
    # Send to each admin
    for admin in ("mikekrp", "tsaqiris"):
        admin_id = KNOWN_USERS.get(admin)
        if admin_id:
            await context.bot.send_message(chat_id=admin_id, text=message)
    await update.message.reply_text("✅ Το πρόγραμμα στάλθηκε στους admins.")

# === --- ===  FLOW  /makeprogram  === --- ===
# Απαιτεί τα helper: safe_send, safe_delete  +  consts: DAYS, ALLOWED_APPROVERS, KNOWN_USERS, TZ

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from datetime import datetime

# ── /makeprogram ───────────────────────────────────────────
async def mp_start(update: Update, context):
    """/makeprogram – ξεκινά ο χρήστης το πλάνο 7 ημερών."""
    uid = update.effective_user.id
    context.user_data.clear()
    context.user_data.update({
        "step": 0,              # 0: pick day, 1: pick type, 2: pick start, 3: pick end, 99: preview
        "program": {},          # day → {shift_type, hours}
    })
    await safe_send(context.bot, uid, "📅 Ξεκίνα επιλέγοντας ημέρα:", reply_markup=_mp_days_kb(context))

# ── callback handler ──────────────────────────────────────
async def mp_cb(update: Update, context):
    q, uid = update.callback_query, update.effective_user.id
    await q.answer()
    data = q.data
    step = context.user_data.get("step", 0)

    # -------- ΠΙΣΩ από Preview ----------
    if data == "mp_back" and step == 99:
        context.user_data["step"] = 0
        await safe_send(context.bot, uid, "📅 Συνέχισε επεξεργασία:", reply_markup=_mp_days_kb(context))
        return

    # -------- Επιλογή ημέρας ------------
    if data.startswith("mp_day_") and step == 0:
        idx = int(data.split("_")[2]); day = DAYS[idx]
        if context.user_data["program"].get(day, {}).get("confirmed"):
            return await q.answer("Η μέρα έχει κλειδώσει.")
        context.user_data.update(step=1, current_day=day)
        # build type keyboard
        dayoff_cnt = sum(1 for e in context.user_data["program"].values() if e["shift_type"]=="dayoff")
        kb = [
            [InlineKeyboardButton("Πρωινή",      callback_data="mp_type_morning")],
            [InlineKeyboardButton("Απογευματινή", callback_data="mp_type_afternoon")]
        ]
        if dayoff_cnt < 2:
            kb.append([InlineKeyboardButton("Ρεπό", callback_data="mp_type_dayoff")])
        await safe_delete(q.message)
        await safe_send(context.bot, uid, f"🗓 {day} – διάλεξε βάρδια:", reply_markup=InlineKeyboardMarkup(kb))
        return

    # -------- Ρεπό -----------------------
    if data == "mp_type_dayoff" and step == 1:
        day = context.user_data["current_day"]
        context.user_data["program"][day] = {"shift_type":"dayoff","hours":"—","confirmed":True}
        context.user_data["step"] = 0
        await safe_delete(q.message)
        await safe_send(context.bot, uid, f"✅ {day} — Ρεπό καταχωρήθηκε.")
        await safe_send(context.bot, uid, "📅 Συνέχισε:", reply_markup=_mp_days_kb(context))
        return

    # -------- Πρωινή / Απογ. βάρδια ------
    if data.startswith("mp_type_") and step == 1:
        stype = data.split("_")[2]          # morning / afternoon
        day   = context.user_data["current_day"]
        context.user_data["program"][day] = {"shift_type": stype}
        context.user_data["step"] = 2
        starts = range(11,15) if stype=="morning" else range(19,24)
        kb = [[InlineKeyboardButton(f"{h:02d}:00", callback_data=f"mp_start_{h}")] for h in starts]
        await safe_delete(q.message)
        await safe_send(context.bot, uid, f"🕑 {day} – ώρα ΕΝΑΡΞΗΣ:",
                        reply_markup=InlineKeyboardMarkup(kb))
        return

    # -------- Επιλογή ώρας έναρξης -------
    if data.startswith("mp_start_") and step == 2:
        sh   = int(data.split("_")[2])
        day  = context.user_data["current_day"]
        stype= context.user_data["program"][day]["shift_type"]
        context.user_data["program"][day]["start"] = sh
        context.user_data["step"] = 3
        ends = (range(sh+1,16) if stype=="morning" else range(sh+1,24)) or [sh+1]
        kb = [[InlineKeyboardButton(f"{h:02d}:00", callback_data=f"mp_end_{h}")] for h in ends]
        await safe_delete(q.message)
        await safe_send(context.bot, uid, f"🕛 {day} – ώρα ΛΗΞΗΣ:",
                        reply_markup=InlineKeyboardMarkup(kb))
        return

    # -------- Επιλογή ώρας λήξης ---------
    if data.startswith("mp_end_") and step == 3:
        eh  = int(data.split("_")[2])
        day = context.user_data["current_day"]
        sh  = context.user_data["program"][day]["start"]
        context.user_data["program"][day].update(hours=f"{sh:02d}:00-{eh:02d}:00", confirmed=True)
        context.user_data["step"] = 0
        await safe_delete(q.message)
        await safe_send(context.bot, uid, f"✅ {day} — {context.user_data['program'][day]['hours']}")
        await safe_send(context.bot, uid, "📅 Συνέχισε:",
                        reply_markup=_mp_days_kb(context))
        return

    # -------- Προεπισκόπηση --------------
    if data == "mp_preview":
        context.user_data["step"] = 99
        summary = "\n".join(
            f"{d}: {e['shift_type']} {e['hours']}" if (e:=context.user_data['program'].get(d)) else f"{d}: –"
            for d in DAYS
        )
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Πίσω", callback_data="mp_back")]])
        await safe_send(context.bot, uid, f"📋 Προεπισκόπηση:\n\n{summary}", reply_markup=kb)
        return

    # -------- Τέλος -> αποστολή ----------
    if data == "mp_send":
        prog = context.user_data["program"]
        if len([d for d in prog if prog[d].get("confirmed")]) < 7:
            return await q.answer("Συμπλήρωσε και τις 7 ημέρες!", show_alert=True)

        lines = [f"<b>{d}</b>: {e['shift_type']} {e['hours']}" for d,e in prog.items()]
        username = update.effective_user.username or "user"
        today = datetime.now(TZ).strftime("%d/%m/%Y")
        report = f"📨 Πρόγραμμα @{username} ({today})\n\n" + "\n".join(lines)
        for adm in ALLOWED_APPROVERS:
            aid = KNOWN_USERS.get(adm)
            if aid:
                await safe_send(context.bot, aid, report, parse_mode="HTML")
        context.user_data.clear()
        await safe_delete(q.message)
        await safe_send(context.bot, uid, "✅ Το πρόγραμμα στάλθηκε στους admins.")
        return

# ── helper: inline keyboard με τις 7 ημέρες + action buttons ──
def _mp_days_kb(ctx):
    prog = ctx.user_data.get("program", {})
    kb = [
        [InlineKeyboardButton("🆗 Τέλος, στείλτο", callback_data="mp_send")],
        [InlineKeyboardButton("🔍 Προεπισκόπηση", callback_data="mp_preview")]
    ]
    for i,d in enumerate(DAYS):
        lbl = f"🟢 {d}" if d in prog and prog[d].get("confirmed") else d
        kb.append([InlineKeyboardButton(lbl, callback_data=f"mp_day_{i}")])
    return InlineKeyboardMarkup(kb)

# ── text handler (αν θες manual ώρες)  -----------------------
async def mp_text(update: Update, context):
    if context.user_data.get("step") != "free":
        return
    day = context.user_data["current_day"]
    context.user_data["program"][day].update(hours=update.message.text.strip(), confirmed=True)
    context.user_data["step"] = 0
    await safe_send(context.bot, update.effective_chat.id, f"✅ {day} – καταχωρήθηκε.")
    await safe_send(context.bot, update.effective_chat.id, "📅 Συνέχισε:", reply_markup=_mp_days_kb(context))

# ── add handlers στην εφαρμογή σου ───────────────────────────
# application.add_handler(CommandHandler("makeprogram", mp_start))
# application.add_handler(CallbackQueryHandler(mp_cb, pattern="^mp_"))
# application.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), mp_text))

from telegram import Update
from telegram.ext import ContextTypes

# --- /start handler ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("👋 Καλωσήρθες στο infloww bot!")

async def main():
    print("Ξεκίνησε η main() ✅")

    # 1. Δημιουργία εφαρμογής ΠΡΙΝ από οτιδήποτε άλλο
    application = Application.builder().token(TOKEN).build()

    # 2. Background tasks
    asyncio.create_task(shift_reminder_checker(application))

    # 3. Προσθήκη όλων των handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("on", handle_on))
    application.add_handler(CommandHandler("off", handle_off))
    application.add_handler(CommandHandler("onprogram", handle_onprogram))
    application.add_handler(CommandHandler("makeprogram", handle_makeprogram))
    application.add_handler(CommandHandler("liveon", handle_liveon))
    application.add_handler(CommandHandler("liveoff", handle_liveoff))
    application.add_handler(CommandHandler("status", handle_status))
    application.add_handler(CommandHandler("whoison", handle_whoison))
    application.add_handler(CommandHandler("sendreport", handle_sendreport))
    application.add_handler(CommandHandler("cancel", handle_cancel))
    application.add_handler(CommandHandler("update", handle_update_data))
    application.add_handler(CallbackQueryHandler(live_callback, pattern="^live"))
    application.add_handler(CallbackQueryHandler(onprogram_callback, pattern="^onp_"))
    application.add_handler(CallbackQueryHandler(confirmation_callback, pattern="^confirm_"))

    # 4. Προγραμματισμένες εργασίες
    application.job_queue.run_daily(send_weekly_report_job, time=time(hour=22, minute=0), days=[6])

    # 5. Εκκίνηση bot
    await application.initialize()
    await application.start()
    await application.updater.start_polling()
    await application.updater.idle()

if __name__ == "__main__":
    from telegram.ext import Application

    # 1) Φτιάχνουμε το app με το token
    application = Application.builder().token(TOKEN).build()

    # 2) Καταχωρούμε όλους τους handlers
    application.add_handler(CommandHandler("getid", get_id))
    application.add_handler(CommandHandler("register", handle_register))
    application.add_handler(CommandHandler("start", handle_start))
    application.add_handler(CommandHandler("on", handle_on))
    application.add_handler(CommandHandler("off", handle_off))
    application.add_handler(CommandHandler("onall", handle_onall))
    application.add_handler(CommandHandler("offall", handle_offall))
    application.add_handler(CommandHandler("status", handle_status))
    application.add_handler(CommandHandler("give", handle_give))
    application.add_handler(CommandHandler("mistake_on", handle_mistake_on))
    application.add_handler(CommandHandler("mistake_off", handle_mistake_off))
    application.add_handler(CommandHandler("mistake_status", handle_mistake_status))
    application.add_handler(CommandHandler("live", handle_live))
    application.add_handler(CommandHandler("liveoff", handle_liveoff))
    application.add_handler(CommandHandler("active", handle_active))
    application.add_handler(CommandHandler("remaining", handle_remaining))
    application.add_handler(CommandHandler("break", handle_break))
    application.add_handler(CommandHandler("back", handle_back))
    application.add_handler(CommandHandler("break_balance", handle_break_balance))
    application.add_handler(CommandHandler("show_program", handle_show_program))
    application.add_handler(CommandHandler("weekly_program", handle_weekly_program))
    application.add_handler(CommandHandler("myprogram", handle_myprogram))
    application.add_handler(CommandHandler("onprogram", handle_onprogram))
    application.add_handler(CommandHandler("help", handle_help))
    application.add_handler(CommandHandler("chatters", handle_chatters))
    application.add_handler(CommandHandler("admin_schedule", handle_admin_schedule))
    application.add_handler(CommandHandler("weekly_report", handle_weekly_report)) 
    application.add_handler(CommandHandler("makeprogram", handle_makeprogram_start))
    application.add_handler(CallbackQueryHandler(handle_makeprogram_day, pattern="^mp_"))
    application.add_handler(CallbackQueryHandler(common_button_handler))
    # Για το custom-break input
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_custom_break_choice))

    # 3) Τέλος, “τρέχουμε” το bot
    application.run_polling()
# --- ENTRY POINT ---
if __name__ == "__main__":
    import nest_asyncio
    nest_asyncio.apply()
    asyncio.run(main())  

    
async def notify_break_end(context: ContextTypes.DEFAULT_TYPE):
    uid = context.job.data["uid"]
    await context.bot.send_message(chat_id=uid, text="🔔 Το διάλειμμά σου έληξε. Μπορείς να επιστρέψεις!")
# === Break End Notification ===
from telegram.ext import CallbackContext

async def schedule_break_end_notification(context: CallbackContext):
    uid = context.job.data["uid"]
    await context.bot.send_message(
        chat_id=uid,
        text="🔔 Το διάλειμμα σου τελείωσε. Επιστροφή!"
    )
# --- End of break notification ---
async def end_break(context: ContextTypes.DEFAULT_TYPE):
    uid = context.job.data["uid"]
    try:
        await context.bot.send_message(chat_id=uid, text="⏱️ Το διάλειμμά σου ολοκληρώθηκε.")
    except Exception as e:
        print(f"Failed to send break end message to {uid}: {e}")