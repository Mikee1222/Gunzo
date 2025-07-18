import os
import asyncio
import nest_asyncio
nest_asyncio.apply()
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from supabase import create_client, Client
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackQueryHandler
import json
from telegram.ext import MessageHandler, filters
import re
import uuid
import requests
import pytz
import csv
import io
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from calendar import monthrange

# === Notification Target ===
TARGET_CHAT_ID = -1002200364773  # t.me/2200364773/25
TARGET_REPLY_TO_MESSAGE_ID = 25   # message ID 25 in that chat

# --- Supabase Setup ---
SUPABASE_URL = "https://cuytywddvbqgdzmnhzou.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImN1eXR5d2RkdmJxZ2R6bW5oem91Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTI3NTIzNDQsImV4cCI6MjA2ODMyODM0NH0.jcbmE2RAYg7xZcR6olB_Tw0dPRISqTjKftsBHt8sH7M"  # Βάλε εδώ το KEY σου
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- Telegram Bot Token ---
TOKEN = "7140433953:AAEOEfkdFM3rkTu-eYn_S9aI3fY_EszkfT8"  # Βάλε εδώ το Telegram token σου

# --- Λίστα με τα models του agency ---
MODELS = [
    "Lydia", "Miss Frost", "Lina", "Frika", "Iris", "Electra", "Nina", "Eirini",
    "Marilia", "Areti", "Silia", "Iwanna", "Elvina", "Stefania", "Elena", "Natalia",
    "Sabrina", "Barbie", "Antwnia", "Κωνσταντίνα Mummy", "Gavriela", "Χριστίνα","Tzwrtzina"
]

# --- Mistake Models List ---
MISTAKE_MODELS = ["Lydia", "Lina", "Nina", "Miss Frost", "Frika", "Electra", "Iris"]

# --- Live Models List ---
LIVE_MODELS = ["Sabrina", "Natalia", "Miss Frost", "Lina", "Nina", "Frika", "Barbie"]

# --- Helper: Debug print for callback_data ---
def dbg_btn(label, callback_data):
    print(f"DEBUG: Creating button '{label}' with callback_data='{callback_data}' (len={len(str(callback_data))})")
    return InlineKeyboardButton(label, callback_data=callback_data)

# --- /start Command ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not update.message:
        return
    # Always upsert user_id and username to Supabase
    if user and user.username:
        try:
            supabase.table("users").upsert({
                "user_id": str(user.id),
                "username": user.username,
                "first_name": user.first_name or ""
            }).execute()
        except Exception as e:
            print(f"DEBUG: Failed to upsert user in /start: {e}")
    await update.message.reply_text("👋 Καλώς ήρθες στο group bot!")

# --- /register Command ---
async def register(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not update.message:
        return
    if user and user.username:
        try:
            supabase.table("users").upsert({
                "user_id": str(user.id),
                "username": user.username,
                "first_name": user.first_name or ""
            }).execute()
            await update.message.reply_text("✅ Εγγράφηκες επιτυχώς στη Supabase!")
        except Exception as e:
            await update.message.reply_text(f"❌ Σφάλμα εγγραφής: {e}")
    else:
        await update.message.reply_text("❌ Πρέπει να έχεις username στο Telegram για να εγγραφείς.")

# --- /on Command ---
async def on_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not update.message:
        return
    user_id = str(update.effective_user.id)
    # Βρες όλα τα models που είναι ήδη on από ΟΛΟΥΣ τους χρήστες (χωρίς εξαίρεση)
    unavailable_models = set()
    try:
        resp = supabase.table("users").select("user_id,models,active").eq("active", True).execute()
        for row in resp.data:
            ms = row.get("models") or []
            if isinstance(ms, str):
                try:
                    import json
                    ms = json.loads(ms)
                except Exception:
                    ms = []
            unavailable_models.update(ms)
    except Exception:
        pass
    # Αρχικοποιούμε το state του session
    selected_models = set()
    # Στέλνουμε το keyboard και κρατάμε το message_id
    sent = await context.bot.send_message(
        chat_id=TARGET_CHAT_ID,
        reply_to_message_id=TARGET_REPLY_TO_MESSAGE_ID,
        text="Επίλεξε τα models σου:",
        reply_markup=build_models_keyboard(selected_models, unavailable_models)
    )
    # Αποθηκεύουμε το session στο chat_data με key το message_id
    if context.chat_data is not None and 'on_sessions' not in context.chat_data:
        context.chat_data['on_sessions'] = {}
    if context.chat_data is not None:
        context.chat_data['on_sessions'][sent.message_id] = {
            'initiator': user_id,
            'selected_models': selected_models,
            'unavailable_models': unavailable_models
        }

def build_models_keyboard(selected, unavailable):
    keyboard = []
    row = []
    for i, model in enumerate(MODELS, 1):
        if model in unavailable:
            row.append(dbg_btn(f"🔒 {model}", "ignore"))
        else:
            checked = "🟢 " if model in selected else ""
            row.append(dbg_btn(f"{checked}{model}", f"model_{model}"))
        if i % 4 == 0 or i == len(MODELS):
            keyboard.append(row)
            row = []
    keyboard.append([dbg_btn("✅ OK", "models_ok"), dbg_btn("❌ Cancel", "cancel_action")])
    return InlineKeyboardMarkup(keyboard)

async def models_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query is None or context.chat_data is None or query.message is None or query.data is None:
        return
    user = query.from_user
    if user is None:
        return
    user_id = str(user.id)
    msg = query.message
    # Βρες το session για το συγκεκριμένο keyboard
    session = context.chat_data.get('on_sessions', {}).get(msg.message_id) if context.chat_data and context.chat_data.get('on_sessions') else None
    if not session:
        await query.answer("Αυτή η επιλογή δεν είναι πλέον ενεργή.", show_alert=True)
        return
    initiator_id = session['initiator']
    selected = session['selected_models']
    unavailable = session['unavailable_models']
    if user_id != initiator_id:
        await query.answer("Δεν έχεις δικαίωμα να κάνεις αυτή την ενέργεια", show_alert=True)
        return
    data = query.data
    if data == "ignore":
        await query.answer("Το μοντέλο αυτή τη στιγμή είναι ήδη on", show_alert=True)
        return
    if data.startswith("model_"):
        model = data[6:]
        if model in unavailable:
            await query.answer("Το model είναι ήδη ενεργό από άλλον χρήστη.", show_alert=True)
            return
        if model in selected:
            selected.remove(model)
        else:
            selected.add(model)
        session['selected_models'] = selected
        # Ενημέρωσε το keyboard
        await query.edit_message_reply_markup(reply_markup=build_models_keyboard(selected, unavailable))
        await query.answer()
    elif data == "models_ok":
        # --- Ανάκτηση προηγούμενων models και start_time ---
        old_models = []
        old_start_time = None
        had_no_models = False
        try:
            resp = supabase.table("users").select("models,start_time").eq("user_id", user_id).execute()
            if resp.data and len(resp.data) > 0:
                old_models = resp.data[0].get("models") or []
                if isinstance(old_models, str):
                    try:
                        import json
                        old_models = json.loads(old_models)
                    except Exception:
                        old_models = []
                if not old_models:
                    had_no_models = True
                # --- Βρες το start_time της βάρδιας από το shifts table ---
                if old_models:
                    shift_resp = supabase.table("shifts").select("start_time").eq("user_id", user_id).eq("mode", "on").order("start_time", desc=True).limit(1).execute()
                    if shift_resp.data and len(shift_resp.data) > 0:
                        old_start_time = shift_resp.data[0].get("start_time")
        except Exception:
            pass
        # --- Υπολογισμός duration ---
        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()
        duration_str = "-"
        # Χρησιμοποιώ τη λογική του /active: duration = now - start_time από το users table
        try:
            resp = supabase.table("users").select("start_time").eq("user_id", user_id).execute()
            start_time = None
            if resp.data and len(resp.data) > 0:
                start_time = resp.data[0].get("start_time")
            if start_time:
                old_dt = datetime.fromisoformat(start_time)
                delta = now - old_dt
                h = int(delta.total_seconds() // 3600)
                m = int((delta.total_seconds() % 3600) // 60)
                duration_str = f"{h}:{m:02d}"
            else:
                duration_str = "0:00"
        except Exception as e:
            print(f"DEBUG: Exception στο duration υπολογισμό (on): {e}")
            duration_str = "0:00"
        # --- Αποθήκευση νέων models και start_time στο users ---
        try:
            # Add logic: ενώσε τα ήδη ενεργά με τα νέα, χωρίς διπλότυπα
            all_models = set(old_models) | set(selected)
            starting_shift = not old_models  # αν δεν είχε καθόλου μοντέλα πριν
            supabase.table("users").upsert({
                "user_id": user_id,
                "username": user.username or f"id_{user_id}",
                "first_name": user.first_name or "",
                "models": list(all_models),
                "active": True,
                "start_time": now_iso if starting_shift else old_start_time if old_start_time else now_iso
            }).execute()
            # --- Εισαγωγή νέου shift log στο shifts ΜΟΝΟ αν ξεκινάει βάρδια ---
            if had_no_models and selected:
                supabase.table("mistake_shifts").insert({
                    "user_id": user_id,
                    "username": user.username or f"id_{user_id}",
                    "models": list(selected),
                    "start_time": now_iso,
                    "on_time": now_iso,
                    "active": True,
                    "mode": "on"
                }).execute()
            msg_text = (
                f"🔛 Shift ON by @{user.username}\n"
                f"🕒 {now.strftime('%H:%M')}   ⏱ Duration: {duration_str}\n"
                f"Models: {'Μόλις μπήκε!' if not old_models else ', '.join(old_models)}\n"
                f"➕ Νέα: {', '.join(selected) if selected else 'κανένα'}"
            )
            await query.edit_message_text(msg_text)
            # Καθάρισε το session
            context.chat_data['on_sessions'].pop(msg.message_id, None)
        except Exception as e:
            await query.edit_message_text(f"❌ Σφάλμα αποθήκευσης: {e}")

# --- /off Command ---
async def off_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not update.message:
        return
    user_id = str(update.effective_user.id)
    user = update.effective_user
    # Βρες τα ενεργά models του χρήστη
    active_models = []
    start_time = None
    try:
        resp = supabase.table("users").select("models,start_time,active").eq("user_id", user_id).execute()
        if resp.data and len(resp.data) > 0:
            active = resp.data[0].get("active")
            if active:
                active_models = resp.data[0].get("models") or []
                if isinstance(active_models, str):
                    try:
                        import json
                        active_models = json.loads(active_models)
                    except Exception:
                        active_models = []
                start_time = resp.data[0].get("start_time")
    except Exception:
        pass
    if not active_models:
        await update.message.reply_text("Δεν έχεις ενεργά μοντέλα.")
        return
    selected_models = set()
    sent = await context.bot.send_message(
        chat_id=TARGET_CHAT_ID,
        reply_to_message_id=TARGET_REPLY_TO_MESSAGE_ID,
        text="Επίλεξε ποια μοντέλα θέλεις να κάνεις off:",
        reply_markup=build_off_keyboard(active_models, selected_models)
    )
    if context.chat_data is not None and 'off_sessions' not in context.chat_data:
        context.chat_data['off_sessions'] = {}
    if context.chat_data is not None:
        context.chat_data['off_sessions'][sent.message_id] = {
            'initiator': user_id,
            'active_models': set(active_models),
            'selected_models': selected_models,
            'start_time': start_time
        }

def build_off_keyboard(active_models, selected):
    keyboard = []
    row = []
    for i, model in enumerate(active_models, 1):
        checked = "🔴 " if model in selected else ""
        row.append(dbg_btn(f"{checked}{model}", f"offmodel_{model}"))
        if i % 4 == 0 or i == len(active_models):
            keyboard.append(row)
            row = []
    keyboard.append([dbg_btn("✅ OK", "offmodels_ok"), dbg_btn("❌ Cancel", "cancel_action")])
    return InlineKeyboardMarkup(keyboard)

async def off_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query is None or context.chat_data is None or query.message is None or query.data is None:
        return
    user = query.from_user
    if user is None:
        return
    user_id = str(user.id)
    msg = query.message
    session = context.chat_data.get('off_sessions', {}).get(msg.message_id) if context.chat_data.get('off_sessions') else None
    if not session:
        await query.answer("Αυτή η επιλογή δεν είναι πλέον ενεργή.", show_alert=True)
        return
    initiator_id = session['initiator']
    active_models = session['active_models']
    selected = session['selected_models']
    start_time = session['start_time']
    if user_id != initiator_id:
        await query.answer("Δεν έχεις δικαίωμα να κάνεις αυτή την ενέργεια", show_alert=True)
        return
    data = query.data
    if data.startswith("offmodel_"):
        model = data[9:]
        if model not in active_models:
            await query.answer("Το μοντέλο δεν είναι ενεργό.", show_alert=True)
            return
        if model in selected:
            selected.remove(model)
        else:
            selected.add(model)
        session['selected_models'] = selected
        await query.edit_message_reply_markup(reply_markup=build_off_keyboard(active_models, selected))
        await query.answer()
    elif data == "offmodels_ok":
        if not selected:
            await query.answer("Επίλεξε τουλάχιστον ένα μοντέλο.", show_alert=True)
            return
        # Υπολογισμός duration στο off_callback
        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()
        duration_str = "-"
        try:
            resp = supabase.table("users").select("start_time").eq("user_id", user_id).execute()
            start_time = None
            if resp.data and len(resp.data) > 0:
                start_time = resp.data[0].get("start_time")
            if start_time:
                old_dt = datetime.fromisoformat(start_time)
                delta = now - old_dt
                h = int(delta.total_seconds() // 3600)
                m = int((delta.total_seconds() % 3600) // 60)
                duration_str = f"{h}:{m:02d}"
            else:
                duration_str = "0:00"
        except Exception as e:
            print(f"DEBUG: Exception στο duration υπολογισμό (off): {e}")
            duration_str = "0:00"
        # Αφαίρεση των selected από τα ενεργά
        new_models = list(active_models - selected)
        # Ενημέρωση users
        try:
            if new_models:
                # Ο χρήστης παραμένει ενεργός, ΔΕΝ αλλάζουμε το start_time!
                supabase.table("users").upsert({
                    "user_id": user_id,
                    "username": user.username or f"id_{user_id}",
                    "models": new_models,
                    "active": True
                }).execute()
            else:
                # Ο χρήστης βγήκε από όλα τα models, μηδενίζουμε το start_time
                supabase.table("users").upsert({
                    "user_id": user_id,
                    "username": user.username or f"id_{user_id}",
                    "models": [],
                    "active": False,
                    "start_time": None
                }).execute()
            # Καταγραφή shift στο shifts
            supabase.table("shifts").insert({
                "user_id": user_id,
                "username": user.username or f"id_{user_id}",
                "models": list(selected),
                "start_time": start_time,
                "on_time": now_iso,
                "active": False,
                "mode": "off"
            }).execute()
            msg_text = (
                f"🔻 Shift OFF by @{user.username}\n"
                f"🕒 {now.strftime('%H:%M')}   ⏱ Duration: {duration_str}\n"
                f"Έκλεισαν: {', '.join(selected)}\n"
                f"{'Ολοκλήρωσες τη βάρδιά σου!' if not new_models else 'Ανοιχτά: ' + ', '.join(new_models)}"
            )
            await query.edit_message_text(msg_text)
            context.chat_data['off_sessions'].pop(msg.message_id, None)
        except Exception as e:
            await query.edit_message_text(f"❌ Σφάλμα αποθήκευσης: {e}")

# --- /active Command ---
async def active_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    allowed_usernames = ["mikekrp", "tsaqiris"]
    user = update.effective_user
    if (user.username or "").lower() not in allowed_usernames:
        await update.message.reply_text("Δεν έχεις δικαίωμα να δεις αυτή τη λίστα.")
        return
    try:
        resp = supabase.table("users").select("user_id,first_name,models,start_time").eq("active", True).execute()
        users = resp.data if resp and resp.data else []
        if not users:
            await update.message.reply_text("Δεν υπάρχουν ενεργοί chatters αυτή τη στιγμή.")
            return
        now = datetime.now(timezone.utc)
        lines = []
        for u in users:
            fname = u.get("first_name") or "Άγνωστος"
            models = u.get("models") or []
            if isinstance(models, str):
                try:
                    import json
                    models = json.loads(models)
                except Exception:
                    models = []
            start_time = u.get("start_time")
            duration_str = "-"
            if start_time:
                try:
                    old_dt = datetime.fromisoformat(start_time)
                    now = datetime.now(timezone.utc)
                    delta = now - old_dt
                    h, m = divmod(int(delta.total_seconds()), 3600)[0], divmod(int(delta.total_seconds()) % 3600, 60)[0]
                    duration_str = f"{h}h {m}m"
                except Exception:
                    duration_str = "-"
            lines.append(f"👤 {fname}\n⏱ {duration_str}\n📦 Models: {', '.join(models) if models else 'κανένα'}\n")
        msg = "\n".join(lines)
        await update.message.reply_text(msg)
    except Exception as e:
        await update.message.reply_text(f"❌ Σφάλμα: {e}")

# --- /freemodels Command ---
async def freemodels_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        resp = supabase.table("users").select("models").eq("active", True).execute()
        users = resp.data if resp and resp.data else []
        busy_models = set()
        for u in users:
            ms = u.get("models") or []
            if isinstance(ms, str):
                try:
                    import json
                    ms = json.loads(ms)
                except Exception:
                    ms = []
            busy_models.update(ms)
        free_models = [m for m in MODELS if m not in busy_models]
        if not free_models:
            await update.message.reply_text("Δεν υπάρχουν ελεύθερα models αυτή τη στιγμή.")
        else:
            keyboard = []
            row = []
            for i, model in enumerate(free_models, 1):
                row.append(dbg_btn(model, f"freepick_{model}"))
                if i % 4 == 0 or i == len(free_models):
                    keyboard.append(row)
                    row = []
            await update.message.reply_text("Επίλεξε ελεύθερο μοντέλο:", reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        await update.message.reply_text(f"❌ Σφάλμα: {e}")

# --- Free Model Pick Callback ---
async def freepick_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = query.from_user
    user_id = str(user.id)
    data = query.data
    if not data.startswith("freepick_"):
        return
    model = data[len("freepick_"):]
    # Ελέγχει αν το model είναι ακόμα ελεύθερο
    try:
        resp = supabase.table("users").select("user_id,models,active").eq("active", True).execute()
        busy_models = set()
        for u in resp.data:
            ms = u.get("models") or []
            if isinstance(ms, str):
                try:
                    import json
                    ms = json.loads(ms)
                except Exception:
                    ms = []
            busy_models.update(ms)
        if model in busy_models:
            await query.answer("Το μοντέλο μόλις έγινε on από άλλον χρήστη!", show_alert=True)
            return
        # Βρες τα ήδη ενεργά models του χρήστη
        user_resp = supabase.table("users").select("models,active,first_name,username").eq("user_id", user_id).execute()
        user_data = user_resp.data[0] if user_resp.data else {}
        old_models = user_data.get("models") or []
        if isinstance(old_models, str):
            try:
                import json
                old_models = json.loads(old_models)
            except Exception:
                old_models = []
        active = user_data.get("active", False)
        first_name = user_data.get("first_name") or ""
        username = user_data.get("username") or f"id_{user_id}"
        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()
        # Freepick callback: ενημέρωση start_time μόνο αν ξεκινάει βάρδια
        starting_shift = not active or not old_models
        supabase.table("users").upsert({
            "user_id": user_id,
            "username": username,
            "first_name": first_name,
            "models": list(all_models),
            "active": True,
            "start_time": now_iso if starting_shift else user_data.get("start_time")
        }).execute()
        # Αν ξεκινάει νέα βάρδια, καταχώρησε shift
        if starting_shift:
            supabase.table("shifts").insert({
                "user_id": user_id,
                "username": username,
                "models": [model],
                "start_time": now_iso,
                "on_time": now_iso,
                "active": True,
                "mode": "on"
            }).execute()
        await query.answer(f"Το μοντέλο {model} προστέθηκε στη βάρδιά σου!", show_alert=True)
        await query.edit_message_text(f"✅ Το μοντέλο {model} προστέθηκε στη βάρδιά σου!")
    except Exception as e:
        await query.answer("Σφάλμα!", show_alert=True)
        await query.edit_message_text(f"❌ Σφάλμα: {e}")

# --- /break Command ---
async def break_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    user = update.effective_user
    # Βρες τα ενεργά models του χρήστη
    active_models = []
    start_time = None
    try:
        resp = supabase.table("users").select("models,start_time,active").eq("user_id", user_id).execute()
        if resp.data and len(resp.data) > 0:
            active = resp.data[0].get("active")
            if active:
                active_models = resp.data[0].get("models") or []
                if isinstance(active_models, str):
                    try:
                        import json
                        active_models = json.loads(active_models)
                    except Exception:
                        active_models = []
                start_time = resp.data[0].get("start_time")
    except Exception:
        pass
    if not active_models:
        await update.message.reply_text("Δεν έχεις ενεργά μοντέλα.")
        return
    # Υπολογισμός υπολοίπου break time
    now = datetime.now(timezone.utc)
    max_break_minutes = 45
    break_minutes_used = 0
    try:
        # Βρες το start_time της βάρδιας (όπως στο duration)
        shifts_resp = supabase.table("shifts").select("mode,start_time").eq("user_id", user_id).order("start_time").execute()
        shifts = shifts_resp.data if shifts_resp and shifts_resp.data else []
        # Βρες το τελευταίο shift με mode='off' πριν το τώρα
        current_index = len(shifts)
        last_off_index = -1
        for i in range(current_index - 1, -1, -1):
            if shifts[i]["mode"] == "off":
                last_off_index = i
                break
        shift_start = None
        for i in range(last_off_index + 1, current_index):
            if shifts[i]["mode"] == "on":
                shift_start = shifts[i]["start_time"]
                break
        if last_off_index == -1 and not shift_start:
            for s in shifts:
                if s["mode"] == "on":
                    shift_start = s["start_time"]
                    break
        # Άθροισε τα break durations από shift_start μέχρι τώρα
        if shift_start:
            for s in shifts:
                if s["mode"] == "break" and s["start_time"] >= shift_start:
                    mins = 0
                    if s.get("duration"):
                        mins = int(s["duration"])
                    else:
                        # fallback: duration = on_time - start_time ή τώρα - start_time
                        try:
                            bstart = datetime.fromisoformat(s["start_time"])
                            bend = now
                            if s.get("on_time"):
                                bend = datetime.fromisoformat(s["on_time"])
                            delta = bend - bstart
                            mins = int(delta.total_seconds() // 60)
                        except Exception:
                            pass
                    break_minutes_used += mins
    except Exception:
        pass
    break_minutes_left = max(0, max_break_minutes - break_minutes_used)
    if break_minutes_left <= 0:
        await update.message.reply_text("Έχεις εξαντλήσει τα 45 λεπτά break για αυτή τη βάρδια!")
        return
    # Εμφάνισε τα κουμπιά
    choices = [10, 15, 20, 25, 30, 45]
    keyboard = []
    row = []
    emoji_map = {10: '🔟', 15: '1️⃣5️⃣', 20: '2️⃣0️⃣', 25: '2️⃣5️⃣', 30: '3️⃣0️⃣', 45: '4️⃣5️⃣'}
    for i, mins in enumerate(choices, 1):
        label = emoji_map.get(mins, '')
        row.append(dbg_btn(label, f"breaklen_{mins}"))
        if i % 3 == 0 or i == len(choices):
            keyboard.append(row)
            row = []
    keyboard.append([dbg_btn("✏️", "breaklen_custom"), dbg_btn("❌ Cancel", "cancel_action")])
    warning = "\n⚠️ Αν επιστρέψεις νωρίτερα με /back, θα αφαιρεθεί μόνο ο πραγματικός χρόνος break!"
    await update.message.reply_text(
        f"⏸️ <b>Διάλειμμα (Break)</b>\n"
        f"Επίλεξε διάρκεια break (σου απομένουν 🕒 <b>{break_minutes_left}</b> λεπτά):"
        f"{warning if break_minutes_left <= 15 else ''}",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='HTML'
    )
    # Αποθήκευσε το υπόλοιπο break time στο user_data
    context.user_data['break_minutes_left'] = break_minutes_left
    context.user_data['active_models'] = active_models
    context.user_data['start_time'] = start_time

# --- Break Length Callback ---
async def breaklen_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = query.from_user
    user_id = str(user.id)
    data = query.data
    break_minutes_left = context.user_data.get('break_minutes_left', 45)
    active_models = context.user_data.get('active_models', [])
    start_time = context.user_data.get('start_time')
    if data.startswith("breaklen_"):
        if data == "breaklen_custom":
            await query.answer()
            await query.edit_message_text("Γράψε πόσα λεπτά break θέλεις (1-45):")
            context.user_data['awaiting_custom_break'] = True
            return
        mins = int(data.split('_')[1])
        if mins > break_minutes_left:
            await query.answer(f"Έχεις υπόλοιπο μόνο {break_minutes_left} λεπτά!", show_alert=True)
            return
        await do_break(user, user_id, mins, active_models, start_time, query, context)

# --- Custom Break Handler ---
async def custom_break_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get('awaiting_custom_break'):
        return
    try:
        mins = int(update.message.text.strip())
        break_minutes_left = context.user_data.get('break_minutes_left', 45)
        active_models = context.user_data.get('active_models', [])
        start_time = context.user_data.get('start_time')
        if not (1 <= mins <= 45):
            await update.message.reply_text("Γράψε έναν αριθμό από 1 έως 45.")
            return
        if mins > break_minutes_left:
            await update.message.reply_text(f"Έχεις υπόλοιπο μόνο {break_minutes_left} λεπτά!")
            return
        user = update.effective_user
        user_id = str(user.id)
        await do_break(user, user_id, mins, active_models, start_time, update.message, context)
    except Exception:
        await update.message.reply_text("Γράψε έναν έγκυρο αριθμό.")
    context.user_data['awaiting_custom_break'] = False

# --- Do Break ---
async def do_break(user, user_id, mins, active_models, start_time, msg_obj, context):
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()
    try:
        # Αφαίρεση όλων των models (βγαίνει τελείως off)
        supabase.table("users").upsert({
            "user_id": user_id,
            "username": user.username or f"id_{user_id}",
            "first_name": user.first_name or "",
            "models": [],
            "active": False,
            "start_time": None
        }).execute()
        # Καταγραφή shift στο shifts
        supabase.table("shifts").insert({
            "user_id": user_id,
            "username": user.username or f"id_{user_id}",
            "models": active_models,
            "start_time": now_iso,
            "on_time": None,
            "active": False,
            "mode": "break",
            "duration": mins
        }).execute()
        # Προγραμμάτισε jobs για ειδοποιήσεις break
        chat_id = user.id
        group_id = msg_obj.chat.id if hasattr(msg_obj, 'chat') else None
        username = user.username or f"id_{user_id}"
        break_end = now + timedelta(minutes=mins)
        # Ακύρωσε προηγούμενα jobs αν υπάρχουν
        if 'break_jobs' in context.user_data:
            for job in context.user_data['break_jobs']:
                try:
                    job.schedule_removal()
                except Exception:
                    pass
            context.user_data['break_jobs'] = []
        # 5 λεπτά πριν το τέλος
        if mins > 5:
            job1 = context.application.job_queue.run_once(
                break_5min_warning, when=mins*60-5*60,
                data={'user_id': user_id, 'chat_id': chat_id, 'username': username}
            )
            context.user_data.setdefault('break_jobs', [])
            context.user_data['break_jobs'].append(job1)
        # Τέλος break
        job2 = context.application.job_queue.run_once(
            break_end_notify, when=mins*60,
            data={'user_id': user_id, 'chat_id': chat_id, 'group_id': group_id, 'username': username, 'break_end': break_end}
        )
        context.user_data.setdefault('break_jobs', [])
        context.user_data['break_jobs'].append(job2)
        msg = (
            f"⏸️ <b>Έκανες break για {mins} λεπτά!</b>\n"
            f"@{username} βγήκε από όλα τα μοντέλα.\n"
        )
        # Υπόλοιπο break
        # Υπολογισμός υπολοίπου break (όπως στο /back)
        shifts_all_resp = supabase.table("shifts").select("mode,start_time,duration").eq("user_id", user_id).order("start_time").execute()
        shifts_all = shifts_all_resp.data if shifts_all_resp and shifts_all_resp.data else []
        current_index = len(shifts_all)
        last_off_index = -1
        for i in range(current_index - 1, -1, -1):
            if shifts_all[i]["mode"] == "off":
                last_off_index = i
                break
        shift_start = None
        for i in range(last_off_index + 1, current_index):
            if shifts_all[i]["mode"] == "on":
                shift_start = shifts_all[i]["start_time"]
                break
        if last_off_index == -1 and not shift_start:
            for s in shifts_all:
                if s["mode"] == "on":
                    shift_start = s["start_time"]
                    break
        max_break_minutes = 45
        break_minutes_used = 0
        if shift_start:
            for s in shifts_all:
                if s["mode"] == "break" and s["start_time"] >= shift_start:
                    mins_used = int(s.get("duration") or 0)
                    break_minutes_used += mins_used
        break_minutes_left = max(0, max_break_minutes - break_minutes_used)
        # Διόρθωση: αφαίρεσε τα λεπτά του break που μόλις πήρε
        break_minutes_left = max(0, break_minutes_left - mins)
        msg += f"Υπόλοιπο break: 🕒 <b>{break_minutes_left}</b> λεπτά"
        # Σωστό reply ανάλογα με το αντικείμενο
        if hasattr(msg_obj, 'reply_text'):
            await msg_obj.reply_text(msg, parse_mode='HTML')
        elif hasattr(msg_obj, 'edit_message_text'):
            await msg_obj.edit_message_text(msg, parse_mode='HTML')
    except Exception as e:
        if hasattr(msg_obj, 'reply_text'):
            await msg_obj.reply_text(f"❌ Σφάλμα αποθήκευσης: {e}")
        elif hasattr(msg_obj, 'edit_message_text'):
            await msg_obj.edit_message_text(f"❌ Σφάλμα αποθήκευσης: {e}")

# --- /back Command ---
async def back_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    user = update.effective_user
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()
    try:
        # Βρες το τελευταίο shift mode='break' χωρίς on_time
        shifts_resp = supabase.table("shifts").select("id,models,start_time,duration,on_time,mode").eq("user_id", user_id).order("start_time", desc=True).limit(5).execute()
        shifts = shifts_resp.data if shifts_resp and shifts_resp.data else []
        break_shift = None
        for s in shifts:
            if s["mode"] == "break" and not s.get("on_time"):
                break_shift = s
                break
        if not break_shift:
            await update.message.reply_text("Δεν είσαι σε break αυτή τη στιγμή.")
            return
        # Υπολόγισε πόσα λεπτά πέρασαν
        bstart = datetime.fromisoformat(break_shift["start_time"])
        mins_used = int((now - bstart).total_seconds() // 60)
        # Ενημέρωσε το shift με το πραγματικό duration και on_time
        supabase.table("shifts").update({"duration": mins_used, "on_time": now_iso}).eq("id", break_shift["id"]).execute()
        # Επαναφορά models και active
        models = break_shift.get("models") or []
        supabase.table("users").upsert({
            "user_id": user_id,
            "username": user.username or f"id_{user_id}",
            "first_name": user.first_name or "",
            "models": models,
            "active": True,
            "start_time": None  # Δεν αλλάζουμε τη βάρδια, μόνο επιστροφή
        }).execute()
        # Υπολόγισε νέο υπόλοιπο break (μόνο το duration κάθε break shift)
        shifts_all_resp = supabase.table("shifts").select("mode,start_time,duration").eq("user_id", user_id).order("start_time").execute()
        shifts_all = shifts_all_resp.data if shifts_all_resp and shifts_all_resp.data else []
        # Βρες το start_time της βάρδιας (όπως στο duration)
        current_index = len(shifts_all)
        last_off_index = -1
        for i in range(current_index - 1, -1, -1):
            if shifts_all[i]["mode"] == "off":
                last_off_index = i
                break
        shift_start = None
        for i in range(last_off_index + 1, current_index):
            if shifts_all[i]["mode"] == "on":
                shift_start = shifts_all[i]["start_time"]
                break
        if last_off_index == -1 and not shift_start:
            for s in shifts_all:
                if s["mode"] == "on":
                    shift_start = s["start_time"]
                    break
        max_break_minutes = 45
        break_minutes_used = 0
        if shift_start:
            for s in shifts_all:
                if s["mode"] == "break" and s["start_time"] >= shift_start:
                    mins = int(s.get("duration") or 0)
                    break_minutes_used += mins
        break_minutes_left = max(0, max_break_minutes - break_minutes_used)
        await update.message.reply_text(f"@{user.username or user_id} Επέστρεψες από break! Χρησιμοποίησες {mins_used} λεπτά, υπόλοιπο break: {break_minutes_left} λεπτά.")
        # Ακύρωσε jobs break
        if 'break_jobs' in context.user_data:
            for job in context.user_data['break_jobs']:
                try:
                    job.schedule_removal()
                except Exception:
                    pass
            context.user_data['break_jobs'] = []
    except Exception as e:
        await update.message.reply_text(f"❌ Σφάλμα: {e}")

# --- Break notification jobs ---
async def break_5min_warning(context: ContextTypes.DEFAULT_TYPE):
    user_id = context.job.data['user_id']
    chat_id = context.job.data['chat_id']
    username = context.job.data.get('username', user_id)
    await context.bot.send_message(chat_id=chat_id, text=f"⏰ @{username} σε 5 λεπτά τελειώνει το break σου!")

async def break_end_notify(context: ContextTypes.DEFAULT_TYPE):
    user_id = context.job.data['user_id']
    chat_id = context.job.data['chat_id']
    group_id = context.job.data['group_id']
    username = context.job.data['username']
    # Προσθήκη κουμπιού επιστροφής
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 Επιστροφή", callback_data=f"breakreturn_{user_id}")]
    ])
    await context.bot.send_message(chat_id=chat_id, text=f"⏰ @{username} το break σου τελείωσε! Επιστροφή στη βάρδια.", reply_markup=keyboard)
    # Προγραμματίζουμε έλεγχο για καθυστέρηση κάθε 1 λεπτό
    context.job_queue.run_repeating(break_late_check, interval=60, first=60, data={
        'user_id': user_id,
        'group_id': group_id,
        'username': username,
        'break_end': context.job.data['break_end']
    }, name=f"latecheck_{user_id}")

# --- Break Return Callback ---
async def breakreturn_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = query.from_user
    user_id = str(user.id)
    group_id = None
    # Βρες το group_id από το τελευταίο break shift
    try:
        shifts_resp = supabase.table("shifts").select("id,models,start_time,duration,on_time,mode").eq("user_id", user_id).order("start_time", desc=True).limit(1).execute()
        shifts = shifts_resp.data if shifts_resp and shifts_resp.data else []
        if shifts:
            group_id = context.bot_data.get('last_group_id')
    except Exception:
        pass
    # Κάνε trigger το /back
    class DummyUpdate:
        def __init__(self, user, query):
            self.effective_user = user
            self.message = query.message
    dummy_update = DummyUpdate(user, query)
    await back_command(dummy_update, context)
    # Υπολόγισε υπόλοιπο break
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()
    try:
        shifts_all_resp = supabase.table("shifts").select("mode,start_time,duration").eq("user_id", user_id).order("start_time").execute()
        shifts_all = shifts_all_resp.data if shifts_all_resp and shifts_all_resp.data else []
        current_index = len(shifts_all)
        last_off_index = -1
        for i in range(current_index - 1, -1, -1):
            if shifts_all[i]["mode"] == "off":
                last_off_index = i
                break
        shift_start = None
        for i in range(last_off_index + 1, current_index):
            if shifts_all[i]["mode"] == "on":
                shift_start = shifts_all[i]["start_time"]
                break
        if last_off_index == -1 and not shift_start:
            for s in shifts_all:
                if s["mode"] == "on":
                    shift_start = s["start_time"]
                    break
        max_break_minutes = 45
        break_minutes_used = 0
        if shift_start:
            for s in shifts_all:
                if s["mode"] == "break" and s["start_time"] >= shift_start:
                    mins = int(s.get("duration") or 0)
                    break_minutes_used += mins
        break_minutes_left = max(0, max_break_minutes - break_minutes_used)
        if group_id:
            await context.bot.send_message(chat_id=group_id, text=f"@{user.username or user_id} επέστρεψε από το break! Υπόλοιπο break: {break_minutes_left} λεπτά.")
    except Exception:
        pass
    await query.answer("Επέστρεψες από break!", show_alert=True)

async def break_late_check(context: ContextTypes.DEFAULT_TYPE):
    user_id = context.job.data['user_id']
    group_id = context.job.data['group_id']
    username = context.job.data['username']
    break_end = context.job.data['break_end']
    # Έλεγξε αν ο χρήστης είναι ακόμα σε break
    try:
        resp = supabase.table("users").select("active").eq("user_id", user_id).execute()
        if resp.data and resp.data[0].get("active"):
            # Επέστρεψε, ακύρωσε το job
            context.job.schedule_removal()
            return
        # Υπολόγισε πόσα λεπτά αργεί
        now = datetime.now(timezone.utc)
        late = int((now - break_end).total_seconds() // 60)
        if late > 0:
            await context.bot.send_message(chat_id=group_id, text=f"⚠️ @{username} άργησε να επιστρέψει από το break του! Αργεί {late} λεπτά.")
    except Exception:
        pass

# --- /status Command ---
async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user is None or not update.message:
        return
    user_id = str(user.id)
    try:
        resp = supabase.table("users").select("models,start_time,active").eq("user_id", user_id).execute()
        if not resp.data or len(resp.data) == 0:
            await update.message.reply_text("Δεν βρέθηκαν δεδομένα για εσένα.")
            return
        user_data = resp.data[0]
        models = user_data.get("models") or []
        if isinstance(models, str):
            try:
                import json
                models = json.loads(models)
            except Exception:
                models = []
        start_time = user_data.get("start_time")
        duration_str = "-"
        if start_time:
            try:
                old_dt = datetime.fromisoformat(start_time)
                now = datetime.now(timezone.utc)
                delta = now - old_dt
                h, m = divmod(int(delta.total_seconds()), 3600)[0], divmod(int(delta.total_seconds()) % 3600, 60)[0]
                duration_str = f"{h}h {m}m"
            except Exception:
                duration_str = "-"
        msg = (
            f"📦 Models: {', '.join(models) if models else 'κανένα'}\n"
            f"⏱ Μέσα: {duration_str}"
        )
        await update.message.reply_text(msg)
    except Exception as e:
        await update.message.reply_text(f"❌ Σφάλμα: {e}")

# --- !status @username Handler ---
async def mention_status_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return
    # Πιάσε το !status @username
    match = re.match(r"!status\s+@?(\w+)", update.message.text.strip())
    if not match:
        return
    username = match.group(1)
    try:
        # Βρες τον χρήστη με αυτό το username
        resp = supabase.table("users").select("models,start_time,active,username,first_name").eq("username", username).execute()
        if not resp.data or len(resp.data) == 0:
            await update.message.reply_text(f"Δεν βρέθηκε χρήστης @{username}.")
            return
        user_data = resp.data[0]
        models = user_data.get("models") or []
        if isinstance(models, str):
            try:
                import json
                models = json.loads(models)
            except Exception:
                models = []
        start_time = user_data.get("start_time")
        duration_str = "-"
        if start_time:
            try:
                old_dt = datetime.fromisoformat(start_time)
                now = datetime.now(timezone.utc)
                delta = now - old_dt
                h, m = divmod(int(delta.total_seconds()), 3600)[0], divmod(int(delta.total_seconds()) % 3600, 60)[0]
                duration_str = f"{h}h {m}m"
            except Exception:
                duration_str = "-"
        fname = user_data.get("first_name") or username
        msg = (
            f"👤 @{username} ({fname})\n"
            f"📦 Models: {', '.join(models) if models else 'κανένα'}\n"
            f"⏱ Μέσα: {duration_str}"
        )
        await update.message.reply_text(msg)
    except Exception as e:
        await update.message.reply_text(f"❌ Σφάλμα: {e}")

# --- Give subsystem (refactored) ---
# In-memory dicts for give flows
GIVE_TARGET = {}
GIVE_SELECTED = {}
CONFIRM_FLOW = {}
RECIPIENT_CONFIRM_FLOW = {}
ALLOWED_APPROVERS = ["mikekrp", "tsaqiris"]  # Add your admin usernames here
KNOWN_USERS = {}  # username (lowercase) -> user_id, fill this at startup or dynamically

# Replace the give command and callbacks with the following:

async def give_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not update.message or not update.message.text:
        return
    user_id = str(update.effective_user.id)
    match = re.match(r"/give\s+@?(\w+)", update.message.text.strip())
    if not match:
        await update.message.reply_text("Χρησιμοποίησε: /give @username")
        return
    target_username = match.group(1)
    if target_username.lower() == (update.effective_user.username or '').lower():
        await update.message.reply_text("Δεν μπορείς να δώσεις models στον εαυτό σου!")
        return
    # Βρες τα ενεργά models του χρήστη που κάνει το command
    try:
        resp = supabase.table("users").select("models").eq("user_id", user_id).execute()
        if not resp.data or len(resp.data) == 0:
            await update.message.reply_text("Δεν βρέθηκαν ενεργά models για εσένα.")
            return
        models = resp.data[0].get("models") or []
        if isinstance(models, str):
            try:
                import json
                models = json.loads(models)
            except Exception:
                models = []
        if not models:
            await update.message.reply_text("Δεν έχεις ενεργά models για να δώσεις.")
            return
        selected_models = set()
        sent = await update.message.reply_text(
            f"Επίλεξε ποια models θέλεις να δώσεις στον @{target_username}:",
            reply_markup=build_give_keyboard(models, selected_models)
        )
        key = (sent.chat.id, sent.message_id)
        GIVE_TARGET[key] = target_username
        GIVE_SELECTED[key] = set()
    except Exception as e:
        await update.message.reply_text(f"❌ Σφάλμα: {e}")

def build_give_keyboard(models, selected):
    keyboard = []
    row = []
    for i, model in enumerate(models, 1):
        checked = "🟢 " if model in selected else ""
        row.append(dbg_btn(f"{checked}{model}", f"givepick_{model}"))
        if i % 4 == 0 or i == len(models):
            keyboard.append(row)
            row = []
    keyboard.append([dbg_btn("✅ OK", "giveok"), dbg_btn("❌ Cancel", "cancel_action")])
    return InlineKeyboardMarkup(keyboard)

async def give_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q or not q.message:
        return
    sel = q.data
    chat = q.message.chat
    uid = q.from_user.id
    key = (chat.id, q.message.message_id)
    if key in GIVE_TARGET:
        if sel.startswith("givepick_"):
            model = sel[len("givepick_"):]
            selset = GIVE_SELECTED[key]
            selset.symmetric_difference_update({model})
            # Only show user's current models, not all models
            user_status = supabase.table("users").select("models").eq("user_id", uid).execute()
            user_models = user_status.data[0].get("models") if user_status.data else []
            if isinstance(user_models, str):
                try:
                    import json
                    user_models = json.loads(user_models)
                except Exception:
                    user_models = []
            return await q.message.edit_reply_markup(reply_markup=build_give_keyboard(user_models, selset))
        elif sel == "giveok":
            selset = GIVE_SELECTED.pop(key, set())
            target = GIVE_TARGET.pop(key)
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
            CONFIRM_FLOW[cm.message_id] = (giver, target, models)
            markup = InlineKeyboardMarkup([[
                dbg_btn("✅ Επιβεβαίωση", f"confirm_{cm.message_id}"),
                dbg_btn("❌ Απόρριψη",     f"reject_{cm.message_id}")
            ]])
            return await cm.edit_reply_markup(reply_markup=markup)
    # --- Confirmation callbacks ---
    if sel.startswith(("confirm_","reject_")):
        approver = q.from_user.username
        action, mid = sel.split("_",1)
        if approver not in ALLOWED_APPROVERS:
            await q.answer("❌ Δεν είσαι admin, τι κάνεις εκεί;", show_alert=True)
            for admin_username in ALLOWED_APPROVERS:
                admin_id = KNOWN_USERS.get(admin_username)
                if admin_id:
                    await context.bot.send_message(
                        chat_id=admin_id,
                        text=f"⚠️ Ο @{approver} προσπάθησε να {action} στο give."
                    )
            return
        mid = int(mid)
        values = CONFIRM_FLOW.pop(mid, None)
        if values is None:
            return await context.bot.send_message(chat.id, "❌ Δεν βρέθηκε η πληροφορία της απόδοσης.")
        giver, target, models = values
        # Always fetch user_id from Supabase
        try:
            resp = supabase.table("users").select("user_id,first_name").eq("username", target).execute()
            if not resp.data or len(resp.data) == 0:
                return await context.bot.send_message(chat.id, f"❌ Δεν βρέθηκε ο χρήστης {target} στη Supabase.")
            target_id = resp.data[0]['user_id']
        except Exception:
            return await context.bot.send_message(chat.id, f"❌ Σφάλμα Supabase κατά το lookup του χρήστη {target}.")
        await q.message.delete()
        recipient_id = target_id
        if action == "confirm":
            RECIPIENT_CONFIRM_FLOW[mid] = (giver, recipient_id, models, chat.id)
            try:
                await context.bot.send_message(
                    chat_id=recipient_id,
                    text=f"🎁 Ο @{giver} θέλει να σου μεταβιβάσει μοντέλα: {models}.\nΠατήστε αποδοχή:",
                    reply_markup=InlineKeyboardMarkup([[
                        dbg_btn("✅ Αποδοχή", f"acceptgive_{mid}")
                    ]]),
                    reply_to_message_id=get_reply_to_message_id(update, TARGET_REPLY_TO_MESSAGE_ID)
                )
            except Exception as ex:
                return await context.bot.send_message(chat.id, f"❌ Δεν μπόρεσα να στείλω μήνυμα στον χρήστη @{target}. Πιθανόν δεν έχει κάνει /start στο bot.")
            await context.bot.send_message(
                chat_id=chat.id,
                text=f"🔔 Οι admins αποδέχτηκαν το αίτημά σου @{giver} και περιμένουμε από τον @{target} να πατήσει Αποδοχή για να γίνει το give.",
                reply_to_message_id=get_reply_to_message_id(update, TARGET_REPLY_TO_MESSAGE_ID)
            )
            return
        else:
            return await context.bot.send_message(
                chat_id=chat.id,
                text=f"❌ Απορρίφθηκε η απόδοση σε {target}."
            )
    if sel.startswith("acceptgive_"):
        mid = int(sel[len("acceptgive_"):])
        values = RECIPIENT_CONFIRM_FLOW.pop(mid, None)
        if values is None:
            return await context.bot.send_message(q.message.chat.id, "❌ Δεν βρέθηκε η πληροφορία της απόδοσης.")
        giver, recipient_id, models, group_chat_id = values
        # Εδώ κάνε το transfer στο supabase και στείλε τα τελικά μηνύματα
        await q.message.delete()
        # --- Fetch usernames and models from Supabase ---
        try:
            # Giver info
            resp_giver = supabase.table("users").select("models,username").eq("username", giver).execute()
            old_giver_models = resp_giver.data[0].get("models") if resp_giver.data else []
            if isinstance(old_giver_models, str):
                try:
                    import json
                    old_giver_models = json.loads(old_giver_models)
                except Exception:
                    old_giver_models = []
            giver_username = resp_giver.data[0].get("username") if resp_giver.data else giver
            # Recipient info
            resp_rec = supabase.table("users").select("models,username").eq("user_id", recipient_id).execute()
            old_rec_models = resp_rec.data[0].get("models") if resp_rec.data else []
            if isinstance(old_rec_models, str):
                try:
                    import json
                    old_rec_models = json.loads(old_rec_models)
                except Exception:
                    old_rec_models = []
            rec_username = resp_rec.data[0].get("username") if resp_rec.data else recipient_id
            # --- Update models in Supabase ---
            given_models = [m.strip() for m in models.split(",")]
            new_giver_models = [m for m in old_giver_models if m not in given_models]
            new_rec_models = list(set(old_rec_models) | set(given_models))
            now = datetime.now(timezone.utc)
            now_iso = now.isoformat()
            now_str = now.strftime('%H:%M')
            # Update giver
            supabase.table("users").upsert({"user_id": str(q.from_user.id), "models": new_giver_models, "active": bool(new_giver_models), "start_time": now_iso if new_giver_models else None}).execute()
            print(f"DEBUG: GIVE FLOW upsert giver user_id={q.from_user.id} models={new_giver_models} active={bool(new_giver_models)}")
            # Update recipient
            supabase.table("users").upsert({"user_id": str(recipient_id), "models": new_rec_models, "active": True, "start_time": now_iso}).execute()
            # Insert shift logs
            supabase.table("shifts").insert({
                "user_id": str(recipient_id),
                "username": rec_username,
                "models": given_models,
                "start_time": now_iso,
                "on_time": now_iso,
                "active": True,
                "mode": "on"
            }).execute()
            supabase.table("shifts").insert({
                "user_id": str(q.from_user.id),
                "username": giver_username,
                "models": given_models,
                "start_time": now_iso,
                "on_time": now_iso,
                "active": False,
                "mode": "off"
            }).execute()
            # --- Group notifications ---
            msg_on = (
                f"🔛 Shift ON by @{rec_username}\n"
                f"🕒 {now_str}   ⏱ Duration: 0:00\n"
                f"Models: {', '.join(old_rec_models) if old_rec_models else 'κανένα'}\n"
                f"➕ Νέα: {', '.join(given_models)}"
            )
            msg_off = (
                f"🔻 Shift OFF by @{giver_username}\n"
                f"🕒 {now_str}   ⏱ Duration: 0:00\n"
                f"Έκλεισαν: {', '.join(new_giver_models) if new_giver_models else 'κανένα'}\n"
                f"Εδωσε: {', '.join(given_models)}"
            )
            await context.bot.send_message(chat_id=group_chat_id, text=msg_on)
            await context.bot.send_message(chat_id=group_chat_id, text=msg_off)
            # --- Private notification ---
            await context.bot.send_message(
                chat_id=recipient_id,
                text=f"🎉 Έλαβες τα μοντέλα: {', '.join(given_models)} από τον @{giver_username}!"
            )
        except Exception as ex:
            return await context.bot.send_message(group_chat_id, f"❌ Σφάλμα κατά το shift transfer: {ex}")
        return

# --- Give Approve/Reject Callback ---
async def give_admin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query is None or query.message is None or query.data is None:
        return
    user = query.from_user
    admin_usernames = ["mikekrp", "tsaqiris"]
    if user is None or (user.username or '').lower() not in admin_usernames:
        await query.answer("Δεν έχεις δικαίωμα να εγκρίνεις/απορρίψεις αυτή τη μεταφορά.", show_alert=True)
        return
    data = query.data
    if data.startswith("giveapprove_"):
        _, give_key = data.split('_', 1)
        give_data = context.bot_data.get('give_pending', {}).get(give_key)
        if not give_data:
            await query.edit_message_text("❌ Η μεταφορά δεν βρέθηκε ή έχει λήξει.")
            return
        from_id = give_data['from_id']
        target_username = give_data['target_username']
        selected = give_data['selected']
        group_id = give_data['group_id']
        # Βρες το user_id του target και στείλε του μήνυμα αποδοχής
        try:
            resp = supabase.table("users").select("user_id,first_name").eq("username", target_username).execute()
            if not resp.data or len(resp.data) == 0:
                await query.edit_message_text(f"Δεν βρέθηκε χρήστης @{target_username}.")
                return
            target_id = resp.data[0]['user_id']
            target_first_name = resp.data[0].get('first_name') or target_username
            # Βρες το username του from_id
            resp_from = supabase.table("users").select("username,first_name").eq("user_id", from_id).execute()
            from_username = resp_from.data[0].get('username') if resp_from.data else from_id
            from_first_name = resp_from.data[0].get('first_name') if resp_from.data else from_id
            # Στείλε προσωπικό μήνυμα στον παραλήπτη
            # Χρησιμοποίησε το ίδιο give_key για τη συνέχεια
            context.bot_data['give_pending'][give_key].update({
                'target_id': target_id,
                'from_username': from_username,
                'from_first_name': from_first_name
            })
            accept_keyboard = InlineKeyboardMarkup([
                [dbg_btn("✅ Αποδοχή", f"givefinalaccept_{give_key}")]
            ])
            try:
                print(f"DEBUG: Sending to user_id={target_id} (type={type(target_id)})")
                chat_id = int(target_id) if isinstance(target_id, str) and str(target_id).isdigit() else target_id
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"👤 Ο @{from_username} θέλει να σου μεταφέρει τα models: {', '.join(selected)}.\nΠαρακαλώ πάτησε Αποδοχή για να ολοκληρωθεί η μεταφορά.",
                    reply_markup=accept_keyboard,
                    reply_to_message_id=get_reply_to_message_id(update, TARGET_REPLY_TO_MESSAGE_ID)
                )
            except Exception as ex:
                print(f"DEBUG: Failed to send to user_id={target_id} ex={ex}")
                await query.edit_message_text(f"Δεν μπόρεσα να στείλω μήνυμα στον χρήστη @{target_username}. Ελέγξτε αν έχει ξεκινήσει το bot. [DEBUG: {target_id} type={type(target_id)}]")
                return
            await query.edit_message_text(f"✅ Η μεταφορά εγκρίθηκε από τον @{user.username}. Περιμένουμε αποδοχή από τον @{target_username}.")
        except Exception as e:
            await query.edit_message_text(f"❌ Σφάλμα: {e}")
    elif data.startswith("givereject_"):
        _, give_key = data.split('_', 1)
        give_data = context.bot_data.get('give_pending', {}).get(give_key)
        if not give_data:
            await query.edit_message_text("❌ Η μεταφορά δεν βρέθηκε ή έχει λήξει.")
            return
        selected = give_data['selected']
        target_username = give_data['target_username']
        await query.edit_message_text(f"❌ Η μεταφορά των μοντέλων: {', '.join(selected)} στον @{target_username} απορρίφθηκε από τον @{user.username}.")
    print(f"DEBUG: [give_admin_callback] group_id={session['group_id']} (type={type(session['group_id'])}) [give_key={give_key}]")

# --- Give Final Accept Callback ---
async def give_final_accept_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query is None or query.data is None or query.from_user is None:
        return
    data = query.data
    user = query.from_user
    if data.startswith("givefinalaccept_"):
        _, give_key = data.split('_', 1)
        give_data = context.bot_data.get('give_pending', {}).get(give_key)
        if not give_data:
            await query.edit_message_text("❌ Η μεταφορά δεν βρέθηκε ή έχει λήξει.")
            return
        from_id = give_data['from_id']
        target_id = give_data['target_id']
        from_username = give_data['from_username']
        target_username = give_data['target_username']
        selected = give_data['selected']
        group_id = give_data['group_id']
        # Επίτρεψε μόνο στον παραλήπτη να το πατήσει
        if str(user.id) != str(target_id):
            await query.answer("Δεν έχεις δικαίωμα να αποδεχτείς αυτή τη μεταφορά.", show_alert=True)
            return
        try:
            # Βρες τα group στα οποία είναι το bot (θα στείλουμε στην ομαδική)
            group_id = None
            if query.message and query.message.chat and query.message.chat.type in ["group", "supergroup"]:
                group_id = query.message.chat.id
            # Πάρε τα προηγούμενα models του παραλήπτη
            resp_target = supabase.table("users").select("models,username").eq("user_id", target_id).execute()
            old_target_models = resp_target.data[0].get('models') if resp_target.data else []
            if isinstance(old_target_models, str):
                try:
                    import json
                    old_target_models = json.loads(old_target_models)
                except Exception:
                    old_target_models = []
            target_username = resp_target.data[0].get('username') if resp_target.data else target_id
            # Κάνε τον παραλήπτη shift on με αυτά τα models
            now = datetime.now(timezone.utc)
            now_iso = now.isoformat()
            now_str = now.strftime('%H:%M')
            new_target_models = list(set(old_target_models) | set(selected))
            supabase.table("users").upsert({
                "user_id": target_id,
                "models": new_target_models,
                "active": True,
                "start_time": now_iso
            }).execute()
            supabase.table("shifts").insert({
                "user_id": target_id,
                "username": user.username or f"id_{target_id}",
                "models": selected,
                "start_time": now_iso,
                "on_time": now_iso,
                "active": True,
                "mode": "on"
            }).execute()
            # Πάρε τα προηγούμενα models του from_id
            resp2 = supabase.table("users").select("models,username").eq("user_id", from_id).execute()
            my_models = resp2.data[0].get('models') if resp2.data else []
            if isinstance(my_models, str):
                try:
                    import json
                    my_models = json.loads(my_models)
                except Exception:
                    my_models = []
            from_username_real = resp2.data[0].get('username') if resp2.data else from_username
            new_my_models = [m for m in my_models if m not in selected]
            supabase.table("users").upsert({"user_id": from_id, "models": new_my_models, "active": False, "start_time": None}).execute()
            supabase.table("shifts").insert({
                "user_id": from_id,
                "username": from_username_real,
                "models": selected,
                "start_time": now_iso,
                "on_time": now_iso,
                "active": False,
                "mode": "off"
            }).execute()
            # Μηνύματα στην ομαδική
            if group_id and group_id != 'None':
                try:
                    group_id_int = int(group_id)
                    print(f"DEBUG: Sending group messages to group_id={group_id_int} (type={type(group_id_int)})")
                    msg_on = (
                        f"🔛 Shift ON by @{target_username}\n"
                        f"🕒 {now_str}   ⏱ Duration: 0:00\n"
                        f"Models: {', '.join(old_target_models) if old_target_models else 'κανένα'}\n"
                        f"➕ Νέα: {', '.join(selected)}"
                    )
                    msg_off = (
                        f"🔻 Shift OFF by @{from_username_real}\n"
                        f"🕒 {now_str}   ⏱ Duration: 0:00\n"
                        f"Έκλεισαν: {', '.join(new_my_models) if new_my_models else 'κανένα'}\n"
                        f"Εδωσε: {', '.join(selected)}"
                    )
                    await context.bot.send_message(chat_id=group_id_int, text=msg_on)
                    await context.bot.send_message(chat_id=group_id_int, text=msg_off)
                except Exception as ex:
                    print(f"DEBUG: Failed to send group messages to group_id={group_id} ex={ex}")
            await query.edit_message_text("✅ Αποδέχτηκες τα models και είσαι πλέον σε shift ON!")
        except Exception as e:
            await query.edit_message_text(f"❌ Σφάλμα: {e}")
        del context.bot_data['give_pending'][give_key]
    print(f"DEBUG: [give_final_accept_callback] group_id={session['group_id']} (type={type(session['group_id'])}) [give_key={give_key}]")

# --- Cancel Callback ---
async def cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query is None or query.message is None:
        return
    msg = query.message
    user = query.from_user
    uname = user.username if user and user.username else user.first_name if user and user.first_name else "άγνωστος"
    # Καθάρισε όλα τα πιθανά sessions για το συγκεκριμένο message_id
    for session_key in ['on_sessions', 'off_sessions', 'give_sessions']:
        if context.chat_data and session_key in context.chat_data:
            context.chat_data[session_key].pop(msg.message_id, None)
    await query.edit_message_text(f"❌ Η ενέργεια ακυρώθηκε από τον @{uname}.")
    await query.answer()

# --- /notify Command ---
async def notify_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not update.message:
        return
    try:
        resp = supabase.table("users").select("user_id,username,first_name,models").eq("active", True).execute()
        users = resp.data if resp and resp.data else []
        if not users:
            await update.message.reply_text("Δεν υπάρχουν ενεργοί chatters αυτή τη στιγμή.")
            return
        keyboard = []
        row = []
        for i, u in enumerate(users, 1):
            uname = u.get("username") or u.get("first_name") or u.get("user_id")
            row.append(dbg_btn(f"@{uname}", f"notifuser_{uname}"))
            if i % 3 == 0 or i == len(users):
                keyboard.append(row)
                row = []
        await update.message.reply_text(
            "Επίλεξε chatter για notify:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            reply_to_message_id=get_reply_to_message_id(update, TARGET_REPLY_TO_MESSAGE_ID)
        )
    except Exception as e:
        await update.message.reply_text(f"❌ Σφάλμα: {e}")

# --- Notify User Callback ---
async def notify_user_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query or not query.data:
        return
    data = query.data
    if not data.startswith("notifuser_"):
        return
    username = data[len("notifuser_"):]
    # Fetch models for this user
    try:
        resp = supabase.table("users").select("models,first_name,user_id").eq("username", username).execute()
        if not resp.data or len(resp.data) == 0:
            await query.answer("Δεν βρέθηκε ο χρήστης.", show_alert=True)
            return
        user_data = resp.data[0]
        models = user_data.get("models") or []
        if isinstance(models, str):
            try:
                import json
                models = json.loads(models)
            except Exception:
                models = []
        if not models:
            await query.edit_message_text(f"Ο χρήστης @{username} δεν έχει ενεργά models.")
            return
        keyboard = []
        row = []
        for i, model in enumerate(models, 1):
            row.append(dbg_btn(model, f"notifymodel_{username}_{model}"))
            if i % 3 == 0 or i == len(models):
                keyboard.append(row)
                row = []
        await query.edit_message_text(
            f"Ενεργά models του @{username}:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            reply_to_message_id=get_reply_to_message_id(update, TARGET_REPLY_TO_MESSAGE_ID)
        )
    except Exception as e:
        await query.edit_message_text(f"❌ Σφάλμα: {e}")

# --- Notify Model Callback ---
async def notify_model_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query or not query.data or not query.from_user:
        return
    data = query.data
    if not data.startswith("notifymodel_"):
        return
    # notifymodel_{username}_{model}
    try:
        parts = data.split("_", 2)
        if len(parts) < 3:
            await query.answer("Σφάλμα callback.", show_alert=True)
            return
        username, model = parts[1], parts[2]
        # Βρες το user_id του κατόχου
        resp = supabase.table("users").select("user_id,first_name").eq("username", username).execute()
        if not resp.data or len(resp.data) == 0:
            await query.answer("Δεν βρέθηκε ο χρήστης.", show_alert=True)
            return
        owner_id = resp.data[0]["user_id"]
        owner_first_name = resp.data[0].get("first_name") or username
        trigger_username = query.from_user.username or query.from_user.first_name or str(query.from_user.id)
        # Στείλε προσωπικό μήνυμα στον κάτοχο του model
        accept_data = f"notifaccept_{username}_{model}_{trigger_username}"
        reject_data = f"notifreject_{username}_{model}_{trigger_username}"
        keyboard = InlineKeyboardMarkup([
            [dbg_btn("✅ Αποδοχή", accept_data), dbg_btn("❌ Απόρριψη", reject_data)]
        ])
        try:
            await context.bot.send_message(
                chat_id=owner_id,
                text=f"🔔 Ο χρήστης @{trigger_username} σε κάνει notify να βγεις από το model: {model}.\nΘέλεις να το αποδεχτείς;",
                reply_markup=keyboard,
                reply_to_message_id=get_reply_to_message_id(update, TARGET_REPLY_TO_MESSAGE_ID)
            )
            await query.answer("Έγινε notify στον χρήστη.", show_alert=True)
        except Exception as ex:
            await query.answer(f"Δεν μπόρεσα να στείλω μήνυμα στον χρήστη. {ex}", show_alert=True)
    except Exception as e:
        await query.answer(f"❌ Σφάλμα: {e}", show_alert=True)

# --- Notify Accept/Reject Callback ---
async def notify_accept_reject_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query or not query.data or not query.from_user:
        return
    data = query.data
    try:
        if data.startswith("notifaccept_"):
            _, username, model, trigger = data.split("_", 3)
            # Αφαίρεσε το model από τα models του χρήστη
            resp = supabase.table("users").select("models,user_id,username").eq("username", username).execute()
            if not resp.data or len(resp.data) == 0:
                await query.answer("Δεν βρέθηκε ο χρήστης.", show_alert=True)
                return
            user_data = resp.data[0]
            owner_id = user_data["user_id"]
            owner_username = user_data.get("username") or username
            models = user_data.get("models") or []
            if isinstance(models, str):
                try:
                    import json
                    models = json.loads(models)
                except Exception:
                    models = []
            if model not in models:
                await query.answer("Το model δεν είναι πλέον ενεργό.", show_alert=True)
                return
            new_models = [m for m in models if m != model]
            supabase.table("users").upsert({"user_id": owner_id, "models": new_models, "active": bool(new_models), "start_time": None if not new_models else None}).execute()
            # Βρες το user_id και models του trigger
            resp_trig = supabase.table("users").select("user_id,models,username").eq("username", trigger).execute()
            if resp_trig.data and len(resp_trig.data) > 0:
                trigger_id = resp_trig.data[0]["user_id"]
                trigger_models = resp_trig.data[0].get("models") or []
                trigger_username = resp_trig.data[0].get("username") or trigger
                if isinstance(trigger_models, str):
                    try:
                        import json
                        trigger_models = json.loads(trigger_models)
                    except Exception:
                        trigger_models = []
                # Προσθήκη του model στον trigger
                new_trigger_models = list(set(trigger_models) | {model})
                now = datetime.now(timezone.utc)
                now_iso = now.isoformat()
                now_str = now.strftime('%H:%M')
                # Update trigger user
                supabase.table("users").upsert({"user_id": trigger_id, "models": new_trigger_models, "active": True, "start_time": now_iso}).execute()
                # Insert shift logs
                supabase.table("shifts").insert({
                    "user_id": trigger_id,
                    "username": trigger_username,
                    "models": [model],
                    "start_time": now_iso,
                    "on_time": now_iso,
                    "active": True,
                    "mode": "on"
                }).execute()
                supabase.table("shifts").insert({
                    "user_id": owner_id,
                    "username": owner_username,
                    "models": [model],
                    "start_time": now_iso,
                    "on_time": now_iso,
                    "active": False,
                    "mode": "off"
                }).execute()
                # Group notifications (Shift ON/Shift OFF)
                # Find group_id: use the first group the bot is in, or set a constant if you want
                group_id = None
                if hasattr(context.bot, 'chat_ids') and context.bot.chat_ids:
                    group_id = list(context.bot.chat_ids)[0]
                else:
                    # Fallback: set your group id here
                    group_id = -1000000000000  # <-- CHANGE THIS TO YOUR GROUP ID
                # Duration: not tracked for single model, so just show '-'
                msg_on = (
                    f"🔛 Shift ON by @{trigger_username}\n"
                    f"🕒 {now_str}   ⏱ Duration: 0:00\n"
                    f"Models: {', '.join(trigger_models) if trigger_models else 'κανένα'}\n"
                    f"➕ Νέα: {model}"
                )
                msg_off = (
                    f"🔻 Shift OFF by @{owner_username}\n"
                    f"🕒 {now_str}   ⏱ Duration: 0:00\n"
                    f"Έκλεισαν: {', '.join(new_models) if new_models else 'κανένα'}\n"
                    f"Εδωσε: {model}"
                )
                try:
                    await context.bot.send_message(chat_id=group_id, text=msg_on)
                    await context.bot.send_message(chat_id=group_id, text=msg_off)
                except Exception as ex:
                    print(f"DEBUG: Failed to send group notifications: {ex}")
                await context.bot.send_message(
                    chat_id=trigger_id,
                    text=f"✅ Ο @{owner_username} αποδέχτηκε το notify και βγήκε από το model: {model}."
                )
            await query.edit_message_text(f"✅ Αποδέχτηκες το notify και βγήκες από το model: {model}.")
        elif data.startswith("notifreject_"):
            _, username, model, trigger = data.split("_", 3)
            # Ενημέρωσε τον trigger user
            resp_trig = supabase.table("users").select("user_id").eq("username", trigger).execute()
            if resp_trig.data and len(resp_trig.data) > 0:
                trigger_id = resp_trig.data[0]["user_id"]
                await context.bot.send_message(
                    chat_id=trigger_id,
                    text=f"❌ Ο @{username} απέρριψε το notify για το model: {model}."
                )
            await query.edit_message_text(f"❌ Απέρριψες το notify για το model: {model}.")
        else:
            await query.answer("Άγνωστη ενέργεια.", show_alert=True)
    except Exception as e:
        await query.answer(f"❌ Σφάλμα: {e}", show_alert=True)

# === Chatter name → Telegram handle mappings ===
CHATTER_HANDLES = {
    "Anastasis": "@Anastasiss12",
    "Ηλίας": "@elias_drag",
    "Mike": "@mikekrp",
    "Kouzou": "@Kouzounias",
    "Μακρο": "@MacRaw99",
    "Maraggos": "@Maraggos",
    "Nikos": "@nikospapadop",   
    "Petridis": "@Bull056",
    "Riggers": "@riggersss",
}
# === Greek day names constant ===
DAYS = ["Δευτέρα", "Τρίτη", "Τετάρτη", "Πέμπτη", "Παρασκευή", "Σάββατο", "Κυριακή"]

# === Google Sheets API configuration ===
SHEETS_API_KEY = "AIzaSyDBbGSp2ndjAVXLgGa_fs_GTn6EuFvtIno"
SPREADSHEET_ID = "YOUR_SPREADSHEET_ID"  # Βάλε εδώ το πραγματικό ID
SHEET_RANGE    = "Sheet1!A1:Z"

# === Timezone ===
TZ = pytz.timezone("Europe/Athens")

# === Fetch values from Google Sheet ===
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

# === /myprogram handler ===
from telegram import Update
from telegram.ext import ContextTypes

async def handle_myprogram(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    username = u.username or ""
    # Find chatter name by Telegram handle
    chatter_name = None
    for name, handle in CHATTER_HANDLES.items():
        if handle and handle.lstrip("@").lower() == username.lower():
            chatter_name = name
            break
    if not chatter_name:
        return await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="❌ Το πρόγραμμα σου δεν βρέθηκε. Βεβαιώσου ότι έχεις καταχωρήσει σωστά το handle σου.",
            reply_to_message_id=get_reply_to_message_id(update, TARGET_REPLY_TO_MESSAGE_ID)
        )

    rows = fetch_sheet_values()
    if not rows or len(rows) < 2:
        return await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="❌ Δεν βρέθηκαν δεδομένα στο sheet.",
            reply_to_message_id=get_reply_to_message_id(update, TARGET_REPLY_TO_MESSAGE_ID)
        )

    days = rows[0][1:]
    today_idx = datetime.now(TZ).weekday()
    if today_idx < 0 or today_idx >= len(days):
        return await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="❌ Σφάλμα στον προσδιορισμό της ημέρας.",
            reply_to_message_id=get_reply_to_message_id(update, TARGET_REPLY_TO_MESSAGE_ID)
        )
    # Collect assignments as tuples (model_name, entry_text)
    assignments = []
    for row in rows[1:]:
        model_name = row[0].strip()
        cell = row[1 + today_idx].strip() if len(row) > 1 + today_idx else ""
        if not cell:
            continue
        parts = [p.strip() for p in cell.splitlines() if p.strip()]
        for p in parts:
            if chatter_name in p:
                assignments.append((model_name, p))

    # Build header
    header = datetime.now(TZ).strftime("📋 Πρόγραμμα για σήμερα (%A)")
    raw_handle = CHATTER_HANDLES.get(chatter_name, "")
    header += f"\nChatter: {chatter_name}"
    if raw_handle:
        header += f", {raw_handle}"

    # Prepare lines for each assignment
    lines = []
    for model_name, entry in assignments:
        entry = entry.strip()
        # Extract time range anywhere in text
        m = re.search(r"(\d{1,2}:\d{2}\s*-\s*\d{1,2}:\d{2})", entry)
        time_range = m.group(1) if m else entry
        # Determine shift type
        try:
            start_hour = int(time_range.split(":")[0])
        except ValueError: # Skip entries without a leading numeric hour
            continue
        shift_type = "πρωινή βάρδια" if start_hour < 18 else "απογευματινή βάρδια"
        lines.append(f"{time_range}  {model_name} ({shift_type})")

    # Send result
    message = header + "\n" + ("\n".join(lines) if lines else "– Δεν έχεις βάρδια.")
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=message,
        reply_to_message_id=get_reply_to_message_id(update, TARGET_REPLY_TO_MESSAGE_ID)
    )

# --- /weekly_program handler ---
async def handle_weekly_program(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rows = fetch_sheet_values()
    if not rows or len(rows) < 2:
        return await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="❌ Το sheet είναι άδειο ή δεν βρέθηκαν δεδομένα στο sheet.",
            reply_to_message_id=get_reply_to_message_id(update, TARGET_REPLY_TO_MESSAGE_ID)
        )
    days = rows[0][1:]
    # build schedule per day, group by model
    schedule = {day: {} for day in days}  # day -> {model: [shifts]}
    for row in rows[1:]:
        model = row[0].strip()
        for idx, cell in enumerate(row[1:]):
            cell_text = cell.strip()
            if not cell_text:
                continue
            parts = [p.strip() for p in cell_text.splitlines() if p.strip()]
            if not parts:
                continue
            if model not in schedule[days[idx]]:
                schedule[days[idx]][model] = []
            schedule[days[idx]][model].extend(parts)
    # Emoji per day
    day_emojis = ["🌞"]*5 + ["🎉", "🎉"]
    MAX_MSG_LEN = 4000
    for i, day in enumerate(days):
        day_entries = schedule.get(day, {})
        msg = f"<b>{day_emojis[i]} {day}</b>\n"
        if not day_entries:
            msg += "• –"
        else:
            for model, shifts in day_entries.items():
                msg += f"\n<b>• {model}</b>\n"
                for shift in shifts:
                    import re
                    m = re.search(r"(\d{1,2}:\d{2}\s*-\s*\d{1,2}:\d{2})", shift)
                    if m:
                        time_str = m.group(1)
                        rest = shift.replace(time_str, "").strip(" -:")
                        msg += f"⏰ <b>{time_str}</b> | {rest}\n"
                    else:
                        msg += f"{shift}\n"
        # Split if too long
        for chunk in [msg[j:j+MAX_MSG_LEN] for j in range(0, len(msg), MAX_MSG_LEN)]:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=chunk.strip(),
                parse_mode="HTML",
                reply_to_message_id=get_reply_to_message_id(update, TARGET_REPLY_TO_MESSAGE_ID)
            )
            await asyncio.sleep(0.5)

# --- /durations_today handler ---
async def handle_durations_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    import pytz
    from datetime import datetime, timezone, timedelta
    tz = pytz.timezone("Europe/Athens")
    now = datetime.now(tz)
    today_str = now.strftime("%Y-%m-%d")
    # Πάρε όλα τα shifts
    try:
        resp = supabase.table("shifts").select("user_id,username,models,start_time,on_time,active,mode").execute()
        shifts = resp.data if resp and resp.data else []
    except Exception as e:
        return await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f"❌ Σφάλμα Supabase: {e}",
            reply_to_message_id=get_reply_to_message_id(update, TARGET_REPLY_TO_MESSAGE_ID)
        )
    # Ομαδοποίησε τα shifts ανά χρήστη
    user_shifts = defaultdict(list)
    for s in shifts:
        st = s.get("start_time")
        if not st:
            continue
        try:
            st_dt = datetime.fromisoformat(st).astimezone(tz)
        except Exception:
            continue
        # Αγνόησε shifts που ξεκινούν στο μέλλον
        if st_dt > now:
            continue
        if st_dt.strftime("%Y-%m-%d") != today_str:
            continue
        user_shifts[s.get("user_id")].append(s)
    # --- Εμφάνιση όλων των chatters, ακόμα και χωρίς shift ---
    all_usernames = set([s.get("username") or uid for uid, shifts in user_shifts.items() for s in shifts]) | set(CHATTER_HANDLES.keys())
    username_to_result = {}
    for user_id in user_shifts:
        shifts = user_shifts[user_id]
        username = shifts[0].get("username") if shifts else user_id
        # Φιλτράρω μόνο τα σημερινά ON/OFF
        filtered_shifts = [s for s in sorted(shifts, key=lambda x: x.get("start_time")) if s.get("mode") in ("on", "off")]
        total_seconds = 0
        on_time = None
        for s in filtered_shifts:
            mode = s.get("mode")
            st = s.get("start_time")
            try:
                st_dt = datetime.fromisoformat(st).astimezone(tz)
            except Exception:
                continue
            if st_dt > now:
                continue
            if mode == "on":
                if on_time is None:
                    on_time = st_dt
            elif mode == "off":
                if on_time is not None and st_dt > on_time:
                    delta = (st_dt - on_time).total_seconds()
                    if 0 < delta <= 16*3600:
                        total_seconds += delta
                    on_time = None
                else:
                    on_time = None
        # Αν υπάρχει ανοιχτό ON μέχρι τώρα
        if on_time is not None:
            delta = (now - on_time).total_seconds()
            if 0 < delta <= 16*3600:
                total_seconds += delta
        h = int(total_seconds // 3600)
        m = int((total_seconds % 3600) // 60)
        username_to_result[username] = (username, h, m, [])
    # Εμφάνιση όλων των chatters (και όσων δεν έχουν shift)
    for chatter in sorted(all_usernames):
        if chatter in username_to_result:
            username, h, m, debug_pairings = username_to_result[chatter]
            if h == 0 and m == 0:
                continue  # Εμφάνισε μόνο όσους έχουν duration > 0
        else:
            continue  # Αγνόησε όσους δεν έχουν shift
        msg = f"<b>{username}</b>: {h}:{m:02d} ώρες\n"
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=msg.strip(),
            parse_mode="HTML",
            reply_to_message_id=get_reply_to_message_id(update, TARGET_REPLY_TO_MESSAGE_ID)
        )
    return

# --- /durations <YYYY-MM> handler ---
async def handle_durations_month(update: Update, context: ContextTypes.DEFAULT_TYPE):
    import pytz
    from datetime import datetime, timezone, timedelta
    from calendar import monthrange
    tz = pytz.timezone("Europe/Athens")
    now = datetime.now(tz)
    # Parse argument
    month_arg = None
    if context.args and len(context.args) > 0:
        month_arg = context.args[0]
    elif update.message and update.message.text:
        # For shortcut commands
        if update.message.text.lower().startswith("/durations_june"):
            month_arg = f"{now.year}-06"
        elif update.message.text.lower().startswith("/durations_may"):
            month_arg = f"{now.year}-05"
        elif update.message.text.lower().startswith("/durations_july"):
            month_arg = f"{now.year}-07"
    if not month_arg:
        month_arg = now.strftime("%Y-%m")
    try:
        year, month = map(int, month_arg.split("-"))
    except Exception:
        return await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="❌ Δώσε μήνα σε μορφή YYYY-MM, π.χ. /durations 2024-06",
            reply_to_message_id=get_reply_to_message_id(update, TARGET_REPLY_TO_MESSAGE_ID)
        )
    # Υπολόγισε αρχή και τέλος μήνα
    first_day = datetime(year, month, 1, tzinfo=tz)
    last_day = datetime(year, month, monthrange(year, month)[1], 23, 59, 59, tzinfo=tz)
    num_days = monthrange(year, month)[1]
    # Πάρε όλα τα shifts του μήνα
    try:
        resp = supabase.table("shifts").select("user_id,username,models,start_time,on_time,active,mode").execute()
        shifts = resp.data if resp and resp.data else []
    except Exception as e:
        return await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f"❌ Σφάλμα Supabase: {e}",
            reply_to_message_id=get_reply_to_message_id(update, TARGET_REPLY_TO_MESSAGE_ID)
        )
    # Ομαδοποίησε τα shifts ανά χρήστη
    user_shifts = defaultdict(list)
    for s in shifts:
        st = s.get("start_time")
        if not st:
            continue
        try:
            st_dt = datetime.fromisoformat(st)
            st_dt = st_dt.astimezone(tz)
        except Exception:
            continue
        # Αγνόησε shifts που ξεκινούν στο μέλλον
        if st_dt > now:
            continue
        if not (first_day <= st_dt <= last_day):
            continue
        user_shifts[s.get("user_id")].append(s)
    # === DEBUG PRINT: δείξε τα shifts που βρέθηκαν ===
    debug_lines = []
    for user_id, shifts in user_shifts.items():
        username = shifts[0].get("username") if shifts else user_id
        debug_lines.append(f"<b>{username}</b>:")
        for s in sorted(shifts, key=lambda x: x.get("start_time")):
            debug_lines.append(f"  {s.get('mode','?')} | {s.get('start_time','?')}")
    if debug_lines:
        debug_msg = "<b>DEBUG: Shifts found for this month:</b>\n" + "\n".join(debug_lines)
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=debug_msg,
            parse_mode="HTML",
            reply_to_message_id=get_reply_to_message_id(update, TARGET_REPLY_TO_MESSAGE_ID)
        )
    # Υπολόγισε συνολικό duration και ανά μέρα ανά χρήστη
    results = []
    for user_id, shifts in user_shifts.items():
        username = None
        total_seconds = 0
        # Φιλτράρω μόνο ON/OFF (αγνοώ break)
        filtered_shifts = [s for s in sorted(shifts, key=lambda x: x.get("start_time")) if s.get("mode") in ("on", "off")]
        on_time = None
        # dict: date_str -> seconds
        day_durations = { (first_day + timedelta(days=i)).strftime("%Y-%m-%d"): 0 for i in range(num_days) }
        for s in filtered_shifts:
            mode = s.get("mode")
            st = s.get("start_time")
            username = s.get("username") or user_id
            try:
                st_dt = datetime.fromisoformat(st).astimezone(tz)
            except Exception:
                continue
            # Αγνόησε shifts που ξεκινούν στο μέλλον
            if st_dt > now:
                continue
            if mode == "on":
                # Αν υπάρχει ήδη ανοιχτό ON, κρατάμε μόνο το τελευταίο
                on_time = st_dt
            elif mode == "off":
                if on_time is None:
                    continue  # Αγνόησε OFF χωρίς προηγούμενο ON
                off_time = st_dt
                # Αν το OFF είναι πριν ή ίσο με το ON, αγνόησέ το
                if off_time <= on_time:
                    on_time = None
                    continue
                # Αν το shift ξεκινάει και τελειώνει ίδια μέρα, απλό
                if on_time.date() == off_time.date():
                    day_str = on_time.strftime("%Y-%m-%d")
                    delta = (off_time - on_time).total_seconds()
                    # Αγνόησε duration > 16 ώρες ή αρνητικά ή μηδενικά
                    if 0 < delta <= 16*3600:
                        day_durations[day_str] += delta
                        total_seconds += delta
                else:
                    # Αν το shift περνάει μέρες, μοίρασέ το σωστά
                    cur = on_time
                    while cur.date() < off_time.date():
                        day_end = datetime(cur.year, cur.month, cur.day, 23, 59, 59, tzinfo=tz)
                        delta = (day_end - cur).total_seconds()
                        day_str = cur.strftime("%Y-%m-%d")
                        if 0 < delta <= 16*3600:
                            day_durations[day_str] += delta
                            total_seconds += delta
                        cur = day_end + timedelta(seconds=1)
                    # Τελευταία μέρα
                    day_str = off_time.strftime("%Y-%m-%d")
                    delta = (off_time - datetime(off_time.year, off_time.month, off_time.day, 0, 0, 0, tzinfo=tz)).total_seconds()
                    if 0 < delta <= 16*3600:
                        day_durations[day_str] += delta
                        total_seconds += delta
                on_time = None
        if on_time:
            # Αν υπάρχει ανοιχτό shift, μετράμε μέχρι τέλος μήνα ή τώρα (ό,τι είναι μικρότερο)
            end_time = min(last_day, now)
            if end_time <= on_time:
                continue
            cur = on_time
            while cur.date() < end_time.date():
                day_end = datetime(cur.year, cur.month, cur.day, 23, 59, 59, tzinfo=tz)
                delta = (day_end - cur).total_seconds()
                day_str = cur.strftime("%Y-%m-%d")
                if 0 < delta <= 16*3600:
                    day_durations[day_str] += delta
                    total_seconds += delta
                cur = day_end + timedelta(seconds=1)
            # Τελευταία μέρα
            day_str = end_time.strftime("%Y-%m-%d")
            delta = (end_time - datetime(end_time.year, end_time.month, end_time.day, 0, 0, 0, tzinfo=tz)).total_seconds()
            if 0 < delta <= 16*3600:
                day_durations[day_str] += delta
                total_seconds += delta
        # ΠΡΟΣΘΕΤΩ: Εμφανίζω όλους τους χρήστες, ακόμα κι αν total_seconds == 0
        h = int(total_seconds // 3600)
        m = int((total_seconds % 3600) // 60)
        results.append((username, h, m, day_durations))
    results.sort(key=lambda x: (-x[1], -x[2], x[0]))
    # Format message
    month_label = f"{year}-{month:02d}"
    if not results:
        msg = f"Δεν υπάρχουν βάρδιες για τον μήνα {month_label}."
    else:
        msg = f"<b>📊 Συνολικές ώρες για {month_label}:</b>\n"
        for username, h, m, day_durations in results:
            msg += f"<b>{username}</b>: {h}:{m:02d} ώρες\n"
            for day in sorted(day_durations.keys()):
                sec = day_durations[day]
                if sec > 0:
                    dh = int(sec // 3600)
                    dm = int((sec % 3600) // 60)
                    msg += f"  {day}: {dh}:{dm:02d} ώρες\n"
                else:
                    msg += f"  {day}: –\n"
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=msg.strip(),
        parse_mode="HTML",
        reply_to_message_id=get_reply_to_message_id(update, TARGET_REPLY_TO_MESSAGE_ID)
    )
    # --- Εμφάνιση όλων των chatters, ακόμα και χωρίς shift ---
    all_usernames = set([x[0] for x in results]) | set(CHATTER_HANDLES.keys())
    username_to_result = {x[0]: x for x in results}
    for chatter in sorted(all_usernames):
        if chatter in username_to_result:
            username, h, m, day_durations = username_to_result[chatter]
        else:
            username = chatter
            h, m = 0, 0
            # Δημιουργώ κενό dict για όλες τις μέρες
            day_durations = { (first_day + timedelta(days=i)).strftime("%Y-%m-%d"): 0 for i in range(num_days) }
        msg = f"<b>{username}</b>: {h}:{m:02d} ώρες\n"
        for day in sorted(day_durations.keys()):
            sec = day_durations[day]
            if sec > 0:
                dh = int(sec // 3600)
                dm = int((sec % 3600) // 60)
                msg += f"  {day}: {dh}:{dm:02d} ώρες\n"
            else:
                msg += f"  {day}: –\n"
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=msg.strip(),
            parse_mode="HTML",
            reply_to_message_id=get_reply_to_message_id(update, TARGET_REPLY_TO_MESSAGE_ID)
        )
        # --- DEBUG: pairing info ---
        if chatter in username_to_result:
            _, _, _, day_durations = username_to_result[chatter]
            debug_lines = []
            for day in sorted(day_durations.keys()):
                sec = day_durations[day]
                debug_lines.append(f"{day}: {sec//3600}:{int((sec%3600)//60):02d} ώρες" if sec > 0 else f"{day}: –")
            debug_msg = f"<b>DEBUG: {username} pairings</b>\n" + "\n".join(debug_lines)
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=debug_msg,
                parse_mode="HTML",
                reply_to_message_id=get_reply_to_message_id(update, TARGET_REPLY_TO_MESSAGE_ID)
            )
    return

# --- /mistakeon Command ---
async def mistakeon_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not update.message:
        return
    user_id = str(update.effective_user.id)
    user = update.effective_user
    
    # Βρες τα ήδη ενεργά models του χρήστη
    active_models = []
    start_time = None
    try:
        resp = supabase.table("users").select("models,start_time,active").eq("user_id", user_id).execute()
        if resp.data and len(resp.data) > 0:
            active = resp.data[0].get("active")
            if active:
                active_models = resp.data[0].get("models") or []
                if isinstance(active_models, str):
                    try:
                        import json
                        active_models = json.loads(active_models)
                    except Exception:
                        active_models = []
                start_time = resp.data[0].get("start_time")
    except Exception:
        pass
    
    # Βρες τα μοντέλα που είναι ήδη ενεργά από άλλους χρήστες
    unavailable_models = set()
    try:
        resp = supabase.table("users").select("models").eq("active", True).execute()
        for user_data in resp.data:
            models = user_data.get("models") or []
            if isinstance(models, str):
                try:
                    import json
                    models = json.loads(models)
                except Exception:
                    models = []
            unavailable_models.update(models)
    except Exception:
        pass
    
    # Φιλτράρω μόνο τα mistake models που είναι διαθέσιμα
    available_mistake_models = [m for m in MISTAKE_MODELS if m not in unavailable_models]
    
    if not available_mistake_models:
        await update.message.reply_text("Δεν υπάρχουν διαθέσιμα mistake models αυτή τη στιγμή.", reply_to_message_id=get_reply_to_message_id(update, TARGET_REPLY_TO_MESSAGE_ID))
        return
    
    selected_models = set()
    sent = await update.message.reply_text(
        "Επίλεξε mistake models για να κάνεις on:",
        reply_markup=build_mistakeon_keyboard(selected_models, unavailable_models)
    )
    
    if context.chat_data is not None and 'mistakeon_sessions' not in context.chat_data:
        context.chat_data['mistakeon_sessions'] = {}
    if context.chat_data is not None:
        context.chat_data['mistakeon_sessions'][sent.message_id] = {
            'initiator': user_id,
            'selected_models': selected_models,
            'unavailable_models': unavailable_models
        }

def build_mistakeon_keyboard(selected, unavailable):
    keyboard = []
    row = []
    for i, model in enumerate(MISTAKE_MODELS, 1):
        if model in unavailable:
            row.append(dbg_btn(f"🔒 {model}", "ignore"))
        else:
            checked = "🟢 " if model in selected else ""
            row.append(dbg_btn(f"{checked}{model}", f"mistakeon_{model}"))
        if i % 4 == 0 or i == len(MISTAKE_MODELS):
            keyboard.append(row)
            row = []
    keyboard.append([dbg_btn("✅ OK", "mistakeon_ok"), dbg_btn("❌ Cancel", "cancel_action")])
    return InlineKeyboardMarkup(keyboard)

async def mistakeon_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    print(f"DEBUG: mistakeon_callback called with data={query.data if query and query.data else 'None'}")
    if query is None or context.chat_data is None or query.message is None or query.data is None:
        return
    user = query.from_user
    if user is None:
        return
    user_id = str(user.id)
    msg = query.message
    
    session = context.chat_data.get('mistakeon_sessions', {}).get(msg.message_id) if context.chat_data and context.chat_data.get('mistakeon_sessions') else None
    if not session:
        await query.answer("Αυτή η επιλογή δεν είναι πλέον ενεργή.", show_alert=True)
        return
    
    initiator_id = session['initiator']
    selected = session['selected_models']
    unavailable = session['unavailable_models']
    
    if user_id != initiator_id:
        await query.answer("Δεν έχεις δικαίωμα να κάνεις αυτή την ενέργεια", show_alert=True)
        return
    
    data = query.data
    if data == "ignore":
        await query.answer("Το μοντέλο αυτή τη στιγμή είναι ήδη on", show_alert=True)
        return
    
    elif data == "mistakeon_ok":
        print(f"DEBUG: OK button pressed! selected={list(selected)}")
        if not selected:
            await query.answer("Επίλεξε τουλάχιστον ένα μοντέλο!", show_alert=True)
            return
        
        # Ανάκτηση προηγούμενων models και start_time
        old_models = []
        old_start_time = None
        had_no_models = False
        try:
            resp = supabase.table("users").select("models,start_time").eq("user_id", user_id).execute()
            if resp.data and len(resp.data) > 0:
                old_models = resp.data[0].get("models") or []
                if isinstance(old_models, str):
                    try:
                        import json
                        old_models = json.loads(old_models)
                    except Exception:
                        old_models = []
                if not old_models:
                    had_no_models = True
                if old_models:
                    shift_resp = supabase.table("mistake_shifts").select("start_time").eq("user_id", user_id).eq("mode", "on").order("start_time", desc=True).limit(1).execute()
                    if shift_resp.data and len(shift_resp.data) > 0:
                        old_start_time = shift_resp.data[0].get("start_time")
        except Exception:
            pass
        
        # Υπολογισμός duration
        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()
        duration_str = "-"
        try:
            resp = supabase.table("users").select("start_time").eq("user_id", user_id).execute()
            start_time = None
            if resp.data and len(resp.data) > 0:
                start_time = resp.data[0].get("start_time")
            if start_time:
                old_dt = datetime.fromisoformat(start_time)
                delta = now - old_dt
                h = int(delta.total_seconds() // 3600)
                m = int((delta.total_seconds() % 3600) // 60)
                duration_str = f"{h}:{m:02d}"
            else:
                duration_str = "0:00"
        except Exception as e:
            print(f"DEBUG: Exception στο duration υπολογισμό (mistakeon): {e}")
            duration_str = "0:00"
        
        # Αποθήκευση νέων models και start_time
        try:
            # Πρόσθεσε τα mistake models στα ενεργά (χωρίς να πειράζεις τα υπόλοιπα)
            all_models = set(old_models) | set(selected)
            starting_shift = not old_models
            print(f"DEBUG: mistakeon upsert users: user_id={user_id} models={list(all_models)} selected={list(selected)} old_models={old_models}")
            supabase.table("users").upsert({
                "user_id": user_id,
                "username": user.username or f"id_{user_id}",
                "first_name": user.first_name or "",
                "models": list(all_models),
                "active": True,
                "start_time": now_iso if starting_shift else old_start_time if old_start_time else now_iso
            }).execute()

            # Καταγραφή shift ΠΑΝΤΑ (όχι μόνο αν δεν είχε κανένα)
            if selected:
                print(f"DEBUG: mistakeon insert mistake_shifts: user_id={user_id} models={list(selected)}")
                supabase.table("mistake_shifts").insert({
                    "user_id": user_id,
                    "username": user.username or f"id_{user_id}",
                    "models": list(selected),
                    "start_time": now_iso,
                    "on_time": now_iso,
                    "active": True,
                    "mode": "on"
                }).execute()

            # Φιλτράρω μόνο τα mistake models που ήταν ήδη ενεργά
            old_mistake_models = [m for m in old_models if m in MISTAKE_MODELS]
            
            msg_text = (
                f"⚡ MISTAKE MODE ON ⚡\n"
                f"👤 @{user.username}\n"
                f"🕐 {now.strftime('%H:%M')} | ⏱ {duration_str}\n"
                f"📋 Mistake Models: {'Μόλις μπήκε!' if not old_mistake_models else ', '.join(old_mistake_models)}\n"
                f"🎯 Νέα: {', '.join(selected) if selected else 'κανένα'}"
            )
            try:
                await query.edit_message_text(msg_text)
            except Exception as ex:
                print(f"DEBUG: edit_message_text error: {ex}")
            context.chat_data['mistakeon_sessions'].pop(msg.message_id, None)
        except Exception as e:
            await query.edit_message_text(f"❌ Σφάλμα αποθήκευσης: {e}")
    
    elif data.startswith("mistakeon_"):
        model = data[10:]  # Remove "mistakeon_" prefix
        if model in unavailable:
            await query.answer("Το model είναι ήδη ενεργό από άλλον χρήστη.", show_alert=True)
            return
        if model in selected:
            selected.remove(model)
        else:
            selected.add(model)
        session['selected_models'] = selected
        try:
            await query.edit_message_reply_markup(reply_markup=build_mistakeon_keyboard(selected, unavailable))
        except Exception as ex:
            print(f"DEBUG: edit_message_reply_markup error: {ex}")
        await query.answer()

# --- /mistakeoff Command ---
async def mistakeoff_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not update.message:
        return
    user_id = str(update.effective_user.id)
    user = update.effective_user
    
    # Βρες τα ενεργά models του χρήστη
    active_models = []
    start_time = None
    try:
        resp = supabase.table("users").select("models,start_time,active").eq("user_id", user_id).execute()
        if resp.data and len(resp.data) > 0:
            active = resp.data[0].get("active")
            if active:
                active_models = resp.data[0].get("models") or []
                if isinstance(active_models, str):
                    try:
                        import json
                        active_models = json.loads(active_models)
                    except Exception:
                        active_models = []
                start_time = resp.data[0].get("start_time")
    except Exception:
        pass
    
    # Φιλτράρω μόνο τα mistake models που είναι ενεργά
    active_mistake_models = [m for m in active_models if m in MISTAKE_MODELS]
    
    if not active_mistake_models:
        await update.message.reply_text("Δεν έχεις ενεργά mistake models.", reply_to_message_id=get_reply_to_message_id(update, TARGET_REPLY_TO_MESSAGE_ID))
        return
    
    selected_models = set()
    sent = await update.message.reply_text(
        "Επίλεξε mistake models για να κάνεις off:",
        reply_markup=build_mistakeoff_keyboard(active_mistake_models, selected_models)
    )
    
    if context.chat_data is not None and 'mistakeoff_sessions' not in context.chat_data:
        context.chat_data['mistakeoff_sessions'] = {}
    if context.chat_data is not None:
        context.chat_data['mistakeoff_sessions'][sent.message_id] = {
            'initiator': user_id,
            'active_models': set(active_models),
            'selected_models': selected_models,
            'start_time': start_time
        }

def build_mistakeoff_keyboard(active_models, selected):
    keyboard = []
    row = []
    for i, model in enumerate(active_models, 1):
        checked = "🔴 " if model in selected else ""
        row.append(dbg_btn(f"{checked}{model}", f"mistakeoff_{model}"))
        if i % 4 == 0 or i == len(active_models):
            keyboard.append(row)
            row = []
    keyboard.append([dbg_btn("✅ OK", "mistakeoff_ok"), dbg_btn("❌ Cancel", "cancel_action")])
    return InlineKeyboardMarkup(keyboard)

async def mistakeoff_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    print(f"DEBUG: mistakeoff_callback called with data={query.data if query and query.data else 'None'}")
    if query is None or context.chat_data is None or query.message is None or query.data is None:
        return
    user = query.from_user
    if user is None:
        return
    user_id = str(user.id)
    msg = query.message
    
    session = context.chat_data.get('mistakeoff_sessions', {}).get(msg.message_id) if context.chat_data and context.chat_data.get('mistakeoff_sessions') else None
    if not session:
        await query.answer("Αυτή η επιλογή δεν είναι πλέον ενεργή.", show_alert=True)
        return
    
    initiator_id = session['initiator']
    active_models = session['active_models']
    selected = session['selected_models']
    start_time = session['start_time']
    
    if user_id != initiator_id:
        await query.answer("Δεν έχεις δικαίωμα να κάνεις αυτή την ενέργεια", show_alert=True)
        return
    
    data = query.data
    if data == "mistakeoff_ok":
        print(f"DEBUG: mistakeoff OK button pressed! selected={list(selected)}")
        if not selected:
            await query.answer("Επίλεξε τουλάχιστον ένα μοντέλο!", show_alert=True)
            return
        
        # Υπολογισμός duration
        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()
        duration_str = "-"
        try:
            if start_time:
                old_dt = datetime.fromisoformat(start_time)
                delta = now - old_dt
                h = int(delta.total_seconds() // 3600)
                m = int((delta.total_seconds() % 3600) // 60)
                duration_str = f"{h}:{m:02d}"
            else:
                duration_str = "0:00"
        except Exception as e:
            print(f"DEBUG: Exception στο duration υπολογισμό (mistakeoff): {e}")
            duration_str = "0:00"
        
        # Αποθήκευση αλλαγών
        try:
            remaining_models = list(active_models - selected)
            supabase.table("users").upsert({
                "user_id": user_id,
                "username": user.username or f"id_{user_id}",
                "first_name": user.first_name or "",
                "models": remaining_models,
                "active": bool(remaining_models),
                "start_time": None if not remaining_models else start_time
            }).execute()
            
            # Εισαγωγή shift log
            supabase.table("mistake_shifts").insert({
                "user_id": user_id,
                "username": user.username or f"id_{user_id}",
                "models": list(selected),
                "start_time": start_time,
                "on_time": now_iso,
                "active": False,
                "mode": "off"
            }).execute()
            
            # Φιλτράρω μόνο τα mistake models που απομένουν
            remaining_mistake_models = [m for m in remaining_models if m in MISTAKE_MODELS]
            
            msg_text = (
                f"🛑 MISTAKE MODE OFF 🛑\n"
                f"👤 @{user.username}\n"
                f"🕐 {now.strftime('%H:%M')} | ⏱ {duration_str}\n"
                f"❌ Έκλεισαν: {', '.join(selected)}\n"
                f"{'🎉 Τέλειωσες τη βάρδιά σου!' if not remaining_mistake_models else '✅ Mistake Models: ' + ', '.join(remaining_mistake_models)}"
            )
            await query.edit_message_text(msg_text)
            context.chat_data['mistakeoff_sessions'].pop(msg.message_id, None)
        except Exception as e:
            await query.edit_message_text(f"❌ Σφάλμα αποθήκευσης: {e}")
    
    elif data.startswith("mistakeoff_"):
        model = data[11:]  # Remove "mistakeoff_" prefix
        if model in selected:
            selected.remove(model)
        else:
            selected.add(model)
        session['selected_models'] = selected
        try:
            await query.edit_message_reply_markup(reply_markup=build_mistakeoff_keyboard([m for m in active_models if m in MISTAKE_MODELS], selected))
        except Exception as ex:
            print(f"DEBUG: edit_message_reply_markup error: {ex}")
        await query.answer()

# --- /liveon Command ---
async def liveon_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not update.message:
        return
    user_id = str(update.effective_user.id)
    user = update.effective_user
    
    # Βρες τα ήδη ενεργά models του χρήστη
    active_models = []
    start_time = None
    try:
        resp = supabase.table("users").select("models,start_time,active").eq("user_id", user_id).execute()
        if resp.data and len(resp.data) > 0:
            active = resp.data[0].get("active")
            if active:
                active_models = resp.data[0].get("models") or []
                if isinstance(active_models, str):
                    try:
                        import json
                        active_models = json.loads(active_models)
                    except Exception:
                        active_models = []
                start_time = resp.data[0].get("start_time")
    except Exception:
        pass
    
    # Βρες τα live models που είναι ήδη σε ενεργές live sessions από άλλους χρήστες
    unavailable_live_models = set()
    try:
        resp = supabase.table("live_sessions").select("models").eq("active", True).execute()
        for session in resp.data:
            models = session.get("models") or []
            if isinstance(models, str):
                try:
                    import json
                    models = json.loads(models)
                except Exception:
                    models = []
            # Φιλτράρω μόνο τα live models
            live_models_in_session = [m for m in models if m in LIVE_MODELS]
            unavailable_live_models.update(live_models_in_session)
    except Exception:
        pass
    
    # Φιλτράρω μόνο τα live models που είναι διαθέσιμα
    available_live_models = [m for m in LIVE_MODELS if m not in unavailable_live_models]
    
    if not available_live_models:
        await update.message.reply_text("Δεν υπάρχουν διαθέσιμα live models αυτή τη στιγμή.", reply_to_message_id=get_reply_to_message_id(update, TARGET_REPLY_TO_MESSAGE_ID))
        return
    
    selected_models = set()
    sent = await update.message.reply_text(
        "Επίλεξε live models για να κάνεις on:",
        reply_markup=build_liveon_keyboard(selected_models, unavailable_live_models)
    )
    
    if context.chat_data is not None and 'liveon_sessions' not in context.chat_data:
        context.chat_data['liveon_sessions'] = {}
    if context.chat_data is not None:
        context.chat_data['liveon_sessions'][sent.message_id] = {
            'initiator': user_id,
            'selected_models': selected_models,
            'unavailable_models': unavailable_live_models
        }

def build_liveon_keyboard(selected, unavailable):
    keyboard = []
    row = []
    for i, model in enumerate(LIVE_MODELS, 1):
        if model in unavailable:
            row.append(dbg_btn(f"🔒 {model}", "ignore"))
        else:
            checked = "🟢 " if model in selected else ""
            row.append(dbg_btn(f"{checked}{model}", f"liveon_{model}"))
        if i % 4 == 0 or i == len(LIVE_MODELS):
            keyboard.append(row)
            row = []
    keyboard.append([dbg_btn("✅ OK", "liveon_ok"), dbg_btn("❌ Cancel", "cancel_action")])
    return InlineKeyboardMarkup(keyboard)

async def liveon_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    print(f"DEBUG: liveon_callback called with data={query.data if query and query.data else 'None'}")
    if query is None or context.chat_data is None or query.message is None or query.data is None:
        return
    user = query.from_user
    if user is None:
        return
    user_id = str(user.id)
    msg = query.message
    
    session = context.chat_data.get('liveon_sessions', {}).get(msg.message_id) if context.chat_data and context.chat_data.get('liveon_sessions') else None
    if not session:
        await query.answer("Αυτή η επιλογή δεν είναι πλέον ενεργή.", show_alert=True)
        return
    
    initiator_id = session['initiator']
    selected = session['selected_models']
    unavailable = session['unavailable_models']
    
    if user_id != initiator_id:
        await query.answer("Δεν έχεις δικαίωμα να κάνεις αυτή την ενέργεια", show_alert=True)
        return
    
    data = query.data
    if data == "ignore":
        await query.answer("Το μοντέλο αυτή τη στιγμή είναι ήδη σε live session", show_alert=True)
        return
    
    elif data == "liveon_ok":
        print(f"DEBUG: liveon OK button pressed! selected={list(selected)}")
        if not selected:
            await query.answer("Επίλεξε τουλάχιστον ένα μοντέλο!", show_alert=True)
            return
        
        # Ανάκτηση προηγούμενων models και start_time
        old_models = []
        old_start_time = None
        had_no_models = False
        try:
            resp = supabase.table("users").select("models,start_time").eq("user_id", user_id).execute()
            if resp.data and len(resp.data) > 0:
                old_models = resp.data[0].get("models") or []
                if isinstance(old_models, str):
                    try:
                        import json
                        old_models = json.loads(old_models)
                    except Exception:
                        old_models = []
                if not old_models:
                    had_no_models = True
                if old_models:
                    shift_resp = supabase.table("live_sessions").select("start_time").eq("user_id", user_id).eq("mode", "on").order("start_time", desc=True).limit(1).execute()
                    if shift_resp.data and len(shift_resp.data) > 0:
                        old_start_time = shift_resp.data[0].get("start_time")
        except Exception:
            pass
        
        # Υπολογισμός duration
        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()
        duration_str = "-"
        try:
            resp = supabase.table("users").select("start_time").eq("user_id", user_id).execute()
            start_time = None
            if resp.data and len(resp.data) > 0:
                start_time = resp.data[0].get("start_time")
            if start_time:
                old_dt = datetime.fromisoformat(start_time)
                delta = now - old_dt
                h = int(delta.total_seconds() // 3600)
                m = int((delta.total_seconds() % 3600) // 60)
                duration_str = f"{h}:{m:02d}"
            else:
                duration_str = "0:00"
        except Exception as e:
            print(f"DEBUG: Exception στο duration υπολογισμό (liveon): {e}")
            duration_str = "0:00"
        
        # Αποθήκευση νέων models και start_time
        try:
            # Πρόσθεσε τα live models στα ενεργά (χωρίς να πειράζεις τα υπόλοιπα)
            all_models = set(old_models) | set(selected)
            starting_shift = not old_models
            print(f"DEBUG: liveon upsert users: user_id={user_id} models={list(all_models)} selected={list(selected)} old_models={old_models}")
            supabase.table("users").upsert({
                "user_id": user_id,
                "username": user.username or f"id_{user_id}",
                "first_name": user.first_name or "",
                "models": list(all_models),
                "active": True,
                "start_time": now_iso if starting_shift else old_start_time if old_start_time else now_iso
            }).execute()

            # Καταγραφή shift ΠΑΝΤΑ (όχι μόνο αν δεν είχε κανένα)
            if selected:
                print(f"DEBUG: liveon insert live_sessions: user_id={user_id} models={list(selected)}")
                supabase.table("live_sessions").insert({
                    "user_id": user_id,
                    "username": user.username or f"id_{user_id}",
                    "models": list(selected),
                    "start_time": now_iso,
                    "on_time": now_iso,
                    "active": True,
                    "mode": "on",
                    "chat_id": str(msg.chat.id) if msg and msg.chat else "0",
                    "message_id": str(msg.message_id) if msg and msg.message_id else "0"
                }).execute()

            # Ειδοποίηση χρηστών που είναι σε live sessions με τα επιλεγμένα models
            users_to_notify = []
            try:
                # Βρες όλους τους χρήστες που έχουν ενεργά live sessions με τα επιλεγμένα models
                print(f"DEBUG: Searching for active live sessions...")
                live_resp = supabase.table("live_sessions").select("user_id,username,models").eq("active", True).execute()
                print(f"DEBUG: Found {len(live_resp.data)} active live sessions")
                
                for session in live_resp.data:
                    session_models = session.get("models") or []
                    if isinstance(session_models, str):
                        try:
                            session_models = json.loads(session_models)
                        except Exception:
                            session_models = []
                    
                    print(f"DEBUG: Session user_id={session.get('user_id')}, models={session_models}")
                    
                    # Ελέγχω αν κάποιο από τα επιλεγμένα models είναι ήδη σε live session
                    conflicting_models = set(session_models) & set(selected)
                    print(f"DEBUG: Conflicting models: {conflicting_models}")
                    
                    if conflicting_models:
                        users_to_notify.append({
                            "user_id": session.get("user_id"),
                            "username": session.get("username"),
                            "models": list(conflicting_models)
                        })
                        print(f"DEBUG: Added user {session.get('username')} to notify for models {conflicting_models}")
            except Exception as e:
                print(f"DEBUG: Error finding users to notify: {e}")
            
            print(f"DEBUG: Total users to notify: {len(users_to_notify)}")

            # Φιλτράρω μόνο τα live models που ήταν ήδη ενεργά
            old_live_models = [m for m in old_models if m in LIVE_MODELS]
            
            msg_text = (
                f"🎥 LIVE MODE ON 🎥\n"
                f"👤 @{user.username}\n"
                f"🕐 {now.strftime('%H:%M')} | ⏱ {duration_str}\n"
                f"📋 Live Models: {', '.join(selected)}"
            )
            
            # Προσθήκη ειδοποίησης για χρήστες που πρέπει να βγουν
            if users_to_notify:
                msg_text += "\n\n⚠️ ΕΙΔΟΠΟΙΗΣΗ:"
                for user_info in users_to_notify:
                    models_str = ', '.join(user_info['models'])
                    msg_text += f"\n👤 @{user_info['username']} - Βγες από: {models_str}"
                    # Στείλε προσωπικό μήνυμα για κάθε model
                    for model in user_info['models']:
                        try:
                            button = InlineKeyboardMarkup([[InlineKeyboardButton("Το είδα", callback_data=f"seenlive_{model}_{msg.chat.id}")]])
                            print(f"DEBUG: Sending seenlive message to user_id={user_info['user_id']} for model={model}")
                            sent_msg = await context.bot.send_message(
                                chat_id=int(user_info['user_id']),
                                text=f"⚠️ Το μοντέλο {model} κάνει live τώρα! Βγες από το live και πάτησε το κουμπί όταν το δεις.",
                                reply_markup=button
                            )
                            print(f"DEBUG: Successfully sent seenlive message: {sent_msg.message_id}")
                        except Exception as ex:
                            print(f"DEBUG: Failed to send seenlive button to {user_info['user_id']}: {ex}")
                            # Fallback: στέλνουμε στην ομάδα
                            try:
                                button = InlineKeyboardMarkup([[InlineKeyboardButton("Το είδα", callback_data=f"seenlive_{model}_{msg.chat.id}_{user_info['user_id']}")]])
                                await context.bot.send_message(
                                    chat_id=msg.chat.id,
                                    text=f"⚠️ @{user_info['username']} - Το μοντέλο {model} κάνει live τώρα! Βγες από το live και πάτησε το κουμπί όταν το δεις.",
                                    reply_markup=button
                                )
                            except Exception as ex2:
                                print(f"DEBUG: Failed to send fallback message: {ex2}")
            
            try:
                await query.edit_message_text(msg_text)
            except Exception as ex:
                print(f"DEBUG: edit_message_text error: {ex}")
            context.chat_data['liveon_sessions'].pop(msg.message_id, None)
        except Exception as e:
            await query.edit_message_text(f"❌ Σφάλμα αποθήκευσης: {e}")
    
    elif data.startswith("liveon_"):
        model = data[7:]  # Remove "liveon_" prefix
        if model in unavailable:
            await query.answer("Το model είναι ήδη σε live session από άλλον χρήστη.", show_alert=True)
            return
        if model in selected:
            selected.remove(model)
        else:
            selected.add(model)
        session['selected_models'] = selected
        try:
            await query.edit_message_reply_markup(reply_markup=build_liveon_keyboard(selected, unavailable))
        except Exception as ex:
            print(f"DEBUG: edit_message_reply_markup error: {ex}")
        await query.answer()

# --- /liveoff Command ---
async def liveoff_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not update.message:
        return
    user_id = str(update.effective_user.id)
    user = update.effective_user
    
    # Βρες τα ενεργά live models του χρήστη
    active_live_models = []
    try:
        resp = supabase.table("live_sessions").select("models").eq("user_id", user_id).eq("active", True).execute()
        for session in resp.data:
            models = session.get("models") or []
            if isinstance(models, str):
                try:
                    import json
                    models = json.loads(models)
                except Exception:
                    models = []
            active_live_models.extend(models)
        active_live_models = list(set(active_live_models))  # Remove duplicates
    except Exception:
        pass
    
    if not active_live_models:
        await update.message.reply_text("Δεν έχεις ενεργά live models αυτή τη στιγμή.", reply_to_message_id=get_reply_to_message_id(update, TARGET_REPLY_TO_MESSAGE_ID))
        return
    
    selected_models = set()
    sent = await update.message.reply_text(
        "Επίλεξε live models για να κάνεις off:",
        reply_markup=build_liveoff_keyboard(active_live_models, selected_models)
    )
    
    if context.chat_data is not None and 'liveoff_sessions' not in context.chat_data:
        context.chat_data['liveoff_sessions'] = {}
    if context.chat_data is not None:
        context.chat_data['liveoff_sessions'][sent.message_id] = {
            'initiator': user_id,
            'selected_models': selected_models,
            'active_models': active_live_models
        }

def build_liveoff_keyboard(active_models, selected):
    keyboard = []
    row = []
    for i, model in enumerate(active_models, 1):
        checked = "🟢 " if model in selected else ""
        row.append(dbg_btn(f"{checked}{model}", f"liveoff_{model}"))
        if i % 4 == 0 or i == len(active_models):
            keyboard.append(row)
            row = []
    keyboard.append([dbg_btn("✅ OK", "liveoff_ok"), dbg_btn("❌ Cancel", "cancel_action")])
    return InlineKeyboardMarkup(keyboard)

async def liveoff_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    print(f"DEBUG: liveoff_callback called with data={query.data if query and query.data else 'None'}")
    if query is None or context.chat_data is None or query.message is None or query.data is None:
        return
    user = query.from_user
    if user is None:
        return
    user_id = str(user.id)
    msg = query.message
    
    session = context.chat_data.get('liveoff_sessions', {}).get(msg.message_id) if context.chat_data and context.chat_data.get('liveoff_sessions') else None
    if not session:
        await query.answer("Αυτή η επιλογή δεν είναι πλέον ενεργή.", show_alert=True)
        return
    
    initiator_id = session['initiator']
    selected = session['selected_models']
    active_models = session['active_models']
    
    if user_id != initiator_id:
        await query.answer("Δεν έχεις δικαίωμα να κάνεις αυτή την ενέργεια", show_alert=True)
        return
    
    data = query.data
    
    if data == "liveoff_ok":
        print(f"DEBUG: liveoff OK button pressed! selected={list(selected)}")
        if not selected:
            await query.answer("Επίλεξε τουλάχιστον ένα μοντέλο!", show_alert=True)
            return
        
        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()
        
        try:
            # Απενεργοποίηση των επιλεγμένων live sessions
            # Πρώτα βρες όλες τις ενεργές live sessions του χρήστη
            live_sessions_resp = supabase.table("live_sessions").select("user_id,models,start_time").eq("user_id", user_id).eq("active", True).execute()
            
            for session in live_sessions_resp.data:
                session_models = session.get("models") or []
                if isinstance(session_models, str):
                    try:
                        session_models = json.loads(session_models)
                    except Exception:
                        session_models = []
                
                # Ελέγχω αν η session περιέχει κάποιο από τα επιλεγμένα models
                if any(model in session_models for model in selected):
                    # Απενεργοποίηση της session χρησιμοποιώντας user_id και start_time
                    supabase.table("live_sessions").update({
                        "active": False,
                        "off_time": now_iso
                    }).eq("user_id", user_id).eq("start_time", session["start_time"]).eq("active", True).execute()
            
            # Αφαίρεση των live models από το users table
            resp = supabase.table("users").select("models").eq("user_id", user_id).execute()
            if resp.data and len(resp.data) > 0:
                current_models = resp.data[0].get("models") or []
                if isinstance(current_models, str):
                    try:
                        current_models = json.loads(current_models)
                    except Exception:
                        current_models = []
                
                # Αφαίρεση μόνο των επιλεγμένων live models
                updated_models = [m for m in current_models if m not in selected]
                
                # Ενημέρωση του users table
                supabase.table("users").update({
                    "models": updated_models
                }).eq("user_id", user_id).execute()
            
            msg_text = (
                f"🎥 LIVE MODE OFF 🎥\n"
                f"👤 @{user.username}\n"
                f"🕐 {now.strftime('%H:%M')}\n"
                f"📋 Live Models: {', '.join(selected)}"
            )
            
            try:
                await query.edit_message_text(msg_text)
            except Exception as ex:
                print(f"DEBUG: edit_message_text error: {ex}")
            context.chat_data['liveoff_sessions'].pop(msg.message_id, None)
        except Exception as e:
            await query.edit_message_text(f"❌ Σφάλμα αποθήκευσης: {e}")
    
    elif data.startswith("liveoff_"):
        model = data[8:]  # Remove "liveoff_" prefix
        if model in selected:
            selected.remove(model)
        else:
            selected.add(model)
        session['selected_models'] = selected
        try:
            await query.edit_message_reply_markup(reply_markup=build_liveoff_keyboard(active_models, selected))
        except Exception as ex:
            print(f"DEBUG: edit_message_reply_markup error: {ex}")
        await query.answer()

# --- Seen Live Callback ---
async def seenlive_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = query.from_user
    user_id = str(user.id)
    data = query.data
    # data: seenlive_{model}_{groupid} or seenlive_{model}_{groupid}_{userid}
    if not data.startswith("seenlive_"):
        return
    parts = data.split("_")
    if len(parts) < 3:
        await query.answer("Σφάλμα callback.", show_alert=True)
        return
    model = parts[1]
    group_id = parts[2]
    target_user_id = parts[3] if len(parts) > 3 else user_id
    
    # Ελέγχω ότι το πάτησε ο σωστός χρήστης (αν υπάρχει target_user_id)
    if len(parts) > 3 and user_id != target_user_id:
        await query.answer("Δεν έχεις δικαίωμα να πατήσεις αυτό το κουμπί.", show_alert=True)
        return
    try:
        # 1. Ενημέρωσε τον χρήστη
        await query.edit_message_text(f"✅ Το είδες!")
        # 2. Στείλε μήνυμα στην ομάδα
        try:
            await context.bot.send_message(
                chat_id=int(group_id),
                text=f"@{user.username} το είδε για το {model}"
            )
        except Exception as ex:
            print(f"DEBUG: Failed to send group seenlive message: {ex}")
    except Exception as e:
        await query.edit_message_text(f"❌ Σφάλμα: {e}")

# --- Seen Live ON Callback ---
async def seenliveon_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = query.from_user
    user_id = str(user.id)
    data = query.data
    # data: seenliveon_{model}_{groupid}_{userid}
    if not data.startswith("seenliveon_"):
        return
    parts = data.split("_")
    if len(parts) < 4:
        await query.answer("Σφάλμα callback.", show_alert=True)
        return
    model = parts[1]
    group_id = parts[2]
    target_user_id = parts[3]
    if user_id != target_user_id:
        await query.answer("Δεν έχεις δικαίωμα να πατήσεις αυτό το κουμπί.", show_alert=True)
        return
    try:
        # 1. Πρόσθεσε το model στα ενεργά του χρήστη
        resp = supabase.table("users").select("models").eq("user_id", user_id).execute()
        current_models = []
        if resp.data and len(resp.data) > 0:
            current_models = resp.data[0].get("models") or []
            if isinstance(current_models, str):
                try:
                    current_models = json.loads(current_models)
                except Exception:
                    current_models = []
        if model not in current_models:
            current_models.append(model)
            supabase.table("users").update({"models": current_models, "active": True}).eq("user_id", user_id).execute()
        # 2. Δημιούργησε νέα live_sessions για το model
        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()
        supabase.table("live_sessions").insert({
            "user_id": user_id,
            "username": user.username or f"id_{user_id}",
            "models": [model],
            "start_time": now_iso,
            "on_time": now_iso,
            "active": True,
            "mode": "on",
            "chat_id": str(group_id),
            "message_id": "0"
        }).execute()
        # 3. Ενημέρωσε τον χρήστη
        await query.edit_message_text(f"✅ Έκανες πάλι on το {model}!")
        # 4. Στείλε μήνυμα στην ομάδα
        try:
            await context.bot.send_message(
                chat_id=int(group_id),
                text=f"@{user.username} το είδε και έκανε πάλι on το {model}"
            )
        except Exception as ex:
            print(f"DEBUG: Failed to send group seenliveon message: {ex}")
    except Exception as e:
        await query.edit_message_text(f"❌ Σφάλμα: {e}")

# --- Main ---
async def main():
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("register", register))
    app.add_handler(CommandHandler("on", on_command))
    app.add_handler(CommandHandler("off", off_command))
    app.add_handler(CommandHandler("active", active_command))
    app.add_handler(CommandHandler("freemodels", freemodels_command))
    app.add_handler(CommandHandler("break", break_command))
    app.add_handler(CommandHandler("back", back_command))
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(CommandHandler("give", give_command))
    app.add_handler(CallbackQueryHandler(models_callback, pattern="^(model_|models_ok)"))
    app.add_handler(CallbackQueryHandler(off_callback, pattern="^(offmodel_|offmodels_ok)"))
    app.add_handler(CallbackQueryHandler(freepick_callback, pattern="^freepick_"))
    app.add_handler(CallbackQueryHandler(breaklen_callback, pattern="^breaklen_"))
    app.add_handler(CallbackQueryHandler(give_callback, pattern="^(givepick_|giveok|confirm_|reject_|acceptgive_)") )
    app.add_handler(CallbackQueryHandler(give_admin_callback, pattern="^(giveapprove_|givereject_)") )
    app.add_handler(CallbackQueryHandler(give_final_accept_callback, pattern="^givefinalaccept_"))
    app.add_handler(CallbackQueryHandler(cancel_callback, pattern="^cancel_action"))
    app.add_handler(MessageHandler(filters.Regex(r"^!status "), mention_status_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, custom_break_handler))
    app.add_handler(CommandHandler("notify", notify_command))
    app.add_handler(CallbackQueryHandler(notify_user_callback, pattern="^notifuser_"))
    app.add_handler(CallbackQueryHandler(notify_model_callback, pattern="^notifymodel_"))
    app.add_handler(CallbackQueryHandler(notify_accept_reject_callback, pattern="^(notifaccept_|notifreject_)"))
    app.add_handler(CommandHandler("myprogram", handle_myprogram))
    app.add_handler(CommandHandler("weekly_program", handle_weekly_program))
    app.add_handler(CommandHandler("durations_today", handle_durations_today))
    app.add_handler(CommandHandler("durations", handle_durations_month))
    app.add_handler(CommandHandler("durations_june", handle_durations_month))
    app.add_handler(CommandHandler("durations_may", handle_durations_month))
    app.add_handler(CommandHandler("durations_july", handle_durations_month))
    app.add_handler(CommandHandler("mistakeon", mistakeon_command))
    app.add_handler(CallbackQueryHandler(mistakeon_callback, pattern="^mistakeon_"))
    app.add_handler(CommandHandler("mistakeoff", mistakeoff_command))
    app.add_handler(CallbackQueryHandler(mistakeoff_callback, pattern="^mistakeoff_"))
    app.add_handler(CommandHandler("liveon", liveon_command))
    app.add_handler(CallbackQueryHandler(liveon_callback, pattern="^liveon_"))
    app.add_handler(CommandHandler("liveoff", liveoff_command))
    app.add_handler(CallbackQueryHandler(liveoff_callback, pattern="^liveoff_"))
    app.add_handler(CallbackQueryHandler(seenlive_callback, pattern="^seenlive_"))
    app.add_handler(CallbackQueryHandler(seenliveon_callback, pattern="^seenliveon_"))
    print("Ξεκινάει το bot...")
    await app.run_polling()

def get_reply_to_message_id(update, fallback_id=None):
    if hasattr(update, 'message') and update.message and hasattr(update.message, 'message_id') and update.message.message_id:
        return update.message.message_id
    if hasattr(update, 'callback_query') and update.callback_query and update.callback_query.message and hasattr(update.callback_query.message, 'message_id') and update.callback_query.message.message_id:
        return update.callback_query.message.message_id
    return fallback_id

if __name__ == "__main__":
    import asyncio
    try:
        asyncio.run(main())
    except RuntimeError as e:
        if "already running" in str(e):
            loop = asyncio.get_event_loop()
            loop.create_task(main())
            loop.run_forever()
        else:
            raise 