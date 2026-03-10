import asyncio
import random
import json
import os
import time
from datetime import datetime, timedelta
from pyrogram import Client, filters
from pyrogram.enums import ChatType
from pyrogram.errors import UserNotParticipant, FloodWait, PeerIdInvalid

# ========== CONFIG ==========
API_ID = 4880420
API_HASH = "fe7c528c27d3993a438599063bc03a3b"
SESSIONS = []  # Will be loaded from devour.json
SUDO_USERS = [6836139884]
DELAY_RANGE = [4, 6]  # Changed to list for easy modification
DATA_FILE = "devour.json"
CURRENT_FILE = "current.json"
PERSONAL_BOT = "im_bakabot"
SCANNING_DAYS = 30  # ⭐ CONFIGURABLE: Change this to scan last 30/45/60 days etc.
AUTO_DELETE = False  # ⭐ Auto-delete sent messages feature

# ========== SHARED STATE ==========
REPLY_TEXT1 = {}
REPLY_TEXT2 = {}
DEVOUR_STATE = {}
LAST_SCAN = {}
ACTIVE_TASKS = {}  # Track active tasks with task_id: {chat_id, session_name, cancel_event, etc}
TASK_ID_MAP = {}  # Map task_id to user_id for easy lookup
PAUSED_TASKS = {}  # Track paused tasks: task_id -> paused_data

def load_data():
    global REPLY_TEXT1, REPLY_TEXT2, LAST_SCAN, SESSIONS, DELAY_RANGE, SCANNING_DAYS, AUTO_DELETE
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r") as f:
            data = json.load(f)
        REPLY_TEXT1 = {int(k): v for k, v in data.get("reply_text1", {}).items()}
        REPLY_TEXT2 = {int(k): v for k, v in data.get("reply_text2", {}).items()}
        LAST_SCAN = {int(k): v for k, v in data.get("last_scan", {}).items()}
        SESSIONS = data.get("sessions", [])
        DELAY_RANGE = data.get("delay_range", [4, 6])
        SCANNING_DAYS = data.get("scanning_days", 30)  # Load scanning days config
        AUTO_DELETE = data.get("auto_delete", False)  # Load auto-delete config
    else:
        SESSIONS = []
        DELAY_RANGE = [4, 6]
        SCANNING_DAYS = 30
        AUTO_DELETE = False
        save_data()

def save_data():
    try:
        with open(DATA_FILE, "r") as f:
            data = json.load(f)
    except Exception:
        data = {
            "reply_text1": {},
            "reply_text2": {},
            "execution_logs": [],
            "last_scan": {},
            "sessions": [],
            "delay_range": [4, 6],
            "scanning_days": 30,
            "auto_delete": False,
        }
    data["reply_text1"] = {str(k): v for k, v in REPLY_TEXT1.items()}
    data["reply_text2"] = {str(k): v for k, v in REPLY_TEXT2.items()}
    data["last_scan"] = {str(k): v for k, v in LAST_SCAN.items()}
    data["sessions"] = SESSIONS
    data["delay_range"] = DELAY_RANGE
    data["scanning_days"] = SCANNING_DAYS
    data["auto_delete"] = AUTO_DELETE
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2)

def generate_task_id():
    """Generate a random unique task ID"""
    while True:
        task_id = str(random.randint(100000, 999999))
        if task_id not in ACTIVE_TASKS:
            return task_id

def update_current_json(task_id, user_id, chat_id, session_name, mode, count, total):
    """Update current.json with task execution progress"""
    try:
        with open(CURRENT_FILE, "r") as f:
            current_data = json.load(f)
    except Exception:
        current_data = {}

    if task_id not in current_data:
        current_data[task_id] = {
            "user_id": user_id,
            "chat_id": chat_id,
            "session_name": session_name,
            "mode": mode,
            "count": count,
            "total": total,
            "timestamp": time.time(),
            "status": "running",
            "paused_at": None
        }
    else:
        current_data[task_id]["count"] = count
        current_data[task_id]["status"] = "running"

    with open(CURRENT_FILE, "w") as f:
        json.dump(current_data, f, indent=2)

def remove_task_json(task_id):
    """Remove task from current.json"""
    try:
        with open(CURRENT_FILE, "r") as f:
            current_data = json.load(f)
    except Exception:
        current_data = {}

    if task_id in current_data:
        current_data[task_id]["status"] = "completed"
        current_data[task_id]["completed_at"] = time.time()

    with open(CURRENT_FILE, "w") as f:
        json.dump(current_data, f, indent=2)

def load_current_json():
    """Load current.json"""
    if os.path.exists(CURRENT_FILE):
        try:
            with open(CURRENT_FILE, "r") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_execution_log(chat_id, msg_links, texts, mode):
    try:
        with open(DATA_FILE, "r") as f:
            data = json.load(f)
    except Exception:
        data = {
            "reply_text1": {},
            "reply_text2": {},
            "execution_logs": [],
            "last_scan": {},
            "sessions": [],
            "delay_range": [4, 6],
            "scanning_days": 30,
            "auto_delete": False,
        }
    log = {
        "chat_id": chat_id,
        "mode": mode,
        "texts": texts,
        "message_links": msg_links,
        "count": len(msg_links),
        "timestamp": time.time(),
    }
    data.setdefault("execution_logs", []).append(log)
    data["reply_text1"] = {str(k): v for k, v in REPLY_TEXT1.items()}
    data["reply_text2"] = {str(k): v for k, v in REPLY_TEXT2.items()}
    data["last_scan"] = {str(k): v for k, v in LAST_SCAN.items()}
    data["sessions"] = SESSIONS
    data["delay_range"] = DELAY_RANGE
    data["scanning_days"] = SCANNING_DAYS
    data["auto_delete"] = AUTO_DELETE
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2)

def owner_or_sudo(_, __, m):
    return m.from_user and (m.from_user.id in SUDO_USERS or m.outgoing)

sudo_filter = filters.create(owner_or_sudo)

def build_main_menu(state, has_scan):
    return (
        f"**🎯 Target:** {state['target_name']}\n"
        f"**Group ID:** `{state['chat_id']}`"
        f"{' | 💾 Scan loaded' if has_scan else ''}\n\n"
        "**Main Menu:**\n"
        "1️⃣ Scan all users (last 30 days, real messages only)\n"
        "2️⃣ Attack by message links\n"
        "3️⃣ Use last scan\n"
        "4️⃣ 2-text blast (/settext1 + /settext2)\n"
        "5️⃣ Temporary text blast\n"
        "6️⃣ Rob mode (/rob 200/150/100/50/1000)\n"
        "7️⃣ Attack a specific message from all accounts\n"
        "8️⃣ Delete all my messages from group\n"
        "9️⃣ Claim `/daily` from all accounts in @im_bakabot\n"
        "🔟 Batch (send to a range of scanned users)\n"
        "Reply `1-10` or use /cancel"
    )

def parse_message_link(link):
    link = link.strip()
    if link.startswith("https://"):
        link = link.replace("https://", "", 1)
    if link.startswith("http://"):
        link = link.replace("http://", "", 1)
    if link.startswith("t.me/"):
        link = link[5:]
    parts = link.split("/")
    if len(parts) < 2:
        raise ValueError("Invalid link format")
    if parts[0] == "c":
        if len(parts) < 3:
            raise ValueError("Invalid /c/ link format")
        channel_id = int(parts[1])
        msg_id = int(parts[2])
        chat_id = int(f"-100{channel_id}")
        return chat_id, msg_id
    else:
        username = parts[0]
        msg_id = int(parts[1])
        return username, msg_id

def is_service_message(message):
    """⭐ Check if message is a service message (join/leave/etc) - IMPROVED"""
    # Check if it's a service message
    if message.service:
        return True
    
    # Check for empty text
    if not message.text:
        return True
    
    # Don't scan pinned messages or system messages
    if message.pinned or message.empty:
        return True
    
    return False

async def add_new_session(apps, name, session_string):
    new_app = Client(
        name,
        api_id=API_ID,
        api_hash=API_HASH,
        session_string=session_string,
    )
    _attach_attack_method(new_app)
    register_handlers(new_app, apps)
    await new_app.start()
    apps.append(new_app)

async def remove_session(apps, name):
    idx = None
    for i, s in enumerate(SESSIONS):
        if s["name"] == name:
            idx = i
            break
    if idx is not None:
        for i, app in enumerate(apps):
            if app.name == name:
                try:
                    await app.stop()
                except Exception:
                    pass
                apps.pop(i)
                break
        SESSIONS.pop(idx)
        save_data()

async def run_parallel_attacks(app_list, chat_id, msg_id, text, times, task_id, cancel_event=None, pause_event=None):
    async def attack_one(app):
        for _ in range(times):
            if cancel_event and cancel_event.is_set():
                break
            # Check pause event
            while pause_event and pause_event.is_set():
                await asyncio.sleep(0.5)
            try:
                msg = await app.send_message(chat_id, text, reply_to_message_id=msg_id)
                # Auto-delete if enabled
                if AUTO_DELETE and msg:
                    await asyncio.sleep(0.5)
                    try:
                        await app.delete_messages(chat_id, msg.id)
                    except Exception:
                        pass
                await asyncio.sleep(0.25)
            except FloodWait as e:
                if cancel_event and cancel_event.is_set():
                    break
                await asyncio.sleep(e.value)
            except Exception:
                break
    tasks = [asyncio.create_task(attack_one(a)) for a in app_list]
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        for t in tasks:
            t.cancel()
        raise

def _attach_attack_method(app):
    async def send_spam_attack(chat_id, msg_id, text, times):
        for _ in range(times):
            try:
                msg = await app.send_message(chat_id, text, reply_to_message_id=msg_id)
                # Auto-delete if enabled
                if AUTO_DELETE and msg:
                    await asyncio.sleep(0.5)
                    try:
                        await app.delete_messages(chat_id, msg.id)
                    except Exception:
                        pass
                await asyncio.sleep(0.25)
            except Exception:
                break
    app.send_spam_attack = send_spam_attack.__get__(app, Client)
    return app

def register_handlers(app, all_apps=None):

    @app.on_message(filters.command("devour") & sudo_filter & filters.private)
    async def devour_start(client, message):
        user_id = message.from_user.id
        DEVOUR_STATE[user_id] = {"step": "await_target"}
        await message.reply("🔗 Send the group link (`https://t.me/...`), @username or chat id (-100...) of the target chat.")

    @app.on_message(filters.command("autodel") & sudo_filter & filters.private)
    async def set_autodel(client, message):
        global AUTO_DELETE
        parts = message.text.split()
        if len(parts) < 2:
            status = "✅ ON" if AUTO_DELETE else "❌ OFF"
            await message.reply(f"**Auto-Delete Status:** {status}\n\n**Usage:** `/autodel on` or `/autodel off`")
            return
        
        option = parts[1].lower().strip()
        if option == "on":
            AUTO_DELETE = True
            save_data()
            await message.reply("✅ Auto-Delete **ENABLED** - Messages will be deleted instantly after sending!")
        elif option == "off":
            AUTO_DELETE = False
            save_data()
            await message.reply("❌ Auto-Delete **DISABLED** - Messages will stay in chat!")
        else:
            await message.reply("❌ Invalid option. Use `/autodel on` or `/autodel off`")

    @app.on_message(filters.command("scandays") & sudo_filter & filters.private)
    async def set_scanning_days(client, message):
        global SCANNING_DAYS
        parts = message.text.split()
        if len(parts) < 2:
            await message.reply(f"❌ **Usage:** `/scandays <days>`\n\nCurrent scanning days: `{SCANNING_DAYS}`")
            return
        try:
            days = int(parts[1])
            if days < 1:
                await message.reply("❌ Days must be a positive number (1 or more).")
                return
            SCANNING_DAYS = days
            save_data()
            await message.reply(f"✅ Scanning days updated to `{days}` days.")
        except ValueError:
            await message.reply("❌ Invalid input. Use a number only.")

    @app.on_message(filters.command("delay") & sudo_filter & filters.private)
    async def set_delay(client, message):
        global DELAY_RANGE
        parts = message.text.split()
        if len(parts) < 3:
            await message.reply(f"❌ **Usage:** `/delay <min> <max>`\n\nCurrent delay: `{DELAY_RANGE[0]}-{DELAY_RANGE[1]}` seconds")
            return
        try:
            min_delay = int(parts[1])
            max_delay = int(parts[2])
            if min_delay < 0 or max_delay < 0 or min_delay > max_delay:
                await message.reply("❌ Invalid values. Min and max must be positive, and min ≤ max.")
                return
            DELAY_RANGE = [min_delay, max_delay]
            save_data()
            await message.reply(f"✅ Delay range updated to `{min_delay}-{max_delay}` seconds.")
        except ValueError:
            await message.reply("❌ Invalid input. Use numbers only.")

    @app.on_message(filters.command("current") & sudo_filter & filters.private)
    async def show_current(client, message):
        user_id = message.from_user.id
        current_data = load_current_json()
        
        # Filter tasks for this user
        user_tasks = {tid: tdata for tid, tdata in current_data.items() if tdata.get("user_id") == user_id and tdata.get("status") == "running"}
        
        if not user_tasks:
            await message.reply("ℹ️ No currently running tasks.")
            return

        text = "**📊 Currently Running Tasks:**\n\n"
        for idx, (task_id, task_data) in enumerate(user_tasks.items(), 1):
            percentage = (task_data["count"] / task_data["total"] * 100) if task_data["total"] > 0 else 0
            status_icon = "⏸️ PAUSED" if task_id in PAUSED_TASKS else "▶️ RUNNING"
            text += (
                f"`{idx}` | **Task ID:** `{task_id}` {status_icon}\n"
                f"    **Target:** `{task_data['chat_id']}`\n"
                f"    **Session:** `{task_data['session_name']}`\n"
                f"    **Mode:** `{task_data['mode']}`\n"
                f"    **Progress:** `{task_data['count']}/{task_data['total']}` ({percentage:.1f}%)\n\n"
            )

        text += "**Commands:**\n"
        text += "`/cancel <task_id>` - Cancel task\n"
        text += "`/pause <task_id>` - Pause task\n"
        text += "`/resume <task_id>` - Resume paused task\n"
        text += "Example: `/cancel 123456`"
        
        state = DEVOUR_STATE.get(user_id)
        if state is None:
            state = {"step": "show_tasks"}
            DEVOUR_STATE[user_id] = state
        else:
            state["step"] = "show_tasks"

        await message.reply(text)

    @app.on_message(
        sudo_filter
        & filters.private
        & ~filters.command(
            [
                "devour",
                "settext1",
                "settext2",
                "help",
                "cancel",
                "stop",
                "pause",
                "resume",
                "joinchat",
                "addacc",
                "delacc",
                "delall",
                "claim",
                "delay",
                "current",
                "scandays",
                "autodel",
            ]
        )
    )
    async def devour_menu(client, message):
        user_id = message.from_user.id
        if user_id not in DEVOUR_STATE:
            return
        state = DEVOUR_STATE[user_id]
        text = message.text.strip()

        def ensure_cancel_event():
            if "cancel_event" not in state or not isinstance(state.get("cancel_event"), asyncio.Event):
                state["cancel_event"] = asyncio.Event()
            if "pause_event" not in state or not isinstance(state.get("pause_event"), asyncio.Event):
                state["pause_event"] = asyncio.Event()
            state.setdefault("background_task", None)

        # STEP 1: choose target chat
        if state["step"] == "await_target":
            chat_input = text
            try:
                if chat_input.startswith("https://t.me/") or chat_input.startswith("http://t.me/"):
                    chat = await client.get_chat(chat_input)
                else:
                    chat = await client.get_chat(chat_input)
                if not chat:
                    await message.reply("❌ Could not find that chat.")
                    DEVOUR_STATE.pop(user_id, None)
                    return

                try:
                    member = await client.get_chat_member(chat.id, "me")
                    if not member:
                        await message.reply("❌ Not a member. Join with /joinchat <invite link>.")
                        DEVOUR_STATE.pop(user_id, None)
                        return
                except UserNotParticipant:
                    await message.reply("❌ Not a member. Join with /joinchat <invite link>.")
                    DEVOUR_STATE.pop(user_id, None)
                    return

                scan = LAST_SCAN.get(chat.id)
                state.update(
                    {
                        "step": "main_menu",
                        "target_name": chat.title or str(chat.id),
                        "chat_id": chat.id,
                    }
                )
                menu = build_main_menu(state, scan is not None)
                await message.reply(menu)
            except PeerIdInvalid:
                await message.reply("❌ Peer ID invalid or unknown. Make sure this account joined the group.")
                DEVOUR_STATE.pop(user_id, None)
            except Exception as e:
                await message.reply(f"❌ Error accessing chat: {e}")
                DEVOUR_STATE.pop(user_id, None)
            return

        # STEP MAIN MENU
        if state["step"] == "main_menu":
            opt = text.lower()
            chat_id = state["chat_id"]

            # 1) Scan all users (with date filter and service message filter) - IMPROVED
            if opt == "1":
                state["step"] = "scanning"
                status = await message.reply(f"🔍 Scanning all users in **{state['target_name']}** (last {SCANNING_DAYS} days, REAL messages only)...")
                user_msgs = {}
                cutoff_date = datetime.utcnow() - timedelta(days=SCANNING_DAYS)
                try:
                    async for msg in client.get_chat_history(chat_id):
                        # ⭐ Filter: Only messages within last SCANNING_DAYS and not service messages
                        if msg.date < cutoff_date:
                            break
                        
                        # IMPROVED: Skip all service messages
                        if is_service_message(msg):
                            continue
                        
                        # Only scan real user messages with actual text content
                        if (
                            msg.from_user
                            and not msg.from_user.is_bot
                            and not getattr(msg.from_user, "is_deleted", False)
                            and msg.text  # Must have text
                        ):
                            if msg.from_user.id not in user_msgs:
                                user_msgs[msg.from_user.id] = msg.id
                except PeerIdInvalid:
                    await status.edit("❌ Peer ID invalid or not joined.")
                    DEVOUR_STATE.pop(user_id, None)
                    return
                last_count = len(user_msgs)
                LAST_SCAN[chat_id] = {
                    "user_msgs": user_msgs,
                    "count": last_count,
                    "timestamp": time.time(),
                }
                save_data()
                state["step"] = "await_count"
                state["user_msgs"] = user_msgs
                await status.edit(
                    f"✅ Found **{last_count}** real users (last {SCANNING_DAYS} days).\n\nHow many to execute (1-{last_count})? Reply with a number."
                )
                return

            # 2) message-links attack
            if opt == "2":
                state["step"] = "await_links"
                await message.reply("📎 Send message links (one per line).")
                return

            # 3) use last scan
            if opt == "3":
                scan = LAST_SCAN.get(chat_id)
                if not scan or not scan.get("user_msgs"):
                    await message.reply("❌ No cached previous scan data. Use option 1 first.")
                    DEVOUR_STATE.pop(user_id, None)
                    return
                state["user_msgs"] = scan["user_msgs"]
                state["step"] = "await_count"
                await message.reply(
                    f"💾 Loaded cached data (**{len(scan['user_msgs'])} users**).\n\nHow many to execute? Reply with number."
                )
                return

            # 4) 2-text blast
            if opt == "4":
                state["step"] = "wait_2text_prepare"
                await message.reply(
                    "📝 Set texts with `/settext1 <text>` and `/settext2 <text>`.\nWhen ready, reply with how many users to execute."
                )
                return

            # 5) temporary text (one-time)
            if opt == "5":
                state["step"] = "await_temptext"
                await message.reply(
                    "📝 Send `/temptext 1 <text>` for one reply, or `/temptext 2 <text>` for two identical replies per user."
                )
                return

            # 6) rob mode
            if opt == "6":
                state["step"] = "rob_select"
                await message.reply(
                    "💰 **Rob Mode**\n"
                    "1️⃣ /rob 200\n"
                    "2️⃣ /rob 150\n"
                    "3️⃣ /rob 100\n"
                    "4️⃣ /rob 50\n"
                    "5️⃣ /rob 1000\n"
                    "Reply with 1-5"
                )
                return

            # 7) attack a specific message from all accounts
            if opt == "7":
                state["step"] = "attack_message_link"
                await message.reply(
                    "🔥 **Attack by Message Link**\nSend the target message link "
                    "(right-click/copy link from the user message)."
                )
                return

            # 8) delete all self messages from group
            if opt == "8":
                status = await message.reply("🗑 Deleting all my messages from group...")
                deleted_count = 0
                async for msg in client.get_chat_history(chat_id):
                    if msg.from_user and msg.from_user.is_self:
                        try:
                            await client.delete_messages(chat_id, msg.id)
                            deleted_count += 1
                            await asyncio.sleep(0.1)
                        except Exception:
                            pass
                await status.edit(f"✅ Done! Deleted {deleted_count} messages.")
                await message.reply("✅ All bot messages removed from group.")
                DEVOUR_STATE.pop(user_id, None)
                return

            # 9) claim daily on @im_bakabot
            if opt == "9":
                await message.reply("⏳ Claiming `/daily` from all accounts in @im_bakabot...")
                failed = 0
                for sess in SESSIONS:
                    try:
                        temp_app = None
                        if all_apps:
                            for a in all_apps:
                                if a.name == sess["name"]:
                                    temp_app = a
                                    break
                        if temp_app is None:
                            temp_app = Client(
                                sess["name"],
                                api_id=API_ID,
                                api_hash=API_HASH,
                                session_string=sess["session_string"],
                            )
                            await temp_app.start()
                        await temp_app.send_message(PERSONAL_BOT, "/daily")
                        if all_apps is None or temp_app not in all_apps:
                            await temp_app.stop()
                        await asyncio.sleep(2)
                    except Exception:
                        failed += 1
                await message.reply(
                    f"✅ `/daily` claimed in @im_bakabot.\nAccounts: {len(SESSIONS)}, Failed: {failed}"
                )
                DEVOUR_STATE.pop(user_id, None)
                return

            # 10) Batch range from scanned data
            if opt == "10":
                user_msgs = state.get("user_msgs")
                if not user_msgs:
                    scan = LAST_SCAN.get(chat_id)
                    if not scan or not scan.get("user_msgs"):
                        await message.reply("❌ No scan data available. Use option 1 to scan or option 3 to load last scan.")
                        DEVOUR_STATE.pop(user_id, None)
                        return
                    state["user_msgs"] = scan["user_msgs"]
                state["step"] = "batch_await_range"
                await message.reply(
                    "📦 Batch send setup:\n"
                    "Reply with a range in one of these formats:\n"
                    "`<start> <end>` (e.g. `1 500`) or a single number `<N>` (e.g. `500` meaning 1 to 500).\n"
                    "Indexing is 1-based and inclusive."
                )
                return

            await message.reply("❌ Invalid option. Reply `1-10`.")
            return

        # ROB MODE
        if state["step"] == "rob_select":
            opt = text
            rob_commands = {
                "1": "/rob 200",
                "2": "/rob 150",
                "3": "/rob 100",
                "4": "/rob 50",
                "5": "/rob 1000",
            }
            if opt in rob_commands:
                state["rob_cmd"] = rob_commands[opt]
                state["step"] = "rob_count"
                await message.reply(
                    f"✅ Selected: `{rob_commands[opt]}`.\nHow many users to execute? Reply with number."
                )
            else:
                await message.reply("❌ Reply with `1-5`.")
            return

        if state["step"] == "rob_count":
            if not text.isdigit():
                await message.reply("❌ Reply with a number.")
                return
            count = int(text)
            scan = LAST_SCAN.get(state["chat_id"])
            pairs = list(scan["user_msgs"].items())[:count] if scan else []
            if not pairs:
                await message.reply("❌ No scan data. Use option 1 first.")
                DEVOUR_STATE.pop(user_id, None)
                return
            state["msg_pairs"] = pairs
            state["step"] = "rob_confirm"
            await message.reply(
                f"✅ Will send `{state['rob_cmd']}` to **{count} users**.\nType `yes` to confirm."
            )
            return

        # ATTACK BY MESSAGE LINK FROM ALL ACCOUNTS
        if state["step"] == "attack_message_link":
            link = text
            try:
                chatid_or_username, msg_id = parse_message_link(link)
            except Exception:
                await message.reply(
                    "❌ Invalid message link.\nUse `t.me/c/<id>/<msg_id>` or `t.me/<username>/<msg_id>` format."
                )
                DEVOUR_STATE.pop(user_id, None)
                return
            state["attack_msg_link"] = link
            state["attack_chat"] = chatid_or_username
            state["attack_msg_id"] = msg_id
            state["step"] = "attack_text"
            await message.reply("✏️ What message/command to spam? (e.g. `/rob 10000`)")
            return

        if state["step"] == "attack_text":
            custom_text = text
            if not custom_text:
                await message.reply("❌ Message text required.")
                return
            state["attack_text"] = custom_text
            state["step"] = "attack_times"
            await message.reply(
                "🔢 How many times to spam (per account)? (1–100)\n"
                "This will be executed from **all accounts** in parallel."
            )
            return

        if state["step"] == "attack_times":
            if not text.isdigit():
                await message.reply("❌ Reply with a number 1–100.")
                return
            times = int(text)
            if times < 1 or times > 100:
                await message.reply("❌ Number must be between 1 and 100.")
                return
            state["attack_times"] = times
            state["step"] = "attack_link_confirm"
            await message.reply(
                f"Ready! Will spam `{state['attack_text']}` {times} times per account at:\n"
                f"`{state['attack_msg_link']}`\n\nType `yes` to confirm."
            )
            return

        if state["step"] == "attack_link_confirm":
            if text.lower() != "yes":
                await message.reply("❌ Type `yes` to execute.")
                return
            ensure_cancel_event()
            task_id = generate_task_id()  # ⭐ Generate random task ID
            ACTIVE_TASKS[task_id] = state
            TASK_ID_MAP[task_id] = user_id
            state["task_id"] = task_id
            state["step"] = "running_attack_links"
            chatid_or_username = state["attack_chat"]
            msg_id = state["attack_msg_id"]
            text_to_send = state["attack_text"]
            times = state["attack_times"]
            try:
                if isinstance(chatid_or_username, str):
                    chat_obj = await client.get_chat(chatid_or_username)
                    chat_id = chat_obj.id
                else:
                    chat_id = chatid_or_username
            except Exception as e:
                await message.reply(f"❌ Failed to resolve chat: {e}")
                DEVOUR_STATE.pop(user_id, None)
                ACTIVE_TASKS.pop(task_id, None)
                TASK_ID_MAP.pop(task_id, None)
                return
            
            await message.reply(f"🚀 Spamming now from all accounts (Task ID: `{task_id}`)...")
            if all_apps:
                task = asyncio.create_task(
                    run_parallel_attacks(all_apps, chat_id, msg_id, text_to_send, times, task_id, cancel_event=state["cancel_event"], pause_event=state["pause_event"])
                )
                state["background_task"] = task
                state["session_name"] = client.name
                update_current_json(task_id, user_id, chat_id, client.name, "message_attack", 0, times * len(all_apps))

                def _done_callback(t):
                    try:
                        remove_task_json(task_id)
                        ACTIVE_TASKS.pop(task_id, None)
                        TASK_ID_MAP.pop(task_id, None)
                        PAUSED_TASKS.pop(task_id, None)
                        DEVOUR_STATE.pop(user_id, None)
                    except Exception:
                        pass

                task.add_done_callback(lambda t: _done_callback(t))
            else:
                sent = 0
                reply_text = text_to_send
                try:
                    for _ in range(times):
                        if state.get("cancel_event") and state["cancel_event"].is_set():
                            break
                        # Check pause
                        while state.get("pause_event") and state["pause_event"].is_set():
                            await asyncio.sleep(0.5)
                        try:
                            msg = await client.send_message(chat_id, reply_text, reply_to_message_id=msg_id)
                            # Auto-delete if enabled
                            if AUTO_DELETE and msg:
                                await asyncio.sleep(0.5)
                                try:
                                    await client.delete_messages(chat_id, msg.id)
                                except Exception:
                                    pass
                            sent += 1
                            await asyncio.sleep(0.25)
                        except FloodWait as e:
                            if state.get("cancel_event") and state["cancel_event"].is_set():
                                break
                            await asyncio.sleep(e.value)
                        except Exception:
                            break
                finally:
                    remove_task_json(task_id)
                    ACTIVE_TASKS.pop(task_id, None)
                    TASK_ID_MAP.pop(task_id, None)
                    PAUSED_TASKS.pop(task_id, None)
                    DEVOUR_STATE.pop(user_id, None)
                await message.reply(f"✅ Done! Sent {sent} messages (single-account mode).")
            return

        # SCAN RESULT EXECUTION (simple single-text mode based on REPLY_TEXT1)
        if state["step"] == "await_count":
            if not text.isdigit():
                await message.reply("❌ Enter a valid number.")
                return
            count = int(text)
            user_msgs = state.get("user_msgs")
            if not user_msgs:
                await message.reply("❌ No user scan loaded.")
                DEVOUR_STATE.pop(user_id, None)
                return
            pairs = list(user_msgs.items())[:count]
            state["step"] = "execution_confirm"
            state["msg_pairs"] = pairs
            await message.reply(
                f"Ready to execute on {count} users using Text1.\nType `yes` to confirm."
            )
            return

        if state["step"] == "execution_confirm":
            if text.lower() != "yes":
                await message.reply("❌ Type `yes` to confirm.")
                return
            ensure_cancel_event()
            task_id = generate_task_id()  # ⭐ Generate random task ID
            ACTIVE_TASKS[task_id] = state
            TASK_ID_MAP[task_id] = user_id
            state["task_id"] = task_id
            state["session_name"] = client.name
            chat_id = state["chat_id"]
            pairs = state.get("msg_pairs", [])
            reply_text = REPLY_TEXT1.get(chat_id, REPLY_TEXT1.get("default", "/kill"))
            
            status_msg = await message.reply(f"🚀 Starting execution... (Task ID: `{task_id}`)\n✅ Progress: 0/{len(pairs)}")
            update_current_json(task_id, user_id, chat_id, client.name, "scan_execution", 0, len(pairs))
            
            sent = 0
            try:
                for idx, (_, msg_id) in enumerate(pairs, 1):
                    if state.get("cancel_event") and state["cancel_event"].is_set():
                        await status_msg.edit(f"🛑 Execution cancelled at {sent}/{len(pairs)}.")
                        break
                    # Check pause
                    while state.get("pause_event") and state["pause_event"].is_set():
                        await asyncio.sleep(0.5)
                    try:
                        msg = await client.send_message(chat_id, reply_text, reply_to_message_id=msg_id)
                        # Auto-delete if enabled
                        if AUTO_DELETE and msg:
                            await asyncio.sleep(0.5)
                            try:
                                await client.delete_messages(chat_id, msg.id)
                            except Exception:
                                pass
                        sent += 1
                        update_current_json(task_id, user_id, chat_id, client.name, "scan_execution", sent, len(pairs))
                        percentage = (sent / len(pairs)) * 100
                        await status_msg.edit(f"✅ Progress: {sent}/{len(pairs)} ({percentage:.1f}%)")
                        await asyncio.sleep(random.uniform(DELAY_RANGE[0], DELAY_RANGE[1]))
                    except FloodWait as e:
                        if state.get("cancel_event") and state["cancel_event"].is_set():
                            break
                        await asyncio.sleep(e.value)
                    except Exception:
                        pass
            finally:
                remove_task_json(task_id)
                ACTIVE_TASKS.pop(task_id, None)
                TASK_ID_MAP.pop(task_id, None)
                PAUSED_TASKS.pop(task_id, None)
                DEVOUR_STATE.pop(user_id, None)
            
            await message.reply(f"✅ Done! Message sent to {sent} users.")
            return

        # Batch range: await range input
        if state["step"] == "batch_await_range":
            user_msgs = state.get("user_msgs")
            if not user_msgs:
                await message.reply("❌ No scanned data available. Use option 1 first.")
                DEVOUR_STATE.pop(user_id, None)
                return
            total = len(user_msgs)
            raw = text.strip()
            parts = None
            if "-" in raw:
                parts = [p for p in raw.split("-") if p.strip()]
            else:
                parts = raw.split()
            try:
                if len(parts) == 1:
                    end = int(parts[0])
                    start = 1
                elif len(parts) >= 2:
                    start = int(parts[0])
                    end = int(parts[1])
                else:
                    raise ValueError
            except Exception:
                await message.reply("❌ Invalid format. Reply like `1 500` or `500`.")
                return
            if start < 1 or end < start or end > total:
                await message.reply(f"❌ Invalid range. Valid 1-based range is 1..{total}.")
                return
            pairs_all = list(user_msgs.items())
            selected_pairs = pairs_all[start - 1 : end]
            state["batch_range"] = (start, end)
            state["msg_pairs"] = selected_pairs
            state["step"] = "batch_await_text"
            await message.reply(
                f"✅ Selected {len(selected_pairs)} users (range {start}-{end}).\n\n"
                f"📝 Now send the text to send to all selected users:"
            )
            return

        # Batch: await custom text
        if state["step"] == "batch_await_text":
            batch_text = text
            if not batch_text:
                await message.reply("❌ Text required.")
                return
            state["batch_text"] = batch_text
            state["step"] = "batch_confirm"
            pairs = state.get("msg_pairs", [])
            await message.reply(
                f"Ready to send `{batch_text}` to {len(pairs)} selected users.\n"
                f"Type `yes` to confirm."
            )
            return

        # Batch confirm and execute
        if state["step"] == "batch_confirm":
            if text.lower() != "yes":
                await message.reply("❌ Type `yes` to confirm.")
                return
            ensure_cancel_event()
            task_id = generate_task_id()  # ⭐ Generate random task ID
            ACTIVE_TASKS[task_id] = state
            TASK_ID_MAP[task_id] = user_id
            state["task_id"] = task_id
            state["session_name"] = client.name
            pairs = state.get("msg_pairs", [])
            chat_id = state["chat_id"]
            batch_text = state.get("batch_text", "/kill")
            
            status_msg = await message.reply(f"🚀 Starting batch execution... (Task ID: `{task_id}`)\n✅ Progress: 0/{len(pairs)}")
            update_current_json(task_id, user_id, chat_id, client.name, "batch", 0, len(pairs))
            
            sent = 0
            try:
                for idx, (_, msg_id) in enumerate(pairs, 1):
                    if state.get("cancel_event") and state["cancel_event"].is_set():
                        await status_msg.edit(f"🛑 Batch cancelled at {sent}/{len(pairs)}.")
                        break
                    # Check pause
                    while state.get("pause_event") and state["pause_event"].is_set():
                        await asyncio.sleep(0.5)
                    try:
                        msg = await client.send_message(chat_id, batch_text, reply_to_message_id=msg_id)
                        # Auto-delete if enabled
                        if AUTO_DELETE and msg:
                            await asyncio.sleep(0.5)
                            try:
                                await client.delete_messages(chat_id, msg.id)
                            except Exception:
                                pass
                        sent += 1
                        update_current_json(task_id, user_id, chat_id, client.name, "batch", sent, len(pairs))
                        percentage = (sent / len(pairs)) * 100
                        await status_msg.edit(f"✅ Progress: {sent}/{len(pairs)} ({percentage:.1f}%)")
                        await asyncio.sleep(random.uniform(DELAY_RANGE[0], DELAY_RANGE[1]))
                    except FloodWait as e:
                        if state.get("cancel_event") and state["cancel_event"].is_set():
                            break
                        await asyncio.sleep(e.value)
                    except Exception:
                        pass
                try:
                    save_execution_log(chat_id, [], [batch_text], "batch")
                except Exception:
                    pass
            finally:
                remove_task_json(task_id)
                ACTIVE_TASKS.pop(task_id, None)
                TASK_ID_MAP.pop(task_id, None)
                PAUSED_TASKS.pop(task_id, None)
                DEVOUR_STATE.pop(user_id, None)
            
            await message.reply(f"✅ Batch done! Message sent to {sent} users.")
            return

        # Simple rob execution
        if state["step"] == "rob_confirm":
            if text.lower() != "yes":
                await message.reply("❌ Type `yes` to execute.")
                return
            ensure_cancel_event()
            task_id = generate_task_id()  # ⭐ Generate random task ID
            ACTIVE_TASKS[task_id] = state
            TASK_ID_MAP[task_id] = user_id
            state["task_id"] = task_id
            state["session_name"] = client.name
            pairs = state.get("msg_pairs", [])
            chat_id = state["chat_id"]
            rob_cmd = state["rob_cmd"]
            
            status_msg = await message.reply(f"🚀 Starting rob execution... (Task ID: `{task_id}`)\n✅ Progress: 0/{len(pairs)}")
            update_current_json(task_id, user_id, chat_id, client.name, "rob", 0, len(pairs))
            
            sent = 0
            try:
                for idx, (_, msg_id) in enumerate(pairs, 1):
                    if state.get("cancel_event") and state["cancel_event"].is_set():
                        await status_msg.edit(f"🛑 Rob execution cancelled at {sent}/{len(pairs)}.")
                        break
                    # Check pause
                    while state.get("pause_event") and state["pause_event"].is_set():
                        await asyncio.sleep(0.5)
                    try:
                        msg = await client.send_message(chat_id, rob_cmd, reply_to_message_id=msg_id)
                        # Auto-delete if enabled
                        if AUTO_DELETE and msg:
                            await asyncio.sleep(0.5)
                            try:
                                await client.delete_messages(chat_id, msg.id)
                            except Exception:
                                pass
                        sent += 1
                        update_current_json(task_id, user_id, chat_id, client.name, "rob", sent, len(pairs))
                        percentage = (sent / len(pairs)) * 100
                        await status_msg.edit(f"✅ Progress: {sent}/{len(pairs)} ({percentage:.1f}%)")
                        await asyncio.sleep(random.uniform(DELAY_RANGE[0], DELAY_RANGE[1]))
                    except FloodWait as e:
                        if state.get("cancel_event") and state["cancel_event"].is_set():
                            break
                        await asyncio.sleep(e.value)
                    except Exception:
                        pass
            finally:
                remove_task_json(task_id)
                ACTIVE_TASKS.pop(task_id, None)
                TASK_ID_MAP.pop(task_id, None)
                PAUSED_TASKS.pop(task_id, None)
                DEVOUR_STATE.pop(user_id, None)
            
            await message.reply(f"✅ Done! `{rob_cmd}` sent to {sent} users.")
            return

    # ========== BASIC COMMANDS ==========

    @app.on_message(filters.command("addacc") & sudo_filter & filters.private)
    async def addacc(client, message):
        parts = message.text.split(" ", 2)
        if len(parts) < 3:
            await message.reply(
                "❌ **Usage:** `/addacc <name> <session_string>`\n"
                "Example: `/addacc acc1 BQAbc123...xyz`"
            )
            return
        name, session_string = parts[1].strip(), parts[2].strip()
        if any(s["name"].lower() == name.lower() for s in SESSIONS):
            await message.reply(f"❌ Account with name `{name}` already exists!")
            return
        SESSIONS.append({"name": name, "session_string": session_string})
        save_data()
        if all_apps is not None:
            await add_new_session(all_apps, name, session_string)
            await message.reply(f"✅ Account `{name}` added & started (no restart needed).")
        else:
            await message.reply(f"✅ Account `{name}` added. Restart to activate.")

    @app.on_message(filters.command("delacc") & sudo_filter & filters.private)
    async def delacc(client, message):
        parts = message.text.split(" ", 1)
        if len(parts) < 2:
            await message.reply("❌ **Usage:** `/delacc <name>`")
            return
        name = parts[1].strip()
        if not any(s["name"] == name for s in SESSIONS):
            await message.reply(f"❌ No such account: {name}")
            return
        if all_apps is not None:
            await remove_session(all_apps, name)
            await message.reply(f"✅ Account `{name}` removed and stopped.")
        else:
            await message.reply("Account removed. Please restart for changes to take effect.")

    @app.on_message(filters.command("joinchat") & sudo_filter & filters.private)
    async def joinchat(client, message):
        parts = message.text.split(" ", 1)
        if len(parts) < 2:
            await message.reply("❌ **Usage:** `/joinchat <invite_link>`")
            return
        try:
            chat = await client.join_chat(parts[1].strip())
            await message.reply(f"✅ Joined **{chat.title or chat.id}**.")
        except Exception as e:
            await message.reply(f"❌ Error: {e}")

    @app.on_message(filters.command(["cancel", "stop"]) & sudo_filter & filters.private)
    async def cancel_task(client, message):
        user_id = message.from_user.id
        parts = message.text.split()
        
        # If no task ID provided, show all active tasks
        if len(parts) == 1:
            current_data = load_current_json()
            user_tasks = {tid: tdata for tid, tdata in current_data.items() if tdata.get("user_id") == user_id and tdata.get("status") == "running"}
            
            if not user_tasks:
                await message.reply("ℹ️ No active tasks to cancel.")
                return
            
            text = "**Active Tasks:**\n"
            for task_id, task_data in user_tasks.items():
                status_icon = "⏸️ PAUSED" if task_id in PAUSED_TASKS else "▶️ RUNNING"
                text += f"Task ID: `{task_id}` {status_icon} | Mode: `{task_data['mode']}` | Progress: `{task_data['count']}/{task_data['total']}`\n"
            text += "\nUse: `/cancel <task_id>` to cancel a specific task"
            await message.reply(text)
            return
        
        # Cancel specific task by ID
        task_id = parts[1].strip()
        if task_id not in ACTIVE_TASKS:
            await message.reply(f"❌ Task ID `{task_id}` not found or already completed.")
            return
        
        state = ACTIVE_TASKS[task_id]
        cancel_event = state.get("cancel_event")
        if cancel_event:
            cancel_event.set()
        
        bg = state.get("background_task")
        if bg and isinstance(bg, asyncio.Task) and not bg.done():
            try:
                bg.cancel()
            except Exception:
                pass
        
        await message.reply(f"🛑 Task `{task_id}` cancellation requested.")

    @app.on_message(filters.command("pause") & sudo_filter & filters.private)
    async def pause_task(client, message):
        user_id = message.from_user.id
        parts = message.text.split()
        
        if len(parts) < 2:
            await message.reply("❌ **Usage:** `/pause <task_id>`\nExample: `/pause 123456`")
            return
        
        task_id = parts[1].strip()
        if task_id not in ACTIVE_TASKS:
            await message.reply(f"❌ Task ID `{task_id}` not found or already completed.")
            return
        
        state = ACTIVE_TASKS[task_id]
        pause_event = state.get("pause_event")
        if pause_event:
            pause_event.set()
            PAUSED_TASKS[task_id] = True
            await message.reply(f"⏸️ Task `{task_id}` **PAUSED**. Use `/resume {task_id}` to continue.")
        else:
            await message.reply(f"❌ Cannot pause task `{task_id}` - no pause event found.")

    @app.on_message(filters.command("resume") & sudo_filter & filters.private)
    async def resume_task(client, message):
        user_id = message.from_user.id
        parts = message.text.split()
        
        if len(parts) < 2:
            await message.reply("❌ **Usage:** `/resume <task_id>`\nExample: `/resume 123456`")
            return
        
        task_id = parts[1].strip()
        if task_id not in PAUSED_TASKS:
            await message.reply(f"❌ Task ID `{task_id}` is not paused or not found.")
            return
        
        state = ACTIVE_TASKS.get(task_id)
        if not state:
            await message.reply(f"❌ Task ID `{task_id}` not found.")
            return
        
        pause_event = state.get("pause_event")
        if pause_event:
            pause_event.clear()
            PAUSED_TASKS.pop(task_id, None)
            await message.reply(f"▶️ Task `{task_id}` **RESUMED**. Continuing execution...")
        else:
            await message.reply(f"❌ Cannot resume task `{task_id}` - no pause event found.")

    @app.on_message(filters.command("settext1") & sudo_filter & filters.private)
    async def settext1(client, message):
        parts = message.text.split(" ", 1)
        if len(parts) == 2 and parts[1].strip():
            REPLY_TEXT1[message.chat.id] = parts[1].strip()
            save_data()
            await message.reply(f"✅ **Text1 set to:** `{parts[1].strip()}`")
        else:
            await message.reply("❌ **Usage:** `/settext1 <text>`")

    @app.on_message(filters.command("settext2") & sudo_filter & filters.private)
    async def settext2(client, message):
        parts = message.text.split(" ", 1)
        if len(parts) == 2 and parts[1].strip():
            REPLY_TEXT2[message.chat.id] = parts[1].strip()
            save_data()
            await message.reply(f"✅ **Text2 set to:** `{parts[1].strip()}`")
        else:
            await message.reply("❌ **Usage:** `/settext2 <text>`")

    @app.on_message(filters.command("delall") & sudo_filter & filters.private)
    async def delall(client, message):
        user_id = message.from_user.id
        state = DEVOUR_STATE.get(user_id)
        if not state or "chat_id" not in state:
            await message.reply("❌ Set a target chat via /devour first.")
            return
        chat_id = state["chat_id"]
        status = await message.reply("🗑 Deleting all my messages from group...")
        deleted_count = 0
        async for msg in client.get_chat_history(chat_id):
            if msg.from_user and msg.from_user.is_self:
                try:
                    await client.delete_messages(chat_id, msg.id)
                    deleted_count += 1
                    await asyncio.sleep(0.1)
                except Exception:
                    pass
        await status.edit(f"✅ Done! Deleted {deleted_count} messages.")
        await message.reply("✅ All my messages removed from group.")

    @app.on_message(filters.command("claim") & sudo_filter & filters.private)
    async def claim(client, message):
        await message.reply("⏳ Claiming `/daily` from all accounts in @im_bakabot...")
        failed = 0
        for sess in SESSIONS:
            try:
                temp_app = None
                if all_apps:
                    for a in all_apps:
                        if a.name == sess["name"]:
                            temp_app = a
                            break
                if temp_app is None:
                    temp_app = Client(
                        sess["name"],
                        api_id=API_ID,
                        api_hash=API_HASH,
                        session_string=sess["session_string"],
                    )
                    await temp_app.start()
                await temp_app.send_message(PERSONAL_BOT, "/daily")
                if all_apps is None or temp_app not in all_apps:
                    await temp_app.stop()
                await asyncio.sleep(2)
            except Exception:
                failed += 1
        await message.reply(
            f"✅ `/daily` claimed in @im_bakabot.\nAccounts: {len(SESSIONS)}, Failed: {failed}"
        )

    @app.on_message(filters.command("help") & sudo_filter & filters.private)
    async def help_msg(client, message):
        await message.reply(
            "**🤖 Devour UserBot Help:**\n\n"
            "**DM Commands:**\n"
            "`/devour` - Open main control menu\n"
            "`/settext1 <text>` - Set Text1\n"
            "`/settext2 <text>` - Set Text2\n"
            "`/joinchat <link>` - Join group/channel\n"
            "`/addacc <name> <session>` - Add new account (hot add)\n"
            "`/delacc <name>` - Remove account (hot remove)\n"
            "`/delall` - Delete all my messages from target group\n"
            "`/claim` - Call `/daily` from all accounts in @im_bakabot\n"
            "`/cancel <task_id>` - Cancel a specific task\n"
            "`/pause <task_id>` - Pause a running task\n"
            "`/resume <task_id>` - Resume a paused task\n"
            "`/delay <min> <max>` - Set delay range (e.g. `/delay 4 6`)\n"
            "`/scandays <days>` - Set scanning period (e.g. `/scandays 30`)\n"
            "`/autodel on/off` - Enable/disable auto-delete (e.g. `/autodel on`)\n"
            "`/current` - Show all currently running tasks\n"
            "`/help` - Show this help\n\n"
            f"**Data:** `{DATA_FILE}` | **Active accounts:** {len(SESSIONS)}\n"
            f"**Current delay:** `{DELAY_RANGE[0]}-{DELAY_RANGE[1]}` seconds\n"
            f"**Scanning period:** `{SCANNING_DAYS}` days"
        )

async def main():
    load_data()
    if not SESSIONS:
        print("❌ No sessions found! Add sessions using /addacc in DM or edit devour.json")
        print("Creating sample devour.json structure...")
        save_data()
        return
    apps = []
    for sess in SESSIONS:
        app = Client(
            sess["name"],
            api_id=API_ID,
            api_hash=API_HASH,
            session_string=sess["session_string"],
        )
        _attach_attack_method(app)
        register_handlers(app, apps)
        apps.append(app)
    print(f"🤖 Running {len(apps)} session(s) with DM-based control.")
    print(f"💾 Data file: {DATA_FILE}")
    print(f"📊 Current file: {CURRENT_FILE}")
    print(f"👤 Sudo users: {SUDO_USERS}")
    print(f"⏱️  Delay range: {DELAY_RANGE[0]}-{DELAY_RANGE[1]} seconds")
    print(f"📅 Scanning period: {SCANNING_DAYS} days")
    await asyncio.gather(*[a.start() for a in apps])
    await asyncio.get_event_loop().create_future()

if __name__ == "__main__":
    asyncio.run(main())
