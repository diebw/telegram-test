import asyncio
import json
import os
import sqlite3
from datetime import datetime
import urllib.parse
import html
import io
import re

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    InputMediaPhoto,
    InputMediaVideo,
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import aiohttp
from bs4 import BeautifulSoup

from tplus_search import (TOKEN_FILE, build_site_query, catalog_configured, close_mcp, format_results,
                          model_line_from_text, search_catalog)
from ai_recognition import RecognitionUnavailable, recognize_photo, transcribe_voice

# ==========================================
# 1. КОНФІГУРАЦІЯ ТА ІНІЦІАЛІЗАЦІЯ
# ==========================================

def _load_env(path: str = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")):
    """Читає налаштування з файлу .env поруч із ботом (рядки КЛЮЧ=значення). Змінні оточення мають пріоритет."""
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))
    except OSError:
        pass


_load_env()

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
if not BOT_TOKEN:
    raise SystemExit("❌ Не задано BOT_TOKEN. Створіть файл .env за зразком .env.example")
MANAGER_GROUP_ID = int(os.getenv("MANAGER_GROUP_ID", "0"))
ARCHIVE_CHANNEL_ID = int(os.getenv("ARCHIVE_CHANNEL_ID", "0"))
STATS_FILE = "closed_tickets.json"
DB_FILE = "bot_users.db"
NEWS_CHANNEL_USERNAME = "tplus_mrkt"

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
router = Router()

active_tickets = {}
group_msg_to_client = {}
last_parsed_news_id = None


# ==========================================
# 2. СТАНИ (FSM)
# ==========================================

class OrderStatusState(StatesGroup):
    waiting_for_order_number = State()
    waiting_for_comment = State()

class CancelOrderState(StatesGroup):
    waiting_for_order_number = State()

class SearchProductState(StatesGroup):
    waiting_for_query = State()


# ==========================================
# 3. ДОПОМІЖНІ ФУНКЦІЇ (ЛОГІКА ТА ПАРСИНГ)
# ==========================================

async def clear_state_by_timeout(chat_id: int, state: FSMContext, target_state: State, delay: int = 600):
    await asyncio.sleep(delay)
    if await state.get_state() == target_state.state:
        await state.clear()
        try:
            await bot.send_message(chat_id=chat_id, text="⏳ Час очікування вичерпано. Дію автоматично скасовано.", reply_markup=get_main_menu())
        except Exception:
            pass


# ==========================================
# 4. КЛАВІАТУРИ
# ==========================================

def get_main_menu():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔍 Пошук товару"), KeyboardButton(text="📦 Статус замовлення")],
            [KeyboardButton(text="📍 Де кур'єр?"), KeyboardButton(text="🚫 Скасування замовлення")],
            [KeyboardButton(text="📍 Контакти та адреса в Рівному"), KeyboardButton(text="🔄 Умови повернення")]
        ],
        resize_keyboard=True
    )

def get_cancel_action_keyboard():
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="❌ Скасувати дію")]], resize_keyboard=True)

def get_status_comment_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Пропустити коментар")], [KeyboardButton(text="❌ Скасувати дію")]],
        resize_keyboard=True
    )

def get_manager_macros_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="👋 Добрий день!", callback_data="macro_hello")],
            [InlineKeyboardButton(text="👤 Уточнити ПІБ", callback_data="macro_name")],
            [InlineKeyboardButton(text="❌ Замовлення скасоване", callback_data="macro_cancel")],
            [InlineKeyboardButton(text="🚚 Замовлення вже в дорозі", callback_data="macro_on_the_way")],
            [InlineKeyboardButton(text="📦 Відправка в дедлайн", callback_data="macro_deadline")],
            [InlineKeyboardButton(text="🔍 Уточнюю у гаранта якості", callback_data="macro_quality")]
        ]
    )


# ==========================================
# 5. РОБОТА З БАЗОЮ ДАНИХ ТА СТАТИСТИКОЮ
# ==========================================

def init_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()

def save_or_update_user(user_id: int, username: str, first_name: str):
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.execute("""
        INSERT INTO users (user_id, username, first_name) VALUES (?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET username=excluded.username, first_name=excluded.first_name
    """, (user_id, username or "", first_name or ""))
    conn.commit()
    conn.close()

def load_all_users():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute("SELECT user_id FROM users")
    users = [row[0] for row in cursor.fetchall()]
    conn.close()
    return users

def load_stats():
    if not os.path.exists(STATS_FILE):
        return []
    try:
        with open(STATS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []

def save_closed_ticket(manager_id: int, manager_name: str):
    data = load_stats()
    data.append({"manager_id": manager_id, "manager_name": manager_name, "closed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
    with open(STATS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def get_all_time_top():
    stats = {}
    for item in load_stats():
        stats[item["manager_name"]] = stats.get(item["manager_name"], 0) + 1
    return sorted(stats.items(), key=lambda x: x[1], reverse=True)

def get_daily_top():
    today = datetime.now().strftime("%Y-%m-%d")
    stats = {}
    for item in load_stats():
        if item["closed_at"].startswith(today):
            stats[item["manager_name"]] = stats.get(item["manager_name"], 0) + 1
    return sorted(stats.items(), key=lambda x: x[1], reverse=True)


# ==========================================
# 6. УТИЛІТИ ПОВІДОМЛЕНЬ ТА ПАРСИНГ НОВИН
# ==========================================

def get_message_description(message: Message) -> str:
    text = message.text or message.caption or ""
    if message.photo: return f"[Фото] {text}".strip()
    if message.video: return f"[Відео] {text}".strip()
    if message.voice: return f"[Голосове] {text}".strip()
    if message.document: return f"[Документ: {message.document.file_name or 'файл'}] {text}".strip()
    if message.audio: return f"[Аудіо] {text}".strip()
    return text if text else "[Медіа]"


async def send_user_content(target_chat_id: int, message: Message, header: str, reply_to_id: int = None, thread_id: int = None, reply_markup=None):
    text = f"{header}\n{message.caption or message.text or ''}".strip()
    kwargs = {"reply_to_message_id": reply_to_id, "parse_mode": "HTML"}
    if thread_id: kwargs["message_thread_id"] = thread_id
    if reply_markup: kwargs["reply_markup"] = reply_markup

    if message.photo: return await bot.send_photo(target_chat_id, photo=message.photo[-1].file_id, caption=text, **kwargs)
    if message.video: return await bot.send_video(target_chat_id, video=message.video.file_id, caption=text, **kwargs)
    if message.document: return await bot.send_document(target_chat_id, document=message.document.file_id, caption=text, **kwargs)
    if message.voice: return await bot.send_voice(target_chat_id, voice=message.voice.file_id, caption=text, **kwargs)
    if message.audio: return await bot.send_audio(target_chat_id, audio=message.audio.file_id, caption=text, **kwargs)
    return await bot.send_message(target_chat_id, text=text or header, **kwargs)


async def fetch_latest_channel_news():
    url = f"https://t.me/s/{NEWS_CHANNEL_USERNAME}"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                if response.status != 200: return None
                html = await response.text()
                
        soup = BeautifulSoup(html, "html.parser")
        posts = soup.find_all("div", class_="tgme_widget_message")
        if not posts: return None
            
        latest_post = posts[-1]
        post_id = latest_post.get("data-post", "")
        
        text_elem = latest_post.find("div", class_="tgme_widget_message_text")
        if text_elem:
            for br in text_elem.find_all("br"): br.replace_with("\n")
            for a in text_elem.find_all("a"): a.replace_with(f'<a href="{a.get("href")}">{a.get_text()}</a>')
            post_text = text_elem.decode_contents()
        else:
            post_text = "Без тексту"

        media_url = None
        photo_div = latest_post.find("div", class_="tgme_widget_message_photo_wrap")
        if photo_div and photo_div.has_attr("style"):
            style = photo_div["style"]
            if "url('" in style: media_url = style.split("url('")[1].split("')")[0]

        post_link = f"https://t.me/{NEWS_CHANNEL_USERNAME}/{post_id.split('/')[-1]}" if '/' in post_id else f"https://t.me/{NEWS_CHANNEL_USERNAME}"
        return {"id": post_id, "text": post_text, "link": post_link, "media_url": media_url}
    except Exception as e:
        print(f"Помилка парсингу новин: {e}")
        return None


async def check_new_posts_job():
    global last_parsed_news_id
    news = await fetch_latest_channel_news()
    if not news: return
        
    if last_parsed_news_id is None:
        last_parsed_news_id = news["id"]
        return
        
    if news["id"] != last_parsed_news_id:
        last_parsed_news_id = news["id"]
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📢 Розіслати клієнтам", callback_data="broadcast_news_action")],
            [InlineKeyboardButton(text="🔗 Читати в каналі", url=news["link"])]
        ])
        msg_text = f"⚡️ <b>Увага! Нова новина у каналі @{NEWS_CHANNEL_USERNAME}!</b>\n\n{news['text']}"
        try:
            if news["media_url"]:
                await bot.send_photo(chat_id=MANAGER_GROUP_ID, photo=news["media_url"], caption=msg_text, reply_markup=kb, parse_mode="HTML")
            else:
                await bot.send_message(chat_id=MANAGER_GROUP_ID, text=msg_text, reply_markup=kb, parse_mode="HTML")
        except Exception as e:
            print(f"Помилка надсилання авто-новини в групу: {e}")


# ==========================================
# 7. СТВОРЕННЯ ТІКЕТІВ
# ==========================================

async def create_ticket(message: Message, ticket_type: str, detail_text: str):
    user_id = message.from_user.id
    save_or_update_user(user_id, message.from_user.username, message.from_user.first_name)
    time_now = datetime.now().strftime("%H:%M:%S")

    media_list = []
    if message.photo: media_list.append({"file_id": message.photo[-1].file_id, "type": "photo", "caption": message.caption or ""})
    elif message.video: media_list.append({"file_id": message.video.file_id, "type": "video", "caption": message.caption or ""})

    if user_id in active_tickets:
        active_tickets[user_id]["history"].append(f"[{time_now}] Клієнт: {detail_text}")
        if media_list: active_tickets[user_id]["media_files"].extend(media_list)
        try:
            msg = await bot.send_message(MANAGER_GROUP_ID, f"📩 <b>{ticket_type}:</b>\n{detail_text}", message_thread_id=active_tickets[user_id]["thread_id"], parse_mode="HTML")
            group_msg_to_client[msg.message_id] = user_id
        except Exception:
            pass
        return

    try:
        topic = await bot.create_forum_topic(chat_id=MANAGER_GROUP_ID, name=f"🎫 {ticket_type}: {message.from_user.full_name}")
        thread_id = topic.message_thread_id
    except Exception:
        thread_id = None

    buttons = [[InlineKeyboardButton(text="Взяти в роботу 📥", callback_data=f"take_{user_id}")]].copy()
    if ticket_type == "📍 Де кур'єр":
        buttons.insert(0, [InlineKeyboardButton(text="🗺 Відкрити карту", url=f"https://t.me/Tracker_TM_bot?start=order_{user_id}")])

    header = f"🔴 <b>Новий запит! ({ticket_type})</b>\n👤 {message.from_user.full_name} (@{message.from_user.username or 'немає'})\n\n💬 {detail_text}"
    
    try:
        sent_msg = await bot.send_message(MANAGER_GROUP_ID, text=header, message_thread_id=thread_id, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")
        group_msg_to_client[sent_msg.message_id] = user_id
    except Exception as e:
        print(f"Помилка створення тікета в групі: {e}")
        sent_msg = None

    active_tickets[user_id] = {
        "manager_id": None, "manager_name": None, "group_msg_id": sent_msg.message_id if sent_msg else None,
        "thread_id": thread_id, "client_name": message.from_user.full_name,
        "client_username": message.from_user.username, "ticket_type": ticket_type,
        "opened_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "history": [f"[{time_now}] Клієнт: {detail_text}"], "media_files": media_list
    }


# ==========================================
# 8. РОУТЕР КЛІЄНТСЬКИХ КОМАНД ТА КНОПОК
# ==========================================

@router.message(F.chat.type == "private", CommandStart())
async def start_cmd(message: Message, state: FSMContext):
    await state.clear()
    save_or_update_user(message.from_user.id, message.from_user.username, message.from_user.first_name)
    await message.answer("Вітаємо у tplus.market! Оберить пункт у меню:", reply_markup=get_main_menu())


@router.message(F.chat.type == "private", F.text == "❌ Скасувати дію")
async def cancel_action(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("🔄 Дію скасовано.", reply_markup=get_main_menu())


@router.message(F.chat.type == "private", F.text == "📍 Контакти та адреса в Рівному")
async def contacts_info(message: Message, state: FSMContext):
    await state.clear()
    text = (
        "📍 <b>Наш офіс у Рівному</b>\n\n"
        "Адреса: <b>вул. Шевченка 3</b>, 4 поверх, каб. 409 (Будинок Побуту).\n\n"
        "📞 <b>Телефони:</b>\n• +38 (067) 357-82-20\n• +38 (097) 270-50-30\n• +38 (067) 430-95-14\n\n"
        "⏰ <b>Графік:</b> Пн-Пт 09:00–18:00 | Сб 09:00–15:00 | Нд 11:00–14:00"
    )
    await message.answer(text, parse_mode="HTML", reply_markup=get_main_menu())


@router.message(F.chat.type == "private", F.text == "🔄 Умови повернення")
async def return_terms(message: Message, state: FSMContext):
    await state.clear()
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔗 Умови повернення", url="https://tplus.market/help/terms-and-conditions-of-return")]])
    await message.answer("📌 Детальні правила повернення та обміну товару:", reply_markup=kb)


# --- Пошук товару через MCP API ---
@router.message(F.chat.type == "private", F.text == "🔍 Пошук товару")
async def search_start(message: Message, state: FSMContext):
    await state.set_state(SearchProductState.waiting_for_query)
    asyncio.create_task(clear_state_by_timeout(message.chat.id, state, SearchProductState.waiting_for_query))
    await message.answer("🔍 Введіть назву або модель товару (наприклад: <i>акб піксель 8 про</i>).\n📷 Або надішліть <b>фото</b> товару чи 🎤 <b>голосове</b>:", parse_mode="HTML", reply_markup=get_cancel_action_keyboard())

def search_result_keyboard(site_query: str) -> InlineKeyboardMarkup:
    web_url = f"https://tplus.market/search?search={urllib.parse.quote(site_query)}"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔎 Відкрити на сайті", url=web_url)],
        [InlineKeyboardButton(text="🔍 Шукати інший товар", callback_data="action_search_again"),
         InlineKeyboardButton(text="💬 Зв'язатися з менеджером", callback_data="action_contact_manager")]
    ])


async def do_search(message: Message, raw_query: str, status_msg: Message = None, note: str = "", outcome=None):
    """Показує знайдену позицію. Якщо є токен MCP - ще й ціни та наявність з каталогу."""
    if status_msg is None:
        status_msg = await message.answer("🔍 Шукаю...")

    site_query = build_site_query(raw_query)
    model_line = ""
    body = "⚠️ Каталог тимчасово недоступний. Перевірити наявність можна на сайті за кнопкою нижче або в менеджера."

    if catalog_configured():  # є вхід у TPlus (tplus_login.py) - шукаємо в каталозі
        try:
            outcome = outcome or await search_catalog(raw_query)
            site_query = outcome.site_query  # запит, який перевірено: на сайті він знаходить товари
            if outcome.client_model:  # клієнт написав заводський код: "CPH2579" -> "OPPO A38 (CPH2579)"
                model_line = f"\n📱 Модель: <i>{html.escape(outcome.client_model)}</i>"
            elif outcome.models:
                brand = outcome.brand or ""
                named = [m if not brand or brand.lower() in m.lower() else f"{brand} {m}" for m in outcome.models[:3]]
                shown = ", ".join(named) + ("…" if len(outcome.models) > 3 else "")
                if outcome.device_codes:  # ідентифікатори, за якими знайдено: "(A2588, A2589)"
                    shown += f" ({', '.join(outcome.device_codes[:3])})"
                model_line = f"\n📱 Модель: <i>{html.escape(shown)}</i>"
            body = format_results(outcome)
        except Exception as e:
            print(f"❌ Помилка пошуку в каталозі: {e!r}")
    else:
        print(f"❌ Каталог не підключено: немає файлу {TOKEN_FILE}. Запустіть: python tplus_login.py")

    header = (note + "\n" if note else "") + f"📦 <b>Запит:</b> <code>{html.escape(site_query)}</code>" + model_line
    await status_msg.edit_text(f"{header}\n\n{body}"[:4000], parse_mode="HTML",
                               reply_markup=search_result_keyboard(site_query), disable_web_page_preview=True)


@router.message(SearchProductState.waiting_for_query, F.chat.type == "private", F.photo)
async def search_by_photo(message: Message, state: FSMContext):
    status = await message.answer("📷 Розпізнаю товар на фото...")
    try:
        buf = io.BytesIO()
        await bot.download(message.photo[-1], destination=buf)
        result = await recognize_photo(buf.getvalue(), hint=message.caption or "")
    except RecognitionUnavailable as e:
        print(f"Розпізнавання фото не налаштоване: {e}")
        return await status.edit_text("⚠️ Пошук за фото тимчасово недоступний. Опишіть товар текстом або голосовим.")
    except Exception as e:
        print(f"Помилка розпізнавання фото: {e!r}")
        return await status.edit_text("⚠️ Не вдалося обробити фото. Спробуйте ще раз або опишіть товар текстом.")

    caption = (message.caption or "").strip()  # "треба акб сюди" - що саме шукати
    model_line = model_line_from_text(result.get("text", ""))
    if model_line:
        # на фото назва пристрою ("Redmi Note 12 Pro 5G") - шукаємо, як за текстом: деталь з підпису + модель
        note = f"📷 Розпізнано: <b>{html.escape(model_line)}</b>"
        return await do_search(message, f"{caption} {model_line}".strip(), status, note)

    if not result["query"]:
        return await status.edit_text("😕 Не знайшов на фото артикул чи маркування. Сфотографуйте наклейку з написом (наприклад, A1484 або EB-BS912ABY) крупніше й різкіше або опишіть товар текстом.")

    # на фото може бути кілька артикулів - беремо перший, який знайдено в каталозі
    chosen, found = result["query"], None
    queries = result.get("queries") or [chosen]
    if caption:  # підпис додаємо до кожного артикулу: "акб" + "EB-BS912ABY"
        queries = [f"{caption} {q}" for q in queries]
        chosen = queries[0]
    if catalog_configured() and len(queries) > 1:
        for candidate in queries[:4]:
            try:
                outcome = await search_catalog(candidate)
            except Exception as e:
                print(f"❌ Помилка пошуку в каталозі: {e!r}")
                break
            if outcome.items or outcome.related:
                chosen, found = candidate, outcome
                break

    note = f"📷 Розпізнано: <b>{html.escape(result['title'] or result['query'])}</b>"
    if not result["confident"]:
        note += "\n⚠️ <i>Модель на фото не видно точно — перевірте позицію.</i>"
    await do_search(message, chosen, status, note, found)


@router.message(SearchProductState.waiting_for_query, F.chat.type == "private", F.voice | F.audio)
async def search_by_voice(message: Message, state: FSMContext):
    voice = message.voice or message.audio
    if (voice.duration or 0) > 60:
        return await message.answer("⚠️ Голосове задовге. Надішліть, будь ласка, коротше (до 60 секунд).")

    status = await message.answer("🎤 Розпізнаю голосове...")
    try:
        buf = io.BytesIO()
        await bot.download(voice, destination=buf)
        text = await transcribe_voice(buf.getvalue())
    except RecognitionUnavailable as e:
        print(f"Розпізнавання голосу не налаштоване: {e}")
        return await status.edit_text("⚠️ Голосовий пошук тимчасово недоступний. Напишіть назву товару текстом.")
    except Exception as e:
        print(f"Помилка розпізнавання голосу: {e!r}")
        return await status.edit_text("⚠️ Не вдалося обробити голосове. Спробуйте ще раз або напишіть текстом.")

    if not text.strip():
        return await status.edit_text("😕 Не вдалося розібрати мовлення. Скажіть чіткіше (наприклад: «акб піксель 8 про») або напишіть текстом.")

    await do_search(message, text.strip(), status, f"🎤 Почув: <i>{html.escape(text.strip())}</i>")


@router.message(SearchProductState.waiting_for_query, F.chat.type == "private")
async def search_process(message: Message, state: FSMContext):
    raw_query = (message.text or "").strip()
    if not raw_query:
        return await message.answer("⚠️ Надішліть назву товару текстом, фото або голосове:", reply_markup=get_cancel_action_keyboard())
    await do_search(message, raw_query)


@router.callback_query(F.data == "action_search_again")
async def cb_search_again(callback: CallbackQuery, state: FSMContext):
    try: await callback.message.delete()
    except Exception: pass
    await state.set_state(SearchProductState.waiting_for_query)
    asyncio.create_task(clear_state_by_timeout(callback.message.chat.id, state, SearchProductState.waiting_for_query))
    await callback.message.answer("🔍 Введіть назву або модель наступного товару:", reply_markup=get_cancel_action_keyboard())
    await callback.answer()

@router.callback_query(F.data == "action_contact_manager")
async def cb_contact_manager(callback: CallbackQuery, state: FSMContext):
    try: await callback.message.delete()
    except Exception: pass
    await state.clear()
    dummy_msg = callback.message
    dummy_msg.from_user = callback.from_user
    await create_ticket(dummy_msg, "💬 Консультація після пошуку", "<b>Клієнт бажає зв'язатися з менеджером після пошуку товарів.</b>")
    await callback.message.answer("✅ Ваше звернення передано менеджерам. Очікуйте на зв'язок!", reply_markup=get_main_menu())
    await callback.answer()


# --- Статус замовлення (СУВОРО ВИКЛЮЧНИЙ ОБРОБНИК СТАНІВ) ---
@router.message(F.chat.type == "private", F.text == "📦 Статус замовлення")
async def status_start(message: Message, state: FSMContext):
    await state.set_state(OrderStatusState.waiting_for_order_number)
    asyncio.create_task(clear_state_by_timeout(message.chat.id, state, OrderStatusState.waiting_for_order_number))
    await message.answer("🔍 Введіть <b>лише цифри</b> замовлення:", parse_mode="HTML", reply_markup=get_cancel_action_keyboard())

@router.message(OrderStatusState.waiting_for_order_number, F.chat.type == "private")
async def status_number(message: Message, state: FSMContext):
    if not message.text.strip().isdigit():
        return await message.answer("⚠ Тільки цифри:", reply_markup=get_cancel_action_keyboard())
    await state.update_data(order_number=message.text.strip())
    await state.set_state(OrderStatusState.waiting_for_comment)
    asyncio.create_task(clear_state_by_timeout(message.chat.id, state, OrderStatusState.waiting_for_comment))
    await message.answer("Напишіть коментар або натисніть «Пропустити коментар»:", reply_markup=get_status_comment_keyboard())

@router.message(OrderStatusState.waiting_for_comment, F.chat.type == "private")
async def status_finish(message: Message, state: FSMContext):
    data = await state.get_data()
    order_num = data.get("order_number")
    
    comment = "не вказано" if message.text == "Пропустити коментар" else (message.text.strip() if message.text else "не вказано")

    await create_ticket(message, "🔍 Статус замовлення", f"<b>Номер:</b> <code>{order_num}</code>\n<b>Коментар:</b> {comment}")
    
    await message.answer(
        f"✅ Запит прийнято! Номер замовлення: <b>#{order_num}</b>\nМенеджери вже опрацьовують інформацію.", 
        parse_mode="HTML", 
        reply_markup=get_main_menu()
    )
    await state.clear()


# --- Кур'єр та скасування замовлення ---
@router.message(F.chat.type == "private", F.text == "📍 Де кур'єр?")
async def courier_handler(message: Message, state: FSMContext):
    await state.clear()
    await create_ticket(message, "📍 Де кур'єр", "<b>Клієнт запитує місцеперебування кур'єра.</b>")
    await message.answer("📍 Запит щодо кур'єра прийнято!", reply_markup=get_main_menu())

@router.message(F.chat.type == "private", F.text == "🚫 Скасування замовлення")
async def cancel_order_start(message: Message, state: FSMContext):
    await state.set_state(CancelOrderState.waiting_for_order_number)
    asyncio.create_task(clear_state_by_timeout(message.chat.id, state, CancelOrderState.waiting_for_order_number))
    await message.answer("🚫 Введіть номер замовлення для скасування:", reply_markup=get_cancel_action_keyboard())

@router.message(CancelOrderState.waiting_for_order_number, F.chat.type == "private")
async def cancel_order_finish(message: Message, state: FSMContext):
    if not message.text.strip().isdigit():
        return await message.answer("⚠ Введіть лише цифри:", reply_markup=get_cancel_action_keyboard())
    await create_ticket(message, "⚠ Скасування замовлення", f"<b>Номер замовлення:</b> <code>{message.text.strip()}</code>")
    await message.answer("📥 Запит на скасування прийнято!", reply_markup=get_main_menu())
    await state.clear()


# ==========================================
# 9. РОУТЕР МЕНЕДЖЕРСЬКИХ КОМАНД ТА ГРУПИ
# ==========================================

@router.message(Command("last_news"), F.chat.id == MANAGER_GROUP_ID)
async def cmd_last_news(message: Message):
    news = await fetch_latest_channel_news()
    if not news: return await message.reply("⚠️ Не вдалося отримати останню новину з каналу.")
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 Розіслати клієнтам", callback_data="broadcast_news_action")],
        [InlineKeyboardButton(text="🔗 Читати в каналі", url=news["link"])]
    ])
    msg_text = f"📌 <b>Остання новина з @{NEWS_CHANNEL_USERNAME}:</b>\n\n{news['text']}"
    
    if news["media_url"]:
        await message.reply_photo(photo=news["media_url"], caption=msg_text, reply_markup=kb, parse_mode="HTML")
    else:
        await message.reply(msg_text, reply_markup=kb, parse_mode="HTML")


@router.callback_query(F.data == "broadcast_news_action")
async def cb_news_broadcast(callback: CallbackQuery):
    msg = callback.message
    full_text = msg.caption or msg.text or ""
    news_to_send = full_text.split("\n\n", 1)[1] if "\n\n" in full_text else full_text

    if not news_to_send.strip(): return await callback.answer("Текст новини не знайдено для розсилки.", show_alert=True)
    users = load_all_users()
    if not users: return await callback.answer("База користувачів порожня.", show_alert=True)
    
    status_msg = await callback.message.answer("🚀 Починаю автоматичну розсилку новини клієнтам...")
    success, blocked, failed = 0, 0, 0
    photo_file_id = msg.photo[-1].file_id if msg.photo else None

    for user_id in users:
        try:
            if photo_file_id: await bot.send_photo(chat_id=user_id, photo=photo_file_id, caption=news_to_send, parse_mode="HTML")
            else: await bot.send_message(chat_id=user_id, text=news_to_send, parse_mode="HTML")
            success += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            err_str = str(e).lower()
            if "bot was blocked" in err_str or "user is deactivated" in err_str or "chat not found" in err_str: blocked += 1
            else: failed += 1

    await status_msg.edit_text(f"✅ <b>Розсилку новини завершено!</b>\n\n📤 Успішно: <b>{success}</b>\n🚫 Заблокували: <b>{blocked}</b>\n❌ Помилок: <b>{failed}</b>", parse_mode="HTML")
    await callback.answer()


@router.message(Command("broadcast"), F.chat.id == MANAGER_GROUP_ID)
async def cmd_broadcast(message: Message):
    text_to_send = message.text.replace("/broadcast", "").strip()
    reply_msg = message.reply_to_message

    if not text_to_send and not reply_msg:
        return await message.reply("⚠️ Напишіть текст після команди або скопіюйте/відведіть відповіддю на повідомлення.")

    users = load_all_users()
    if not users: return await message.reply("⚠️ База користувачів порожня.")

    status_msg = await message.reply(f"🚀 Починаю розсилку для {len(users)} користувачів...")
    success, blocked, failed = 0, 0, 0

    for user_id in users:
        try:
            if reply_msg: await reply_msg.send_copy(chat_id=user_id)
            else: await bot.send_message(chat_id=user_id, text=text_to_send, parse_mode="HTML")
            success += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            err_str = str(e).lower()
            if "bot was blocked" in err_str or "user is deactivated" in err_str or "chat not found" in err_str: blocked += 1
            else: failed += 1

    await status_msg.edit_text(f"✅ <b>Розсилку завершено!</b>\n\n📤 Успішно: <b>{success}</b>\n🚫 Заблокували: <b>{blocked}</b>\n❌ Помилок: <b>{failed}</b>", parse_mode="HTML")


@router.message(Command("top"), F.chat.id == MANAGER_GROUP_ID)
async def cmd_top(message: Message):
    data = get_all_time_top()
    text = "🏆 <b>Загальний топ менеджерів:</b>\n\n" + ("\n".join([f"{i}. {n} — <b>{c}</b>" for i, (n, c) in enumerate(data, 1)]) if data else "Пусто.")
    await message.answer(text, parse_mode="HTML")

@router.message(Command("top_day"), F.chat.id == MANAGER_GROUP_ID)
async def cmd_top_day(message: Message):
    data = get_daily_top()
    text = "📅 <b>Топ за сьогодні:</b>\n\n" + ("\n".join([f"{i}. {n} — <b>{c}</b>" for i, (n, c) in enumerate(data, 1)]) if data else "Сьогодні ще немає закритих тікетів.")
    await message.answer(text, parse_mode="HTML")


# --- ЗАГАЛЬНИЙ ОБРОБНИК ПРИВАТНИХ ПОВІДОМЛЬЕНЬ (ЗАВЖДИ ОСТАННІМ) ---
@router.message(F.chat.type == "private")
async def private_msg_handler(message: Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state is not None:
        return  

    user_id = message.from_user.id
    save_or_update_user(user_id, message.from_user.username, message.from_user.first_name)
    time_now = datetime.now().strftime("%H:%M:%S")
    desc = get_message_description(message)

    media_data = []
    if message.photo: media_data.append({"file_id": message.photo[-1].file_id, "type": "photo", "caption": message.caption or ""})
    elif message.video: media_data.append({"file_id": message.video.file_id, "type": "video", "caption": message.caption or ""})

    if media_data:
        if user_id in active_tickets: active_tickets[user_id]["media_files"].extend(media_data)
        manager_id = next((cid for cid, d in active_tickets.items() if d.get("manager_id") == user_id), None)
        if manager_id: active_tickets[manager_id]["media_files"].extend(media_data)

    manager_client_id = next((cid for cid, d in active_tickets.items() if d.get("manager_id") == user_id), None)
    if manager_client_id:
        active_tickets[manager_client_id]["history"].append(f"[{time_now}] Менеджер ({message.from_user.first_name}): {desc}")
        try:
            await send_user_content(manager_client_id, message, "<b>Менеджер:</b>")
            await message.answer("✅ Відправлено клієнту.")
        except Exception as e:
            await message.answer(f"❌ Помилка: {e}")
        return

    if user_id in active_tickets:
        active_tickets[user_id]["history"].append(f"[{time_now}] Клієнт: {desc}")
        t_data = active_tickets[user_id]
        if t_data["manager_id"]:
            try: await send_user_content(t_data["manager_id"], message, "<b>Клієнт:</b>")
            except Exception: pass
        try:
            msg = await send_user_content(MANAGER_GROUP_ID, message, "📩 <b>Повідомлення від клієнта:</b>", thread_id=t_data["thread_id"])
            if msg: group_msg_to_client[msg.message_id] = user_id
        except Exception:
            pass
        return

    try:
        topic = await bot.create_forum_topic(chat_id=MANAGER_GROUP_ID, name=f"🎫 Тікет: {message.from_user.full_name}")
        thread_id = topic.message_thread_id
    except Exception:
        thread_id = None

    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Взяти в роботу 📥", callback_data=f"take_{user_id}")]])
    header = f"🔴 <b>Новий запит!</b>\n👤 {message.from_user.full_name} (@{message.from_user.username or 'немає'})\n\n💬 <b>Питання:</b>"
    
    try:
        sent_msg = await bot.send_message(MANAGER_GROUP_ID, text=header, message_thread_id=thread_id, reply_markup=kb, parse_mode="HTML")
        group_msg_to_client[sent_msg.message_id] = user_id
    except Exception:
        sent_msg = None

    active_tickets[user_id] = {
        "manager_id": None, "manager_name": None, "group_msg_id": sent_msg.message_id if sent_msg else None,
        "thread_id": thread_id, "client_name": message.from_user.full_name,
        "client_username": message.from_user.username, "ticket_type": "Загальне питання",
        "opened_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "history": [f"[{time_now}] Клієнт: {desc}"], "media_files": media_data
    }
    await message.answer("Ваш запит передано менеджерам. Очікуйте на зв'язок!", reply_markup=get_main_menu())


@router.message(F.chat.id == MANAGER_GROUP_ID)
async def group_msg_handler(message: Message):
    if message.text and message.text.startswith("/"): return
    thread_id = message.message_thread_id
    if not thread_id: return

    client_id = next((cid for cid, d in active_tickets.items() if d.get("thread_id") == thread_id), None)
    if not client_id or client_id not in active_tickets: return

    t_data = active_tickets[client_id]
    if not t_data.get("manager_id") or t_data["manager_id"] != message.from_user.id:
        return await message.reply("⚠️ Натисніть кнопку <b>«Взяти в роботу»</b> в цьому тікеті!", parse_mode="HTML")

    time_now = datetime.now().strftime("%H:%M:%S")
    desc = get_message_description(message)
    t_data["history"].append(f"[{time_now}] Менеджер ({message.from_user.first_name}): {desc}")

    if message.photo: t_data["media_files"].append({"file_id": message.photo[-1].file_id, "type": "photo", "caption": message.caption or ""})
    elif message.video: t_data["media_files"].append({"file_id": message.video.file_id, "type": "video", "caption": message.caption or ""})

    try:
        await send_user_content(client_id, message, "<b>Менеджер:</b>")
        await message.react([{"type": "emoji", "emoji": "👍"}])
        group_msg_to_client[message.message_id] = client_id
    except Exception as e:
        await message.reply(f"❌ Помилка: {e}")


@router.callback_query(F.data.startswith("take_"))
async def take_ticket(callback: CallbackQuery):
    client_id = int(callback.data.split("_")[1])
    if client_id not in active_tickets: return await callback.answer("Цей тікет вже неактуальний.", show_alert=True)

    t_data = active_tickets[client_id]
    t_data["manager_id"] = callback.from_user.id
    t_data["manager_name"] = callback.from_user.first_name

    close_kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Закрити звернення", callback_data=f"close_{client_id}")]])
    is_media = bool(callback.message.photo or callback.message.video or callback.message.document)
    orig_text = (callback.message.caption if is_media else callback.message.text) or ""
    new_text = orig_text.replace("🔴 Новий запит!", f"🟡 <b>В роботі: {callback.from_user.first_name} 🟢</b>")

    if is_media: await callback.message.edit_caption(caption=new_text, reply_markup=close_kb, parse_mode="HTML")
    else: await callback.message.edit_text(text=new_text, reply_markup=close_kb, parse_mode="HTML")

    await bot.send_message(client_id, f"Менеджер {callback.from_user.first_name} підключився до діалогу.")
    await bot.send_message(chat_id=MANAGER_GROUP_ID, text="⚡️ <b>Швидкі відповіді для клієнта:</b>", message_thread_id=t_data.get("thread_id"), reply_markup=get_manager_macros_keyboard(), parse_mode="HTML")
    await callback.answer("Ви взяли запит у роботу!")


@router.callback_query(F.data.startswith("macro_"))
async def handle_macros(callback: CallbackQuery):
    manager_id = callback.from_user.id
    client_id = next((cid for cid, d in active_tickets.items() if d.get("manager_id") == manager_id), None)
    if not client_id or client_id not in active_tickets: return await callback.answer("Ви не ведете активного діалогу.", show_alert=True)

    macros = {
        "macro_hello": "Добрий день!",
        "macro_name": "Підкажіть, Ваше прізвище та ім'я.",
        "macro_cancel": "Замовлення скасоване.",
        "macro_on_the_way": "Замовлення вже в дорозі до Вас.",
        "macro_deadline": "Ваше замовлення буде відправлено в наступний дедлайн.",
        "macro_quality": "Уточнюю інформацію у гаранта якості."
    }

    text = macros.get(callback.data)
    if not text: return await callback.answer("Помилка макросу.", show_alert=True)

    time_now = datetime.now().strftime("%H:%M:%S")
    t_data = active_tickets[client_id]
    t_data["history"].append(f"[{time_now}] Менеджер ({callback.from_user.first_name}): {text}")

    try:
        await bot.send_message(client_id, f"<b>Менеджер:</b>\n{text}", parse_mode="HTML")
        msg = await bot.send_message(chat_id=MANAGER_GROUP_ID, text=f"<b>Менеджер ({callback.from_user.first_name}) [Швидка відповідь]:</b>\n{text}", message_thread_id=t_data.get("thread_id"), parse_mode="HTML")
        group_msg_to_client[msg.message_id] = client_id
        await callback.answer("✅ Надіслано!")
    except Exception as e:
        await callback.answer(f"❌ Помилка: {e}", show_alert=True)


@router.callback_query(F.data.startswith("close_"))
async def close_ticket(callback: CallbackQuery):
    client_id = int(callback.data.split("_")[1])
    if client_id not in active_tickets: return await callback.answer("Тікет вже закритий.", show_alert=True)

    d = active_tickets[client_id]
    if d.get("manager_id"): save_closed_ticket(d["manager_id"], d["manager_name"])

    archive_text = (
        f"📁 <b>Архів: Закритий тікет</b>\n\n"
        f"📌 <b>Тип:</b> {d['ticket_type']}\n"
        f"👤 <b>Клієнт:</b> {d['client_name']} (@{d['client_username'] or 'немає'}) [<code>{client_id}</code>]\n"
        f"👨‍💻 <b>Менеджер:</b> {d['manager_name']}\n"
        f"⏱ <b>Створено:</b> {d['opened_at']} | ✅ <b>Закрито:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        f"📜 <b>Історія:</b>\n" + ("\n".join(d["history"]) if d["history"] else "<i>Пусто</i>")
    )

    if len(archive_text) > 4000: archive_text = archive_text[:3950] + "\n\n<i>[Скорочено]</i>"

    try:
        archive_msg = await bot.send_message(ARCHIVE_CHANNEL_ID, archive_text, parse_mode="HTML")
        if d["media_files"]:
            media_group = []
            for i, m in enumerate(d["media_files"][:10]):
                cap = "📎 Медіа до тікета" if i == 0 else ""
                if m["type"] == "photo": media_group.append(InputMediaPhoto(media=m["file_id"], caption=cap))
                elif m["type"] == "video": media_group.append(InputMediaVideo(media=m["file_id"], caption=cap))
            if media_group: await bot.send_media_group(ARCHIVE_CHANNEL_ID, media=media_group, reply_to_message_id=archive_msg.message_id)
    except Exception as e:
        print(f"Помилка архіву: {e}")

    del active_tickets[client_id]

    try: await bot.send_message(client_id, "Діалог завершено. Якщо є ще питання — напишіть сюди знову!", reply_markup=get_main_menu())
    except Exception: pass

    if d["thread_id"]:
        try: await bot.delete_forum_topic(MANAGER_GROUP_ID, d["thread_id"])
        except Exception: pass

    if d["group_msg_id"]:
        try: await bot.delete_message(MANAGER_GROUP_ID, d["group_msg_id"])
        except Exception: pass

    await callback.answer("Звернення закрито і збережено в архіві!")


# ==========================================
# 10. ЗАПУСК БОТА
# ==========================================

async def main():
    init_db()
    scheduler = AsyncIOScheduler()
    
    scheduler.add_job(lambda: bot.send_message(MANAGER_GROUP_ID, f"📊 <b>Підсумки дня:</b>\n\n" + ("\n".join([f"{i}. {n} — <b>{c}</b>" for i, (n, c) in enumerate(get_daily_top(), 1)]) if get_daily_top() else "Сьогодні без закритих тікетів."), parse_mode="HTML"), 'cron', hour=18, minute=0)
    scheduler.add_job(check_new_posts_job, 'interval', minutes=15)
    
    scheduler.start()
    dp.include_router(router)
    
    print("✅ Бот успішно запущено!")
    if catalog_configured():
        print(f"✅ Каталог TPlus підключено (токени: {TOKEN_FILE})")
    else:
        print(f"⚠️ Каталог TPlus НЕ підключено: немає {TOKEN_FILE}. Запустіть: python tplus_login.py")
    try:
        await dp.start_polling(bot)
    finally:
        await close_mcp()

if __name__ == "__main__":
    asyncio.run(main())