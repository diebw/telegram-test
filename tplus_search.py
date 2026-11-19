"""
Пошук по каталогу TPlus через MCP для Telegram-бота.

Що робить цей модуль (так само, як це робить Claude з конектором):
  1. Нормалізує запит: "акб піксель 8 про" -> категорія АКБ + бренд Google + модель "pixel 8 pro".
  2. Знаходить id бренду (list_brands) і моделі (list_models).
  3. Викликає search_products з фільтрами brandIds / modelIds / categoryId.
  4. Якщо нічого не знайдено - послідовно послаблює фільтри.
  5. Форматує відповідь для Telegram (ціна, наявність у Рівному, дата доставки).

Авторизація: сервер TPlus використовує OAuth. Один раз запустіть `python tplus_login.py` -
токени збережуться у tplus_tokens.json, далі бот оновлює їх сам.

Необов'язкові змінні оточення:
  TPLUS_MCP_URL     - адреса MCP (за замовчуванням https://dev.tplus.market/api/mcp)
  TPLUS_TOKEN_FILE  - шлях до файлу з токенами (за замовчуванням tplus_tokens.json поруч із ботом)
  TPLUS_MCP_TOKEN   - готовий статичний токен (якщо вже є, і вхід через tplus_login.py не потрібен)
"""

import asyncio
import difflib
import html
import json
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import aiohttp

import part_lookup

MCP_URL = os.getenv("TPLUS_MCP_URL", "https://dev.tplus.market/api/mcp")
MCP_TOKEN = os.getenv("TPLUS_MCP_TOKEN", "")
TOKEN_FILE = os.getenv("TPLUS_TOKEN_FILE", os.path.join(os.path.dirname(os.path.abspath(__file__)), "tplus_tokens.json"))
PROTOCOL_VERSION = "2025-03-26"
CACHE_TTL = 6 * 3600  # бренди й моделі змінюються рідко


# ==========================================
# 1. MCP-КЛІЄНТ (JSON-RPC поверх HTTP)
# ==========================================

class MCPError(Exception):
    pass


class MCPAuthError(MCPError):
    """Сервер відповів 401/403 - потрібен (інший) токен."""


class _SessionExpired(Exception):
    pass


def _content_text(result: dict) -> str:
    return "\n".join(
        block.get("text", "")
        for block in result.get("content", [])
        if isinstance(block, dict) and block.get("type") == "text"
    )


class TokenStore:
    """Читає токени з файлу (його створює tplus_login.py) і оновлює access_token через refresh_token."""

    def __init__(self, path: str, static_token: str = ""):
        self.path = path
        self.static = static_token
        self._data: Optional[dict] = None
        self._lock = asyncio.Lock()

    def _load(self) -> dict:
        if self._data is None:
            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    self._data = json.load(f)
            except (OSError, ValueError):
                self._data = {}
        return self._data

    def _reload(self) -> dict:
        self._data = None
        return self._load()

    def configured(self) -> bool:
        return bool(self._load().get("access_token") or self.static)

    def _expired(self) -> bool:
        return time.time() > self._load().get("expires_at", 0) - 60

    async def get(self, bad_token: Optional[str] = None) -> str:
        """Актуальний access_token. bad_token - токен, який сервер щойно відхилив (потрібне оновлення)."""
        data = self._load()
        if not data.get("access_token"):
            return self.static
        if bad_token is None and not self._expired():
            return data["access_token"]
        async with self._lock:
            data = self._reload()  # інший процес (другий запуск бота, tplus_debug.py) міг уже оновити токен
            if bad_token is not None:
                already_renewed = data.get("access_token") != bad_token
            else:
                already_renewed = not self._expired()
            if not already_renewed:
                await self._refresh(data)
            return self._load().get("access_token", "")

    async def _refresh(self, data: dict, retry: bool = True):
        if not data.get("refresh_token"):
            raise MCPAuthError("Немає refresh_token - запустіть python tplus_login.py ще раз")
        form = {
            "grant_type": "refresh_token",
            "refresh_token": data["refresh_token"],
            "client_id": data["client_id"],
            "resource": data.get("resource", MCP_URL),
        }
        if data.get("client_secret"):
            form["client_secret"] = data["client_secret"]
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
            async with session.post(data["token_endpoint"], data=form, headers={"Accept": "application/json"}) as resp:
                body = await resp.text()
                if resp.status != 200:
                    if retry and "invalid_grant" in body:  # токен міг оновити інший процес - беремо свіжий із файлу
                        fresh = self._reload()
                        if fresh.get("refresh_token") and fresh.get("refresh_token") != data.get("refresh_token"):
                            return await self._refresh(fresh, retry=False)
                    raise MCPAuthError(f"Не вдалося оновити токен (HTTP {resp.status}): {body[:200]}. "
                                       "Запустіть python tplus_login.py ще раз. "
                                       "Переконайтеся, що бот запущений лише в одному вікні")
        token = json.loads(body)
        data["access_token"] = token["access_token"]
        if token.get("refresh_token"):  # сервер може міняти refresh_token при кожному оновленні
            data["refresh_token"] = token["refresh_token"]
        data["expires_at"] = time.time() + int(token.get("expires_in", 3600))
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, self.path)


class MCPClient:
    def __init__(self, url: str, tokens: TokenStore):
        self.url = url
        self.tokens = tokens
        self._http: Optional[aiohttp.ClientSession] = None
        self._sid: Optional[str] = None
        self._ready = False
        self._rid = 0
        self._init_lock = asyncio.Lock()

    def _headers(self, token: str) -> dict:
        headers = {
            "Content-Type": "application/json",
            # MCP-сервер має право відповісти і JSON, і SSE - приймаємо обидва
            "Accept": "application/json, text/event-stream",
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"
        if self._sid:
            headers["Mcp-Session-Id"] = self._sid
        return headers

    def _session(self) -> aiohttp.ClientSession:
        if self._http is None or self._http.closed:
            self._http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))
        return self._http

    async def close(self):
        if self._http and not self._http.closed:
            await self._http.close()

    @staticmethod
    def _decode(text: str, content_type: str, rid: int) -> dict:
        if "text/event-stream" in content_type:
            for line in text.splitlines():
                if not line.startswith("data:"):
                    continue
                try:
                    msg = json.loads(line[5:].strip())
                except ValueError:
                    continue
                if isinstance(msg, dict) and msg.get("id") == rid:
                    return msg
            raise MCPError("SSE-відповідь не містить результату")
        try:
            msg = json.loads(text)
        except ValueError:
            raise MCPError(f"Некоректна відповідь MCP: {text[:200]}")
        if not isinstance(msg, dict):
            raise MCPError("Неочікуваний формат відповіді MCP")
        return msg

    async def _post(self, payload: dict, wait: bool = True) -> Optional[dict]:
        token = await self.tokens.get()
        for attempt in (1, 2):
            async with self._session().post(self.url, json=payload, headers=self._headers(token)) as resp:
                if resp.status in (401, 403):
                    if attempt == 1:  # токен міг прострочитись - пробуємо оновити й повторити
                        new_token = await self.tokens.get(bad_token=token)
                        if new_token and new_token != token:
                            token = new_token
                            continue
                    raise MCPAuthError(f"HTTP {resp.status}")
                if resp.status == 404 and self._sid:
                    raise _SessionExpired()
                if resp.status >= 400:
                    raise MCPError(f"HTTP {resp.status}: {(await resp.text())[:200]}")
                if resp.headers.get("Mcp-Session-Id"):
                    self._sid = resp.headers["Mcp-Session-Id"]
                if not wait:
                    return None
                text = await resp.text()
                msg = self._decode(text, resp.headers.get("Content-Type", ""), payload["id"])
            if "error" in msg:
                err = msg["error"]
                raise MCPError(err.get("message", str(err)) if isinstance(err, dict) else str(err))
            return msg.get("result", {})

    async def _ensure_ready(self):
        async with self._init_lock:
            if self._ready:
                return
            self._rid += 1
            await self._post({
                "jsonrpc": "2.0", "id": self._rid, "method": "initialize",
                "params": {
                    "protocolVersion": PROTOCOL_VERSION,
                    "capabilities": {},
                    "clientInfo": {"name": "tplus-telegram-bot", "version": "1.0"},
                },
            })
            await self._post({"jsonrpc": "2.0", "method": "notifications/initialized"}, wait=False)
            self._ready = True

    async def call_tool(self, name: str, arguments: dict) -> dict:
        result = None
        for attempt in (1, 2):
            await self._ensure_ready()
            self._rid += 1
            try:
                result = await self._post({
                    "jsonrpc": "2.0", "id": self._rid, "method": "tools/call",
                    "params": {"name": name, "arguments": arguments},
                })
                break
            except _SessionExpired:
                self._sid, self._ready = None, False
                if attempt == 2:
                    raise MCPError("Сесія MCP недійсна")
        if result.get("isError"):
            raise MCPError(_content_text(result) or "Помилка інструмента MCP")
        structured = result.get("structuredContent")
        if isinstance(structured, dict) and "items" in structured:
            return structured
        text = _content_text(result)
        try:
            data = json.loads(text)
            return data if isinstance(data, dict) else {"items": data}
        except ValueError:
            return structured if isinstance(structured, dict) else {"text": text}


_tokens = TokenStore(TOKEN_FILE, MCP_TOKEN)
_client = MCPClient(MCP_URL, _tokens)


def catalog_configured() -> bool:
    """True, якщо бот уже авторизований у TPlus (є файл з токенами або статичний токен)."""
    return _tokens.configured()
_cache: dict = {}


async def close_mcp():
    await _client.close()


async def _cached(key, factory):
    hit = _cache.get(key)
    if hit and time.time() - hit[0] < CACHE_TTL:
        return hit[1]
    value = await factory()
    _cache[key] = (time.time(), value)
    return value


# ==========================================
# 2. РОЗБІР ЗАПИТУ КОРИСТУВАЧА
# ==========================================

# --- що саме шукають: слово -> (id категорії в TPlus, слово для пошуку в назвах товарів)
_KIND_GROUPS = [
    (("акб", "acb", "акумулятор", "акумулятори", "акумуляторів", "аккумулятор", "аккумуляторы", "аккум", "акум",
      "батарея", "батареї", "батарею", "battery"), 177, "акумулятор"),
    (("дисплей", "дисплеї", "дисплея", "екран", "экран", "модуль", "lcd", "display"), 194, "дисплей"),
    (("матриця", "матрица", "матрицю"), 243, "матриця"),
    (("тачскрін", "тачскрин", "тачскріни", "сенсор", "touchscreen"), 298, "тачскрін"),
    (("шлейф", "шлейфи", "шлейфа"), 314, "шлейф"),
    (("динамік", "динамик", "динаміки", "спікер", "speaker"), 193, "динамік"),
    (("мікрофон", "микрофон"), 247, "мікрофон"),
    (("камера", "камери", "камеру", "camera"), 230, "камера"),
    (("роз'єм", "розєм", "розъем", "разъем", "разъём", "гніздо", "конектор"), 287, "роз'єм"),
    (("кнопка", "кнопки", "кнопку"), 235, "кнопка"),
    (("кришка", "кришку", "крышка"), 845, "кришка"),
    (("корпус", "корпуса", "корпусу"), 342, "корпус"),
    (("чохол", "чохли", "чехол"), 179, "чохол"),
    (("захисне_скло",), 218, "скло"),
    (("кабель", "шнур"), 228, "кабель"),
    (("зарядка", "зарядку", "зарядний", "зарядне", "зарядное"), 217, "зарядний"),
    (("powerbank", "павербанк", "повербанк"), 131, "power bank"),
    (("навушники", "наушники"), 344, "навушники"),
]
KIND_WORDS = {w: (cid, label) for words, cid, label in _KIND_GROUPS for w in words}

# ремонтні категорії: без моделі в них шукати безглуздо (тисячі позицій)
BATTERY_CATEGORIES = {177, 176, 834, 825}  # АКБ телефонів/планшетів/ноутбуків: шукаються також за артикулом

REPAIR_CATEGORIES = {177, 176, 194, 243, 298, 314, 193, 247, 230, 287, 235, 845, 342}

# група брендів у каталозі: моделі Redmi лежать у Xiaomi, Honor перемішаний з Huawei
BRAND_GROUPS = {
    "samsung": ("samsung",), "google": ("google",), "apple": ("apple",),
    "xiaomi": ("xiaomi", "redmi"), "huawei": ("huawei", "honor"), "honor": ("honor", "huawei"),
    "realme": ("realme",), "oppo": ("oppo",), "vivo": ("vivo",), "oneplus": ("oneplus",),
    "motorola": ("motorola",), "nokia": ("nokia",), "sony": ("sony",), "lg": ("lg",),
    "lenovo": ("lenovo",), "asus": ("asus",), "acer": ("acer",), "hp": ("hp",), "dell": ("dell",), "msi": ("msi",),
    "infinix": ("infinix",), "tecno": ("tecno",), "zte": ("zte",), "meizu": ("meizu",), "alcatel": ("alcatel",),
    "nothing": ("nothing phone",), "microsoft": ("microsoft",),
}
_BRAND_ALIAS_GROUPS = {
    "samsung": ("samsung", "самсунг", "самсунга", "самсунгу", "сумсунг", "самсунгі"),
    "google": ("google", "гугл"),
    "apple": ("apple", "епл", "эпл", "еппл"),
    "xiaomi": ("xiaomi", "сяомі", "сяоми", "ксяомі", "ксіомі", "ксіаомі", "ксяоми", "ксиаоми"),
    "huawei": ("huawei", "хуавей", "хуавэй", "хуявей", "хуйвей", "хуаві", "хуавеї"),
    "honor": ("honor", "хонор"),
    "realme": ("realme", "реалмі", "реалми", "ріалмі"),
    "oppo": ("oppo", "оппо", "опо"), "vivo": ("vivo", "віво", "виво"),
    "oneplus": ("oneplus", "ванплас", "ванпласс"),
    "motorola": ("motorola", "моторола"), "nokia": ("nokia", "нокіа", "нокия"),
    "sony": ("sony", "сони", "соні"), "lg": ("lg", "лджі", "елджі"),
    "lenovo": ("lenovo", "леново"), "asus": ("asus", "асус"), "acer": ("acer", "асер", "ейсер"),
    "hp": ("hp", "хп"), "dell": ("dell", "делл"), "msi": ("msi", "мси"),
    "infinix": ("infinix", "інфінікс", "инфиникс"), "tecno": ("tecno", "текно", "техно"),
    "zte": ("zte", "зте"), "meizu": ("meizu", "мейзу"), "alcatel": ("alcatel", "алкатель"),
    "nothing": ("nothing", "нотінг"), "microsoft": ("microsoft", "майкрософт"),
}
BRAND_ALIASES = {alias: key for key, aliases in _BRAND_ALIAS_GROUPS.items() for alias in aliases}

# слова-серії: визначають бренд І лишаються в назві моделі (Pixel 9, iPhone 13, Redmi Note 12)
FAMILY = {
    "pixel": ("google", "pixel"), "піксель": ("google", "pixel"), "пиксель": ("google", "pixel"),
    "galaxy": ("samsung", "galaxy"), "галакси": ("samsung", "galaxy"),
    "iphone": ("apple", "iphone"), "айфон": ("apple", "iphone"), "ipad": ("apple", "ipad"), "айпад": ("apple", "ipad"),
    "macbook": ("apple", "macbook"), "макбук": ("apple", "macbook"),
    "redmi": ("xiaomi", "redmi"), "редмі": ("xiaomi", "redmi"), "редми": ("xiaomi", "redmi"),
    "poco": ("xiaomi", "poco"), "поко": ("xiaomi", "poco"),
}
FAMILY_WORDS = {latin for _, latin in FAMILY.values()}  # pixel, iphone, redmi... - з нашого словника, не шум
OPTIONAL_WORDS = {"galaxy"}  # є не в усіх назвах моделей Samsung

MODEL_WORDS = {
    "про": "pro", "плюс": "plus", "ультра": "ultra", "макс": "max", "міні": "mini", "мини": "mini",
    "лайт": "lite", "хл": "xl", "фе": "fe", "нот": "note", "нео": "neo", "прайм": "prime",
    "свіфт": "swift", "свифт": "swift", "аспаєр": "aspire", "аспайр": "aspire", "нітро": "nitro", "нитро": "nitro",
    "ідеапад": "ideapad", "идеапад": "ideapad", "вівобук": "vivobook", "вивобук": "vivobook",
    "зенбук": "zenbook", "тінкпад": "thinkpad", "тинкпад": "thinkpad",
    "таб": "tab", "мейт": "mate", "мэйт": "mate", "нова": "nova", "фолд": "fold", "фліп": "flip", "флип": "flip",
    "ейр": "air", "аір": "air", "еір": "air", "еир": "air", "эир": "air", "аир": "air", "эйр": "air", "эйр": "air", "мікс": "mix", "ейс": "ace", "актив": "active", "смарт": "smart",
    "ксперія": "xperia", "експерія": "xperia",
}

# якість / колір: додаються до пошукового тексту (на сайті "дисплей oled" звужує список)
QUALITY = {
    "oled": "oled", "амолед": "amoled", "amoled": "amoled", "інселл": "incell", "incell": "incell", "in-cell": "incell",
    "tft": "tft", "service": "service", "сервіс": "service", "oem": "oem", "оем": "oem", "hard": "hard", "soft": "soft",
    "refurbished": "refurbished", "refurb": "refurbished", "used": "used", "оригінал": "original",
    "оригинал": "original", "original": "original",
    "black": "black", "чорний": "black", "чорна": "black", "чорне": "black", "чёрный": "black",
    "white": "white", "білий": "white", "біла": "white", "белый": "white", "blue": "blue", "синій": "blue",
    "green": "green", "зелений": "green", "gold": "gold", "золотий": "gold", "silver": "silver", "red": "red",
    "червоний": "red", "pink": "pink", "рожевий": "pink", "gray": "gray", "grey": "gray", "сірий": "gray",
}

STOPWORDS = {"для", "на", "до", "в", "у", "і", "та", "и", "з", "із", "с", "від", "по", "при", "под", "for", "the",
             "купити", "купить", "хочу", "треба", "потрібен", "потрібна", "потрібно", "нужен", "нужна", "нужно",
             "є", "есть", "ціна", "цена", "скільки", "коштує", "шукаю", "ищу", "знайти", "мені", "мне", "під",
             "будь", "ласка", "підкажіть", "підкажи", "покажи", "покажіть", "чи", "який", "яка", "які", "шт", "штук",
             "pcs", "пожалуйста", "найти", "найди", "знайди", "потрібні", "нужны",
             "терміново", "срочно", "швидко", "недорого", "дешево", "дешевий", "якісний", "хороший", "гарний",
             "новий", "нова", "нове", "наявність", "наявності", "дякую", "привіт", "добрий", "день", "вечір",
             "здрастуйте", "вітаю", "please", "hello", "hi", "дуже", "просто", "можна", "можливо",
             "сюди", "сюда", "тут", "цей", "ця", "це", "цього", "такий", "таку", "такої", "такий", "такое",
             "ось", "вот", "фото", "фотці", "фото", "моделі", "модель", "модели", "телефон", "телефону", "телефона"}

# слова "ноутбук": акумулятор/дисплей тоді шукаємо у ноутбучних категоріях
LAPTOP_WORDS = {"ноутбук", "ноутбука", "ноутбуку", "ноутбуки", "ноут", "лептоп", "ноутбучний", "laptop", "notebook"}
LAPTOP_CATEGORY = {177: 176, 194: 243}  # АКБ -> Акумулятори для ноутбуків, дисплей -> Матриці для ноутбуків

# числівники словами (голосові): "дев'ять" -> 9, "двадцять три" -> 23
_NUM = {
    "нуль": 0, "ноль": 0, "один": 1, "одна": 1, "одне": 1, "два": 2, "дві": 2, "две": 2, "три": 3, "чотири": 4,
    "четыре": 4, "п'ять": 5, "пять": 5, "шість": 6, "шесть": 6, "сім": 7, "семь": 7, "вісім": 8, "восемь": 8,
    "дев'ять": 9, "девять": 9, "десять": 10, "одинадцять": 11, "одиннадцать": 11, "дванадцять": 12,
    "двенадцать": 12, "тринадцять": 13, "тринадцать": 13, "чотирнадцять": 14, "четырнадцать": 14,
    "п'ятнадцять": 15, "пятнадцать": 15, "шістнадцять": 16, "шестнадцать": 16, "сімнадцять": 17,
    "семнадцать": 17, "вісімнадцять": 18, "восемнадцать": 18, "дев'ятнадцять": 19, "девятнадцать": 19,
}
_TENS = {"двадцять": 20, "двадцать": 20, "тридцять": 30, "тридцать": 30, "сорок": 40, "п'ятдесят": 50,
         "пятьдесят": 50, "шістдесят": 60, "шестьдесят": 60, "сімдесят": 70, "семьдесят": 70,
         "вісімдесят": 80, "восемьдесят": 80, "дев'яносто": 90, "девяносто": 90}

# кирилиця -> латиниця (фонетично). У кодах моделей (з цифрою або 1-2 літери) р -> p, як у "р30"
_PHON = {"а": "a", "б": "b", "в": "v", "г": "g", "ґ": "g", "д": "d", "е": "e", "є": "ye", "ж": "zh", "з": "z",
         "и": "i", "і": "i", "ї": "yi", "й": "y", "к": "k", "л": "l", "м": "m", "н": "n", "о": "o", "п": "p",
         "р": "r", "с": "s", "т": "t", "у": "y", "ф": "f", "х": "x", "ц": "c", "ч": "ch", "ш": "sh",
         "щ": "sch", "ь": "", "ъ": "", "ы": "y", "э": "e", "ю": "yu", "я": "ya"}
_ALT = {"с": "c", "в": "b", "н": "h"}  # інше читання: "с23" -> c23, "в21" -> b21


@dataclass
class ParsedQuery:
    brand_key: Optional[str] = None
    category_id: Optional[int] = None
    kind_label: Optional[str] = None
    model_tokens: list = field(default_factory=list)  # основне написання кожного слова моделі
    model_cands: list = field(default_factory=list)   # усі можливі написання латиницею (list[list[str]])
    display: list = field(default_factory=list)       # як слово показувати/шукати на сайті (невідомі слова - як написав клієнт)
    soft: list = field(default_factory=list)          # True - невпевнене слово (транслітерація), може не збігтися
    extras: list = field(default_factory=list)        # якість/колір: oled, black ...
    laptop: bool = False

    @property
    def brand_names(self) -> tuple:
        return BRAND_GROUPS.get(self.brand_key, ()) if self.brand_key else ()

    def prefix(self, k: int) -> "ParsedQuery":
        return ParsedQuery(self.brand_key, self.category_id, self.kind_label, self.model_tokens[:k],
                           self.model_cands[:k], self.display[:k], self.soft[:k], self.extras, self.laptop)

    def drop(self, index: int):
        for lst in (self.model_tokens, self.model_cands, self.display, self.soft):
            lst.pop(index)


def _lat_candidates(token: str) -> list:
    """Можливі латинські написання слова/коду, набраного кирилицею."""
    code_mode = bool(re.search(r"\d", token)) or len(token) <= 2
    base = dict(_PHON)
    alt = dict(_PHON)
    alt.update(_ALT)
    if code_mode:  # "р30" - це P30, "хр" - XR (або XP)
        base["р"] = "p"
        alt["р"] = "r"
    first = "".join(base.get(ch, ch) for ch in token)
    second = "".join(alt.get(ch, ch) for ch in token)
    return [first] if first == second else [first, second]


# літери-двійники: виглядають однаково в латиниці й кирилиці (клієнт міг набрати "рixеl" у змішаній розкладці)
_CYR_TO_LAT = {"а": "a", "в": "b", "е": "e", "є": "e", "і": "i", "ї": "i", "ј": "j", "к": "k", "м": "m",
               "н": "h", "о": "o", "р": "p", "с": "c", "т": "t", "у": "y", "х": "x", "ѕ": "s"}
_LAT_TO_CYR = {"a": "а", "b": "в", "c": "с", "e": "е", "h": "н", "i": "і", "k": "к", "m": "м", "o": "о",
               "p": "р", "t": "т", "x": "х", "y": "у"}
_CYR = re.compile(r"[а-яіїєґё]")
_LAT = re.compile(r"[a-z]")


def _fix_homoglyphs(tok: str) -> str:
    """
    Слово зі змішаних абеток -> одна абетка: "рixеl" -> "pixel", "aкб" -> "акб".
    Перемагає абетка, якої в слові більше; при рівності - латиниця (моделі пишуться латиницею).
    Біля цифр кирилична "з"/"о" - це цифри 3/0: "1з" -> "13".
    """
    if re.search(r"\d", tok):
        tok = re.sub(r"(?<=\d)[зЗ]|[зЗ](?=\d)", "3", tok)
        tok = re.sub(r"(?<=\d)[оo](?=\d)", "0", tok)
    cyr, lat = len(_CYR.findall(tok)), len(_LAT.findall(tok))
    if not cyr or not lat:
        return tok
    as_lat = "".join(_CYR_TO_LAT.get(ch, ch) for ch in tok)
    as_cyr = "".join(_LAT_TO_CYR.get(ch, ch) for ch in tok)
    as_lat = as_lat if not _CYR.search(as_lat) else None  # є кирилиця без двійника - латиницею не буде
    as_cyr = as_cyr if not _LAT.search(as_cyr) else None
    # спершу - варіант, що дає відоме слово ("ріхеl" -> pixel, "aкум" -> акум)
    known = set(KIND_WORDS) | set(BRAND_ALIASES) | set(FAMILY) | set(MODEL_WORDS) | set(QUALITY) | STOPWORDS
    for cand in (as_lat, as_cyr):
        if cand and (cand in known or re.search(r"\d", cand)):
            return cand
    for cand, vocab in ((as_lat, _BRAND_VOCAB), (as_cyr, list(KIND_WORDS) + _BRAND_VOCAB)):
        if cand and _fuzzy(cand, vocab):
            return cand
    if lat >= cyr:
        return as_lat or tok
    return as_cyr or tok


def _prepare_tokens(raw: str) -> list:
    q = raw.lower().replace("’", "'").replace("ʼ", "'").replace("`", "'")
    q = re.sub(r"(\d)\s*\+", r"\1 plus", q)                      # "s23+" -> "s23 plus"
    q = re.sub(r"([a-zа-яіїєґ])\+", r"\1 plus", q)               # "pro+" -> "pro plus"
    q = re.sub(r"\b(павер|повер|power)\s*-?\s*банк\b", "powerbank", q)
    q = re.sub(r"захисн\w*\s+(скл\w*|плівк\w*|пленк\w*|стекл\w*)", "захисне_скло", q)
    q = re.sub(r"\b\d+\s*(шт|штук\w*|pcs)\b", " ", q)            # "2 шт" - це кількість, а не модель
    q = re.sub(r"(\d)([а-яіїєґ]{2,})", r"\1 \2", q)              # "8про" -> "8 про"
    q = re.sub(r"\b([a-zа-яіїєґ])\s+(\d{1,3}[a-zа-яіїєґ]?)\b", r"\1\2", q)  # "с 23" -> "с23", "а 54" -> "а54"
    tokens = []
    for tok in re.findall(r"[\w'\-_]+", q):
        tokens += _split_glued(_fix_homoglyphs(tok)).split()
    return tokens


_SERIES = None


def _split_glued(tok: str) -> str:
    """
    Розклеює назви моделей без пробілів (з OCR чи набрані швидко): "note12pro" -> "note 12 pro",
    "pixel8pro" -> "pixel 8 pro", "iphone13" -> "iphone 13". Лише для відомих слів серій і уточнень,
    тож артикули (eb-bs912aby, bp4k, a52s) не чіпаються.
    """
    global _SERIES
    if _SERIES is None:
        _SERIES = ({latin for _, latin in FAMILY.values()} | {"note", "mate", "nova", "tab", "reno", "find", "nord",
                   "spark", "camon", "hot", "edge", "moto", "xperia", "galaxy", "honor", "realme", "narzo", "mi"})
    m = re.fullmatch(r"([a-z]{2,})(\d{1,3})((?:pro|plus|max|ultra|lite|mini|neo|fe|se|xl|prime|[45]g)*)", tok)
    if m and m.group(1) in _SERIES:
        tail = re.findall(r"pro|plus|max|ultra|lite|mini|neo|fe|se|xl|prime|[45]g", m.group(3))
        return " ".join([m.group(1), m.group(2)] + tail)
    m = re.fullmatch(r"(\d{1,3}[a-z]?)((?:pro|plus|max|ultra|lite|mini|neo|fe|xl|prime)+)", tok)
    if m:  # "12pro" -> "12 pro" (але "9a" лишається)
        return " ".join([m.group(1)] + re.findall(r"pro|plus|max|ultra|lite|mini|neo|fe|xl|prime", m.group(2)))
    return tok


def _words_to_numbers(tokens: list) -> list:
    out, i = [], 0
    while i < len(tokens):
        tok = tokens[i]
        nxt = tokens[i + 1] if i + 1 < len(tokens) else ""
        if tok in _TENS and nxt in _NUM and 1 <= _NUM[nxt] <= 9:
            out.append(str(_TENS[tok] + _NUM[nxt]))
            i += 2
            continue
        if tok in _TENS:
            out.append(str(_TENS[tok]))
        elif tok in _NUM and not (tok in ("сім", "семь") and nxt.startswith(("карт", "слот", "тримач"))):
            out.append(str(_NUM[tok]))
        else:
            out.append(tok)
        i += 1
    return out


def _fuzzy(token: str, vocabulary) -> Optional[str]:
    """Виправляє друкарські помилки й відмінки: "самсунга" -> "самсунг", "акамулятор" -> "акумулятор"."""
    if len(token) < 5 or re.search(r"\d", token):
        return None
    match = difflib.get_close_matches(token, list(vocabulary), n=1, cutoff=0.8)
    return match[0] if match else None


_BRAND_VOCAB = list(BRAND_ALIASES) + list(FAMILY)


_SAMSUNG_CODE = re.compile(r"sm-?([a-z]\d{3,4})([a-z]{0,2}\d{0,2})")
_SUFFIX_CODE = re.compile(r"([a-z]{1,2}\d{3,4})([a-z]{1,2}\d{1,2})")


def _model_code(tok: str):
    """
    Заводський код моделі -> (повний код, базовий код для каталогу).
    "sm-g990b2" -> ("g990b2", "g990"), "g990b2" -> ("g990b2", "g990"). Звичайні "a366b" не чіпаємо.
    """
    m = _SAMSUNG_CODE.fullmatch(tok) or _SUFFIX_CODE.fullmatch(tok)
    if not m:
        return None
    return m.group(1) + m.group(2), m.group(1)


def parse_query(raw: str) -> ParsedQuery:
    pq = ParsedQuery()
    known = set(KIND_WORDS) | set(BRAND_ALIASES) | set(FAMILY) | set(MODEL_WORDS) | set(QUALITY)
    for tok in _words_to_numbers(_prepare_tokens(raw)):
        if tok in STOPWORDS:
            continue
        if tok in LAPTOP_WORDS:
            pq.laptop = True
            continue
        if tok not in known:
            tok = _fuzzy(tok, KIND_WORDS) or _fuzzy(tok, _BRAND_VOCAB) or tok
        if tok in KIND_WORDS:
            if pq.category_id is None:
                pq.category_id, pq.kind_label = KIND_WORDS[tok]
            continue
        if tok in QUALITY:
            if QUALITY[tok] not in pq.extras:
                pq.extras.append(QUALITY[tok])
            continue
        if tok in BRAND_ALIASES:
            pq.brand_key = pq.brand_key or BRAND_ALIASES[tok]
            continue
        if tok in FAMILY:
            brand_key, latin = FAMILY[tok]
            pq.brand_key = pq.brand_key or brand_key
            pq.model_tokens.append(latin)
            pq.model_cands.append([latin])
            pq.display.append(latin)
            pq.soft.append(False)
            continue
        code = _model_code(tok)
        if code:  # SM-G990B2 / G990B2 / A525F -> у каталозі модель записана як G990, A525
            full, base = code
            pq.model_tokens.append(base)
            pq.model_cands.append(list(dict.fromkeys([base, full])))
            pq.display.append(base)
            pq.soft.append(False)
            continue
        if tok in MODEL_WORDS:
            cands, soft = [MODEL_WORDS[tok]], False
        elif re.search(r"[а-яіїєґ]", tok):
            cands = _lat_candidates(tok)
            soft = not (re.search(r"\d", tok) or len(tok) <= 2)  # слова без цифр - невпевнені
        else:
            cands, soft = [tok], False
        # код з літерою в кінці (a366b, s911b): у каталозі модель може бути записана без неї (A366)
        cands = cands + [c[:-1] for c in cands if re.fullmatch(r"[a-z]{1,2}\d{3,4}[a-z]", c)]
        pq.model_tokens.append(cands[0])
        pq.model_cands.append(cands)
        pq.display.append(tok if soft else cands[0])  # невідоме слово лишаємо українським - так його знайде сайт
        pq.soft.append(soft)
    if pq.laptop and pq.category_id in LAPTOP_CATEGORY:
        pq.category_id = LAPTOP_CATEGORY[pq.category_id]
    return pq


def model_line_from_text(text: str) -> str:
    """
    З тексту з фото (екран «Про телефон», коробка, наклейка) - рядок з назвою пристрою:
    бренд/серія + слово моделі з цифрою ("Redmi Note 12 Pro 5G", "Galaxy A52", "iPhone 13 Pro").
    Порожній рядок, якщо такого рядка нема (тоді бот шукає за кодами з фото).
    """
    best, best_score = "", 0
    lines = [ln.strip() for ln in re.split(r"[\r\n]+", text or "") if ln.strip()]
    for i, line in enumerate(lines):
        for cand in (line, f"{line} {lines[i + 1]}" if i + 1 < len(lines) else ""):  # "Redmi Note 12 Pro" / "5G"
            if not cand or len(cand) > 60:
                continue
            pq = parse_query(cand)
            family = any(c[0] in FAMILY_WORDS for c in pq.model_cands)
            hard = [t for t, soft in zip(pq.model_tokens, pq.soft) if not soft]
            if not (pq.brand_key or family) or not any(re.search(r"\d", t) for t in hard):
                continue
            model_like = set(MODEL_WORDS.values()) | FAMILY_WORDS | _NETWORK
            good = sum(1 for t in hard if re.search(r"\d", t) or t in model_like)
            other = len(pq.model_tokens) - good  # "my device", "model name" - сторонні слова екрана
            score = 2 * bool(pq.brand_key) + 2 * family + good - other
            if score > best_score:
                best, best_score = cand, score
    return best


def build_site_query(raw_query: str) -> str:
    """Запит для пошуку на сайті: "акб піксель 9 про" -> "google pixel 9 pro акумулятор"."""
    return _site_text(parse_query(raw_query), raw_query)


def _site_text(pq: ParsedQuery, raw_query: str) -> str:
    model_text = " ".join(d for d, soft in zip(pq.display, pq.soft) if not soft)
    part = pq.kind_label or " ".join(d for d, soft in zip(pq.display, pq.soft) if soft)  # "рамка", "нижня плата"
    # бренд не додаємо: у назвах товарів його часто нема ("Рамка дисплея iphone XS"), а сайт шукає за всіма словами
    brand = pq.brand_key if not model_text else ""
    parts = [part, brand, model_text] + pq.extras
    return " ".join(p for p in parts if p) or raw_query.strip()


# ==========================================
# 3. ПОШУК БРЕНДУ, МОДЕЛІ ТА ТОВАРІВ
# ==========================================

DEBUG = os.getenv("TPLUS_DEBUG", "1") == "1"  # TPLUS_DEBUG=0 вимикає журнал запитів у консолі


def _log(message: str):
    if DEBUG:
        print(f"[TPlus] {message}", flush=True)


async def _call(name: str, args: dict) -> dict:
    data = await _client.call_tool(name, args)
    items = data.get("items")
    shown = len(items) if isinstance(items, list) else f"немає items, ключі: {list(data.keys())}"
    _log(f"{name} {json.dumps(args, ensure_ascii=False)} -> {shown}")
    return data


_NETWORK = {"4g", "5g", "lte", "wifi", "wi", "fi", "nfc", "dual", "sim", "version"}
_CODE = re.compile(r"^[a-z]{1,2}\d{3,}[a-z]{0,2}$")  # s911, s911b, a546 - внутрішні коди, не відрізняють модель


def _name_parts(name: str):
    """Слова з назви моделі + їх частини (magic7 -> magic, 7). '+' читається як plus."""
    base = re.findall(r"[a-z0-9]+", name.lower().replace("+", " plus "))
    parts = set(base)
    for tok in base:
        parts.update(re.findall(r"[a-z]+|\d+", tok))
    return base, parts


def _skeleton(word: str) -> str:
    """Слово без різниці в голосних: eir / air / ayr -> "ar" (для слів, набраних кирилицею)."""
    return re.sub(r"[aeiouy]+", "a", word)


def _word_in(word: str, parts) -> Optional[str]:
    """
    Слово запиту є в назві моделі? Повертає слово з назви, з яким збіглося.
    Заводський код із суфіксом регіону/пам'яті збігається з базовим кодом моделі
    для будь-якого бренду: a525f -> a525, g990b2 -> g990, m1906g7g -> m1906.
    """
    if word in parts:
        return word
    if not re.search(r"\d", word):
        return None
    for p in parts:
        if (len(p) >= 3 and len(word) > len(p) and word.startswith(p) and p[-1].isdigit()
                and re.search(r"[a-z]", p) and word[len(p)].isalpha()):
            return p
    return None


def _model_vocab(models: list) -> set:
    vocab = set()
    for m in models:
        vocab |= _name_parts(m.get("name", ""))[1]
    return vocab


def drop_noise(models: list, pq: ParsedQuery):
    """
    Загальне правило замість окремих заплаток: якщо бренд відомий, а впевнене слово запиту
    не трапляється в ЖОДНІЙ моделі цього бренду (SM-G990B2/DS, A2638, 2021, global...), воно
    не може описувати модель - це заводський код, рік, регіон тощо. Такі слова прибираємо,
    щоб вони не блокували збіг. Повертає (новий запит, прибрані слова) або (None, []).
    """
    vocab = _model_vocab(models)
    if not vocab:
        return None, []
    keep, noise = [], []
    for i, (cands, soft) in enumerate(zip(pq.model_cands, pq.soft)):
        known = soft or cands[0] in FAMILY_WORDS or any(
            all(_word_in(w, vocab) for w in re.findall(r"[a-z0-9]+", c)) for c in cands)
        (keep if known else noise).append(i)
    if not noise or not any(not pq.soft[i] and pq.model_cands[i] != ["galaxy"] for i in keep):
        return None, []
    clean = ParsedQuery(pq.brand_key, pq.category_id, pq.kind_label,
                        [pq.model_tokens[i] for i in keep], [pq.model_cands[i] for i in keep],
                        [pq.display[i] for i in keep], [pq.soft[i] for i in keep], pq.extras, pq.laptop)
    return clean, [pq.display[i] for i in noise]


def split_models(models: list, pq: ParsedQuery):
    """
    Порівнює слова запиту з назвами моделей.
    Повертає (точні, схожі): точні - немає зайвих слів (Pixel 9 Pro для "pixel 9 pro"),
    схожі - є зайві (Pixel 9 Pro XL, Pixel 9 Pro Fold) - відсортовані за кількістю зайвих слів.
    """
    if not pq.model_tokens:
        return [], []

    # словник слів усіх моделей - щоб виправити помилки ("свіфт" -> swift)
    vocab = set()
    parsed = []
    for m in models:
        base, parts = _name_parts(m.get("name", ""))
        parsed.append((m, base, parts))
        vocab |= parts
    vocab_list = sorted(vocab)

    expanded = []
    for cands in pq.model_cands:
        ext = list(cands)
        if not any(re.sub(r"[^a-z0-9]", "", c) in vocab for c in cands):
            for c in cands:
                if len(c) >= 4 and c.isalpha():
                    ext += difflib.get_close_matches(c, vocab_list, n=1, cutoff=0.8)
                if len(c) >= 3 and c.isalpha():  # транслітерація плутає голосні: "еір" -> eir ~ air
                    ext += [w for w in vocab_list if len(w) >= 3 and w.isalpha() and w[-1] == c[-1]
                            and _skeleton(w) == _skeleton(c)]
        expanded.append(ext)

    exact, related = [], []
    for m, base, parts in parsed:
        consumed, ok = set(), True
        for cands, soft in zip(expanded, pq.soft):
            if cands == ["galaxy"]:
                continue
            hit = None
            for c in cands:
                words = re.findall(r"[a-z0-9]+", c)
                matched = [_word_in(w, parts) for w in words]
                if words and all(matched):
                    hit = set(matched)
                    break
            if hit:
                consumed |= hit
            elif not soft:  # впевнене слово обов'язкове; невпевнене (транслітерація) можна проігнорувати
                ok = False
                break
        if not ok or not consumed:
            continue
        extras = []
        for tok in base:
            sub = re.findall(r"[a-z]+|\d+", tok)
            if tok in consumed or all(s in consumed for s in sub):
                continue
            if tok in OPTIONAL_WORDS or tok in _NETWORK or _CODE.match(tok):
                continue
            extras.append(tok)
        (exact if not extras else related).append((len(extras), m))

    exact_models = [m for _, m in sorted(exact, key=lambda x: x[1].get("name", ""))]
    related_models = [m for _, m in sorted(related, key=lambda x: (x[0], x[1].get("name", "")))]
    related_counts = sorted(n for n, _ in related)
    return exact_models, list(zip(related_counts, related_models))


def _free_words(chosen: list, pq: ParsedQuery) -> list:
    """Невпевнені слова запиту, яких нема в назвах знайдених моделей ("нижня", "плата") - опис деталі."""
    vocab = set()
    for m in chosen:
        vocab |= _name_parts(m.get("name", ""))[1]
    vocab_list = sorted(vocab)
    free = []
    for shown, cands, soft in zip(pq.display, pq.model_cands, pq.soft):
        if not soft:
            continue
        ext = list(cands)
        for c in cands:
            if len(c) >= 4 and c.isalpha():
                ext += difflib.get_close_matches(c, vocab_list, n=1, cutoff=0.8)
        if not any(w in vocab for c in ext for w in re.findall(r"[a-z0-9]+", c)):
            free.append(shown)
    return free


def _nearest(related: list) -> list:
    """Найближчі схожі моделі - з мінімальною кількістю зайвих слів."""
    if not related:
        return []
    best = related[0][0]
    return [m for n, m in related if n == best]


async def _brand_catalog() -> list:
    data = await _cached("brands", lambda: _call("list_brands", {}))
    return data.get("items", [])


def _pick_brand_ids(brands: list, names: tuple) -> list:
    ids = []
    for wanted in names:
        for b in brands:
            low = b["name"].lower()
            if (low == wanted or low.startswith(wanted)) and b["id"] not in ids:
                ids.append(b["id"])
                break
    return ids


async def _load_models(brand_ids: list, pq: ParsedQuery) -> list:
    if brand_ids:  # повний список моделей бренду (кешується): далі фільтруємо самі, з урахуванням помилок
        models = []
        for bid in brand_ids:
            data = await _cached(("brand_models", bid), lambda b=bid: _call("list_models", {"brandIds": [b]}))
            models += data.get("items", [])
        return models
    # бренд невідомий: просимо сервер знайти за найконкретнішим словом (з цифрою, інакше найдовшим)
    hard = [c for c, soft in zip(pq.model_cands, pq.soft) if not soft]
    if not hard:
        return []
    chosen = next((c for c in hard if re.search(r"\d", c[0])), max(hard, key=lambda c: len(c[0])))
    for token in chosen:  # усі написання слова: a366b, потім a366
        data = await _cached(("models", token), lambda t=token: _call("list_models", {"search": t}))
        if data.get("items"):
            return data["items"]
    return []


# ---------- категорії з дерева сайту ("Нижні плати", "Сканери відбитків пальців" ...) ----------

def _flatten_categories(data) -> list:
    """Плоский список категорій з будь-якої форми відповіді list_categories (плоский список або дерево)."""
    raw = data.get("items") if isinstance(data, dict) else data
    if raw is None and isinstance(data, dict):
        raw = data.get("categories") or data.get("data") or []
    out = []

    def walk(nodes, parent):
        for node in nodes or []:
            if not isinstance(node, dict):
                continue
            cid, name = node.get("id"), node.get("name") or node.get("title")
            pid = node.get("parentId", node.get("parent_id"))
            if pid is None and isinstance(node.get("parent"), dict):
                pid = node["parent"].get("id")
            elif pid is None and isinstance(node.get("parent"), (int, str)):
                pid = node["parent"]
            if pid is None:
                pid = parent
            if cid is not None and name:
                out.append({"id": cid, "name": str(name), "parent_id": pid})
            walk(node.get("children") or node.get("subcategories"), cid)

    walk(raw, None)
    return out


async def _category_index() -> list:
    try:
        data = await _cached("categories", lambda: _call("list_categories", {}))
    except MCPAuthError:
        raise
    except Exception as e:
        _log(f"list_categories недоступний: {e!r}")
        return []
    cats = _flatten_categories(data)
    if not cats:
        _log(f"list_categories: не вдалося розібрати відповідь (ключі: {list(data.keys()) if isinstance(data, dict) else type(data)})")
    return cats


_CAT_STOP = {"для", "та", "інше", "інші", "інший", "решта", "all"}


def _stem(word: str) -> str:
    word = re.sub(r"[^\w]", "", word.lower().replace("'", ""))
    stem = re.sub(r"[аеєиіїоуюяьй]+$", "", word)
    return stem if len(stem) >= 3 else word


def _stem_match(a: str, b: str) -> bool:
    return a == b or (min(len(a), len(b)) >= 5 and (a.startswith(b) or b.startswith(a)))


async def _category_for_words(words: list, prefer_root: Optional[int]):
    """
    "нижня плата" -> категорія "Нижні плати". Категорія підходить, якщо КОЖНЕ слово її назви
    є серед слів клієнта. Повертає (категорія або None, слова, що лишилися).
    """
    cats = await _category_index()
    if not cats:
        return None, words
    parent = {c["id"]: c["parent_id"] for c in cats}

    def depth(cid):
        d, seen = 0, set()
        while parent.get(cid) is not None and cid not in seen:
            seen.add(cid)
            cid = parent[cid]
            d += 1
        return d

    def under(cid, root):
        seen = set()
        while cid is not None and cid not in seen:
            if cid == root:
                return True
            seen.add(cid)
            cid = parent.get(cid)
        return False

    wanted = {w: _stem(w) for w in words}
    best, best_key = None, None
    for c in cats:
        cstems = [_stem(w) for w in re.findall(r"[\w']+", c["name"].lower()) if w not in _CAT_STOP]
        cstems = [x for x in cstems if len(x) >= 3]
        if not cstems:
            continue
        if not all(any(_stem_match(cs, ws) for ws in wanted.values()) for cs in cstems):
            continue
        consumed = {w for w, ws in wanted.items() if any(_stem_match(cs, ws) for cs in cstems)}
        key = (len(consumed), len(cstems), prefer_root is not None and under(c["id"], prefer_root),
               -depth(c["id"]))
        if best_key is None or key > best_key:
            best, best_key = (c, consumed), key
    if not best:
        return None, words
    cat, consumed = best
    return cat, [w for w in words if w not in consumed]


@dataclass
class SearchOutcome:
    items: list                                   # товари, знайдені для моделі з запиту
    total: int
    models: list                                  # назви моделей, що збіглися точно
    site_query: str
    related: list = field(default_factory=list)   # [(назва схожої моделі, [товари])]
    fuzzy: bool = False                           # знайдено лише за збігом тексту
    requested: str = ""                           # що саме шукав клієнт
    too_broad: bool = False                       # запит збігся з надто великою кількістю моделей
    laptop: bool = False                          # запит про ноутбук (потрібен точний артикул АКБ)
    suggestions: list = field(default_factory=list)  # моделі на вибір
    suggest_reason: str = ""                      # ambiguous / missing / brand
    need_part: bool = False                       # модель знайдено, але не сказано, що саме потрібно
    extras_ignored: list = field(default_factory=list)  # слова (oled, black), яких у назвах не знайшлося
    kind: str = ""                                # що шукали (акумулятор, дисплей ...)
    brand: str = ""                               # назва бренду з каталогу (Samsung)
    part: str = ""                                # деталь словами клієнта (дисплей, рамка, нижня плата)
    part_codes: list = field(default_factory=list)  # артикули деталі, за якими знайдено (EB-BG980ABY)
    factory_code: str = ""                        # заводський код моделі від клієнта (CPH2579)
    client_model: str = ""                        # модель клієнта для показу: "OPPO A38 (CPH2579)"
    device_codes: list = field(default_factory=list)  # ідентифікатори пристрою, за якими знайдено (A2588)


def _rank(item: dict) -> float:
    products = item.get("products") or [{}]
    return max((p.get("rank") or 0) for p in products)


def _only_model(items: list, chosen: list, similar: list) -> list:
    """
    Після текстового пошуку лишає товари саме для цієї моделі: у назві мають бути слова моделі,
    і не має бути слів, якими від неї відрізняються схожі (для "iPhone XS" відкидаємо "XS Max").
    """
    own = set().union(*(_name_parts(m["name"])[1] for m in chosen)) if chosen else set()
    need = set(_name_parts(chosen[0]["name"])[0]) - _NETWORK - OPTIONAL_WORDS if chosen else set()
    need = {w for w in need if not _CODE.match(w)}  # код (G990) у назвах товарів буває не завжди
    extra = set()
    for m in similar:
        extra |= set(_name_parts(m["name"])[0]) - own
    out = []
    for item in items:
        words = set(re.findall(r"[a-z0-9]+", (item.get("name") or "").lower().replace("+", " plus ")))
        if need <= words and not (words & extra):
            out.append(item)
    return out


def _drop_weak(items: list) -> list:
    """Лишає найрелевантніші позиції: слабкі збіги (rank < 10% від найкращого) відкидаються."""
    if not items:
        return items
    best = max(_rank(i) for i in items)
    if best < 1:
        return items
    return sorted((i for i in items if _rank(i) >= best * 0.1), key=_rank, reverse=True)


_QUALITY_WORDS = {"oem", "service", "original", "refurbished", "used", "incell", "oled", "amoled", "tft", "ips",
                  "hard", "soft", "copy", "high", "max", "bat", "redline", "galaxyline", "tota", "flycdi", "ver",
                  "version", "black", "white", "dual", "sim", "holder", "big", "small", "size", "lcd", "plus"}


def _articles_in(name: str, skip: set) -> list:
    """
    Артикули деталі в назві товару: "Акумулятор для Xiaomi BP4K (OEM)" -> ["BP4K"],
    "Акумулятор для Google G020I-B (OEM)" -> ["G020I-B"]. Слова моделі, якість і колір пропускаємо.
    """
    out = []
    for tok in re.findall(r"[A-Za-z0-9][A-Za-z0-9-]*[A-Za-z0-9]", name):
        low = tok.lower()
        if low in skip or low in _QUALITY_WORDS or low in _NETWORK or len(tok) < 3:
            continue
        if not (re.search(r"[A-Za-z]", tok) and re.search(r"\d", tok)):
            continue
        if re.fullmatch(r"\d+(mah|gb|tb|mm|w|v|hz)", low):
            continue
        out.append(tok)
    return out


def _looks_like_code(tokens: list) -> bool:
    return any(re.search(r"[a-z]", t) and re.search(r"\d", t) and len(t) >= 5 and not re.fullmatch(r"\d+[a-z]", t)
               for t in tokens)


def _names_in(data: dict) -> set:
    """Назви всіх товарів з відповіді search_products (будь-якої форми: items/products, групи з products)."""
    names = set()
    for item in (data.get("items") or []) + (data.get("products") or []):
        if not isinstance(item, dict):
            continue
        if item.get("name"):
            names.add(item["name"].lower())
        for p in item.get("products") or []:
            if isinstance(p, dict) and p.get("name"):
                names.add(p["name"].lower())
    return names


def _item_name(item: dict) -> str:
    return item.get("name") or ((item.get("products") or [{}])[0].get("name") or "")


async def _site_finds(query: str, expect=()) -> bool:
    """
    Чи покаже сайт за цим запитом те, що знайшов бот (пошук на сайті = search_products без фільтрів).
    expect - назви знайдених ботом товарів: достатньо, щоб на сайті з'явився хоча б один із них.
    """
    async def check():
        data = await _call("search_products", {"query": query, "limit": 100})
        total = data.get("total") or (data.get("meta") or {}).get("totalItems") or 0
        return {"names": sorted(_names_in(data)), "total": total}
    try:
        res = await _cached(("site", query.lower()), check)
    except MCPAuthError:
        raise
    except Exception as e:
        _log(f"перевірка запиту для сайту не вдалася: {e!r}")
        return False
    if not expect:
        return bool(res["names"]) or res["total"] > 0
    return bool({e.lower() for e in expect} & set(res["names"]))


# ---------- сумісність: список «Моделі» в картці товару ----------

async def _product_models(pid) -> Optional[list]:
    """Список сумісних моделей з картки товару ("OPPO A38 (CPH2579)", ...); None - не вдалося отримати."""
    if not isinstance(pid, int) or pid <= 0:
        return None

    async def fetch():
        data = await _client.call_tool("get_product", {"id": pid})
        models = data.get("models") if isinstance(data, dict) else None
        return {"models": [str(m) for m in models] if isinstance(models, list) else None}
    try:
        return (await _cached(("pmodels", pid), fetch))["models"]
    except MCPAuthError:
        raise
    except Exception as e:
        _log(f"get_product {pid} недоступний: {e!r}")
        return None


def _item_pid(item: dict):
    products = item.get("products")
    if isinstance(products, list) and products and isinstance(products[0], dict):
        return products[0].get("id")
    return item.get("id")


def _model_key(text: str, brand_words: set) -> frozenset:
    """
    Слова, що визначають модель: без бренду, без заводських кодів у дужках, без "galaxy" і внутрішніх кодів.
    "Samsung G980 Galaxy S20 (SM-G980F)" і "G980 Galaxy S20" -> {s20}; "OPPO A38 (CPH2579)" -> {a38}.
    """
    # дужки з кодами ("(CPH2579)", "(SM-G980, SM-G980F)", "(22101316C, 22101316I)") прибираємо,
    # а короткі уточнення моделі лишаємо: "Redmi Note 12 Pro (5G)" -> ... pro 5g
    def paren(m):
        inner = m.group(1)
        return " " if ("," in inner or re.search(r"\d{4}", inner) or _CODE.match(inner.strip())) else f" {inner} "
    core = re.sub(r"\(([^)]*)\)", paren, text.lower()).replace("+", " plus ")
    core = re.sub(r"\b\d{1,2}[.,]\d\b", " ", core)  # діагональ "10.9", "6.7" - не частина назви моделі
    words = [w for w in re.findall(r"[a-z0-9]+", core) if w not in brand_words]
    key = {w for w in words if w not in OPTIONAL_WORDS and not _CODE.match(w)}
    return frozenset(key or words)


def _entry_fits(entry: str, targets: list, codes: list, brand_words: set) -> bool:
    low = entry.lower()
    if any(c and re.search(rf"(?<![a-z0-9]){re.escape(c)}(?![a-z0-9])", low) for c in codes):
        return True  # ідентифікатор пристрою: "OPPO A38 (CPH2579)", "A2588"
    key = _model_key(entry, brand_words)
    # точний збіг, або в картці ще й номер покоління: "iPad Air 2022" ~ "iPad Air 5 2022"
    # (зайві слова - лише числа; "Pixel 4 XL", "S20 5G", "iPhone 13 Pro" - інші моделі)
    return any(key == t or (t and t < key and all(w.isdigit() for w in key - t)) for t in targets)


async def _compat_filter(items: list, chosen: list, brand_words: set, codes: list, similar: list) -> list:
    """
    Лишає товари, що підходять саме до моделі клієнта.
    Назва містить модель (і не містить слів схожих моделей) - підходить одразу;
    інакше дивимось список «Моделі» в картці товару (дисплей A17 підходить і для A38).
    """
    by_name = _only_model(items, chosen, similar)
    targets = [_model_key(m["name"], brand_words) for m in chosen]
    rest = [i for i in items if i not in by_name][:24]
    sem = asyncio.Semaphore(6)

    async def models_of(item):
        async with sem:
            return await _product_models(_item_pid(item))
    lists = await asyncio.gather(*(models_of(i) for i in rest))
    fits = [i for i, models in zip(rest, lists)
            if models and any(_entry_fits(e, targets, codes, brand_words) for e in models)]
    return by_name + fits


async def _model_from_products(items: list, code: str) -> str:
    """Заводський код -> модель за карткою товару: "cph2579" -> "OPPO A38" (з "OPPO A38 (CPH2579)")."""
    votes = {}
    for item in items[:6]:
        for entry in await _product_models(_item_pid(item)) or []:
            if code.lower() in entry.lower():
                name = re.sub(r"\s*\([^)]*\)", "", entry).strip()
                if name:
                    votes[name] = votes.get(name, 0) + 1
    return max(votes, key=votes.get) if votes else ""


async def _pick_site_query(outcome: SearchOutcome) -> str:
    """
    Запит для кнопки «Відкрити на сайті», який точно щось знаходить.
    Бот шукає з фільтрами (модель, категорія), а сайт - лише за текстом, тому
    вихідний запит клієнта ("... sm-g990b2 дисплей") на сайті може дати 0 результатів.
    """
    if outcome.need_part or outcome.suggestions or outcome.too_broad:
        return outcome.site_query  # клієнту ще треба уточнити запит - перевіряти нічого
    first_items = outcome.items or [i for _, group in outcome.related for i in group]
    expect = [_item_name(i) for i in first_items[:5] if _item_name(i)]
    part = outcome.part or (outcome.kind if outcome.kind else "")
    model = outcome.models[0] if outcome.models else (outcome.related[0][0] if outcome.related else "")
    brand = outcome.brand if outcome.brand and model and outcome.brand.lower() not in model.lower() else ""
    candidates = list(outcome.part_codes[:1])  # артикул АКБ (EB-BG980ABY) - найточніший запит для сайту
    if model:
        # від вужчого до ширшого: "дисплей Samsung G990 Galaxy S21 FE 5G" -> "OPPO A38" (усі запчастини моделі:
        # сайт шукає і по назвах, і по списку сумісних моделей у картках)
        candidates += [f"{part} {brand} {model}", f"{part} {model}"]
        candidates += outcome.device_codes[:1]  # A2588 - на сайті покаже всі запчастини саме цього пристрою
        candidates += [f"{brand} {model}", model]
    candidates.append(outcome.site_query)
    top_name = _item_name(first_items[0]) if first_items else ""
    if top_name:
        # деякі товари названі без моделі телефону ("Акумулятор для Xiaomi BM4J (Max Bat RedLine)") -
        # назва без уточнень у дужках покаже на сайті всі варіанти цієї деталі
        candidates.append(re.sub(r"\s*\([^)]*\)", "", top_name))
    for query in dict.fromkeys(re.sub(r"\s+", " ", c).strip() for c in candidates if c and c.strip()):
        if await _site_finds(query, expect):  # сайт має показати хоча б один товар, який знайшов бот
            return query
    if top_name:  # повна назва знайденого товару - на сайті вона є завжди
        return top_name
    return candidates[0].strip() if candidates else outcome.site_query


async def search_catalog(raw_query: str) -> SearchOutcome:
    outcome = await _search_catalog(raw_query)
    outcome.site_query = await _pick_site_query(outcome)
    return outcome


async def _search_catalog(raw_query: str, depth: int = 0, factory_code: str = "") -> SearchOutcome:
    pq = parse_query(raw_query)
    site_query = build_site_query(raw_query)
    brands = await _brand_catalog()

    # латинське слово збігається з брендом у каталозі ("tecno spark 10") - це бренд, а не модель
    if not pq.brand_key:
        for i, cands in enumerate(pq.model_cands):
            hit = next((b for b in brands if len(cands[0]) >= 3 and b["name"].lower() == cands[0]), None)
            if hit and i < len(pq.model_tokens):
                pq.brand_key = hit["name"].lower()
                BRAND_GROUPS.setdefault(pq.brand_key, (pq.brand_key,))
                pq.drop(i)
                break

    brand_ids = _pick_brand_ids(brands, pq.brand_names)
    brand_label = next((b["name"] for b in brands if b["id"] in brand_ids), "")
    models = await _load_models(brand_ids, pq) if pq.model_tokens else []
    exact, related = split_models(models, pq)

    # модель не впізнано - можливо, заважають слова, яких нема в жодній моделі бренду (коди, рік, регіон)
    noise = []
    if not exact and brand_ids and models:
        clean, dropped = drop_noise(models, pq)
        if clean is not None:
            ex2, rel2 = split_models(models, clean)
            # прибирання шуму має звужувати до конкретної моделі, а не розширювати запит ("pixel 4" -> "4")
            if 1 <= len(ex2) <= 3 or (not ex2 and rel2 and not related and len(_nearest(rel2)) <= 3):
                _log(f"ігнорую слова, яких нема в моделях {brand_label}: {dropped}")
                pq, exact, related, noise = clean, ex2, rel2, dropped
                site_query = _site_text(pq, raw_query)

    kind = pq.kind_label or ""
    cat = {"categoryId": pq.category_id} if pq.category_id else {}
    model_text = " ".join(pq.display)

    def make(items=(), total=0, **kw):
        return SearchOutcome(list(items), total, kw.pop("models", []), site_query, laptop=pq.laptop,
                             kind=kw.pop("kind", kind), requested=kw.pop("requested", model_text),
                             brand=brand_label, factory_code=factory_code, **kw)

    def brand_words(chosen: list) -> set:
        words = set()
        for name in [brand_label, *pq.brand_names] + [(m.get("brand") or {}).get("name", "") for m in chosen]:
            words |= set(re.findall(r"[a-z0-9]+", (name or "").lower()))
        return words

    async def by_device_codes(chosen: list, cat_: dict, similar: list):
        """
        Ідентифікатори пристрою клієнта (A-номери Apple, SM-... Samsung, CPH... OPPO): заводський код від клієнта
        + знайдені в інтернеті для моделі (кешуються). Шукаємо кожен у категорії деталі й лишаємо товари,
        у картці яких цей код є серед сумісних. Повертає (коди, що спрацювали, товари).
        """
        brand = brand_label or (chosen[0].get("brand") or {}).get("name", "")
        codes = [factory_code.upper()] if factory_code else []
        try:  # інтернет - лише підказка: будь-яка помилка тут не має ламати пошук
            codes += await part_lookup.web_device_codes(brand, chosen[0]["name"])
        except Exception as e:
            _log(f"ідентифікатори {brand} {chosen[0]['name']} з інтернету недоступні: {e!r}")
        codes = list(dict.fromkeys(codes))[:6]
        found, worked = [], []
        for code in codes:
            items, _ = await run({**cat_, "query": code})
            new = [i for i in items if _item_name(i) not in {_item_name(f) for f in found}]
            fits = await _compat_filter(new, chosen, brand_words(chosen), [code.lower()], similar)
            if fits:
                worked.append(code)
                found += fits
        if worked:
            _log(f"знайдено за ідентифікаторами {worked}: {len(found)}")
        return worked, found

    async def compat(chosen: list, cat_: dict, similar: list) -> list:
        """
        Товари, сумісні з моделлю клієнта, як їх шукає сайт: "бренд + модель" у категорії деталі.
        Сайт шукає і в назвах, і в списку «Моделі» картки, тому знаходить, напр., дисплей A17 для OPPO A38.
        Кожен товар з іншою моделлю в назві звіряємо з карткою (_compat_filter).
        """
        brand = brand_label or (chosen[0].get("brand") or {}).get("name", "")
        queries = []
        for m in chosen[:3]:
            queries += [f"{brand} {m['name']}".strip(), m["name"]]
        if factory_code:
            queries.append(factory_code.upper())
        found = []
        for q in dict.fromkeys(queries):
            items, _ = await run({**cat_, "query": q})
            found += [i for i in items if _item_name(i) not in {_item_name(f) for f in found}]
            if len(found) >= 40:
                break
        codes = [factory_code.lower()] if factory_code else []
        return await _compat_filter(found, chosen, brand_words(chosen), codes, similar)

    def text_query(with_extras: bool, free: list = ()) -> str:
        return " ".join(p for p in [kind] + list(free) + (pq.extras if with_extras else []) if p)

    fallback_word = next((d for d, soft in zip(pq.display, pq.soft) if not soft), "")

    async def run(args: dict):
        args = {k: v for k, v in args.items() if not (k == "query" and not v)}  # порожній query не передаємо
        try:
            data = await _call("search_products", args)
        except MCPAuthError:
            raise
        except MCPError:
            if "query" in args or not fallback_word:  # сервер вимагає query - підставляємо слово моделі
                raise
            data = await _call("search_products", {**args, "query": fallback_word})
        items = _drop_weak(data.get("items", []))
        return items, len(items)

    async def by_code(cat_: dict, code: str) -> list:
        """Товари категорії, у назві яких є цей код (EB-BG980ABY, або G980 всередині EB-BG980ABY)."""
        items, _ = await run({**cat_, "query": code.upper()})
        low = code.lower()
        return [i for i in items if low in (i.get("name") or "").lower()]

    async def by_part_names(chosen: list, cat_: dict, part: str, seed: list, dev_codes: list, similar: list):
        """
        Як може називатися потрібна позиція в каталозі. Деталі часто названі власним артикулом, а не моделлю
        телефону ("Акумулятор для Xiaomi BP4K", "Акумулятор для Samsung EB-BG980ABY"), тому шукаємо за артикулами:
          1) з назв уже знайдених сумісних товарів (BP4K -> усі варіанти BP4K: OEM, Service, Max Bat);
          2) код моделі всередині артикулу (G980 -> EB-BG980ABY);
          3) з інтернету: "<бренд> <модель> <деталь англійською> part number" (кешується в part_codes.json).
        Кожен знайдений товар перевіряємо за карткою (сумісні моделі), тож чужий артикул з інтернету
        (BN5E від Note 12 Pro 4G для Note 12 Pro 5G) у відповідь не потрапить.
        Повертає (артикули, що спрацювали, товари).
        """
        brand = brand_label or (chosen[0].get("brand") or {}).get("name", "")
        model = chosen[0]["name"]
        # слова всіх моделей бренду ("a17", "a18") - це не артикули деталі
        model_words = _model_vocab(models) | set().union(*(_name_parts(m["name"])[1] for m in chosen)) | brand_words(chosen)
        model_codes = list(dict.fromkeys(w for m in chosen for w in _name_parts(m["name"])[0] if _CODE.match(w)))
        dev = {c.lower() for c in dev_codes}

        sources = []  # (артикул, звідки)
        for item in seed:
            sources += [(a, "catalog") for a in _articles_in(_item_name(item), model_words | dev)]
        sources += [(c, "model-code") for c in model_codes]
        web = part_lookup.cached_codes(brand, model, part)
        fresh = web is None
        if fresh:
            exclude = [c.upper() for c in model_codes + [f"SM-{c}" for c in model_codes]] + [c.upper() for c in dev]
            try:  # інтернет - лише підказка: будь-яка помилка тут не має ламати пошук
                web = await part_lookup.web_codes(brand, model, part, exclude=exclude)
            except Exception as e:
                _log(f"пошук артикулу «{part}» в інтернеті не вдався: {e!r}")
                web, fresh = [], False
        sources += [(c, "web") for c in web[:6]]

        found, worked, tried = [], [], set()
        names = {_item_name(i) for i in seed}
        for code, origin in sources:
            if code.lower() in tried or len(tried) >= 8:
                continue
            tried.add(code.lower())
            items = [i for i in await by_code(cat_, code) if _item_name(i) not in names]
            if not items:
                continue
            # кожен товар - лише з підтвердженням картки; виняток: код моделі всередині артикулу
            # (G980 -> EB-BG980ABY), коли нічого іншого не знайдено, а картка без списку моделей
            checked = await _compat_filter(items, chosen, brand_words(chosen), list(dev), similar)
            no_card = [i for i in items if i not in checked and not await _product_models(_item_pid(i))]
            if origin == "catalog":
                # той самий артикул, що й у перевіреного товару ("BP4K (Service)" без списку моделей) - підходить
                checked += no_card
            elif not checked and origin == "model-code" and not seed and not found:
                checked = no_card
            items = checked
            if items:
                worked.append(code.upper() if origin != "catalog" else code)
                names |= {_item_name(i) for i in items}
                found += items
        if fresh:
            part_lookup.remember(brand, model, part, [c for c, o in sources if o == "web" and c in worked])
        if worked:
            _log(f"назви позиції «{part}»: {worked} -> +{len(found)}")
        return worked, found

    async def search_models(chosen: list, near_related: list, free: list):
        names = [m["name"] for m in chosen]
        ids = [m["id"] for m in chosen]
        cat_, label, free_ = cat, kind, list(free)
        part = " ".join(filter(None, [kind] + list(free)))  # деталь словами клієнта: "дисплей", "рамка"
        if not cat_ and free_:  # "нижня плата" - це категорія сайту "Нижні плати": вибираємо її фільтром, як на сайті
            hit, free_ = await _category_for_words(free_, None if pq.laptop else 8)
            if hit:
                cat_, label = {"categoryId": hit["id"]}, hit["name"]
        base = {**cat_, "modelIds": ids}
        ignored = []
        items, total = await run({**base, "query": text_query(True, free_)})
        if not items and pq.extras:
            items, total = await run({**base, "query": text_query(False, free_)})
            ignored = list(pq.extras)
        if not items and free_ and cat_:  # невідоме слово ("круто") не знайшлося - показуємо за категорією
            items, total = await run({**base, "query": text_query(False)})
            ignored = list(pq.extras) + list(free_)
        if not items and cat_ is not cat:
            # категорію вгадано за словом клієнта ("рамка" -> "Рамки"), але товар лежить в іншій гілці
            # ("Рамки дисплея iPhone") - шукаємо слово по товарах моделі без категорії
            items, total = await run({"modelIds": ids, "query": " ".join(free)})
        dev_codes = []
        if cat_ and not (pq.extras and ignored == []  and items):
            # фільтр за моделлю бачить не всі сумісні товари - додаємо те, що сайт знаходить за моделлю
            seen = {_item_name(i) for i in items}
            extra = [i for i in await compat(chosen, cat_, [m for _, m in near_related]) if _item_name(i) not in seen]
            # і за ідентифікаторами пристрою (A2588, SM-A525F, CPH2579): у картках модель часто записана саме так
            dev_codes, by_dev = await by_device_codes(chosen, cat_, [m for _, m in near_related])
            seen |= {_item_name(i) for i in extra}
            extra += [i for i in by_dev if _item_name(i) not in seen]
            if extra:
                _log(f"сумісні за карткою товару: +{len(extra)}")
                items = list(items) + extra
                total = len(items)
        part_codes = []
        if cat_ and not (pq.extras and ignored == [] and items):
            # як може називатися позиція: артикули деталі (BP4K, EB-BG980ABY) з каталогу та інтернету
            part_codes, by_names = await by_part_names(chosen, cat_, part or label, items, dev_codes,
                                                       [m for _, m in near_related])
            if by_names:
                items = list(items) + by_names
                total = len(items)
        if items:
            return make(items, total, models=names, extras_ignored=ignored, part=part, device_codes=dev_codes,
                        part_codes=part_codes)
        if part:
            # як на сайті: "рамка iPhone XS" за текстом (деякі товари не прив'язані до моделі чи категорії)
            items, total = await run({"query": f"{part} {names[0]}"})
            items = _only_model(items, chosen, [m for _, m in near_related])
            if items:
                return make(items, len(items), models=names, fuzzy=True, part=part)
        groups = []
        for m in _nearest(near_related)[:5]:  # для точної моделі нічого немає - дивимось схожі (9 Pro -> 9 Pro XL)
            sub, _ = await run({**cat_, "modelIds": [m["id"]], "query": text_query(False, free_)})
            if sub:
                groups.append((m["name"], sub))
        return make(models=names, related=groups, requested=names[0], kind=label or " ".join(free_), part=part)

    # 0) код (CPH2579, SM-A525F, A2638 - заводський код моделі; EB-BS912ABY, BM4J - артикул деталі)
    if pq.model_tokens and not exact and _looks_like_code(pq.model_tokens):
        code_items = []
        for args in ([{**cat, "query": model_text}] if cat else []) + [{"query": model_text}]:
            code_items, total = await run(args)
            if code_items:
                break
        code = max((t for t in pq.model_tokens if re.search(r"\d", t) and re.search(r"[a-z]", t)),
                   key=len, default="")
        is_part_article = any(code and code in _item_name(i).lower() for i in code_items)  # "Акумулятор ... EB-BS912ABY"
        if depth == 0 and code and not is_part_article:
            # заводський код -> модель, і далі шукаємо як за назвою моделі (з перевіркою сумісності)
            resolved = await _model_from_products(code_items, code)
            if not resolved:
                try:  # інтернет - лише підказка: будь-яка помилка тут не має ламати пошук
                    resolved = await part_lookup.web_model(code, [b["name"] for b in brands])
                except Exception as e:
                    _log(f"пошук моделі {code.upper()} в інтернеті не вдався: {e!r}")
                    resolved = ""
            if resolved:
                _log(f"код {code.upper()} -> модель {resolved}")
                part_words = [pq.kind_label or ""] + [d for d, soft in zip(pq.display, pq.soft) if soft]
                out = await _search_catalog(" ".join(filter(None, part_words + [resolved])), depth + 1, code)
                if out.items or out.related or out.models or out.need_part:
                    out.client_model = f"{resolved} ({code.upper()})"
                    return out
        if code_items:
            return make(code_items, len(code_items))

    # 1) модель знайдено точно
    if exact:
        if not brand_ids and len({m["brand"]["id"] for m in exact if m.get("brand")}) > 1:
            names = [f'{m["brand"]["name"]} {m["name"]}' for m in exact][:8]
            return make(suggestions=names, suggest_reason="brand")
        if len(exact) > 8:
            return make(too_broad=True)
        free = _free_words(exact, pq)
        if not pq.category_id and not pq.extras and not free:
            return make(models=[m["name"] for m in exact][:3], need_part=True, requested=exact[0]["name"])
        return await search_models(exact, [m for m in related], free)

    # 2) точної моделі нема, але є схожі
    if related:
        nearest = _nearest(related)
        if len(nearest) == 1:
            free = _free_words(nearest, pq)
            if not pq.category_id and not pq.extras and not free:
                return make(models=[nearest[0]["name"]], need_part=True, requested=nearest[0]["name"])
            return await search_models(nearest, [], free)
        if len(related) > 12:
            return make(too_broad=True)
        return make(suggestions=[m["name"] for _, m in related][:8], suggest_reason="ambiguous")

    # 3) слова не збіглися з жодною моделлю - пробуємо без останніх слів ("y7 lite" -> "y7")
    if models and len(pq.model_tokens) >= 2:
        for k in range(len(pq.model_tokens) - 1, 0, -1):
            ex, rel = split_models(models, pq.prefix(k))
            names = [m["name"] for m in ex] + [m["name"] for _, m in rel]
            if names:
                return make(suggestions=names[:8], suggest_reason="missing")

    # 4) модель не вказано: бренд + категорія
    if not pq.model_tokens:
        if pq.category_id in REPAIR_CATEGORIES or not (pq.category_id or pq.extras):
            return make(too_broad=True)
        items, total = await run({**cat, "query": text_query(True) or kind})
        return make(items, total)

    # 5) текстовий пошук "бренд + слова запиту", як на сайті (для ноутбуків, артикулів, незвичних назв)
    fuzzy = not _looks_like_code(pq.model_tokens)
    fuzzy_text = " ".join(filter(None, [brand_label, model_text])) or raw_query.strip()
    attempts = [{**({"brandIds": brand_ids} if brand_ids else {}), **cat, "query": fuzzy_text}]
    if cat:
        attempts.append({**cat, "query": fuzzy_text})
    attempts.append({"query": fuzzy_text})
    tried = []
    for args in attempts:
        if args in tried:
            continue
        tried.append(args)
        items, total = await run(args)
        if items:
            return make(items, total, fuzzy=fuzzy)
    return make()


# ==========================================
# 4. ФОРМАТУВАННЯ ВІДПОВІДІ
# ==========================================

def _fmt_date(iso: str) -> str:
    try:
        return datetime.fromisoformat(iso.replace("Z", "+00:00")).strftime("%d.%m")
    except (ValueError, AttributeError):
        return ""


def _availability(product: dict):
    """Повертає (ранг для сортування, текст статусу)."""
    stock = [s for s in product.get("stock") or [] if (s.get("available") or 0) > 0]
    local = next((s for s in stock if s.get("location") == "local"), None)
    if local:
        return 0, f"✅ В наявності · {html.escape(local.get('warehouse') or 'локально')}"
    dates = sorted(s["deliveryTime"] for s in stock if s.get("deliveryTime"))
    if dates:
        return 1, f"🚚 Доставка {_fmt_date(dates[0])}"
    expect = (product.get("expectDate") or {}).get("expectDate")
    if expect:
        return 2, f"⏳ Очікується {_fmt_date(expect)}"
    return 3, "⛔ Немає в наявності"


def _format_items(items: list, total: int, max_items: int, by_rank: bool = False, own=None) -> str:
    """own(item) -> False: у назві інша модель (сумісний за карткою) - такі йдуть після «своїх» і з позначкою 🔁."""
    rows = []
    for item in items:
        product = (item.get("products") or [{}])[0]
        rank, status = _availability(product)
        price = product.get("price") or {}
        amount = price.get("amount")
        if amount is None:
            price_text = "ціна уточнюється"
        elif price.get("currency") == "USD":
            price_text = f"{amount:g} $"
        else:
            price_text = f"{amount:g} {price.get('currency', '')}".strip()
        mine = own(item) if own else True
        mark = "" if mine else "🔁 "
        rows.append((0 if mine else 1, rank, amount if amount is not None else 1e9,
                     f"{mark}<b>{html.escape(item.get('name', 'Без назви'))}</b>\n{price_text} · {status}"))
    if not by_rank:  # для неточних збігів лишаємо порядок за релевантністю від сервера
        rows.sort(key=lambda r: (r[0], r[1], r[2]))
    text = "\n\n".join(r[3] for r in rows[:max_items])
    hidden = max(total, len(rows)) - min(len(rows), max_items)
    if hidden > 0:
        text += f"\n\n…і ще {hidden} — дивіться повні результати на сайті."
    return text


def _own_model_check(outcome: SearchOutcome):
    """Функція item -> чи є в назві товару модель клієнта (None, якщо модель невідома)."""
    if not outcome.models:
        return None
    brand_words = set(re.findall(r"[a-z0-9]+", (outcome.brand or "").lower()))
    target = _model_key(outcome.models[0], brand_words)
    if not target:
        return None

    def own(item):
        words = set(re.findall(r"[a-z0-9]+", _item_name(item).lower().replace("+", " plus ")))
        return target <= words
    return own


LAPTOP_HINT = ("\n\n💡 Для ноутбука акумулятор підбирається за точною моделлю або артикулом АКБ "
               "(наклейка на батареї, напр. AP18C7M). Надішліть артикул або фото наклейки.")


def format_results(outcome: SearchOutcome, max_items: int = 8) -> str:
    hint = LAPTOP_HINT if outcome.laptop else ""
    what = html.escape(outcome.requested or "вказаної моделі")

    if outcome.too_broad:
        return ("Запит надто загальний — збігається з дуже багатьма моделями 🙈\n"
                "Уточніть бренд і модель, наприклад: «акб самсунг s23» або «дисплей iphone 13 pro».")

    if outcome.need_part:
        model = html.escape(", ".join(outcome.models) or outcome.requested)
        return (f"Знайшов модель: <b>{model}</b> 📱\nЩо саме потрібно? Акумулятор, дисплей, шлейф, камера, "
                f"динамік, кришка…\nНапишіть, наприклад: «акумулятор {html.escape(outcome.site_query)}».")

    if outcome.suggestions:
        lines = "\n".join(f"• {html.escape(name)}" for name in outcome.suggestions)
        if outcome.suggest_reason == "brand":
            return (f"Така модель є в кількох брендів 🤔\n{lines}\n\n"
                    f"Додайте бренд до запиту, наприклад: «{html.escape(outcome.suggestions[0])}».")
        example = outcome.suggestions[0]
        if outcome.requested and outcome.requested in outcome.site_query:
            example = outcome.site_query.replace(outcome.requested, outcome.suggestions[0])
        head = (f"Моделі <b>{what}</b> у каталозі не знайдено 😕\nМожливо, ви мали на увазі:"
                if outcome.suggest_reason == "missing"
                else f"Під запит <b>{what}</b> підходить кілька моделей, уточніть:")
        return f"{head}\n{lines}\n\nНапишіть точну модель, наприклад: «{html.escape(example)}»."

    if outcome.items:
        note = ""
        if outcome.extras_ignored:
            note = (f"ℹ️ <i>З «{html.escape(', '.join(outcome.extras_ignored))}» точного збігу нема — "
                    f"показую всі варіанти.</i>\n\n")
        if outcome.part_codes:
            model = html.escape(outcome.models[0]) if outcome.models else what
            battery = "акумулятор" in (outcome.kind or "").lower() or "акумулятор" in (outcome.part or "").lower()
            check = "на наклейці старої АКБ" if battery else "(партномер) на старій деталі"
            note = (f"{'🔋' if battery else '🔧'} Підібрано за артикулом "
                    f"<b>{html.escape(', '.join(outcome.part_codes[:3]))}</b> для <b>{model}</b>.\n"
                    f"<i>Звірте артикул {check}.</i>\n\n")
            return note + _format_items(outcome.items, outcome.total, max_items)
        if outcome.fuzzy:
            note = "⚠️ <i>Точного збігу по моделі нема, знайдено за текстом. Перевірте сумісність!</i>\n\n"
            return note + _format_items(outcome.items, outcome.total, 5, by_rank=True) + hint
        own = _own_model_check(outcome)
        if own and not all(own(i) for i in outcome.items):
            model = html.escape(outcome.client_model or outcome.models[0])
            note += f"🔁 — <i>у назві товару немає {model}, але в його картці ця модель є серед сумісних.</i>\n\n"
        return note + _format_items(outcome.items, outcome.total, max_items, own=own)

    if outcome.related:
        parts = [f"Для <b>{what}</b> у каталозі такої позиції не знайдено 😕\n"
                 f"Є для схожих моделей (перевірте сумісність):"]
        for model_name, items in outcome.related:
            parts.append(f"📱 <b>{html.escape(model_name)}</b>\n\n" + _format_items(items, len(items), 3))
        return "\n\n".join(parts)

    if outcome.models:
        kind = html.escape(outcome.kind or "таких позицій")
        return (f"Для <b>{html.escape(outcome.models[0])}</b> у каталозі позиції «{kind}» не знайдено 😕\n"
                f"Натисніть «Зв'язатися з менеджером» — підкажуть альтернативу.")

    return "Нічого не знайдено 😕 Спробуйте уточнити модель або натисніть «Зв'язатися з менеджером»." + hint
