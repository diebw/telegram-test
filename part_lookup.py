"""
Пошук артикулу деталі (АКБ) в інтернеті, коли в каталозі TPlus вона не прив'язана до моделі.

Приклад: "акб самсунг s20" -> у каталозі АКБ названа "Акумулятор для Samsung EB-BG980ABY",
а фільтр за моделлю її не повертає. Модуль шукає в інтернеті "Samsung Galaxy S20 battery model",
збирає з результатів схожі на артикули коди (EB-BG980ABY, BM4J, GVYZ7...) і повертає їх
від найчастішого. Перевіряє коди вже tplus_search - по каталогу, тому випадковий код у видачі
нічого не зламає: у клієнта з'явиться лише те, що реально є на сайті.

Знайдені артикули зберігаються у part_codes.json (бренд|модель|деталь -> [артикули]).
Цей файл можна редагувати вручну: дописати правильний артикул або виправити помилковий.

Без ключів і платних сервісів: DuckDuckGo (HTML-версія), запасний варіант - Bing.
"""

import asyncio
import html as html_lib
import json
import os
import re
import time
import urllib.parse
from collections import Counter

import aiohttp

CACHE_FILE = os.getenv("PART_CODES_FILE", os.path.join(os.path.dirname(os.path.abspath(__file__)), "part_codes.json"))
NEGATIVE_TTL = 24 * 3600  # якщо нічого не знайшли - повторюємо пошук не частіше ніж раз на добу
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/124.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9,uk;q=0.8",
}

# що дописати до запиту, щоб у видачі були артикули
PART_WORDS = {
    "battery": "battery model number", "акумулятор": "battery model number",
    "дисплей": "LCD display part number", "матриця": "LCD panel part number",
    "тачскрін": "touch screen digitizer part number", "шлейф": "flex cable part number",
    "камера": "camera module part number", "динамік": "speaker part number", "мікрофон": "microphone part number",
    "роз'єм": "charging port part number", "кнопка": "button flex part number", "кришка": "back cover part number",
    "корпус": "housing part number", "power bank": "power bank", "зарядний": "charger model",
    "нижня плата": "charging port board part number", "рамка": "middle frame part number",
}

# схоже на артикул: латиниця + цифри, 4-14 символів, можливо з дефісом (EB-BG980ABY, BM4J, HB396285ECW, BLP837)
_TOKEN = re.compile(r"\b[A-Z][A-Z0-9]{0,4}(?:-[A-Z0-9]{2,12}|[A-Z0-9]{2,12})\b")
_NOT_CODE = re.compile(
    r"^(?:\d+(?:MAH|WH|V|W|MM|GB|TB|G|HZ)|USB\w*|LTE|NFC|OLED|AMOLED|LCD|HTML|HTTPS?|WWW|COM|ISBN\w*"
    r"|SM-\w+|\d+[A-Z]?|[A-Z]+|[A-Z]\d{1,2})$")


def _load() -> dict:
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def _save(data: dict):
    tmp = CACHE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, CACHE_FILE)


def _key(brand: str, model: str, part: str) -> str:
    return f"{brand}|{model}|{part}".lower()


def cached_codes(brand: str, model: str, part: str):
    """Збережені артикули (список) або None, якщо ще не шукали."""
    entry = _load().get(_key(brand, model, part))
    if entry is None:
        return None
    if isinstance(entry, list):  # записано вручну
        return entry
    if not entry.get("codes") and time.time() - entry.get("checked", 0) > NEGATIVE_TTL:
        return None  # минулого разу нічого не знайшли - пробуємо знову
    return entry.get("codes", [])


def remember(brand: str, model: str, part: str, codes: list):
    data = _load()
    data[_key(brand, model, part)] = {"codes": codes, "checked": time.time()}
    try:
        _save(data)
    except OSError as e:
        print(f"[part_lookup] не вдалося зберегти {CACHE_FILE}: {e!r}")


def _text_of(page: str) -> str:
    page = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", " ", page)
    return html_lib.unescape(re.sub(r"<[^>]+>", " ", page))


async def _fetch(session: aiohttp.ClientSession, url: str) -> str:
    try:
        async with session.get(url, headers=HEADERS) as resp:
            if resp.status != 200:
                print(f"[part_lookup] {url.split('?')[0]} -> HTTP {resp.status}")
                return ""
            return await resp.text(errors="replace")
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        print(f"[part_lookup] {url.split('?')[0]} недоступний: {e!r}")
        return ""


async def _search_text(query: str) -> str:
    q = urllib.parse.quote_plus(query)
    urls = [f"https://html.duckduckgo.com/html/?q={q}", f"https://www.bing.com/search?q={q}&setlang=en"]
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=12)) as session:
        for url in urls:
            text = _text_of(await _fetch(session, url))
            if text.strip():
                return text
    return ""


_MODEL_TAIL = r"[A-Za-z]{0,3}\d{1,4}[A-Za-z]{0,3}(?:\s(?:Pro|Plus|Max|Lite|Ultra|Neo|Prime|Mini|[45]G|FE|SE|XL|S))*"


def models_in_text(text: str, code: str, brands: list) -> list:
    """
    Моделі, які в тексті стоять поруч із заводським кодом: "Oppo A38 (CPH2579)", "CPH2579 - OPPO A38",
    "Samsung Galaxy A52 SM-A525F". Повертає ["Oppo A38", ...] від найчастішого.
    """
    votes = Counter()
    code_re = re.escape(code)
    names = sorted({b for b in brands if len(b) >= 2}, key=len, reverse=True)
    for brand in names:
        b = re.escape(brand)
        model = rf"((?:[A-Za-z]+\s)?{_MODEL_TAIL})"   # "A38", "Galaxy A52", "Redmi Note 8 Pro" (до 1 слова серії)
        before = rf"\b({b})\s+{model}\s*[\(\[,/–-]?\s*{code_re}\b"
        after = rf"\b{code_re}\s*[\)\]]?\s*[,:–-]?\s*\(?\s*({b})\s+{model}"
        for m in re.finditer(before, text, re.I):
            votes[f"{m.group(1)} {m.group(2)}".strip()] += 1
        for m in re.finditer(after, text, re.I):
            votes[f"{m.group(1)} {m.group(2)}".strip()] += 1
    merged = Counter()
    for name, n in votes.items():  # "Oppo A38" і "OPPO A38" - одне й те саме
        merged[name.title() if name.isupper() else name] += n
    return [name for name, _ in merged.most_common()]


async def web_model(code: str, brands: list) -> str:
    """Заводський код моделі -> назва ("CPH2579" -> "Oppo A38") з інтернету; результат кешується."""
    key = f"model|{code}".lower()
    entry = _load().get(key)
    if isinstance(entry, str):  # записано вручну
        return entry
    if isinstance(entry, dict) and (entry.get("model") or time.time() - entry.get("checked", 0) < NEGATIVE_TTL):
        return entry.get("model", "")
    text = await _search_text(f"{code.upper()} phone model")
    found = models_in_text(text, code, brands)
    print(f"[part_lookup] модель для {code.upper()} -> {found[:3]}")
    model = found[0] if found else ""
    data = _load()
    data[key] = {"model": model, "checked": time.time()}
    try:
        _save(data)
    except OSError as e:
        print(f"[part_lookup] не вдалося зберегти {CACHE_FILE}: {e!r}")
    return model


# ідентифікатори пристрою (не деталі!) за брендом: так модель записана в картках каталогу
# («Сумісні моделі та партномери»: A2588, SM-A525F, CPH2579 ...)
DEVICE_CODE_PATTERNS = {
    "apple": r"\bA[12]\d{3}\b",                                   # A2588, A1484
    "samsung": r"\bSM-[A-Z]\d{3,4}[A-Z]{0,2}\d?\b",               # SM-A525F, SM-G990B2
    "oppo": r"\bCPH\d{4}\b", "realme": r"\bRMX\d{4}\b",
    "oneplus": r"\b(?:CPH|BE|IN|KB|LE|NE)\d{4}\b",
    "xiaomi": r"\b(?:M\d{4}[A-Z]\d{1,2}[A-Z]{1,2}|2\d{5,7}[A-Z]{1,3})\b",  # M1906G7G, 2201117TG
    "huawei": r"\b[A-Z]{3}-[A-Z]{1,2}\d{2}[A-Z]?\b", "honor": r"\b[A-Z]{3}-[A-Z]{1,2}\d{2}[A-Z]?\b",  # VOG-L29
    "motorola": r"\bXT\d{4}(?:-\d{1,2})?\b", "nokia": r"\bTA-\d{4}\b",
    "google": r"\bG[A-Z0-9]{4}\b", "vivo": r"\bV\d{4}[A-Z]?\b", "tecno": r"\b[A-Z]{2}\d[A-Z]?\b",
    "infinix": r"\bX\d{3,4}[A-Z]?\b", "sony": r"\b[A-Z]{1,2}\d{4}\b", "lenovo": r"\bTB-?[A-Z0-9]{4,7}\b",
}


async def web_device_codes(brand: str, model: str) -> list:
    """
    Ідентифікатори пристрою з інтернету: "Apple iPad Air 5 2022" -> ["A2588", "A2589", "A2591"].
    Кешується назавжди (порожній результат - на добу). Файл можна правити вручну:
    "device|apple|ipad air 5 2022": ["A2588", "A2589", "A2591"].
    """
    pattern = DEVICE_CODE_PATTERNS.get((brand or "").lower())
    if not pattern:
        return []
    key = f"device|{brand}|{model}".lower()
    entry = _load().get(key)
    if isinstance(entry, list):
        return entry
    if isinstance(entry, dict) and (entry.get("codes") or time.time() - entry.get("checked", 0) < NEGATIVE_TTL):
        return entry.get("codes", [])
    text = await _search_text(f"{brand} {model} model number")
    counts = Counter(m.upper() for m in re.findall(pattern, text, re.I))
    codes = [c for c, n in counts.most_common(6)]
    print(f"[part_lookup] ідентифікатори {brand} {model} -> {codes}")
    data = _load()
    data[key] = {"codes": codes, "checked": time.time()}
    try:
        _save(data)
    except OSError as e:
        print(f"[part_lookup] не вдалося зберегти {CACHE_FILE}: {e!r}")
    return codes


async def web_codes(brand: str, model: str, part: str = "battery", exclude=()) -> list:
    """Артикули з результатів пошуку в інтернеті, від найчастішого. exclude - коди самої моделі (G980, SM-G980F)."""
    query = f"{brand} {model} {PART_WORDS.get(part, part + ' part number')}"
    q = urllib.parse.quote_plus(query)
    urls = [f"https://html.duckduckgo.com/html/?q={q}", f"https://www.bing.com/search?q={q}&setlang=en"]
    skip = {e.upper() for e in exclude}
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=12)) as session:
        for url in urls:
            text = _text_of(await _fetch(session, url))
            if not text:
                continue
            counts = Counter()
            for tok in _TOKEN.findall(text.upper()):
                if len(tok) < 4 or _NOT_CODE.match(tok) or tok in skip:
                    continue
                if not (re.search(r"[A-Z]", tok) and re.search(r"\d", tok)):
                    continue
                if any(s and s in tok and len(tok) - len(s) <= 2 for s in skip):  # G980F, G980FD - варіанти моделі
                    continue
                counts[tok] += 1
            codes = [c for c, _ in counts.most_common(8)]
            print(f"[part_lookup] «{query}» -> {codes}")
            if codes:
                return codes
    return []
