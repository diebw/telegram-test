"""
Розпізнавання товару за фото та голосовим для Telegram-бота.

Нічого не вимагає від TPlus - тільки визначає, ЩО саме шукає клієнт:

  * Фото      -> БЕЗКОШТОВНО: розпізнавання тексту на фото (OCR) і пошук артикулів/маркування
                 (A1484, EB-BS912ABY, GVYZ7...). Потрібен один із варіантів:
                     pip install rapidocr-onnxruntime pillow     (лише pip, без додаткових програм)
                     або pytesseract + програма Tesseract-OCR
                 Якщо задано ANTHROPIC_API_KEY - використовується Claude (платно): він визначає
                 товар і за виглядом, навіть без маркування.
  * Голосове  -> ffmpeg конвертує ogg у wav, SpeechRecognition (безкоштовний сервіс Google,
                 без ключа) перетворює мовлення на текст. Мови: українська, потім російська.

Залежності:
    pip install SpeechRecognition
    + встановлений ffmpeg (https://ffmpeg.org)
"""

import asyncio
import base64
import io
import json
import os
import re
import shutil
import tempfile

import aiohttp

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-5-5")

PHOTO_PROMPT = (
    "Ти помічник магазину запчастин для телефонів і гаджетів (TPlus). "
    "На фото запчастина або пристрій. Визнач, що це, а також модель пристрою чи артикул, "
    "якщо вони видні (наклейка, маркування, напис на корпусі).\n"
    "Відповідай ТІЛЬКИ JSON без пояснень і без markdown:\n"
    '{"title": "коротка назва позиції українською", '
    '"query": "короткий пошуковий запит, напр. \'акб Samsung EB-BS912ABY\' або \'дисплей Google Pixel 8 Pro\'", '
    '"confident": true або false}\n'
    "Не вгадуй модель, якщо її не видно: вкажи лише тип деталі і confident=false. "
    "Якщо визначити неможливо, залиш query порожнім."
)


class RecognitionUnavailable(Exception):
    """Розпізнавання не налаштоване (немає ключа, ffmpeg або бібліотеки)."""


def _ffmpeg_path():
    """ffmpeg з PATH, а якщо його нема - вбудований у пакет imageio-ffmpeg (pip install imageio-ffmpeg)."""
    path = shutil.which("ffmpeg")
    if path:
        return path
    try:
        import imageio_ffmpeg
        return imageio_ffmpeg.get_ffmpeg_exe()
    except Exception:
        return None


def _parse_json(text: str) -> dict:
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        return {}
    try:
        data = json.loads(match.group(0))
    except ValueError:
        return {}
    return data if isinstance(data, dict) else {}


async def _recognize_with_claude(image_bytes: bytes, hint: str = "") -> dict:
    """Платний варіант (потрібен ANTHROPIC_API_KEY): {"title", "query", "confident"}."""
    if not ANTHROPIC_API_KEY:
        raise RecognitionUnavailable("Не задано ANTHROPIC_API_KEY")

    prompt = PHOTO_PROMPT + (f"\nПідказка від клієнта: {hint}" if hint else "")
    payload = {
        "model": ANTHROPIC_MODEL,
        "max_tokens": 300,
        "messages": [{
            "role": "user",
            "content": [
                {"type": "image", "source": {
                    "type": "base64", "media_type": "image/jpeg",
                    "data": base64.b64encode(image_bytes).decode(),
                }},
                {"type": "text", "text": prompt},
            ],
        }],
    }
    headers = {
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session:
        async with session.post("https://api.anthropic.com/v1/messages", json=payload, headers=headers) as resp:
            body = await resp.text()
            if resp.status != 200:
                raise RuntimeError(f"Anthropic API {resp.status}: {body[:200]}")
    data = json.loads(body)
    text = "".join(b.get("text", "") for b in data.get("content", []) if b.get("type") == "text")
    parsed = _parse_json(text)
    return {
        "title": str(parsed.get("title", "")).strip(),
        "query": str(parsed.get("query", "")).strip(),
        "confident": bool(parsed.get("confident", False)),
    }


async def transcribe_voice(ogg_bytes: bytes) -> str:
    """Голосове повідомлення -> текст (порожній рядок, якщо мовлення не розібрано)."""
    try:
        import speech_recognition as sr
    except ImportError:
        raise RecognitionUnavailable("Не встановлено SpeechRecognition")

    ffmpeg = _ffmpeg_path()
    if not ffmpeg:
        raise RecognitionUnavailable("Не встановлено ffmpeg (виконайте: pip install imageio-ffmpeg)")

    with tempfile.TemporaryDirectory() as tmp:
        src, wav = os.path.join(tmp, "voice.ogg"), os.path.join(tmp, "voice.wav")
        with open(src, "wb") as f:
            f.write(ogg_bytes)
        try:
            proc = await asyncio.create_subprocess_exec(
                ffmpeg, "-y", "-i", src, "-ar", "16000", "-ac", "1", wav,
                stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
            )
        except FileNotFoundError:
            raise RecognitionUnavailable("Не встановлено ffmpeg")
        if await proc.wait() != 0:
            raise RuntimeError("ffmpeg не зміг обробити аудіо")

        def _recognize() -> str:
            recognizer = sr.Recognizer()
            with sr.AudioFile(wav) as source:
                audio = recognizer.record(source)
            for language in ("uk-UA", "ru-RU"):
                try:
                    return recognizer.recognize_google(audio, language=language)
                except sr.UnknownValueError:
                    continue
            return ""

        return await asyncio.get_running_loop().run_in_executor(None, _recognize)


# ---------- безкоштовне розпізнавання тексту (OCR) ----------

_UNIT = re.compile(r"^\d+([.,]\d+)?(mah|ah|wh|whr|v|w|hz|mp|gb|tb|mm)$", re.I)
_CODE = re.compile(r"[A-Za-z0-9][A-Za-z0-9\-]{3,18}[A-Za-z0-9]")


def extract_codes(text: str) -> list:
    """Артикули з тексту: літери+цифри, від 5 символів, без одиниць виміру (8827mAh)."""
    codes, seen = [], set()
    for raw in _CODE.findall(text):
        token = raw.strip("-")
        if len(token) < 5 or _UNIT.match(token):
            continue
        if not (re.search(r"[A-Za-z]", token) and re.search(r"\d", token)):
            continue
        if token.upper() not in seen:
            seen.add(token.upper())
            codes.append(token)
    return codes


def _ocr_sync(image_bytes: bytes) -> str:
    try:
        from rapidocr_onnxruntime import RapidOCR
        result, _ = RapidOCR()(image_bytes)
        return "\n".join(line[1] for line in (result or []))
    except ImportError:
        pass

    try:
        import pytesseract
        from PIL import Image
    except ImportError:
        raise RecognitionUnavailable("Встановіть: pip install rapidocr-onnxruntime pillow")
    cmd = os.getenv("TESSERACT_CMD") or r"C:\Program Files\Tesseract-OCR\tesseract.exe"
    if os.path.exists(cmd):
        pytesseract.pytesseract.tesseract_cmd = cmd
    try:
        return pytesseract.image_to_string(Image.open(io.BytesIO(image_bytes)), lang="eng")
    except pytesseract.TesseractNotFoundError:
        raise RecognitionUnavailable("Не встановлено програму Tesseract-OCR")


async def recognize_photo(image_bytes: bytes, hint: str = "") -> dict:
    """
    Повертає {"title": str, "query": str, "queries": [str], "confident": bool}.
    З ключем ANTHROPIC_API_KEY - через Claude, інакше безкоштовно через OCR.
    """
    if ANTHROPIC_API_KEY:
        result = await _recognize_with_claude(image_bytes, hint)
        result["queries"] = [result["query"]] if result["query"] else []
        result["text"] = result["query"]
        return result

    text = await asyncio.get_running_loop().run_in_executor(None, _ocr_sync, image_bytes)
    codes = extract_codes(f"{hint}\n{text}")
    return {
        "title": ", ".join(codes[:3]),
        "query": codes[0] if codes else "",
        "queries": codes[:4],
        "confident": True,
        "text": text,  # увесь розпізнаний текст: з нього бот вибере рядок з назвою моделі ("Redmi Note 12 Pro 5G")
    }
