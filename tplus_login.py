"""
Одноразовий вхід бота в TPlus (OAuth 2.1 + PKCE).

Запуск (один раз, на тому ж комп'ютері, де працює бот):
    python tplus_login.py

Що відбудеться:
  1. Скрипт зареєструє "клієнта" на сервері TPlus (автоматично).
  2. Відкриється браузер зі сторінкою входу TPlus - увійдіть в акаунт, під яким бот
     має бачити ціни й наявність (краще окремий акаунт для бота).
  3. Токени збережуться у файл tplus_tokens.json поруч зі скриптом.
     Далі бот сам оновлює токен; повторювати вхід потрібно, лише якщо
     оновлення перестане працювати.

ВАЖЛИВО: tplus_tokens.json дає доступ до акаунта TPlus. Нікому його не пересилайте
і не публікуйте (GitHub тощо).

Скрипт не потребує додаткових бібліотек.
"""

import base64
import hashlib
import http.server
import json
import os
import secrets
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import webbrowser

ISSUER = os.getenv("TPLUS_ISSUER", "https://dev.tplus.market/api")
RESOURCE = os.getenv("TPLUS_MCP_URL", "https://dev.tplus.market/api/mcp")
AUTH_URL = f"{ISSUER}/oauth/authorize"
TOKEN_URL = f"{ISSUER}/oauth/token"
REGISTER_URL = f"{ISSUER}/oauth/register"
PORT = int(os.getenv("TPLUS_LOGIN_PORT", "8765"))
REDIRECT_URI = f"http://127.0.0.1:{PORT}/callback"
TOKEN_FILE = os.getenv("TPLUS_TOKEN_FILE", os.path.join(os.path.dirname(os.path.abspath(__file__)), "tplus_tokens.json"))
WAIT_SECONDS = 300


def _request(req: urllib.request.Request) -> dict:
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")[:500]
        raise SystemExit(f"\n❌ Сервер відповів {e.code} на {req.full_url}\n{body}\n"
                         "Скопіюйте цей текст і надішліть мені - підкажу, що змінити.")
    except urllib.error.URLError as e:
        raise SystemExit(f"\n❌ Немає зв'язку з {req.full_url}: {e.reason}")


def register_client() -> dict:
    payload = {
        "client_name": "TPlus Telegram bot",
        "redirect_uris": [REDIRECT_URI],
        "grant_types": ["authorization_code", "refresh_token"],
        "response_types": ["code"],
        "token_endpoint_auth_method": "none",
        "scope": "mcp",
    }
    req = urllib.request.Request(REGISTER_URL, data=json.dumps(payload).encode(),
                                 headers={"Content-Type": "application/json", "Accept": "application/json"})
    return _request(req)


def exchange_code(client: dict, code: str, verifier: str) -> dict:
    form = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": REDIRECT_URI,
        "client_id": client["client_id"],
        "code_verifier": verifier,
        "resource": RESOURCE,
    }
    if client.get("client_secret"):
        form["client_secret"] = client["client_secret"]
    req = urllib.request.Request(TOKEN_URL, data=urllib.parse.urlencode(form).encode(),
                                 headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"})
    return _request(req)


def make_callback_server() -> tuple:
    result: dict = {}

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            parsed = urllib.parse.urlparse(self.path)
            if parsed.path != "/callback":
                self.send_response(404)
                self.end_headers()
                return
            result.update({k: v[0] for k, v in urllib.parse.parse_qs(parsed.query).items()})
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write("<h3>Готово! Цю вкладку можна закрити й повернутися в консоль.</h3>".encode("utf-8"))

        def log_message(self, *args):
            pass

    try:
        server = http.server.HTTPServer(("127.0.0.1", PORT), Handler)
    except OSError:
        raise SystemExit(f"❌ Порт {PORT} зайнятий. Закрийте програму, що його використовує, "
                         f"або задайте інший: set TPLUS_LOGIN_PORT=8766")
    server.timeout = 1
    return server, result


def wait_for_callback(server, result: dict) -> dict:
    deadline = time.time() + WAIT_SECONDS
    while not result and time.time() < deadline:
        server.handle_request()
    server.server_close()
    if not result:
        raise SystemExit("❌ Не дочекався входу (5 хвилин). Запустіть скрипт ще раз.")
    return result


def save_tokens(client: dict, token: dict):
    data = {
        "client_id": client["client_id"],
        "client_secret": client.get("client_secret", ""),
        "access_token": token["access_token"],
        "refresh_token": token.get("refresh_token", ""),
        "expires_at": time.time() + int(token.get("expires_in", 3600)),
        "token_endpoint": TOKEN_URL,
        "resource": RESOURCE,
    }
    tmp = TOKEN_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, TOKEN_FILE)
    try:
        os.chmod(TOKEN_FILE, 0o600)
    except OSError:
        pass


def main():
    print("1/4 Реєструю бота на сервері TPlus...")
    client = register_client()

    verifier = secrets.token_urlsafe(64)
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()
    state = secrets.token_urlsafe(16)
    url = AUTH_URL + "?" + urllib.parse.urlencode({
        "response_type": "code",
        "client_id": client["client_id"],
        "redirect_uri": REDIRECT_URI,
        "code_challenge": challenge,
        "code_challenge_method": "S256",
        "scope": "mcp",
        "state": state,
        "resource": RESOURCE,
    })

    server, result = make_callback_server()
    print("2/4 Відкриваю браузер. Увійдіть в акаунт TPlus і підтвердьте доступ.")
    print("    Якщо браузер не відкрився, скопіюйте це посилання вручну:\n\n    " + url + "\n")
    webbrowser.open(url)

    print("3/4 Чекаю на вхід...")
    result = wait_for_callback(server, result)
    if "error" in result:
        raise SystemExit(f"❌ Сервер повернув помилку: {result.get('error')} {result.get('error_description', '')}")
    if result.get("state") != state:
        raise SystemExit("❌ Невідповідність state - вхід скасовано з міркувань безпеки.")
    if result.get("iss") and result["iss"] != ISSUER:
        raise SystemExit(f"❌ Відповідь прийшла від іншого сервера ({result['iss']}) - вхід скасовано.")
    if "code" not in result:
        raise SystemExit("❌ Сервер не повернув код авторизації.")

    print("4/4 Отримую токен...")
    token = exchange_code(client, result["code"], verifier)
    if "access_token" not in token:
        raise SystemExit(f"❌ Сервер не видав токен: {token}")
    save_tokens(client, token)

    print(f"\n✅ Готово! Токени збережено в {TOKEN_FILE}")
    if not token.get("refresh_token"):
        print("⚠️ Сервер не видав refresh_token: токен діятиме обмежений час, "
              "і вхід доведеться повторювати.")
    print("Тепер запустіть бота: python main.py")


if __name__ == "__main__":
    sys.exit(main())
