"""
Діагностика пошуку без Telegram.

Запуск:  python tplus_debug.py акб піксель 9 про
Показує, як розібрано запит, які запити йдуть на сервер TPlus і що він повернув.
"""
import asyncio
import sys

import tplus_search as t


async def main():
    query = " ".join(sys.argv[1:]) or "акб піксель 9 про"
    print(f"Запит: {query}")
    print(f"Вхід у TPlus виконано: {t.catalog_configured()}  (файл: {t.TOKEN_FILE})")
    print(f"Розібрано: {t.parse_query(query)}\n")
    outcome = await t.search_catalog(query)
    print("\n--- Що побачить клієнт у Telegram ---")
    print(t.format_results(outcome))
    await t.close_mcp()


asyncio.run(main())
