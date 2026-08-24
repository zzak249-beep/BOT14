"""
Comprueba Telegram ANTES de desplegar.

    python test_telegram.py

Un bot que no puede avisarte es un bot mudo, y lo peor es que parece
que funciona: los logs dicen "señal detectada" y a ti no te llega nada.
Treinta segundos aquí ahorran esa confusión.
"""
from __future__ import annotations

import asyncio
import sys

import httpx

import config


async def main() -> int:
    print("Comprobando configuración de Telegram...\n")

    if not config.TELEGRAM_TOKEN:
        print("✗ Falta TELEGRAM_TOKEN")
        print("  Créalo hablando con @BotFather en Telegram: /newbot")
        return 1
    if not config.TELEGRAM_CHAT_ID:
        print("✗ Falta TELEGRAM_CHAT_ID")
        print("  Escribe algo a tu bot y abre:")
        print(f"  https://api.telegram.org/bot{config.TELEGRAM_TOKEN}/getUpdates")
        print('  El id está en result[0].message.chat.id')
        return 1

    async with httpx.AsyncClient() as c:
        # 1. ¿El token es válido?
        r = await c.get(f"https://api.telegram.org/bot{config.TELEGRAM_TOKEN}/getMe", timeout=15)
        if r.status_code != 200 or not r.json().get("ok"):
            print(f"✗ Token rechazado por Telegram: {r.text[:200]}")
            return 1
        nombre = r.json()["result"].get("username", "?")
        print(f"✓ Token válido — bot @{nombre}")

        # 2. ¿Llega el mensaje al chat?
        r = await c.post(
            f"https://api.telegram.org/bot{config.TELEGRAM_TOKEN}/sendMessage",
            json={
                "chat_id": config.TELEGRAM_CHAT_ID,
                "text": (
                    "✅ <b>Prueba correcta</b>\n"
                    "Si lees esto, el bot puede avisarte.\n\n"
                    f"Modo configurado: {config.describe()}\n"
                    f"Amplitud mínima: {config.MIN_ATR_PCT}% y {config.MIN_COST_COVER:.0f}× el coste\n"
                    f"Tiempo máximo por operación: {config.max_trade_seconds() // 60} min"
                ),
                "parse_mode": "HTML",
            },
            timeout=15,
        )
        if r.status_code != 200 or not r.json().get("ok"):
            print(f"✗ No se pudo enviar al chat {config.TELEGRAM_CHAT_ID}: {r.text[:200]}")
            print("  Comprueba que has hablado con el bot al menos una vez.")
            return 1
        print(f"✓ Mensaje entregado al chat {config.TELEGRAM_CHAT_ID}")

    print(f"\nTodo listo. Modo: {config.describe()}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
