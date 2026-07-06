"""
Diagnóstico de credenciales BingX — standalone, sin depender del resto del bot.
================================================================================
Corré esto en TU máquina (no en Railway) para aislar el problema en segundos,
sin esperar un redeploy. Va a mostrar exactamente qué string se firma y qué
firma se genera, para que puedas comparar contra lo que BingX espera.

Uso:
    pip install aiohttp
    export BINGX_API_KEY="tu_key_real"
    export BINGX_API_SECRET="tu_secret_real"
    python3 diagnostico_bingx.py
"""
import asyncio
import hashlib
import hmac
import os
import time
from urllib.parse import urlencode

import aiohttp

API_KEY = os.getenv("BINGX_API_KEY", "").strip()
API_SECRET = os.getenv("BINGX_API_SECRET", "").strip()
BASE_URL = "https://open-api.bingx.com"


def sign(params: dict, secret: str) -> str:
    qs = urlencode(sorted(params.items()))
    return hmac.new(secret.encode("utf-8"), qs.encode("utf-8"), hashlib.sha256).hexdigest()


async def main():
    print("=" * 70)
    print("DIAGNÓSTICO DE CREDENCIALES BINGX")
    print("=" * 70)

    # Chequeos básicos ANTES de llamar a la API — la mayoría de los problemas
    # de firma en la práctica son esto, no matemática de HMAC.
    print(f"\nBINGX_API_KEY presente: {bool(API_KEY)}  (longitud: {len(API_KEY)})")
    print(f"BINGX_API_SECRET presente: {bool(API_SECRET)}  (longitud: {len(API_SECRET)})")

    if not API_KEY or not API_SECRET:
        print("\n❌ Falta una de las dos variables de entorno. Exportalas y volvé a correr.")
        return

    # Detectar espacios/saltos de línea invisibles — causa MUY común
    raw_key = os.getenv("BINGX_API_KEY", "")
    raw_secret = os.getenv("BINGX_API_SECRET", "")
    if raw_key != API_KEY:
        print(f"⚠️  BINGX_API_KEY tiene espacios/saltos de línea al principio o final "
              f"(sin limpiar: {len(raw_key)} caracteres, limpio: {len(API_KEY)}). "
              f"Esto solo podría pasar si Railway guardó la variable con espacios extra.")
    if raw_secret != API_SECRET:
        print(f"⚠️  BINGX_API_SECRET tiene espacios/saltos de línea al principio o final "
              f"(sin limpiar: {len(raw_secret)} caracteres, limpio: {len(API_SECRET)}).")

    params = {"timestamp": int(time.time() * 1000)}
    signature = sign(params, API_SECRET)
    params["signature"] = signature
    query_string = urlencode(sorted(params.items()))
    url = f"{BASE_URL}/openApi/swap/v2/user/balance?{query_string}"

    print(f"\nString exacta que se firmó: timestamp={params['timestamp']}")
    print(f"Firma HMAC-SHA256 generada: {signature}")
    print(f"URL completa enviada:\n  {url}")

    print("\nEnviando request real a BingX...")
    async with aiohttp.ClientSession() as session:
        headers = {"X-BX-APIKEY": API_KEY}
        async with session.request("GET", url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            data = await resp.json(content_type=None)

    print(f"\nRespuesta de BingX: {data}")

    if data.get("code") == 0:
        print("\n✅ FUNCIONA — las credenciales y la firma están bien. El problema")
        print("   está en otro lado (Railway no está pasando las variables igual,")
        print("   o hay una versión vieja del código corriendo ahí).")
    elif data.get("code") == 100001:
        print("\n❌ Mismo error de firma acá, en tu máquina, con la misma lógica.")
        print("   Esto descarta problemas de Railway/deploy — apunta directo a:")
        print("   1. El BINGX_API_SECRET no es el que corresponde a ese BINGX_API_KEY")
        print("      (par mezclado, o se regeneró uno de los dos sin actualizar el otro)")
        print("   2. Hay caracteres invisibles en el secret que no se detectaron arriba")
        print("   3. La API key fue revocada o expiró")
        print("   -> Más rápido: andá a BingX, borrá esta key, creá una nueva,")
        print("      copiala con cuidado (sin seleccionar espacios extra) y probá de nuevo.")
    elif data.get("code") == 100413:
        print("\n❌ API key null/no reconocida — la key en sí está mal, no es un tema de firma.")
    else:
        print(f"\n❓ Error distinto a los esperados (code={data.get('code')}) — "
              f"leé el mensaje de arriba, puede ser un tema de permisos de la key "
              f"(¿tiene habilitado Futuros?) o de whitelist de IP.")


if __name__ == "__main__":
    asyncio.run(main())
