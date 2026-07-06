"""
Diagnóstico de credenciales BingX — standalone, sin depender del resto del bot.
================================================================================
CÓMO USAR (sin comandos, sin terminal):
1. Abrí este archivo con el Bloc de notas (click derecho -> Abrir con -> Bloc de notas).
2. Reemplazá AQUI_TU_KEY y AQUI_TU_SECRET de las dos líneas de abajo (entre las
   comillas) por tu Key y Secret reales de BingX.
3. Guardá el archivo (Ctrl+S) y cerralo.
4. Hacé doble clic en "correr_diagnostico.bat" (tiene que estar en la misma carpeta).
5. Se abre una ventana negra con el resultado. Copiá TODO el texto que aparece
   (click derecho dentro de la ventana -> Seleccionar todo, después Enter o
   click derecho de nuevo -> Copiar) y pegámelo.
"""
import asyncio
import base64
import hashlib
import hmac
import time
from urllib.parse import urlencode

import aiohttp

# ── PONÉ TUS CREDENCIALES ACÁ ABAJO, ENTRE LAS COMILLAS ────────────────────
API_KEY = "AQUI_TU_KEY"
API_SECRET = "AQUI_TU_SECRET"
# ────────────────────────────────────────────────────────────────────────

_RAW_KEY = API_KEY
_RAW_SECRET = API_SECRET
API_KEY = API_KEY.strip()
API_SECRET = API_SECRET.strip()
BASE_URL = "https://open-api.bingx.com"


def sign_hex(params: dict, secret: str) -> str:
    qs = urlencode(sorted(params.items()))
    return hmac.new(secret.encode("utf-8"), qs.encode("utf-8"), hashlib.sha256).hexdigest()


def sign_base64(params: dict, secret: str) -> str:
    qs = urlencode(sorted(params.items()))
    raw = hmac.new(secret.encode("utf-8"), qs.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(raw).decode("utf-8")


async def try_balance(session, signature, params_without_sig):
    params = dict(params_without_sig)
    params["signature"] = signature
    query_string = urlencode(sorted(params.items()))
    url = f"{BASE_URL}/openApi/swap/v2/user/balance?{query_string}"
    headers = {"X-BX-APIKEY": API_KEY}
    async with session.request("GET", url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
        return await resp.json(content_type=None)


async def main():
    print("=" * 70)
    print("DIAGNÓSTICO DE CREDENCIALES BINGX")
    print("=" * 70)

    if API_KEY == "AQUI_TU_KEY" or API_SECRET == "AQUI_TU_SECRET":
        print("\n❌ Todavía no completaste tu Key/Secret en este archivo.")
        print("   Abrí diagnostico_bingx.py con el Bloc de notas, reemplazá")
        print("   AQUI_TU_KEY y AQUI_TU_SECRET por tus valores reales, guardá,")
        print("   y volvé a hacer doble clic en correr_diagnostico.bat.")
        input("\nApretá Enter para cerrar...")
        return

    # Chequeo NUEVO: desincronización de reloj. BingX usa una ventana
    # (recvWindow) para aceptar el timestamp de la request — si el reloj de
    # esta máquina difiere mucho del reloj real de BingX, algunas exchanges
    # (no confirmado si BingX es una de ellas) devuelven el mismo error
    # genérico de firma en vez de un error específico de timestamp/recvWindow.
    print("\nChequeando sincronización de reloj contra el servidor de BingX...")
    local_time_before = int(time.time() * 1000)
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(f"{BASE_URL}/openApi/swap/v2/server/time",
                                     timeout=aiohttp.ClientTimeout(total=10)) as resp:
                time_data = await resp.json(content_type=None)
            local_time_after = int(time.time() * 1000)
            server_time = time_data.get("data", {}).get("serverTime")
            if server_time:
                local_avg = (local_time_before + local_time_after) // 2
                drift_ms = local_avg - server_time
                print(f"Hora local (tu máquina): {local_avg}")
                print(f"Hora del servidor BingX: {server_time}")
                print(f"Diferencia: {drift_ms}ms ({drift_ms/1000:.1f}s)")
                if abs(drift_ms) > 5000:
                    print(f"⚠️  DESINCRONIZADO por más de 5 segundos — esto podría ser la causa real")
                    print(f"   del error de firma. Sincronizá el reloj de Windows (Configuración ->")
                    print(f"   Hora e idioma -> Sincronizar ahora) y volvé a correr este script.")
                else:
                    print("✅ Reloj sincronizado dentro de un margen razonable — no parece ser la causa")
            else:
                print(f"No se pudo leer serverTime de la respuesta: {time_data}")
        except Exception as e:
            print(f"No se pudo consultar la hora del servidor: {e}")

    # Chequeos básicos ANTES de llamar a la API — la mayoría de los problemas
    # de firma en la práctica son esto, no matemática de HMAC.
    print(f"\nBINGX_API_KEY presente: {bool(API_KEY)}  (longitud: {len(API_KEY)})")
    print(f"BINGX_API_SECRET presente: {bool(API_SECRET)}  (longitud: {len(API_SECRET)})")

    if not API_KEY or not API_SECRET:
        print("\n❌ Falta una de las dos variables de entorno. Exportalas y volvé a correr.")
        return

    # Detectar espacios/saltos de línea invisibles — causa MUY común
    if _RAW_KEY != API_KEY:
        print("⚠️  Tu API_KEY tiene espacios/saltos de línea al principio o final del "
              "texto que pegaste entre las comillas.")
    if _RAW_SECRET != API_SECRET:
        print("⚠️  Tu API_SECRET tiene espacios/saltos de línea al principio o final del "
              "texto que pegaste entre las comillas.")

    # recvWindow explícito (10s, generoso) -- antes no se mandaba ninguno,
    # dependiendo del default de BingX. Se agrega por las dudas de que el
    # default sea más chico de lo esperado.
    base_params = {"timestamp": int(time.time() * 1000), "recvWindow": 10000}

    print("\n" + "=" * 70)
    print("PROBANDO LAS DOS CODIFICACIONES DE FIRMA POSIBLES")
    print("=" * 70)

    sig_hex = sign_hex(base_params, API_SECRET)
    sig_b64 = sign_base64(base_params, API_SECRET)
    print(f"\nFirma HEX:    {sig_hex}")
    print(f"Firma BASE64: {sig_b64}")

    async with aiohttp.ClientSession() as session:
        print("\n--- Probando con HEX (lo que usé hasta ahora) ---")
        data_hex = await try_balance(session, sig_hex, base_params)
        print(f"Respuesta: {data_hex}")

        print("\n--- Probando con BASE64 ---")
        data_b64 = await try_balance(session, sig_b64, base_params)
        print(f"Respuesta: {data_b64}")

    print("\n" + "=" * 70)
    if data_hex.get("code") == 0:
        print("✅ HEX FUNCIONA — el problema no era la codificación, era otra cosa.")
    elif data_b64.get("code") == 0:
        print("✅✅✅ BASE64 FUNCIONA Y HEX NO — ERA ESTO. Avisame para que actualice")
        print("   exchange_client.py a usar base64 en vez de hexdigest().")
    else:
        print("❌ Ninguna de las dos funcionó — la codificación no era la causa,")
        print("   seguimos con las mismas credenciales/cuenta sin explicación clara.")
    print("=" * 70)

    input("\nListo. Copiá TODO este texto (click derecho -> Seleccionar todo, "
          "después click derecho -> Copiar) y pegaselo a Claude. Apretá Enter para cerrar...")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
