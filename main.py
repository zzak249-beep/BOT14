"""
Railway entry point para renewed-love.
Usa runpy para ejecutar el scanner sin depender del nombre exacto del archivo.
"""
import runpy
import os
import sys

# Busca el scanner en orden de prioridad
candidates = [
    "ema9_vwap_scanner",
    "scanner",
    "bot",
]

for name in candidates:
    for ext in [".py", ""]:
        path = f"/app/{name}{ext}"
        if os.path.exists(path if ext else path + ".py"):
            try:
                runpy.run_module(name, run_name="__main__", alter_sys=True)
                sys.exit(0)
            except ImportError:
                continue

# Si nada funciona, log del error
print("ERROR: No se encontró el scanner. Archivos en /app:")
for f in sorted(os.listdir("/app")):
    print(f"  {f}")
sys.exit(1)
