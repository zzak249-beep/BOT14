"""
Diario de operaciones reales.

POR QUÉ ESTO IMPORTA MÁS QUE OTRA ESTRATEGIA
Cuando el bot empiece a operar de verdad, la pregunta que va a decidir
todo no es "¿cuánto gané?" sino "¿en qué se diferencia lo real de lo
que decía el backtest?". Esa diferencia —deslizamiento, órdenes que no
se llenan, salidas peores— es el número más caro de conseguir y el más
valioso que vas a tener. Solo se obtiene arriesgando dinero, así que
conviene no desperdiciar ni una operación sin registrarla.

Los contadores del estado (wins, losses) no sirven para eso: dicen
cuántas ganaste, no CÓMO. Aquí se guarda cada operación con el precio
que el bot esperaba y el que realmente hubo.

Formato CSV, en el volumen. Se abre con cualquier hoja de cálculo y no
depende de que el bot siga vivo para leerlo.
"""
from __future__ import annotations

import csv
import datetime as dt
import logging
import os
from pathlib import Path

log = logging.getLogger("journal")

COLUMNAS = [
    "fecha_utc", "symbol", "lado", "modo",
    "entrada_esperada", "entrada_real", "deslizamiento_pct",
    "sl", "tp", "qty", "riesgo_pct",
    "atr_pct", "er_corto", "er_largo", "estiron",
    "coste_estimado_r",
    "salida", "precio_salida", "r_real", "minutos",
]


class Journal:
    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self._asegurar()

    def _asegurar(self) -> None:
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            if not self.path.exists():
                with self.path.open("w", newline="") as f:
                    csv.writer(f).writerow(COLUMNAS)
        except Exception as exc:  # noqa: BLE001
            log.error("No se pudo crear el diario: %s", exc)

    def abrir(self, sig, qty: float, modo: str, entrada_real: float | None = None) -> None:
        """
        Registra la apertura. entrada_real solo se conoce después del
        fill; si no llega, se deja vacío y el deslizamiento se calcula
        al cerrar con lo que haya.
        """
        try:
            desliz = ""
            if entrada_real and sig.entry:
                desliz = f"{(entrada_real - sig.entry) / sig.entry * 100:.4f}"
            riesgo_pct = abs(sig.entry - sig.sl) / sig.entry * 100.0 if sig.entry else 0.0
            fila = {
                "fecha_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
                "symbol": sig.symbol,
                "lado": sig.side,
                "modo": modo,
                "entrada_esperada": f"{sig.entry:.10g}",
                "entrada_real": f"{entrada_real:.10g}" if entrada_real else "",
                "deslizamiento_pct": desliz,
                "sl": f"{sig.sl:.10g}",
                "tp": f"{sig.tp:.10g}" if getattr(sig, "tp", None) else "",
                "qty": f"{qty:.10g}",
                "riesgo_pct": f"{riesgo_pct:.3f}",
                "atr_pct": f"{getattr(sig, 'atr_pct', 0):.3f}",
                "er_corto": "",
                "er_largo": "",
                "estiron": f"{getattr(sig, 'stretch', 0):.2f}",
                "coste_estimado_r": "",
                "salida": "", "precio_salida": "", "r_real": "", "minutos": "",
            }
            with self.path.open("a", newline="") as f:
                csv.DictWriter(f, COLUMNAS).writerow(fila)
        except Exception as exc:  # noqa: BLE001
            log.error("No se pudo registrar la apertura: %s", exc)

    def cerrar(self, symbol: str, salida: str, precio: float, r_real: float, minutos: int) -> None:
        """
        Completa la ÚLTIMA fila abierta de ese símbolo. Se reescribe el
        archivo entero: son pocas filas y así no hace falta un índice
        que pueda desincronizarse.
        """
        try:
            if not self.path.exists():
                return
            with self.path.open(newline="") as f:
                filas = list(csv.DictReader(f))
            for fila in reversed(filas):
                if fila["symbol"] == symbol and not fila["salida"]:
                    fila["salida"] = salida
                    fila["precio_salida"] = f"{precio:.10g}"
                    fila["r_real"] = f"{r_real:.3f}"
                    fila["minutos"] = str(minutos)
                    break
            with self.path.open("w", newline="") as f:
                w = csv.DictWriter(f, COLUMNAS)
                w.writeheader()
                w.writerows(filas)
        except Exception as exc:  # noqa: BLE001
            log.error("No se pudo registrar el cierre: %s", exc)

    def resumen(self) -> str:
        """Lo que de verdad hay que mirar: real frente a lo esperado."""
        try:
            if not self.path.exists():
                return "Diario vacío."
            with self.path.open(newline="") as f:
                filas = [x for x in csv.DictReader(f) if x.get("r_real")]
            if not filas:
                return "Sin operaciones cerradas en el diario."

            rs = [float(x["r_real"]) for x in filas]
            desliz = [abs(float(x["deslizamiento_pct"])) for x in filas if x.get("deslizamiento_pct")]
            ganadoras = [r for r in rs if r > 0]
            minutos = [int(x["minutos"]) for x in filas if x.get("minutos")]

            texto = (
                f"📓 <b>Diario de operaciones reales</b>\n"
                f"Cerradas: <b>{len(rs)}</b> · aciertos {len(ganadoras)} "
                f"({len(ganadoras)/len(rs)*100:.0f}%)\n"
                f"Expectativa REAL: <b>{sum(rs)/len(rs):+.3f} R</b>\n"
                f"Total: {sum(rs):+.1f} R"
            )
            if desliz:
                texto += f"\nDeslizamiento medio: {sum(desliz)/len(desliz):.3f}%"
            if minutos:
                texto += f"\nDuración media: {sum(minutos)//len(minutos)} min"
            if len(rs) < 20:
                texto += "\n<i>Con menos de 20 operaciones esto todavía no decide nada.</i>"
            else:
                texto += (
                    "\n\n<i>Compara esta expectativa con la del backtest en los "
                    "mismos símbolos: la diferencia es lo que cuesta operar de "
                    "verdad, y es el dato que ningún backtest te puede dar.</i>"
                )
            return texto
        except Exception as exc:  # noqa: BLE001
            return f"No se pudo leer el diario: {exc}"
