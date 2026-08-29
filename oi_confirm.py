"""
Confirmación por Open Interest (contratos abiertos) — ASIMÉTRICA a
propósito, y NO bloqueante.

EL MARCO (bien establecido, lo usa gente seria como Glassnode en su
métrica LPOC): cruzar la dirección del precio con la del Open Interest
separa cuatro situaciones. Las dos que importan aquí:

  · Precio CAE + OI CAE  -> liquidación forzada de largos. Venta sin
    convicción nueva, el vendedor ya no tiene con qué seguir vendiendo.
  · Precio SUBE + OI CAE -> cobertura de cortos (short squeeze). Compra
    sin convicción nueva, pero la evidencia es que el mercado tiende a
    SEGUIR subiendo después, no a devolverlo.

ASIMETRÍA A PROPÓSITO: por eso una señal BUY (apostar a que un dump
revierte) que coincide con OI cayendo se trata como confirmación
FUERTE — el mismo peso que una cascada de liquidaciones. Una señal
SELL (apostar a que un pump se desinfla) que coincide con OI cayendo
NO se trata como confirmación — ni a favor ni en contra, simplemente
no suma. No es una corazonada: coincide con lo que el propio proyecto
ya midió en su historial (cortos ~79% de acierto, largos a
contra-tendencia ~43%) — dos líneas de evidencia independientes
señalando la misma asimetría.

AVISO DE HONESTIDAD, IMPORTANTE: el número concreto "el cuadrante de
liquidación de largos tiene ventaja estadísticamente validada" viene
de la página de venta de un indicador de pago en el marketplace de
TradingView — no de un estudio independiente, sin metodología ni
tamaño de muestra publicados. El marco general sí es sólido; ese
número concreto NO está verificado de forma independiente. Por eso
esto se implementa como capa de puntuación (score.py) que se GUARDA
por operación (igual que el resto de componentes del score), no como
filtro que bloquee entradas — para poder comprobar con datos propios,
vía stats.buckets_por_score() o un análisis equivalente, si de verdad
aporta algo antes de dejar que decida nada por sí solo. Gana el
backtest, siempre — esto no es la excepción.

QUÉ NO HACE: BingX no da serie histórica de Open Interest en su
endpoint público (solo una foto actual: openInterest/symbol/time), así
que este módulo construye su PROPIO historial corto muestreando en
cada ciclo — mismo patrón que liquidations.py con los streams, que
tampoco traen histórico de fábrica.
"""
from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass

import config


@dataclass
class OiSample:
    ts: float
    oi: float


class OpenInterestTracker:
    """
    Guarda una ventana corta de OI por símbolo y responde una sola
    pregunta: ¿subía o bajaba en los últimos OI_LOOKBACK_MIN minutos?

    None si no hay muestras suficientes para opinar — igual que
    liquidations.cascade_status(), la ausencia de dato NUNCA se trata
    como "confirma" ni "contradice": simplemente no aporta nada esta
    vez, y no bloquea la señal.
    """

    def __init__(self) -> None:
        self._samples: dict[str, deque[OiSample]] = {}

    def _prune(self, symbol: str) -> None:
        dq = self._samples.get(symbol)
        if not dq:
            return
        limite = time.time() - config.OI_LOOKBACK_MIN * 60
        while dq and dq[0].ts < limite:
            dq.popleft()

    def record(self, symbol: str, oi: float | None) -> None:
        if oi is None or oi <= 0:
            return
        dq = self._samples.setdefault(symbol, deque())
        dq.append(OiSample(time.time(), oi))
        self._prune(symbol)

    def direction(self, symbol: str) -> str | None:
        """
        Compara la muestra más reciente con la más antigua DENTRO de la
        ventana. Con solo 1 muestra no hay con qué comparar -> None.
        Un cambio menor al 0.5% se trata como "sin cambio claro" (None):
        el ruido de muestreo no debe leerse como dirección.
        """
        self._prune(symbol)
        dq = self._samples.get(symbol)
        if not dq or len(dq) < 2:
            return None
        primero, ultimo = dq[0].oi, dq[-1].oi
        if primero <= 0:
            return None
        cambio_pct = (ultimo - primero) / primero * 100.0
        if abs(cambio_pct) < 0.5:
            return None
        return "subiendo" if cambio_pct > 0 else "bajando"


def confirms_buy(oi_dir: str | None) -> bool:
    """
    Señal BUY = fading un dump. Confirma FUERTE si el dump vino con OI
    bajando (liquidación de largos, no venta con convicción nueva).
    """
    return oi_dir == "bajando"


def sell_is_unsupported(oi_dir: str | None) -> bool:
    """
    Señal SELL = fading un pump. Si el pump vino con OI bajando
    (cobertura de cortos), la evidencia dice que NO conviene tratarlo
    como confirmación — el mercado tiende a seguir subiendo. Esto NO
    bloquea la señal (el número no está verificado de forma
    independiente para tratarlo como bloqueo), solo evita sumarle
    puntos de confirmación que sí se le darían a un BUY en la misma
    situación. La asimetría vive aquí, en qué SUMA, no en qué bloquea.
    """
    return oi_dir == "bajando"
