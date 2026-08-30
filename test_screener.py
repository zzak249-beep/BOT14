"""Tests de screener.py: filtrado de mercados/volumen y detección de señales
reutilizando la misma lógica ya validada en strategy.py."""

import pandas as pd
import pytest

import screener as scr
from exchange_client import ExchangeClient


def _fake_markets():
    return {
        "BTC/USDT": {"spot": True, "active": True, "quote": "USDT", "base": "BTC"},
        "ETH/USDT": {"spot": True, "active": True, "quote": "USDT", "base": "ETH"},
        "DEADCOIN/USDT": {"spot": True, "active": True, "quote": "USDT", "base": "DEADCOIN"},
        "SOL/BTC": {"spot": True, "active": True, "quote": "BTC", "base": "SOL"},
        "OLD/USDT": {"spot": True, "active": False, "quote": "USDT", "base": "OLD"},
        "ETHUSDT-SWAP": {"spot": False, "active": True, "quote": "USDT", "base": "ETH"},
    }


def test_filters_by_spot_active_and_quote_and_volume():
    ec = ExchangeClient("", "", demo=False)
    ec._markets_loaded = True
    ec.exchange.markets = _fake_markets()
    ec._with_retries = lambda func, *a, **k: {
        "BTC/USDT": {"quoteVolume": 5_000_000},
        "ETH/USDT": {"quoteVolume": 1_000_000},
        "DEADCOIN/USDT": {"quoteVolume": 500},
    }

    symbols = scr.get_candidate_symbols(ec, quote="USDT", min_volume_usdt=200000)
    assert symbols == ["BTC/USDT", "ETH/USDT"]


def test_no_volume_filter_keeps_all_usdt_spot_pairs():
    ec = ExchangeClient("", "", demo=False)
    ec._markets_loaded = True
    ec.exchange.markets = _fake_markets()

    symbols = scr.get_candidate_symbols(ec, quote="USDT", min_volume_usdt=0)
    # sin filtro de volumen, deben quedar los 3 pares spot/activos cotizados en USDT
    # (DEADCOIN/USDT incluida; OLD/USDT e SOL/BTC quedan fuera por 'active' y 'quote')
    assert set(symbols) == {"BTC/USDT", "ETH/USDT", "DEADCOIN/USDT"}


@pytest.fixture
def synthetic_buy_signal_df():
    raw = pd.read_csv("../bot_dev/synthetic_ohlcv.csv").iloc[:139].reset_index(drop=True)  # indice 138: special_buy=True
    raw["datetime"] = pd.to_datetime(range(len(raw)), unit="s", utc=True)
    return raw


def test_analyze_symbol_detects_known_buy_signal(synthetic_buy_signal_df):
    class FakeArgs:
        timeframe = "15m"
        candles = 500
        rsi_length = 10
        signal_length = 10
        trigger_level = 50.0
        target_cross_count = 2
        atr_period = 10
        st_factor = 2.5
        trade_amount = 100.0
        fee_pct = 0.1

    ec = ExchangeClient("", "", demo=False)
    ec.fetch_ohlcv_df = lambda symbol, timeframe, limit: synthetic_buy_signal_df

    result = scr.analyze_symbol(ec, "BTC/USDT", FakeArgs())
    assert result is not None
    assert result["senal_compra_ahora"] is True
    assert result["symbol"] == "BTC/USDT"
    assert 0 <= result["rsi"] <= 100
