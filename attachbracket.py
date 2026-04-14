#!/usr/bin/env python3
import json
import os
from datetime import datetime, timezone
from typing import Dict, Any, Optional

import requests

C2_API4_BASE = "https://api4-general.collective2.com"
REQUEST_TIMEOUT = 30

MES_SYMBOL = "@MESM6"
STOP_POINTS = float(os.getenv("STOP_POINTS", "80"))
TARGET_POINTS = float(os.getenv("TARGET_POINTS", "160"))


def api4_get(path: str, apikey: str, params: Dict[str, Any]) -> dict:
    url = f"{C2_API4_BASE}{path}"
    headers = {
        "Authorization": f"Bearer {apikey}",
        "Content-Type": "application/json",
    }
    r = requests.get(url, headers=headers, params=params, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()


def api4_post(path: str, apikey: str, payload: dict) -> dict:
    url = f"{C2_API4_BASE}{path}"
    headers = {
        "Authorization": f"Bearer {apikey}",
        "Content-Type": "application/json",
    }
    r = requests.post(url, headers=headers, json=payload, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()


def get_open_positions(apikey: str, strategy_id: int) -> dict:
    return api4_get(
        "/Strategies/GetStrategyOpenPositions",
        apikey,
        {"StrategyIds": str(strategy_id)},
    )


def get_active_orders(apikey: str, strategy_id: int) -> dict:
    return api4_get(
        "/Strategies/GetStrategyActiveOrders",
        apikey,
        {"StrategyIds": str(strategy_id)},
    )


def find_open_mes_position(open_positions: dict) -> Optional[dict]:
    """
    Your GetStrategyOpenPositions response looks like:
      {
        "Results": [
          {
            "StrategyName": "...",
            "StrategyId": 155560805,
            "Quantity": 1,
            "AvgPx": 7004.25,
            "C2Symbol": {"FullSymbol": "@MESM6", ...}
          }
        ],
        ...
      }
    So each open position is a top-level object in Results.
    """
    results = open_positions.get("Results", [])
    for p in results:
        sym = p.get("C2Symbol", {}).get("FullSymbol")
        qty = p.get("Quantity")
        if sym == MES_SYMBOL and qty and qty != 0:
            return p
    return None


def has_existing_exits(active_orders: dict, parent_symbol: str) -> bool:
    """
    Rough check: do we already have any stop/limit orders for this symbol?
    (Kept for possible future use, but not used in main() now.)
    """
    results = active_orders.get("Results", [])
    for strat in results:
        orders = strat.get("Orders", [])
        for o in orders:
            sym = o.get("C2Symbol", {}).get("FullSymbol")
            if sym != parent_symbol:
                continue
            otype = o.get("OrderType")  # "2"=limit, "3"=stop
            if otype in ("2", "3"):
                return True
    return False


def build_child_stop(strategy_id: int, qty: int, side: str, stop_price: float) -> dict:
    # For a long position, exit is SELL ("2"); for short, BUY ("1")
    exit_side = "2" if side == "long" else "1"
    return {
        "Order": {
            "StrategyId": strategy_id,
            "OrderType": "3",  # stop
            "Side": exit_side,
            "OrderQuantity": abs(qty),
            "TIF": "1",        # GTC
            "Stop": round(stop_price, 2),
            "C2Symbol": {
                "FullSymbol": MES_SYMBOL,
                "SymbolType": "future"
            }
        }
    }


def build_child_target(strategy_id: int, qty: int, side: str, limit_price: float) -> dict:
    exit_side = "2" if side == "long" else "1"
    return {
        "Order": {
            "StrategyId": strategy_id,
            "OrderType": "2",  # limit
            "Side": exit_side,
            "OrderQuantity": abs(qty),
            "TIF": "1",        # GTC
            "Limit": round(limit_price, 2),
            "C2Symbol": {
                "FullSymbol": MES_SYMBOL,
                "SymbolType": "future"
            }
        }
    }


def main():
    apikey = os.getenv("C2_API_KEY", "").strip()
    systemid_raw = os.getenv("C2_SYSTEM_ID", "").strip()
    dry_run = os.getenv("DRY_RUN", "1") == "1"

    if not apikey or not systemid_raw:
        raise RuntimeError("C2_API_KEY and C2_SYSTEM_ID must be set")

    strategy_id = int(systemid_raw)
    now = datetime.now(timezone.utc).isoformat()
    print(f"=== C2 ATTACH BRACKET at {now} UTC ===")
    print(f"StrategyId: {strategy_id}")

    # Always check open positions first
    open_pos = get_open_positions(apikey, strategy_id)
    print("Open positions raw:", json.dumps(open_pos, ensure_ascii=False))

    mes_pos = find_open_mes_position(open_pos)
    if mes_pos:
        qty = mes_pos.get("Quantity")
        entry_price = (
            mes_pos.get("AvgPx")
            or mes_pos.get("AvgEntryPrice")
            or mes_pos.get("EntryPrice")
        )
        print(f"Open MES position detected: Qty={qty}, EntryPrice={entry_price}")
        print("Open position exists → not sending stop or target orders.")
        return

    print("No open MES position detected → no stop or target orders sent.")
    return


if __name__ == "__main__":
    main()