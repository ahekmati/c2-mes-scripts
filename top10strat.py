#!/usr/bin/env python3
import json
import os
import re
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any

import requests
from bs4 import BeautifulSoup


AMP_URL = "https://ampfutures.isystems.com/Systems/TopStrategies"
C2_API4_BASE = "https://api4-general.collective2.com"
REQUEST_TIMEOUT = 30

TOP_N = int(os.getenv("TOP_N", "10"))
MES_SYMBOL = "@MESM6"


@dataclass
class ScrapedRow:
    rank: int
    system: str
    product: str
    pnl: float
    current_position: str
    nearest_order: str
    developer: str = ""


@dataclass
class ParsedPosition:
    side: str
    qty: int


def fetch_amp_html() -> str:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        )
    }
    r = requests.get(AMP_URL, headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.text


def money_to_float(s: str) -> Optional[float]:
    s = s.replace("$", "").replace(",", "").strip()
    try:
        return float(s)
    except Exception:
        return None


def parse_current_session(html: str) -> List[ScrapedRow]:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", id="tableCurrentSession")
    if not table:
        raise RuntimeError("Could not find tableCurrentSession in AMP HTML")
    body = table.find("tbody")
    if not body:
        raise RuntimeError("tableCurrentSession has no tbody")

    rows: List[ScrapedRow] = []
    for tr in body.find_all("tr"):
        rank_td = None
        system_td = tr.find("td", id=re.compile(r"^rankID_"))
        product_td = tr.find("td", id=re.compile(r"^rankProduct_"))
        developer_td = tr.find("td", id=re.compile(r"^rankDeveloperName_"))
        pnl_td = tr.find("td", id=re.compile(r"^rankNetResult_"))
        pos_td = tr.find("td", id=re.compile(r"^rankCurrentPosition_"))
        nearest_td = tr.find("td", id=re.compile(r"^rankClosestOrder_"))
        all_tds = tr.find_all("td")
        if len(all_tds) >= 2:
            rank_td = all_tds[1]

        if not all([rank_td, system_td, product_td, pnl_td, pos_td, nearest_td]):
            continue

        m = re.search(r"#(\d+)", rank_td.get_text(" ", strip=True))
        if not m:
            continue

        rank = int(m.group(1))
        if rank > TOP_N:
            continue

        pnl = money_to_float(pnl_td.get_text(" ", strip=True))
        if pnl is None:
            continue

        rows.append(
            ScrapedRow(
                rank=rank,
                system=system_td.get_text(" ", strip=True),
                product=product_td.get_text(" ", strip=True).upper(),
                pnl=pnl,
                current_position=pos_td.get_text(" ", strip=True),
                nearest_order=nearest_td.get_text(" ", strip=True),
                developer=developer_td.get_text(" ", strip=True) if developer_td else "",
            )
        )
    return rows


def pick_best_es(rows: List[ScrapedRow]) -> Optional[ScrapedRow]:
    es_rows = [r for r in rows if r.product == "ES"]
    if not es_rows:
        return None
    es_rows.sort(key=lambda x: (x.rank, -x.pnl))
    return es_rows[0]


def parse_direction_and_size(text: str) -> Optional[ParsedPosition]:
    """
    Example: 'Long 1 @ 6807.50' → side='long', qty=1
    """
    text = text.strip()
    if text in {"", "--", "-"}:
        return None
    m = re.match(r"^(Long|Short)\s+(\d+)\s*@", text, flags=re.I)
    if not m:
        return None
    side = m.group(1).lower()
    qty = int(m.group(2))
    return ParsedPosition(side=side, qty=qty)


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


def find_open_mes_position(open_positions: dict) -> Optional[dict]:
    """
    Actual response shape:
    {
      "Results": [
        {
          "StrategyId": 155560805,
          "Quantity": 1,
          "AvgPx": 7004.25,
          "C2Symbol": {"FullSymbol": "@MESM6", ...}
        }
      ],
      "ResponseStatus": {"ErrorCode": "200"}
    }
    """
    results = open_positions.get("Results", [])
    for p in results:
        sym = p.get("C2Symbol", {}).get("FullSymbol")
        qty = p.get("Quantity")
        if sym == MES_SYMBOL and qty and qty != 0:
            return p
    return None


def build_market_entry(systemid: int, pos: ParsedPosition, mes_symbol: str) -> dict:
    side_code = "1" if pos.side == "long" else "2"
    return {
        "Order": {
            "StrategyId": systemid,
            "OrderType": "1",   # market
            "Side": side_code,
            "OrderQuantity": pos.qty,
            "TIF": "0",         # day
            "C2Symbol": {
                "FullSymbol": mes_symbol,
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

    systemid = int(systemid_raw)
    now = datetime.now(timezone.utc).isoformat()
    print(f"=== C2 ENTRY at {now} UTC ===")
    print(f"StrategyId: {systemid}")

    # 1) Check for existing MES position first
    open_pos = get_open_positions(apikey, systemid)
    print("Open positions raw:", json.dumps(open_pos, ensure_ascii=False))

    mes_pos = find_open_mes_position(open_pos)
    if mes_pos:
        qty = mes_pos.get("Quantity")
        entry = (
            mes_pos.get("AvgPx")
            or mes_pos.get("AvgEntryPrice")
            or mes_pos.get("EntryPrice")
        )
        side = "long" if qty and qty > 0 else "short"
        print(f"Existing MES position detected: Qty={qty}, Side={side}, Entry={entry}")
        print("Open position exists → not sending another entry order.")
        return

    # 2) Scrape AMP for ES direction/size
    html = fetch_amp_html()
    rows = parse_current_session(html)
    if not rows:
        raise RuntimeError("No rows parsed from AMP current session table")

    best_es = pick_best_es(rows)
    if not best_es:
        raise RuntimeError("No ES strategy found in top rows")

    parsed_pos = parse_direction_and_size(best_es.current_position)
    if not parsed_pos:
        raise RuntimeError(f"ES current position not parsable: {best_es.current_position}")

    print("Selected ES strategy:", json.dumps(asdict(best_es), ensure_ascii=False))
    print("Parsed direction/size:", json.dumps(asdict(parsed_pos), ensure_ascii=False))
    print("MES symbol:", MES_SYMBOL)

    # 3) Build and (optionally) send market entry
    entry_payload = build_market_entry(systemid, parsed_pos, MES_SYMBOL)
    print("Entry payload:", json.dumps(entry_payload, ensure_ascii=False))

    if dry_run:
        print("DRY_RUN=1 → not sending entry order.")
        return

    result = api4_post("/Strategies/NewStrategyOrder", apikey, entry_payload)
    print("Entry result:", json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()