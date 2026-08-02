"""
Generate hl_symbol_map.json — the Binance-perp ⇄ Hyperliquid symbol translation table.

Since the switch from spot to USDT-M perpetual futures, the two exchanges name the
same coin differently for high-supply "meme" tokens:

    Hyperliquid : k-prefix means 1000×   →  KPEPE, KSHIB, KBONK, KFLOKI, KLUNC
    Binance perp: 1000-prefix means 1000× →  1000PEPEUSDT, 1000SHIBUSDT, …

so KPEPE ⇄ 1000PEPEUSDT, not KPEPE ⇄ KPEPEUSDT. The k prefix is NOT always a
multiplier, though: KAITO (Kaito) and KAS (Kaspa) are real coin names whose leading
K is literal (→ KAITOUSDT, KASUSDT). A blind prefix rewrite would corrupt those, so
we resolve every candidate against the *actual* live perp symbol set and keep only
matches that exist. For a k-prefixed HL coin we prefer the 1000-form (correct scale)
and fall back to the literal name.

The HL coin list is external to this repo (it comes from the trend_trader meta
snapshot, ``scripts.meta_data.get_hl_coins``). Pass it in via --hl-coins or
--hl-coins-file; each entry is an upper-case bare HL name (BTC, KPEPE, …).

Usage
-----
    python build_symbol_map.py --hl-coins-file hl_coins.txt
    python build_symbol_map.py --hl-coins BTC,ETH,KPEPE,KAITO
"""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

import aiohttp

from downloader import fetch_all_symbols

_MAP_FILE = Path(__file__).parent / "hl_symbol_map.json"


def _candidates(hl_coin: str) -> list[str]:
    """Binance perp symbols to try for an HL coin, most-specific first.

    For a k-prefixed name we try the 1000-form first so the 1000× meme tokens map to
    the scale-matched perp (KPEPE → 1000PEPEUSDT); the literal form is the fallback
    that catches genuinely K-named coins (KAITO → KAITOUSDT).
    """
    cands: list[str] = []
    if hl_coin.startswith("K") and len(hl_coin) > 1:
        cands.append(f"1000{hl_coin[1:]}USDT")
    cands.append(f"{hl_coin}USDT")
    return cands


def build_map(hl_coins: set[str], perp_symbols: set[str]) -> dict:
    """Return {"binance_to_hl": {...}, "hl_to_binance": {...}} for coins present on both."""
    binance_to_hl: dict[str, str] = {}
    hl_to_binance: dict[str, str] = {}
    unmapped: list[str] = []

    for coin in sorted(hl_coins):
        hit = next((c for c in _candidates(coin) if c in perp_symbols), None)
        if hit is None:
            unmapped.append(coin)
            continue
        binance_to_hl[hit] = coin
        hl_to_binance[coin] = hit

    if unmapped:
        # Coins on HL with no scale-matched USDT perp (HL-native like PURR, or 1×-only
        # on Binance like NEIRO vs HL's 1000× KNEIRO). They simply get no Binance feed.
        print(f"unmapped ({len(unmapped)}): {', '.join(unmapped)}")

    return {"binance_to_hl": binance_to_hl, "hl_to_binance": hl_to_binance}


async def _fetch_perps() -> set[str]:
    async with aiohttp.ClientSession() as session:
        return set(await fetch_all_symbols(session))


def _load_hl_coins(args) -> set[str]:
    if args.hl_coins_file:
        raw = Path(args.hl_coins_file).read_text()
        tokens = raw.replace(",", "\n").split()
    elif args.hl_coins:
        tokens = args.hl_coins.split(",")
    else:
        raise SystemExit("provide --hl-coins or --hl-coins-file")
    return {t.strip().upper() for t in tokens if t.strip()}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--hl-coins", help="comma-separated upper-case HL coin names")
    parser.add_argument("--hl-coins-file", help="file of HL coin names (comma/newline separated)")
    parser.add_argument("--out", type=Path, default=_MAP_FILE, help="output json path")
    args = parser.parse_args()

    hl_coins = _load_hl_coins(args)
    perps = asyncio.run(_fetch_perps())
    mapping = build_map(hl_coins, perps)

    args.out.write_text(json.dumps(mapping, indent=2, sort_keys=True) + "\n")
    print(f"wrote {len(mapping['hl_to_binance'])} mappings → {args.out}")


if __name__ == "__main__":
    main()
