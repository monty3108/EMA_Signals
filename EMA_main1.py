#!/usr/bin/env python3
# EMA_main.py
# Single-file portfolio manager (consolidation + sell PnL + dividends + reports)
# Updated: cleaned groupby.apply/iterrows usages and re-added get_session_id integration.
from __future__ import annotations

import os
import sys
import csv
import math
import json
import datetime as dt
from typing import Dict, List, Optional

import pandas as pd

# Optional local modules (safe imports; if missing we degrade gracefully)
try:
    import config  # your config.py (contains alice, telegram settings, etc.)
except Exception:
    class _ConfigFallback:
        alice = None
        telegram_notification = False
        print_logger = True
    config = _ConfigFallback()

try:
    import Alice_Module  # your existing Alice_Module.py that defines get_session_id / session_id_generate
except Exception:
    Alice_Module = None

try:
    from Notification_Module import send_docs  # optional Telegram/send file utility
except Exception:
    def send_docs(files: List[str]):
        print("[Notification disabled] would send:", files)


# ---------------------- Constants / File names ----------------------
DATE_FMT = "%d %b %Y"         # '04 Sep 2025'
TODAY_STR = dt.datetime.now().strftime("%d-%b-%Y")
POSITIONS_CSV = "positions.csv"
DIVIDENDS_CSV = "dividends.csv"
CONSOLIDATED_CSV = "consolidated.csv"
REALIZED_PNL_CSV = "realized_pnl.csv"
PKL_DIR = "pkl_obj"


# ---------------------- Utilities ----------------------
def print_android(s: str) -> None:
    """Small formatted print used across the program."""
    print("    " + s)


def ensure_dirs_and_files() -> None:
    """Ensure CSV files and data dir exist and have expected headers."""
    os.makedirs(PKL_DIR, exist_ok=True)

    if not os.path.exists(POSITIONS_CSV):
        with open(POSITIONS_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["date", "stock_name", "qty", "price", "demat", "notes", "type"])

    if not os.path.exists(DIVIDENDS_CSV):
        with open(DIVIDENDS_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["date", "stock_name", "amount", "notes"])

    if not os.path.exists(REALIZED_PNL_CSV):
        with open(REALIZED_PNL_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["date", "stock_name", "qty_sold", "sell_price", "avg_cost", "realized_pnl", "notes"])


def parse_date(s: str) -> Optional[dt.date]:
    try:
        return dt.datetime.strptime(s, DATE_FMT).date()
    except Exception:
        return None


def date_to_str(d: dt.date) -> str:
    return d.strftime(DATE_FMT)


def df_to_text(path: str, df: pd.DataFrame) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        if df is None or df.empty:
            f.write("No data\n")
        else:
            f.write(df.to_string(index=False))


# ---------------------- Alice / Session helpers ----------------------
def get_session_id(force_generate: bool = False) -> Optional[object]:
    """
    Ensure there is an active session in config.alice.
    Preferred: call Alice_Module.get_session_id() if available (this is how the original code worked).
    If not present, attempt to call Alice_Module.session_id_generate() if available.
    If neither is available, do nothing (LTP fetching will gracefully fallback).
    Returns: config.alice object (if set) or None.
    """
    try:
        # If config already has an alice object and not forcing, return
        alice_obj = getattr(config, "alice", None)
        if alice_obj is not None and not force_generate:
            return alice_obj

        # Try to use Alice_Module provided utilities (original repo had get_session_id/session_id_generate)
        if Alice_Module is not None:
            if hasattr(Alice_Module, "get_session_id"):
                try:
                    Alice_Module.get_session_id()
                    return getattr(config, "alice", None)
                except Exception:
                    # fallback to generator
                    pass

            if hasattr(Alice_Module, "session_id_generate"):
                try:
                    Alice_Module.session_id_generate()
                    return getattr(config, "alice", None)
                except Exception:
                    pass

        # No alice generator found; return whatever is in config (may be None)
        return getattr(config, "alice", None)
    except Exception as e:
        print_android(f"get_session_id: error {e}")
        return getattr(config, "alice", None)


def get_ltp(symbol: str) -> Optional[float]:
    """
    Try to fetch LTP using config.alice (after ensuring session).
    If LTP unavailable or API not configured, return None.
    """
    try:
        alice = getattr(config, "alice", None)
        if alice is None:
            # attempt to create session once
            alice = get_session_id()
            if alice is None:
                return None

        # attempt typical interfaces; adapt if your alice client has a different method
        if hasattr(alice, "get_ltp"):
            try:
                # some clients expose get_ltp(symbol)
                return float(alice.get_ltp(symbol))
            except Exception:
                pass

        # Many AliceBlue wrappers use `get_scrip_ltp` or instrument/quote patterns. Try common patterns:
        if hasattr(alice, "get_scrip_info") and hasattr(alice, "get_instrument_by_symbol"):
            try:
                inst = alice.get_instrument_by_symbol("NSE", symbol)
                info = alice.get_scrip_info(inst)
                ltp = float(info.get("Ltp", 0.0) or 0.0)
                return ltp
            except Exception:
                pass

        # fallback None
        return None
    except Exception:
        return None


# ---------------------- Core: Transactions & consolidation ----------------------
def add_transaction(date: str,
                    stock: str,
                    qty: int,
                    price: float,
                    demat: str = "",
                    notes: str = "",
                    txn_type: Optional[str] = None) -> None:
    """
    Add a transaction row to positions.csv.
      - txn_type optional: 'BUY' or 'SELL'. If omitted, inferred from qty sign or positive qty => BUY.
      - For SELL we record qty as negative in CSV (so legacy files are compatible).
    After adding, the function will (a) compute realized P&L if SELL and (b) rebuild consolidated.csv.
    """
    ensure_dirs_and_files = ensure_dirs_and_files  # hint for linter
    ensure_dirs_and_files()
    stock = stock.strip().upper()
    txn_type = (txn_type or "").strip().upper()
    if not txn_type:
        txn_type = "BUY" if qty > 0 else "SELL" if qty < 0 else "BUY"
    signed_qty = qty if txn_type == "BUY" else -abs(qty)

    # Prepare row
    row = {
        "date": date,
        "stock_name": stock,
        "qty": int(signed_qty),
        "price": float(price),
        "demat": demat,
        "notes": notes,
        "type": txn_type
    }
    df_row = pd.DataFrame([row])

    header_needed = not os.path.exists(POSITIONS_CSV) or os.path.getsize(POSITIONS_CSV) == 0
    df_row.to_csv(POSITIONS_CSV, mode="a", header=header_needed, index=False)

    # If SELL: compute realized PnL (based on weighted avg cost of buys up to sell date)
    if txn_type == "SELL":
        realized = compute_realized_pnl_on_sell(stock=stock,
                                                qty_sold=abs(signed_qty),
                                                sell_price=float(price),
                                                notes=notes,
                                                date=date)
        if realized is not None:
            hdr = not os.path.exists(REALIZED_PNL_CSV) or os.path.getsize(REALIZED_PNL_CSV) == 0
            pd.DataFrame([realized]).to_csv(REALIZED_PNL_CSV, mode="a", header=hdr, index=False)

    # Rebuild consolidated file
    consolidate_positions()


def compute_realized_pnl_on_sell(stock: str, qty_sold: int, sell_price: float,
                                 notes: str = "", date: Optional[str] = None) -> Optional[Dict]:
    """
    Compute realized P&L for a SELL using weighted-average cost of BUY legs BEFORE sell.
    Returns dict suitable for appending to realized_pnl.csv or None if no buys available.
    """
    ensure_dirs_and_files()
    try:
        df = pd.read_csv(POSITIONS_CSV)
    except Exception:
        return None

    # Normalize stock name
    stock = stock.strip().upper()
    if "stock_name" not in df.columns:
        return None

    # Filter rows for this stock
    df_stock = df[df["stock_name"].str.upper() == stock].copy()
    if df_stock.empty:
        return None

    # If a 'date' is provided for this sell, keep only rows <= that date (so same-day buys count)
    if date:
        d_obj = parse_date(date)
        if d_obj:
            df_stock["_parsed"] = pd.to_datetime(df_stock["date"], format=DATE_FMT, errors="coerce")
            df_stock = df_stock[df_stock["_parsed"].dt.date <= d_obj]

    # Consider only BUY legs (qty > 0)
    buys = df_stock[df_stock["qty"] > 0].copy()
    total_qty_before = buys["qty"].sum()
    if total_qty_before <= 0:
        # nothing to match; no realized PnL
        return None

    total_value_before = (buys["qty"] * buys["price"]).sum()
    avg_cost = total_value_before / total_qty_before

    eff_qty = min(int(total_qty_before), int(qty_sold))
    realized_pnl = (sell_price - avg_cost) * eff_qty

    return {
        "date": date or dt.datetime.now().strftime(DATE_FMT),
        "stock_name": stock,
        "qty_sold": eff_qty,
        "sell_price": float(sell_price),
        "avg_cost": float(round(avg_cost, 4)),
        "realized_pnl": float(round(realized_pnl, 2)),
        "notes": notes
    }


def consolidate_positions() -> pd.DataFrame:
    """
    Build consolidated.csv with columns:
      stock_name, total_qty, avg_price, total_value_sum, transactions_detail
    - Works with positions.csv that may or may not have 'type' column (we infer it).
    - Uses vectorized groupby / agg, NO groupby.apply on grouping columns and NO iterrows.
    """
    ensure_dirs_and_files()
    try:
        df = pd.read_csv(POSITIONS_CSV)
    except Exception:
        # write empty consolidated file and return empty DF
        pd.DataFrame(columns=["stock_name", "total_qty", "avg_price", "total_value_sum", "transactions_detail"]).to_csv(CONSOLIDATED_CSV, index=False)
        return pd.DataFrame(columns=["stock_name", "total_qty", "avg_price", "total_value_sum", "transactions_detail"])

    if df.empty:
        pd.DataFrame(columns=["stock_name", "total_qty", "avg_price", "total_value_sum", "transactions_detail"]).to_csv(CONSOLIDATED_CSV, index=False)
        return pd.DataFrame(columns=["stock_name", "total_qty", "avg_price", "total_value_sum", "transactions_detail"])

    # Normalize column names
    df.columns = [c.strip() for c in df.columns]

    # Ensure basic columns exist
    for c in ("date", "stock_name", "qty", "price"):
        if c not in df.columns:
            raise RuntimeError(f"Missing required column '{c}' in {POSITIONS_CSV}")

    # 1) Derive 'type' if missing (BUY if qty > 0 else SELL)
    if "type" not in df.columns:
        df["type"] = df["qty"].apply(lambda q: "BUY" if q > 0 else "SELL")

    # 2) Ensure date is string in desired format (leave as stored)
    df["date"] = df["date"].astype(str)

    # 3) Create a compact per-row transaction string (vectorized)
    df["txn_str"] = df["date"].astype(str) + ":" + df["type"].astype(str) + ":" + df["qty"].astype(str) + "@" + df["price"].astype(str)

    # 4) total_qty (incl. sells as negative)
    grp_qty = df.groupby("stock_name", as_index=False)["qty"].sum().rename(columns={"qty": "total_qty"})

    # 5) avg_price & total_value from buys only (weighted avg of buy legs)
    buys = df[df["qty"] > 0].copy()
    if not buys.empty:
        buys["contrib"] = buys["qty"] * buys["price"]
        grp_buys = buys.groupby("stock_name", as_index=False).agg(total_value_sum=("contrib", "sum"), total_qty_buy=("qty", "sum"))
    else:
        # empty DataFrame with expected columns
        grp_buys = pd.DataFrame(columns=["stock_name", "total_value_sum", "total_qty_buy"])

    merged = pd.merge(grp_qty, grp_buys, on="stock_name", how="left").fillna(0.0)
    merged["avg_price"] = merged.apply(lambda r: (r["total_value_sum"] / r["total_qty_buy"]) if r["total_qty_buy"] > 0 else 0.0, axis=1)

    # Current holding value (avg_price * total_qty)
    merged["total_value_sum"] = merged["avg_price"] * merged["total_qty"]

    # 6) transactions_detail: join txn_str per stock (safe agg)
    details = df.groupby("stock_name", group_keys=False)["txn_str"].agg(" | ".join).reset_index().rename(columns={"txn_str": "transactions_detail"})

    merged = pd.merge(merged, details, on="stock_name", how="left")
    merged = merged[["stock_name", "total_qty", "avg_price", "total_value_sum", "transactions_detail"]]
    merged.sort_values(by="stock_name", inplace=True)
    merged.to_csv(CONSOLIDATED_CSV, index=False)
    return merged


# ---------------------- Views / Reports (no iterrows or groupby.apply) ----------------------
def view_position_status() -> pd.DataFrame:
    """
    Build a holdings DataFrame showing current LTP (if available), value and gain.
    Returns empty DataFrame if no holdings.
    """
    cons = consolidate_positions()
    if cons.empty:
        return pd.DataFrame(columns=["index", "stock", "qty", "avg_price", "ltp", "value", "gain", "percent"])

    stocks = cons["stock_name"].tolist()
    qtys = cons["total_qty"].astype(int).tolist()
    avgs = cons["avg_price"].astype(float).tolist()

    rows = []
    for stock, qty, avg in zip(stocks, qtys, avgs):
        if qty == 0:
            continue
        # attempt to fetch LTP (ensure session available)
        ltp = get_ltp(stock)
        if ltp is None:
            ltp = avg  # fallback so view still meaningful
        value = qty * ltp
        gain = (ltp - avg) * qty
        percent = (gain / (avg * qty) * 100.0) if (avg * qty) != 0 else 0.0
        rows.append({
            "stock": stock,
            "qty": qty,
            "avg_price": round(avg, 2),
            "ltp": round(ltp, 2),
            "value": round(value, 2),
            "gain": round(gain, 2),
            "percent": round(percent, 2)
        })

    if not rows:
        return pd.DataFrame(columns=["index", "stock", "qty", "avg_price", "ltp", "value", "gain", "percent"])

    df = pd.DataFrame(rows)
    df = df.sort_values(by=["percent", "stock"], ascending=[False, True])
    df.insert(0, "index", range(1, len(df) + 1))
    return df


def view_pnl() -> pd.DataFrame:
    """Alias of view_position_status for compatibility (keeps interface)."""
    return view_position_status()


def view_stock_transactions() -> pd.DataFrame:
    """
    Return all transactions with holding age for BUY rows (vectorized).
    """
    ensure_dirs_and_files()
    try:
        df = pd.read_csv(POSITIONS_CSV)
    except Exception:
        return pd.DataFrame(columns=["index", "date", "stock", "type", "qty", "price", "demat", "notes", "holding_age"])

    if df.empty:
        return pd.DataFrame(columns=["index", "date", "stock", "type", "qty", "price", "demat", "notes", "holding_age"])

    # Normalize columns & types
    df["date_parsed"] = pd.to_datetime(df["date"], format=DATE_FMT, errors="coerce")
    if "type" not in df.columns:
        df["type"] = df["qty"].apply(lambda q: "BUY" if q > 0 else "SELL")

    # Compute holding_age for BUY rows
    today = pd.to_datetime(dt.date.today())
    df["holding_age_days"] = (today - df["date_parsed"]).dt.days.clip(lower=0)
    df["holding_age"] = df.apply(lambda r: f"{r['holding_age_days']}d" if r["type"] == "BUY" else "", axis=1)

    # sort & format
    df = df.sort_values(by=["stock_name", "date_parsed"], ascending=[True, True])
    df["date"] = df["date_parsed"].dt.strftime("%d-%b-%Y")
    df = df.rename(columns={"stock_name": "stock"})
    df.insert(0, "index", range(1, len(df) + 1))
    return df[["index", "date", "stock", "type", "qty", "price", "demat", "notes", "holding_age"]]


def view_monthly_investment() -> pd.DataFrame:
    """
    Monthly total invested (BUYS only).
    """
    ensure_dirs_and_files()
    try:
        df = pd.read_csv(POSITIONS_CSV)
    except Exception:
        return pd.DataFrame(columns=["Month", "Monthly Investment"])

    if df.empty:
        return pd.DataFrame(columns=["Month", "Monthly Investment"])

    df["date"] = pd.to_datetime(df["date"], format=DATE_FMT, errors="coerce")
    buys = df[df["qty"] > 0].copy()
    if buys.empty:
        return pd.DataFrame(columns=["Month", "Monthly Investment"])

    buys["invested"] = buys["qty"] * buys["price"]
    buys["Month"] = buys["date"].dt.to_period("M").astype(str)
    monthly = buys.groupby("Month", as_index=False)["invested"].sum().rename(columns={"invested": "Monthly Investment"})
    return monthly


# ---------------------- Dividends ----------------------
def add_dividend(date: str, stock: str, amount: float, notes: str = "") -> None:
    ensure_dirs_and_files()
    df = pd.DataFrame([{
        "date": date,
        "stock_name": stock.strip().upper(),
        "amount": float(amount),
        "notes": notes
    }])
    hdr = not os.path.exists(DIVIDENDS_CSV) or os.path.getsize(DIVIDENDS_CSV) == 0
    df.to_csv(DIVIDENDS_CSV, mode="a", header=hdr, index=False)


def dividend_report() -> pd.DataFrame:
    ensure_dirs_and_files()
    try:
        df = pd.read_csv(DIVIDENDS_CSV)
    except Exception:
        return pd.DataFrame(columns=["Year", "Stock", "Total Dividend"])

    if df.empty:
        return pd.DataFrame(columns=["Year", "Stock", "Total Dividend"])

    df["date"] = pd.to_datetime(df["date"], format=DATE_FMT, errors="coerce")
    df["Year"] = df["date"].dt.year
    rep = df.groupby(["Year", "stock_name"], as_index=False)["amount"].sum()
    rep = rep.rename(columns={"stock_name": "Stock", "amount": "Total Dividend"})
    return rep.sort_values(["Year", "Stock"]).reset_index(drop=True)


# ---------------------- Reports (write + optionally send) ----------------------
def send_positions_report() -> None:
    """Write holdings and pnl files and send via Notification_Module.send_docs if available."""
    os.makedirs(PKL_DIR, exist_ok=True)
    h_df = view_position_status()
    p_df = view_pnl()
    h_path = os.path.join(PKL_DIR, f"Holdings_{TODAY_STR}.txt")
    p_path = os.path.join(PKL_DIR, f"PnL_{TODAY_STR}.txt")
    df_to_text(h_path, h_df)
    df_to_text(p_path, p_df)
    try:
        send_docs([h_path, p_path])
    finally:
        if os.path.exists(h_path):
            os.remove(h_path)
        if os.path.exists(p_path):
            os.remove(p_path)


def send_all_transactions() -> None:
    df = view_stock_transactions()
    path = os.path.join(PKL_DIR, f"All_Trans_{TODAY_STR}.txt")
    df_to_text(path, df)
    try:
        send_docs([path])
    finally:
        if os.path.exists(path):
            os.remove(path)


def send_monthly_investment() -> None:
    df = view_monthly_investment()
    path = os.path.join(PKL_DIR, f"Inv_Monthly_{TODAY_STR}.txt")
    df_to_text(path, df)
    try:
        send_docs([path])
    finally:
        if os.path.exists(path):
            os.remove(path)


def send_dividend_summary() -> None:
    df = dividend_report()
    path = os.path.join(PKL_DIR, f"Dividends_{TODAY_STR}.txt")
    df_to_text(path, df)
    try:
        send_docs([path])
    finally:
        if os.path.exists(path):
            os.remove(path)


# ---------------------- CLI helpers ----------------------
def prompt_date(default_today: bool = True) -> str:
    if default_today:
        dflt = dt.datetime.now().strftime(DATE_FMT)
        s = input(f"Date [{dflt}]: ").strip()
        return s or dflt
    return input("Date (dd Mmm YYYY): ").strip()


def cli_buy() -> None:
    print_android("BUY ---")
    date = prompt_date()
    stock = input("Stock (symbol): ").strip().upper()
    qty = int(input("Qty: ").strip())
    price = float(input("Price: ").strip())
    demat = input("Demat: ").strip()
    notes = input("Notes (optional): ").strip()
    add_transaction(date, stock, qty, price, demat, notes, "BUY")
    print_android("Buy recorded & consolidated.")


def cli_sell() -> None:
    print_android("SELL ---")
    date = prompt_date()
    stock = input("Stock (symbol): ").strip().upper()
    qty = int(input("Qty to SELL: ").strip())
    price = float(input("Sell Price: ").strip())
    demat = input("Demat: ").strip()
    notes = input("Notes (optional): ").strip()
    add_transaction(date, stock, qty, price, demat, notes, "SELL")
    print_android("Sell recorded; realized PnL computed & consolidated.")


def cli_dividend() -> None:
    print_android("DIVIDEND ---")
    date = prompt_date()
    stock = input("Stock (symbol): ").strip().upper()
    amount = float(input("Amount: ").strip())
    notes = input("Notes (optional): ").strip()
    add_dividend(date, stock, amount, notes)
    print_android("Dividend recorded.")


# ---------------------- Menu ----------------------
def cli_menu() -> None:
    ensure_dirs_and_files()
    actions = {
        "1": ("Add BUY", cli_buy),
        "2": ("Add SELL", cli_sell),
        "3": ("Add Dividend", cli_dividend),
        "4": ("Show Holdings", lambda: print(view_position_status().to_string(index=False))),
        "5": ("Show P&L", lambda: print(view_pnl().to_string(index=False))),
        "6": ("Show Transactions", lambda: print(view_stock_transactions().to_string(index=False))),
        "7": ("Show Monthly Invest", lambda: print(view_monthly_investment().to_string(index=False))),
        "8": ("Show Dividends", lambda: print(dividend_report().to_string(index=False))),
        "9": ("Send Holdings & PnL report", send_positions_report),
        "10": ("Send All Transactions report", send_all_transactions),
        "11": ("Send Monthly Invest report", send_monthly_investment),
        "12": ("Send Dividend Summary", send_dividend_summary),
        "13": ("Rebuild Consolidated", lambda: (consolidate_positions(), print_android("Consolidated"))),
        "0": ("Exit", None),
    }
    while True:
        print("\n=== Portfolio Manager Menu ===")
        for k in sorted(actions, key=lambda x: int(x) if x.isdigit() else 999):
            print(f" {k}. {actions[k][0]}")
        ch = input("Choose: ").strip()
        if ch == "0":
            print_android("Exiting...")
            break
        action = actions.get(ch)
        if action and action[1]:
            try:
                action[1]()
            except Exception as e:
                print_android(f"Error: {e}")
        else:
            print_android("Invalid choice.")


# ---------------------- Entrypoint ----------------------
if __name__ == "__main__":
    # default: show interactive menu
    cli_menu()
