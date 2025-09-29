#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
아미코젠(092040) 1년치 OHLCV + 공매도(거래량/비중) + 공매도잔고(수량/비중) 생성

출력: data/latest_market_ohlcv.csv
스키마:
  일자,시가,고가,저가,종가,거래량,등락률,공매도,공매도비중,공매도잔고,공매도잔고비중

저장 형식: UTF-8(BOM 없음), LF 개행
"""

import os
import sys
import io
import re
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

from pykrx import stock

# -------- 설정 --------
TICKER = os.getenv("TICKER", "092040")          # 아미코젠
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "data")
OUTFILE = os.getenv("OUTFILE", "latest_market_ohlcv.csv")
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "400"))  # 영업일 기준 1년+여유

# -------- 유틸 --------
def _to_num(x):
    if pd.isna(x): return np.nan
    if isinstance(x, (int, float, np.integer, np.floating)):
        return float(x)
    s = str(x).replace(",", "").strip()
    try:
        return float(s)
    except Exception:
        return np.nan

def _normalize_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = df[c].apply(_to_num)
    return df

def _yyyymmdd_range(days: int):
    end = datetime.today()
    start = end - timedelta(days=days)
    return start.strftime("%Y%m%d"), end.strftime("%Y%m%d")

# -------- 메인 로직 --------
def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    s, e = _yyyymmdd_range(LOOKBACK_DAYS)

    # 1) OHLCV
    #   반환: index=날짜, cols=[시가,고가,저가,종가,거래량,거래대금,등락률]
    ohlcv = stock.get_market_ohlcv(s, e, TICKER)
    if ohlcv.index.name is not None:
        ohlcv = ohlcv.reset_index()
    ohlcv = ohlcv.rename(columns={"날짜": "일자"})
    ohlcv["일자"] = pd.to_datetime(ohlcv["일자"]).dt.strftime("%Y-%m-%d")
    ohlcv = _normalize_numeric(ohlcv, ["시가","고가","저가","종가","거래량","거래대금","등락률"])
    base_cols = [c for c in ["일자","시가","고가","저가","종가","거래량","등락률"] if c in ohlcv.columns]
    ohlcv = ohlcv[base_cols].drop_duplicates("일자").sort_values("일자")

    # 2) 공매도 거래량/비중
    #   예시 문서 기준: 컬럼 이름은 '공매도', '매수', '비중'
    sv = stock.get_shorting_volume_by_date(s, e, TICKER)
    if sv is not None and isinstance(sv, pd.DataFrame) and not sv.empty:
        if sv.index.name is not None:
            sv = sv.reset_index()
        sv = sv.rename(columns={c: str(c).strip() for c in sv.columns})
        date_col = "날짜" if "날짜" in sv.columns else sv.columns[0]
        sv = sv[[date_col, "공매도", "비중"]].copy()
        sv.columns = ["일자", "공매도", "공매도비중"]
        sv["일자"] = pd.to_datetime(sv["일자"]).dt.strftime("%Y-%m-%d")
        sv = _normalize_numeric(sv, ["공매도","공매도비중"]).dropna(subset=["일자"])
    else:
        sv = pd.DataFrame(columns=["일자","공매도","공매도비중"])

    # 3) 공매도 잔고/비중
    #   예시 문서 기준: '공매도잔고', '상장주식수', '공매도금액', '시가총액', '비중'
    sb = stock.get_shorting_balance_by_date(s, e, TICKER)
    if sb is not None and isinstance(sb, pd.DataFrame) and not sb.empty:
        if sb.index.name is not None:
            sb = sb.reset_index()
        sb = sb.rename(columns={c: str(c).strip() for c in sb.columns})
        date_col = "날짜" if "날짜" in sb.columns else sb.columns[0]
        sb = sb[[date_col, "공매도잔고", "비중"]].copy()
        sb.columns = ["일자", "공매도잔고", "공매도잔고비중"]
        sb["일자"] = pd.to_datetime(sb["일자"]).dt.strftime("%Y-%m-%d")
        sb = _normalize_numeric(sb, ["공매도잔고","공매도잔고비중"]).dropna(subset=["일자"])
    else:
        sb = pd.DataFrame(columns=["일자","공매도잔고","공매도잔고비중"])

    # 4) 병합(LEFT JOIN by 일자)
    out = ohlcv.merge(sv, on="일자", how="left").merge(sb, on="일자", how="left")

    # 5) 결측 처리(없으면 0으로; 필요 시 NaN 유지 원하시면 주석 처리)
    for c in ["공매도","공매도비중","공매도잔고","공매도잔고비중"]:
        if c in out.columns:
            out[c] = out[c].fillna(0)

    # 6) 저장 (UTF-8, LF, 최신일자→과거 정렬을 원하시면 ascending=False)
    out = out.sort_values("일자", ascending=False)
    outfile = os.path.join(OUTPUT_DIR, OUTFILE)
    out.to_csv(outfile, index=False, encoding="utf-8", lineterminator="\n")
    print(f"[OK] saved: {outfile}")
    print(f"cols = {list(out.columns)}")
    print(f"rows = {len(out)} (from {out['일자'].min()} to {out['일자'].max()})")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(1)



