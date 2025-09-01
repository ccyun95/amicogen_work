import os
import pandas as pd
from pykrx import stock
from src.common import kst_dates_1y, normalize_numeric

TICKER = os.getenv("TICKER", "092040")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "data")

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    start, end = kst_dates_1y()

    # df: index=날짜, columns=[시가, 고가, 저가, 종가, 거래량, 거래대금, 등락률]
    df = stock.get_market_ohlcv(start, end, TICKER)
    if df.index.name is not None:
        df = df.reset_index()

    # 컬럼 표준화(샘플 파일과 이름 맞추기; 필요 시 여기에서 이름 조정)
    # 샘플을 기준으로 아래 순서를 권장: 일자, 시가, 고가, 저가, 종가, 거래량, 거래대금, 등락률
    df = df.rename(columns={"날짜": "일자"})

    if "일자" not in df.columns:
        # 일부 버전은 인덱스가 datetime일 수 있음
        df["일자"] = pd.to_datetime(df.iloc[:, 0]).dt.strftime("%Y-%m-%d")
        # 나머지 컬럼명은 그대로 둠
    else:
        df["일자"] = pd.to_datetime(df["일자"]).dt.strftime("%Y-%m-%d")

    # 숫자 정규화 (거래량/거래대금은 정수, 시가/고가/저가/종가는 정수/원단위 기준)
    numeric_cols = [c for c in ["시가","고가","저가","종가","거래량","거래대금"] if c in df.columns]
    df = normalize_numeric(df, numeric_cols)

    # 등락률이 실수(%)로 오면 소수 보존 (여기선 그대로 둡니다)
    # 최신이 위로
    df = df.sort_values("일자", ascending=False)

    ordered = [c for c in ["일자","시가","고가","저가","종가","거래량","거래대금","등락률"] if c in df.columns]
    df = df[ordered]

    df.to_csv(os.path.join(OUTPUT_DIR, "latest_market_ohlcv.csv"),
              index=False, encoding="utf-8-sig")

if __name__ == "__main__":
    main()

