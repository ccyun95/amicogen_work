import os
import pandas as pd
from pykrx import stock
from common import kst_dates_1y, KST, normalize_numeric

TICKER = os.getenv("TICKER", "092040")         # 아미코젠
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "data")
INCLUDE_ETC_CORP = os.getenv("INCLUDE_ETC_CORP", "0") == "1"  # 기관합계에 기타법인 포함 여부(기본 미포함)

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    start, end = kst_dates_1y()

    # df: [날짜(index), 기관합계, 기타법인, 개인, 외국인합계, 전체]
    df = stock.get_market_trading_volume_by_date(start, end, TICKER)
    if df.index.name is not None:
        df = df.reset_index()

    # 컬럼명 샘플과 동일하게 맞추기
    colmap = {
        "날짜": "일자",
        "기관합계": "기관 합계",
        "기타법인": "기타법인",
        "개인": "개인",
        "외국인합계": "외국인 합계",
        "전체": "전체",
    }
    df = df.rename(columns={c: colmap.get(c, c) for c in df.columns})

    # 날짜 형식 YYYY-MM-DD
    if "일자" not in df.columns:
        raise RuntimeError("pykrx 결과에 '일자'(날짜) 컬럼이 없습니다.")
    df["일자"] = pd.to_datetime(df["일자"]).dt.strftime("%Y-%m-%d")

    # 숫자 정규화
    numeric_cols = ["기관 합계", "기타법인", "개인", "외국인 합계", "전체"]
    df = normalize_numeric(df, numeric_cols)

    # (선택) 기관 합계에 기타법인 포함
    if INCLUDE_ETC_CORP:
        df["기관 합계"] = df["기관 합계"] + df["기타법인"]

    # 최신이 위로
    df = df.sort_values("일자", ascending=False)
    ordered = ["일자", "기관 합계", "기타법인", "개인", "외국인 합계", "전체"]
    df = df[ordered]

    df.to_csv(os.path.join(OUTPUT_DIR, "latest_trading_volume.csv"),
              index=False, encoding="utf-8-sig")

if __name__ == "__main__":
    main()
