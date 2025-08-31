import os
from datetime import datetime, timedelta
import pandas as pd
import pytz

KST = pytz.timezone("Asia/Seoul")

def yyyymmdd(d):
    return d.strftime("%Y%m%d")

def kst_today():
    return datetime.now(KST).date()

def kst_dates_1y():
    today = kst_today()
    start = today - timedelta(days=365)
    return yyyymmdd(start), yyyymmdd(today)

def normalize_numeric(df: pd.DataFrame, cols):
    for c in cols:
        if c not in df.columns:
            df[c] = 0
        df[c] = (
            df[c].astype(str)
                .str.replace(",", "")
                .str.replace("+", "")
                .str.replace("−", "-", regex=False)
                .str.replace("–", "-", regex=False)
        )
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
    return df
