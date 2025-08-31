import os
from datetime import datetime
import pytz
import pandas as pd
from pykrx import stock

KST = pytz.timezone("Asia/Seoul")
TICKER = os.getenv("TICKER", "092040")

def is_trading_day_kst() -> bool:
    today = datetime.now(KST).date().strftime("%Y%m%d")
    df = stock.get_market_ohlcv_by_date(today, today, TICKER)
    return isinstance(df, pd.DataFrame) and len(df) > 0

if __name__ == "__main__":
    is_trading = is_trading_day_kst()
    out = os.environ.get("GITHUB_OUTPUT")
    if out:
        with open(out, "a", encoding="utf-8") as f:
            f.write(f"is_trading_day={'true' if is_trading else 'false'}\n")
    print(f"is_trading_day={is_trading}")
