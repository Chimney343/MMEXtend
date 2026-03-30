import sqlite3, pandas as pd
fpath = r"C:\Users\mkkom\Mój dysk\PersonalFinance\MMEX\personal_finance.mmb"
conn = sqlite3.connect(fpath)
rows = conn.execute("""
  SELECT t.TRANSID, t.TRANSDATE, t.TRANSCODE, t.TRANSAMOUNT, t.TOTRANSAMOUNT,
         cur.CURRENCY_SYMBOL, a.ACCOUNTNAME
  FROM CHECKINGACCOUNT_V1 t
  JOIN ACCOUNTLIST_V1 a ON t.ACCOUNTID = a.ACCOUNTID
  JOIN CURRENCYFORMATS_V1 cur ON a.CURRENCYID = cur.CURRENCYID
  WHERE cur.CURRENCY_SYMBOL = "GBP"
  AND t.TRANSDATE BETWEEN "2024-02-22" AND "2024-02-25"
""").fetchall()
for r in rows:
    print(r)
conn.close()
