import sys
import os
import json
import requests
import shutil
from datetime import datetime, timedelta, UTC
from bs4 import BeautifulSoup
import pandas as pd
import openpyxl
import re
import time
import threading
import boto3
from io import BytesIO, StringIO
from botocore.client import Config

import subprocess
import random
import psycopg2

# ----------- PARAMETRELER VE AYARLAR -----------
DAYS = 3   # Buradaki günü değiştirerek aranan dosya gün filtresini ayarlayabilirsin (örn: 1, 3, 7)

AWS_BUCKET = "alaybey"
AWS_REGION = os.getenv("AWS_REGION")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET")
ENDPOINT_URL = f"https://s3.{AWS_REGION}.wasabisys.com"

USER_AGENT = "Alper Alaybey <a.alaybey@gmail.com>"  # Kendi SEC-compliant agent
HEADERS = {"User-Agent": USER_AGENT}

s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    endpoint_url=ENDPOINT_URL,
    config=Config(signature_version="s3v4"),
)

def s3_path(key):  # Her path için kullan
    rel_path = key.replace("\\", "/").replace("./", "")
    return rel_path.lstrip("/")

def s3_exists(key):
    try:
        s3.head_object(Bucket=AWS_BUCKET, Key=s3_path(key))
        return True
    except:
        return False

def s3_read_text(key):
    obj = s3.get_object(Bucket=AWS_BUCKET, Key=s3_path(key))
    return obj["Body"].read().decode("utf-8")

def s3_read_bytes(key):
    obj = s3.get_object(Bucket=AWS_BUCKET, Key=s3_path(key))
    return obj["Body"].read()

def s3_write_text(key, text):
    s3.put_object(Bucket=AWS_BUCKET, Key=s3_path(key), Body=text.encode("utf-8"))

def s3_write_bytes(key, b):
    s3.put_object(Bucket=AWS_BUCKET, Key=s3_path(key), Body=b)

def s3_delete(key):
    s3.delete_object(Bucket=AWS_BUCKET, Key=s3_path(key))

def s3_list_dir(prefix):
    paginator = s3.get_paginator("list_objects_v2")
    result = paginator.paginate(Bucket=AWS_BUCKET, Prefix=s3_path(prefix))
    return [content['Key'] for page in result for content in page.get('Contents', [])]

# ----------- Tetikleyici dosyasından ticker çekme -----------
def get_trigger_ticker():
    """Wasabi bucket'ında trigger*.txt dosyasını bulur, ticker'ı döndürür."""
    resp = s3.list_objects_v2(Bucket=AWS_BUCKET)
    trigger_key = None
    ticker = None
    for obj in resp.get('Contents', []):
        key = obj['Key']
        if key.startswith('trigger') and key.endswith('.txt') and len(key) > 10:
            trigger_key = key
            ticker = key[len('trigger'):-len('.txt')]
            break
    if not trigger_key or not ticker:
        raise Exception("trigger*.txt bulunamadı!")
    return ticker.upper(), trigger_key

# ----------- tickers.txt'den cik numarası bulma -----------
def get_cik_for_ticker(ticker, tickers_file="tickers.txt"):
    """Aynı klasördeki tickers.txt'den (ör: AAPL,1234567890) ticker'a karşılık gelen cik'i bulur"""
    with open(tickers_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or ',' not in line:
                continue
            t, cik = line.split(",", 1)
            if t.strip().upper() == ticker.upper():
                return cik.strip().zfill(10)
    raise Exception(f"{ticker} için cik bulunamadı (tickers.txt'de yok)")

# ----------- a1.py'den gelen diğer yardımcılar -----------
def incr_request_and_sleep():
    # Proxy yok, throttle sadece log
    incr_request_and_sleep.counter += 1
    if incr_request_and_sleep.counter % 10 == 0:
        print(f"🕒 {incr_request_and_sleep.counter} request atıldı, 0.5 sn bekleniyor...")
        time.sleep(0.5)
incr_request_and_sleep.counter = 0

def inc_error_and_kill_if_limit(limit=50):
    inc_error_and_kill_if_limit.counter += 1
    if inc_error_and_kill_if_limit.counter >= limit:
        print("LOG: Script kill oldu! (üst üste hata limiti aşıldı)")
        print(f"❌ ÜST ÜSTE {limit} HATA! Script kill ediliyor.")
        sys.exit(1)
inc_error_and_kill_if_limit.counter = 0

def download_file(url, s3key):
    backoff_count = 0
    while True:
        try:
            incr_request_and_sleep()
            resp = requests.get(url, headers=HEADERS)
            if resp.status_code in [429, 403]:
                backoff_count += 1
                print(f"⏳ Rate-limit algılandı! {backoff_count}. kez 2 dakika bekleniyor... [download_file] {url}")
                if backoff_count >= 3:
                    print("❌ 3 kez üst üste backoff, script kill ediliyor!")
                    sys.exit(1)
                time.sleep(120)
                continue
            if resp.status_code >= 400:
                print(f"📥 Dosya indirme hatası: {url} => {resp.status_code} {resp.reason}")
                break
            s3_write_bytes(s3key, resp.content)
            break
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            backoff_count += 1
            print(f"🌐 Bağlantı hatası: {url}")
            if backoff_count >= 3:
                print("❌ 3 kez üst üste backoff, script kill ediliyor!")
                sys.exit(1)
            time.sleep(120)
            continue
        except Exception as e:
            print(f"📥 Dosya indirme hatası: {url} => {e}")
            inc_error_and_kill_if_limit()
            break

def s3_load_workbook(key):
    b = s3_read_bytes(key)
    return openpyxl.load_workbook(BytesIO(b), data_only=True)

def download_xlsx(index_url, folder_path, file_name, is_10k=False):
    print(f"🔎 XLSX aranıyor: {file_name} [{index_url}]")
    backoff_count = 0
    while True:
        try:
            incr_request_and_sleep()
            resp = requests.get(index_url, headers=HEADERS)
            if resp.status_code in [429, 403]:
                backoff_count += 1
                print(f"⏳ Rate-limit algılandı! {backoff_count}. kez 2 dakika bekleniyor... [download_xlsx] {index_url}")
                if backoff_count >= 3:
                    print("❌ 3 kez üst üste backoff, script kill ediliyor!")
                    sys.exit(1)
                time.sleep(120)
                continue
            if resp.status_code >= 400:
                print(f"❌ Index sayfası hatası: {index_url} => {resp.status_code} {resp.reason}")
                return None, None, None, None
            break
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            backoff_count += 1
            print(f"🌐 Bağlantı hatası: {index_url}")
            if backoff_count >= 3:
                print("❌ 3 kez üst üste backoff, script kill ediliyor!")
                sys.exit(1)
            time.sleep(120)
            continue
        except Exception as e:
            print(f"❌ Index sayfası alınamadı: {e}")
            inc_error_and_kill_if_limit()
            return None, None, None, None

    soup = BeautifulSoup(resp.text, "html.parser")
    xlsx_url = None
    for link in soup.find_all("a"):
        href = link.get("href", "")
        if href.lower().endswith(".xlsx"):
            if href.startswith("/"):
                xlsx_url = "https://www.sec.gov" + href
            else:
                base = index_url.rsplit("/", 1)[0]
                xlsx_url = base + "/" + href
            break

    if not xlsx_url:
        return None, None, None, None

    temp_path = f"{folder_path}/temp.xlsx"
    if s3_exists(temp_path):
        try:
            s3_delete(temp_path)
            print(f"ℹ️ Eski temp.xlsx dosyası silindi: {temp_path}")
        except Exception as e:
            print(f"⚠️ Eski temp dosyası silinemedi: {e}")

    download_file(xlsx_url, temp_path)

    if not s3_exists(temp_path):
        print(f"⚠️ Dosya temp.xlsx oluşturulamadı: {temp_path}")
        inc_error_and_kill_if_limit()
        return None, None, None, None

    try:
        b = s3_read_bytes(temp_path)
        wb = openpyxl.load_workbook(BytesIO(b), data_only=True)
        year, quarter = None, None
        for sheet in wb.worksheets:
            for row in sheet.iter_rows():
                for cell in row:
                    if isinstance(cell.value, str) and "Document Fiscal Year Focus" in cell.value:
                        idx = row.index(cell)
                        if idx + 1 < len(row):
                            year_cell = row[idx + 1]
                            if year_cell.value is not None:
                                year = str(year_cell.value).strip()
            if is_10k:
                quarter = "Q4"
        if not is_10k:
            for sheet in wb.worksheets:
                for row in sheet.iter_rows():
                    for cell in row:
                        if isinstance(cell.value, str) and "Document Fiscal Period Focus" in cell.value:
                            idx = row.index(cell)
                            if idx + 1 < len(row):
                                qval = row[idx + 1].value
                                if qval is not None:
                                    qval = str(qval).strip().upper()
                                    if qval.startswith("Q"):
                                        quarter = qval
                                    else:
                                        quarter = "Q" + qval
        base_ticker = file_name.split("-")[0]
        if not year or not quarter:
            parts = file_name.split("-")
            if len(parts) >= 3:
                year = parts[1]
                quarter = parts[2]
        symbol = base_ticker
        new_base = f"{symbol}-{year}-{quarter}"
        final_name = new_base + ".xlsx"
        final_path = f"{folder_path}/{final_name}"

        if s3_exists(final_path):
            print(f"ℹ️ Zaten var, atlandı: {final_path}")
            s3_delete(temp_path)
        else:
            b = s3_read_bytes(temp_path)
            s3_write_bytes(final_path, b)
            s3_delete(temp_path)
            print(f"✅ Kaydedildi: {final_path}")

        return final_path, symbol, year, quarter

    except Exception as e:
        print(f"⚠️ XLSX işleme hatası: {e}")
        fallback = f"{folder_path}/{file_name}.xlsx"
        b = s3_read_bytes(temp_path)
        s3_write_bytes(fallback, b)
        s3_delete(temp_path)
        print(f"⚠️ Geçici adla kaydedildi: {fallback}")
        inc_error_and_kill_if_limit()
        parts = file_name.split("-")
        if len(parts) >= 3:
            symbol = parts[0]
            year = parts[1]
            quarter = parts[2]
            return fallback, symbol, year, quarter
        return fallback, None, None, None

def get_sheet_title(sheet):
    try:
        top_left = sheet["A1"].value
        if isinstance(top_left, str):
            return top_left.strip().lower()
        else:
            return ""
    except:
        return ""

def detect_multiplier_from_title(title):
    if not title:
        return 1.0
    words = title.split()
    if not words:
        return 1.0
    last = re.sub(r"[^\w]", "", words[-1]).lower()
    if last == "millions":
        return 1_000_000.0
    elif last == "thousands":
        return 1_000.0
    else:
        return 1.0

def find_metric_value(sheet, keyword_groups, multiplier=1.0, exclude_term=None, reverse=False, quarter=None, row_start=None):
    rows = list(sheet.iter_rows())
    if reverse:
        rows = rows[::-1]
    if row_start is not None:
        rows = rows[row_start:]
    col_idx = None
    if quarter is not None:
        quarter_map = {"Q1": "3 month", "Q2": "6 month", "Q3": "9 month", "Q4": "12 month"}
        target_phrase = quarter_map.get(quarter.upper())
        if target_phrase:
            for i in range(min(2, len(rows))):
                for j, cell in enumerate(rows[i]):
                    if cell.value and target_phrase.lower() in str(cell.value).lower():
                        col_idx = j
                        break
                if col_idx is not None:
                    break
    for group in keyword_groups:
        for row in rows:
            row_text = " ".join(str(c.value).lower() for c in row if c.value is not None)
            if exclude_term and exclude_term.lower() in row_text:
                continue
            if all(k.lower() in row_text for k in group):
                label_idx = None
                for idx, c in enumerate(row):
                    if isinstance(c.value, str):
                        cell_text = c.value.lower()
                        if all(k.lower() in cell_text for k in group):
                            label_idx = idx
                            break
                if label_idx is not None:
                    if col_idx is not None:
                        c = row[col_idx]
                        if isinstance(c.value, (int, float)):
                            return c.value * multiplier
                    for c in row[label_idx + 1:]:
                        if isinstance(c.value, (int, float)):
                            return c.value * multiplier
                    for c in row:
                        if isinstance(c.value, (int, float)):
                            return c.value * multiplier
    return None

def s3_write_excel(df, key):
    out = BytesIO()
    df.to_excel(out, index=False)
    out.seek(0)
    s3_write_bytes(key, out.read())

def extract_metrics(file_path, symbol, year, quarter):
    print(f"📊 Veriler çıkarılıyor: {symbol} - {year} {quarter}")

    try:
        wb = s3_load_workbook(file_path)
    except Exception as e:
        print(f"⚠️ Excel açılamadı: {file_path} => {e}")
        inc_error_and_kill_if_limit()
        return

    cash_flow_metrics = {
        "Depreciation and Amortization": [["depreciation", "amortization"]],
        "Amortization": [["amortization"]],
        "Depreciation": [["depreciation"]],
        "Cash From Operations": [["cash", "operat"]],
        "PPE Purchase": [["property", "equipment", "purchas"], ["property", "equipment", "add"], ["property", "equipment", "payment"], ["property", "equipment", "acqui"], ["property", "add"], ["property", "purchas"], ["property", "acqui"]],
        "PPE Sale": [["sale", "property"], ["sale", "fixed"], ["disposal", "property"], ["disposal", "fixed"], ["sale", "tangible"], ["disposal", "tangible"]],
        "Cash Business Acquisitions": [["business"]],
        "Dividends": [["payment", "dividend"]],
        "Cash Taxes": [["paid", "tax"], ["cash", "tax"], ["tax"]],
        "Cash Interest": [["paid", "interest"], ["cash", "interest"], ["interest"]],
    }

    income_metrics = {
        "Sales": [["revenue"], ["sale"]],
        "Cost of Sales": [["cost of"]],
        "Gross Profit": [["gross"], ["margin"], ["gross", "profit"]],
        "EBIT": [["operating", "income"], ["operating", "loss"], ["income", "operations"], ["loss", "operations"]],
        "Net Income": [["net income"], ["net loss"], ["net", "income"], ["net", "loss"]],
        "Interest Income": [["interest", "income"], ["other", "net"]],
        "Interest Expense": [["interest", "expense"]],
        "EPS": [["basic", "net", "income"], ["basic", "net", "loss"], ["basic", "dollar"], ["basic", "usd"]],
        "Shares Outstanding": [["basic"]],
    }

    balance_metrics = {
        "Total Assets": [["total assets"]],
        "Total Equity": [["total", "equity"]],
        "Noncontrolling interest": [["non", "control", "interest"]],
        "Cash and cash equivalents": [["cash", "equivalents"]],
        "Inventories": [["inventor"]],
        "Accounts receivable": [["accounts", "receivable"]],
        "Prepaid expenses": [["prepaid", "expenses"]],
        "PPE": [["property", "equipment"]],
        "Intangible assets": [["intangible", "asset"]],
        "Operating lease assets": [["leas", "operati", "asset"]],
        "Digital assets": [["digital", "asset"]],
        "Right-of-use assets": [["use", "asset"]],
        "Accounts payable": [["account", "payable"]],
        "Operating lease liabilities": [["leas", "operati", "liab"]],
        "Accrued liabilities": [["accrue", "liabilit"], ["accrue", "expense"]],
        "Deferred revenue": [["defer", "revenue"], ["unearn", "revenue"]],
    }

    extracted = {}

    cash_flow_sheet = None
    cash_flow_multiplier = 1.0
    for sheet in wb.worksheets:
        title = get_sheet_title(sheet)
        if "cash flow" in title:
            cash_flow_sheet = sheet
            cash_flow_multiplier = detect_multiplier_from_title(title)
            break

    if cash_flow_sheet is not None:
        for metric, kw_groups in cash_flow_metrics.items():
            val = find_metric_value(
                cash_flow_sheet, kw_groups, multiplier=cash_flow_multiplier, exclude_term=None, reverse=False, quarter=quarter
            )
            extracted[metric] = val if val is not None else 0.0
    else:
        for metric in cash_flow_metrics.keys():
            extracted[metric] = 0.0

    income_sheet = None
    income_general_multiplier = 1.0
    income_share_multiplier = None

    for sheet in wb.worksheets:
        title = get_sheet_title(sheet)
        if "statements of operations" in title:
            income_sheet = sheet
            income_general_multiplier = detect_multiplier_from_title(title)
            low = title.lower()
            if "shares in millions" in low:
                income_share_multiplier = 1_000_000.0
            elif "shares in thousands" in low:
                income_share_multiplier = 1_000.0
            else:
                income_share_multiplier = 1.0
            break

    if income_sheet is None:
        for sheet in wb.worksheets:
            title = get_sheet_title(sheet)
            if "income" in title and "statement" in title:
                income_sheet = sheet
                income_general_multiplier = detect_multiplier_from_title(title)
                low = title.lower()
                if "shares in millions" in low:
                    income_share_multiplier = 1_000_000.0
                elif "shares in thousands" in low:
                    income_share_multiplier = 1_000.0
                else:
                    income_share_multiplier = 1.0
                break

    if income_sheet is not None:
        for metric, kw_groups in income_metrics.items():
            if metric == "Shares Outstanding":
                multiplier = income_share_multiplier or 1.0
                value = find_metric_value(income_sheet, kw_groups, multiplier=multiplier, exclude_term=None, reverse=True, quarter=quarter)
            else:
                value = find_metric_value(income_sheet, kw_groups, multiplier=income_general_multiplier, exclude_term=None, reverse=False, quarter=quarter)
            extracted[metric] = value if value is not None else 0.0
    else:
        for metric in income_metrics.keys():
            extracted[metric] = 0.0

    balance_sheet = None
    balance_multiplier = 1.0

    for sheet in wb.worksheets:
        title = get_sheet_title(sheet)
        if "balance sheet" in title and "parenthetical" not in title:
            balance_sheet = sheet
            balance_multiplier = detect_multiplier_from_title(title)
            break

    if balance_sheet is not None:
        for metric, kw_groups in balance_metrics.items():
            value = find_metric_value(balance_sheet, kw_groups, multiplier=balance_multiplier, exclude_term=None, reverse=False, quarter=quarter)
            extracted[metric] = value if value is not None else 0.0
    else:
        for metric in balance_metrics.keys():
            extracted[metric] = 0.0

    s3_out_dir = "Final"
    out_path = f"{s3_out_dir}/{symbol}.xlsx"

    all_metrics = list(cash_flow_metrics.keys()) + list(income_metrics.keys()) + list(balance_metrics.keys())

    if s3_exists(out_path):
        df_existing = pd.read_excel(BytesIO(s3_read_bytes(out_path)))
        if "Metric" not in df_existing.columns:
            df_existing.insert(0, "Metric", all_metrics)
        df = df_existing.set_index("Metric")
    else:
        df = pd.DataFrame(index=all_metrics)

    col_name = f"{year} {quarter}"
    col_values = [extracted.get(m, 0.0) for m in all_metrics]
    df[col_name] = col_values

    df_to_save = df.reset_index().rename(columns={"index": "Metric"})
    s3_write_excel(df_to_save, out_path)

import yfinance as yf

def fill_dates_and_prices_in_ws(ws_dst):
    ticker = ws_dst["B40"].value
    index_ticker = ws_dst["C40"].value

    if not ticker:
        print(f"B40 hücresinde ticker yok, atlanıyor.")
        return
    if not index_ticker:
        print(f"C40 hücresinde index ticker yok, atlanıyor.")
        return

    if index_ticker.upper().startswith("ARCX:"):
        index_ticker = index_ticker.split(":", 1)[1]
    index_ticker = index_ticker.strip()

    start_row = 41
    end_row = 107
    curr_date = datetime.today().date()
    min_date = curr_date - timedelta(days=(end_row - start_row) * 30 + 30)

    def get_hist(ticker_code):
        try:
            yf_ticker = yf.Ticker(ticker_code)
            hist = yf_ticker.history(start=min_date.strftime("%Y-%m-%d"), end=(curr_date + timedelta(days=1)).strftime("%Y-%m-%d"))
            hist = hist.reset_index()
            if 'Date' in hist:
                hist['Date'] = hist['Date'].dt.date
            return hist
        except Exception as e:
            print(f"{ticker_code} için fiyat verisi alınamadı: {e}")
            return None

    hist_company = get_hist(ticker)
    hist_index = get_hist(index_ticker)

    prev_date = curr_date
    for i, row in enumerate(range(start_row, end_row + 1)):
        if i == 0:
            this_date = curr_date
        else:
            this_date = prev_date - timedelta(days=30)

        def find_price(hist, date):
            search_date = date
            close_price = None
            while close_price is None and search_date >= min_date:
                if hist is not None:
                    row_df = hist[hist['Date'] == search_date]
                    if not row_df.empty and not row_df['Close'].isnull().all():
                        close_price = float(row_df['Close'].iloc[0])
                        break
                search_date -= timedelta(days=1)
            return close_price

        price_company = find_price(hist_company, this_date)
        price_index = find_price(hist_index, this_date)

        ws_dst[f"A{row}"] = this_date.strftime("%Y-%m-%d")
        ws_dst[f"B{row}"] = price_company if price_company is not None else "Veri yok"
        ws_dst[f"C{row}"] = price_index if price_index is not None else "Veri yok"

        prev_date = this_date

def create_final2_file_for_ticker(ticker):
    """Final2 için tek bir ticker'ın dosyasını oluşturur."""
    final_folder = s3_path("Final")
    final2_folder = s3_path("Final2")
    template_path = s3_path("Companies1/donusturucu.xlsx")  # Şablon tek script için burada

    src_key = s3_path(f"Final/{ticker}.xlsx")
    if not s3_exists(src_key):
        print(f"{ticker} için Final dosyası yok.")
        return

    if not s3_exists(template_path):
        print(f"Şablon bulunamadı: {template_path}")
        return

    try:
        wb_src = openpyxl.load_workbook(BytesIO(s3_read_bytes(src_key)), data_only=True)
        ws_src = wb_src.active

        dst_key = s3_path(f"Final2/{ticker}.xlsx")
        template_bytes = s3_read_bytes(template_path)
        s3_write_bytes(dst_key, template_bytes)

        wb_dst = openpyxl.load_workbook(BytesIO(template_bytes), data_only=False)
        ws_dst = wb_dst.active

        # Tüm hücreleri kopyala (ilk 36 satır, BG'ye kadar)
        from openpyxl.utils import column_index_from_string
        max_col = column_index_from_string("BG")
        max_row = 36
        for r in range(1, max_row + 1):
            for c in range(1, max_col + 1):
                src_val = ws_src.cell(row=r, column=c).value
                if src_val is not None:
                    ws_dst.cell(row=r, column=c).value = src_val

        ws_dst["B40"].value = ticker
        ws_dst["C40"].value = "^GSPC"

        fill_dates_and_prices_in_ws(ws_dst)

        yf_ticker = yf.Ticker(ticker)

        # E41: Sector
        try:
            ws_dst["E41"].value = yf_ticker.info.get("sector", "")
        except Exception as e:
            print(f"{ticker} - sector alınamadı: {e}")
            ws_dst["E41"].value = ""

        # F41: Industry
        try:
            ws_dst["F41"].value = yf_ticker.info.get("industry", "")
        except Exception as e:
            print(f"{ticker} - industry alınamadı: {e}")
            ws_dst["F41"].value = ""

        # G41: Employees
        try:
            ws_dst["G41"].value = yf_ticker.info.get("fullTimeEmployees", "")
        except Exception as e:
            print(f"{ticker} - employees alınamadı: {e}")
            ws_dst["G41"].value = ""

        # H41: Description/Summary
        try:
            summary = yf_ticker.info.get("longBusinessSummary", yf_ticker.info.get("summary", ""))
            ws_dst["H41"].value = summary
        except Exception as e:
            print(f"{ticker} - description alınamadı: {e}")
            ws_dst["H41"].value = ""

        # E45: Beta
        try:
            ws_dst["E45"].value = yf_ticker.info.get("beta", "")
        except Exception as e:
            print(f"{ticker} - beta alınamadı: {e}")
            ws_dst["E45"].value = ""

        # F45: US 10-Year Treasury Yield
        try:
            tnx_ticker = yf.Ticker("^TNX")
            tnx_yield = tnx_ticker.info.get("regularMarketPrice", "")
            ws_dst["F45"].value = tnx_yield
        except Exception as e:
            print(f"{ticker} - US 10Y yield alınamadı: {e}")
            ws_dst["F45"].value = ""

        # I41: Earnings Date
        try:
            cal = yf_ticker.calendar
            earning_date = ""
            if isinstance(cal, pd.DataFrame):
                if not cal.empty and "Earnings Date" in cal.index:
                    earning_date = cal.loc["Earnings Date"][0]
            elif isinstance(cal, dict):
                earning_date = cal.get("Earnings Date", [None])[0]
            ws_dst["I41"].value = str(earning_date) if earning_date else ""
        except Exception as e:
            print(f"{ticker} - earnings date alınamadı: {e}")
            ws_dst["I41"].value = ""

        # Formül fonksiyonları (excel python donusum.txt) -- burada opsiyonel!
        # Bu dosya yoksa bu satırı devre dışı bırakabilirsin:
        try:
            with open("excel python donusum.txt", "r", encoding="utf-8") as f:
                formul_code = f.read()
            formul_ns = {}
            exec(formul_code, formul_ns)
            formul_ns["hesapla_tum_formuller"](ws_dst)
        except Exception as e:
            print(f"Formül fonksiyonu hatası: {e}")

        # Final2 dosyasını tekrar s3'e yükle
        buffer = BytesIO()
        wb_dst.save(buffer)
        buffer.seek(0)
        s3_write_bytes(dst_key, buffer.read())
        print(f"Final2 kaydedildi: {dst_key}")

    except Exception as e:
        print(f"Final2 oluşturulamadı: {e}")

def get_data_from_excel(filepath, range_tuple):
    wb = openpyxl.load_workbook(BytesIO(s3_read_bytes(filepath)), data_only=True)
    ws = wb.active
    start_col, start_row = range_tuple[0][0], int(range_tuple[0][1:])
    end_col, end_row = range_tuple[1][0], int(range_tuple[1][1:])
    data = []
    for row in ws.iter_rows(min_row=start_row, max_row=end_row,
                            min_col=ord(start_col)-64, max_col=ord(end_col)-64):
        values = [cell.value for cell in row]
        data.append(values)
    return data

def insert_company_info_to_db(cursor, ticker, sector, industry, employees, earnings_date, summary, radar=None, market_cap=None):
    sql = """
        INSERT INTO company_info (ticker, sector, industry, employees, earnings_date, summary, radar, market_cap)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ticker) DO UPDATE
        SET sector=EXCLUDED.sector,
            industry=EXCLUDED.industry,
            employees=EXCLUDED.employees,
            earnings_date=EXCLUDED.earnings_date,
            summary=EXCLUDED.summary,
            radar=EXCLUDED.radar,
            market_cap=EXCLUDED.market_cap
    """
    cursor.execute(sql, (ticker, sector, industry, employees, earnings_date, summary, radar, market_cap))

def insert_data_to_db(cursor, ticker, data):
    headers = data[0]
    for row in data[1:]:
        metric = row[0]
        for i in range(1, len(headers)):
            period = headers[i]
            value = row[i]
            sql = """
            INSERT INTO excel_metrics (ticker, metric, period, value)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (ticker, metric, period)
            DO UPDATE SET value = EXCLUDED.value
            """
            cursor.execute(sql, (ticker, metric, period, str(value) if value is not None else None))

def upload_to_db(ticker):
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASS")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME")

    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        user=db_user,
        password=db_pass,
        dbname=db_name
    )

    cursor = conn.cursor()
    fpath = s3_path(f"Final2/{ticker}.xlsx")
    if not s3_exists(fpath):
        print(f"{ticker}.xlsx Final2’de yok, atlanıyor.")
        return
    try:
        EXCEL_RANGE = ('A191', 'O202')
        data = get_data_from_excel(fpath, EXCEL_RANGE)
        insert_data_to_db(cursor, ticker, data)
        conn.commit()

        # --- GENEL FİRMASAL VERİLERİ EKLE ---
        wb = openpyxl.load_workbook(BytesIO(s3_read_bytes(fpath)), data_only=True)
        ws = wb.active
        sector = ws["B204"].value
        industry = ws["B205"].value
        employees = ws["B206"].value
        earnings_date = ws["B207"].value
        summary = ws["B208"].value
        radar = ws["B209"].value
        market_cap = ws["B210"].value

        insert_company_info_to_db(cursor, ticker, sector, industry, employees, earnings_date, summary,
                                  int(radar) if radar is not None else None,
                                  int(market_cap) if market_cap is not None else None)

        conn.commit()
        print(f"{ticker} yüklendi.")
    except Exception as e:
        print(f"{ticker} hata: {e}")
    cursor.close()
    conn.close()

def main():
    try:
        ticker, trigger_key = get_trigger_ticker()
        print(f"Tetiklenen şirket: {ticker}  | Trigger dosyası: {trigger_key}")

        cik = get_cik_for_ticker(ticker)
        print(f"{ticker} için CIK: {cik}")

        # SEC API'den çekilecek gün aralığı
        days = DAYS

        # SEC veri çekimi
        url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        print(f"SEC filings indiriliyor: {url}")

        backoff_count = 0
        while True:
            try:
                incr_request_and_sleep()
                r = requests.get(url, headers=HEADERS)
                if r.status_code in [429, 403]:
                    backoff_count += 1
                    print(f"⏳ Rate-limit algılandı! {backoff_count}. kez 2 dakika bekleniyor... [main] {url}")
                    if backoff_count >= 3:
                        print("❌ 3 kez üst üste backoff, script kill ediliyor!")
                        sys.exit(1)
                    time.sleep(120)
                    continue
                if r.status_code >= 400:
                    print(f"⚠️ {ticker}: {cik} - API hatası veya veri yok. {r.status_code} {r.reason}")
                    inc_error_and_kill_if_limit()
                    return
                data = r.json().get("filings", {}).get("recent", {})
                break
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                backoff_count += 1
                print(f"🌐 Bağlantı hatası: {url}")
                if backoff_count >= 3:
                    print("❌ 3 kez üst üste backoff, script kill ediliyor!")
                    sys.exit(1)
                time.sleep(120)
                continue
            except Exception as e:
                print(f"⚠️ {ticker}: {cik} - Veri çekilemedi: {e}")
                inc_error_and_kill_if_limit()
                return

        forms = data.get("form", [])
        accessions = data.get("accessionNumber", [])
        report_dates = data.get("reportDate", [])

        found = False
        for i, ftype in enumerate(forms):
            if ftype not in ["10-Q", "10-K"]:
                continue
            try:
                rpt_date = datetime.strptime(report_dates[i], "%Y-%m-%d")
            except:
                continue
            filter_date = (datetime.now(UTC) - timedelta(days=days)).date()
            if rpt_date.date() < filter_date:
                continue
            acc = accessions[i].replace("-", "")
            year = str(rpt_date.year)
            month = rpt_date.month
            quarter_idx = (month - 1) // 3 + 1
            is_10k = (ftype == "10-K")
            quarter_label = f"Q{quarter_idx}" if not is_10k else "Q4"
            folder = f"Companies1/{ticker}"
            index_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc}/index.html"
            fname = f"{ticker}-{year}-{quarter_label}"
            file_path, sym, y, q = download_xlsx(index_url, folder, fname, is_10k)
            if file_path and sym and y and q:
                extract_metrics(file_path, sym, y, q)
                found = True
                break  # Son 1 dosya yeterli, diğerlerini alma

        if not found:
            print(f"Son {days} günde {ticker} için yeni finansal bulunamadı!")
            return

        # Final2 excel oluştur
        create_final2_file_for_ticker(ticker)

        # DB'ye yükle
        upload_to_db(ticker)

    finally:
        # Ne olursa olsun trigger dosyasını sil
        try:
            _, trigger_key = get_trigger_ticker()
            s3_delete(trigger_key)
            print(f"Trigger dosyası silindi: {trigger_key}")
        except Exception as e:
            print(f"Trigger dosyası silinemedi: {e}")

if __name__ == "__main__":
    main()
