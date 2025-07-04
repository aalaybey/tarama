import subprocess
import threading
import time
import random
import requests
import sys
import os
import shutil
import openpyxl
from openpyxl.utils import column_index_from_string
from threading import Lock
import yfinance as yf
import datetime
import psycopg2
import pandas as pd
import boto3
from io import BytesIO, StringIO

# --- S3 ayarları ---
import os

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET")
AWS_REGION = os.getenv("AWS_REGION")  # bölgen neyse
S3_BUCKET = "alaybey"
S3_PREFIX = "s3/"  # S3'te dosya yolu başı, sonunda / olmalı!

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)

def s3_listdir(prefix):
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key != prefix and not key.endswith('/'):
                yield key

def s3_file_exists(key):
    try:
        s3_client.head_object(Bucket=S3_BUCKET, Key=key)
        return True
    except Exception:
        return False

def s3_download_bytes(key):
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
    return obj['Body'].read()

def s3_upload_bytes(key, data):
    s3_client.put_object(Bucket=S3_BUCKET, Key=key, Body=data)

def s3_download_str(key):
    return s3_download_bytes(key).decode("utf-8")

def s3_upload_str(key, data):
    s3_upload_bytes(key, data.encode("utf-8"))

def s3_download_xlsx(key):
    data = s3_download_bytes(key)
    return openpyxl.load_workbook(BytesIO(data), data_only=True)

def s3_upload_xlsx(key, workbook):
    buffer = BytesIO()
    workbook.save(buffer)
    buffer.seek(0)
    s3_upload_bytes(key, buffer.read())

def s3_copy(src_key, dst_key):
    s3_client.copy_object(Bucket=S3_BUCKET, CopySource={'Bucket': S3_BUCKET, 'Key': src_key}, Key=dst_key)

def s3_isdir(prefix):
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix, Delimiter='/')
    return 'CommonPrefixes' in response or 'Contents' in response

# --- Dosya yolu yardımcıları ---
def s3_path(local_path):
    # Eski kodda geçen her local path yerine bu fonksiyon ile s3 yolunu üret
    rel_path = local_path.replace("\\", "/").replace("./", "")
    if rel_path.startswith(S3_PREFIX):
        return rel_path
    return f"{S3_PREFIX}{rel_path}".replace("//", "/")

FINAL2_DIR = s3_path("Final2")
EXCEL_RANGE = ('A191', 'O202')

# --- Formül fonksiyonlarını yükle
formul_path = s3_path("excel python donusum.txt")
formul_ns = {}
formul_code = s3_download_str(formul_path)
exec(formul_code, formul_ns)

PROXY_FILE = s3_path("sec_calısan_proxyler.txt")
EMAILS_FILE = s3_path("emailler.txt")

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

def get_final_xlsx_times(final_folder=s3_path("Final")):
    """Final klasöründeki tüm .xlsx dosyalarının {TICKER:mtime} şeklinde dict’ini döner."""
    xlsx_times = {}
    # S3'te dosya isimlerinde .xlsx kontrolü yapılıyor
    for key in s3_listdir(final_folder):
        if key.lower().endswith(".xlsx"):
            try:
                stat = s3_client.head_object(Bucket=S3_BUCKET, Key=key)
                ticker = os.path.splitext(os.path.basename(key))[0].upper()
                xlsx_times[ticker] = stat['LastModified'].timestamp()
            except Exception:
                pass
    return xlsx_times

def load_proxies_from_file(proxy_file):
    text = s3_download_str(proxy_file)
    proxies = [line.strip() for line in text.splitlines() if line.strip()]
    return proxies

def load_emails_from_file(emails_file):
    code = s3_download_str(emails_file)
    ns = {}
    exec(code, ns)
    if "EMAILS" in ns:
        return ns["EMAILS"]
    raise Exception("EMAILS listesi emailler.txt'de bulunamadı!")

PROXIES = load_proxies_from_file(PROXY_FILE)
EMAILS = load_emails_from_file(EMAILS_FILE)

SCRIPTS = [
    "a1.py", "a2.py", "a3.py", "a4.py", "a5.py", "a6.py", "a7.py", "a8.py", "a9.py", "a10.py",
    "a11.py", "a12.py", "a13.py", "a14.py", "a15.py", "a16.py", "a17.py", "a18.py", "a19.py", "a20.py"
]

# ---- YENİ: Proxy kullanımını takip eden set ve lock
proxies_in_use: set[str] = set()
proxy_lock = Lock()

def test_proxy(proxy_url: str) -> bool:
    proxies = {
        "http": proxy_url,
        "https": proxy_url
    }
    try:
        r = requests.get("http://httpbin.org/ip", proxies=proxies, timeout=15)
        if r.status_code == 200:
            return True
    except Exception:
        pass
    return False

def get_working_proxy(proxy_pool, blacklist, proxies_in_use, proxy_lock):
    tries = 0
    while tries < len(proxy_pool):
        candidate = None
        with proxy_lock:
            available = [p for p in proxy_pool if p not in blacklist and p not in proxies_in_use]
            if available:
                candidate = random.choice(available)
                proxies_in_use.add(candidate)
        if not candidate:
            time.sleep(2)
            tries += 1
            continue
        if test_proxy(candidate):
            return candidate
        else:
            with proxy_lock:
                blacklist.add(candidate)
                proxies_in_use.discard(candidate)
            print(f"[!] Proxy çalışmıyor: {candidate}")
            time.sleep(3)
            tries += 1
    return None

def release_proxy(proxy, proxies_in_use, proxy_lock):
    with proxy_lock:
        proxies_in_use.discard(proxy)

def run_script_with_proxy(script, proxy_url, script_id, user_agent, proxies_in_use, proxy_lock, timeout_seconds: int = 180):
    print(f"[{script}] başlatılıyor: {proxy_url} (user_agent: {user_agent})")
    process = subprocess.Popen(
        ["python", script, "--proxy", proxy_url, f"--script_id={script_id}", f"--user_agent={user_agent}"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1
    )
    log_fname = f"log_{script.replace('.py','')}.txt"
    last_output_time = time.time()

    def reader(pipe, tag):
        nonlocal last_output_time
        log_s3key = s3_path(log_fname)
        old_content = ""
        try:
            old_content = s3_download_str(log_s3key)
        except Exception:
            pass
        buffer = old_content
        for line in iter(pipe.readline, ''):
            if not line:
                break
            buffer += f"[{tag}] {line}"
            last_output_time = time.time()
        s3_upload_str(log_s3key, buffer)
        pipe.close()

    t_out = threading.Thread(target=reader, args=(process.stdout, "STDOUT"))
    t_err = threading.Thread(target=reader, args=(process.stderr, "STDERR"))
    t_out.start()
    t_err.start()

    success = False
    kill_reason = None
    while process.poll() is None:
        if time.time() - last_output_time > timeout_seconds:
            print(f"[{script}] {timeout_seconds} sn çıktı gelmedi, process öldürülüyor ve yeni proxy deneniyor.")
            process.kill()
            kill_reason = f"--- {time.strftime('%Y-%m-%d %H:%M:%S')} ---\nProcess kill: uzun süre çıktı yok.\n"
            break
        time.sleep(1)

    t_out.join()
    t_err.join()

    if process.returncode == 0:
        success = True

    log_s3key = s3_path(log_fname)
    old_content = ""
    try:
        old_content = s3_download_str(log_s3key)
    except Exception:
        pass
    if kill_reason:
        old_content += kill_reason
        s3_upload_str(log_s3key, old_content)

    release_proxy(proxy_url, proxies_in_use, proxy_lock)

    if success:
        print(f"[{script}] SORUNSUZ tamamlandı ({proxy_url})")
    else:
        print(f"[{script}] HATA ile bitti! ({proxy_url})")

    return success

def script_launcher_loop(script, script_id, user_agent, proxy_pool, proxy_blacklist, proxies_in_use, proxy_lock):
    max_attempts = 100
    attempts = 0
    while attempts < max_attempts:
        proxy_url = get_working_proxy(proxy_pool, proxy_blacklist, proxies_in_use, proxy_lock)
        if not proxy_url:
            print(f"[{script}] için uygun proxy bulunamadı.")
            return
        time.sleep(3 + random.uniform(0, 2))
        success = run_script_with_proxy(script, proxy_url, script_id, user_agent, proxies_in_use, proxy_lock)
        if success:
            break
        else:
            print(f"[{script}] Proxy HATALI: {proxy_url}. Yeni proxy deneniyor.")
            attempts += 1
            time.sleep(4 + random.uniform(0, 2))
    if attempts >= max_attempts:
        print(f"[{script}] için uygun çalışan proxy bulunamadı, işlemi sonlandırıyor.")

def fill_dates_and_prices_in_ws(ws_dst):
    import yfinance as yf
    import datetime

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
    curr_date = datetime.date.today()
    min_date = curr_date - datetime.timedelta(days=(end_row - start_row) * 30 + 30)

    def get_hist(ticker_code):
        try:
            yf_ticker = yf.Ticker(ticker_code)
            hist = yf_ticker.history(start=min_date.strftime("%Y-%m-%d"), end=(curr_date + datetime.timedelta(days=1)).strftime("%Y-%m-%d"))
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
            this_date = prev_date - datetime.timedelta(days=30)

        def find_price(hist, date):
            search_date = date
            close_price = None
            while close_price is None and search_date >= min_date:
                if hist is not None:
                    row_df = hist[hist['Date'] == search_date]
                    if not row_df.empty and not row_df['Close'].isnull().all():
                        close_price = float(row_df['Close'].iloc[0])
                        break
                search_date -= datetime.timedelta(days=1)
            return close_price

        price_company = find_price(hist_company, this_date)
        price_index = find_price(hist_index, this_date)

        ws_dst[f"A{row}"] = this_date.strftime("%Y-%m-%d")
        ws_dst[f"B{row}"] = price_company if price_company is not None else "Veri yok"
        ws_dst[f"C{row}"] = price_index if price_index is not None else "Veri yok"

        prev_date = this_date

# ---------------------------------------------------------------------------
#  GÜNCELLENEN FONKSİYON: Final2 dosyalarını formülleri bozmadan oluşturur
# ---------------------------------------------------------------------------

def create_final2_files(filtered_tickers=None):
    """
    filtered_tickers: None ise tüm şirketler. Liste ise sadece o şirketler için.
    """
    final_folder = s3_path("Final")
    if not s3_isdir(final_folder):
        print("[create_final2] 'Final' klasörü bulunamadı, atlanıyor.")
        return

    final2_folder = s3_path("Final2")
    # S3'te create etmene gerek yok, yükleyince oluşur

    max_col = column_index_from_string("BG")  # 59
    max_row = 36

    for script_idx, script in enumerate(SCRIPTS, start=1):
        companies_txt = s3_path(f"a{script_idx}.txt")
        template_path = s3_path(f"Companies{script_idx}/donusturucu.xlsx")

        if not s3_file_exists(companies_txt):
            continue
        if not s3_file_exists(template_path):
            print(f"[create_final2] Şablon bulunamadı: {template_path}")
            continue

        # Ticker listesi oku
        text = s3_download_str(companies_txt)
        tickers = [line.split(",")[0].strip().upper() for line in text.splitlines() if "," in line]

        if filtered_tickers is not None:
            tickers = [t for t in tickers if t in filtered_tickers]

        for ticker in tickers:
            src_key = s3_path(f"Final/{ticker}.xlsx")
            if not s3_file_exists(src_key):
                continue

            try:
                wb_src = openpyxl.load_workbook(BytesIO(s3_download_bytes(src_key)), data_only=True)
                ws_src = wb_src.active

                dst_key = s3_path(f"Final2/{ticker}.xlsx")
                # S3 template'ı indirip kopyala
                template_bytes = s3_download_bytes(template_path)
                s3_upload_bytes(dst_key, template_bytes)

                wb_dst = openpyxl.load_workbook(BytesIO(template_bytes), data_only=False)
                ws_dst = wb_dst.active

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

                formul_ns["hesapla_tum_formuller"](ws_dst)

                # Final2 dosyasını tekrar s3'e yükle
                buffer = BytesIO()
                wb_dst.save(buffer)
                buffer.seek(0)
                s3_upload_bytes(dst_key, buffer.read())
                print(f"[create_final2] Kaydedildi: {dst_key}")
            except Exception as e:
                print(f"[create_final2] Hata ({ticker}): {e}")

def get_data_from_excel(filepath, range_tuple):
    wb = openpyxl.load_workbook(BytesIO(s3_download_bytes(filepath)), data_only=True)
    ws = wb.active
    start_col, start_row = range_tuple[0][0], int(range_tuple[0][1:])
    end_col, end_row = range_tuple[1][0], int(range_tuple[1][1:])
    data = []
    for row in ws.iter_rows(min_row=start_row, max_row=end_row,
                            min_col=ord(start_col)-64, max_col=ord(end_col)-64):
        values = [cell.value for cell in row]
        data.append(values)
    return data

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

def upload_changed_to_db(changed_tickers):
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
    for ticker in changed_tickers:
        fpath = s3_path(f"Final2/{ticker}.xlsx")
        if not s3_file_exists(fpath):
            print(f"{ticker}.xlsx Final2’de yok, atlanıyor.")
            continue
        try:
            data = get_data_from_excel(fpath, EXCEL_RANGE)
            insert_data_to_db(cursor, ticker, data)
            conn.commit()

            # --- GENEL FİRMASAL VERİLERİ EKLE ---
            wb = openpyxl.load_workbook(BytesIO(s3_download_bytes(fpath)), data_only=True)
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

# ---------------------------------------------------------------------------
#  ANA FONKSİYON
# ---------------------------------------------------------------------------

def main():
    start_time = time.time()

    before_times = get_final_xlsx_times(s3_path("Final"))

    proxy_blacklist = set()
    proxy_pool = PROXIES.copy()
    threads = []

    for idx, script in enumerate(SCRIPTS):
        if idx < len(EMAILS):
            user_agent = EMAILS[idx]
        else:
            user_agent = EMAILS[-1]
        t = threading.Thread(
            target=script_launcher_loop,
            args=(script, idx + 1, user_agent, proxy_pool, proxy_blacklist, proxies_in_use, proxy_lock)
        )
        t.start()
        threads.append(t)
        time.sleep(2)

    for t in threads:
        t.join()

    elapsed = time.time() - start_time
    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)
    sure_log = f"\nToplam çalışma süresi: {minutes} dakika {seconds} saniye\n"
    print(sure_log)

    for script in SCRIPTS:
        log_fname = f"log_{script.replace('.py','')}.txt"
        log_s3key = s3_path(log_fname)
        try:
            old_content = s3_download_str(log_s3key)
        except Exception:
            old_content = ""
        s3_upload_str(log_s3key, old_content + sure_log)

    after_times = get_final_xlsx_times(s3_path("Final"))
    changed_tickers = []

    for ticker, new_time in after_times.items():
        if ticker not in before_times:
            changed_tickers.append(ticker)
        elif before_times[ticker] != new_time:
            changed_tickers.append(ticker)

    if not changed_tickers:
        print("[main] Final klasöründe hiç değişiklik olmamış, Final2 oluşturulmayacak. Çıkılıyor.")
        return

    print(f"[main] Değişen/eklenen şirketler: {changed_tickers}")
    create_final2_files(filtered_tickers=changed_tickers)

    if changed_tickers:
        print("[main] Final2 excelleri DB’ye yükleniyor...")
        upload_changed_to_db(changed_tickers)

if __name__ == "__main__":
    main()
