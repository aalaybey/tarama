import os
import time
import boto3
import runpy
import sys
from botocore.client import Config      # ← EKLE

# ── stdout’u satır satır anında Render’a gönder ──
sys.stdout.reconfigure(line_buffering=True)

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS")   # Wasabi key’in
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET")
AWS_REGION = os.getenv("AWS_REGION") or "eu-central-1"

# Wasabi endpoint → aynı region’da “s3.<region>.wasabisys.com”
ENDPOINT_URL = f"https://s3.{AWS_REGION}.wasabisys.com"   # ← EKLE

S3_BUCKET = "alaybey"
# S3_PREFIX = "s3/"   # ← KALDIRILDI, artık yok

TRIGGER_KEY = "trigger.txt"    # prefix olmadan direkt bucket kökü

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
    endpoint_url=ENDPOINT_URL,                  # ← EKLE
    config=Config(signature_version="s3v4"),    # ← EKLE (Wasabi zorunlu)
)


def trigger_exists() -> bool:
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=TRIGGER_KEY)
        return True
    except Exception:
        return False


def delete_trigger() -> None:
    try:
        s3.delete_object(Bucket=S3_BUCKET, Key=TRIGGER_KEY)
    except Exception:
        pass


def run_y_oto() -> None:
    """y_oto.py’yi AYNI süreçte çalıştır, tüm print’ler direkt log’da görünür."""
    try:
        print(">>> y_oto.py başlatılıyor...")
        # y_oto.py içindeki __main__ bloğu da çalışsın
        runpy.run_path("y_oto.py", run_name="__main__")
        print(">>> y_oto.py tamamlandı.")
    except Exception as e:
        print("y_oto.py çalıştırılamadı:", e)


def main() -> None:
    print("trigger.py aktif. S3’te trigger.txt bekleniyor…")
    is_running = False
    while True:
        if not is_running and trigger_exists():
            is_running = True
            delete_trigger()
            run_y_oto()
            is_running = False
        time.sleep(5)  # 5 saniyede bir kontrol


if __name__ == "__main__":
    main()
