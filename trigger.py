import os
import time
import boto3
import runpy
import sys
from botocore.client import Config

sys.stdout.reconfigure(line_buffering=True)

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET")
AWS_REGION = os.getenv("AWS_REGION") or "eu-central-1"
ENDPOINT_URL = f"https://s3.{AWS_REGION}.wasabisys.com"
S3_BUCKET = "alaybey"

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
    endpoint_url=ENDPOINT_URL,
    config=Config(signature_version="s3v4"),
)

def trigger_exists():
    try:
        s3.head_object(Bucket=S3_BUCKET, Key="trigger.txt")
        return True
    except Exception:
        return False

def delete_trigger(key):
    try:
        s3.delete_object(Bucket=S3_BUCKET, Key=key)
    except Exception:
        pass

def run_y_oto():
    try:
        print(">>> y_oto.py başlatılıyor...")
        runpy.run_path("y_oto.py", run_name="__main__")
        print(">>> y_oto.py tamamlandı.")
    except Exception as e:
        print("y_oto.py çalıştırılamadı:", e)

def main():
    print("trigger.py aktif. S3’te trigger.txt bekleniyor…")
    is_running = False
    while True:
        if not is_running and trigger_exists():
            is_running = True
            print("Tetikleyici bulundu: trigger.txt")
            try:
                run_y_oto()
            finally:
                delete_trigger("trigger.txt")
                print("trigger.txt silindi.")
            is_running = False
        time.sleep(5)

if __name__ == "__main__":
    main()
