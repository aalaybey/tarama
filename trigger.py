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

def get_trigger_keys():
    try:
        resp = s3.list_objects_v2(Bucket=S3_BUCKET)
        keys = [obj['Key'] for obj in resp.get('Contents', [])]
        print("S3'teki dosyalar:", keys)
        matching_keys = [k for k in keys if 'trigger.txt' in k]
        return matching_keys
    except Exception as e:
        print("S3 list hatası:", e)
        return []



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
    print("trigger.py aktif. S3’te trigger*.txt bekleniyor…")
    is_running = False
    while True:
        keys = get_trigger_keys()
        if not is_running and keys:
            is_running = True
            for key in keys:
                print(f"Tetikleyici bulundu: {key}")
                try:
                    run_y_oto()
                finally:
                    delete_trigger(key)
                    print(f"{key} silindi.")
            is_running = False
        time.sleep(5)

if __name__ == "__main__":
    main()
