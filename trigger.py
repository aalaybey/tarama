import boto3
import os
import time
import subprocess

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET")
AWS_REGION = os.getenv("AWS_REGION") or "eu-central-1"
S3_BUCKET = "alaybey"
S3_PREFIX = "s3/"
TRIGGER_KEY = f"{S3_PREFIX}trigger.txt"

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)


def trigger_exists():
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=TRIGGER_KEY)
        return True
    except Exception:
        return False


def delete_trigger():
    try:
        s3.delete_object(Bucket=S3_BUCKET, Key=TRIGGER_KEY)
    except Exception:
        pass


def run_y_oto():
    try:
        print(">>> y_oto.py başlatılıyor...")

        # un-buffered çalıştır, çıktıları anında al
        proc = subprocess.Popen(
            ["python", "-u", "y_oto.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        for line in proc.stdout:
            print(line, end="", flush=True)

        proc.wait()
        print(">>> y_oto.py tamamlandı.")
    except Exception as e:
        print("y_oto.py çalıştırılamadı:", e)


def main():
    print("trigger.py aktif. S3 trigger bekleniyor.")
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
