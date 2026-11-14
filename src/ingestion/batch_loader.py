"""
TikPulse - Batch Ingestion Engine (F-02 Requirement)
Fungsi: Memuat katalog produk toko (CSV) ke Bronze Layer (MinIO Object Storage)
Fitur: Idempotent (anti-duplikasi ingest), Checksum MD5, Metadata Tracking (batch_id, ingest_time, row_count)
"""

import os
import sys
import hashlib
import uuid
import json
from datetime import datetime, timezone
import pandas as pd

try:
    import boto3
    from botocore.client import Config
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

# Konfigurasi Environment
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio_secure_password_2026")
MINIO_BUCKET = os.getenv("MINIO_BUCKET_NAME", "tikpulse-lakehouse")

def calculate_checksum(file_path: str) -> str:
    """Menghitung MD5 checksum untuk verifikasi integritas data."""
    hasher = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hasher.update(chunk)
    return hasher.hexdigest()

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1"
    )

def ingest_catalog_batch(csv_path: str, force_reload: bool = False) -> dict:
    """
    Ingest katalog CSV ke MinIO Bronze Layer secara Idempotent.
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"File katalog tidak ditemukan di: {csv_path}")

    # 1. Hitung Checksum & Row Count
    file_checksum = calculate_checksum(csv_path)
    df = pd.read_csv(csv_path)
    row_count = len(df)
    batch_id = str(uuid.uuid4())
    ingest_time = datetime.now(timezone.utc).isoformat()

    # 2. Coba MinIO, jika gagal gunakan Local File Storage (Bronze Lakehouse)
    use_minio = HAS_BOTO3
    if use_minio:
        try:
            s3 = get_s3_client()
            s3.head_bucket(Bucket=MINIO_BUCKET)
        except Exception as e:
            use_minio = False
            print(f"[STORAGE FALLBACK] MinIO tidak terjangkau ({e}). Menggunakan Local Bronze Storage.")
    else:
        print("[STORAGE FALLBACK] Boto3 belum terinstal. Menggunakan Local Bronze Storage.")

    local_bronze_dir = os.path.join(os.path.dirname(__file__), "..", "..", "data", "bronze", "catalog")
    os.makedirs(local_bronze_dir, exist_ok=True)
    local_meta_path = os.path.join(local_bronze_dir, "metadata_latest.json")

    # Idempotency Check
    if not force_reload:
        if use_minio:
            try:
                resp = s3.get_object(Bucket=MINIO_BUCKET, Key="bronze/catalog/metadata_latest.json")
                existing_meta = json.loads(resp["Body"].read().decode("utf-8"))
                if existing_meta.get("checksum") == file_checksum:
                    print(f"[IDEMPOTENT] Dataset dengan checksum {file_checksum} sudah pernah di-ingest ke MinIO. Lewati.")
                    return {"status": "SKIPPED_ALREADY_INGESTED", "checksum": file_checksum}
            except Exception:
                pass
        elif os.path.exists(local_meta_path):
            with open(local_meta_path, "r") as f:
                existing_meta = json.load(f)
                if existing_meta.get("checksum") == file_checksum:
                    print(f"[IDEMPOTENT] Dataset dengan checksum {file_checksum} sudah pernah di-ingest lokal. Lewati.")
                    return {"status": "SKIPPED_ALREADY_INGESTED", "checksum": file_checksum}

    # Simpan ke Bronze Storage
    bronze_filename = f"catalog_{batch_id}.csv"
    manifest = {
        "status": "SUCCESS",
        "batch_id": batch_id,
        "source_file": os.path.basename(csv_path),
        "target_file": bronze_filename,
        "checksum": file_checksum,
        "row_count": row_count,
        "ingest_time": ingest_time,
        "storage_mode": "MINIO_S3" if use_minio else "LOCAL_BRONZE"
    }

    if use_minio:
        with open(csv_path, "rb") as f:
            s3.put_object(Bucket=MINIO_BUCKET, Key=f"bronze/catalog/{bronze_filename}", Body=f)
        s3.put_object(Bucket=MINIO_BUCKET, Key="bronze/catalog/metadata_latest.json", Body=json.dumps(manifest, indent=2))
    else:
        import shutil
        target_csv_path = os.path.join(local_bronze_dir, bronze_filename)
        shutil.copyfile(csv_path, target_csv_path)
        with open(local_meta_path, "w") as f:
            json.dump(manifest, f, indent=2)

    print(f"[SUCCESS] Ingest Batch Berhasil ({manifest['storage_mode']})!")
    print(f" -> Batch ID    : {batch_id}")
    print(f" -> Total Baris : {row_count}")
    print(f" -> Checksum MD5: {file_checksum}")
    return manifest

if __name__ == "__main__":
    # Jalankan secara manual atau via cron
    default_path = os.path.join(os.path.dirname(__file__), "..", "..", "data", "batch", "catalog_products.csv")
    csv_input = sys.argv[1] if len(sys.argv) > 1 else default_path
    ingest_catalog_batch(csv_input)
