"""
TikPulse - End-to-End Live Demonstration Runner (F-20 Requirement)
Fungsi:
  1. Menjalankan Batch Catalog Ingestion (Bronze)
  2. Mengalirkan sample event live TikTok riil ke Bronze -> Silver
  3. Menganalisis ML Intent & Sentimen
  4. Mengagregasi ke Gold Marts (PostgreSQL / SQLite)
  5. Memastikan semua data siap ditampilkan di Live Host Cockpit Dashboard!
"""

import os
import sys
import time
import uuid

# Tambahkan src ke path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from ingestion.batch_loader import ingest_catalog_batch
from pipeline.bronze_to_silver import BronzeToSilverProcessor, SILVER_OUTPUT_DIR
from pipeline.silver_to_gold import aggregate_silver_to_gold
import pandas as pd

def run_full_pipeline_demo():
    print("\n" + "="*60)
    print("🚀 MEMULAI DEMONSTRASI PIPELINE END-TO-END TIKPULSE")
    print("="*60)

    # 1. BATCH INGESTION (F-02)
    catalog_csv = os.path.join(os.path.dirname(__file__), "..", "data", "batch", "catalog_products.csv")
    print(f"\n[STEP 1] Menjalankan Batch Ingest Katalog Produk...")
    batch_res = ingest_catalog_batch(catalog_csv, force_reload=True)
    print(f" -> Status: {batch_res.get('status')} | Rows: {batch_res.get('row_count')}")

    # 2. STREAM SIMULATION (F-04, F-05, F-06, F-12)
    print(f"\n[STEP 2] Memproses Aliran Komentar & Saweran Live (Bronze -> Silver)...")
    processor = BronzeToSilverProcessor()
    
    sample_events = [
        {"type": "comment", "user": "Dinda_Style", "text": "Kak tolong spill kemeja flanel no 1 dong mau checkout!"},
        {"type": "comment", "user": "Rian_Fashion", "text": "Etalase 1 warna hitam ada ukuran XL gak min?"},
        {"type": "comment", "user": "Budi_Santoso", "text": "Mau ambil kemeja flanel no 1 bisa COD kak?"},
        {"type": "comment", "user": "Siti_Bandung", "text": "Spill celana chino nomor 3 kak mau beli"},
        {"type": "comment", "user": "Andi_Pratama", "text": "Celana chino no 3 bahannya melar gak kak?"},
        {"type": "comment", "user": "Maya_Kurnia", "text": "Kemeja putih nomor 2 ready stok min?"},
        {"type": "comment", "user": "Jessica_99", "text": "Dress nomor 5 bagus banget mau order"},
        {"type": "comment", "user": "Fajar_Surabaya", "text": "Halo kak salam dari Surabaya"},
        {"type": "comment", "user": "Rizky_Distro", "text": "Topi no 10 ada warna navy gak?"},
        {"type": "comment", "user": "Dewi_Pelanggan", "text": "Kak paket kemarin robek jahitannya tolong ditukar ya"},
        {"type": "gift", "user": "Sultan_Jakarta", "gift_name": "Paus", "diamond": 2150},
        {"type": "gift", "user": "Juragan_Surabaya", "gift_name": "Cap", "diamond": 99},
        {"type": "gift", "user": "Dinda_Style", "gift_name": "Rose", "diamond": 10}
    ]

    silver_buffer = []
    session_id = f"demo_live_{int(time.time())}"
    now_iso = pd.Timestamp.now(tz="UTC").isoformat()

    for item in sample_events:
        evt_id = f"evt_{uuid.uuid4().hex[:10]}"
        if item["type"] == "comment":
            raw = {
                "event_id": evt_id,
                "session_id": session_id,
                "streamer": "@tokobajumurah",
                "event_type": "comment",
                "event_time": now_iso,
                "data": {
                    "user_id": f"u_{hash(item['user']) % 10000}",
                    "nickname": item["user"],
                    "comment_text": item["text"]
                }
            }
        else:
            raw = {
                "event_id": evt_id,
                "session_id": session_id,
                "streamer": "@tokobajumurah",
                "event_type": "gift",
                "event_time": now_iso,
                "data": {
                    "user_id": f"u_{hash(item['user']) % 10000}",
                    "nickname": item["user"],
                    "gift_name": item["gift_name"],
                    "diamond_count": item["diamond"],
                    "repeat_count": 1
                }
            }

        rec = processor.process_single_event(raw)
        if rec:
            silver_buffer.append(rec)
            print(f" -> [PROCESSED] {rec['event_type']} | Etalase: #{rec['extracted_etalase']} | ML: {rec['ml_intent']} ({rec['ml_confidence_score']})")

    # Simpan ke Parquet Silver
    df_silver = pd.DataFrame(silver_buffer)
    parquet_filename = f"silver_events_demo_{int(time.time())}.parquet"
    parquet_path = os.path.join(SILVER_OUTPUT_DIR, parquet_filename)
    df_silver.to_parquet(parquet_path, engine="pyarrow", index=False)
    print(f"\n[STEP 3] Menyimpan {len(silver_buffer)} event ke Parquet Silver:")
    print(f" -> Lokasi: {parquet_path}")

    # 3. AGGREGATION TO GOLD (F-06, F-10)
    print(f"\n[STEP 4] Mengagregasi data ke Gold Data Marts...")
    aggregate_silver_to_gold()

    print("\n" + "="*60)
    print("✅ DEMO PIPELINE SELESAI DENGAN SUKSES!")
    print("="*60)
    print("Buka browser Anda sekarang di:")
    print("👉 Web Frontend Live Cockpit : http://localhost:8000")
    print("👉 Swagger Interactive API    : http://localhost:8000/docs")
    print("="*60 + "\n")

if __name__ == "__main__":
    run_full_pipeline_demo()
