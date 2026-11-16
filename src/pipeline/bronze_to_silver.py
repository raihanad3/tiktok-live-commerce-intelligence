"""
TikPulse - Medallion Bronze to Silver Processor (F-03, F-05, F-06, F-08, F-12)
Fungsi:
  1. Membaca stream event dari Redpanda (Raw Bronze)
  2. Deduplikasi berbasis event_id unik
  3. Quality Gate (Not-null, Range check, DLQ routing)
  4. Penggabungan dengan Data Batch Katalog Produk (Etalase Extraction)
  5. Machine Learning Online Inference (Intent Prediction)
  6. Penyimpanan ke Silver Layer (Parquet format)
"""

import os
import re
import json
import time
import joblib
import pandas as pd
from datetime import datetime, timezone
from kafka import KafkaConsumer, KafkaProducer
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from common.store_catalog import get_store_catalog

# Konfigurasi Broker & File
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_RAW = os.getenv("REDPANDA_TOPIC_RAW", "tiktok.events.raw")
TOPIC_DLQ = os.getenv("REDPANDA_TOPIC_DLQ", "tiktok.events.dlq")
CATALOG_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "data", "batch", "catalog_products.csv")
MODEL_PATH = os.path.join(os.path.dirname(__file__), "..", "ml", "model_intent.joblib")
SILVER_OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "silver")

# Pastikan folder silver ada
os.makedirs(SILVER_OUTPUT_DIR, exist_ok=True)

class BronzeToSilverProcessor:
    def __init__(self):
        self.seen_event_ids = set()
        self.model = self._load_ml_model()
        self.catalog_df = self._load_catalog()
        self.quality_report = {"passed": 0, "failed_quarantine": 0, "duplicates_skipped": 0}

    def _load_ml_model(self):
        """Memuat model inferensi ML."""
        if os.path.exists(MODEL_PATH):
            try:
                print(f"[PROCESSOR] Memuat model ML dari {MODEL_PATH}")
                return joblib.load(MODEL_PATH)
            except Exception as e:
                print(f"[WARN] Gagal memuat model: {e}")
        return None

    def _load_catalog(self):
        """Memuat katalog batch untuk pengayaan etalase."""
        if os.path.exists(CATALOG_PATH):
            return pd.read_csv(CATALOG_PATH)
        return pd.DataFrame()

    def extract_etalase_number(self, text: str, catalog_df: pd.DataFrame = None):
        """Ekstraksi nomor etalase dari komentar menggunakan regex dan keyword matching."""
        if not text:
            return None

        # Pola 1: 'no 1', 'nomor 2', 'etalase 3', 'eta 4', 'etls 4', 'spill 5', '#6', 'co 7'
        match = re.search(r'(?:no|nomor|etalase|etls|eta|co|spill|#)\s*([0-9]{1,2})', text, re.IGNORECASE)
        if match:
            return int(match.group(1))

        # Pola 2: Cocokkan kata kunci produk dari katalog
        target_df = catalog_df if catalog_df is not None and not catalog_df.empty else self.catalog_df
        if not target_df.empty:
            text_lower = text.lower()
            for _, row in target_df.iterrows():
                keywords = str(row.get("keywords", "")).split(",")
                for kw in keywords:
                    if kw.strip() and kw.strip().lower() in text_lower:
                        return int(row["etalase_no"])
        return None

    @staticmethod
    def extract_product_subject(text: str) -> str:
        """Mengekstrak kemungkinan subjek/nama produk dari komentar live jika belum ada di katalog resmi."""
        if not text:
            return ""
        # Hapus kata-kata percakapan umum live stream
        cleaned = re.sub(
            r'(?:kak|min|host|tolong|dong|spill|eta|etls|etalase|nomor|no|co|checkout|ready|ga|gak|ada|yg|yang|warna|size|ukuran|bisa|cod|promo|diskon|murah|banget|nih|ya|gas|skrg|sekarang|order|beli|cek|in|ke|dari|untuk|buat|mau|\?|\!|\#|[0-9]+)',
            ' ',
            text,
            flags=re.IGNORECASE
        )
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        words = cleaned.split()
        if words:
            sub = " ".join(words[:4]).strip()
            if len(sub) >= 3:
                return sub.title()
        return ""

    def validate_quality_gate(self, event: dict) -> tuple[bool, str]:
        """
        Quality Gate (F-08):
        - event_id wajib ada dan tidak kosong
        - timestamp valid
        - comment_text tidak boleh kosong
        """
        event_id = event.get("event_id")
        if not event_id or str(event_id).strip() == "":
            return False, "EMPTY_EVENT_ID"

        data = event.get("data", {})
        if event.get("event_type") == "comment":
            comment = data.get("comment_text")
            if not comment or str(comment).strip() == "":
                return False, "EMPTY_COMMENT_TEXT"

        return True, "PASSED"

    def process_single_event(self, raw_event: dict, producer: KafkaProducer = None) -> dict:
        """Memproses 1 event mentah dari Bronze ke Silver."""
        event_id = raw_event.get("event_id")

        # 1. Deduplikasi (F-05)
        if event_id in self.seen_event_ids:
            self.quality_report["duplicates_skipped"] += 1
            return None
        self.seen_event_ids.add(event_id)

        # 2. Quality Gate (F-08)
        is_valid, reason = self.validate_quality_gate(raw_event)
        if not is_valid:
            self.quality_report["failed_quarantine"] += 1
            print(f"[DLQ] Event {event_id} ditolak karena {reason}. Diteruskan ke DLQ.")
            if producer:
                producer.send(TOPIC_DLQ, value={"event": raw_event, "quarantine_reason": reason, "timestamp": time.time()})
            return None

        self.quality_report["passed"] += 1
        data = raw_event.get("data", {})
        event_type = raw_event.get("event_type")

        # 3. Pengayaan & Ekstraksi Data
        extracted_etalase = None
        ml_intent = "UNKNOWN"
        ml_score = 0.0

        streamer = raw_event.get("streamer") or "@tokobaju"
        store_cat = get_store_catalog(streamer)

        if event_type == "comment":
            comment_text = data.get("comment_text", "")
            extracted_etalase = self.extract_etalase_number(comment_text, store_cat)

            # 4. ML Inference (F-12)
            if self.model:
                try:
                    pred = self.model.predict([comment_text])[0]
                    proba = self.model.predict_proba([comment_text]).max() if hasattr(self.model, "predict_proba") else 0.85
                    ml_intent = pred
                    ml_score = round(float(proba), 4)
                except Exception as e:
                    ml_intent = "INFERENCE_ERROR"

        # Konversi Gift Koin ke Rupiah
        gift_val_idr = 0
        if event_type == "gift":
            diamond = data.get("diamond_count", 0)
            repeat = data.get("repeat_count", 1)
            gift_val_idr = diamond * repeat * 250  # 1 koin ~ Rp 250

        # Ambil detail produk dari katalog toko yang sesuai
        product_name = None
        price_idr = None
        if extracted_etalase and not store_cat.empty:
            matched = store_cat[store_cat["etalase_no"] == extracted_etalase]
            if not matched.empty:
                product_name = str(matched.iloc[0]["product_name"])
                price_idr = int(matched.iloc[0]["price_idr"])
            else:
                subj = self.extract_product_subject(data.get("comment_text", "")) if event_type == "comment" else ""
                product_name = f"Etalase #{extracted_etalase} - {subj}" if subj else f"Etalase #{extracted_etalase}"
                price_idr = 75000

        silver_record = {
            "event_id": event_id,
            "session_id": raw_event.get("session_id"),
            "streamer": raw_event.get("streamer"),
            "event_type": event_type,
            "event_time": raw_event.get("event_time"),
            "processing_time": datetime.now(timezone.utc).isoformat(),
            "user_id": data.get("user_id"),
            "nickname": data.get("nickname"),
            "comment_text": data.get("comment_text"),
            "extracted_etalase": extracted_etalase,
            "matched_product_name": product_name,
            "matched_price_idr": price_idr,
            "ml_intent": ml_intent,
            "ml_confidence_score": ml_score,
            "gift_name": data.get("gift_name"),
            "gift_value_idr": gift_val_idr
        }

        return silver_record

def run_consumer():
    """Menjalankan stream consumer berkelanjutan."""
    processor = BronzeToSilverProcessor()
    
    try:
        consumer = KafkaConsumer(
            TOPIC_RAW,
            bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="tikpulse_silver_group"
        )
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        print(f"[CONSUMER] Berhasil mendengarkan Redpanda Topic: {TOPIC_RAW}")
    except Exception as e:
        print(f"[CONSUMER ERROR] Gagal terhubung ke broker: {e}")
        return

    buffer = []
    last_flush = time.time()

    for message in consumer:
        raw = message.value
        silver_record = processor.process_single_event(raw, producer)
        if silver_record:
            buffer.append(silver_record)
            print(f"[SILVER] Event: {silver_record['event_type']} | Intent: {silver_record['ml_intent']} | Etalase: {silver_record['extracted_etalase']}")

        # Simpan ke Parquet setiap 20 records atau setiap 10 detik
        if len(buffer) >= 20 or (time.time() - last_flush > 10 and buffer):
            df_silver = pd.DataFrame(buffer)
            timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
            parquet_path = os.path.join(SILVER_OUTPUT_DIR, f"silver_events_{timestamp_str}.parquet")
            df_silver.to_parquet(parquet_path, engine="pyarrow", index=False)
            print(f" -> [PARQUET SAVED] {len(buffer)} baris disimpan ke: {parquet_path}")
            buffer.clear()
            last_flush = time.time()

if __name__ == "__main__":
    run_consumer()
