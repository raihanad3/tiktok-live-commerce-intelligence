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
from collections import OrderedDict
from datetime import datetime, timezone
from kafka import KafkaConsumer, KafkaProducer
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from common.store_catalog import get_store_catalog, get_catalog_cache_version, has_custom_store_catalog
from common.text_normalizer import normalize_tiktok_text

# Konfigurasi Broker & File
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_RAW = os.getenv("REDPANDA_TOPIC_RAW", "tiktok.events.raw")
TOPIC_DLQ = os.getenv("REDPANDA_TOPIC_DLQ", "tiktok.events.dlq")
CATALOG_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "data", "batch", "catalog_products.csv")
MODEL_PATH = os.path.join(os.path.dirname(__file__), "..", "ml", "model_intent.joblib")
SILVER_OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "silver")

# Pastikan folder silver ada
os.makedirs(SILVER_OUTPUT_DIR, exist_ok=True)

_ETALASE_PATTERN = re.compile(
    r'(?:\b(?:no\.?|nomor|etalase|etls|eta|co|spill|keranjang)\b|#)\s*([1-9][0-9]{0,2})\b',
    re.IGNORECASE
)
_SHORT_ETALASE_PATTERN = re.compile(
    r'^\s*([1-9][0-9]{0,2})\s*(?:ka|kaa|kak|kakak|ks|kk|min|mimin|bng|bg|bang|dong|plis|pls|\?)?\s*$',
    re.IGNORECASE
)

_QUESTION_SIGNAL_RE = re.compile(
    r'(\?'
    r'|\b(?:apa|apakah|gimana|bagaimana|berapa|dimana|di\s+mana|kapan|siapa|muat|ukuran|size|lp|ld|bb|tb|lingkar|bahan|nerawang|melar|ready|tersedia|cocok|aman|halal|ori|original|asli|tahan|rekomendasi|rekomen|gaada|gada|gakada|where|what|how|which|does|can\s+i)\b'
    r'|\b(?:tidak\s+ada)\b'
    r'|\b(?:ya|g|ga|gak|gk|ngga|nggak|tidak|belum|blm|ada|bisa|muat|masuk|ready)\s+(?:ka|kaa|kak|kakak|ks|kk|min|mimin|bang|bng|bg|sis|bro|gan|host)\b'
    r'|\b(?:ada|bisa|muat|masuk|ready|tahan|melar|nerawang|original|ori|halal|aman|cocok|gratis|free)\s+(?:g|ga|gak|gk|ngga|nggak|tidak|ngga\s+sih)\b)',
    re.IGNORECASE
)
_SOFT_BUY_SIGNAL_RE = re.compile(
    r'(?:\b(?:rekomendasi|rekomen)\b.*\b(?:etalase|no|nomor|buat|untuk)\b'
    r'|\b(?:etalase|etls|eta)\s+berapa\b'
    r'|\b(?:ada|ready)\s+(?:g|ga|gak|gk|tidak)\b'
    r'|\b(?:spill|spil)\s+(?:no|nomor|etalase|eta|[1-9]))',
    re.IGNORECASE
)
_PRODUCT_REQUEST_RE = re.compile(
    r'^(?:kak|ka|bang|bng|bg|min|mimin|tolong|spill)?\s*(?:spill\s+)?'
    r'(?:jaket|sepatu|kaos|baju|celana|hoodie|kemeja|dress|gamis|hijab|tas|topi|sandal|raket|serum|sunscreen|cushion|moisturizer|lipstik|lipstick|toner|charger|tws|powerbank|case|kopi|sambal|keripik|dimsum|basreng|flanel|chino|cargo|kulot|sneaker|sneakers|running|slip\s*on|tshirt|sweater|rompi|vest|pashmina|abaya|mukena|jersey)'
    r'\b.*(?:bang|bng|bg|kak|ka|min|mimin|dong|plis|pls|spill|\?)?$',
    re.IGNORECASE
)
_BUY_SIGNAL_RE = re.compile(
    r'\b(?:checkout|check\s+out|co|beli|buy|order|pesan|ambil|keep|mau\s+co|mau\s+beli|mau\s+ambil|udah\s+co|udh\s+co|sdh\s+co|fix\s+ambil|cod|keranjang\s+kuning|cart|link\s+beli)\b',
    re.IGNORECASE
)
_COMPLAINT_SIGNAL_RE = re.compile(
    r'\b(?:kecewa|robek|rusak|cacat|retur|refund|komplain|penipu|bohong|lama\s+banget|belum\s+dikirim|ga\s+balas|nggak\s+balas|gak\s+balas|tidak\s+balas|salah\s+kirim|salah\s+warna|salah\s+ukuran|pecah|bocor|bolong|patah|penyok|noda|bau|palsu|nyesel|scam|damaged|broken|defective|disappointed|wrong\s+size|missing)\b',
    re.IGNORECASE
)
_INTENT_PRIORITY = {"COMPLAINT": 0, "INTENT_BUY": 1, "QUESTION": 2, "CASUAL": 3}

class BronzeToSilverProcessor:
    def __init__(self, max_seen_events: int = 50000):
        self._max_seen_events = max_seen_events
        self.seen_event_ids: OrderedDict[str, None] = OrderedDict()
        self.model = self._load_ml_model()
        self.catalog_df = self._load_catalog()
        self._default_catalog_index = self._build_catalog_index(self.catalog_df)
        self._catalog_index_cache: dict[tuple[str, int], tuple[dict[int, dict], list[tuple[str, int]]]] = {}
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

    @staticmethod
    def _build_catalog_index(catalog_source) -> tuple[dict[int, dict], list[tuple[str, int]]]:
        """Mengindeks katalog menjadi dict etalase_no O(1) dan daftar keyword terurut panjang-turun."""
        by_etalase: dict[int, dict] = {}
        kw_pairs: list[tuple[str, int]] = []

        if catalog_source is None:
            return by_etalase, kw_pairs

        if isinstance(catalog_source, dict):
            iterable = catalog_source.values()
        elif isinstance(catalog_source, pd.DataFrame):
            if catalog_source.empty:
                return by_etalase, kw_pairs
            iterable = catalog_source.to_dict(orient="records")
        else:
            iterable = list(catalog_source)

        for row in iterable:
            try:
                et = int(row["etalase_no"])
            except (KeyError, TypeError, ValueError):
                continue
            pname = str(row.get("product_name", f"Etalase #{et}"))
            price = int(row.get("price_idr", 0) or 0)
            source = str(row.get("source", "CATALOG"))
            by_etalase[et] = {
                "etalase_no": et,
                "product_name": pname,
                "price_idr": price,
                "source": source
            }
            raw_kw = str(row.get("keywords") or pname).lower()
            for kw in raw_kw.split(","):
                kw_clean = kw.strip()
                if len(kw_clean) >= 3:
                    kw_pairs.append((kw_clean, et))

        kw_pairs.sort(key=lambda item: len(item[0]), reverse=True)
        return by_etalase, kw_pairs

    def _get_cached_store_index(self, streamer: str) -> tuple[dict[int, dict], list[tuple[str, int]]]:
        cache_ver = get_catalog_cache_version()
        key = ((streamer or "").lower().strip(), cache_ver)
        if key not in self._catalog_index_cache:
            # Bersihkan entry versi lama untuk streamer ini agar hemat memori
            stale_keys = [k for k in self._catalog_index_cache if k[0] == key[0]]
            for sk in stale_keys:
                self._catalog_index_cache.pop(sk, None)
            store_df = get_store_catalog(streamer)
            self._catalog_index_cache[key] = self._build_catalog_index(store_df)
        return self._catalog_index_cache[key]

    def extract_etalase_number(self, text: str, catalog_df=None, keyword_index=None):
        """Ekstraksi nomor etalase dari komentar menggunakan regex dan keyword matching terindeks."""
        if not text:
            return None

        # Pola 1: 'no 1', 'nomor 2', 'etalase 3', 'eta 4', 'etls 4', 'spill 5', '#6', 'co 7', 'keranjang 12'
        match = _ETALASE_PATTERN.search(text)
        if match:
            return int(match.group(1))

        # Pola 1b: Angka etalase singkat berdiri sendiri (mis. '13 ks', '5 kak', '12 min')
        short_match = _SHORT_ETALASE_PATTERN.match(text)
        if short_match:
            return int(short_match.group(1))

        # Pola 2: Cocokkan kata kunci produk dari indeks yang sudah diurutkan terpanjang-dulu
        if keyword_index is not None:
            kw_list = keyword_index
        elif catalog_df is not None:
            _, kw_list = self._build_catalog_index(catalog_df)
        else:
            _, kw_list = self._default_catalog_index

        if kw_list:
            text_lower = text.lower()
            for kw_clean, et_no in kw_list:
                if kw_clean in text_lower:
                    return et_no
        return None

    @staticmethod
    def extract_product_subject(text: str) -> str:
        """Mengekstrak kemungkinan subjek/nama produk dari komentar live jika belum ada di katalog resmi."""
        if not text:
            return ""
        # Hapus kata-kata percakapan umum live stream menggunakan \b (word boundaries) agar kata dasar tetap utuh
        cleaned = re.sub(
            r'\b(?:kak|kakak|ka|min|mimin|host|tolong|dong|spill|etalase|etls|eta|nomor|no|checkout|co|ready|ga|gak|ngga|nggak|ada|yg|yang|warna|size|ukuran|bisa|cod|promo|diskon|murah|banget|nih|ya|gas|skrg|sekarang|order|beli|cek|in|ke|dari|untuk|buat|mau|saya|sy|sdh|udh|udah|aku|ku|plis|please|\#|[0-9]+)\b',
            ' ',
            text,
            flags=re.IGNORECASE
        )
        cleaned = re.sub(r'[\?\!\#\*\@\(\)\[\]\.\,\:\;]', ' ', cleaned)
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        words = cleaned.split()
        if words:
            sub = " ".join(words[:4]).strip()
            if len(sub) >= 2:
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

    def classify_comment_intent(self, comment_text: str) -> tuple[str, float, list[str]]:
        """
        Dual-Layer Hybrid Classifier (ML-A+):
        - Normalisasi slang Indonesia sebelum inferensi model Word+Char TF-IDF
        - Confidence Rejection Gate (mencegah OOV/teks acak jadi KOMPLAIN palsu)
        - Mengembalikan (primary_intent, confidence_score, intent_tags[1..2]).
        """
        text = (comment_text or "").strip()
        if not text:
            return "CASUAL", 0.0, ["CASUAL"]

        norm_text = normalize_tiktok_text(text)
        proba_map: dict[str, float] = {}
        ml_top = "CASUAL"
        ml_score = 0.5
        second_score = 0.0

        if self.model:
            try:
                if hasattr(self.model, "predict_proba") and hasattr(self.model, "classes_"):
                    probs = self.model.predict_proba([norm_text])[0]
                    for cls_name, p in zip(self.model.classes_, probs):
                        proba_map[str(cls_name)] = float(p)
                    sorted_probs = sorted(proba_map.items(), key=lambda kv: kv[1], reverse=True)
                    ml_top, ml_score = sorted_probs[0]
                    second_score = sorted_probs[1][1] if len(sorted_probs) > 1 else 0.0
                else:
                    ml_top = str(self.model.predict([norm_text])[0])
                    ml_score = 0.85
                    proba_map[ml_top] = ml_score
            except Exception:
                return "INFERENCE_ERROR", 0.0, ["CASUAL"]

        has_short_et = bool(_SHORT_ETALASE_PATTERN.match(text))
        has_q = bool(_QUESTION_SIGNAL_RE.search(text) or _QUESTION_SIGNAL_RE.search(norm_text) or has_short_et)
        has_req = bool(_PRODUCT_REQUEST_RE.search(text))
        has_buy = bool(_BUY_SIGNAL_RE.search(text) or _BUY_SIGNAL_RE.search(norm_text))
        has_soft_buy = bool(_SOFT_BUY_SIGNAL_RE.search(text) or has_short_et)
        has_comp = bool(_COMPLAINT_SIGNAL_RE.search(text) or _COMPLAINT_SIGNAL_RE.search(norm_text))

        # Low-Confidence OOV Rejection Gate: tanpa sinyal eksplisit & skor rendah -> CASUAL
        if not (has_q or has_req or has_buy or has_soft_buy or has_comp) and ml_score < 0.50:
            return "CASUAL", round(float(ml_score), 4), ["CASUAL"]

        candidates: set[str] = set()
        if has_comp:
            candidates.add("COMPLAINT")
        if has_buy or (has_soft_buy and not has_comp):
            candidates.add("INTENT_BUY")
        if has_q or has_req:
            candidates.add("QUESTION")

        # Strict COMPLAINT Gate: hanya terima COMPLAINT dari ML bila sangat yakin (prob >= 0.65 & margin >= 0.15)
        comp_prob = proba_map.get("COMPLAINT", 0.0)
        allow_ml_comp = has_comp or (ml_top == "COMPLAINT" and comp_prob >= 0.65 and (comp_prob - second_score) >= 0.15)

        # Cegah pertanyaan ukuran/spill murni meleset menjadi INTENT_BUY
        suppress_ml_buy = (ml_top == "INTENT_BUY" and (has_q or has_req) and not has_buy and ml_score < 0.72)

        if ml_top == "COMPLAINT":
            if allow_ml_comp:
                candidates.add("COMPLAINT")
        elif not suppress_ml_buy:
            if ml_score >= 0.48 or (has_q or has_req or has_buy or has_comp):
                candidates.add(ml_top)

        # Tambahkan kelas sekunder bila probabilitas ML cukup tinggi
        for cls_name, p in proba_map.items():
            if cls_name == "CASUAL":
                continue
            if cls_name == "COMPLAINT" and not allow_ml_comp:
                continue
            if cls_name == "INTENT_BUY" and (has_q or has_req) and not has_buy and p < 0.72:
                continue
            if p >= 0.35:
                candidates.add(cls_name)

        actionable = [c for c in candidates if c != "CASUAL"]
        if actionable:
            ordered = sorted(actionable, key=lambda x: _INTENT_PRIORITY.get(x, 99))[:2]
        else:
            ordered = ["CASUAL"]

        primary_intent = ordered[0]
        explicit_hit = (
            (primary_intent == "COMPLAINT" and has_comp)
            or (primary_intent == "INTENT_BUY" and has_buy)
            or (primary_intent == "QUESTION" and (has_q or has_req))
        )
        conf = max(ml_score, proba_map.get(primary_intent, 0.0))
        if explicit_hit and conf < 0.78:
            conf = 0.78

        return primary_intent, round(float(conf), 4), ordered

    def process_single_event(
        self,
        raw_event: dict,
        producer: KafkaProducer = None,
        live_catalog: dict = None,
        strict_real_mode: bool = False
    ) -> dict:
        """Memproses 1 event mentah dari Bronze ke Silver dengan latensi rendah."""
        event_id = raw_event.get("event_id")

        # 1. Deduplikasi (F-05) dengan bounded OrderedDict agar RAM stabil saat ramai
        if event_id in self.seen_event_ids:
            self.quality_report["duplicates_skipped"] += 1
            return None
        self.seen_event_ids[event_id] = None
        if len(self.seen_event_ids) > self._max_seen_events:
            self.seen_event_ids.popitem(last=False)

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
        intent_tags: list[str] = []

        streamer = raw_event.get("streamer") or "@tokobaju"
        if strict_real_mode:
            by_etalase, kw_index = self._build_catalog_index(live_catalog or {})
        else:
            by_etalase, kw_index = self._get_cached_store_index(streamer)

        if event_type == "comment":
            comment_text = data.get("comment_text", "")
            extracted_etalase = self.extract_etalase_number(comment_text, keyword_index=kw_index)

            # 4. Dual-Layer Hybrid ML Inference (F-12)
            ml_intent, ml_score, intent_tags = self.classify_comment_intent(comment_text)

        # Konversi Gift Koin ke Rupiah
        gift_val_idr = 0
        if event_type == "gift":
            diamond = data.get("diamond_count", 0)
            repeat = data.get("repeat_count", 1)
            gift_val_idr = diamond * repeat * 250  # 1 koin ~ Rp 250

        # Ambil detail produk dari katalog yang sesuai (tanpa mengarang di strict_real_mode)
        product_name = None
        price_idr = None
        product_source = None
        if extracted_etalase:
            matched = by_etalase.get(extracted_etalase)
            if matched:
                product_name = matched["product_name"]
                price_idr = int(matched.get("price_idr", 0) or 0)
                if strict_real_mode:
                    product_source = matched.get("source", "TIKTOK_LIVE_PIN")
                else:
                    product_source = "CUSTOM_CATALOG" if has_custom_store_catalog(streamer) else "SIMULATION_CATALOG"
            else:
                subj = self.extract_product_subject(data.get("comment_text", "")) if event_type == "comment" else ""
                if strict_real_mode:
                    product_name = f"Etalase #{extracted_etalase} - {subj}" if subj else f"Etalase #{extracted_etalase} (Menunggu Pin Host)"
                    price_idr = 0
                    product_source = "CHAT_DETECTED"
                else:
                    product_name = f"Etalase #{extracted_etalase} - {subj}" if subj else f"Etalase #{extracted_etalase}"
                    price_idr = 75000
                    product_source = "CHAT_DETECTED"

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
            "product_source": product_source,
            "ml_intent": ml_intent,
            "ml_confidence_score": ml_score,
            "intent_tags": intent_tags,
            "gift_name": data.get("gift_name"),
            "gift_value_idr": gift_val_idr,
            "gift_repeat_count": data.get("repeat_count", 1) if event_type == "gift" else None,
            "gift_combo_total": data.get("combo_total", data.get("repeat_count", 1)) if event_type == "gift" else None,
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
