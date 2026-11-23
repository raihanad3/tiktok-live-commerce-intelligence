"""
Unit Tests for Bronze to Silver Transformation & Quality Gate (F-18 Requirement)
"""

import pytest
import os
import sys

# Tambahkan src ke path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from pipeline.bronze_to_silver import BronzeToSilverProcessor

@pytest.fixture
def processor():
    return BronzeToSilverProcessor()

def test_extract_etalase_number_explicit(processor):
    """Menguji ekstraksi nomor etalase dengan pola kata kunci langsung."""
    assert processor.extract_etalase_number("Kak spill nomor 2 dong") == 2
    assert processor.extract_etalase_number("Etalase 5 ready kak?") == 5
    assert processor.extract_etalase_number("Mau co no 10 warna hitam") == 10
    assert processor.extract_etalase_number("Spill no 1 kak") == 1

def test_extract_etalase_number_fallback_none(processor):
    """Menguji jika komentar tidak mengandung nomor atau nama produk."""
    assert processor.extract_etalase_number("Halo kak semangat live nya") is None
    assert processor.extract_etalase_number("Follback dong kak") is None

def test_quality_gate_valid_comment(processor):
    """Menguji event yang sah harus lolos Quality Gate."""
    valid_event = {
        "event_id": "evt_test_123",
        "event_type": "comment",
        "data": {"comment_text": "Spill no 1 kak", "nickname": "Budi"}
    }
    is_valid, reason = processor.validate_quality_gate(valid_event)
    assert is_valid is True
    assert reason == "PASSED"

def test_quality_gate_empty_event_id(processor):
    """Menguji event tanpa event_id harus ditolak oleh Quality Gate."""
    invalid_event = {
        "event_id": "",
        "event_type": "comment",
        "data": {"comment_text": "Spill no 1 kak"}
    }
    is_valid, reason = processor.validate_quality_gate(invalid_event)
    assert is_valid is False
    assert reason == "EMPTY_EVENT_ID"

def test_quality_gate_empty_comment_text(processor):
    """Menguji komentar kosong harus ditolak ke DLQ."""
    invalid_event = {
        "event_id": "evt_test_456",
        "event_type": "comment",
        "data": {"comment_text": "   "}
    }
    is_valid, reason = processor.validate_quality_gate(invalid_event)
    assert is_valid is False
    assert reason == "EMPTY_COMMENT_TEXT"

def test_deduplication(processor):
    """Menguji penanganan event duplikat (F-05)."""
    event = {
        "event_id": "evt_dup_999",
        "event_type": "comment",
        "data": {"comment_text": "Komentar pertama"}
    }
    # Pertama kali diproses -> harus lolos
    first_res = processor.process_single_event(event)
    assert first_res is not None

    # Kedua kali dengan event_id yang sama -> harus diabaikan (None)
    second_res = processor.process_single_event(event)
    assert second_res is None
    assert processor.quality_report["duplicates_skipped"] == 1

def test_extract_etalase_3_digits_and_no_false_positive(processor):
    """Menguji ekstraksi nomor etalase 1-3 digit dan penolakan false-positive (misal 'coco 5', 'eta 0')."""
    assert processor.extract_etalase_number("Kak spill etalase 105 dong") == 105
    assert processor.extract_etalase_number("Keranjang 24 ready kak?") == 24
    assert processor.extract_etalase_number("coco 5 enak nih") is None
    assert processor.extract_etalase_number("eta 0 min") is None

def test_strict_real_mode_never_invents_fake_catalog_products():
    """Menguji mode strict_real_mode=True tidak pernah meminjam katalog dummy atau harga palsu."""
    proc = BronzeToSilverProcessor()
    generic_event = {
        "event_id": "evt_real_generic_1",
        "session_id": "sess_real_test",
        "streamer": "@somethincofficial",
        "event_type": "comment",
        "data": {"comment_text": "Warna putih ready gak kak?"}
    }
    res1 = proc.process_single_event(generic_event, live_catalog={}, strict_real_mode=True)
    assert res1 is not None
    assert res1["extracted_etalase"] is None
    assert res1["matched_product_name"] is None

    etalase_event = {
        "event_id": "evt_real_eta_12",
        "session_id": "sess_real_test",
        "streamer": "@somethincofficial",
        "event_type": "comment",
        "data": {"comment_text": "Kak spill etalase 12 dong"}
    }
    res2 = proc.process_single_event(etalase_event, live_catalog={}, strict_real_mode=True)
    assert res2 is not None
    assert res2["extracted_etalase"] == 12
    assert res2["matched_price_idr"] == 0
    assert res2["product_source"] == "CHAT_DETECTED"
    assert "Etalase #12" in res2["matched_product_name"]

def test_bounded_deduplication_cache():
    """Menguji batas cache deduplikasi agar memori tidak membengkak saat live ramai."""
    proc = BronzeToSilverProcessor(max_seen_events=5)
    for i in range(8):
        res = proc.process_single_event({
            "event_id": f"evt_bound_{i}",
            "event_type": "comment",
            "data": {"comment_text": "Halo kak"}
        })
        assert res is not None
    assert len(proc.seen_event_ids) == 5
    # Event paling baru masih terdeteksi duplikat
    assert proc.process_single_event({
        "event_id": "evt_bound_7",
        "event_type": "comment",
        "data": {"comment_text": "Halo kak"}
    }) is None
    assert proc.quality_report["duplicates_skipped"] == 1

def test_dual_layer_hybrid_intent_tags(tmp_path):
    """Menguji klasifikasi Dual-Layer Hybrid (1 primary intent + maks 2 tags) serta serialisasi Parquet."""
    import pandas as pd

    proc = BronzeToSilverProcessor()

    # 1. Pertanyaan ukuran (kasus screenshot Nurysanty) -> murni QUESTION, bukan INTENT_BUY
    p1, _, t1 = proc.classify_comment_intent("Lp 86 bb 66 tb 156 celana yg di pegang size apa?")
    assert p1 == "QUESTION"
    assert t1 == ["QUESTION"]

    # 2. Komentar ganda Niat Beli + Tanya (kasus screenshot ngoc bich) -> 2 tags
    p2, _, t2 = proc.classify_comment_intent("buy product in where?")
    assert p2 == "INTENT_BUY"
    assert t2 == ["INTENT_BUY", "QUESTION"]

    # 3. Komplain + Tanya -> 2 tags dengan prioritas COMPLAINT
    p3, _, t3 = proc.classify_comment_intent("Barang datang robek gimana cara retur?")
    assert p3 == "COMPLAINT"
    assert t3 == ["COMPLAINT", "QUESTION"]

    # 4. Sapaan santai -> CASUAL
    p4, _, t4 = proc.classify_comment_intent("Halo kak salam dari Medan")
    assert p4 == "CASUAL"
    assert t4 == ["CASUAL"]

    # 5. Pastikan record dengan intent_tags bisa disimpan ke Parquet oleh PyArrow
    rec = proc.process_single_event({
        "event_id": "evt_parquet_multitag_1",
        "session_id": "sess_test_pq",
        "streamer": "@tokotest",
        "event_type": "comment",
        "data": {"comment_text": "buy product in where?"}
    })
    assert rec["intent_tags"] == ["INTENT_BUY", "QUESTION"]
    pq_file = tmp_path / "test_multitag.parquet"
    pd.DataFrame([rec]).to_parquet(pq_file, engine="pyarrow", index=False)
    df_back = pd.read_parquet(pq_file)
    assert list(df_back.iloc[0]["intent_tags"]) == ["INTENT_BUY", "QUESTION"]

def test_ml_a_plus_slang_short_long_semi_english():
    """Regresi kasus screenshot nyata: pastikan tidak ada lagi false-positive KOMPLAIN."""
    proc = BronzeToSilverProcessor()

    # 1. Permintaan produk singkat -> QUESTION (bukan COMPLAINT!)
    i1, _, t1 = proc.classify_comment_intent("jaket bng")
    assert i1 == "QUESTION"
    assert "COMPLAINT" not in t1

    # 2. Tanya lisan "... ga ka" -> QUESTION (bukan COMPLAINT!)
    i2, _, t2 = proc.classify_comment_intent("udah masuk ga ka")
    assert i2 == "QUESTION"
    assert "COMPLAINT" not in t2

    # 3. Cerita panjang & tawa -> CASUAL
    i3, _, t3 = proc.classify_comment_intent("dapat diskon langsung wkwkw dari 200 san ke 180 han")
    assert i3 == "CASUAL"
    assert t3 == ["CASUAL"]

    # 4. Tanya ukuran slang -> QUESTION
    i4, _, t4 = proc.classify_comment_intent("Lp 86 bb 66 tb 156 celana yg di pegang size apa?")
    assert i4 == "QUESTION"
    assert t4 == ["QUESTION"]

    # 5. Semi-English ganda -> INTENT_BUY + QUESTION
    i5, _, t5 = proc.classify_comment_intent("buy product in where?")
    assert i5 == "INTENT_BUY"
    assert t5 == ["INTENT_BUY", "QUESTION"]

    # 6. Komplain asli -> COMPLAINT + QUESTION
    i6, _, t6 = proc.classify_comment_intent("Barang datang robek gimana cara retur?")
    assert i6 == "COMPLAINT"
    assert t6 == ["COMPLAINT", "QUESTION"]

    # 7. Kata acak / OOV murni -> Rejection Gate menahan menjadi CASUAL
    i7, _, t7 = proc.classify_comment_intent("zzzzz xqwert yuiop")
    assert i7 == "CASUAL"
    assert t7 == ["CASUAL"]

def test_real_aerostreet_chat_cases_and_short_etalase():
    """Regresi kasus chat nyata @aerostreet: hapus duplikasi, etalase singkat, dan nuansa 2 tag."""
    proc = BronzeToSilverProcessor()

    # 1. '13 ks' -> etalase 13 & 2 tag (INTENT_BUY + QUESTION)
    assert proc.extract_etalase_number("13 ks") == 13
    _, _, t1 = proc.classify_comment_intent("13 ks")
    assert t1 == ["INTENT_BUY", "QUESTION"]

    # 2. 'buat paper bag sepatunya gaada ya ka' -> QUESTION
    i2, _, t2 = proc.classify_comment_intent("buat paper bag sepatunya gaada ya ka")
    assert i2 == "QUESTION"
    assert t2 == ["QUESTION"]

    # 3. 'rekomendasi buat cewe yang buat ke gunung etalase berapa ka' -> 2 tags (INTENT_BUY + QUESTION)
    _, _, t3 = proc.classify_comment_intent("rekomendasi buat cewe yang buat ke gunung etalase berapa ka")
    assert t3 == ["INTENT_BUY", "QUESTION"]

    # 4. 'kk spatu DC ada g' -> 2 tags (INTENT_BUY + QUESTION)
    _, _, t4 = proc.classify_comment_intent("kk spatu DC ada g")
    assert t4 == ["INTENT_BUY", "QUESTION"]

    # 5. 'aku uda dapet promo kaa' -> CASUAL
    i5, _, _ = proc.classify_comment_intent("aku uda dapet promo kaa")
    assert i5 == "CASUAL"

    # 6. 'banyakin dong design yg lain untuk kaos motornya' -> CASUAL
    i6, _, _ = proc.classify_comment_intent("banyakin dong design yg lain untuk kaos motornya")
    assert i6 == "CASUAL"
