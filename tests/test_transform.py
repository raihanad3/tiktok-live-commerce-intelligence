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
