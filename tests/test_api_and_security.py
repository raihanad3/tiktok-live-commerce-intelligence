"""
Unit Tests for Serving API & Security Role Separation (F-16 & F-18 Requirements)
"""

import pytest
import os
import sys
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from serving.app import app

client = TestClient(app)

def test_api_health():
    """Menguji endpoint health check."""
    response = client.get("/api/v1/health")
    assert response.status_code == 200
    assert response.json()["status"] == "HEALTHY"

def test_security_admin_access_forbidden_for_viewer():
    """Menguji kontrol keamanan F-16: Viewer dilarang mengakses endpoint Admin (HTTP 403)."""
    response = client.post(
        "/api/v1/admin/reset-session",
        headers={"role": "viewer"}
    )
    assert response.status_code == 403
    assert "Akses ditolak" in response.json()["detail"]

def test_ml_inference_input_validation():
    """Menguji validasi payload inferensi: komentar kosong menghasilkan HTTP 422/400 (F-12)."""
    response = client.post(
        "/api/v1/ml/predict",
        json={"comment_text": " "},
        headers={"role": "viewer"}
    )
    # Jika gagal validasi pydantic atau business logic
    assert response.status_code in [400, 422]

def test_get_catalog_endpoint():
    """Menguji endpoint pengambilan katalog toko."""
    response = client.get("/api/v1/catalog?streamer=@aerostreet", headers={"role": "viewer"})
    assert response.status_code == 200
    data = response.json()
    assert "products" in data
    assert len(data["products"]) > 0
    assert any("Aerostreet" in p["product_name"] for p in data["products"])

def test_update_catalog_endpoint():
    """Menguji endpoint pembaruan katalog toko custom."""
    test_products = [
        {"etalase_no": 1, "product_name": "Sepatu Test Edition 1", "price_idr": 150000},
        {"etalase_no": 2, "product_name": "Kaos Test Edition 2", "price_idr": 75000}
    ]
    response = client.post(
        "/api/v1/catalog/update",
        json={"streamer": "@teststore", "products": test_products},
        headers={"role": "viewer"}
    )
    assert response.status_code == 200
    assert response.json()["status"] == "success"

    # Verifikasi langsung lewat GET
    get_res = client.get("/api/v1/catalog?streamer=@teststore", headers={"role": "viewer"})
    assert get_res.status_code == 200
    prods = get_res.json()["products"]
    assert len(prods) == 2
    assert prods[0]["product_name"] == "Sepatu Test Edition 1"

def test_get_available_niches_endpoint():
    """Menguji endpoint pengambilan daftar niche yang tersedia (F-10 / Hybrid Niche)."""
    response = client.get("/api/v1/niches/available", headers={"role": "viewer"})
    assert response.status_code == 200
    data = response.json()
    assert "niches" in data
    assert data["total_niches"] >= 7
    keys = [n["key"] for n in data["niches"]]
    assert "fashion" in keys
    assert "sports" in keys
    assert "skincare" in keys

def test_switch_session_with_niche_override():
    """Menguji beralih toko dengan manual niche override."""
    payload = {
        "username": "@ambiguous_shop_123",
        "niche_override": "kuliner"
    }
    response = client.post("/api/v1/session/switch", json=payload, headers={"role": "viewer"})
    assert response.status_code == 200
    data = response.json()
    assert data["niche"] == "culinary"
    assert data["niche_override_used"] is True

def test_hybrid_niche_detection_hash_consistency():
    """Menguji konsistensi hash-based smart random assignment untuk username ambigu."""
    from common.store_catalog import detect_store_niche
    niche_1 = detect_store_niche("@tokomisterix")
    niche_2 = detect_store_niche("@tokomisterix")
    assert niche_1 == niche_2
    assert niche_1 in ["fashion", "sports", "shoes", "skincare", "hijab", "gadgets", "culinary"]


def test_niche_benchmark_endpoint():
    """Menguji endpoint Batch Niche Benchmark dan metadata korpus ML."""
    response = client.get("/api/v1/benchmark/niche", headers={"role": "viewer"})
    assert response.status_code == 200
    data = response.json()
    assert "niche_benchmark" in data
    assert "ml_model_stats" in data
    assert "golden_hour_wib" in data["niche_benchmark"]


def test_batch_harvest_rbac_forbidden_for_viewer():
    """Menguji kontrol akses RBAC: viewer dilarang memicu batch harvester (HTTP 403)."""
    response = client.post(
        "/api/v1/batch/harvest/run",
        json={"duration_sec": 5},
        headers={"role": "viewer"}
    )
    assert response.status_code == 403


def test_auto_label_intent_harvester():
    """Menguji weak supervision auto-labeling pada TikTok Batch Harvester."""
    from ingestion.tiktok_batch_harvester import auto_label_intent
    assert auto_label_intent("kak spill no 5 mau checkout sekarang")[0] == "INTENT_BUY"
    assert auto_label_intent("ukuran M buat bb 60 muat ga kak?")[0] == "QUESTION"
    assert auto_label_intent("barang datang robek kecewa banget")[0] == "COMPLAINT"
    assert auto_label_intent("halo kak salam dari bandung")[0] == "CASUAL"



