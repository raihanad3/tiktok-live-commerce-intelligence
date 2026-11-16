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

