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
