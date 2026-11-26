"""Test workbench UI and workstation page print button and A4 media print styling contract."""
from pathlib import Path
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from SCRWebService.clinical_cohort_3tier_router import build_clinical_cohort_3tier_router
from src.cin2_wave8_global_wa_app import app as wave8_app

REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def cohort_client():
    cohort_app = FastAPI()
    cohort_app.include_router(
        build_clinical_cohort_3tier_router("integration-adaptors", "integration_adaptors", REPO_ROOT)
    )
    return TestClient(cohort_app)


@pytest.fixture
def workstation_client():
    return TestClient(wave8_app)


def test_workbench_ui_renders_page_print_button_and_a4_media_print(cohort_client):
    """Verify PagePrintButton and @media print contract are present on /workbench-ui."""
    resp = cohort_client.get("/api/v1/million-cohort/workbench-ui")
    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"
    html = resp.text

    # Print button element contract
    assert 'id="wb-print-btn"' in html
    assert 'data-action="print-page"' in html
    assert 'data-print-hidden="true"' in html
    assert 'data-testid="btn-page-print"' in html
    assert 'onclick="window.print();"' in html
    assert 'aria-label="Print current view (A4)"' in html

    # @media print CSS contract
    assert "@media print" in html
    assert "size: A4 portrait" in html
    assert "#wb-print-btn" in html
    assert "[data-print-hidden=\"true\"]" in html


def test_workstation_ui_renders_page_print_button_and_a4_media_print(workstation_client):
    """Verify PagePrintButton and @media print contract are present on /ui/workstation."""
    resp = workstation_client.get("/ui/workstation")
    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"
    html = resp.text

    assert 'id="wb-print-btn"' in html
    assert 'data-action="print-page"' in html
    assert 'data-print-hidden="true"' in html
    assert 'data-testid="btn-page-print"' in html
    assert 'onclick="window.print();"' in html
    assert "@media print" in html
    assert "size: A4 portrait" in html
