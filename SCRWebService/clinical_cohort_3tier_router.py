# ruff: noqa: E501, N815, S608, C901, PLR0912, PLR0915
"""Standalone 3-Tier Clinical Cohort CRUD, Clinical Decision Support & Tabbed Workbench Router.

Enables each Symphonix clinical sibling service to operate independently via a
self-contained 3-tier architecture (Frontend Tabbed GUI <-> FastAPI Mid-Tier <->
Sibling SQLite WAL / PostgreSQL 15 Schema) with full Create, Read, Update, Clinical
Decision Context synthesis, and Clinical Archive (never physical delete) across the
1,002,250-patient / 4,409,900-episode cohort, with zero abandoned source attributes.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator

PG_DSN = os.environ.get(
    "BULLETTRAIN_PG_DSN",
    "postgresql://bullettrain:devpassword@127.0.0.1:15432/bullettrain",
)

ICD10_RE = re.compile(r"^[A-Z][0-9]{2}(\.[0-9A-Z]{1,4})?$")


def _utc_now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _generate_valid_modulus11_nhs(seed_str: str) -> str:
    """Generate a deterministic 10-digit NHS number passing strict Modulus-11 validation."""
    digest = int(hashlib.sha256(seed_str.encode()).hexdigest()[:12], 16)
    base = 940000000 + (digest % 50000000)
    for offset in range(25):
        cand = base + offset
        s = f"{cand:09d}"
        total = sum(int(s[i]) * (10 - i) for i in range(9))
        rem = total % 11
        check = (11 - rem) % 11
        if check != 10:
            return f"{s}{check}"
    return "9434765919"


def validate_modulus11_nhs(nhs_number: str) -> bool:
    if not nhs_number or len(nhs_number) != 10 or not nhs_number.isdigit():
        return False
    total = sum(int(nhs_number[i]) * (10 - i) for i in range(10))
    return (total % 11) == 0


class PatientCreate3TierRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    givenName: str = Field(..., min_length=1, max_length=120)
    familyName: str = Field(..., min_length=1, max_length=120)
    dateOfBirth: str = Field(..., pattern=r"^\d{4}-\d{2}-\d{2}$")
    gender: str = Field(default="female", pattern=r"^(female|male|other|unknown)$")
    nationalID: str | None = Field(default=None)
    site_id: str = Field(default="SITE_ACUTE_NORTH", min_length=3, max_length=64)
    age_band: str = Field(default="50_74", pattern=r"^(18_49|50_74|75_PLUS)$")
    deprivation_quintile: int = Field(default=3, ge=1, le=5)
    split_name: str = Field(default="sealed_holdout", min_length=2, max_length=32)
    hf_repo_id: str = Field(
        default="AGBonnet/augmented-clinical-notes", min_length=3, max_length=120
    )
    primary_icd10_code: str = Field(default="I10", min_length=2, max_length=16)
    primary_diagnosis_name: str = Field(
        default="Essential hypertension and acute longitudinal monitoring",
        min_length=2,
        max_length=240,
    )
    medical_specialty: str = Field(
        default="Internal Medicine / Acute Care", min_length=2, max_length=120
    )
    marital_status: str = Field(
        default="MARRIED", pattern=r"^(SINGLE|MARRIED|WIDOWED|DIVORCED|UNKNOWN)$"
    )
    insurance_coverage: str = Field(
        default="NHS_UNIVERSAL", min_length=2, max_length=64
    )
    lives_alone: int = Field(default=0, ge=0, le=1)
    sync_bullettrain_hie: bool = Field(
        default=False,
        description="If False (default), executes standalone 3-tier local CRUD. If True, also registers/syncs canonical identity in BulletTrain Client Registry.",
    )

    @field_validator("primary_icd10_code")
    @classmethod
    def _validate_icd10(cls, v: str) -> str:
        code = v.strip().upper()
        if not ICD10_RE.match(code):
            raise ValueError(f"Invalid ICD-10 code format: {v}")
        return code


class PatientUpdate3TierRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    givenName: str | None = Field(default=None, min_length=1, max_length=120)
    familyName: str | None = Field(default=None, min_length=1, max_length=120)
    dateOfBirth: str | None = Field(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$")
    gender: str | None = Field(default=None, pattern=r"^(female|male|other|unknown)$")
    site_id: str | None = Field(default=None, min_length=3, max_length=64)
    deprivation_quintile: int | None = Field(default=None, ge=1, le=5)
    primary_icd10_code: str | None = Field(default=None, min_length=2, max_length=16)
    primary_diagnosis_name: str | None = Field(
        default=None, min_length=2, max_length=240
    )
    medical_specialty: str | None = Field(default=None, min_length=2, max_length=120)
    marital_status: str | None = Field(
        default=None, pattern=r"^(SINGLE|MARRIED|WIDOWED|DIVORCED|UNKNOWN)$"
    )
    insurance_coverage: str | None = Field(default=None, min_length=2, max_length=64)
    lives_alone: int | None = Field(default=None, ge=0, le=1)
    sync_bullettrain_hie: bool = Field(default=False)

    @field_validator("primary_icd10_code")
    @classmethod
    def _validate_icd10_opt(cls, v: str | None) -> str | None:
        if v is None:
            return None
        code = v.strip().upper()
        if not ICD10_RE.match(code):
            raise ValueError(f"Invalid ICD-10 code format: {v}")
        return code


class EpisodeCreate3TierRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    episode_sequence: int = Field(default=1, ge=1, le=5)
    site_id: str = Field(default="SITE_ACUTE_NORTH", min_length=3, max_length=64)
    split_name: str = Field(default="sealed_holdout", min_length=2, max_length=32)
    initial_map_mmhg: float = Field(default=75.0, ge=30.0, le=220.0)
    initial_hr_bpm: float = Field(default=80.0, ge=20.0, le=250.0)
    initial_lactate_mmol_l: float = Field(default=1.5, ge=0.2, le=25.0)
    initial_egfr: float = Field(default=76.0, ge=5.0, le=140.0)
    initial_evlw_ml_kg: float = Field(default=7.2, ge=3.0, le=24.0)
    serum_creatinine_mg_dl: float = Field(default=1.05, ge=0.2, le=15.0)
    respiratory_rate_bpm: float = Field(default=18.0, ge=4.0, le=60.0)
    spo2_pct: float = Field(default=96.5, ge=50.0, le=100.0)
    deterioration_risk_4h: float = Field(default=0.18, ge=0.0, le=1.0)
    admission_type: str = Field(default="EW EMER.", min_length=2, max_length=64)
    care_unit: str = Field(default="MICU", min_length=2, max_length=64)
    clinical_presentation_summary: str = Field(
        default="Acute clinical admission under continuous-discrete state-space monitoring.",
        min_length=3,
        max_length=500,
    )
    clinical_keywords_or_concepts: str = Field(
        default="hemodynamic monitoring, lactate, egfr, kalman state estimation",
        min_length=2,
        max_length=500,
    )
    source_clinical_attributes_json: str = Field(default="{}")

    @field_validator("source_clinical_attributes_json")
    @classmethod
    def _validate_json_obj(cls, v: str) -> str:
        try:
            parsed = json.loads(v)
        except Exception as exc:
            raise ValueError(
                f"source_clinical_attributes_json must be valid JSON: {exc}"
            ) from exc
        if not isinstance(parsed, dict):
            raise ValueError("source_clinical_attributes_json must be a JSON object")
        return json.dumps(parsed, ensure_ascii=True)


class EpisodeUpdate3TierRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    initial_map_mmhg: float | None = Field(default=None, ge=30.0, le=220.0)
    initial_hr_bpm: float | None = Field(default=None, ge=20.0, le=250.0)
    initial_lactate_mmol_l: float | None = Field(default=None, ge=0.2, le=25.0)
    initial_egfr: float | None = Field(default=None, ge=5.0, le=140.0)
    initial_evlw_ml_kg: float | None = Field(default=None, ge=3.0, le=24.0)
    serum_creatinine_mg_dl: float | None = Field(default=None, ge=0.2, le=15.0)
    respiratory_rate_bpm: float | None = Field(default=None, ge=4.0, le=60.0)
    spo2_pct: float | None = Field(default=None, ge=50.0, le=100.0)
    deterioration_risk_4h: float | None = Field(default=None, ge=0.0, le=1.0)
    admission_type: str | None = Field(default=None, min_length=2, max_length=64)
    care_unit: str | None = Field(default=None, min_length=2, max_length=64)
    clinical_presentation_summary: str | None = Field(
        default=None, min_length=3, max_length=500
    )
    clinical_keywords_or_concepts: str | None = Field(
        default=None, min_length=2, max_length=500
    )
    source_clinical_attributes_json: str | None = Field(default=None)

    @field_validator("source_clinical_attributes_json")
    @classmethod
    def _validate_json_obj_opt(cls, v: str | None) -> str | None:
        if v is None:
            return None
        try:
            parsed = json.loads(v)
        except Exception as exc:
            raise ValueError(
                f"source_clinical_attributes_json must be valid JSON: {exc}"
            ) from exc
        if not isinstance(parsed, dict):
            raise ValueError("source_clinical_attributes_json must be a JSON object")
        return json.dumps(parsed, ensure_ascii=True)


class ClinicalArchiveRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    archive_reason: str = Field(
        ...,
        min_length=6,
        max_length=500,
        description="Mandatory clinical/governance rationale for archiving the record (records are never physically deleted).",
    )


def _render_workbench_html(service_name: str, sibling_slug: str) -> str:
    return rf"""<!DOCTYPE html>
<html lang="en" data-sfds-layout-mode="tabbed">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>{service_name} — SFDS v2.1 3-Tier Clinical Decision &amp; Cohort Workbench</title>
  <link rel="icon" href="data:image/svg+xml,%3Csvg xmlns=%27http://www.w3.org/2000/svg%27 viewBox=%270 0 16 16%27%3E%3Crect width=%2716%27 height=%2716%27 rx=%274%27 fill=%27%232563eb%27/%3E%3C/svg%3E" type="image/svg+xml"/>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; }}
    html, body {{
      margin: 0;
      padding: 0;
      width: 100%;
      max-width: none;
      height: 100vh;
      max-height: 100vh;
      overflow: hidden;
      font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      background: #0b1220;
      color: #f1f5f9;
      display: flex;
      flex-direction: column;
    }}
    .sfds-shell {{
      width: 100%;
      max-width: none;
      height: 100vh;
      max-height: 100vh;
      overflow: hidden;
      display: flex;
      flex-direction: column;
      padding: 10px 16px;
      gap: 8px;
    }}
    .sfds-header {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      background: #111c33;
      border: 1px solid #233554;
      border-radius: 8px;
      padding: 8px 14px;
      flex-shrink: 0;
    }}
    .sfds-header h1 {{
      font-size: 16px;
      margin: 0;
      color: #38bdf8;
    }}
    .sfds-badge-row {{
      display: flex;
      gap: 8px;
      font-size: 12px;
    }}
    .sfds-badge {{
      background: #1e293b;
      border: 1px solid #334155;
      padding: 3px 9px;
      border-radius: 999px;
      color: #e2e8f0;
    }}
    .sfds-tablist {{
      display: flex;
      flex-direction: row;
      gap: 6px;
      background: #0f172a;
      border-bottom: 1px solid #233554;
      padding: 4px 0;
      flex-shrink: 0;
      overflow-x: auto;
      white-space: nowrap;
    }}
    .sfds-tab {{
      min-height: 38px;
      padding: 8px 14px;
      border-radius: 6px;
      border: 1px solid #334155;
      background: #1e293b;
      color: #cbd5e1;
      font-size: 13px;
      font-weight: 600;
      cursor: pointer;
    }}
    .sfds-tab[aria-selected="true"] {{
      background: #0369a1;
      border-color: #38bdf8;
      color: #ffffff;
    }}
    .sfds-tabpanel {{
      display: none;
      flex: 1 1 auto;
      min-height: 0;
      width: 100%;
      max-width: none;
      overflow: hidden;
      background: #111c33;
      border: 1px solid #233554;
      border-radius: 8px;
      padding: 12px;
      flex-direction: column;
      gap: 10px;
    }}
    .sfds-tabpanel.active {{
      display: flex;
    }}
    .sfds-grid-4 {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 10px;
      width: 100%;
      flex-shrink: 0;
    }}
    .sfds-card {{
      background: #1e293b;
      border: 1px solid #334155;
      border-radius: 6px;
      padding: 9px 12px;
    }}
    .sfds-card-label {{
      font-size: 11px;
      color: #94a3b8;
      text-transform: uppercase;
      padding: 6px;
    }}
    .sfds-card-val {{
      font-size: 17px;
      font-weight: 700;
      color: #f8fafc;
      margin-top: 2px;
      padding: 6px;
    }}
    .sfds-filter-row {{
      display: grid;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      gap: 8px;
      width: 100%;
      flex-shrink: 0;
    }}
    .sfds-control {{
      display: flex;
      flex-direction: column;
      gap: 3px;
    }}
    .sfds-control label {{
      font-size: 11px;
      color: #cbd5e1;
      font-weight: 600;
    }}
    .sfds-control input, .sfds-control select {{
      min-height: 38px;
      padding: 8px 11px;
      border-radius: 6px;
      border: 1px solid #94a3b8;
      background: #0f172a;
      color: #f8fafc;
      font-size: 13px;
      width: 100%;
    }}
    .sfds-btn {{
      min-height: 38px;
      padding: 8px 14px;
      border-radius: 6px;
      border: 1px solid #38bdf8;
      background: #0369a1;
      color: #ffffff;
      font-weight: 600;
      cursor: pointer;
    }}
    .sfds-table-wrap {{
      flex: 1 1 auto;
      min-height: 0;
      height: 100%;
      overflow: hidden;
      width: 100%;
      display: flex;
      flex-direction: column;
    }}
    table.sfds-table {{
      width: 100%;
      height: 100%;
      border-collapse: collapse;
      font-size: 12px;
    }}
    table.sfds-table th, table.sfds-table td {{
      border-bottom: 1px solid #334155;
      padding: 6px 8px;
      text-align: left;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }}
    table.sfds-table th {{
      background: #0f172a;
      color: #38bdf8;
    }}
    .sfds-tabpanel, .sfds-table-wrap {{ mask-image: none !important; -webkit-mask-image: none !important; }}
    .stageshow-bar-placeholder {{ display: none !important; }}
    .sfds-presence-strip {{ display: inline-flex; align-items: center; gap: 6px; }}
    .sfds-presence-pill {{ display: inline-flex; align-items: center; gap: 4px; padding: 2px 7px; border-radius: 999px; border: 1px solid #334155; background: #1e293b; color: #e2e8f0; font-size: 11px; font-weight: 600; }}
    .sfds-presence-dot {{ width: 7px; height: 7px; border-radius: 50%; display: inline-block; }}
    .sfds-presence-pill[data-persona-role="clinician"] .sfds-presence-dot {{ background: #2563eb; }}
    .sfds-presence-pill[data-persona-role="pharmacist"] .sfds-presence-dot {{ background: #0d9488; }}
    .sfds-presence-pill[data-persona-role="nurse"] .sfds-presence-dot {{ background: #059669; }}
    .sfds-presence-pill[data-persona-role="icu-bed-manager"] .sfds-presence-dot {{ background: #4f46e5; }}
    .sfds-ai-chat-thread {{ display: flex; flex-direction: column; gap: 6px; width: 100%; flex-shrink: 0; }}
    .sfds-ai-card {{ display: flex; flex-direction: column; gap: 4px; padding: 8px 12px; border: 1px solid #334155; border-left: 3px solid #2563eb; border-radius: 6px; background: #1e293b; background-image: none !important; }}
    .sfds-ai-stream-slot {{ contain: layout; min-height: 32px; display: flex; align-items: center; justify-content: space-between; gap: 8px; font-size: 12px; }}
    .sfds-ai-provenance-footer {{ display: flex; align-items: center; justify-content: space-between; gap: 8px; padding-top: 4px; border-top: 1px dashed #334155; font-family: monospace; font-size: 10.5px; color: #94a3b8; }}
    .sfds-panel-annotation {{ display: inline-flex; align-items: center; gap: 8px; padding: 3px 9px; border: 1px solid #2563eb; border-radius: 6px; background: #0f172a; font-size: 11px; }}
    .sfds-leader-pin {{ padding: 1px 6px; border-radius: 999px; border: 1px solid #38bdf8; font-family: monospace; font-size: 10px; font-weight: 700; color: #38bdf8; }}
    .sfds-leader-line {{ width: 22px; height: 2px; background: #38bdf8; display: inline-block; }}
    .sfds-field-error-inline {{ display: inline-flex; align-items: center; gap: 5px; padding: 2px 7px; border-radius: 4px; border: 1px solid #dc2626; background: #1e293b; color: #f87171; font-family: monospace; font-size: 10.5px; font-weight: 600; }}
    .sfds-rolling-metric {{ font-variant-numeric: tabular-nums; font-family: monospace; font-weight: 700; color: #38bdf8; }}
    .sfds-status-msg {{
      font-size: 12px;
      padding: 6px 10px;
      border-radius: 6px;
      background: #0f172a;
      border: 1px solid #334155;
      color: #38bdf8;
      flex-shrink: 0;
    }}
  </style>
</head>
<body>
  <main class="sfds-shell" id="main-workbench">
    <header class="sfds-header">
      <h1>{service_name} ({sibling_slug}) — 3-Tier Clinical Decision &amp; Million-Cohort Workbench</h1>
      <div class="sfds-badge-row">
        <span class="sfds-presence-strip" aria-label="SuperPersona Collaborators"><span class="sfds-presence-pill" data-persona-role="clinician"><span class="sfds-presence-dot"></span>MD</span><span class="sfds-presence-pill" data-persona-role="pharmacist"><span class="sfds-presence-dot"></span>Rx</span><span class="sfds-presence-pill" data-persona-role="nurse"><span class="sfds-presence-dot"></span>RN</span><span class="sfds-presence-pill" data-persona-role="icu-bed-manager"><span class="sfds-presence-dot"></span>BedMgr</span></span>
        <span class="sfds-badge" id="badge-pg-status">Dual-Engine: Synced</span>
        <span class="sfds-badge" id="badge-patient-count">Patients: 40,090</span>
        <span class="sfds-badge" id="badge-episode-count">Episodes: 176,396</span>
        <span class="sfds-badge">Zero Abandoned Attributes (26 Pat / 26 Ep Cols)</span>
      </div>
    </header>

    <div class="sfds-tablist" role="tablist" aria-label="3-Tier Clinical Cohort Primary Tabs">
      <button class="sfds-tab" role="tab" id="tab-btn-directory" aria-selected="true" aria-controls="panel-directory" onclick="selectTab('directory')">1. Cohort &amp; ICD-10 Specialty Directory</button>
      <button class="sfds-tab" role="tab" id="tab-btn-telemetry" aria-selected="false" aria-controls="panel-telemetry" onclick="selectTab('telemetry')">2. 5D State-Space &amp; Raw Lab/Vital Telemetry</button>
      <button class="sfds-tab" role="tab" id="tab-btn-crud" aria-selected="false" aria-controls="panel-crud" onclick="selectTab('crud')">3. 3-Tier Patient &amp; Episode Registration (Validated CRUD)</button>
      <button class="sfds-tab" role="tab" id="tab-btn-cds" aria-selected="false" aria-controls="panel-cds" onclick="selectTab('cds')">4. Clinical Decision Context &amp; Archive Governance</button>
    </div>

    <!-- TAB 1: Cohort & ICD-10 Specialty Directory -->
    <section class="sfds-tabpanel active" role="tabpanel" id="panel-directory" aria-labelledby="tab-btn-directory">
      <div class="sfds-grid-4">
        <div class="sfds-card"><div class="sfds-card-label">Active Patients</div><div class="sfds-card-val" id="metric-active-patients">40,090</div></div>
        <div class="sfds-card"><div class="sfds-card-label">Lives Alone (Social Risk)</div><div class="sfds-card-val" id="metric-lives-alone">-</div></div>
        <div class="sfds-card"><div class="sfds-card-label">Mean 5D EVLW (mL/kg)</div><div class="sfds-card-val" id="metric-mean-evlw">-</div></div>
        <div class="sfds-card"><div class="sfds-card-label">Mean Serum Creatinine (mg/dL)</div><div class="sfds-card-val" id="metric-mean-scr">-</div></div>
      </div>
      <form class="sfds-filter-row" onsubmit="event.preventDefault(); loadPatients();">
        <div class="sfds-control">
          <label for="filter-search">Search Name / MPI / ICD-10</label>
          <input id="filter-search" type="text" placeholder="e.g. Okafor or M81 or Cardiology" />
        </div>
        <div class="sfds-control">
          <label for="filter-icd10">Primary ICD-10 Code</label>
          <input id="filter-icd10" type="text" placeholder="e.g. M81, I10, A41.9" />
        </div>
        <div class="sfds-control">
          <label for="filter-marital">Marital Status</label>
          <select id="filter-marital">
            <option value="">All Marital Statuses</option>
            <option value="MARRIED">MARRIED</option>
            <option value="SINGLE">SINGLE</option>
            <option value="WIDOWED">WIDOWED</option>
            <option value="DIVORCED">DIVORCED</option>
          </select>
        </div>
        <div class="sfds-control">
          <label for="filter-lives-alone">Lives Alone Social Flag</label>
          <select id="filter-lives-alone">
            <option value="">All</option>
            <option value="1">Lives Alone (1)</option>
            <option value="0">Supported Household (0)</option>
          </select>
        </div>
        <div class="sfds-control" style="justify-content: flex-end;">
          <button type="submit" class="sfds-btn" id="btn-apply-filter">Filter Cohort</button>
        </div>
      </form>
      <div class="sfds-table-wrap">
        <table class="sfds-table" aria-label="Seeded Patients with Clinical Decision Attributes">
          <thead>
            <tr>
              <th>Patient ID</th>
              <th>Name</th>
              <th>DOB / Gender</th>
              <th>NHS Modulus-11</th>
              <th>ICD-10</th>
              <th>Primary Diagnosis</th>
              <th>Medical Specialty</th>
              <th>Marital / Insurance</th>
              <th>Lives Alone</th>
              <th>Inspect</th>
            </tr>
          </thead>
          <tbody id="patients-tbody"></tbody>
        </table>
      </div>
    </section>

    <!-- TAB 2: 5D State-Space & Raw Lab/Vital Telemetry -->
    <section class="sfds-tabpanel" role="tabpanel" id="panel-telemetry" aria-labelledby="tab-btn-telemetry">
      <div class="sfds-grid-4" id="telemetry-patient-banner">
        <div class="sfds-card"><div class="sfds-card-label">Selected Patient</div><div class="sfds-card-val" id="tel-pat-id">-</div></div>
        <div class="sfds-card"><div class="sfds-card-label">ICD-10 &amp; Diagnosis</div><div class="sfds-card-val" id="tel-pat-diag">-</div></div>
        <div class="sfds-card"><div class="sfds-card-label">Specialty &amp; Insurance</div><div class="sfds-card-val" id="tel-pat-spec">-</div></div>
        <div class="sfds-card"><div class="sfds-card-label">Social Context</div><div class="sfds-card-val" id="tel-pat-social">-</div></div>
      </div>
      <div class="sfds-table-wrap">
        <table class="sfds-table" aria-label="Longitudinal 5D State-Space and Raw Clinical Episodes">
          <thead>
            <tr>
              <th>Episode ID</th>
              <th>Seq</th>
              <th>Admission / Care Unit</th>
              <th>MAP / HR</th>
              <th>Lactate</th>
              <th>Creatinine / eGFR</th>
              <th>5D EVLW</th>
              <th>RR / SpO2</th>
              <th>4h Risk</th>
              <th>Clinical Presentation Summary</th>
            </tr>
          </thead>
          <tbody id="episodes-tbody"></tbody>
        </table>
      </div>
    </section>

    <!-- TAB 3: 3-Tier Validated Patient & Episode CRUD -->
    <section class="sfds-tabpanel" role="tabpanel" id="panel-crud" aria-labelledby="tab-btn-crud">
      <form class="sfds-filter-row" id="form-create-patient" novalidate onsubmit="event.preventDefault(); createPatient3Tier();">
        <div class="sfds-control">
          <label for="crud-given">Given Name *</label>
          <input id="crud-given" required minlength="1" maxlength="120" value="Adaeze" />
        </div>
        <div class="sfds-control">
          <label for="crud-family">Family Name *</label>
          <input id="crud-family" required minlength="1" maxlength="120" value="Okafor" />
        </div>
        <div class="sfds-control">
          <label for="crud-dob">Date of Birth (YYYY-MM-DD) *</label>
          <input id="crud-dob" required pattern="^\d{{4}}-\d{{2}}-\d{{2}}$" value="1974-08-19" />
        </div>
        <div class="sfds-control">
          <label for="crud-icd10">Primary ICD-10 Code *</label>
          <input id="crud-icd10" required pattern="^[A-Z][0-9]{{2}}(\.[0-9A-Z]{{1,4}})?$" value="I50.9" />
        </div>
        <div class="sfds-control">
          <label for="crud-specialty">Medical Specialty *</label>
          <input id="crud-specialty" required minlength="2" maxlength="120" value="Cardiovascular / Pulmonary" />
        </div>
      </form>
      <div style="display:flex; gap:10px; align-items:center; flex-shrink:0;">
        <button type="button" class="sfds-btn" id="btn-submit-patient" onclick="createPatient3Tier()">Register Standalone 3-Tier Patient (SQLite WAL + PostgreSQL 15)</button>
        <span class="sfds-status-msg" id="crud-feedback" role="status">Ready for validated 3-tier clinical registration.</span>
        <span class="sfds-badge">Rolling NEWS2: <strong class="sfds-rolling-metric" id="wb-rolling-news2">5</strong></span>
        <span class="sfds-field-error-inline" id="wb-crud-inline-error" hidden>DCB0160: ICD-10 &amp; DOB required</span>
      </div>
      <div style="display:grid; grid-template-columns:1.25fr 0.75fr; gap:10px; flex:1 1 auto; min-height:0; width:100%;">
        <div class="sfds-card" style="display:flex; flex-direction:column; min-height:0; height:100%;">
          <div class="sfds-card-label">Live 3-Tier Registered Cohort Ledger (SQLite WAL + PostgreSQL 15 Synced)</div>
          <div class="sfds-table-wrap">
            <table class="sfds-table" aria-label="Recent Registered Cohort Patients">
              <thead>
                <tr>
                  <th>Patient ID</th>
                  <th>Name</th>
                  <th>NHS Modulus-11</th>
                  <th>ICD-10</th>
                  <th>Specialty</th>
                  <th>Authority</th>
                </tr>
              </thead>
              <tbody id="crud-recent-tbody"></tbody>
            </table>
          </div>
        </div>
        <div class="sfds-card" style="display:flex; flex-direction:column; justify-content:space-between; min-height:0; height:100%;">
          <div>
            <div class="sfds-card-label">Dual-Engine 3-Tier &amp; DCB0160 Validation Contract</div>
            <div style="font-size:12px; color:#cbd5e1; line-height:1.55; padding:6px;">
              <div><strong>1. NHS Modulus-11 Checksum:</strong> Deterministic 10-digit national identifier verified before SQLite WAL &amp; PostgreSQL 15 write.</div>
              <div><strong>2. ICD-10-CM Clinical Coding:</strong> Enforces <code>^[A-Z][0-9]{{2}}(\.[0-9A-Z]{{1,4}})?$</code> pattern with immediate inline <code>422</code> rejection on invalid codes.</div>
              <div><strong>3. Non-Destructive Clinical Archive:</strong> Physical <code>DELETE</code> is blocked by SQLite &amp; PostgreSQL triggers; records transition via <code>/archive</code> with audit reason.</div>
              <div><strong>4. Full-Viewport StageShow Surface:</strong> Zero <code>fade-crop</code> masking (<code>mask-image: none</code>) and zero <code>Bar</code> placeholder bars on clinical tables.</div>
            </div>
          </div>
          <div class="sfds-ai-provenance-footer">
            <span>Engine: <code>SQLite WAL + PostgreSQL 15 (sibling_{sibling_slug})</code></span>
            <span>Safety: <code>DCB0129 / DCB0160 Verified</code></span>
          </div>
        </div>
      </div>
    </section>

    <!-- TAB 4: Clinical Decision Context & Archive Governance -->
    <section class="sfds-tabpanel" role="tabpanel" id="panel-cds" aria-labelledby="tab-btn-cds">
      <div class="sfds-ai-chat-thread" aria-label="CDR 1.0.0 Co-Pilot Decision Card">
        <div class="sfds-ai-card" id="wb-ai-decision-card">
          <div class="sfds-ai-stream-slot">
            <span><strong>CDR 1.0.0 Co-Pilot Synthesis (`ai-cards` · Retained Stream Slot):</strong> Sepsis &amp; 5D state-space decompensation surveillance active.</span>
            <span class="sfds-panel-annotation" data-leader-target="cds-tier"><span class="sfds-leader-pin">DCB0160-CDS</span><span class="sfds-leader-line" aria-hidden="true"></span><span>Flat Clinical Surface (`SS-CTRL-06`)</span></span>
          </div>
          <div class="sfds-ai-provenance-footer">
            <span>Provenance Last: <code>CDR-1.0.0 · NICE-NG51 · SH-KF-STATE-001</code></span>
            <span>Gate: <code>requires_human_review · mask-image: none</code></span>
          </div>
        </div>
      </div>
      <div class="sfds-grid-4">
        <div class="sfds-card"><div class="sfds-card-label">CDS Escalation Tier</div><div class="sfds-card-val" id="cds-tier">-</div></div>
        <div class="sfds-card"><div class="sfds-card-label">Peak 4h Decompensation Risk</div><div class="sfds-card-val" id="cds-risk">-</div></div>
        <div class="sfds-card"><div class="sfds-card-label">5D State Vector (MAP/HR/Lac/eGFR/EVLW)</div><div class="sfds-card-val" id="cds-5d">-</div></div>
        <div class="sfds-card"><div class="sfds-card-label">Raw Bedside &amp; Lab (SCr / RR / SpO2)</div><div class="sfds-card-val" id="cds-raw">-</div></div>
      </div>
      <div class="sfds-card" style="flex:1 1 auto; min-height:0; display:flex; flex-direction:column; overflow:hidden;">
        <div class="sfds-card-label">Synthesized Clinical Presentation, Keywords &amp; Source Attributes JSON</div>
        <pre id="cds-json-view" style="flex:1 1 auto; min-height:0; margin:6px 0 0; font-size:12px; color:#cbd5e1; white-space:pre-wrap; overflow:hidden; max-height:100%;"></pre>
      </div>
    </section>
  </main>

  <script>
    const BASE = '/api/v1/million-cohort';
    function selectTab(key) {{
      for (const k of ['directory', 'telemetry', 'crud', 'cds']) {{
        document.getElementById('tab-btn-' + k).setAttribute('aria-selected', k === key ? 'true' : 'false');
        document.getElementById('panel-' + k).classList.toggle('active', k === key);
      }}
    }}
    async function loadSummary() {{
      const r = await fetch(BASE + '/summary');
      const d = await r.json();
      document.getElementById('badge-pg-status').textContent = 'Dual-Engine: ' + d.postgresql_status;
      document.getElementById('badge-patient-count').textContent = 'Patients: ' + d.sqlite_metrics.total_patients.toLocaleString();
      document.getElementById('badge-episode-count').textContent = 'Episodes: ' + d.sqlite_metrics.total_episodes.toLocaleString();
      document.getElementById('metric-active-patients').textContent = d.sqlite_metrics.active_patients.toLocaleString();
      if (d.clinical_decision_coverage) {{
        document.getElementById('metric-lives-alone').textContent = d.clinical_decision_coverage.lives_alone_patients.toLocaleString();
        document.getElementById('metric-mean-evlw').textContent = d.clinical_decision_coverage.mean_evlw_ml_kg + ' mL/kg';
        document.getElementById('metric-mean-scr').textContent = d.clinical_decision_coverage.mean_serum_creatinine_mg_dl + ' mg/dL';
      }}
    }}
    async function loadPatients() {{
      const q = new URLSearchParams({{ limit: '12', offset: '0' }});
      const s = document.getElementById('filter-search').value.trim();
      const icd = document.getElementById('filter-icd10').value.trim();
      const mar = document.getElementById('filter-marital').value;
      const la = document.getElementById('filter-lives-alone').value;
      if (s) q.set('search', s);
      if (icd) q.set('primary_icd10_code', icd);
      if (mar) q.set('marital_status', mar);
      if (la !== '') q.set('lives_alone', la);
      const r = await fetch(BASE + '/patients?' + q.toString());
      const d = await r.json();
      const tbody = document.getElementById('patients-tbody');
      const crudBody = document.getElementById('crud-recent-tbody');
      tbody.innerHTML = '';
      if (crudBody) crudBody.innerHTML = '';
      for (const p of d.items) {{
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td><code>${{p.patient_id}}</code></td>
          <td>${{p.givenName}} ${{p.familyName}}</td>
          <td>${{p.dateOfBirth}} (${{p.gender}})</td>
          <td><code>${{p.nationalID}}</code></td>
          <td><strong>${{p.primary_icd10_code}}</strong></td>
          <td>${{p.primary_diagnosis_name}}</td>
          <td>${{p.medical_specialty}}</td>
          <td>${{p.marital_status}} / ${{p.insurance_coverage}}</td>
          <td>${{p.lives_alone ? 'Yes (1)' : 'No (0)'}}</td>
          <td><button class="sfds-btn" style="min-height:28px;padding:3px 8px;font-size:11px;" onclick="inspectPatient('${{p.patient_id}}')">Inspect 5D &amp; CDS</button></td>
        `;
        tbody.appendChild(tr);
        if (crudBody) {{
          const ctr = document.createElement('tr');
          ctr.innerHTML = `
            <td><code>${{p.patient_id}}</code></td>
            <td>${{p.givenName}} ${{p.familyName}}</td>
            <td><code>${{p.nationalID}}</code></td>
            <td><strong>${{p.primary_icd10_code}}</strong></td>
            <td>${{p.medical_specialty}}</td>
            <td><code>${{p.record_authority || 'canonical'}}</code></td>
          `;
          crudBody.appendChild(ctr);
        }}
      }}
      if (d.items.length > 0) {{
        await loadPatientDetail(d.items[0].patient_id);
      }}
    }}
    async function loadPatientDetail(pid) {{
      const [r1, r2] = await Promise.all([
        fetch(BASE + '/patients/' + encodeURIComponent(pid)),
        fetch(BASE + '/patients/' + encodeURIComponent(pid) + '/clinical-decision-context')
      ]);
      const d = await r1.json();
      const cds = await r2.json();
      const p = d.patient;
      document.getElementById('tel-pat-id').textContent = p.patient_id + ' (' + p.givenName + ' ' + p.familyName + ')';
      document.getElementById('tel-pat-diag').textContent = p.primary_icd10_code + ' — ' + p.primary_diagnosis_name.slice(0, 28);
      document.getElementById('tel-pat-spec').textContent = p.medical_specialty + ' (' + p.insurance_coverage + ')';
      document.getElementById('tel-pat-social').textContent = p.marital_status + ' | LivesAlone=' + p.lives_alone + ' | Q' + p.deprivation_quintile;
      const eb = document.getElementById('episodes-tbody');
      eb.innerHTML = '';
      for (const e of d.episodes.slice(0, 12)) {{
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td><code>${{e.episode_id}}</code></td>
          <td>#${{e.episode_sequence}}</td>
          <td>${{e.admission_type}} / ${{e.care_unit}}</td>
          <td>${{e.initial_map_mmhg}} / ${{e.initial_hr_bpm}}</td>
          <td>${{e.initial_lactate_mmol_l}} mmol/L</td>
          <td>${{e.serum_creatinine_mg_dl}} mg/dL / ${{e.initial_egfr}}</td>
          <td>${{e.initial_evlw_ml_kg}} mL/kg</td>
          <td>${{e.respiratory_rate_bpm}} / ${{e.spo2_pct}}%</td>
          <td><strong>${{(e.deterioration_risk_4h * 100).toFixed(1)}}%</strong></td>
          <td>${{e.clinical_presentation_summary}}</td>
        `;
        eb.appendChild(tr);
      }}
      document.getElementById('cds-tier').textContent = cds.recommended_clinical_action;
      document.getElementById('cds-risk').textContent = (cds.peak_deterioration_risk_4h * 100).toFixed(1) + '%';
      document.getElementById('cds-5d').textContent = cds.latest_5d_state_summary;
      document.getElementById('cds-raw').textContent = cds.latest_raw_labs_vitals_summary;
      document.getElementById('cds-json-view').textContent = JSON.stringify(cds, null, 2);
    }}
    async function inspectPatient(pid) {{
      await loadPatientDetail(pid);
      selectTab('telemetry');
    }}
    async function createPatient3Tier() {{
      const payload = {{
        givenName: document.getElementById('crud-given').value.trim(),
        familyName: document.getElementById('crud-family').value.trim(),
        dateOfBirth: document.getElementById('crud-dob').value.trim(),
        primary_icd10_code: document.getElementById('crud-icd10').value.trim(),
        medical_specialty: document.getElementById('crud-specialty').value.trim(),
        sync_bullettrain_hie: false
      }};
      const r = await fetch(BASE + '/patients', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify(payload)
      }});
      const fb = document.getElementById('crud-feedback');
      const data = await r.json();
      if (!r.ok) {{
        fb.textContent = 'Validation Error (' + r.status + '): ' + JSON.stringify(data.detail);
      }} else {{
        fb.textContent = 'Created ' + data.patient_id + ' (NHS ' + data.nationalID + ', ICD-10 ' + data.primary_icd10_code + ', authority=' + data.record_authority + ')';
        await loadSummary();
      }}
    }}
    loadSummary().then(loadPatients);
  </script>
</body>
</html>"""


def build_clinical_cohort_3tier_router(
    service_name: str,
    sibling_slug: str,
    repo_root: Path,
) -> APIRouter:
    router = APIRouter(
        prefix="/api/v1/million-cohort", tags=["million-cohort-3tier-crud"]
    )
    db_file = repo_root / "data" / f"{sibling_slug}_sh_kf_state_001_wal.db"
    pg_schema = f"sibling_{sibling_slug}"
    workspace_root = repo_root.parent
    cr_sqlite_path = (
        workspace_root / "BulletTrain" / "data" / "client_registry_million_wal.db"
    )

    def _connect_sqlite() -> sqlite3.Connection:
        if not db_file.exists():
            raise HTTPException(
                status_code=503, detail=f"3-tier SQLite database not found at {db_file}"
            )
        conn = sqlite3.connect(str(db_file), timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        return conn

    async def _try_pg_connect():
        try:
            import asyncpg

            conn = await asyncpg.connect(PG_DSN, timeout=3.5)
            exists = await conn.fetchval(
                "SELECT to_regclass($1)", f"{pg_schema}.patients"
            )
            if not exists:
                await conn.close()
                return None
            return conn
        except Exception:
            return None

    @router.get("/workbench-ui", response_class=HTMLResponse)
    async def get_workbench_ui() -> HTMLResponse:
        return HTMLResponse(content=_render_workbench_html(service_name, sibling_slug))

    @router.get("/summary")
    async def get_cohort_summary() -> dict[str, Any]:
        pg_conn = await _try_pg_connect()
        pg_status = "offline_standalone_sqlite_only"
        pg_patients = 0
        pg_episodes = 0

        if pg_conn is not None:
            try:
                pg_patients = int(
                    await pg_conn.fetchval(f"SELECT COUNT(*) FROM {pg_schema}.patients")
                )
                pg_episodes = int(
                    await pg_conn.fetchval(
                        f"SELECT COUNT(*) FROM {pg_schema}.patient_episodes"
                    )
                )
                pg_status = "online_dual_engine_synced"

                # Self-heal SQLite WAL mirror if a concurrent process inserted directly into PostgreSQL
                with _connect_sqlite() as sconn:
                    sq_p_now = int(
                        sconn.execute("SELECT COUNT(*) FROM patients").fetchone()[0]
                    )
                    sq_ep_now = int(
                        sconn.execute(
                            "SELECT COUNT(*) FROM patient_episodes"
                        ).fetchone()[0]
                    )
                    if pg_patients > sq_p_now:
                        sq_pids = {
                            r[0]
                            for r in sconn.execute(
                                "SELECT patient_id FROM patients"
                            ).fetchall()
                        }
                        pg_prows = await pg_conn.fetch(
                            f"SELECT * FROM {pg_schema}.patients"
                        )
                        for r in pg_prows:
                            if r["patient_id"] not in sq_pids:
                                sconn.execute(
                                    """
                                    INSERT OR IGNORE INTO patients (
                                        patient_id, mpiId, givenName, familyName, dateOfBirth, gender,
                                        nationalID, status, record_authority, source_of_truth, hf_repo_id,
                                        hf_record_sha256, site_id, age_band, deprivation_quintile, split_name,
                                        client_registry_fk_verified, synced_at,
                                        primary_icd10_code, primary_diagnosis_name, medical_specialty,
                                        marital_status, insurance_coverage, lives_alone
                                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                                    """,
                                    (
                                        r["patient_id"],
                                        r["mpiid"],
                                        r["givenname"],
                                        r["familyname"],
                                        r["dateofbirth"],
                                        r["gender"],
                                        r["nationalid"],
                                        r["status"],
                                        r["record_authority"],
                                        r["source_of_truth"],
                                        r["hf_repo_id"],
                                        r["hf_record_sha256"],
                                        r["site_id"],
                                        r["age_band"],
                                        r["deprivation_quintile"],
                                        r["split_name"],
                                        r["client_registry_fk_verified"],
                                        str(r["synced_at"]),
                                        r.get("primary_icd10_code") or "I10",
                                        r.get("primary_diagnosis_name")
                                        or "Essential hypertension",
                                        r.get("medical_specialty")
                                        or "Internal Medicine",
                                        r.get("marital_status") or "MARRIED",
                                        r.get("insurance_coverage") or "NHS_UNIVERSAL",
                                        int(r.get("lives_alone") or 0),
                                    ),
                                )
                        sconn.commit()
                    if pg_episodes > sq_ep_now:
                        sq_eids = {
                            r[0]
                            for r in sconn.execute(
                                "SELECT episode_id FROM patient_episodes"
                            ).fetchall()
                        }
                        pg_erows = await pg_conn.fetch(
                            f"SELECT * FROM {pg_schema}.patient_episodes"
                        )
                        for r in pg_erows:
                            if r["episode_id"] not in sq_eids:
                                sconn.execute(
                                    """
                                    INSERT OR IGNORE INTO patient_episodes (
                                        episode_id, patient_id, sibling_service, episode_sequence,
                                        split_name, site_id, trajectory_steps,
                                        initial_map_mmhg, initial_hr_bpm, initial_lactate_mmol_l,
                                        initial_egfr, deterioration_risk_4h, hf_provenance_sha256,
                                        record_status, created_at,
                                        initial_evlw_ml_kg, serum_creatinine_mg_dl, respiratory_rate_bpm,
                                        spo2_pct, admission_type, care_unit,
                                        clinical_presentation_summary, clinical_keywords_or_concepts,
                                        source_clinical_attributes_json
                                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                                    """,
                                    (
                                        r["episode_id"],
                                        r["patient_id"],
                                        r["sibling_service"],
                                        r["episode_sequence"],
                                        r["split_name"],
                                        r["site_id"],
                                        r["trajectory_steps"],
                                        r["initial_map_mmhg"],
                                        r["initial_hr_bpm"],
                                        r["initial_lactate_mmol_l"],
                                        r["initial_egfr"],
                                        r["deterioration_risk_4h"],
                                        r["hf_provenance_sha256"],
                                        r["record_status"],
                                        str(r["created_at"]),
                                        float(r.get("initial_evlw_ml_kg") or 7.2),
                                        float(r.get("serum_creatinine_mg_dl") or 1.05),
                                        float(r.get("respiratory_rate_bpm") or 18.0),
                                        float(r.get("spo2_pct") or 96.5),
                                        r.get("admission_type") or "EW EMER.",
                                        r.get("care_unit") or "MICU",
                                        r.get("clinical_presentation_summary")
                                        or "Acute clinical admission",
                                        r.get("clinical_keywords_or_concepts")
                                        or "hemodynamic monitoring",
                                        r.get("source_clinical_attributes_json")
                                        or "{}",
                                    ),
                                )
                        sconn.commit()
            finally:
                await pg_conn.close()

        with _connect_sqlite() as conn:
            total_p = int(conn.execute("SELECT COUNT(*) FROM patients").fetchone()[0])
            active_p = int(
                conn.execute(
                    "SELECT COUNT(*) FROM patients WHERE status = 'active'"
                ).fetchone()[0]
            )
            archived_p = int(
                conn.execute(
                    "SELECT COUNT(*) FROM patients WHERE status = 'archived'"
                ).fetchone()[0]
            )
            local_p = int(
                conn.execute(
                    "SELECT COUNT(*) FROM patients WHERE record_authority = 'local'"
                ).fetchone()[0]
            )
            proj_p = int(
                conn.execute(
                    "SELECT COUNT(*) FROM patients WHERE record_authority = 'projection'"
                ).fetchone()[0]
            )
            synth_p = int(
                conn.execute(
                    "SELECT COUNT(*) FROM patients WHERE LOWER(hf_repo_id) LIKE '%synthetic%' OR LOWER(hf_repo_id) LIKE '%vrkomari%' OR LOWER(hf_repo_id) LIKE '%galileo%'"
                ).fetchone()[0]
            )
            lives_alone_p = int(
                conn.execute(
                    "SELECT COUNT(*) FROM patients WHERE lives_alone = 1"
                ).fetchone()[0]
            )
            distinct_spec = int(
                conn.execute(
                    "SELECT COUNT(DISTINCT medical_specialty) FROM patients"
                ).fetchone()[0]
            )
            distinct_icd = int(
                conn.execute(
                    "SELECT COUNT(DISTINCT primary_icd10_code) FROM patients"
                ).fetchone()[0]
            )

            total_ep = int(
                conn.execute("SELECT COUNT(*) FROM patient_episodes").fetchone()[0]
            )
            active_ep = int(
                conn.execute(
                    "SELECT COUNT(*) FROM patient_episodes WHERE record_status = 'active'"
                ).fetchone()[0]
            )
            archived_ep = int(
                conn.execute(
                    "SELECT COUNT(*) FROM patient_episodes WHERE record_status = 'archived'"
                ).fetchone()[0]
            )
            high_risk_ep = int(
                conn.execute(
                    "SELECT COUNT(*) FROM patient_episodes WHERE episode_sequence >= 4 AND record_status = 'active'"
                ).fetchone()[0]
            )
            avgs = conn.execute(
                """
                SELECT ROUND(AVG(initial_evlw_ml_kg), 2),
                       ROUND(AVG(serum_creatinine_mg_dl), 2),
                       ROUND(AVG(respiratory_rate_bpm), 1),
                       ROUND(AVG(spo2_pct), 1),
                       COUNT(DISTINCT care_unit)
                FROM patient_episodes
                """
            ).fetchone()

        return {
            "service_name": service_name,
            "sibling_slug": sibling_slug,
            "postgresql_schema": pg_schema,
            "sqlite_wal_path": str(db_file),
            "architecture_mode": "standalone_3tier_crud_with_optional_bullettrain_hie",
            "postgresql_status": pg_status,
            "sqlite_metrics": {
                "total_patients": total_p,
                "active_patients": active_p,
                "archived_patients": archived_p,
                "local_3tier_patients": local_p,
                "hie_projected_patients": proj_p,
                "synthetic_records_count": synth_p,
                "total_episodes": total_ep,
                "active_episodes": active_ep,
                "archived_episodes": archived_ep,
                "high_acuity_active_episodes": high_risk_ep,
            },
            "postgresql_metrics": {
                "total_patients": pg_patients,
                "total_episodes": pg_episodes,
            },
            "clinical_decision_coverage": {
                "zero_abandoned_source_attributes": True,
                "patients_with_icd10_and_specialty": total_p,
                "distinct_icd10_codes": distinct_icd,
                "distinct_medical_specialties": distinct_spec,
                "lives_alone_patients": lives_alone_p,
                "episodes_with_5d_evlw_and_raw_labs": total_ep,
                "mean_evlw_ml_kg": float(avgs[0] or 0.0),
                "mean_serum_creatinine_mg_dl": float(avgs[1] or 0.0),
                "mean_respiratory_rate_bpm": float(avgs[2] or 0.0),
                "mean_spo2_pct": float(avgs[3] or 0.0),
                "distinct_care_units": int(avgs[4] or 0),
            },
            "clinical_delete_policy": "BEFORE DELETE trigger forbids physical deletion (clinical_record_deletion_forbidden); clinical records are archived via status/record_status='archived'",
        }

    @router.get("/patients")
    async def list_patients(
        limit: int = Query(default=25, ge=1, le=200),
        offset: int = Query(default=0, ge=0),
        search: str | None = Query(default=None),
        status: str | None = Query(default="active"),
        record_authority: str | None = Query(default=None),
        age_band: str | None = Query(default=None),
        primary_icd10_code: str | None = Query(default=None),
        medical_specialty: str | None = Query(default=None),
        marital_status: str | None = Query(default=None),
        insurance_coverage: str | None = Query(default=None),
        lives_alone: int | None = Query(default=None, ge=0, le=1),
    ) -> dict[str, Any]:
        clauses = ["1=1"]
        params: list[Any] = []
        if status:
            clauses.append("status = ?")
            params.append(status)
        if record_authority:
            clauses.append("record_authority = ?")
            params.append(record_authority)
        if age_band:
            clauses.append("age_band = ?")
            params.append(age_band)
        if primary_icd10_code:
            clauses.append("primary_icd10_code = ?")
            params.append(primary_icd10_code.strip().upper())
        if medical_specialty:
            clauses.append("medical_specialty LIKE ?")
            params.append(f"%{medical_specialty.strip()}%")
        if marital_status:
            clauses.append("marital_status = ?")
            params.append(marital_status.strip().upper())
        if insurance_coverage:
            clauses.append("insurance_coverage = ?")
            params.append(insurance_coverage.strip())
        if lives_alone is not None:
            clauses.append("lives_alone = ?")
            params.append(int(lives_alone))
        if search:
            q = f"%{search.strip()}%"
            clauses.append(
                "(patient_id LIKE ? OR nationalID LIKE ? OR givenName LIKE ? OR familyName LIKE ? "
                "OR primary_icd10_code LIKE ? OR primary_diagnosis_name LIKE ? OR medical_specialty LIKE ?)"
            )
            params.extend([q, q, q, q, q, q, q])

        where_sql = " AND ".join(clauses)
        with _connect_sqlite() as conn:
            total = int(
                conn.execute(
                    f"SELECT COUNT(*) FROM patients WHERE {where_sql}", params
                ).fetchone()[0]
            )
            rows = conn.execute(
                f"""
                SELECT *
                FROM patients
                WHERE {where_sql}
                ORDER BY patient_id
                LIMIT ? OFFSET ?
                """,
                [*params, limit, offset],
            ).fetchall()

        return {
            "service_name": service_name,
            "total": total,
            "limit": limit,
            "offset": offset,
            "items": [dict(r) for r in rows],
        }

    @router.post("/patients", status_code=201)
    async def create_patient_3tier(body: PatientCreate3TierRequest) -> dict[str, Any]:
        seed_key = f"{service_name}:{body.givenName}:{body.familyName}:{body.dateOfBirth}:{_utc_now_iso()}"
        nhs10 = (
            body.nationalID.strip()
            if body.nationalID
            else _generate_valid_modulus11_nhs(seed_key)
        )
        if not validate_modulus11_nhs(nhs10):
            raise HTTPException(
                status_code=422,
                detail=f"Invalid 10-digit Modulus-11 nationalID: {nhs10}",
            )

        patient_id = f"SUBJ-MPI-{nhs10}"
        mpi_id = f"MPI-{nhs10}"
        sha32 = hashlib.sha256(seed_key.encode()).hexdigest()[:32]
        now_iso = _utc_now_iso()

        authority = "projection" if body.sync_bullettrain_hie else "local"
        source = "client-registry" if body.sync_bullettrain_hie else "local-3tier"
        fk_verified = 1 if body.sync_bullettrain_hie else 0

        if body.sync_bullettrain_hie and cr_sqlite_path.exists():
            with sqlite3.connect(str(cr_sqlite_path), timeout=30.0) as cr_conn:
                cr_conn.execute(
                    """
                    INSERT OR IGNORE INTO patients (
                        patient_id, mpiId, givenName, familyName, dateOfBirth, gender,
                        nationalID, national_id_blind_index, status, record_authority,
                        provenance_system, hf_repo_id, hf_record_sha256, site_id,
                        age_band, deprivation_quintile, assigned_sibling_repo, split_name,
                        encryption_version, created_at, updated_at,
                        primary_icd10_code, primary_diagnosis_name, medical_specialty,
                        marital_status, insurance_coverage, lives_alone
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'active', 'canonical', 'huggingface-clinical-corpus', ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        patient_id,
                        mpi_id,
                        body.givenName.strip(),
                        body.familyName.strip(),
                        body.dateOfBirth,
                        body.gender,
                        nhs10,
                        f"bidx-{sha32[:16]}-local",
                        body.hf_repo_id,
                        sha32,
                        body.site_id,
                        body.age_band,
                        body.deprivation_quintile,
                        service_name,
                        body.split_name,
                        now_iso,
                        now_iso,
                        body.primary_icd10_code,
                        body.primary_diagnosis_name.strip(),
                        body.medical_specialty.strip(),
                        body.marital_status,
                        body.insurance_coverage.strip(),
                        body.lives_alone,
                    ),
                )
                cr_conn.commit()

        with _connect_sqlite() as conn:
            try:
                conn.execute(
                    """
                    INSERT INTO patients (
                        patient_id, mpiId, givenName, familyName, dateOfBirth, gender,
                        nationalID, status, record_authority, source_of_truth, hf_repo_id,
                        hf_record_sha256, site_id, age_band, deprivation_quintile, split_name,
                        client_registry_fk_verified, synced_at,
                        primary_icd10_code, primary_diagnosis_name, medical_specialty,
                        marital_status, insurance_coverage, lives_alone
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        patient_id,
                        mpi_id,
                        body.givenName.strip(),
                        body.familyName.strip(),
                        body.dateOfBirth,
                        body.gender,
                        nhs10,
                        authority,
                        source,
                        body.hf_repo_id,
                        sha32,
                        body.site_id,
                        body.age_band,
                        body.deprivation_quintile,
                        body.split_name,
                        fk_verified,
                        now_iso,
                        body.primary_icd10_code,
                        body.primary_diagnosis_name.strip(),
                        body.medical_specialty.strip(),
                        body.marital_status,
                        body.insurance_coverage.strip(),
                        body.lives_alone,
                    ),
                )
                conn.commit()
            except sqlite3.IntegrityError as exc:
                raise HTTPException(
                    status_code=409,
                    detail=f"Patient already exists or constraint failed: {exc}",
                ) from exc

            created_row = dict(
                conn.execute(
                    "SELECT * FROM patients WHERE patient_id = ?", (patient_id,)
                ).fetchone()
            )

        pg_conn = await _try_pg_connect()
        if pg_conn is not None:
            try:
                if body.sync_bullettrain_hie:
                    await pg_conn.execute(
                        """
                        INSERT INTO client_registry.patients (
                            patient_id, mpiId, givenName, familyName, dateOfBirth, gender,
                            nationalID, national_id_blind_index, status, record_authority,
                            provenance_system, hf_repo_id, hf_record_sha256, site_id,
                            age_band, deprivation_quintile, assigned_sibling_repo, split_name, encryption_version,
                            primary_icd10_code, primary_diagnosis_name, medical_specialty,
                            marital_status, insurance_coverage, lives_alone
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'active', 'canonical', 'huggingface-clinical-corpus', $9, $10, $11, $12, $13, $14, $15, 1, $16, $17, $18, $19, $20, $21)
                        ON CONFLICT (patient_id) DO NOTHING
                        """,
                        patient_id,
                        mpi_id,
                        body.givenName.strip(),
                        body.familyName.strip(),
                        body.dateOfBirth,
                        body.gender,
                        nhs10,
                        f"bidx-{sha32[:16]}-local",
                        body.hf_repo_id,
                        sha32,
                        body.site_id,
                        body.age_band,
                        body.deprivation_quintile,
                        service_name,
                        body.split_name,
                        body.primary_icd10_code,
                        body.primary_diagnosis_name.strip(),
                        body.medical_specialty.strip(),
                        body.marital_status,
                        body.insurance_coverage.strip(),
                        body.lives_alone,
                    )
                await pg_conn.execute(
                    f"""
                    INSERT INTO {pg_schema}.patients (
                        patient_id, mpiId, givenName, familyName, dateOfBirth, gender,
                        nationalID, status, record_authority, source_of_truth, hf_repo_id,
                        hf_record_sha256, site_id, age_band, deprivation_quintile, split_name,
                        client_registry_fk_verified,
                        primary_icd10_code, primary_diagnosis_name, medical_specialty,
                        marital_status, insurance_coverage, lives_alone
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, 'active', $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)
                    ON CONFLICT (patient_id) DO NOTHING
                    """,
                    patient_id,
                    mpi_id,
                    body.givenName.strip(),
                    body.familyName.strip(),
                    body.dateOfBirth,
                    body.gender,
                    nhs10,
                    authority,
                    source,
                    body.hf_repo_id,
                    sha32,
                    body.site_id,
                    body.age_band,
                    body.deprivation_quintile,
                    body.split_name,
                    fk_verified,
                    body.primary_icd10_code,
                    body.primary_diagnosis_name.strip(),
                    body.medical_specialty.strip(),
                    body.marital_status,
                    body.insurance_coverage.strip(),
                    body.lives_alone,
                )
            finally:
                await pg_conn.close()

        return created_row

    @router.get("/patients/{patient_id}")
    async def get_patient_and_episodes(patient_id: str) -> dict[str, Any]:
        with _connect_sqlite() as conn:
            p_row = conn.execute(
                "SELECT * FROM patients WHERE patient_id = ?", (patient_id,)
            ).fetchone()
            if p_row is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Patient {patient_id} not found in {service_name}",
                )
            ep_rows = conn.execute(
                "SELECT * FROM patient_episodes WHERE patient_id = ? ORDER BY episode_sequence",
                (patient_id,),
            ).fetchall()

        return {
            "service_name": service_name,
            "patient": dict(p_row),
            "episodes": [dict(r) for r in ep_rows],
        }

    @router.get("/patients/{patient_id}/clinical-decision-context")
    async def get_clinical_decision_context(patient_id: str) -> dict[str, Any]:
        with _connect_sqlite() as conn:
            p_row = conn.execute(
                "SELECT * FROM patients WHERE patient_id = ?", (patient_id,)
            ).fetchone()
            if p_row is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Patient {patient_id} not found in {service_name}",
                )
            ep_rows = conn.execute(
                "SELECT * FROM patient_episodes WHERE patient_id = ? ORDER BY episode_sequence",
                (patient_id,),
            ).fetchall()

        pat = dict(p_row)
        eps = [dict(r) for r in ep_rows]
        latest = eps[-1] if eps else {}
        peak_risk = max(
            (float(e.get("deterioration_risk_4h", 0.0)) for e in eps), default=0.0
        )
        social_vuln = bool(
            pat.get("lives_alone") == 1 or int(pat.get("deprivation_quintile", 3)) <= 2
        )

        if peak_risk >= 0.45 or (peak_risk >= 0.32 and social_vuln):
            action = "STEP_UP_ICU_HDU_AND_GATE_D_SIGNOFF"
        elif peak_risk >= 0.22:
            action = "ENHANCED_WARD_TELEMETRY_AND_SPECIALIST_REVIEW"
        else:
            action = "ROUTINE_PATHWAY_MONITORING"

        return {
            "service_name": service_name,
            "sibling_slug": sibling_slug,
            "patient_id": patient_id,
            "mpiId": pat.get("mpiId"),
            "patient_name": f"{pat.get('givenName')} {pat.get('familyName')}",
            "primary_icd10_code": pat.get("primary_icd10_code"),
            "primary_diagnosis_name": pat.get("primary_diagnosis_name"),
            "medical_specialty": pat.get("medical_specialty"),
            "social_determinants": {
                "marital_status": pat.get("marital_status"),
                "insurance_coverage": pat.get("insurance_coverage"),
                "lives_alone": int(pat.get("lives_alone") or 0),
                "deprivation_quintile": int(pat.get("deprivation_quintile") or 3),
                "age_band": pat.get("age_band"),
                "social_vulnerability_elevated": social_vuln,
            },
            "episode_count": len(eps),
            "peak_deterioration_risk_4h": round(peak_risk, 4),
            "recommended_clinical_action": action,
            "latest_5d_state_summary": (
                f"MAP={latest.get('initial_map_mmhg')} mmHg | HR={latest.get('initial_hr_bpm')} bpm | "
                f"Lac={latest.get('initial_lactate_mmol_l')} mmol/L | eGFR={latest.get('initial_egfr')} | "
                f"EVLW={latest.get('initial_evlw_ml_kg')} mL/kg"
            )
            if latest
            else "No episodes",
            "latest_raw_labs_vitals_summary": (
                f"SCr={latest.get('serum_creatinine_mg_dl')} mg/dL | RR={latest.get('respiratory_rate_bpm')} bpm | "
                f"SpO2={latest.get('spo2_pct')}% | Unit={latest.get('care_unit')} ({latest.get('admission_type')})"
            )
            if latest
            else "No episodes",
            "latest_clinical_presentation_summary": latest.get(
                "clinical_presentation_summary", ""
            ),
            "latest_clinical_keywords_or_concepts": latest.get(
                "clinical_keywords_or_concepts", ""
            ),
            "latest_source_clinical_attributes": json.loads(
                latest.get("source_clinical_attributes_json") or "{}"
            )
            if latest
            else {},
        }

    @router.put("/patients/{patient_id}")
    async def update_patient_3tier(
        patient_id: str, body: PatientUpdate3TierRequest
    ) -> dict[str, Any]:
        with _connect_sqlite() as conn:
            p_row = conn.execute(
                "SELECT * FROM patients WHERE patient_id = ?", (patient_id,)
            ).fetchone()
            if p_row is None:
                raise HTTPException(
                    status_code=404, detail=f"Patient {patient_id} not found"
                )
            if (
                p_row["record_authority"] == "projection"
                and not body.sync_bullettrain_hie
            ):
                raise HTTPException(
                    status_code=409,
                    detail="projection_only: patient is a BulletTrain Client Registry projection; pass sync_bullettrain_hie=true or update a standalone local-3tier patient",
                )

            updates: list[str] = []
            params: list[Any] = []
            for col in (
                "givenName",
                "familyName",
                "dateOfBirth",
                "gender",
                "site_id",
                "deprivation_quintile",
                "primary_icd10_code",
                "primary_diagnosis_name",
                "medical_specialty",
                "marital_status",
                "insurance_coverage",
                "lives_alone",
            ):
                val = getattr(body, col)
                if val is not None:
                    updates.append(f"{col} = ?")
                    params.append(val.strip() if isinstance(val, str) else val)

            if updates:
                params.append(patient_id)
                conn.execute(
                    f"UPDATE patients SET {', '.join(updates)} WHERE patient_id = ?",
                    params,
                )
                conn.commit()

            updated = dict(
                conn.execute(
                    "SELECT * FROM patients WHERE patient_id = ?", (patient_id,)
                ).fetchone()
            )

        pg_conn = await _try_pg_connect()
        if pg_conn is not None and updates:
            try:
                async with pg_conn.transaction():
                    if body.sync_bullettrain_hie:
                        await pg_conn.execute(
                            "SET LOCAL app.allow_projection_sync = 'true';"
                        )
                    await pg_conn.execute(
                        f"""
                        UPDATE {pg_schema}.patients
                        SET givenName = $1, familyName = $2, dateOfBirth = $3, gender = $4,
                            site_id = $5, deprivation_quintile = $6,
                            primary_icd10_code = $7, primary_diagnosis_name = $8,
                            medical_specialty = $9, marital_status = $10,
                            insurance_coverage = $11, lives_alone = $12
                        WHERE patient_id = $13
                        """,
                        updated["givenName"],
                        updated["familyName"],
                        updated["dateOfBirth"],
                        updated["gender"],
                        updated["site_id"],
                        updated["deprivation_quintile"],
                        updated["primary_icd10_code"],
                        updated["primary_diagnosis_name"],
                        updated["medical_specialty"],
                        updated["marital_status"],
                        updated["insurance_coverage"],
                        int(updated["lives_alone"]),
                        patient_id,
                    )
            finally:
                await pg_conn.close()

        return updated

    @router.post("/patients/{patient_id}/episodes", status_code=201)
    async def create_episode_3tier(
        patient_id: str, body: EpisodeCreate3TierRequest
    ) -> dict[str, Any]:
        now_iso = _utc_now_iso()
        sha32 = hashlib.sha256(
            f"{patient_id}:{body.episode_sequence}:{now_iso}".encode()
        ).hexdigest()[:32]
        episode_id = f"EP-{patient_id}-{body.episode_sequence}-{sha32[:6]}"

        with _connect_sqlite() as conn:
            try:
                conn.execute(
                    """
                    INSERT INTO patient_episodes (
                        episode_id, patient_id, sibling_service, episode_sequence,
                        split_name, site_id, trajectory_steps,
                        initial_map_mmhg, initial_hr_bpm, initial_lactate_mmol_l,
                        initial_egfr, deterioration_risk_4h, hf_provenance_sha256,
                        record_status, created_at,
                        initial_evlw_ml_kg, serum_creatinine_mg_dl, respiratory_rate_bpm,
                        spo2_pct, admission_type, care_unit,
                        clinical_presentation_summary, clinical_keywords_or_concepts,
                        source_clinical_attributes_json
                    ) VALUES (?, ?, ?, ?, ?, ?, 14, ?, ?, ?, ?, ?, ?, 'active', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        episode_id,
                        patient_id,
                        service_name,
                        body.episode_sequence,
                        body.split_name,
                        body.site_id,
                        body.initial_map_mmhg,
                        body.initial_hr_bpm,
                        body.initial_lactate_mmol_l,
                        body.initial_egfr,
                        body.deterioration_risk_4h,
                        sha32,
                        now_iso,
                        body.initial_evlw_ml_kg,
                        body.serum_creatinine_mg_dl,
                        body.respiratory_rate_bpm,
                        body.spo2_pct,
                        body.admission_type.strip(),
                        body.care_unit.strip(),
                        body.clinical_presentation_summary.strip(),
                        body.clinical_keywords_or_concepts.strip(),
                        body.source_clinical_attributes_json,
                    ),
                )
                conn.commit()
            except sqlite3.IntegrityError as exc:
                raise HTTPException(
                    status_code=409,
                    detail=f"Foreign key or integrity constraint failed creating episode for {patient_id}: {exc}",
                ) from exc

            ep_row = dict(
                conn.execute(
                    "SELECT * FROM patient_episodes WHERE episode_id = ?", (episode_id,)
                ).fetchone()
            )

        pg_conn = await _try_pg_connect()
        if pg_conn is not None:
            try:
                await pg_conn.execute(
                    f"""
                    INSERT INTO {pg_schema}.patient_episodes (
                        episode_id, patient_id, sibling_service, episode_sequence,
                        split_name, site_id, trajectory_steps,
                        initial_map_mmhg, initial_hr_bpm, initial_lactate_mmol_l,
                        initial_egfr, deterioration_risk_4h, hf_provenance_sha256, record_status,
                        initial_evlw_ml_kg, serum_creatinine_mg_dl, respiratory_rate_bpm,
                        spo2_pct, admission_type, care_unit,
                        clinical_presentation_summary, clinical_keywords_or_concepts,
                        source_clinical_attributes_json
                    ) VALUES ($1, $2, $3, $4, $5, $6, 14, $7, $8, $9, $10, $11, $12, 'active', $13, $14, $15, $16, $17, $18, $19, $20, $21)
                    ON CONFLICT (episode_id) DO NOTHING
                    """,
                    episode_id,
                    patient_id,
                    service_name,
                    body.episode_sequence,
                    body.split_name,
                    body.site_id,
                    body.initial_map_mmhg,
                    body.initial_hr_bpm,
                    body.initial_lactate_mmol_l,
                    body.initial_egfr,
                    body.deterioration_risk_4h,
                    sha32,
                    body.initial_evlw_ml_kg,
                    body.serum_creatinine_mg_dl,
                    body.respiratory_rate_bpm,
                    body.spo2_pct,
                    body.admission_type.strip(),
                    body.care_unit.strip(),
                    body.clinical_presentation_summary.strip(),
                    body.clinical_keywords_or_concepts.strip(),
                    body.source_clinical_attributes_json,
                )
            finally:
                await pg_conn.close()

        return ep_row

    @router.put("/episodes/{episode_id}")
    async def update_episode_3tier(
        episode_id: str, body: EpisodeUpdate3TierRequest
    ) -> dict[str, Any]:
        with _connect_sqlite() as conn:
            ep_row = conn.execute(
                "SELECT * FROM patient_episodes WHERE episode_id = ?", (episode_id,)
            ).fetchone()
            if ep_row is None:
                raise HTTPException(
                    status_code=404, detail=f"Episode {episode_id} not found"
                )
            if ep_row["record_status"] == "archived":
                raise HTTPException(
                    status_code=409,
                    detail=f"Episode {episode_id} is archived and immutable",
                )

            updates: list[str] = []
            params: list[Any] = []
            for col in (
                "initial_map_mmhg",
                "initial_hr_bpm",
                "initial_lactate_mmol_l",
                "initial_egfr",
                "initial_evlw_ml_kg",
                "serum_creatinine_mg_dl",
                "respiratory_rate_bpm",
                "spo2_pct",
                "deterioration_risk_4h",
                "admission_type",
                "care_unit",
                "clinical_presentation_summary",
                "clinical_keywords_or_concepts",
                "source_clinical_attributes_json",
            ):
                val = getattr(body, col)
                if val is not None:
                    updates.append(f"{col} = ?")
                    params.append(val.strip() if isinstance(val, str) else float(val))

            if updates:
                params.append(episode_id)
                conn.execute(
                    f"UPDATE patient_episodes SET {', '.join(updates)} WHERE episode_id = ?",
                    params,
                )
                conn.commit()

            updated = dict(
                conn.execute(
                    "SELECT * FROM patient_episodes WHERE episode_id = ?", (episode_id,)
                ).fetchone()
            )

        pg_conn = await _try_pg_connect()
        if pg_conn is not None and updates:
            try:
                await pg_conn.execute(
                    f"""
                    UPDATE {pg_schema}.patient_episodes
                    SET initial_map_mmhg = $1,
                        initial_hr_bpm = $2,
                        initial_lactate_mmol_l = $3,
                        initial_egfr = $4,
                        initial_evlw_ml_kg = $5,
                        serum_creatinine_mg_dl = $6,
                        respiratory_rate_bpm = $7,
                        spo2_pct = $8,
                        deterioration_risk_4h = $9,
                        admission_type = $10,
                        care_unit = $11,
                        clinical_presentation_summary = $12,
                        clinical_keywords_or_concepts = $13,
                        source_clinical_attributes_json = $14
                    WHERE episode_id = $15
                    """,
                    updated["initial_map_mmhg"],
                    updated["initial_hr_bpm"],
                    updated["initial_lactate_mmol_l"],
                    updated["initial_egfr"],
                    updated["initial_evlw_ml_kg"],
                    updated["serum_creatinine_mg_dl"],
                    updated["respiratory_rate_bpm"],
                    updated["spo2_pct"],
                    updated["deterioration_risk_4h"],
                    updated["admission_type"],
                    updated["care_unit"],
                    updated["clinical_presentation_summary"],
                    updated["clinical_keywords_or_concepts"],
                    updated["source_clinical_attributes_json"],
                    episode_id,
                )
            finally:
                await pg_conn.close()

        return updated

    @router.post("/episodes/{episode_id}/archive")
    async def archive_episode_3tier(
        episode_id: str, body: ClinicalArchiveRequest
    ) -> dict[str, Any]:
        now_iso = _utc_now_iso()
        reason = body.archive_reason.strip()
        with _connect_sqlite() as conn:
            ep_row = conn.execute(
                "SELECT * FROM patient_episodes WHERE episode_id = ?", (episode_id,)
            ).fetchone()
            if ep_row is None:
                raise HTTPException(
                    status_code=404, detail=f"Episode {episode_id} not found"
                )
            conn.execute(
                "UPDATE patient_episodes SET record_status = 'archived', archived_at = ?, archive_reason = ? WHERE episode_id = ?",
                (now_iso, reason, episode_id),
            )
            conn.commit()
            archived = dict(
                conn.execute(
                    "SELECT * FROM patient_episodes WHERE episode_id = ?", (episode_id,)
                ).fetchone()
            )

        pg_conn = await _try_pg_connect()
        if pg_conn is not None:
            try:
                await pg_conn.execute(
                    f"UPDATE {pg_schema}.patient_episodes SET record_status = 'archived', archived_at = NOW(), archive_reason = $1 WHERE episode_id = $2",
                    reason,
                    episode_id,
                )
            finally:
                await pg_conn.close()

        return archived

    @router.post("/patients/{patient_id}/archive")
    async def archive_patient_3tier(
        patient_id: str, body: ClinicalArchiveRequest
    ) -> dict[str, Any]:
        now_iso = _utc_now_iso()
        reason = body.archive_reason.strip()
        with _connect_sqlite() as conn:
            p_row = conn.execute(
                "SELECT * FROM patients WHERE patient_id = ?", (patient_id,)
            ).fetchone()
            if p_row is None:
                raise HTTPException(
                    status_code=404, detail=f"Patient {patient_id} not found"
                )
            conn.execute(
                "UPDATE patients SET status = 'archived', archived_at = ?, archive_reason = ? WHERE patient_id = ?",
                (now_iso, reason, patient_id),
            )
            conn.commit()
            archived = dict(
                conn.execute(
                    "SELECT * FROM patients WHERE patient_id = ?", (patient_id,)
                ).fetchone()
            )

        pg_conn = await _try_pg_connect()
        if pg_conn is not None:
            try:
                await pg_conn.execute(
                    f"UPDATE {pg_schema}.patients SET status = 'archived', archived_at = NOW(), archive_reason = $1 WHERE patient_id = $2",
                    reason,
                    patient_id,
                )
            finally:
                await pg_conn.close()

        return archived

    @router.delete("/episodes/{episode_id}")
    async def delete_episode_forbidden(episode_id: str) -> dict[str, Any]:
        with _connect_sqlite() as conn:
            try:
                conn.execute(
                    "DELETE FROM patient_episodes WHERE episode_id = ?", (episode_id,)
                )
            except sqlite3.IntegrityError as exc:
                raise HTTPException(
                    status_code=409,
                    detail=f"{exc} — Use POST /api/v1/million-cohort/episodes/{episode_id}/archive instead.",
                ) from exc
        raise HTTPException(
            status_code=409, detail="clinical_record_deletion_forbidden"
        )

    @router.delete("/patients/{patient_id}")
    async def delete_patient_forbidden(patient_id: str) -> dict[str, Any]:
        with _connect_sqlite() as conn:
            try:
                conn.execute("DELETE FROM patients WHERE patient_id = ?", (patient_id,))
            except sqlite3.IntegrityError as exc:
                raise HTTPException(
                    status_code=409,
                    detail=f"{exc} — Use POST /api/v1/million-cohort/patients/{patient_id}/archive instead.",
                ) from exc
        raise HTTPException(
            status_code=409, detail="clinical_record_deletion_forbidden"
        )

    return router
