<!-- symphonix-health-ecosystem:start -->
## Symphonix Health ecosystem

<p>
  <a href="https://github.com/symphonix-health"><img alt="Symphonix Health" src="https://img.shields.io/badge/Symphonix_Health-clinical_or_operational_sibling-4DD0E1?style=flat-square" /></a>
  <a href="https://github.com/symphonix-health/symphonix-health.github.io/blob/main/docs/system-catalogue.md"><img alt="System catalogue" src="https://img.shields.io/badge/catalogue-live_index-14919B?style=flat-square" /></a>
</p>

| Field | Value |
|---|---|
| Organisation | `Symphonix-Health` |
| Ecosystem layer | Clinical & Operational Sibling |
| Repository role | National Integration Adaptors (`integration-adaptors`, `sibling_slug="integration_adaptors"`) — MHS, NHAIS, 111, and Summary Care Record (SCR) adaptor runtime with Dual-Engine PostgreSQL 15 + 3-tier SQLite WAL persistence and SFDS v2.4 `/workbench-ui` workstation. |
| Catalogue | [Symphonix system catalogue](https://github.com/symphonix-health/symphonix-health.github.io/blob/main/docs/system-catalogue.md) |
<!-- symphonix-health-ecosystem:end -->

# National Integration Adaptors (`integration-adaptors`)

This repository provides the **Symphonix-Health National Integration Adaptors** (`integration-adaptors`) for NHS Spine Message Handling Service (`mhs`), Summary Care Record (`SCR` / `SCRWebService`), `nhais-adaptor`, and `OneOneOne` clinical messaging workflows, alongside the FastAPI 3-tier clinical cohort runtime (`src/app.py` and `SCRWebService/main.py`).

These adaptors hide the protocol complexity of legacy Spine ebXML/HL7v3, EDIFACT, and ITK interfaces behind a standardised REST/FHIR adaptor surface routed through the [BulletTrain](../BulletTrain/README.md) HIE hub.

```mermaid
flowchart LR
    Supplier["Clinical Sibling / Supplier System"] -->|"Standardised REST / FHIR"| Adaptor["Integration Adaptors (src/app.py & SCRWebService)"]
    Adaptor -->|"Spine MHS / ebXML / HL7v3 / ITK"| Spine["BulletTrain HIE / NHS Spine Interfaces"]
    Adaptor -->|"Dual-Engine Sync"| PG["PostgreSQL 15 (sibling_integration_adaptors)"]
    Adaptor -->|"3-Tier Local WAL"| WAL["SQLite WAL (integration_adaptors_sh_kf_state_001_wal.db)"]
```

## Repository Contents

- [`src/`](src/) — FastAPI clinical cohort service (`src/app.py`) and 3-tier router (`src/clinical_cohort_3tier_router.py`) serving `/workbench-ui` and `/api/v1/cohort/3tier/*`.
- [`common/`](common/) — Shared Python utilities for message handling, retries, and logging across adaptors.
- [`mhs/`](mhs/) — Message Handling Service (MHS) adaptor encapsulating Spine ebXML/HL7v3 messaging.
- [`nhais-adaptor/`](nhais-adaptor/) — NHAIS EDIFACT-to-FHIR registration adaptor.
- [`OneOneOne/`](OneOneOne/) — NHS 111 ITK receiver and testbench integration.
- [`pipeline/`](pipeline/) — Container build and deployment pipeline scripts.
- [`SCR/`](SCR/) — HL7 GP Summary Upload request message builder package.
- [`SCRWebService/`](SCRWebService/) — Summary Care Record web service (`SCRWebService/main.py`) with mounted 3-tier clinical cohort router.
- [`supplier-example/`](supplier-example/) — Reference supplier client demonstrating GP Summary Update requests via the SCR and MHS adaptors.

<!-- symphonix-dual-engine-architecture:start -->
## Dual-Engine Persistence (`PostgreSQL 15` + 3-Tier `SQLite WAL`) & SFDS v2.4 `/workbench-ui`

In compliance with Symphonix-Health mandate **L-431**, `integration-adaptors` operates a synchronous **Dual-Engine** persistence architecture:

1. **PostgreSQL 15 (`bt-postgres-dev`):**
   - DSN: `postgresql://bullettrain:devpassword@127.0.0.1:15432/bullettrain`
   - Schema: `sibling_integration_adaptors` synchronized with master `client_registry.patients` (`40,090` canonical patients) and `referential_integrity.entity_references` (`176,396` canonical episodes), enforcing `FOREIGN KEY ... ON DELETE RESTRICT` and `BEFORE DELETE` WORM protection triggers (`client_registry.forbid_clinical_record_delete()`).
2. **3-Tier SQLite WAL (`SH-KF-STATE-001`):**
   - Database file: `data/integration_adaptors_sh_kf_state_001_wal.db`
   - Tiered clinical cohorts: Tier 1 (`40,000` longitudinal baseline), Tier 2 (`50` rich full-pathway), Tier 3 (`40` edge/adversarial).
3. **Runtime Endpoints & SFDS v2.4 Workstation:**
   - `GET /workbench-ui` — Full-viewport (`100vh`, `scrollHeight <= innerHeight`, `width: 100%; max-width: none;`) 4-tab WAI-ARIA (`role="tablist"` / `role="tab"` / `role="tabpanel"`) clinical workstation.
   - `GET /api/v1/cohort/3tier/summary` — Dual-engine sync status and cohort counts (`postgresql_15` + `sqlite_3tier_wal`).
   - `GET /api/v1/cohort/3tier/patients` & `GET /api/v1/cohort/3tier/patients/{patient_id}` — Patient trajectory and episode queries.
   - `POST /api/v1/cohort/3tier/episodes` — Synchronous dual-engine WORM episode creation across PostgreSQL 15 and SQLite WAL.
<!-- symphonix-dual-engine-architecture:end -->

## Running Local MHS and SCR Adaptors

Refer to [running the MHS and SCR adaptors locally](mhs/running-mhs-adaptor-locally.md) for step-by-step instructions on configuring local MHS and SCR instances, or launch the FastAPI workstation directly:

```powershell
uvicorn src.app:app --host 127.0.0.1 --port 8000
```

## ChartRecall (`Memorilla-v2`) 3-Tier Clinical Decision Support Integration (`clinical_cohort_3tier_router.py`)

- **Clinician-Facing Service & Icon**: **`ChartRecall`** (`clinical_service_name="ChartRecall"`, `clinical_icon="stethoscope-synapse-spark"`, `engine="MemorillaV2Module"`, `operating_mode="M2_HYBRID_MEMORILLA_GOVERNED"`).
- **3-Tier Cohort & On-Demand Agentic CDS Endpoints**:
  - `GET /api/v1/million-cohort/patients/{patient_id}/clinical-decision-context`: Returns `chartrecall_live_cds` with bounded-rationality (`<= 3` cards) Tier-0 `D4_ALLERGY_RENAL_PGX`, `D1_D2_D5_DIAGNOSTIC_PREREQ`, and `D3_D6_LONGITUDINAL_PATHWAY` cards synthesized from `integration-adaptors` dual-engine (`PostgreSQL 15` + `3-Tier SQLite WAL`) patient and episode records.
  - `POST /api/v1/million-cohort/patients/{patient_id}/chartrecall-evaluate`: Evaluates live consultation utterances or specialist prescribing/diagnostic/pathway queries against the patient's 3-tier record with two-state calibrated abstention (`CONVERSATIONAL_QUIET` vs. `PROTECTIVE_SAFETY_HOLD`) and `HCI-STE001` dual-layer explanations.
