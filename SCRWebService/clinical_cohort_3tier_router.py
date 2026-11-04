"""Standalone 3-Tier Clinical Cohort CRUD + Archive Router.

Enables each Symphonix clinical sibling service to operate independently via a
self-contained 3-tier architecture (Frontend UI <-> FastAPI Backend <-> Sibling
SQLite WAL / PostgreSQL Schema) with full Create, Read, Update, and Clinical
Archive (never physical delete) across the 1,002,250-patient / 4,409,900-episode
cohort, while supporting optional BulletTrain HIE (`client-registry`) sync when
cross-system integration is requested.
"""

from __future__ import annotations

import hashlib
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

PG_DSN = os.environ.get(
    "BULLETTRAIN_PG_DSN",
    "postgresql://bullettrain:devpassword@127.0.0.1:15432/bullettrain",
)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _generate_valid_modulus11_nhs(seed_str: str) -> str:
    """Generate a deterministic 10-digit NHS number passing strict Modulus-11 validation."""
    digest = int(hashlib.sha256(seed_str.encode("utf-8")).hexdigest()[:12], 16)
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
    givenName: str = Field(..., min_length=1, max_length=120)
    familyName: str = Field(..., min_length=1, max_length=120)
    dateOfBirth: str = Field(..., pattern=r"^\d{4}-\d{2}-\d{2}$")
    gender: str = Field(default="female", pattern=r"^(female|male|other|unknown)$")
    nationalID: str | None = Field(default=None)
    site_id: str = Field(default="SITE_ACUTE_NORTH", min_length=3)
    age_band: str = Field(default="50_74")
    deprivation_quintile: int = Field(default=3, ge=1, le=5)
    split_name: str = Field(default="sealed_holdout")
    hf_repo_id: str = Field(default="AGBonnet/augmented-clinical-notes")
    sync_bullettrain_hie: bool = Field(
        default=False,
        description="If False (default), executes standalone 3-tier local CRUD. If True, also registers/syncs canonical identity in BulletTrain Client Registry.",
    )


class PatientUpdate3TierRequest(BaseModel):
    givenName: str | None = Field(default=None, min_length=1, max_length=120)
    familyName: str | None = Field(default=None, min_length=1, max_length=120)
    dateOfBirth: str | None = Field(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$")
    gender: str | None = Field(default=None, pattern=r"^(female|male|other|unknown)$")
    site_id: str | None = Field(default=None, min_length=3)
    deprivation_quintile: int | None = Field(default=None, ge=1, le=5)
    sync_bullettrain_hie: bool = Field(default=False)


class EpisodeCreate3TierRequest(BaseModel):
    episode_sequence: int = Field(default=1, ge=1, le=5)
    site_id: str = Field(default="SITE_ACUTE_NORTH", min_length=3)
    split_name: str = Field(default="sealed_holdout")
    initial_map_mmhg: float = Field(default=75.0, ge=30.0, le=220.0)
    initial_hr_bpm: float = Field(default=80.0, ge=20.0, le=250.0)
    initial_lactate_mmol_l: float = Field(default=1.5, ge=0.2, le=25.0)
    initial_egfr: float = Field(default=76.0, ge=5.0, le=140.0)
    deterioration_risk_4h: float = Field(default=0.18, ge=0.0, le=1.0)


class EpisodeUpdate3TierRequest(BaseModel):
    initial_map_mmhg: float | None = Field(default=None, ge=30.0, le=220.0)
    initial_hr_bpm: float | None = Field(default=None, ge=20.0, le=250.0)
    initial_lactate_mmol_l: float | None = Field(default=None, ge=0.2, le=25.0)
    initial_egfr: float | None = Field(default=None, ge=5.0, le=140.0)
    deterioration_risk_4h: float | None = Field(default=None, ge=0.0, le=1.0)


class ClinicalArchiveRequest(BaseModel):
    archive_reason: str = Field(
        ...,
        min_length=6,
        max_length=500,
        description="Mandatory clinical/governance rationale for archiving the record (records are never physically deleted).",
    )


def build_clinical_cohort_3tier_router(
    service_name: str,
    sibling_slug: str,
    repo_root: Path,
) -> APIRouter:
    router = APIRouter(prefix="/api/v1/million-cohort", tags=["million-cohort-3tier-crud"])
    db_file = repo_root / "data" / f"{sibling_slug}_sh_kf_state_001_wal.db"
    pg_schema = f"sibling_{sibling_slug}"
    workspace_root = repo_root.parent
    cr_sqlite_path = workspace_root / "BulletTrain" / "data" / "client_registry_million_wal.db"

    def _connect_sqlite() -> sqlite3.Connection:
        if not db_file.exists():
            raise HTTPException(status_code=503, detail=f"3-tier SQLite database not found at {db_file}")
        conn = sqlite3.connect(str(db_file), timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        return conn

    async def _try_pg_connect():
        try:
            import asyncpg
            conn = await asyncpg.connect(PG_DSN, timeout=2.5)
            exists = await conn.fetchval("SELECT to_regclass($1)", f"{pg_schema}.patients")
            if not exists:
                await conn.close()
                return None
            return conn
        except Exception:
            return None

    @router.get("/summary")
    async def get_cohort_summary() -> dict[str, Any]:
        with _connect_sqlite() as conn:
            total_p = int(conn.execute("SELECT COUNT(*) FROM patients").fetchone()[0])
            active_p = int(conn.execute("SELECT COUNT(*) FROM patients WHERE status = 'active'").fetchone()[0])
            archived_p = int(conn.execute("SELECT COUNT(*) FROM patients WHERE status = 'archived'").fetchone()[0])
            local_p = int(conn.execute("SELECT COUNT(*) FROM patients WHERE record_authority = 'local'").fetchone()[0])
            proj_p = int(conn.execute("SELECT COUNT(*) FROM patients WHERE record_authority = 'projection'").fetchone()[0])
            synth_p = int(
                conn.execute(
                    "SELECT COUNT(*) FROM patients WHERE LOWER(hf_repo_id) LIKE '%synthetic%' OR LOWER(hf_repo_id) LIKE '%vrkomari%' OR LOWER(hf_repo_id) LIKE '%galileo%'"
                ).fetchone()[0]
            )
            total_ep = int(conn.execute("SELECT COUNT(*) FROM patient_episodes").fetchone()[0])
            active_ep = int(conn.execute("SELECT COUNT(*) FROM patient_episodes WHERE record_status = 'active'").fetchone()[0])
            archived_ep = int(conn.execute("SELECT COUNT(*) FROM patient_episodes WHERE record_status = 'archived'").fetchone()[0])
            high_risk_ep = int(
                conn.execute(
                    "SELECT COUNT(*) FROM patient_episodes WHERE episode_sequence >= 4 AND record_status = 'active'"
                ).fetchone()[0]
            )

        pg_conn = await _try_pg_connect()
        pg_status = "offline_standalone_sqlite_only"
        pg_patients = 0
        pg_episodes = 0
        if pg_conn is not None:
            try:
                pg_patients = int(await pg_conn.fetchval(f"SELECT COUNT(*) FROM {pg_schema}.patients"))
                pg_episodes = int(await pg_conn.fetchval(f"SELECT COUNT(*) FROM {pg_schema}.patient_episodes"))
                pg_status = "online_dual_engine_synced"
            finally:
                await pg_conn.close()

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
        if search:
            q = f"%{search.strip()}%"
            clauses.append("(patient_id LIKE ? OR nationalID LIKE ? OR givenName LIKE ? OR familyName LIKE ?)")
            params.extend([q, q, q, q])

        where_sql = " AND ".join(clauses)
        with _connect_sqlite() as conn:
            total = int(conn.execute(f"SELECT COUNT(*) FROM patients WHERE {where_sql}", params).fetchone()[0])
            rows = conn.execute(
                f"""
                SELECT patient_id, mpiId, givenName, familyName, dateOfBirth, gender,
                       nationalID, status, record_authority, source_of_truth, hf_repo_id,
                       site_id, age_band, deprivation_quintile, split_name,
                       archived_at, archive_reason, synced_at
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
        nhs10 = body.nationalID.strip() if body.nationalID else _generate_valid_modulus11_nhs(seed_key)
        if not validate_modulus11_nhs(nhs10):
            raise HTTPException(status_code=422, detail=f"Invalid 10-digit Modulus-11 nationalID: {nhs10}")

        patient_id = f"SUBJ-MPI-{nhs10}"
        mpi_id = f"MPI-{nhs10}"
        sha32 = hashlib.sha256(seed_key.encode("utf-8")).hexdigest()[:32]
        now_iso = _utc_now_iso()

        authority = "projection" if body.sync_bullettrain_hie else "local"
        source = "client-registry" if body.sync_bullettrain_hie else "local-3tier"
        fk_verified = 1 if body.sync_bullettrain_hie else 0

        # If BulletTrain HIE integration is explicitly requested, register upstream first
        if body.sync_bullettrain_hie and cr_sqlite_path.exists():
            with sqlite3.connect(str(cr_sqlite_path), timeout=30.0) as cr_conn:
                cr_conn.execute(
                    """
                    INSERT OR IGNORE INTO patients (
                        patient_id, mpiId, givenName, familyName, dateOfBirth, gender,
                        nationalID, national_id_blind_index, status, record_authority,
                        provenance_system, hf_repo_id, hf_record_sha256, site_id,
                        age_band, deprivation_quintile, assigned_sibling_repo, split_name,
                        encryption_version, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'active', 'canonical', 'huggingface-clinical-corpus', ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
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
                        client_registry_fk_verified, synced_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    ),
                )
                conn.commit()
            except sqlite3.IntegrityError as exc:
                raise HTTPException(status_code=409, detail=f"Patient already exists or constraint failed: {exc}") from exc

            created_row = dict(
                conn.execute("SELECT * FROM patients WHERE patient_id = ?", (patient_id,)).fetchone()
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
                            age_band, deprivation_quintile, assigned_sibling_repo, split_name, encryption_version
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'active', 'canonical', 'huggingface-clinical-corpus', $9, $10, $11, $12, $13, $14, $15, 1)
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
                    )
                await pg_conn.execute(
                    f"""
                    INSERT INTO {pg_schema}.patients (
                        patient_id, mpiId, givenName, familyName, dateOfBirth, gender,
                        nationalID, status, record_authority, source_of_truth, hf_repo_id,
                        hf_record_sha256, site_id, age_band, deprivation_quintile, split_name,
                        client_registry_fk_verified
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, 'active', $8, $9, $10, $11, $12, $13, $14, $15, $16)
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
                )
            finally:
                await pg_conn.close()

        return created_row

    @router.get("/patients/{patient_id}")
    async def get_patient_and_episodes(patient_id: str) -> dict[str, Any]:
        with _connect_sqlite() as conn:
            p_row = conn.execute("SELECT * FROM patients WHERE patient_id = ?", (patient_id,)).fetchone()
            if p_row is None:
                raise HTTPException(status_code=404, detail=f"Patient {patient_id} not found in {service_name}")
            ep_rows = conn.execute(
                "SELECT * FROM patient_episodes WHERE patient_id = ? ORDER BY episode_sequence",
                (patient_id,),
            ).fetchall()

        return {
            "service_name": service_name,
            "patient": dict(p_row),
            "episodes": [dict(r) for r in ep_rows],
        }

    @router.put("/patients/{patient_id}")
    async def update_patient_3tier(patient_id: str, body: PatientUpdate3TierRequest) -> dict[str, Any]:
        with _connect_sqlite() as conn:
            p_row = conn.execute("SELECT * FROM patients WHERE patient_id = ?", (patient_id,)).fetchone()
            if p_row is None:
                raise HTTPException(status_code=404, detail=f"Patient {patient_id} not found")
            if p_row["record_authority"] == "projection" and not body.sync_bullettrain_hie:
                raise HTTPException(
                    status_code=409,
                    detail="projection_only: patient is a BulletTrain Client Registry projection; pass sync_bullettrain_hie=true or update a standalone local-3tier patient",
                )

            updates: list[str] = []
            params: list[Any] = []
            for col in ("givenName", "familyName", "dateOfBirth", "gender", "site_id", "deprivation_quintile"):
                val = getattr(body, col)
                if val is not None:
                    updates.append(f"{col} = ?")
                    params.append(val.strip() if isinstance(val, str) else val)

            if updates:
                params.append(patient_id)
                conn.execute(f"UPDATE patients SET {', '.join(updates)} WHERE patient_id = ?", params)
                conn.commit()

            updated = dict(conn.execute("SELECT * FROM patients WHERE patient_id = ?", (patient_id,)).fetchone())

        pg_conn = await _try_pg_connect()
        if pg_conn is not None and updates:
            try:
                async with pg_conn.transaction():
                    if body.sync_bullettrain_hie:
                        await pg_conn.execute("SET LOCAL app.allow_projection_sync = 'true';")
                    await pg_conn.execute(
                        f"""
                        UPDATE {pg_schema}.patients
                        SET givenName = $1, familyName = $2, dateOfBirth = $3, gender = $4, site_id = $5, deprivation_quintile = $6
                        WHERE patient_id = $7
                        """,
                        updated["givenName"],
                        updated["familyName"],
                        updated["dateOfBirth"],
                        updated["gender"],
                        updated["site_id"],
                        updated["deprivation_quintile"],
                        patient_id,
                    )
            finally:
                await pg_conn.close()

        return updated

    @router.post("/patients/{patient_id}/episodes", status_code=201)
    async def create_episode_3tier(patient_id: str, body: EpisodeCreate3TierRequest) -> dict[str, Any]:
        now_iso = _utc_now_iso()
        sha32 = hashlib.sha256(f"{patient_id}:{body.episode_sequence}:{now_iso}".encode("utf-8")).hexdigest()[:32]
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
                        record_status, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, 14, ?, ?, ?, ?, ?, ?, 'active', ?)
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
                    ),
                )
                conn.commit()
            except sqlite3.IntegrityError as exc:
                raise HTTPException(
                    status_code=409,
                    detail=f"Foreign key or integrity constraint failed creating episode for {patient_id}: {exc}",
                ) from exc

            ep_row = dict(conn.execute("SELECT * FROM patient_episodes WHERE episode_id = ?", (episode_id,)).fetchone())

        pg_conn = await _try_pg_connect()
        if pg_conn is not None:
            try:
                await pg_conn.execute(
                    f"""
                    INSERT INTO {pg_schema}.patient_episodes (
                        episode_id, patient_id, sibling_service, episode_sequence,
                        split_name, site_id, trajectory_steps,
                        initial_map_mmhg, initial_hr_bpm, initial_lactate_mmol_l,
                        initial_egfr, deterioration_risk_4h, hf_provenance_sha256, record_status
                    ) VALUES ($1, $2, $3, $4, $5, $6, 14, $7, $8, $9, $10, $11, $12, 'active')
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
                )
            finally:
                await pg_conn.close()

        return ep_row

    @router.put("/episodes/{episode_id}")
    async def update_episode_3tier(episode_id: str, body: EpisodeUpdate3TierRequest) -> dict[str, Any]:
        with _connect_sqlite() as conn:
            ep_row = conn.execute("SELECT * FROM patient_episodes WHERE episode_id = ?", (episode_id,)).fetchone()
            if ep_row is None:
                raise HTTPException(status_code=404, detail=f"Episode {episode_id} not found")
            if ep_row["record_status"] == "archived":
                raise HTTPException(status_code=409, detail=f"Episode {episode_id} is archived and immutable")

            updates: list[str] = []
            params: list[Any] = []
            for col in (
                "initial_map_mmhg",
                "initial_hr_bpm",
                "initial_lactate_mmol_l",
                "initial_egfr",
                "deterioration_risk_4h",
            ):
                val = getattr(body, col)
                if val is not None:
                    updates.append(f"{col} = ?")
                    params.append(float(val))

            if updates:
                params.append(episode_id)
                conn.execute(f"UPDATE patient_episodes SET {', '.join(updates)} WHERE episode_id = ?", params)
                conn.commit()

            updated = dict(conn.execute("SELECT * FROM patient_episodes WHERE episode_id = ?", (episode_id,)).fetchone())

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
                        deterioration_risk_4h = $5
                    WHERE episode_id = $6
                    """,
                    updated["initial_map_mmhg"],
                    updated["initial_hr_bpm"],
                    updated["initial_lactate_mmol_l"],
                    updated["initial_egfr"],
                    updated["deterioration_risk_4h"],
                    episode_id,
                )
            finally:
                await pg_conn.close()

        return updated

    @router.post("/episodes/{episode_id}/archive")
    async def archive_episode_3tier(episode_id: str, body: ClinicalArchiveRequest) -> dict[str, Any]:
        now_iso = _utc_now_iso()
        reason = body.archive_reason.strip()
        with _connect_sqlite() as conn:
            ep_row = conn.execute("SELECT * FROM patient_episodes WHERE episode_id = ?", (episode_id,)).fetchone()
            if ep_row is None:
                raise HTTPException(status_code=404, detail=f"Episode {episode_id} not found")
            conn.execute(
                "UPDATE patient_episodes SET record_status = 'archived', archived_at = ?, archive_reason = ? WHERE episode_id = ?",
                (now_iso, reason, episode_id),
            )
            conn.commit()
            archived = dict(conn.execute("SELECT * FROM patient_episodes WHERE episode_id = ?", (episode_id,)).fetchone())

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
    async def archive_patient_3tier(patient_id: str, body: ClinicalArchiveRequest) -> dict[str, Any]:
        now_iso = _utc_now_iso()
        reason = body.archive_reason.strip()
        with _connect_sqlite() as conn:
            p_row = conn.execute("SELECT * FROM patients WHERE patient_id = ?", (patient_id,)).fetchone()
            if p_row is None:
                raise HTTPException(status_code=404, detail=f"Patient {patient_id} not found")
            conn.execute(
                "UPDATE patients SET status = 'archived', archived_at = ?, archive_reason = ? WHERE patient_id = ?",
                (now_iso, reason, patient_id),
            )
            conn.commit()
            archived = dict(conn.execute("SELECT * FROM patients WHERE patient_id = ?", (patient_id,)).fetchone())

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
                conn.execute("DELETE FROM patient_episodes WHERE episode_id = ?", (episode_id,))
            except sqlite3.IntegrityError as exc:
                raise HTTPException(
                    status_code=409,
                    detail=f"{exc} — Use POST /api/v1/million-cohort/episodes/{episode_id}/archive instead.",
                ) from exc
        raise HTTPException(status_code=409, detail="clinical_record_deletion_forbidden")

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
        raise HTTPException(status_code=409, detail="clinical_record_deletion_forbidden")

    return router
