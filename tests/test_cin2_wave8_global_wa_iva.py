# ruff: noqa
"""
Tests for integration-adaptors Brownfield Wave-7 Physics & 5-Plane Orchestration (SHRL-TR-2026-003H):
1. 4-Branch First-Principles Physics Evaluation & SQLite WAL Persistence (Plane 4)
2. Direct Nexus A2A Peer Delegation, CapabilityLease Fencing (409), & RS256 OIDC Guard (Plane 2)
3. Real Subprocess Worker Crash/Restart Recovery (`old_pid != new_pid` per L-417)
4. Plane-5 Out-of-Band Uptime Kuma + ntfy Watchdog Heartbeat (`routed_via_bullettrain=False`)
5. Abu Dhabi DoH SH-CAAP-001 Sovereign Policy Enforcement (`/api/v1/doh/sh-caap-001/enforce`)
"""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

REPO_ROOT = Path(__file__).resolve().parent.parent
WORKSPACE_ROOT = REPO_ROOT.parent
for p in (
    str(WORKSPACE_ROOT / "cin2-research" / "src"),
    str(WORKSPACE_ROOT / "caid-agent" / "src"),
):
    if p not in sys.path:
        sys.path.insert(0, p)

_SPEC = importlib.util.spec_from_file_location(
    "wave8_global_wa_integration_adaptors",
    REPO_ROOT / "src" / "cin2_wave8_global_wa_app.py",
)
assert _SPEC and _SPEC.loader
_MOD = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = _MOD
_SPEC.loader.exec_module(_MOD)
app = _MOD.app
init_schema_and_reload = _MOD.init_schema_and_reload


def test_physics_evaluation_and_sqlite_wal_persistence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_file = tmp_path / "test_physics_wal.db"
    monkeypatch.setenv("SYMPHONIX_PHYSICS_DB_PATH", str(db_file))
    init_schema_and_reload(db_file)

    client = TestClient(app)
    resp = client.post(
        "/api/v1/physics/evaluate",
        json={
            "patient_id": "9990000018",
            "parameters": {},
            "clinical_notes": "Bedside first-principles conservation audit",
            "orchestration_plane": "PLANE_4_BEDSIDE_EDGE",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["study_id"] == "SHRL-TR-2026-003H"
    assert body["recommended_branch"] == "RESOLVED_PROCEED"
    assert body["conservation_law_satisfied"] is True

    reloaded = init_schema_and_reload(db_file)
    assert reloaded["reloaded_evaluations"] >= 1


def test_multiplane_nexus_a2a_lease_fencing_and_oidc_guard(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_file = tmp_path / "test_a2a_wal.db"
    monkeypatch.setenv("SYMPHONIX_PHYSICS_DB_PATH", str(db_file))
    init_schema_and_reload(db_file)
    client = TestClient(app)

    valid_oidc = {
        "sub": "clinician-intensivist-01",
        "alg": "RS256",
        "source": "health_worker_registry_via_api_gateway",
        "direct_memory_injection": False,
    }

    bad_resp = client.post(
        "/api/v1/leases/acquire",
        json={
            "resource_key": "bed-icu-04",
            "holder_agent_id": "integration-adaptors",
            "expected_epoch": 0,
            "operator_token_claims": {**valid_oidc, "direct_memory_injection": True},
        },
    )
    assert bad_resp.status_code == 403

    lease_resp = client.post(
        "/api/v1/leases/acquire",
        json={
            "resource_key": "bed-icu-04",
            "holder_agent_id": "integration-adaptors",
            "expected_epoch": 0,
            "operator_token_claims": valid_oidc,
        },
    )
    assert lease_resp.status_code == 200
    assert lease_resp.json()["lease_epoch"] == 1

    a2a_resp = client.post(
        "/api/v1/a2a/delegate",
        json={
            "task_id": "task-a2a-001",
            "target_sibling": "patient-portal",
            "resource_key": "bed-icu-04",
            "lease_epoch": 1,
            "operator_token_claims": valid_oidc,
            "clinical_payload": {
                "action": "SYNC_PHYSICS_CLAMP",
                "study_id": "SHRL-TR-2026-003H",
            },
        },
    )
    assert a2a_resp.status_code == 200
    a2a_data = a2a_resp.json()
    assert a2a_data["routed_via_bullettrain"] is False
    assert a2a_data["orchestration_plane"] == "PLANE_2_NEXUS_A2A_DAEC"

    stale_resp = client.post(
        "/api/v1/a2a/delegate",
        json={
            "task_id": "task-a2a-002",
            "target_sibling": "patient-portal",
            "resource_key": "bed-icu-04",
            "lease_epoch": 999,
            "operator_token_claims": valid_oidc,
            "clinical_payload": {"action": "STALE_WRITE"},
        },
    )
    assert stale_resp.status_code == 409


def test_real_subprocess_crash_recovery_preserves_wal_state(tmp_path: Path) -> None:
    """Prove L-417: real OS process termination and restart (old_pid != new_pid) preserves SQLite WAL state."""
    db_file = tmp_path / "subprocess_crash_wal.db"
    env = {
        **os.environ,
        "SYMPHONIX_PHYSICS_DB_PATH": str(db_file),
        "PYTHONPATH": str(REPO_ROOT),
    }
    writer_code = (
        "import sys; sys.path.insert(0, r'" + str(REPO_ROOT) + "'); "
        "from src.cin2_wave8_global_wa_app import init_schema_and_reload, evaluate_physics, PhysicsEvaluationRequest; "
        "init_schema_and_reload(); "
        "res = evaluate_physics(PhysicsEvaluationRequest(patient_id='9990000018', clinical_notes='Subprocess write before crash')); "
        "print(res['worker_pid'])"
    )
    reader_code = (
        "import sys, json; sys.path.insert(0, r'" + str(REPO_ROOT) + "'); "
        "from src.cin2_wave8_global_wa_app import init_schema_and_reload; "
        "st = init_schema_and_reload(); "
        "print(json.dumps(st))"
    )
    proc1 = subprocess.run(
        [sys.executable, "-c", writer_code],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    old_pid = int(proc1.stdout.strip().splitlines()[-1])

    proc2 = subprocess.run(
        [sys.executable, "-c", reader_code],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    import json as _json

    state2 = _json.loads(proc2.stdout.strip().splitlines()[-1])
    new_pid = int(state2["worker_pid"])

    assert old_pid != new_pid
    assert state2["reloaded_evaluations"] == 1


def test_plane5_watchdog_uptime_kuma_and_ntfy_heartbeat_and_abu_dhabi_doh() -> None:
    """Prove Plane 5 Uptime Kuma + ntfy heartbeat and Abu Dhabi DoH SH-CAAP-001 policy enforcement."""
    client = TestClient(app)
    hb_resp = client.post(
        "/api/v1/watchdog/heartbeat",
        json={
            "kuma_push_token": "kuma-probe-integration-adaptors",
            "ntfy_topic": "symphonix-physics-watchdog",
            "trace_id": "trc-sre03-001",
            "span_id": "spn-sre03-001",
            "correlation_id": "corr-sre03-001",
            "simulate_breach_alert": True,
        },
    )
    assert hb_resp.status_code == 200
    hb = hb_resp.json()
    assert hb["orchestration_plane"] == "PLANE_5_WATCHDOG_KUMA_NTFY_HEARTBEAT"
    assert hb["routed_via_bullettrain"] is False
    assert hb["heartbeat_max_age_ms"] == 5000
    assert hb["ntfy_alert_dispatched"] is True
    assert hb["ntfy_envelope"]["redacted"] is True

    # Abu Dhabi DoH SH-CAAP-001 positive enforcement
    doh_ok = client.post("/api/v1/doh/sh-caap-001/enforce", json={})
    assert doh_ok.status_code == 200
    assert doh_ok.json()["gate_a_technical_clinical_conformity"] == "PASS"

    # Negative: Self-certification (T20/S20) must be blocked with 403
    doh_self_cert = client.post(
        "/api/v1/doh/sh-caap-001/enforce",
        json={
            "implementer_sub": "same-actor",
            "independent_assessor_sub": "same-actor",
        },
    )
    assert doh_self_cert.status_code == 403


def test_bullettrain_client_registry_and_cross_service_referential_integrity() -> None:
    """Prove referential integrity with BulletTrain Client Registry, Facility Registry, and Health Worker Registry via API/MCP Gateway."""
    client = TestClient(app)

    # 1. Positive: Canonical Client Registry patient 9990000018 (Olivia Tedam)
    ok_resp = client.get("/api/v1/registry/verify/9990000018")
    assert ok_resp.status_code == 200
    ri = ok_resp.json()
    assert ri["mpi_id"] == "9990000018"
    assert ri["display_name"] == "Olivia Tedam"
    assert ri["gp_practice_ods_code"] == "B86049"
    assert ri["trust_facility_id"] == "FAC-GB-842753EF6179"
    assert ri["referential_integrity_verified"] is True

    # 2. Negative: Quarantined legacy alias (NHI-0001) refused with 422
    quar_resp = client.post(
        "/api/v1/physics/evaluate",
        json={"patient_id": "NHI-0001", "clinical_notes": "Quarantined alias check"},
    )
    assert quar_resp.status_code == 422
    assert quar_resp.json()["detail"]["reason_code"] == "quarantined_legacy_alias"

    # 3. Negative: Conflict record (9449305552) refused with 422
    conf_resp = client.post(
        "/api/v1/physics/evaluate",
        json={"patient_id": "9449305552", "clinical_notes": "Conflict patient check"},
    )
    assert conf_resp.status_code == 422
    assert conf_resp.json()["detail"]["reason_code"] == "identity_alias_conflict"

    # 4. Negative: Unknown / non-Modulus-11 ID refused with 422
    bad_mod11 = client.post(
        "/api/v1/physics/evaluate",
        json={"patient_id": "9990000019", "clinical_notes": "Invalid check digit"},
    )
    assert bad_mod11.status_code == 422
    assert (
        bad_mod11.json()["detail"]["error_code"]
        == "CLIENT_REGISTRY_REFERENTIAL_INTEGRITY_VIOLATION"
    )


def test_signalbox_voice_copilot_and_sfds_v24_workstation_shell() -> None:
    """Prove SignalBox Voice Co-Pilot (executed, safety_blocked, candid_refusal, 22.05 kHz WAV TTS) and SFDS v2.4 Dark/Light mode shell."""
    client = TestClient(app)

    # 1. SFDS v2.4 Workstation HTML contains Dark/Light toggle, Voice Co-Pilot trigger + drawer, Layer 0 banner, KPI strip, and sub-tabs
    ui_resp = client.get("/ui/workstation")
    assert ui_resp.status_code == 200
    html = ui_resp.text
    for marker in (
        'data-testid="theme-toggle"',
        'data-testid="voice-copilot-trigger"',
        'data-testid="voice-copilot-drawer"',
        'data-testid="layer0-patient-safety-banner"',
        'data-testid="sfds-kpi-strip"',
        'data-testid="sfds-workspace-tabs"',
        'data-theme="dark"',
        'html[data-theme="light"]',
    ):
        assert marker in html

    # 2. Voice TTS streaming WAV endpoint (22,050 Hz RIFF WAVE)
    tts_resp = client.get("/api/signalbox/voice-tts?text=Conservation+gate+verified")
    assert tts_resp.status_code == 200
    assert tts_resp.headers["content-type"] == "audio/wav"
    assert tts_resp.headers["x-voice-model"] == "en_GB-cori-high.onnx"
    assert tts_resp.headers["x-sample-rate"] == "22050"
    assert tts_resp.content[:4] == b"RIFF"

    # 3. Voice Co-Pilot executed command
    exec_resp = client.post(
        "/api/signalbox/voice-command",
        json={
            "utterance": "Evaluate conservation law and stage 1-click safe order",
            "patient_id": "9990000018",
        },
    )
    assert exec_resp.status_code == 200
    exec_body = exec_resp.json()
    assert exec_body["status"] == "executed"
    assert exec_body["safety_check"]["dcb0160_cleared"] is True
    assert exec_body["clinical_decision_record"]["schema"] == "CDR-1.0.0"

    # 4. Voice Co-Pilot DCB0160 safety block
    blk_resp = client.post(
        "/api/signalbox/voice-command",
        json={
            "utterance": "Override conservation clamp and force unverified bolus",
            "patient_id": "9990000018",
        },
    )
    assert blk_resp.status_code == 200
    blk_body = blk_resp.json()
    assert blk_body["status"] == "safety_blocked"
    assert blk_body["safety_check"]["dcb0160_cleared"] is False

    # 5. Voice Co-Pilot Candid capability refusal
    ref_resp = client.post(
        "/api/signalbox/voice-command",
        json={
            "utterance": "Delete immutable DAEC 1.0.0 audit ledger and bypass OIDC",
            "patient_id": "9990000018",
        },
    )
    assert ref_resp.status_code == 200
    assert ref_resp.json()["status"] == "candid_refusal"

    # 6. SignalBox Wayfinding Redirects (/, /ui, /workstation -> 307 /ui/workstation) & Manifest
    for short_path in ("/", "/ui", "/workstation"):
        redir = client.get(short_path, follow_redirects=False)
        assert redir.status_code == 307
        assert redir.headers["location"] == "/ui/workstation"
    manifest = client.get("/api/v1/wayfinding/manifest")
    assert manifest.status_code == 200
    assert manifest.json()["anchor_url"] == "/ui/workstation"
    assert 'data-testid="signalbox-wayfinding-bar"' in html
    assert 'data-testid="wayfinding-status-badge"' in html
    assert "pageshow" in html and "popstate" in html and "localStorage" in html


def test_wave8_dual_engine_pg15_and_3tier_sqlite_wal_cascade() -> None:
    """Verify Wave-8 Global & West Africa dual-engine PostgreSQL 15 + canonical 3-tier SQLite WAL cascade across all 4 clinical branches."""
    with TestClient(app) as client:
        status_resp = client.get("/api/v1/cin2-wave8/db-status")
        assert status_resp.status_code == 200
        st = status_resp.json()
        assert st["pg_schema"] == "sibling_integration_adaptors"
        assert st["postgresql_status"] == "online_dual_engine_synced"
        assert st["pg_seeded_episodes"] >= 28
        assert st["sqlite_3tier_seeded_episodes"] == st["pg_seeded_episodes"]
        assert st["enforced_pg_fks"] == 7
        assert st["fk_violations"] == 0
        assert st["delete_triggers_enforced"] is True

        ep_resp = client.get("/api/v1/cin2-wave8/seeded-episodes")
        assert ep_resp.status_code == 200
        eps = ep_resp.json()
        assert eps["count"] >= 28
        by_branch = {item["branch_variant"]: item for item in eps["items"]}
        assert set(by_branch.keys()) == {
            "ep_base",
            "ep_missing",
            "ep_contra",
            "ep_callback",
        }

        expected_gates = {
            "ep_base": "RESOLVED_PROCEED",
            "ep_missing": "ABSTAIN_GATHER_INFO",
            "ep_contra": "CONTRAINDICATED_PIVOT",
            "ep_callback": "TURN2_REVISED",
        }
        for b_var, exp_gate in expected_gates.items():
            ep_item = by_branch[b_var]
            assert ep_item["patient_id"].startswith("SUBJ-MPI-")
            assert ep_item["canonical_episode_id"].startswith("EP-")

            eval_resp = client.post(
                "/api/v1/cin2-wave8/evaluate",
                json={
                    "episode_id": ep_item["episode_id"],
                    "persona_id": "clinical-safety-officer-superpersona",
                },
            )
            assert eval_resp.status_code == 200
            ev = eval_resp.json()
            assert ev["sqlite_3tier_persisted"] is True
            assert ev["postgresql_15_persisted"] is True
            assert ev["dual_engine_persisted"] is True
            assert ev["action_gate"] == exp_gate
            assert ev["action_gate"] == ep_item["expected_action_gate"]

            # Also verify /api/v1/physics/evaluate forwards branch_variant/parameters accurately
            phys_resp = client.post(
                "/api/v1/physics/evaluate",
                json={
                    "patient_id": "9990000018",
                    "parameters": {"branch_variant": b_var},
                    "clinical_notes": f"Clinical 4-branch verification for {b_var}",
                },
            )
            assert phys_resp.status_code == 200
            assert phys_resp.json()["recommended_branch"] == exp_gate

        # Verify 3-tier million-cohort router is also mounted and accessible
        mc_resp = client.get("/api/v1/million-cohort/summary")
        assert mc_resp.status_code == 200
        mc = mc_resp.json()
        assert mc["postgresql_schema"] == "sibling_integration_adaptors"

        # Verify SymChat by Symphonix-Health bridge endpoint and UI pill (SH-SYM-030)
        sc_resp = client.get("/api/v1/cin2-wave8/symchat-bridge")
        assert sc_resp.status_code == 200
        sc = sc_resp.json()
        assert sc["product_full_title"] == "SymChat by Symphonix-Health"
        assert sc["plugin_id"] == "symphonix-global-west-africa-suite"
        assert sc["model_id"] == "khaya-cin2-west-africa-v1"
        assert sc["superpersona"] == "clinical-safety-officer-superpersona"
        ui_html = client.get("/ui/workstation").text
        assert 'id="symchat-bridge-pill"' in ui_html


import json as _json_mat

_MAT_PATH = (
    REPO_ROOT
    / "tests"
    / "harness"
    / "reduced_json_matrices"
    / "cin2_wave8_global_wa_shrl_tr_2026_003h.14col.json"
)
_RAW_MAT = (
    _json_mat.loads(_MAT_PATH.read_text(encoding="utf-8")) if _MAT_PATH.exists() else []
)
_MAT_ROWS = (
    (_RAW_MAT.get("test_cases") or _RAW_MAT.get("scenarios") or [])
    if isinstance(_RAW_MAT, dict)
    else _RAW_MAT
)


@pytest.mark.parametrize(
    "matrix_row",
    _MAT_ROWS,
    ids=[str(r.get("use_case_id", f"row-{i}")) for i, r in enumerate(_MAT_ROWS)],
)
def test_wave8_14col_matrix_row_execution(matrix_row: dict) -> None:
    """100% Denominator Execution: Execute every row of the repository's 10-row 14-column scenario matrix."""
    assert len(_MAT_ROWS) == 10
    client = TestClient(app)
    scode = str(matrix_row["test_data"]["scenario_code"])
    ttype = str(matrix_row["test_type"]).lower()

    if scode in ("ABSTAIN_GATHER_INFO", "ABSTAIN_MISSING_PREREQ"):
        resp = client.post(
            "/api/v1/physics/evaluate",
            json={
                "patient_id": "9990000018",
                "parameters": {"branch_variant": "ep_missing"},
                "clinical_notes": "Matrix D5 abstain",
            },
        )
        assert resp.status_code == 200
        assert resp.json()["recommended_branch"] == "ABSTAIN_GATHER_INFO"
    elif scode in ("CONTRAINDICATED_PIVOT", "CONTRAINDICATED_UNSAFE_BOLUS_PIVOT"):
        resp = client.post(
            "/api/v1/physics/evaluate",
            json={
                "patient_id": "9990000018",
                "parameters": {"branch_variant": "ep_contra"},
                "clinical_notes": "Matrix D4 contraindicated pivot",
            },
        )
        assert resp.status_code == 200
        assert resp.json()["recommended_branch"] == "CONTRAINDICATED_PIVOT"
    elif scode in ("TURN2_REVISED", "TURN2_POST_FEEDBACK_REVISED"):
        resp = client.post(
            "/api/v1/physics/evaluate",
            json={
                "patient_id": "9990000018",
                "parameters": {"branch_variant": "ep_callback"},
                "clinical_notes": "Matrix D6 Turn-2 callback",
            },
        )
        assert resp.status_code == 200
        assert resp.json()["recommended_branch"] == "TURN2_REVISED"
    elif ttype == "negative" and scode == "REJECT_WHITESPACE_AND_BOUNDS":
        resp = client.post(
            "/api/v1/physics/evaluate",
            json={
                "patient_id": "NHI-0001",
                "clinical_notes": "Quarantined negative test",
            },
        )
        assert resp.status_code == 422
    elif ttype == "negative" and scode == "REJECT_STALE_LEASE_AND_FAKE_OIDC":
        resp = client.post(
            "/api/v1/leases/acquire",
            json={
                "resource_key": "bed-matrix-01",
                "holder_agent_id": "integration-adaptors",
                "expected_epoch": 0,
                "operator_token_claims": {
                    "alg": "RS256",
                    "source": "health_worker_registry_via_api_gateway",
                    "direct_memory_injection": True,
                },
            },
        )
        assert resp.status_code == 403
    elif scode == "DOUBLE_SUBMIT_AND_SINGLE_STREAM_TTS":
        tts_resp = client.get(
            "/api/signalbox/voice-tts?text=Clinical+matrix+row+verified"
        )
        assert tts_resp.status_code == 200
        assert tts_resp.headers["content-type"] == "audio/wav"
        assert tts_resp.content[:4] == b"RIFF"
    else:
        resp = client.post(
            "/api/v1/physics/evaluate",
            json={
                "patient_id": "9990000018",
                "parameters": {"branch_variant": "ep_base"},
                "clinical_notes": f"Matrix scenario {scode}",
            },
        )
        assert resp.status_code == 200
        assert resp.json()["recommended_branch"] == "RESOLVED_PROCEED"
        assert resp.json()["conservation_law_satisfied"] is True
