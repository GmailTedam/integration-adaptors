"""Drive BulletTrain's live OIDC runtime proof for this repository.

Vendored from BulletTrain/scripts/security/oidc_runtime_proof_test.py.tmpl. Do not
edit it here: change the template and re-vendor. The drift check is
workspace-tooling/check_vendored.py (artefact ``oidc-runtime-proof-test``), run by
``python workspace-tooling/estate_doctor.py --vendored``.

A sibling that needs more than this (an interpreter that can boot BulletTrain, a
repository id that differs from its folder) keeps its own code OUTSIDE the delimited
locator block below; the drift check compares only that block for such a copy.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


# --- BEGIN oidc proof locator (canonical) ---
def _main_checkout() -> Path | None:
    """The MAIN checkout of this repository, from any linked worktree.

    ``REPO_ROOT.parent`` is the workspace only in the main checkout; from
    ``C:/w/<short>`` it is a folder of other worktrees, where ``BulletTrain`` is
    a different (or absent) directory and the folder name is not the repository's.
    git's common directory is the MAIN checkout's ``.git`` from every worktree.
    """
    try:
        local = subprocess.run(
            ["git", "rev-parse", "--local-env-vars"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        ).stdout.split()
        env = {key: value for key, value in os.environ.items() if key not in local}
        answer = subprocess.run(
            ["git", "rev-parse", "--path-format=absolute", "--git-common-dir"],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
            env=env,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    common = (
        Path(answer.stdout.strip())
        if answer.returncode == 0 and answer.stdout.strip()
        else None
    )
    if common is None or common.resolve().name != ".git":
        return None
    return common.resolve().parent


def _bullettrain_root() -> Path:
    """The BulletTrain checkout that holds the proof script.

    An explicit BULLETTRAIN_ROOT wins, then the operator's workspace variables, then
    the workspace derived from git, then the folder beside this checkout. A candidate
    counts only if it really holds the proof script.
    """
    proof = Path("scripts") / "security" / "oidc_sibling_runtime_proof.py"
    candidates: list[Path] = []
    explicit = (os.environ.get("BULLETTRAIN_ROOT") or "").strip()
    if explicit:
        candidates.append(Path(explicit))
    for name in ("SYMPHONIX_WORKSPACE_ROOT", "CAID_WORKSPACE"):
        value = (os.environ.get(name) or "").strip()
        if value:
            candidates.append(Path(value) / "BulletTrain")
    main = _main_checkout()
    if main is not None:
        candidates.append(main.parent / "BulletTrain")
    candidates.append(REPO_ROOT.parent / "BulletTrain")
    for candidate in candidates:
        if (candidate / proof).is_file():
            return candidate
    return REPO_ROOT.parent / "BulletTrain"


def _repo_name() -> str:
    """The repository's own name: its main checkout's folder, not this worktree's."""
    main = _main_checkout()
    return (main or REPO_ROOT).name


# --- END oidc proof locator ---

BULLETTRAIN_ROOT = _bullettrain_root()
REPO_NAME = _repo_name()


# Budget arithmetic, kept explicit so this wrapper cannot silently disagree with
# the script it drives. It previously allowed 270s against that script's own
# 240s startup budget, leaving 30s for the proof itself -- so a slow-but-healthy
# startup made the WRAPPER kill the run and surface `subprocess.TimeoutExpired`
# instead of the script's own diagnosis.
#
# The request budget was 10s (the script's old default) and produced a
# load-sensitive FALSE FAILURE: a full repo-root pytest run reported
# "- timed out" while the same test passed in isolation seconds later. A proof
# request drives an OIDC validation that fetches JWKS over HTTP, a policy
# decision, and a best-effort audit emit; on a box running many concurrent
# agent sessions those inflate past 10s. Measured healthy cost is ~1.6s, so 45s
# is roughly a 28x margin -- generous enough to absorb contention, nowhere near
# generous enough to let a wedged gateway pass.
#
# Retries apply to TRANSPORT failures only. A proof the gateway actually
# answers -- any non-200, a wrong subject, a missing scope -- fails on the
# first attempt and is never retried.
STARTUP_TIMEOUT_SECONDS = 240
REQUEST_TIMEOUT_SECONDS = 45
REQUEST_ATTEMPTS = 2
SUBPROCESS_TIMEOUT_SECONDS = (
    STARTUP_TIMEOUT_SECONDS + REQUEST_TIMEOUT_SECONDS * REQUEST_ATTEMPTS + 60
)


def test_repo_has_live_bullettrain_oidc_runtime_proof(tmp_path: Path) -> None:
    report_path = tmp_path / f"oidc_runtime_proof_{REPO_NAME}.json"
    result = subprocess.run(
        [
            sys.executable,
            str(
                BULLETTRAIN_ROOT
                / "scripts"
                / "security"
                / "oidc_sibling_runtime_proof.py"
            ),
            "--repo",
            REPO_NAME,
            "--startup-timeout",
            str(STARTUP_TIMEOUT_SECONDS),
            "--request-timeout",
            str(REQUEST_TIMEOUT_SECONDS),
            "--request-attempts",
            str(REQUEST_ATTEMPTS),
            "--json-out",
            str(report_path),
        ],
        cwd=BULLETTRAIN_ROOT,
        capture_output=True,
        text=True,
        timeout=SUBPROCESS_TIMEOUT_SECONDS,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "pass"
    assert report["proof_count"] == report["expected_count"] == 1
    assert report["proofs"][0]["repo"] == REPO_NAME
    assert report["proofs"][0]["status"] == "pass"
