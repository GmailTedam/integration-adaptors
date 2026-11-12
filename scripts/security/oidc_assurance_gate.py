"""Run BulletTrain's shared repo-level OIDC assurance gate for this repository.

Vendored from BulletTrain/scripts/security/oidc_gate_wrapper.py.tmpl. Do not
edit it here: change the template and re-vendor. The drift check is
BulletTrain/scripts/security/check_vendored_oidc_gate_wrappers.py.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

# --- BEGIN bullettrain locator (canonical) ---
_SHARED_GATE = Path("scripts") / "security" / "repo_oidc_assurance_gate.py"


def _main_checkout_workspace() -> Path | None:
    """The folder holding this repository's siblings, from any linked worktree.

    ``ROOT.parent`` is the workspace only in the main checkout; a worktree's
    neighbours are other worktrees (or a stale clone that happens to share
    BulletTrain's name). git's common directory is the MAIN checkout's ``.git``
    from every worktree, so its grandparent is the real workspace.
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
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
            env=env,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    common = answer.stdout.strip()
    if answer.returncode != 0 or not common:
        return None
    return Path(common).resolve().parent.parent


def _bullettrain_root() -> Path:
    """The BulletTrain checkout that holds the shared gate.

    An explicit BULLETTRAIN_ROOT wins, then the operator's workspace variables,
    then the workspace derived from git, then the folder beside this checkout.
    A candidate counts only if it really holds the shared gate.
    """
    candidates: list[Path] = []
    explicit = (os.environ.get("BULLETTRAIN_ROOT") or "").strip()
    if explicit:
        candidates.append(Path(explicit))
    for name in ("SYMPHONIX_WORKSPACE_ROOT", "CAID_WORKSPACE"):
        value = (os.environ.get(name) or "").strip()
        if value:
            candidates.append(Path(value) / "BulletTrain")
    derived = _main_checkout_workspace()
    if derived is not None:
        candidates.append(derived / "BulletTrain")
    candidates.append(ROOT.parent / "BulletTrain")
    for candidate in candidates:
        if (candidate / _SHARED_GATE).is_file():
            return candidate
    return ROOT.parent / "BulletTrain"


# --- END bullettrain locator ---


BULLETTRAIN_ROOT = _bullettrain_root()
if str(BULLETTRAIN_ROOT) not in sys.path:
    sys.path.insert(0, str(BULLETTRAIN_ROOT))

from scripts.security.repo_oidc_assurance_gate import main  # noqa: E402

if __name__ == "__main__":
    if "--root" not in sys.argv[1:]:
        # Answer for the repository this wrapper lives in, not for whatever
        # directory the shell happens to be in.
        sys.argv[1:1] = ["--root", str(ROOT)]
    raise SystemExit(main())
