"""Root pytest conftest — enforces zero-silent-fail / zero-skip policy."""

from __future__ import annotations

import os
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent
for _sub in ("src", "backend", "backend/src"):
    _cand = _REPO_ROOT / _sub
    if _cand.is_dir() and str(_cand) not in sys.path:
        sys.path.insert(0, str(_cand))


def _resolve_workspace_root(repo_root: Path) -> Path:
    for key in ("SYMPHONIX_WORKSPACE_ROOT", "CAID_WORKSPACE"):
        val = os.environ.get(key)
        if val and (Path(val) / "workspace-tooling").is_dir():
            return Path(val).resolve()
    parent = repo_root.parent
    if (parent / "workspace-tooling").is_dir():
        return parent.resolve()
    git_file = repo_root / ".git"
    if git_file.is_file():
        text = git_file.read_text(encoding="utf-8", errors="ignore").strip()
        if text.startswith("gitdir:"):
            gitdir = Path(text.split("gitdir:", 1)[1].strip())
            if len(gitdir.parents) >= 3:
                ws = gitdir.parents[3]
                if (ws / "workspace-tooling").is_dir():
                    return ws.resolve()
    return parent.resolve()


_WS = _resolve_workspace_root(_REPO_ROOT)
os.environ["SYMPHONIX_WORKSPACE_ROOT"] = str(_WS)
os.environ["CAID_WORKSPACE"] = str(_WS)


def pytest_sessionfinish(session, exitstatus) -> None:  # type: ignore[no-untyped-def]
    """Fail the pytest session if any test is skipped or xfailed (L-FIX-DO-NOT-SKIP)."""
    if os.environ.get("SYMPHONIX_ALLOW_TEST_SKIPS") == "1":
        return
    terminal = session.config.pluginmanager.get_plugin("terminalreporter")
    if terminal is None:
        return
    skipped = terminal.stats.get("skipped", [])
    xfailed = terminal.stats.get("xfailed", [])
    if skipped or xfailed:
        session.exitstatus = 1
