"""Utility helpers for installing optional runtime dependencies."""
from __future__ import annotations

import subprocess
import sys
from typing import List, Sequence, Tuple


def _run_subprocess(command: Sequence[str]) -> Tuple[bool, str]:
    """Execute ``command`` returning a success flag and combined output."""

    result = subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        check=False,
    )
    output = (result.stdout or "").strip()
    return result.returncode == 0, output


def _pip_args(packages: Sequence[str], *extra: str) -> List[str]:
    return [
        sys.executable,
        "-m",
        "pip",
        "--disable-pip-version-check",
        "install",
        *packages,
        *extra,
    ]


def install_packages(packages: Sequence[str]) -> Tuple[bool, str]:
    """Install ``packages`` using pip, attempting to bootstrap pip when necessary."""

    packages = [package for package in packages if package]
    if not packages:
        return True, ""

    success, output = _run_subprocess(_pip_args(packages))
    if success:
        return True, output

    lowered = output.lower()
    pip_missing = "no module named pip" in lowered or "pip is not recognized" in lowered
    if pip_missing:
        ensure_success, ensure_output = _run_subprocess(
            [sys.executable, "-m", "ensurepip", "--upgrade"]
        )
        if not ensure_success:
            details = ensure_output or output
            return False, f"pip could not be bootstrapped: {details}"
        success, output = _run_subprocess(_pip_args(packages))
        if success:
            return True, output

    permission_error = any(
        keyword in lowered for keyword in ("permission", "access is denied", "permission denied")
    )
    if permission_error:
        success, user_output = _run_subprocess(_pip_args(packages, "--user"))
        if success:
            return True, user_output
        output = f"{output}\n{user_output}".strip()

    return False, output
