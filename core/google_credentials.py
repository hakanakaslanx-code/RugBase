"""Helpers for validating and normalising Google service account credentials."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Iterable, Mapping

__all__ = [
    "CredentialsFileInvalidError",
    "REQUIRED_FIELDS",
    "ensure_service_account_file",
    "load_service_account_data",
]


class CredentialsFileInvalidError(Exception):
    """Raised when a service account JSON file is missing required data."""


REQUIRED_FIELDS: Iterable[str] = (
    "type",
    "project_id",
    "private_key_id",
    "private_key",
    "client_email",
    "client_id",
    "token_uri",
    "auth_uri",
    "auth_provider_x509_cert_url",
    "client_x509_cert_url",
)


def _normalise_private_key(key: str) -> str:
    key = key.replace("\r\n", "\n").replace("\r", "\n")
    key = key.replace("\\n", "\n")
    if not key.endswith("\n"):
        key += "\n"
    return key


def _load_json(path: Path) -> Mapping[str, object]:
    try:
        with path.open("r", encoding="utf-8-sig") as handle:
            raw = handle.read()
    except OSError as exc:
        raise CredentialsFileInvalidError(f"JSON dosyası okunamadı: {exc}") from exc

    payload_text = raw.lstrip("\ufeff").strip()
    if not payload_text:
        raise CredentialsFileInvalidError("Service account JSON içeriği boş.")

    try:
        return json.loads(payload_text)
    except json.JSONDecodeError as exc:
        raise CredentialsFileInvalidError(f"JSON parse hatası: {exc.msg}") from exc


def _validate_payload(payload: Mapping[str, object]) -> Dict[str, object]:
    data: Dict[str, object] = dict(payload)
    missing: list[str] = []

    for field in REQUIRED_FIELDS:
        value = data.get(field)
        if not isinstance(value, str) or not value.strip():
            missing.append(field)

    if data.get("type") != "service_account":
        missing.append("type")

    if missing:
        ordered = ", ".join(sorted(dict.fromkeys(missing)))
        raise CredentialsFileInvalidError(f"JSON eksik alanlar: {ordered}")

    private_key = str(data["private_key"])
    data["private_key"] = _normalise_private_key(private_key)
    return data


def load_service_account_data(path: Path) -> Dict[str, object]:
    """Return validated service account data without modifying ``path``."""

    return _validate_payload(_load_json(path))


def ensure_service_account_file(path: Path) -> Dict[str, object]:
    """Validate ``path`` and persist a normalised copy of the credentials."""

    payload = load_service_account_data(path)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    return payload

