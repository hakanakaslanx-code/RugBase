import json
from pathlib import Path

import pytest

from core.google_credentials import (
    CredentialsFileInvalidError,
    ensure_service_account_file,
    load_service_account_data,
)


def _sample_payload() -> dict[str, str]:
    return {
        "type": "service_account",
        "project_id": "demo-project",
        "private_key_id": "abc123",
        "private_key": "-----BEGIN PRIVATE KEY-----\\nLINE1\\nLINE2\\n-----END PRIVATE KEY-----",
        "client_email": "demo@example.com",
        "client_id": "123456789",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/demo",
    }


def test_ensure_service_account_file_normalises_private_key(tmp_path: Path) -> None:
    payload = _sample_payload()
    path = tmp_path / "service_account.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    normalised = ensure_service_account_file(path)

    assert "\n" in normalised["private_key"]
    saved = json.loads(path.read_text(encoding="utf-8"))
    assert saved["private_key"].endswith("\n")


def test_load_service_account_data_requires_fields(tmp_path: Path) -> None:
    path = tmp_path / "invalid.json"
    path.write_text(json.dumps({"type": "wrong"}), encoding="utf-8")

    with pytest.raises(CredentialsFileInvalidError) as excinfo:
        load_service_account_data(path)

    assert "CredentialsFileInvalidError" not in str(excinfo.value)
    assert "JSON missing fields" in str(excinfo.value)
