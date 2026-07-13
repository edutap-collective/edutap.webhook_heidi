from edutap.webhook_heidi.signing import sign
from edutap.webhook_heidi.signing import SIGNATURE_HEADER
from edutap.webhook_heidi.signing import verify

import json


SECRET = "0123456789abcdef" * 4  # 64 hex chars, wie secrets.token_hex(32)
NOW = 1752422820
BODY = b'{"id":"evt_1","type":"pass.installed"}'


def test_header_name():
    assert SIGNATURE_HEADER == "Heidi-Signature"


def test_sign_format():
    header = sign(SECRET, NOW, BODY)
    t_part, v1_part = header.split(",")
    assert t_part == f"t={NOW}"
    assert v1_part.startswith("v1=")
    assert len(v1_part[3:]) == 64  # sha256 hex


def test_valid_signature():
    assert verify(SECRET, sign(SECRET, NOW, BODY), BODY, now=NOW) is True


def test_tampered_body():
    header = sign(SECRET, NOW, BODY)
    assert verify(SECRET, header, BODY + b" ", now=NOW) is False


def test_wrong_secret():
    assert verify("anderes-secret", sign(SECRET, NOW, BODY), BODY, now=NOW) is False


def test_timestamp_too_old():
    header = sign(SECRET, NOW, BODY)
    assert verify(SECRET, header, BODY, now=NOW + 301) is False


def test_timestamp_too_far_in_the_future():
    """abs() — auch nach vorne verschobene Zeitstempel sind ungültig."""
    header = sign(SECRET, NOW, BODY)
    assert verify(SECRET, header, BODY, now=NOW - 301) is False


def test_timestamp_within_tolerance():
    header = sign(SECRET, NOW, BODY)
    assert verify(SECRET, header, BODY, now=NOW + 299) is True


def test_malformed_headers():
    for header in (
        "",
        "garbage",
        "t=abc,v1=x",
        "v1=x,t=1",
        f"t={NOW}",
        f"t={NOW};v1=x",
    ):
        assert verify(SECRET, header, BODY, now=NOW) is False


def test_retry_bytes_verify_independently():
    """DER Fall, der eine naive Implementierung bricht.

    heidi.cloud sendet Retries aus einem JSONB-Roundtrip: gleiche Nachricht,
    andere Bytes (normalisierte Key-Reihenfolge, kompakte Separatoren), pro
    Versuch neu signiert. Wer gegen re-serialisiertes JSON prüft statt gegen die
    Raw Bytes, besteht den Erstversuch und scheitert am Retry.
    """
    event = {
        "type": "pass.installed",
        "id": "evt_1",
        "data": {"pass_id": "p", "person_id": "x"},
    }
    first_attempt = json.dumps(event).encode()
    retry = json.dumps(event, sort_keys=True, separators=(",", ":")).encode()
    assert first_attempt != retry

    assert (
        verify(SECRET, sign(SECRET, NOW, first_attempt), first_attempt, now=NOW) is True
    )
    assert verify(SECRET, sign(SECRET, NOW, retry), retry, now=NOW) is True
    # Signatur des einen Byte-Strings gilt NICHT für den anderen:
    assert verify(SECRET, sign(SECRET, NOW, first_attempt), retry, now=NOW) is False


def test_non_ascii_body():
    body = json.dumps({"title": "Bibliotheksausweis Universität"}).encode()
    assert verify(SECRET, sign(SECRET, NOW, body), body, now=NOW) is True
