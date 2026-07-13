"""HMAC-Prüfung der Webhook-Zustellungen (Stripe-Stil).

Signiert wird ``f"{timestamp}."`` + die **rohen Body-Bytes**. Niemals gegen
re-serialisiertes JSON prüfen: Retries von heidi.cloud tragen dieselbe Nachricht
mit anderen Bytes.
"""

import hashlib
import hmac


SIGNATURE_HEADER = "Heidi-Signature"


def sign(secret: str, timestamp: int, body: bytes) -> str:
    """Erzeugt den Header-Wert ``t=<unix>,v1=<hex>``."""
    digest = hmac.new(
        secret.encode(), f"{timestamp}.".encode() + body, hashlib.sha256
    ).hexdigest()
    return f"t={timestamp},v1={digest}"


def verify(
    secret: str,
    header_value: str,
    body: bytes,
    *,
    now: int,
    tolerance_seconds: int = 300,
) -> bool:
    """Prüft den ``Heidi-Signature``-Header gegen die Raw Bytes des Bodys."""
    if not secret:
        return False
    try:
        t_part, v1_part = header_value.split(",", 1)
        if not t_part.startswith("t=") or not v1_part.startswith("v1="):
            return False
        if not t_part[2:].isdigit():
            return False
        timestamp = int(t_part[2:])
        signature = v1_part[3:]
    except ValueError:
        return False
    if abs(now - timestamp) > tolerance_seconds:
        return False
    expected = hmac.new(
        secret.encode(), f"{timestamp}.".encode() + body, hashlib.sha256
    ).digest()
    try:
        provided = bytes.fromhex(signature)
    except ValueError:
        return False
    return hmac.compare_digest(expected, provided)
