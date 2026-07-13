# edutap.webhook_heidi Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Einen FastAPI-Endpoint bauen, der die signierten Pass-Events von `heidi.cloud` entgegennimmt, verifiziert und in eine Kafka-Queue schreibt — plus die Consumer-Seite, mit der ein Spooler die Queue liest.

**Architecture:** Der Webhook ist bewusst dünn: Raw Bytes lesen → HMAC prüfen → Envelope parsen → `enqueue()` → 2xx. Die Queue liegt hinter einem `QueueBackend`-Protocol, das per setuptools-Entry-Point ausgewählt wird (eduTAP-Hauskonvention). Ausprogrammiert wird Kafka; ein In-Memory-Backend dient Tests. Dedupliziert wird **nicht** hier, sondern beim Consumer über die `eventid`.

**Tech Stack:** Python ≥3.10, FastAPI, Pydantic v2, pydantic-settings, aiokafka, pytest + `fastapi.testclient.TestClient`, uv, ruff.

**Spec:** [docs/superpowers/specs/2026-07-13-webhook-heidi-design.md](../specs/2026-07-13-webhook-heidi-design.md) — bei Widersprüchen gilt die Spec.

## Global Constraints

- **Python ≥ 3.10** (`requires-python`), CI-Matrix 3.10–3.14. Keine 3.11+-only-Syntax.
- **Coverage-Gate: `fail_under = 90`** (`pyproject.toml`). Jede Task hält das ein.
- **Ruff-Import-Stil** (`[tool.ruff.lint.isort]`): `force-single-line`, `from-first`, `no-sections`, `lines-after-imports = 2`. Also: alle `from X import Y` **einzeln** und alphabetisch **vor** den `import X`, Stdlib und Third-Party gemischt, zwei Leerzeilen nach dem Importblock. Beispiel:
  ```python
  from edutap.webhook_heidi.settings import Settings
  from fastapi import APIRouter
  from pydantic import BaseModel

  import hashlib
  import time


  ```
  Im Zweifel `uvx ruff check --fix . && uvx ruff format .` laufen lassen — das ist die Autorität.
- **`print()` ist verboten** (ruff `T20`), außer in Tests.
- **Keine KI-Attribution in Commits.** Keine `Co-Authored-By:`-Trailer, kein „Generated with"-Footer. (Sonst erscheint der Bot im GitHub-Contributor-Graph.)
- **Changelog:** Jede Task ergänzt `CHANGES.md` unter `## unreleased`.
- **Sender-Vertrag ist gegeben, nicht verhandelbar.** Signatur-Header `Heidi-Signature`, HMAC-SHA256 über `f"{t}." + raw_body`, Toleranz 300 s, Erfolg = jedes 2xx, Body wird vom Sender nie gelesen.

## Dateistruktur

| Datei | Verantwortung | Task |
|---|---|---|
| `src/edutap/webhook_heidi/settings.py` | **Alle** Konfigurationswerte (einzige Config-Quelle) | 1 |
| `src/edutap/webhook_heidi/models.py` | `WebhookEvent` (Envelope, lax) + `QueueMessage` | 2 |
| `src/edutap/webhook_heidi/signing.py` | `verify()` — HMAC gegen Raw Bytes | 3 |
| `src/edutap/webhook_heidi/protocols.py` | `QueueBackend`-Protocol + `QueueUnavailable` | 4 |
| `src/edutap/webhook_heidi/plugins.py` | Backend-Auswahl per Entry-Point | 4 |
| `src/edutap/webhook_heidi/queues/memory.py` | `InMemoryQueueBackend` (Tests) | 4 |
| `src/edutap/webhook_heidi/handlers/fastapi.py` | `router` — der POST-Endpoint | 5 |
| `src/edutap/webhook_heidi/queues/kafka.py` | `KafkaQueueBackend` (Producer + Consumer) | 6 |

---

### Task 1: Settings

Die einzige Konfigurationsquelle. Alles Weitere baut darauf auf.

**Files:**
- Create: `src/edutap/webhook_heidi/settings.py`
- Test: `tests/test_settings.py`
- Modify: `CHANGES.md`

**Interfaces:**
- Produces: `ENV_PREFIX: str`, `class Settings(BaseSettings)` mit den Feldern `handler_prefix`, `webhook_secret: SecretStr` (**required, kein Default**), `signature_tolerance_seconds`, `enqueue_timeout`, `kafka_bootstrap_servers`, `kafka_topic`, `kafka_consumer_group`, `kafka_security_protocol`, `kafka_sasl_mechanism`, `kafka_sasl_username`, `kafka_sasl_password`.

- [ ] **Step 1: Write the failing test**

`tests/test_settings.py`:

```python
from edutap.webhook_heidi.settings import ENV_PREFIX
from edutap.webhook_heidi.settings import Settings

import pydantic
import pytest


def test_env_prefix():
    assert ENV_PREFIX == "EDUTAP_WEBHOOK_HEIDI_"


def test_secret_is_required(monkeypatch):
    """Ohne Secret muss der Start scheitern — nicht still unsicher laufen."""
    monkeypatch.delenv(f"{ENV_PREFIX}WEBHOOK_SECRET", raising=False)
    with pytest.raises(pydantic.ValidationError):
        Settings(_env_file=None)


def test_settings_from_env(monkeypatch):
    monkeypatch.setenv(f"{ENV_PREFIX}WEBHOOK_SECRET", "s3cret")
    monkeypatch.setenv(f"{ENV_PREFIX}KAFKA_TOPIC", "other.topic")
    settings = Settings(_env_file=None)
    assert settings.webhook_secret.get_secret_value() == "s3cret"
    assert settings.kafka_topic == "other.topic"


def test_defaults(monkeypatch):
    monkeypatch.setenv(f"{ENV_PREFIX}WEBHOOK_SECRET", "s3cret")
    settings = Settings(_env_file=None)
    assert settings.handler_prefix == "/webhook/heidi"
    assert settings.signature_tolerance_seconds == 300
    assert settings.enqueue_timeout == 10.0
    assert settings.kafka_topic == "heidi.pass-events"
    assert settings.kafka_consumer_group == "heidi-pass-spooler"
    assert settings.kafka_security_protocol == "PLAINTEXT"


def test_secret_is_not_leaked_in_repr(monkeypatch):
    """SecretStr darf nicht in Logs/Tracebacks landen."""
    monkeypatch.setenv(f"{ENV_PREFIX}WEBHOOK_SECRET", "s3cret")
    assert "s3cret" not in repr(Settings(_env_file=None))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_settings.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'edutap.webhook_heidi.settings'`

- [ ] **Step 3: Write minimal implementation**

`src/edutap/webhook_heidi/settings.py`:

```python
"""Konfiguration. Einzige Config-Quelle des Pakets — alles läuft über Settings."""

from pydantic import SecretStr
from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict


ENV_PREFIX = "EDUTAP_WEBHOOK_HEIDI_"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix=ENV_PREFIX,
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # HTTP-Endpoint
    handler_prefix: str = "/webhook/heidi"

    # Signaturprüfung — Sender-Vertrag, siehe Spec §2.2
    webhook_secret: SecretStr
    """HMAC-Secret aus der heidi.cloud-Admin-UI. Kein Default: fehlt es, schlägt
    der Start fehl, statt still jede Signatur abzulehnen."""

    signature_tolerance_seconds: int = 300

    # Enqueue — muss deutlich unter dem 30-s-Timeout des Senders bleiben
    enqueue_timeout: float = 10.0

    # Kafka-Backend (Extra [kafka])
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "heidi.pass-events"
    kafka_consumer_group: str = "heidi-pass-spooler"
    kafka_security_protocol: str = "PLAINTEXT"
    kafka_sasl_mechanism: str | None = None
    kafka_sasl_username: str | None = None
    kafka_sasl_password: SecretStr | None = None
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_settings.py -v`
Expected: 5 passed

- [ ] **Step 5: Lint & commit**

```bash
uvx ruff check --fix . && uvx ruff format .
git add src/edutap/webhook_heidi/settings.py tests/test_settings.py CHANGES.md
git commit -m "feat(settings): pydantic-settings als einzige Config-Quelle"
```

`CHANGES.md` unter `## unreleased`: `- Settings (pydantic-settings) für Webhook-Secret, Signatur-Toleranz und Kafka.`

---

### Task 2: Modelle — Envelope und Queue-Message

Der Envelope wird **bewusst lax** modelliert: unbekannte `type`-/`reason`-Werte und neue Felder müssen durchgehen, weil jedes Non-2xx beim Sender 12 Retries über 48 h auslöst.

**Files:**
- Create: `src/edutap/webhook_heidi/models.py`
- Test: `tests/test_models.py`
- Modify: `CHANGES.md`

**Interfaces:**
- Consumes: nichts.
- Produces:
  - `WEBHOOK_TEST: str = "webhook.test"`
  - `class WebhookEventData(BaseModel)` — `pass_id: str`, `person_id: str`, `extra="allow"`
  - `class WebhookEvent(BaseModel)` — `id: str`, `type: str`, `created: datetime`, `api_version: str | None`, `data: WebhookEventData`, `extra="allow"`
  - `class QueueMessage(BaseModel)` — `eventid, passid, personid, action: str`, `timestamp: datetime`, `payload: dict[str, Any]`; Klassenmethode `QueueMessage.from_event(event: WebhookEvent) -> QueueMessage`

- [ ] **Step 1: Write the failing test**

`tests/test_models.py`:

```python
from edutap.webhook_heidi.models import QueueMessage
from edutap.webhook_heidi.models import WebhookEvent

import pydantic
import pytest


INSTALLED = {
    "id": "evt_9f4c2a1e6b8d4f0eae7cd3a2b1f0c9e8",
    "type": "pass.installed",
    "created": "2026-07-09T12:34:56Z",
    "api_version": "2026-07-09",
    "data": {
        "pass_id": "0c0ffee0-0000-0000-0000-000000000001",
        "person_id": "12345",
        "template_id": "0c0ffee0-0000-0000-0000-000000000002",
        "template_title": "Student ID 2026",
        "preset": {"uuid": "0c0ffee0-0000-0000-0000-000000000003", "title": "Student ID"},
        "wallet_type": "APPLE_ACCESS",
        "state": "ACTIVE",
        "reason": "provisioning",
        "confirmation": "device",
        "device": {"type": "1", "bundle_identifier": "a1b2c3"},
        "error": None,
    },
}


def test_parses_the_canonical_envelope():
    event = WebhookEvent.model_validate(INSTALLED)
    assert event.id == "evt_9f4c2a1e6b8d4f0eae7cd3a2b1f0c9e8"
    assert event.type == "pass.installed"
    assert event.data.pass_id == "0c0ffee0-0000-0000-0000-000000000001"


def test_unknown_type_is_accepted():
    """type ist KEIN Enum — heidi.cloud darf Typen additiv ergänzen."""
    event = WebhookEvent.model_validate({**INSTALLED, "type": "pass.teleported"})
    assert event.type == "pass.teleported"


def test_unknown_reason_and_new_fields_are_accepted():
    payload = {**INSTALLED, "data": {**INSTALLED["data"], "reason": "brandneu", "future": 42}}
    event = WebhookEvent.model_validate(payload)
    assert event.data.reason == "brandneu"


def test_empty_error_category_is_accepted():
    """Apple-Access-Fehler liefern category="" (leerer String, nicht null)."""
    payload = {**INSTALLED, "data": {**INSTALLED["data"], "error": {"category": "", "message": ""}}}
    assert WebhookEvent.model_validate(payload).data.error == {"category": "", "message": ""}


def test_missing_pass_id_is_rejected():
    """Ohne pass_id ist es kein brauchbares Event -> 400 im Endpoint."""
    broken = {**INSTALLED, "data": {"person_id": "12345"}}
    with pytest.raises(pydantic.ValidationError):
        WebhookEvent.model_validate(broken)


def test_missing_envelope_field_is_rejected():
    broken = {k: v for k, v in INSTALLED.items() if k != "id"}
    with pytest.raises(pydantic.ValidationError):
        WebhookEvent.model_validate(broken)


def test_queue_message_from_event_maps_all_fields():
    message = QueueMessage.from_event(WebhookEvent.model_validate(INSTALLED))
    assert message.eventid == INSTALLED["id"]
    assert message.passid == INSTALLED["data"]["pass_id"]
    assert message.personid == INSTALLED["data"]["person_id"]
    assert message.action == "pass.installed"
    assert message.timestamp.year == 2026


def test_payload_carries_data_verbatim():
    """payload enthält data vollständig — inkl. unbekannter Zusatzfelder."""
    payload = {**INSTALLED, "data": {**INSTALLED["data"], "future": 42}}
    message = QueueMessage.from_event(WebhookEvent.model_validate(payload))
    assert message.payload["state"] == "ACTIVE"
    assert message.payload["reason"] == "provisioning"
    assert message.payload["device"] == {"type": "1", "bundle_identifier": "a1b2c3"}
    assert message.payload["future"] == 42


def test_queue_message_roundtrips_through_json():
    """Die Nachricht muss verlustfrei durch Kafka (JSON) gehen."""
    message = QueueMessage.from_event(WebhookEvent.model_validate(INSTALLED))
    restored = QueueMessage.model_validate_json(message.model_dump_json())
    assert restored == message
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_models.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'edutap.webhook_heidi.models'`

- [ ] **Step 3: Write minimal implementation**

`src/edutap/webhook_heidi/models.py`:

```python
"""Datenmodelle: der eingehende Envelope und die Queue-Nachricht.

Der Envelope wird bewusst LAX validiert. heidi.cloud behandelt jedes Non-2xx als
Fehlschlag und wiederholt 12x über 48 h — strenge Validierung erzeugt also keinen
sauberen Reject, sondern einen Retry-Sturm. Unbekannte Event-Typen, unbekannte
``reason``-Werte und neue Felder müssen deshalb durchgehen.
"""

from datetime import datetime
from pydantic import BaseModel
from pydantic import ConfigDict
from typing import Any


WEBHOOK_TEST = "webhook.test"
"""Konnektivitätstest aus der heidi.cloud-Admin-UI. Wird angenommen, aber nicht
in die Queue geschrieben (Null-UUID als pass_id)."""


class WebhookEventData(BaseModel):
    """``data`` aus dem Envelope. Nur die Felder, die wir wirklich brauchen —
    alles Übrige wird durchgereicht (``extra="allow"``)."""

    model_config = ConfigDict(extra="allow")

    pass_id: str
    person_id: str


class WebhookEvent(BaseModel):
    """Der Envelope von heidi.cloud.

    ``type`` ist bewusst ``str`` und kein Enum: der Sender darf Event-Typen
    additiv ergänzen, ohne dass wir mit 400 antworten.
    """

    model_config = ConfigDict(extra="allow")

    id: str
    type: str
    created: datetime
    api_version: str | None = None
    data: WebhookEventData


class QueueMessage(BaseModel):
    """Was in die Pass-Queue geschrieben wird."""

    eventid: str
    """``evt_…`` — Dedup-Key. Ausgewertet wird er vom Consumer, nicht von uns."""

    passid: str
    """Kafka-Partition-Key: garantiert Reihenfolge je Pass."""

    personid: str
    action: str
    timestamp: datetime
    payload: dict[str, Any]
    """``data`` roh und vollständig."""

    @classmethod
    def from_event(cls, event: WebhookEvent) -> "QueueMessage":
        return cls(
            eventid=event.id,
            passid=event.data.pass_id,
            personid=event.data.person_id,
            action=event.type,
            timestamp=event.created,
            payload=event.data.model_dump(mode="json"),
        )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_models.py -v`
Expected: 9 passed

- [ ] **Step 5: Lint & commit**

```bash
uvx ruff check --fix . && uvx ruff format .
git add src/edutap/webhook_heidi/models.py tests/test_models.py CHANGES.md
git commit -m "feat(models): Envelope (lax) und Queue-Message"
```

---

### Task 3: Signaturprüfung

Der sicherheitskritische Teil. **Gegen Raw Bytes verifizieren** — nie gegen re-serialisiertes JSON: heidi.cloud sendet Retries aus einem JSONB-Roundtrip mit normalisierter Key-Reihenfolge, also dieselbe Nachricht mit *anderen Bytes*, pro Versuch neu signiert.

**Files:**
- Create: `src/edutap/webhook_heidi/signing.py`
- Test: `tests/test_signing.py`
- Modify: `CHANGES.md`

**Interfaces:**
- Produces: `SIGNATURE_HEADER: str = "Heidi-Signature"`, `sign(secret: str, timestamp: int, body: bytes) -> str`, `verify(secret: str, header_value: str, body: bytes, *, now: int, tolerance_seconds: int = 300) -> bool`

- [ ] **Step 1: Write the failing test**

`tests/test_signing.py`:

```python
from edutap.webhook_heidi.signing import SIGNATURE_HEADER
from edutap.webhook_heidi.signing import sign
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
    for header in ("", "garbage", "t=abc,v1=x", "v1=x,t=1", f"t={NOW}", f"t={NOW};v1=x"):
        assert verify(SECRET, header, BODY, now=NOW) is False


def test_retry_bytes_verify_independently():
    """DER Fall, der eine naive Implementierung bricht.

    heidi.cloud sendet Retries aus einem JSONB-Roundtrip: gleiche Nachricht,
    andere Bytes (normalisierte Key-Reihenfolge, kompakte Separatoren), pro
    Versuch neu signiert. Wer gegen re-serialisiertes JSON prüft statt gegen die
    Raw Bytes, besteht den Erstversuch und scheitert am Retry.
    """
    event = {"type": "pass.installed", "id": "evt_1", "data": {"pass_id": "p", "person_id": "x"}}
    first_attempt = json.dumps(event).encode()
    retry = json.dumps(event, sort_keys=True, separators=(",", ":")).encode()
    assert first_attempt != retry

    assert verify(SECRET, sign(SECRET, NOW, first_attempt), first_attempt, now=NOW) is True
    assert verify(SECRET, sign(SECRET, NOW, retry), retry, now=NOW) is True
    # Signatur des einen Byte-Strings gilt NICHT für den anderen:
    assert verify(SECRET, sign(SECRET, NOW, first_attempt), retry, now=NOW) is False


def test_non_ascii_body():
    body = json.dumps({"title": "Bibliotheksausweis Universität"}).encode()
    assert verify(SECRET, sign(SECRET, NOW, body), body, now=NOW) is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_signing.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'edutap.webhook_heidi.signing'`

- [ ] **Step 3: Write minimal implementation**

`src/edutap/webhook_heidi/signing.py` — spiegelt `heidi.cloud/src/heidi/cloud/webhooks/signing.py`:

```python
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
    try:
        t_part, v1_part = header_value.split(",", 1)
        if not t_part.startswith("t=") or not v1_part.startswith("v1="):
            return False
        timestamp = int(t_part[2:])
        signature = v1_part[3:]
    except ValueError:
        return False
    if abs(now - timestamp) > tolerance_seconds:
        return False
    expected = sign(secret, timestamp, body)
    return hmac.compare_digest(expected, f"t={timestamp},v1={signature}")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_signing.py -v`
Expected: 11 passed

- [ ] **Step 5: Lint & commit**

```bash
uvx ruff check --fix . && uvx ruff format .
git add src/edutap/webhook_heidi/signing.py tests/test_signing.py CHANGES.md
git commit -m "feat(signing): HMAC-SHA256-Prüfung gegen Raw Bytes"
```

---

### Task 4: QueueBackend — Protocol, Plugin-Auswahl, In-Memory-Backend

Die Abstraktion trägt **beide** Seiten der Queue: `enqueue()` für den Webhook, `consume()`/`ack()` für den Consumer. Nur so ist das Backend auch für den Spooler austauschbar.

**Files:**
- Create: `src/edutap/webhook_heidi/protocols.py`
- Create: `src/edutap/webhook_heidi/plugins.py`
- Create: `src/edutap/webhook_heidi/queues/__init__.py`
- Create: `src/edutap/webhook_heidi/queues/memory.py`
- Create: `tests/conftest.py`
- Create: `tests/plugins.py`
- Test: `tests/test_plugins.py`, `tests/test_queues_memory.py`
- Modify: `CHANGES.md`

**Interfaces:**
- Consumes: `QueueMessage` (Task 2), `Settings` (Task 1).
- Produces:
  - `class QueueUnavailable(RuntimeError)` — vom Backend geworfen, wenn der Write nicht bestätigt wurde.
  - `class QueueBackend(Protocol)` mit `async enqueue(message: QueueMessage) -> None`, `consume() -> AsyncIterator[QueueMessage]`, `async ack(message: QueueMessage) -> None`, `async stop() -> None`.
  - `ENTRY_POINT_GROUP: str = "edutap.webhook_heidi.plugins"`
  - `get_queue_backend() -> QueueBackend` (gecachte Instanz), `add_plugin(klass)`, `reset_queue_backend()`.
  - `class InMemoryQueueBackend` — implementiert das Protocol; zusätzlich `messages: list[QueueMessage]` (alles je Enqueuete, für Assertions) und `acked: list[str]`.

- [ ] **Step 1: Write the failing test**

`tests/test_plugins.py`:

```python
from edutap.webhook_heidi.plugins import add_plugin
from edutap.webhook_heidi.plugins import get_queue_backend
from edutap.webhook_heidi.plugins import reset_queue_backend
from edutap.webhook_heidi.protocols import QueueBackend
from edutap.webhook_heidi.queues.memory import InMemoryQueueBackend

import pytest


def test_no_backend_registered():
    reset_queue_backend()
    with pytest.raises(NotImplementedError):
        get_queue_backend()


def test_registered_backend_is_returned():
    reset_queue_backend()
    add_plugin(InMemoryQueueBackend)
    assert isinstance(get_queue_backend(), InMemoryQueueBackend)


def test_backend_instance_is_cached():
    """Ein Kafka-Producer darf nicht pro Request neu aufgebaut werden."""
    reset_queue_backend()
    add_plugin(InMemoryQueueBackend)
    assert get_queue_backend() is get_queue_backend()


def test_two_backends_are_rejected():
    reset_queue_backend()
    add_plugin(InMemoryQueueBackend)
    add_plugin(InMemoryQueueBackend)
    with pytest.raises(ValueError):
        get_queue_backend()


def test_non_conforming_class_is_rejected():
    class NotABackend:
        pass

    reset_queue_backend()
    with pytest.raises(TypeError):
        add_plugin(NotABackend)


def test_memory_backend_conforms_to_protocol():
    assert issubclass(InMemoryQueueBackend, QueueBackend)
```

`tests/test_queues_memory.py`:

```python
from datetime import datetime
from datetime import timezone
from edutap.webhook_heidi.models import QueueMessage
from edutap.webhook_heidi.queues.memory import InMemoryQueueBackend


def _message(eventid: str = "evt_1") -> QueueMessage:
    return QueueMessage(
        eventid=eventid,
        passid="p1",
        personid="x",
        action="pass.installed",
        timestamp=datetime(2026, 7, 9, 12, 34, 56, tzinfo=timezone.utc),
        payload={"state": "ACTIVE"},
    )


async def test_enqueue_records_the_message():
    backend = InMemoryQueueBackend()
    await backend.enqueue(_message())
    assert [m.eventid for m in backend.messages] == ["evt_1"]


async def test_consume_yields_enqueued_messages():
    backend = InMemoryQueueBackend()
    await backend.enqueue(_message("evt_1"))
    await backend.enqueue(_message("evt_2"))

    seen = []
    async for message in backend.consume():
        seen.append(message.eventid)
        await backend.ack(message)

    assert seen == ["evt_1", "evt_2"]
    assert backend.acked == ["evt_1", "evt_2"]


async def test_duplicates_are_kept():
    """Der Webhook dedupliziert NICHT — das ist Aufgabe des Consumers."""
    backend = InMemoryQueueBackend()
    await backend.enqueue(_message("evt_1"))
    await backend.enqueue(_message("evt_1"))
    assert len(backend.messages) == 2
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_plugins.py tests/test_queues_memory.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'edutap.webhook_heidi.protocols'`

- [ ] **Step 3: Write minimal implementation**

`src/edutap/webhook_heidi/protocols.py`:

```python
"""Die austauschbare Queue-Abstraktion."""

from collections.abc import AsyncIterator
from edutap.webhook_heidi.models import QueueMessage
from typing import Protocol
from typing import runtime_checkable


class QueueUnavailable(RuntimeError):
    """Der Write konnte nicht bestätigt werden.

    Der Endpoint übersetzt das in 503 — heidi.cloud wiederholt dann. Auf keinen
    Fall 2xx antworten: der Sender wiederholt nur bei Non-2xx, ein verschluckter
    Fehler wäre ein endgültig verlorenes Event.
    """


@runtime_checkable
class QueueBackend(Protocol):
    """Beide Seiten der Pass-Queue — Producer für den Webhook, Consumer für den
    Spooler. Ein Consumer soll weder aiokafka noch Offsets kennen müssen."""

    async def enqueue(self, message: QueueMessage) -> None:
        """Schreibt die Nachricht dauerhaft in die Queue.

        :raises QueueUnavailable: wenn der Write nicht bestätigt wurde.
        """
        ...

    def consume(self) -> AsyncIterator[QueueMessage]:
        """Liefert Nachrichten, bis der Consumer abbricht."""
        ...

    async def ack(self, message: QueueMessage) -> None:
        """Bestätigt die Verarbeitung (Kafka: Offset-Commit)."""
        ...

    async def stop(self) -> None:
        """Fährt Producer/Consumer sauber herunter."""
        ...
```

`src/edutap/webhook_heidi/plugins.py`:

```python
"""Backend-Auswahl per setuptools-Entry-Point — eduTAP-Hauskonvention.

Ein Consumer registriert sein Backend in der eigenen ``pyproject.toml``::

    [project.entry-points.'edutap.webhook_heidi.plugins']
    QueueBackend = 'edutap.webhook_heidi.queues.kafka:KafkaQueueBackend'
"""

from edutap.webhook_heidi.protocols import QueueBackend
from importlib.metadata import entry_points


ENTRY_POINT_GROUP = "edutap.webhook_heidi.plugins"
PLUGIN_NAME = "QueueBackend"

_registry: list[type] = []
_backend: QueueBackend | None = None


def add_plugin(klass: type) -> None:
    """Registriert ein Backend programmatisch (für Tests und Einbettung)."""
    if not issubclass(klass, QueueBackend):
        raise TypeError(f"{klass!r} implementiert QueueBackend nicht.")
    _registry.append(klass)


def reset_queue_backend() -> None:
    """Leert Registry und Instanz-Cache. Für Tests."""
    global _backend
    _registry.clear()
    _backend = None


def get_queue_backend() -> QueueBackend:
    """Liefert das konfigurierte Backend — genau eines, gecacht."""
    global _backend
    if _backend is not None:
        return _backend

    candidates = [
        ep.load()
        for ep in entry_points(group=ENTRY_POINT_GROUP)
        if ep.name == PLUGIN_NAME
    ]
    candidates += _registry

    if not candidates:
        raise NotImplementedError(
            f"Kein QueueBackend registriert. Entry-Point '{PLUGIN_NAME}' in der "
            f"Gruppe '{ENTRY_POINT_GROUP}' setzen oder add_plugin() nutzen."
        )
    if len(candidates) > 1:
        raise ValueError(f"Mehrere QueueBackends gefunden: {candidates!r}")

    _backend = candidates[0]()
    return _backend
```

`src/edutap/webhook_heidi/queues/__init__.py`:

```python
"""Queue-Backends. Ausprogrammiert ist Kafka; In-Memory dient Tests."""
```

`src/edutap/webhook_heidi/queues/memory.py`:

```python
"""In-Memory-Backend — für Tests und lokale Entwicklung, ohne Broker."""

from collections.abc import AsyncIterator
from edutap.webhook_heidi.models import QueueMessage


class InMemoryQueueBackend:
    """Hält die Nachrichten in einer Liste. Nicht für den Produktivbetrieb."""

    def __init__(self) -> None:
        self.messages: list[QueueMessage] = []
        self.acked: list[str] = []

    async def enqueue(self, message: QueueMessage) -> None:
        self.messages.append(message)

    async def consume(self) -> AsyncIterator[QueueMessage]:
        for message in list(self.messages):
            yield message

    async def ack(self, message: QueueMessage) -> None:
        self.acked.append(message.eventid)

    async def stop(self) -> None:
        return None
```

`tests/plugins.py`:

```python
"""Test-Backends. Getrennt von den Tests, damit sie per Entry-Point ladbar wären."""

from edutap.webhook_heidi.models import QueueMessage
from edutap.webhook_heidi.protocols import QueueUnavailable
from edutap.webhook_heidi.queues.memory import InMemoryQueueBackend


class FailingQueueBackend(InMemoryQueueBackend):
    """Simuliert eine nicht erreichbare Queue -> der Endpoint muss 503 liefern."""

    async def enqueue(self, message: QueueMessage) -> None:
        raise QueueUnavailable("Broker nicht erreichbar")
```

`tests/conftest.py`:

```python
"""Gemeinsame Fixtures.

Wichtig: ``handlers.fastapi`` instanziiert ``Settings()`` beim Import (der
Router-Prefix kommt aus den Settings — Hauskonvention von wallet_google/apple).
``webhook_secret`` hat keinen Default, also muss die Env-Var gesetzt sein, BEVOR
das Modul importiert wird. Deshalb hier auf Modulebene, nicht in einer Fixture.
"""

from edutap.webhook_heidi.plugins import add_plugin
from edutap.webhook_heidi.plugins import get_queue_backend
from edutap.webhook_heidi.plugins import reset_queue_backend
from edutap.webhook_heidi.queues.memory import InMemoryQueueBackend

import os
import pytest


TEST_SECRET = "0123456789abcdef" * 4

os.environ.setdefault("EDUTAP_WEBHOOK_HEIDI_WEBHOOK_SECRET", TEST_SECRET)


@pytest.fixture
def memory_backend() -> InMemoryQueueBackend:
    """Hängt das In-Memory-Backend ein und räumt danach auf."""
    reset_queue_backend()
    add_plugin(InMemoryQueueBackend)
    backend = get_queue_backend()
    yield backend
    reset_queue_backend()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_plugins.py tests/test_queues_memory.py -v`
Expected: 9 passed

- [ ] **Step 5: Lint & commit**

```bash
uvx ruff check --fix . && uvx ruff format .
git add src/edutap/webhook_heidi/protocols.py src/edutap/webhook_heidi/plugins.py \
        src/edutap/webhook_heidi/queues/ tests/conftest.py tests/plugins.py \
        tests/test_plugins.py tests/test_queues_memory.py CHANGES.md
git commit -m "feat(queue): QueueBackend-Protocol, Entry-Point-Auswahl, In-Memory-Backend"
```

---

### Task 5: Der FastAPI-Endpoint

Hier laufen alle Regeln zusammen. Die Statuscodes sind nicht Geschmackssache: **jedes** Non-2xx löst beim Sender 12 Retries über 48 h aus.

**Files:**
- Create: `src/edutap/webhook_heidi/handlers/__init__.py`
- Create: `src/edutap/webhook_heidi/handlers/fastapi.py`
- Test: `tests/test_handlers_fastapi.py`
- Modify: `src/edutap/webhook_heidi/__init__.py`, `CHANGES.md`

**Interfaces:**
- Consumes: `Settings` (1), `WebhookEvent`/`QueueMessage`/`WEBHOOK_TEST` (2), `sign`/`verify`/`SIGNATURE_HEADER` (3), `get_queue_backend`/`QueueUnavailable` (4).
- Produces: `router: APIRouter` — mountbar via `app.include_router(router)`.

- [ ] **Step 1: Write the failing test**

`tests/test_handlers_fastapi.py`:

```python
from conftest import TEST_SECRET
from edutap.webhook_heidi.plugins import add_plugin
from edutap.webhook_heidi.plugins import reset_queue_backend
from edutap.webhook_heidi.signing import SIGNATURE_HEADER
from edutap.webhook_heidi.signing import sign
from fastapi import FastAPI
from fastapi.testclient import TestClient
from plugins import FailingQueueBackend

import json
import pytest
import time


PATH = "/webhook/heidi"

EVENT = {
    "id": "evt_9f4c2a1e6b8d4f0eae7cd3a2b1f0c9e8",
    "type": "pass.installed",
    "created": "2026-07-09T12:34:56Z",
    "api_version": "2026-07-09",
    "data": {
        "pass_id": "0c0ffee0-0000-0000-0000-000000000001",
        "person_id": "12345",
        "wallet_type": "APPLE_ACCESS",
        "state": "ACTIVE",
        "reason": "provisioning",
        "confirmation": "device",
    },
}


@pytest.fixture
def client() -> TestClient:
    from edutap.webhook_heidi.handlers.fastapi import router

    app = FastAPI()
    app.include_router(router)
    return TestClient(app)


def _post(client: TestClient, body: bytes, *, secret: str = TEST_SECRET, now: int | None = None):
    timestamp = int(time.time()) if now is None else now
    return client.post(
        PATH,
        content=body,
        headers={
            "Content-Type": "application/json",
            SIGNATURE_HEADER: sign(secret, timestamp, body),
        },
    )


def test_valid_event_is_enqueued(client, memory_backend):
    body = json.dumps(EVENT).encode()
    response = _post(client, body)

    assert response.status_code == 204
    assert len(memory_backend.messages) == 1
    message = memory_backend.messages[0]
    assert message.eventid == EVENT["id"]
    assert message.passid == EVENT["data"]["pass_id"]
    assert message.action == "pass.installed"
    assert message.payload["state"] == "ACTIVE"


def test_retry_with_different_bytes_is_accepted(client, memory_backend):
    """Retries kommen aus einem JSONB-Roundtrip: gleiche Nachricht, andere Bytes."""
    retry_body = json.dumps(EVENT, sort_keys=True, separators=(",", ":")).encode()
    assert retry_body != json.dumps(EVENT).encode()

    assert _post(client, retry_body).status_code == 204
    assert len(memory_backend.messages) == 1


def test_unknown_event_type_is_accepted(client, memory_backend):
    """Unbekannte Typen dürfen NICHT abgelehnt werden — sonst 48 h Retry-Sturm."""
    body = json.dumps({**EVENT, "type": "pass.teleported"}).encode()

    assert _post(client, body).status_code == 204
    assert memory_backend.messages[0].action == "pass.teleported"


def test_missing_signature_header(client, memory_backend):
    response = client.post(PATH, content=json.dumps(EVENT).encode())
    assert response.status_code == 401
    assert memory_backend.messages == []


def test_wrong_secret(client, memory_backend):
    body = json.dumps(EVENT).encode()
    assert _post(client, body, secret="falsch").status_code == 401
    assert memory_backend.messages == []


def test_tampered_body(client, memory_backend):
    body = json.dumps(EVENT).encode()
    timestamp = int(time.time())
    response = client.post(
        PATH,
        content=body.replace(b"12345", b"99999"),
        headers={SIGNATURE_HEADER: sign(TEST_SECRET, timestamp, body)},
    )
    assert response.status_code == 401
    assert memory_backend.messages == []


def test_stale_timestamp(client, memory_backend):
    body = json.dumps(EVENT).encode()
    assert _post(client, body, now=int(time.time()) - 301).status_code == 401
    assert memory_backend.messages == []


def test_malformed_envelope(client, memory_backend):
    """Signatur gültig, aber strukturell kein Envelope -> 400."""
    body = json.dumps({"id": "evt_1"}).encode()
    assert _post(client, body).status_code == 400
    assert memory_backend.messages == []


def test_body_is_not_json(client, memory_backend):
    assert _post(client, b"kein json").status_code == 400
    assert memory_backend.messages == []


def test_webhook_test_is_accepted_but_not_enqueued(client, memory_backend):
    """Konnektivitätstest: 200, aber keine Null-UUID in der Queue."""
    body = json.dumps(
        {
            "id": "evt_test",
            "type": "webhook.test",
            "created": "2026-07-09T12:34:56Z",
            "api_version": "2026-07-09",
            "data": {
                "pass_id": "00000000-0000-0000-0000-000000000000",
                "person_id": "test",
                "wallet_type": "UNSET",
                "state": "NEW",
                "reason": "test",
                "confirmation": "platform",
            },
        }
    ).encode()

    assert _post(client, body).status_code == 200
    assert memory_backend.messages == []


def test_queue_unavailable_yields_503(client):
    """Kein 2xx bei kaputter Queue — sonst ist das Event endgültig verloren."""
    reset_queue_backend()
    add_plugin(FailingQueueBackend)

    assert _post(client, json.dumps(EVENT).encode()).status_code == 503

    reset_queue_backend()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_handlers_fastapi.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'edutap.webhook_heidi.handlers'`

- [ ] **Step 3: Write minimal implementation**

`src/edutap/webhook_heidi/handlers/__init__.py`:

```python
"""HTTP-Handler."""
```

`src/edutap/webhook_heidi/handlers/fastapi.py`:

```python
"""Der Webhook-Endpoint.

Bewusst dünn: Signatur prüfen, Envelope parsen, enqueuen, 2xx. Keine
Geschäftslogik — die gehört in den Consumer.

Die Statuscodes sind Teil des Vertrags: heidi.cloud wertet JEDES Non-2xx als
Fehlschlag und wiederholt bis zu 12x über 48 h (auch bei 4xx). Deshalb wird hier
so lax wie möglich validiert und nur abgelehnt, was wir kryptografisch nicht
verifizieren können (401) oder was strukturell kein Envelope ist (400).
"""

from edutap.webhook_heidi.models import WEBHOOK_TEST
from edutap.webhook_heidi.models import QueueMessage
from edutap.webhook_heidi.models import WebhookEvent
from edutap.webhook_heidi.plugins import get_queue_backend
from edutap.webhook_heidi.protocols import QueueUnavailable
from edutap.webhook_heidi.settings import Settings
from edutap.webhook_heidi.signing import SIGNATURE_HEADER
from edutap.webhook_heidi.signing import verify
from fastapi import APIRouter
from fastapi import HTTPException
from fastapi import Request
from fastapi import Response

import pydantic
import time


settings = Settings()

router = APIRouter(
    prefix=settings.handler_prefix,
    tags=["edutap.webhook_heidi"],
)


@router.post("", status_code=204)
async def handle_pass_event(request: Request) -> Response:
    """Nimmt ein Pass-Event von heidi.cloud entgegen und schreibt es in die Queue."""
    # Raw Bytes, VOR jedem Parsen: Retries tragen dieselbe Nachricht mit anderen
    # Bytes (JSONB-Roundtrip), pro Versuch neu signiert.
    body = await request.body()

    if not verify(
        settings.webhook_secret.get_secret_value(),
        request.headers.get(SIGNATURE_HEADER, ""),
        body,
        now=int(time.time()),
        tolerance_seconds=settings.signature_tolerance_seconds,
    ):
        raise HTTPException(status_code=401, detail="Invalid signature.")

    try:
        event = WebhookEvent.model_validate_json(body)
    except pydantic.ValidationError as exc:
        raise HTTPException(status_code=400, detail="Malformed event envelope.") from exc

    if event.type == WEBHOOK_TEST:
        return Response(status_code=200)

    try:
        await get_queue_backend().enqueue(QueueMessage.from_event(event))
    except QueueUnavailable as exc:
        # Kein 2xx: der Sender wiederholt nur bei Non-2xx.
        raise HTTPException(status_code=503, detail="Queue unavailable.") from exc

    return Response(status_code=204)
```

`src/edutap/webhook_heidi/__init__.py` ersetzen:

```python
"""edutap.webhook_heidi — Webhook-Endpoint und austauschbare Pass-Event-Queue.

Empfänger der Customer-Webhooks von heidi.cloud. Siehe
``docs/superpowers/specs/2026-07-13-webhook-heidi-design.md``.
"""
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_handlers_fastapi.py -v`
Expected: 11 passed

- [ ] **Step 5: Alte Smoke-Test-Datei entfernen**

`tests/test_smoke.py` löschen — sie prüfte nur den Import und ist durch echte Tests ersetzt.

```bash
git rm tests/test_smoke.py
uv run pytest
```
Expected: alle Tests grün, Coverage ≥ 90 %.

- [ ] **Step 6: Lint & commit**

```bash
uvx ruff check --fix . && uvx ruff format .
git add src/edutap/webhook_heidi/ tests/test_handlers_fastapi.py CHANGES.md
git commit -m "feat(handlers): FastAPI-Endpoint mit Signaturprüfung und Enqueue"
```

---

### Task 6: Kafka-Backend

Producer **idempotent mit `acks="all"`** (so macht es heidi.cloud auch) — wir antworten erst 2xx, wenn der Broker den Write bestätigt hat. Consumer mit **manuellem Offset-Commit**, damit `ack()` echte Bedeutung hat.

**Files:**
- Create: `src/edutap/webhook_heidi/queues/kafka.py`
- Test: `tests/test_queues_kafka.py`
- Modify: `pyproject.toml`, `.github/workflows/tests.yaml`, `tests/conftest.py`, `CHANGES.md`

**Interfaces:**
- Consumes: `Settings` (1), `QueueMessage` (2), `QueueUnavailable`/`QueueBackend` (4).
- Produces: `class KafkaQueueBackend` — implementiert `QueueBackend`; Konstruktor `KafkaQueueBackend(settings: Settings | None = None)`.

- [ ] **Step 1: Kafka-Service in die CI und Marker in pytest**

`.github/workflows/tests.yaml` — im Job `test` unter `runs-on:` einfügen:

```yaml
    services:
      kafka:
        image: apache/kafka:latest
        ports:
          - 9092:9092
        env:
          KAFKA_NODE_ID: "1"
          KAFKA_PROCESS_ROLES: "broker,controller"
          KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT"
          KAFKA_LISTENERS: "PLAINTEXT://:9092,CONTROLLER://:9093"
          KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://localhost:9092"
          KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
          KAFKA_CONTROLLER_QUORUM_VOTERS: "1@localhost:9093"
          KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "1"
          KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "1"
          KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: "1"
          KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: "0"
          KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
          CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"
```

Und den Install-Schritt auf das Kafka-Extra erweitern:

```yaml
      - name: Install dependencies
        run: uv venv && uv pip install -e ".[test,kafka]"
```

`pyproject.toml` — im `test`-Extra `aiokafka` **nicht** doppeln; stattdessen unter `[tool.pytest.ini_options]` den Marker registrieren:

```toml
[tool.pytest.ini_options]
minversion = "7.0"
testpaths = ["tests"]
asyncio_mode = "auto"
markers = [
    "kafka: braucht einen laufenden Kafka-Broker (wird ohne Broker übersprungen)",
]
```

`tests/conftest.py` — Fixture ergänzen (Imports oben entsprechend erweitern):

```python
def _broker_reachable(host: str, port: int, timeout: float = 0.5) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout)
        return sock.connect_ex((host, port)) == 0


@pytest.fixture
def kafka_settings() -> Settings:
    """Settings gegen den lokalen Broker. Ohne Broker: Test überspringen.

    In der CI liefert der Service-Container den Broker. Lokal:
    ``docker run -p 9092:9092 apache/kafka:latest``
    """
    if not _broker_reachable("localhost", 9092):
        pytest.skip("Kein Kafka-Broker auf localhost:9092")
    return Settings(
        _env_file=None,
        webhook_secret=TEST_SECRET,
        kafka_topic=f"test.pass-events.{uuid.uuid4().hex[:8]}",
        kafka_consumer_group=f"test-group-{uuid.uuid4().hex[:8]}",
    )
```

(Benötigte Imports in `tests/conftest.py`: `from edutap.webhook_heidi.settings import Settings`, `import socket`, `import uuid`.)

- [ ] **Step 2: Write the failing test**

`tests/test_queues_kafka.py`:

```python
from datetime import datetime
from datetime import timezone
from edutap.webhook_heidi.models import QueueMessage
from edutap.webhook_heidi.protocols import QueueBackend
from edutap.webhook_heidi.protocols import QueueUnavailable
from edutap.webhook_heidi.queues.kafka import KafkaQueueBackend
from edutap.webhook_heidi.settings import Settings

import asyncio
import pytest


pytestmark = pytest.mark.kafka


def _message(eventid: str, passid: str = "p1") -> QueueMessage:
    return QueueMessage(
        eventid=eventid,
        passid=passid,
        personid="x",
        action="pass.installed",
        timestamp=datetime(2026, 7, 9, 12, 34, 56, tzinfo=timezone.utc),
        payload={"state": "ACTIVE", "reason": "provisioning"},
    )


def test_conforms_to_protocol():
    assert issubclass(KafkaQueueBackend, QueueBackend)


async def test_roundtrip(kafka_settings):
    """enqueue -> consume -> ack, verlustfrei."""
    backend = KafkaQueueBackend(kafka_settings)
    try:
        await backend.enqueue(_message("evt_1"))
        await backend.enqueue(_message("evt_2"))

        seen = []
        async for message in backend.consume():
            seen.append(message)
            await backend.ack(message)
            if len(seen) == 2:
                break

        assert [m.eventid for m in seen] == ["evt_1", "evt_2"]
        assert seen[0].payload == {"state": "ACTIVE", "reason": "provisioning"}
        assert seen[0].timestamp.year == 2026
    finally:
        await backend.stop()


async def test_partition_key_is_the_passid(kafka_settings):
    """Reihenfolge je Pass hängt daran: gleicher passid -> gleiche Partition."""
    backend = KafkaQueueBackend(kafka_settings)
    try:
        await backend.enqueue(_message("evt_1", passid="pass-a"))
        async for message in backend.consume():
            break
        assert backend.last_key == b"pass-a"
    finally:
        await backend.stop()


async def test_unreachable_broker_raises_queue_unavailable():
    """Der Endpoint braucht QueueUnavailable, um 503 zu liefern."""
    settings = Settings(
        _env_file=None,
        webhook_secret="s3cret",
        kafka_bootstrap_servers="localhost:1",  # nichts hört hier
        enqueue_timeout=2.0,
    )
    backend = KafkaQueueBackend(settings)
    with pytest.raises(QueueUnavailable):
        await backend.enqueue(_message("evt_1"))
    await backend.stop()


async def test_enqueue_timeout_raises_queue_unavailable(kafka_settings, monkeypatch):
    """Ein hängender Broker darf uns nicht ins 30-s-Timeout des Senders laufen lassen."""
    backend = KafkaQueueBackend(kafka_settings)
    await backend.enqueue(_message("evt_warmup"))  # Producer hochfahren

    async def _hang(*args, **kwargs):
        await asyncio.sleep(10)

    monkeypatch.setattr(backend._producer, "send_and_wait", _hang)
    backend._settings.enqueue_timeout = 0.1

    try:
        with pytest.raises(QueueUnavailable):
            await backend.enqueue(_message("evt_1"))
    finally:
        await backend.stop()
```

- [ ] **Step 3: Run test to verify it fails**

Run: `uv run pytest tests/test_queues_kafka.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'edutap.webhook_heidi.queues.kafka'`
(Ohne lokalen Broker werden die Broker-Tests übersprungen; `test_conforms_to_protocol` und `test_unreachable_broker_raises_queue_unavailable` laufen trotzdem.)

- [ ] **Step 4: Write minimal implementation**

`src/edutap/webhook_heidi/queues/kafka.py`:

```python
"""Kafka-Backend.

Producer idempotent mit ``acks="all"``: wir antworten dem Sender erst dann mit
2xx, wenn der Broker den Write bestätigt hat. Consumer mit manuellem
Offset-Commit — nur so hat ``ack()`` Bedeutung.
"""

from aiokafka import AIOKafkaConsumer
from aiokafka import AIOKafkaProducer
from aiokafka import TopicPartition
from aiokafka.errors import KafkaError
from collections.abc import AsyncIterator
from edutap.webhook_heidi.models import QueueMessage
from edutap.webhook_heidi.protocols import QueueUnavailable
from edutap.webhook_heidi.settings import Settings

import asyncio
import json


class KafkaQueueBackend:
    """Pass-Queue auf Kafka."""

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or Settings()
        self._producer: AIOKafkaProducer | None = None
        self._consumer: AIOKafkaConsumer | None = None
        self._records: dict[str, tuple[str, int, int]] = {}
        self.last_key: bytes | None = None

    def _auth(self) -> dict:
        password = self._settings.kafka_sasl_password
        return {
            "security_protocol": self._settings.kafka_security_protocol,
            "sasl_mechanism": self._settings.kafka_sasl_mechanism,
            "sasl_plain_username": self._settings.kafka_sasl_username,
            "sasl_plain_password": (
                password.get_secret_value() if password is not None else None
            ),
        }

    async def _get_producer(self) -> AIOKafkaProducer:
        if self._producer is None:
            producer = AIOKafkaProducer(
                bootstrap_servers=self._settings.kafka_bootstrap_servers,
                value_serializer=lambda value: json.dumps(value).encode(),
                # Broker dedupliziert producer-seitige Retries; impliziert acks="all".
                enable_idempotence=True,
                acks="all",
                **self._auth(),
            )
            try:
                await producer.start()
            except KafkaError as exc:
                raise QueueUnavailable(str(exc)) from exc
            self._producer = producer
        return self._producer

    async def _get_consumer(self) -> AIOKafkaConsumer:
        if self._consumer is None:
            consumer = AIOKafkaConsumer(
                self._settings.kafka_topic,
                bootstrap_servers=self._settings.kafka_bootstrap_servers,
                group_id=self._settings.kafka_consumer_group,
                value_deserializer=lambda value: json.loads(value),
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                **self._auth(),
            )
            await consumer.start()
            self._consumer = consumer
        return self._consumer

    async def enqueue(self, message: QueueMessage) -> None:
        producer = await self._get_producer()
        key = message.passid.encode()
        try:
            await asyncio.wait_for(
                producer.send_and_wait(
                    self._settings.kafka_topic,
                    value=message.model_dump(mode="json"),
                    key=key,
                ),
                timeout=self._settings.enqueue_timeout,
            )
        except (KafkaError, asyncio.TimeoutError) as exc:
            raise QueueUnavailable(str(exc)) from exc
        self.last_key = key

    async def consume(self) -> AsyncIterator[QueueMessage]:
        consumer = await self._get_consumer()
        async for record in consumer:
            message = QueueMessage.model_validate(record.value)
            self._records[message.eventid] = (
                record.topic,
                record.partition,
                record.offset,
            )
            self.last_key = record.key
            yield message

    async def ack(self, message: QueueMessage) -> None:
        record = self._records.pop(message.eventid, None)
        if record is None or self._consumer is None:
            return
        topic, partition, offset = record
        await self._consumer.commit({TopicPartition(topic, partition): offset + 1})

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None
```

- [ ] **Step 5: Run tests to verify they pass**

Lokal einen Broker starten, sonst werden die Broker-Tests übersprungen:

```bash
docker run -d --name kafka-test -p 9092:9092 apache/kafka:latest
uv pip install -e ".[test,kafka]"
uv run pytest tests/test_queues_kafka.py -v
```
Expected: 5 passed

- [ ] **Step 6: Gesamte Suite + Coverage**

Run: `uv run pytest --cov`
Expected: alle Tests grün, Coverage ≥ 90 %.

- [ ] **Step 7: Lint & commit**

```bash
uvx ruff check --fix . && uvx ruff format .
git add src/edutap/webhook_heidi/queues/kafka.py tests/test_queues_kafka.py \
        tests/conftest.py pyproject.toml .github/workflows/tests.yaml CHANGES.md
git commit -m "feat(kafka): Kafka-Backend mit idempotentem Producer und manuellem Commit"
```

---

### Task 7: Consumer-Dokumentation

Ohne diese Doku weiß LMU nicht, wie der Spooler aufgesetzt wird — und die Dedup-Pflicht wäre eine unsichtbare Falle.

**Files:**
- Modify: `README.md`, `CHANGES.md`

**Interfaces:**
- Consumes: alles Vorherige. Produces: nichts (nur Doku).

- [ ] **Step 1: README ergänzen**

In `README.md` nach dem Abschnitt „Installation" einfügen:

````markdown
## Verwendung

### Webhook einbinden

```python
from edutap.webhook_heidi.handlers.fastapi import router
from fastapi import FastAPI

app = FastAPI()
app.include_router(router)
```

Konfiguration ausschließlich über Umgebungsvariablen (Prefix
`EDUTAP_WEBHOOK_HEIDI_`) bzw. `.env`:

```bash
EDUTAP_WEBHOOK_HEIDI_WEBHOOK_SECRET=<Secret aus der heidi.cloud-Admin-UI>
EDUTAP_WEBHOOK_HEIDI_KAFKA_BOOTSTRAP_SERVERS=kafka:9092
EDUTAP_WEBHOOK_HEIDI_KAFKA_TOPIC=heidi.pass-events
```

Das Kafka-Backend wird per Entry-Point ausgewählt — in der `pyproject.toml` der
Consumer-Anwendung:

```toml
[project.entry-points.'edutap.webhook_heidi.plugins']
QueueBackend = 'edutap.webhook_heidi.queues.kafka:KafkaQueueBackend'
```

### Queue lesen (Spooler)

```python
from edutap.webhook_heidi.plugins import get_queue_backend

backend = get_queue_backend()
async for message in backend.consume():
    if already_seen(message.eventid):   # <- Pflicht, siehe unten
        await backend.ack(message)
        continue
    handle(message)                     # eigene Logik
    await backend.ack(message)
```

> **Deduplizieren ist Pflicht.** heidi.cloud liefert **at-least-once** (bis zu 12
> Versuche über 48 h), und Kafka kann beim Schreiben nicht deduplizieren.
> Dieselbe `eventid` kann also mehrfach ankommen. Die `eventid` mindestens 48 h
> vorhalten — besser 28 Tage, dann sind auch manuelle Redeliveries aus der
> heidi.cloud-Admin-UI abgedeckt.

Reihenfolge: Nachrichten mit derselben `passid` landen in derselben
Kafka-Partition und kommen damit in Sendereihenfolge an. Über Pässe hinweg gibt
es keine Ordnungsgarantie — nach `timestamp` sortieren, nicht nach Ankunftszeit.
````

Im Abschnitt „What this package does" die Zeile zu den Backends korrigieren:
Ausprogrammiert ist **Kafka**; Postgres/Redis sind Platzhalter-Extras.

- [ ] **Step 2: Commit**

```bash
git add README.md CHANGES.md
git commit -m "docs: Verwendung von Webhook und Consumer-API"
```

---

## Nach dem Plan

Offen und bewusst **nicht** Teil dieses Plans (Betriebsentscheidungen, siehe Spec §10):

- Topic- und Consumer-Group-Namen sowie Partitionszahl mit dem LRZ abstimmen.
- Trusted Publisher + GitHub-Environments für den Release (`RELEASE.md`).
- Postgres-/Redis-Backends — erst wenn ein Consumer sie braucht.
