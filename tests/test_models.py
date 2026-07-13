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
        "preset": {
            "uuid": "0c0ffee0-0000-0000-0000-000000000003",
            "title": "Student ID",
        },
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
    payload = {
        **INSTALLED,
        "data": {**INSTALLED["data"], "reason": "brandneu", "future": 42},
    }
    event = WebhookEvent.model_validate(payload)
    assert event.data.reason == "brandneu"


def test_empty_error_category_is_accepted():
    """Apple-Access-Fehler liefern category="" (leerer String, nicht null)."""
    payload = {
        **INSTALLED,
        "data": {**INSTALLED["data"], "error": {"category": "", "message": ""}},
    }
    assert WebhookEvent.model_validate(payload).data.error == {
        "category": "",
        "message": "",
    }


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
