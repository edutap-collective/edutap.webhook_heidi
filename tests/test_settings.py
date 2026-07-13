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
