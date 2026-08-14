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
    assert settings.max_body_bytes == 1_048_576
    assert settings.enqueue_timeout == 10.0
    assert settings.kafka_topic == "heidi.pass-events"
    assert settings.kafka_consumer_group == "heidi-pass-spooler"
    assert settings.kafka_security_protocol == "PLAINTEXT"


def test_secret_is_not_leaked_in_repr(monkeypatch):
    """SecretStr darf nicht in Logs/Tracebacks landen."""
    monkeypatch.setenv(f"{ENV_PREFIX}WEBHOOK_SECRET", "s3cret")
    assert "s3cret" not in repr(Settings(_env_file=None))


def test_ssl_settings_default_to_none(monkeypatch):
    """Ohne TLS-Angaben bleibt das Backend bei PLAINTEXT — der Default darf
    keinen Truststore erfinden."""
    monkeypatch.setenv(f"{ENV_PREFIX}WEBHOOK_SECRET", "s3cret")
    settings = Settings(_env_file=None)
    assert settings.kafka_ssl_cafile is None
    assert settings.kafka_ssl_certfile is None
    assert settings.kafka_ssl_keyfile is None


def test_ssl_settings_from_env(monkeypatch):
    monkeypatch.setenv(f"{ENV_PREFIX}WEBHOOK_SECRET", "s3cret")
    monkeypatch.setenv(f"{ENV_PREFIX}KAFKA_SECURITY_PROTOCOL", "SSL")
    monkeypatch.setenv(f"{ENV_PREFIX}KAFKA_SSL_CAFILE", "/run/secrets/kafka_ca")
    monkeypatch.setenv(f"{ENV_PREFIX}KAFKA_SSL_CERTFILE", "/run/secrets/cert")
    monkeypatch.setenv(f"{ENV_PREFIX}KAFKA_SSL_KEYFILE", "/run/secrets/key")
    settings = Settings(_env_file=None)
    assert settings.kafka_security_protocol == "SSL"
    assert settings.kafka_ssl_cafile == "/run/secrets/kafka_ca"
    assert settings.kafka_ssl_certfile == "/run/secrets/cert"
    assert settings.kafka_ssl_keyfile == "/run/secrets/key"


def test_certfile_without_keyfile_is_rejected(monkeypatch):
    """Halbes Client-Material ist eine Fehlkonfiguration, kein Grenzfall.

    Der Broker dieses Clusters verlangt Client-Auth; ein Cert ohne Key ergibt
    einen Handshake, der erst beim ersten Enqueue scheitert — also lange nach
    dem Deploy. Deshalb beim Start.
    """
    monkeypatch.setenv(f"{ENV_PREFIX}WEBHOOK_SECRET", "s3cret")
    monkeypatch.setenv(f"{ENV_PREFIX}KAFKA_SSL_CERTFILE", "/run/secrets/cert")
    with pytest.raises(pydantic.ValidationError):
        Settings(_env_file=None)


def test_keyfile_without_certfile_is_rejected(monkeypatch):
    monkeypatch.setenv(f"{ENV_PREFIX}WEBHOOK_SECRET", "s3cret")
    monkeypatch.setenv(f"{ENV_PREFIX}KAFKA_SSL_KEYFILE", "/run/secrets/key")
    with pytest.raises(pydantic.ValidationError):
        Settings(_env_file=None)
