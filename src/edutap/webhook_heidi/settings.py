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

    max_body_bytes: int = 1_048_576
    """Obergrenze für den rohen Request-Body (1 MiB). Echte Pass-Events sind
    wenige KB groß; der Endpoint prüft diese Grenze VOR dem Lesen/Puffern des
    Bodys (wo möglich, per ``Content-Length``), damit ein unauthentifizierter
    Absender nicht beliebig viel Speicher belegen kann (Memory-DoS)."""

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
