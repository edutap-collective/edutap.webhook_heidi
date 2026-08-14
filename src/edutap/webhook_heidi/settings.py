"""Konfiguration. Einzige Config-Quelle des Pakets — alles läuft über Settings."""

from pydantic import model_validator
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
    wenige KB groß; der Endpoint prüft diese Grenze VOR jedem vollständigen
    Puffern des Bodys — wo möglich billig per ``Content-Length``, sonst
    inkrementell beim Streamen —, damit ein unauthentifizierter Absender
    nicht beliebig viel Speicher belegen kann (Memory-DoS).

    Vorsicht bei kleineren Werten als dem Default: heidi.cloud wiederholt
    JEDES Non-2xx (auch 413) bis zu 12x über 48 h. Ist das Limit zu knapp
    gewählt, laufen dadurch legitime, nur etwas größere Events innerhalb
    dieses Fensters endgültig ins Leere und sind danach unwiederbringlich
    verloren. Der Default ist deshalb bewusst großzügig bemessen."""

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

    kafka_ssl_cafile: str | None = None
    """Truststore: die CA, gegen die das Broker-Zertifikat geprüft wird.

    Ohne Angabe nimmt der Kontext die Systemzertifikate. Broker mit einer
    eigenen internen CA — der Regelfall in einem Cluster — brauchen sie."""

    kafka_ssl_certfile: str | None = None
    """Client-Zertifikat für mTLS. Nur zusammen mit ``kafka_ssl_keyfile``.

    Ein Broker mit ``ssl.client.auth=required`` autorisiert je Principal, und
    der Principal IST der CN dieses Zertifikats. Ohne Client-Material kommt
    keine Verbindung zustande."""

    kafka_ssl_keyfile: str | None = None
    """Der private Schlüssel zu ``kafka_ssl_certfile``."""

    @model_validator(mode="after")
    def _client_material_comes_in_pairs(self) -> "Settings":
        """Cert ohne Key (oder umgekehrt) ist eine Fehlkonfiguration.

        Warum das hier scheitern muss und nicht später: der Producer verbindet
        sich erst beim ersten Enqueue. Halbes Client-Material fiele damit nicht
        beim Deploy auf, sondern beim ersten echten Pass-Event — als 503, das
        wie ein Broker-Ausfall aussieht.
        """
        if bool(self.kafka_ssl_certfile) != bool(self.kafka_ssl_keyfile):
            raise ValueError(
                "kafka_ssl_certfile and kafka_ssl_keyfile belong together — "
                "client authentication needs both, and half of the material "
                "would only fail at the first enqueue."
            )
        return self
