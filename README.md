# edutap.webhook_heidi

Webhook endpoint and swappable **pass-event queue** for the eduTAP / HEIDI
system. Part of the [eduTAP](https://github.com/edutap-eu/) organisation.

> For background on the project (motivation, architecture, house
> conventions, data model, error handling), see the **design spec**:
> **[docs/superpowers/specs/2026-07-13-webhook-heidi-design.md](docs/superpowers/specs/2026-07-13-webhook-heidi-design.md)**.
> `docs/HANDOFF.md` is a historical scaffolding document and is **outdated**
> on several points the spec corrects (see the notice at the top of that
> file) — do not use it as a reference.

## What this package does

- **`HeidiWebHook`** — a FastAPI REST endpoint triggered by `heidi.cloud` when a
  pass is provisioned, suspended, deactivated, etc. It validates the event and
  writes it to the pass-event queue.
- **Pass-Queue** — a queue abstraction with **swappable backends**. **Kafka**
  is implemented (`[kafka]` extra) and is the only backend in v1. Postgres/Redis
  are placeholder extras in `pyproject.toml` only — not implemented; they will
  land once a consumer actually needs them.

Consumers depend on this package, run the webhook, and read the queue with their
own domain-specific spooler. The first consumer and reference use-case is
**`lmu_edutap_full_view`** (LMU) — see the
[design spec](docs/superpowers/specs/2026-07-13-webhook-heidi-design.md).

## Installation

```bash
pip install edutap.webhook-heidi

# with the Kafka queue backend (the only implemented backend in v1):
pip install "edutap.webhook-heidi[kafka]"

# [postgres] / [redis] are placeholder extras only — no backend behind them yet.
```

## Verwendung

### Webhook einbinden

```python
from edutap.webhook_heidi.handlers.fastapi import router
from fastapi import FastAPI

app = FastAPI()
app.include_router(router)
```

> **Sauberes Shutdown.** `QueueBackend.stop()` (`protocols.py`) fährt
> Producer/Consumer sauber herunter — bei Kafka verlässt der Consumer damit
> ordentlich die Consumer-Group, statt bis zum Session-Timeout zu hängen und
> ein Rebalance zu blockieren. Ruft niemand `stop()` auf, passiert das nicht
> automatisch. Für den Webhook (Producer-Seite) deshalb per FastAPI-Lifespan
> beim Shutdown aufrufen:
>
> ```python
> from contextlib import asynccontextmanager
> from edutap.webhook_heidi.handlers.fastapi import router
> from edutap.webhook_heidi.plugins import get_queue_backend
> from fastapi import FastAPI
>
>
> @asynccontextmanager
> async def lifespan(app: FastAPI):
>     yield
>     await get_queue_backend().stop()
>
>
> app = FastAPI(lifespan=lifespan)
> app.include_router(router)
> ```

Konfiguration ausschließlich über Umgebungsvariablen (Prefix
`EDUTAP_WEBHOOK_HEIDI_`) bzw. `.env`:

```bash
# Pflicht
EDUTAP_WEBHOOK_HEIDI_WEBHOOK_SECRET=<Secret aus der heidi.cloud-Admin-UI>

# Optional, mit Defaults aus settings.py
EDUTAP_WEBHOOK_HEIDI_HANDLER_PREFIX=/webhook/heidi
EDUTAP_WEBHOOK_HEIDI_SIGNATURE_TOLERANCE_SECONDS=300
EDUTAP_WEBHOOK_HEIDI_MAX_BODY_BYTES=1048576       # 1 MiB; siehe Warnung unten
EDUTAP_WEBHOOK_HEIDI_ENQUEUE_TIMEOUT=10.0         # muss deutlich unter dem 30s-Sender-Timeout bleiben

# Kafka-Backend (Extra [kafka])
EDUTAP_WEBHOOK_HEIDI_KAFKA_BOOTSTRAP_SERVERS=kafka:9092
EDUTAP_WEBHOOK_HEIDI_KAFKA_TOPIC=heidi.pass-events
EDUTAP_WEBHOOK_HEIDI_KAFKA_CONSUMER_GROUP=heidi-pass-spooler
EDUTAP_WEBHOOK_HEIDI_KAFKA_SECURITY_PROTOCOL=PLAINTEXT   # z.B. SASL_SSL im Produktivbetrieb
EDUTAP_WEBHOOK_HEIDI_KAFKA_SASL_MECHANISM=PLAIN           # optional, nur mit SASL
EDUTAP_WEBHOOK_HEIDI_KAFKA_SASL_USERNAME=<user>            # optional
EDUTAP_WEBHOOK_HEIDI_KAFKA_SASL_PASSWORD=<password>        # optional
```

> `EDUTAP_WEBHOOK_HEIDI_MAX_BODY_BYTES` nicht ohne Grund verkleinern:
> heidi.cloud wiederholt JEDES Non-2xx (auch 413) bis zu 12x über 48 h — ein
> zu knapp gewähltes Limit lässt legitime, nur etwas größere Events innerhalb
> dieses Fensters endgültig ins Leere laufen.

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
try:
    async for message in backend.consume():
        if already_seen(message.eventid):   # <- Pflicht, siehe unten
            await backend.ack(message)
            continue
        handle(message)                     # eigene Logik
        await backend.ack(message)          # dasselbe `message`-Objekt, siehe unten
finally:
    await backend.stop()   # verlässt die Kafka-Consumer-Group sauber, siehe unten
```

> **`stop()` nicht vergessen.** Ohne `await backend.stop()` beim Beenden des
> Spooler-Tasks (z. B. bei Shutdown/Neustart) verlässt der Kafka-Consumer die
> Consumer-Group nicht sauber — das Rebalance hängt dann bis zum
> Session-Timeout. `try`/`finally` stellt sicher, dass `stop()` auch bei
> einer Exception in `handle()` noch läuft.

> **Deduplizieren ist Pflicht.** heidi.cloud liefert **at-least-once** (bis zu
> 12 Versuche über 48 h), und Kafka kann beim Schreiben nicht deduplizieren.
> Dieselbe `eventid` kann also mehrfach ankommen. Die `eventid` mindestens
> 48 h vorhalten — besser 28 Tage, dann sind auch manuelle Redeliveries aus
> der heidi.cloud-Admin-UI abgedeckt.

> **`webhook.test` muss verworfen werden.** Nachrichten mit `action ==
> "webhook.test"` (Konnektivitätstest aus der heidi.cloud-Admin-UI, siehe
> [Design](docs/superpowers/specs/2026-08-14-webhook-test-enqueue-design.md))
> durchlaufen denselben Pfad wie jedes andere Event und landen ebenfalls in
> der Queue — `edutap.webhook_heidi` kann das Verwerfen nicht erzwingen, das
> ist Aufgabe des Consumers. Das Testevent trägt die Null-UUID
> `00000000-0000-0000-0000-000000000000` als `pass_id`; ein Consumer, der es
> wie ein echtes Pass-Event behandelt, versucht einen nicht existierenden
> Pass zu verarbeiten (z. B. Fremdschlüsselverletzung beim Upsert) — und weil
> alle Testevents in derselben Kafka-Partition landen, kann ein daran
> hängenbleibender Consumer diese Partition blockieren und damit echte
> Pass-Events verzögern, nicht nur Testverkehr.

> **`ack()` muss sequenziell erfolgen.** Kafka-Offset-Commits sind kumulativ
> — es gibt kein „committe nur diese eine Nachricht". Ein Consumer MUSS daher
> konsumieren → verarbeiten → acken, erst danach die nächste Nachricht aus
> `consume()` holen. Wer Nachrichten stapelt und außer der Reihe (oder
> unvollständig) ackt, committet dabei stillschweigend auch die
> dazwischenliegenden Offsets mit — diese Nachrichten gelten dann als
> verarbeitet, obwohl sie es nie waren.

> **`ack()` braucht dasselbe Objekt, das `consume()` geliefert hat** — der
> Offset wird intern über die Objekt-Identität (`id(message)`) gefunden,
> nicht über `eventid` oder sonstige Gleichheit. Wer die Nachricht kopiert
> oder z. B. durch eine eigene Queue schickt
> (`QueueMessage.model_validate(m.model_dump())`) und dann die Kopie ackt,
> committet dadurch **nichts** — ohne Exception. Ergebnis ist eine
> Redelivery-Endlosschleife. Das Kafka-Backend loggt diesen Fall inzwischen
> als `warning`, darauf sollte man sich aber nicht verlassen: die Nachricht
> muss bis zum `ack()`-Aufruf referenziert bleiben, nicht re-serialisiert
> werden.

### Betrieb

- **Reihenfolge:** Nachrichten mit derselben `passid` landen in derselben
  Kafka-Partition und kommen damit in Sendereihenfolge an. Über Pässe hinweg
  gibt es **keine** Ordnungsgarantie — nach `timestamp` sortieren, nicht nach
  Ankunftszeit.
- **Secret-Rotation:** heidi.cloud rotiert das Webhook-Secret ohne
  Überlappungsfenster. Bei einem Mismatch antwortet der Endpoint mit 401, der
  Sender wiederholt bis zu 12x über 48 h — ein Deployment mit dem neuen
  Secret innerhalb dieser Frist kostet kein Event.
- **HTTPS ist Pflicht:** Die HMAC-Signatur schützt vor Manipulation des
  Payloads, aber nicht vor Mitlesen. Der Payload enthält `person_id` und
  `pass_id` — den Endpoint deshalb nur über HTTPS betreiben.

## Development

```bash
uv venv && uv pip install -e ".[test,develop]"
uv run pytest
pre-commit install
```

Tooling: **hatchling** + **hatch-vcs** (git tag = version), **ruff**
(lint + format), **pytest** + **coverage**, **uv**-based CI. Release process:
see [RELEASE.md](RELEASE.md).

## License

[EUPL 1.2](LICENSE).
