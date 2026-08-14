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

## Usage

### Wiring up the webhook

```python
from edutap.webhook_heidi.handlers.fastapi import router
from fastapi import FastAPI

app = FastAPI()
app.include_router(router)
```

> **Shutting down cleanly.** `QueueBackend.stop()` (`protocols.py`) shuts the
> producer and consumer down properly — with Kafka the consumer then leaves the
> consumer group in an orderly fashion instead of hanging until the session
> timeout and blocking a rebalance. If nobody calls `stop()`, that does not
> happen by itself. For the webhook (the producer side) call it from the FastAPI
> lifespan on shutdown:
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

Configuration is environment-only (prefix `EDUTAP_WEBHOOK_HEIDI_`) or `.env`:

```bash
# Required
EDUTAP_WEBHOOK_HEIDI_WEBHOOK_SECRET=<secret from the heidi.cloud admin UI>

# Optional, defaults live in settings.py
EDUTAP_WEBHOOK_HEIDI_HANDLER_PREFIX=/webhook/heidi
EDUTAP_WEBHOOK_HEIDI_SIGNATURE_TOLERANCE_SECONDS=300
EDUTAP_WEBHOOK_HEIDI_MAX_BODY_BYTES=1048576       # 1 MiB; see the warning below
EDUTAP_WEBHOOK_HEIDI_ENQUEUE_TIMEOUT=10.0         # must stay well below the sender's 30s timeout

# Kafka backend (the [kafka] extra)
EDUTAP_WEBHOOK_HEIDI_KAFKA_BOOTSTRAP_SERVERS=kafka:9092
EDUTAP_WEBHOOK_HEIDI_KAFKA_TOPIC=heidi.pass-events
EDUTAP_WEBHOOK_HEIDI_KAFKA_CONSUMER_GROUP=heidi-pass-spooler
EDUTAP_WEBHOOK_HEIDI_KAFKA_SECURITY_PROTOCOL=PLAINTEXT   # SSL / SASL_SSL / SASL_PLAINTEXT
EDUTAP_WEBHOOK_HEIDI_KAFKA_SASL_MECHANISM=PLAIN           # optional, only with SASL
EDUTAP_WEBHOOK_HEIDI_KAFKA_SASL_USERNAME=<user>            # optional
EDUTAP_WEBHOOK_HEIDI_KAFKA_SASL_PASSWORD=<password>        # optional

# TLS / mTLS — only read when the protocol contains SSL
EDUTAP_WEBHOOK_HEIDI_KAFKA_SSL_CAFILE=/run/secrets/kafka_ca      # the broker's CA
EDUTAP_WEBHOOK_HEIDI_KAFKA_SSL_CERTFILE=/run/secrets/kafka_cert  # client certificate
EDUTAP_WEBHOOK_HEIDI_KAFKA_SSL_KEYFILE=/run/secrets/kafka_key    # its private key
```

### Logging and tracing

The log calls in this package are **structlog** calls, so the library carries
`structlog` and nothing else — a consumer that mounts the router keeps its own
logging setup and this package does not touch it.

The standalone service is the other case: `standalone.py` calls
`install_observability()` from
[`edutap.observability_settings`](https://pypi.org/project/edutap.observability-settings/)
before anything else, which configures structured JSON logging, Sentry and the OTLP
exporter the way every eduTAP service does. It comes with the `[observability]`
extra, which the `Dockerfile` installs:

```bash
pip install "edutap.webhook-heidi[kafka,observability]"
```

The level is `EDUTAP_LOG_LEVEL` — that package's variable, deliberately not a second
one under this package's prefix. `DEBUG` makes the path of an event readable, which
is what commissioning needs and steady state does not:

```
event received       body_bytes=174 signature_header=True
signature verified
envelope parsed      event_id=evt_… event_type=webhook.test pass_id=0000…
connectivity test accepted, deliberately not enqueued   event_id=evt_… status=200
```

> **`person_id` is never logged**, at any level. At a university it resolves to a
> human being — at the LMU it is the student number. Event id, event type and
> `pass_id` are opaque and are logged; the payload is not. A service that does need
> the person in its logs takes `person_label()` from
> `edutap.observability_settings`, which pseudonymises, shows or omits it according
> to `EDUTAP_PERSON_UID_MODE`.

> **mTLS.** A broker configured with `ssl.client.auth=required` authorises per
> principal, and the principal is the **CN of the client certificate** — so that
> CN needs the produce ACL on the topic. `CERTFILE` and `KEYFILE` belong
> together: half of the pair is rejected when the settings are constructed, not
> at the first enqueue. That distinction matters here, because the producer only
> connects when the first event arrives; a configuration error that waits that
> long surfaces as a 503 long after the deployment, looking like a broker
> outage.
>
> `CAFILE` is separate from the client material: it is the truststore the
> broker's own certificate is checked against. A cluster-internal CA has to be
> named here, otherwise verification falls back to the system trust store and
> fails.

> Do not lower `EDUTAP_WEBHOOK_HEIDI_MAX_BODY_BYTES` without a reason:
> heidi.cloud retries EVERY non-2xx (including 413) up to 12 times over 48 h — a
> limit chosen too tightly lets legitimate, merely slightly larger events run out
> of retries and be lost for good.

The Kafka backend is selected through an entry point, declared in the consuming
application's `pyproject.toml`:

```toml
[project.entry-points.'edutap.webhook_heidi.plugins']
QueueBackend = 'edutap.webhook_heidi.queues.kafka:KafkaQueueBackend'
```

### Reading the queue (spooler)

```python
from edutap.webhook_heidi.plugins import get_queue_backend

backend = get_queue_backend()
try:
    async for message in backend.consume():
        if already_seen(message.eventid):  # <- mandatory, see below
            await backend.ack(message)
            continue
        handle(message)  # your own logic
        await backend.ack(message)  # the same `message` object, see below
finally:
    await backend.stop()  # leaves the Kafka consumer group cleanly, see below
```

> **Do not forget `stop()`.** Without `await backend.stop()` when the spooler task
> ends (on shutdown or restart, say), the Kafka consumer does not leave the
> consumer group cleanly and the rebalance stalls until the session timeout.
> `try`/`finally` makes sure `stop()` still runs when `handle()` raises.

> **Deduplication is mandatory.** heidi.cloud delivers **at-least-once** (up to 12
> attempts over 48 h), and Kafka cannot deduplicate on write. The same `eventid`
> can therefore arrive more than once. Keep every `eventid` for at least 48 h —
> better 28 days, which also covers manual redeliveries from the heidi.cloud
> admin UI.

> **`ack()` has to be sequential.** Kafka offset commits are cumulative — there is
> no "commit only this one message". A consumer MUST therefore consume → process →
> ack, and only then take the next message from `consume()`. Batching messages and
> acking out of order (or incompletely) silently commits the offsets in between as
> well; those messages then count as processed although they never were.

> **`ack()` needs the very object `consume()` returned.** The offset is looked up
> by object identity (`id(message)`), not by `eventid` or any other notion of
> equality. Copying the message or passing it through a queue of your own
> (`QueueMessage.model_validate(m.model_dump())`) and acking the copy commits
> **nothing** — without raising. The result is an endless redelivery loop. The
> Kafka backend now logs this case as a `warning`, but do not rely on that: the
> message has to stay referenced until `ack()` is called, not be re-serialised.

### Operations

- **Ordering:** messages carrying the same `passid` land in the same Kafka
  partition and therefore arrive in the order they were sent. Across passes there
  is **no** ordering guarantee — sort by `timestamp`, not by arrival time.
- **Secret rotation:** heidi.cloud rotates the webhook secret without an overlap
  window. On a mismatch the endpoint answers 401 and the sender retries up to 12
  times over 48 h — deploying the new secret inside that window costs no events.
- **HTTPS is mandatory:** the HMAC signature protects the payload from tampering,
  not from being read. The payload carries `person_id` and `pass_id`, so run the
  endpoint over HTTPS only.

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
