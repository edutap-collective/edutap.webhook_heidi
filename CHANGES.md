# Changelog

All notable changes to this project are documented here.

## unreleased

### Features

- **Structured logging and tracing, wired as in every eduTAP service.** The log calls
  in the library are structlog calls (`structlog` is a core dependency); the standalone
  service calls `install_observability()` from `edutap.observability_settings` before
  anything else, which configures JSON logging, Sentry and the OTLP exporter. That
  package is the new `[observability]` extra, installed by the `Dockerfile` — an extra
  and not a core dependency, because a consumer that only mounts the router should not
  inherit logfire and sentry-sdk along with it.

  The level is `EDUTAP_LOG_LEVEL`, the estate's own variable, deliberately not a second
  one under this package's prefix — the second name is the one that eventually
  disagrees with the one that takes effect.

  On `DEBUG` the path of an event is now readable end to end (`event received` →
  `signature verified` → `envelope parsed` → `enqueueing` → `event enqueued`). Before
  this the whole successful path logged a single line and the `webhook.test` path
  logged nothing at all — which made the first commissioning test look like a lost
  event: the sender saw its 200 while the queue and the log stayed empty, exactly as
  designed and with nothing anywhere saying so. That case now says it in as many words:
  `connectivity test accepted, deliberately not enqueued`.

  **`person_id` is not logged, at any level.** At a university it resolves to a human
  being — at the LMU it is the student number — and DEBUG logging is precisely where
  such a value slips in and from where it reaches every log backend nobody removes it
  from again. Event id, event type and `pass_id` are opaque and are logged; the payload
  is not. A regression test asserts it, and was verified to fail when the field is
  added on purpose. Consumers that do need the person use `person_label()` from
  `edutap.observability_settings`.

- **TLS and mTLS for the Kafka backend** (`Settings.kafka_ssl_cafile` /
  `kafka_ssl_certfile` / `kafka_ssl_keyfile`, `KafkaQueueBackend._ssl_context`).
  Until now the backend could only reach a broker over PLAINTEXT or SASL: it built
  its producer arguments from `security_protocol` and the four SASL fields, and had
  no way to pass a `ssl_context` to aiokafka at all. A broker that listens on
  `SSL://` with `ssl.client.auth=required` — the ordinary shape of a cluster-internal
  Kafka — was therefore unreachable, and because the producer only connects when the
  first event arrives, that surfaced as a 503 on the first real pass event rather
  than at deployment.

  The context is built by aiokafka's own `create_ssl_context` helper, only when the
  protocol actually contains `SSL` (a context handed to a PLAINTEXT listener is not
  merely useless — the handshake fails), and exactly once: it reads PEM files from
  disk, and producer and consumer share the same instance.

  `certfile` and `keyfile` are validated as a pair when the settings are
  constructed. Half of the client material is a misconfiguration, and the deferred
  connect would otherwise turn it into a 503 that looks like a broker outage. Note
  that on a broker with client authentication the **CN of the client certificate is
  the Kafka principal** — that CN needs the produce ACL on the topic; the
  certificate alone does not authorise anything.

  Tested against real TLS material (a small CA plus a client certificate, generated
  in the `ssl_material` fixture) rather than a mocked context: an `ssl.SSLContext`
  reads the files and rejects broken ones, which is the one property worth
  asserting. This adds `cryptography` to the `[test]` extra.
- Settings (pydantic-settings) for the webhook secret, the signature tolerance and
  Kafka.
- Data models `WebhookEvent`/`WebhookEventData` (the envelope, deliberately validated
  loosely, `extra="allow"`) and `QueueMessage` including `from_event()` for the
  pass-queue message.
- HMAC-SHA256 signature verification (`signing.sign`/`signing.verify`, Stripe style,
  `Heidi-Signature` header) against the raw body bytes — not against re-serialised
  JSON, so that re-signed retries with a different key order stay verifiable.
- The `QueueBackend` protocol (`protocols.py`) with `enqueue`/`consume`/`ack`/`stop`
  — both sides of the pass queue in one abstraction, so the consumer (the LMU
  spooler) needs to know neither aiokafka nor offsets. The backend is selected
  through a setuptools entry point (`plugins.py`, group
  `edutap.webhook_heidi.plugins`), as in `edutap.wallet_google` and
  `edutap.wallet_apple`; `get_queue_backend()` returns a cached instance, while
  `add_plugin()`/`reset_queue_backend()` serve tests and embedding.
- `InMemoryQueueBackend` (`queues/memory.py`) for tests and local development
  without a broker; it deliberately does not deduplicate — that is the consumer's
  job.
- `KafkaQueueBackend` (`queues/kafka.py`, extra `[kafka]`) — the production queue
  backend: the webhook endpoint writes pass events into it (`enqueue`), the LMU
  spooler reads them out (`consume`/`ack`). The producer is idempotent with
  `acks="all"` — we only answer heidi.cloud with a 2xx once the broker has confirmed
  the write; a produce call lost before the ack would be an event lost for good,
  since the sender only retries on non-2xx (mirroring `heidi.cloud/kafka/base.py`).
  The partition key is `passid`, not `eventid`: Kafka guarantees ordering only
  within a partition, and that is the only guarantee that, say, a `pass.uninstalled`
  is never processed before its `pass.installed`. The consumer commits offsets
  manually (`enable_auto_commit=False`) so that `ack()` means something. Enqueue
  failures (broker gone, timeout via `Settings.enqueue_timeout`) are always raised as
  `QueueUnavailable`, never as an aiokafka-specific exception — only then does the
  endpoint produce its 503.
- Tests marked `@pytest.mark.kafka` (`tests/test_queues_kafka.py`) run against a real
  local broker (the `kafka_settings` fixture, `tests/conftest.py`) and are skipped
  when no broker is reachable on `localhost:9092`. CI
  (`.github/workflows/tests.yaml`) starts an `apache/kafka` service container on port
  9092 for them and installs the `[test,kafka]` extra.
- FastAPI endpoint (`handlers/fastapi.py`, `router` under `POST {handler_prefix}`)
  for the incoming heidi.cloud webhooks: it verifies the signature against the raw
  body bytes, then parses loosely into `WebhookEvent` and writes to the configured
  queue. Status codes are contract, not taste — heidi.cloud retries every non-2xx up
  to 12 times over 48 h: 204 on a successful enqueue, 200 for `webhook.test`
  (accepted but not enqueued), 401 only for an invalid or missing signature, 400 only
  when there is structurally no envelope, 503 when the queue is unreachable. Unknown
  `type` values are passed through and end in a 204.
- `Settings.max_body_bytes` (default 1 MiB): an upper bound on the raw request body,
  checked before it is buffered.
- Logging in the webhook endpoint (`logging.getLogger(__name__)` in
  `handlers/fastapi.py`): 401 as `warning` (without body or signature value, with a
  hint about a possible secret rotation), 400 as `info` with the reason, 413 as
  `warning` with the size, 503 as `error` with `event.id`, a successful enqueue as
  `debug` with `event.id`.
- OpenAPI documentation of the status codes (`responses={...}` on `@router.post`) for
  200/204/400/401/413/503.
- `README.md`: a new "Usage" section for consumers (LMU's `lmu_edutap_full_view`) —
  wiring up the webhook, a complete environment variable reference (including
  `max_body_bytes` and the Kafka SASL fields), entry-point registration of the Kafka
  backend, and reading and acking the queue in a spooler. It documents explicitly the
  three contract rules from `protocols.py`/`queues/kafka.py` that would otherwise
  become silent bugs on the consumer side: deduplication is mandatory (at-least-once,
  keep for at least 48 h, 28 days recommended), `ack()` has to be sequential
  (cumulative offset commits), and `ack()` needs the very object `consume()` returned
  (identity, not equality — a copy acks into the void). A new "Operations" section
  covers ordering guarantees (per `passid` only, not global), secret rotation without
  an overlap window (a 401 triggers retries, so no events are lost if the deployment
  is timely), and the requirement to use HTTPS (the signature does not protect against
  eavesdropping; the payload carries `person_id`/`pass_id`). The "What this package
  does" section is corrected: Kafka is implemented, Postgres and Redis are placeholder
  extras in `pyproject.toml` only. Every code example was verified against the actual
  import paths, function names and environment variable names.
- The standalone deployment shape is published as a container image
  (`.github/workflows/docker-publish.yml`, image
  `ghcr.io/<owner>/edutap.webhook_heidi`). `release.yaml` publishes the package to
  PyPI, which serves the consumers that embed the router — it does nothing for the
  deployment that runs the webhook on its own, and a deployment pulling the `:latest`
  tag had nothing to pull. The workflow mirrors the one in the sibling services
  (`edutap.data_provider`, `edutap.pass_builder`) with one addition they do not need:
  `.git` is excluded from the build context, so hatch-vcs cannot derive a version
  inside the image build. The version is therefore derived in the workflow, where the
  checkout still has its tags, and handed to the build as
  `SETUPTOOLS_SCM_PRETEND_VERSION` — without it the image would report the
  `0.0.0.dev0` fallback of the `Dockerfile`.

### Fixes

- **The `[observability]` extra narrowed the package's own Python support window.**
  `edutap.observability_settings`, and `edutap.data_models` under it, require Python
  >=3.13 while this package supports >=3.10 like its sibling wallet packages. An
  unmarked extra is resolved across the whole supported range, so the CI went red at
  dependency resolution — before a single test ran — on 3.10, 3.11 and 3.12.

  Fixed with an environment marker rather than by raising `requires-python`: the
  library runs on 3.10+ perfectly well, since structlog carries its log calls and
  nothing in it needs the observability stack. Only the standalone service does, and
  its image is `python:3.13-slim`. `tests/test_standalone.py` skips where the module is
  legitimately absent — a skip and not an xfail, because nothing is missing that ought
  to be there on those versions.

- **Final review — the entry-point path was untested (IMPORTANT 1):** until this fix
  no test suite ever wired a backend in through the real
  `importlib.metadata.entry_points()` mechanism — all of them used `add_plugin()`. A
  typo or rename in `plugins.ENTRY_POINT_GROUP`/`plugins.PLUGIN_NAME` therefore passed
  CI green (verified: both constants set to nonsense on purpose, all 90 existing tests
  stayed green). New: an `entrypoints_testing` fixture (`tests/conftest.py`, modelled
  on `edutap.wallet_apple/tests/conftest.py`) that monkeypatches `entry_points` both in
  `importlib.metadata` and in the already imported `plugins` module, plus
  `EntryPointQueueBackend` (`tests/plugins.py`) and
  `test_backend_loaded_via_real_entry_point_mechanism` (`tests/test_plugins.py`),
  which exercises `get_queue_backend()` exclusively through the real entry-point load
  path. Verified: with the same twisted constants exactly this one test now fails.
- **Final review — a trailing slash lost events (IMPORTANT 2):** `POST
  {handler_prefix}/` (with the slash) returned `307` (Starlette's `redirect_slashes`).
  heidi.cloud does not follow redirects (the httpx default) and counts only 2xx as
  success — a 307 would have triggered 12 retries over 48 h and then lost the event for
  good, without our code ever logging anything (the redirect happens before the
  handler). `handlers/fastapi.py` now registers the route with the slash as well
  (`@router.post("/", status_code=204, include_in_schema=False)`), so both forms work
  directly (`204`) regardless of how the consumer configures its
  `FastAPI(redirect_slashes=...)`. The OpenAPI documentation still shows a single entry
  (`include_in_schema=False` on the slash variant). New test
  `test_trailing_slash_is_accepted_directly` (`tests/test_handlers_fastapi.py`), red
  without the fix (307 instead of 204).
- **Final review — no seam tests from endpoint to Kafka (MINOR 6):** every test so far
  covered only its own layer (the handler only against the in-memory backend, the Kafka
  backend only directly). New: `tests/test_integration_kafka.py`
  (`@pytest.mark.kafka`), which walks the real path — ASGI POST (a signed event) →
  `router` → `KafkaQueueBackend` as the registered plugin → a real broker →
  `consume()`/`ack()`. It checks that the `payload` including nested fields
  (`preset`/`device`) arrives unchanged, that `timestamp` is correct, and that ordering
  per `passid` is preserved. It deliberately uses `httpx.AsyncClient`/`ASGITransport`
  rather than `fastapi.testclient.TestClient`: outside a `with TestClient(...) as
  client:` block, `TestClient` opens a new short-lived `anyio` portal thread with its
  own event loop for every `.post()` call, and the lazily created aiokafka producer
  binds to it — every later access (a second request, `consume()`/`stop()` in the test)
  then ran on an already closed loop (`RuntimeError: Event loop is closed` at teardown,
  a timeout on the second enqueue). `httpx.AsyncClient` instead performs the ASGI call
  inside the running pytest-asyncio loop.
- **Final review — the CI matrix collapsed onto one interpreter (MINOR 7):**
  `.github/workflows/tests.yaml` called `uv venv` without `--python` — which only
  happened to be right because each runner had exactly one managed interpreter. Fixed
  to `uv venv --python ${{ matrix.python-version }}`.
- **Final review — the README pointed at the outdated HANDOFF (MINOR 3):**
  `README.md` linked twice to `docs/HANDOFF.md`, which is wrong on three points that
  matter for LMU (there is no EPPN as the person key, only `person_id`; it recommends
  Postgres rather than the decided Kafka; it lists wrong `state` values). The README now
  points at the spec (`docs/superpowers/specs/2026-07-13-webhook-heidi-design.md`)
  instead, and `docs/HANDOFF.md` now carries a notice at the top saying it is historical
  and superseded, naming those three errors.
- **Final review — the installation example advertised unimplemented extras
  (MINOR 4):** `README.md` suggested `pip install "edutap.webhook-heidi[postgres]"`
  among others, although only `[kafka]` is implemented. The installation example now
  shows `[kafka]`, with Postgres and Redis marked explicitly as placeholders. In
  `pyproject.toml` the stale comment on the extras ("common data format still TBD. See
  docs/HANDOFF.md") is replaced by a description of the current state (Kafka decided and
  implemented, Postgres/Redis placeholders).
- **Final review — `stop()` was never called or documented (MINOR 5):**
  `protocols.py` declares `stop()` and both backends implement it, but neither the
  README spooler example nor any lifespan hint ever called it — with the consequence
  that the Kafka consumer does not leave the consumer group cleanly and the rebalance
  stalls until the session timeout. README: the spooler example gained
  `try`/`finally: await backend.stop()`, and a new FastAPI lifespan code block calls
  `await get_queue_backend().stop()` on shutdown.
- `tests/conftest.py` (the `kafka_settings` fixture): the previous broker check was a
  plain TCP connect against `localhost:9092` — on developer machines with a port
  forward on exactly that port (VS Code, for instance) it wrongly reported "broker
  present", and the tests then started only to fail with `KafkaConnectionError` (no
  real Kafka protocol behind it). The check now starts a real `AIOKafkaProducer` as a
  probe (2.5 s timeout) instead of merely checking the port. The address is
  configurable through `EDUTAP_WEBHOOK_HEIDI_KAFKA_BOOTSTRAP_SERVERS` (still defaulting
  to `localhost:9092`) so one can move to a free port locally without changing code.
  New: `EDUTAP_WEBHOOK_HEIDI_TEST_REQUIRE_KAFKA=1` switches "no broker -> skip" to
  "no broker -> `pytest.fail`" — set in `.github/workflows/tests.yaml` for the test job
  so CI fails hard if the Kafka service container is unreachable for any reason, rather
  than staying quietly (and possibly permanently) green.
- `tests/test_queues_kafka_unit.py` (new): unit tests for `KafkaQueueBackend` against
  fake producers and consumers, without a broker dependency — covering
  `_get_producer`/`_get_consumer` (including caching), `enqueue` (success and timeout),
  the `consume`/`ack` round trip and `stop()`, all of which were previously reachable
  only through the `@pytest.mark.kafka` tests and therefore only with a running broker.
  The reason: `queues/kafka.py` is the only production queue backend and has to be
  measured in full regardless of whether a broker happens to run — otherwise coverage
  locally (without a broker) falls below the 90 % gate although CI (with a broker) would
  be green. The existing `@pytest.mark.kafka` integration tests remain unchanged and
  were verified against a real local broker (a lossless round trip including
  `timestamp`/`payload`, `passid` as the partition key, `QueueUnavailable` both for an
  unreachable broker and for an enqueue timeout).
- `signing.verify`: non-ASCII characters in a forged `Heidi-Signature` header
  previously raised an unhandled `TypeError` inside `hmac.compare_digest` (Starlette
  decodes headers as latin-1), turning a 401 into a 500. The comparison now runs over
  the decoded digest bytes (`bytes.fromhex`) rather than header strings; invalid hex
  yields `False`. In addition an empty secret is rejected, and the timestamp has to be
  a pure digit string (no `+` prefix, no `_` separators).
- `handlers/fastapi.py`: two paths previously led to a 500 rather than a 503 — no
  backend registered (`get_queue_backend()` raises `NotImplementedError`) and a backend
  that raises something other than `QueueUnavailable` (`ConnectionResetError`,
  `asyncio.TimeoutError`). The enqueue path now catches `Exception` broadly and always
  answers 503 — a 500 would be identical to a 503 for heidi.cloud (both non-2xx, both
  triggering up to 12 retries over 48 h) but produces needless stack traces and alerts.
  In addition the backend is resolved on module import as a probe (fail fast in
  production, where the entry point is statically present) without breaking the import
  if it is still missing there.
- `handlers/fastapi.py`: `await request.body()` buffers the whole body in memory before
  anything has been checked — an unauthenticated memory DoS. The endpoint now checks
  `Content-Length` against `settings.max_body_bytes` before reading the body (413
  without reading); if the header is missing or wrong, the length is checked after
  reading (413 when exceeded).
- `handlers/fastapi.py`: the `Content-Length` pre-check does not help when the header
  is absent (chunked transfer encoding without it, for instance) — `await
  request.body()` then still buffers without limit before the size check applies. The
  body is now read incrementally through a new `_read_body_limited()` using
  `request.stream()` and aborts with a 413 the moment `max_body_bytes` is exceeded,
  rather than buffering the rest.
- `handlers/fastapi.py`: the 400 log on envelope parsing logged `str(exc)` of a pydantic
  `ValidationError` — which embeds the validated input value (the matriculation number
  in `person_id` at LMU, for example) and thus ended up at INFO level unintentionally.
  Only `exc.error_count()` and the error locations (`e["loc"]` per error) are logged
  now, never a value.
- `handlers/fastapi.py`: `logger.error(...)` in the generic enqueue failure path
  replaced by `logger.exception(...)` so the traceback survives.
  `QueueMessage.from_event(event)` moved out of the `try` block, so that a model error
  there is not misreported as a 503 rather than the bug it is. The import-time backend
  resolution now logs a `warning` when no backend can be resolved at import, instead of
  swallowing it silently.
- `queues/kafka.py`: five review findings fixed, each of them evidenced and first
  reproduced by a failing test:
  - **Coverage theatre around the core promise**: the fake producers and consumers in
    `tests/test_queues_kafka_unit.py` swallowed the constructor kwargs without checking
    them — a mutation removing or inverting
    `enable_idempotence`/`acks="all"`/`enable_auto_commit` stayed green at 100 %
    coverage. New tests (`test_get_producer_is_idempotent_with_acks_all`,
    `test_get_consumer_uses_manual_commit_and_group_id`) now assert exactly those
    values; verified by twisting the values on purpose and watching the suite go red.
  - **Silent data loss on duplicate `eventid`s**: `_records` was keyed by
    `message.eventid`, although duplicates are expected by design (heidi.cloud delivers
    at-least-once, which is why the consumer deduplicates and we do not). Two messages
    with the same `eventid` at different offsets overwrote each other; acking the first
    copy then committed the offset of the second as well and left the messages in
    between unprocessed for ever. Reproduced against a real broker (offsets 0 and 3 with
    the same `eventid`, 1 and 2 lost in between) and rebuilt as a unit test
    (`test_ack_commits_only_up_to_its_own_offset_with_duplicate_eventids`). Fix:
    `_records` is now keyed by `id(message)` rather than `eventid`.
    `QueueBackend.ack()` (`protocols.py`) now documents explicitly that Kafka commits
    are cumulative and that consumers therefore have to work sequentially (consume →
    process → ack, and only then fetch the next message).
  - **A race on cold start**: `_get_producer()`/`_get_consumer()` checked `if
    self._producer is None`, then yielded control at `await ...start()` and assigned
    only afterwards — several concurrent `enqueue()` calls on a cold backend therefore
    each created their own producer that was never stopped (measured: 5 concurrent calls
    → 5 producers, 4 of them orphaned). Fix: an `asyncio.Lock` per getter with
    double-checked locking.
  - **`enqueue_timeout` did not cover the producer start**: `wait_for` only wrapped
    `send_and_wait` while `_get_producer()` ran ahead of it unbounded — against a
    listener that accepts TCP but never answers, `QueueUnavailable` was raised after
    40 s rather than the configured 2 s. Fix: producer start and send now run inside one
    `wait_for`.
  - **An empty error message on timeout**: `str(asyncio.TimeoutError())` is `""`, which
    made `QueueUnavailable(str(exc))` say nothing. It now raises `f"Enqueue timeout
    after {timeout}s"` (or, for a `KafkaError`, the exception class name with its
    message), each with `from exc`.
- `tests/conftest.py`: the Kafka broker probe was a single 2.5 s attempt without retry,
  and the CI service container in `.github/workflows/tests.yaml` has no health check of
  its own. With `EDUTAP_WEBHOOK_HEIDI_TEST_REQUIRE_KAFKA=1` a broker still starting up
  during the first test run could therefore cause a hard, flaky CI failure. New:
  `_wait_for_kafka_broker()` polls for up to 30 s in 2 s steps, but only on the
  `TEST_REQUIRE_KAFKA` path — locally, without a deliberately running broker, the fast
  single attempt remains so the skip notice is not delayed artificially.
- `queues/kafka.py`: a regression from the previous fix (`enqueue_timeout` now wrapping
  the producer start) fixed — `asyncio.wait_for` aborts `_get_producer()` with a
  `CancelledError` in the middle of `await producer.start()`. The local `producer`
  variable was lost in the process and `self._producer` stayed `None`: the half-started
  producer (an open socket plus background tasks) was then unreachable for anyone,
  including `stop()`. Measured against a hanging broker: a linear leak, one open socket
  per aborted `enqueue()`, never released. Fix: `_get_producer()` now also catches
  `BaseException` (explicitly including `CancelledError`, which no longer inherits from
  `Exception` as of Python 3.8) and stops the half-started producer before re-raising.
  `_get_consumer()` gets the same fix (same double-checked locking pattern, same risk of
  being cancelled, for example while the spooler task shuts down). Reproduced in
  `test_repeated_enqueue_timeout_during_producer_start_does_not_leak` (5 `enqueue()`
  calls against a hanging fake producer, red without the fix: the socket counter grew
  linearly) and `test_get_consumer_stops_half_started_consumer_on_cancel`.
- `protocols.py`/`queues/kafka.py`: the identity rule for `ack()` (`id(message)`
  requires the very object `consume()` returned) lived only in the docstring of
  `kafka.py`, not in the protocol — the actual contract LMU programs against. It is now
  documented in `QueueBackend.ack()` (`protocols.py`) as well. In addition, acking an
  unknown message object (because the caller copied or recreated the message rather than
  acking the original) used to be swallowed silently — no commit, no exception, no log,
  an endless redelivery loop without any hint. `ack()` now logs that case as a `warning`
  (module logger). The test was renamed (`test_ack_unknown_eventid_is_a_noop` →
  `test_ack_unknown_message_warns_and_does_not_commit`) and extended with a `caplog`
  assertion.
