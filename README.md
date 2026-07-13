# edutap.webhook_heidi

Webhook endpoint and swappable **pass-event queue** for the eduTAP / HEIDI
system. Part of the [eduTAP](https://github.com/edutap-eu/) organisation.

> ⚠️ **Pre-alpha / scaffold.** This repository currently contains only the
> project boilerplate (CI/CD, packaging, pre-commit) and a handoff document.
> There is no functional code yet. Start at **[docs/HANDOFF.md](docs/HANDOFF.md)**.

## What this package does

- **`HeidiWebHook`** — a FastAPI REST endpoint triggered by `heidi.cloud` when a
  pass is provisioned, suspended, deactivated, etc. It validates the event and
  writes it to the pass-event queue.
- **Pass-Queue** — a queue abstraction with **swappable backends**. Reference
  implementations planned: **Postgres · Kafka · Redis** (common data format
  still TBD).

Consumers depend on this package, run the webhook, and read the queue with their
own domain-specific spooler. The first consumer and reference use-case is
**`lmu_edutap_full_view`** (LMU) — see [docs/HANDOFF.md](docs/HANDOFF.md).

## Installation

```bash
pip install edutap.webhook-heidi

# with a queue backend:
pip install "edutap.webhook-heidi[postgres]"   # or [kafka] / [redis]
```

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
