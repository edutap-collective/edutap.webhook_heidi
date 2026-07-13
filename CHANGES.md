# Changelog

All notable changes to this project are documented here.

## unreleased

### Features

- Initial project scaffold: CI/CD, packaging, pre-commit, and handoff
  documentation. No functional code yet — see [docs/HANDOFF.md](docs/HANDOFF.md).
- Settings (pydantic-settings) für Webhook-Secret, Signatur-Toleranz und Kafka.
- Datenmodelle `WebhookEvent`/`WebhookEventData` (Envelope, bewusst lax
  validiert, `extra="allow"`) und `QueueMessage` inkl. `from_event()` für die
  Pass-Queue-Nachricht.
