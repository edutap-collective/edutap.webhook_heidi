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
- HMAC-SHA256-Signaturprüfung (`signing.sign`/`signing.verify`, Stripe-Stil,
  `Heidi-Signature`-Header) gegen die rohen Body-Bytes — nicht gegen
  re-serialisiertes JSON, damit erneut signierte Retries mit anderer
  Key-Reihenfolge verifizierbar bleiben.
