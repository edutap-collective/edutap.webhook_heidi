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

### Fixes

- `signing.verify`: Nicht-ASCII-Zeichen in einem gefälschten
  `Heidi-Signature`-Header lösten zuvor eine unbehandelte `TypeError`
  in `hmac.compare_digest` aus (Starlette dekodiert Header mit
  latin-1), was aus einer 401 eine 500 machte. Der Vergleich läuft
  jetzt über die dekodierten Digest-Bytes (`bytes.fromhex`) statt über
  Header-Strings; ungültiges Hex ergibt `False`. Zusätzlich: leeres
  Secret wird abgelehnt, und der Zeitstempel muss ein reiner
  Ziffernstring sein (kein `+`-Präfix, keine `_`-Trenner).
