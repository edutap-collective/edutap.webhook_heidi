# CLAUDE.md — edutap.webhook_heidi

Repository-specific rules. They take precedence over the global defaults.

## Language

**English only.** This repository belongs to eduTAP proper, not to any single
institution: README, changelog, documentation, docstrings, code comments, commit
messages, pull request titles and bodies, and replies to review comments.

The language follows the repository, not the conversation. A discussion held in
German still produces English artefacts here.

## What this package is

A webhook endpoint plus a swappable pass-event queue. It is a **library**, not a
service: consumers run the endpoint and read the queue with their own spooler.

## Guard rails

**Three contract rules must survive every refactoring.** They are the ones that turn
into silent bugs on the consumer side, so they are documented in `protocols.py` and
in the README, and they are tested:

* **Deduplication is mandatory.** heidi.cloud delivers at-least-once — up to 12
  attempts over 48 h. Kafka cannot deduplicate on write.
* **`ack()` has to be sequential.** Kafka offset commits are cumulative; acking out
  of order silently commits everything in between.
* **`ack()` needs the identical object `consume()` returned.** The offset is looked
  up by `id(message)`. Acking a copy commits nothing and raises nothing.

**Status codes are contract, not taste.** heidi.cloud retries every non-2xx up to 12
times over 48 h. 204 on enqueue, 200 for `webhook.test`, 401 only for a bad
signature, 400 only for a structurally absent envelope, 503 when the queue is down.
Changing one changes sender behaviour.

**Verify signatures against the raw body bytes**, never against re-serialised JSON —
a re-signed retry may order keys differently.

**Never log a validation error's message.** It embeds the validated value, which
carries `person_id`. Log the error count and locations instead.

## Confidentiality

No vendor internals from Apple or NXP — not in files, not in commit messages. What a
platform's behaviour *means for us* is documentable ("the platform enforces a
deadline, it is self-healing, it is outside our control"); the mechanics, concrete
values and rule sets behind it are not.

Contract and regulatory material is fine and wanted: eduPersonAssurance, GÉANT and
eduGAIN terms.

## Working practice

Branch first, never commit on `main`. Push only when asked. Lint and tests green
before opening a pull request.

Design records under `docs/superpowers/` record a decision at a point in time — do
not rewrite them to match a later state; write a new one.
