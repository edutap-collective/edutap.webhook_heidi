# Handoff: edutap.webhook_heidi

> **Purpose of this document.** This repository was scaffolded (boilerplate +
> CI/CD only) so that *you*, the next agent working here, can finish setting up
> and then implement the package. This document carries the context you need:
> what this package is, the concrete real-world use-case that motivates it, the
> CI/CD philosophy that was adopted, the house conventions to follow, and the
> open questions still to decide.
>
> **Stand:** 2026-07-09.

---

## 1. What this package is

`edutap.webhook_heidi` is an eduTAP open-source package
([github.com/edutap-eu](https://github.com/edutap-eu/)) that provides **two
tightly-related things**:

1. **`HeidiWebHook`** — a **FastAPI** REST endpoint. It is triggered by
   `heidi.cloud` whenever a wallet pass changes lifecycle state (provisioned,
   suspended, deactivated, …). The endpoint validates the incoming pass event
   and **writes it to the pass-event queue**.

2. **Pass-Queue** — a queue **abstraction with swappable backends**. Planned
   reference implementations: **Postgres · Kafka · Redis**. All backends share a
   **common data format that is still TBD** (see open points).

The package deliberately does **not** contain any consumer-side business logic.
A *consumer* application depends on `edutap.webhook_heidi`, mounts/runs the
webhook, and reads the queue with its own **domain-specific spooler**. This
keeps the package reusable across eduTAP deployments while each institution
keeps its own downstream logic.

```
heidi.cloud ──pass event──▶ HeidiWebHook (FastAPI)
                                  │
                                  ▼
                            Pass-Queue  (backend: Postgres | Kafka | Redis)
                                  │
                                  ▼
                    consumer's own spooler  ──▶  consumer's own store
                    (NOT part of this package)
```

---

## 2. The reference use-case: `lmu_edutap_full_view` (LMU)

The first consumer — and the concrete use-case that motivated splitting this
package out — is **`lmu_edutap_full_view`**, the LMU sync service.

- **Local path:** `~/dev/projects/edutap/heidi/lmu/lmu_edutap_full_view`
- **Key docs to read there:**
  - `README.md` — overview + component table + data model
  - `docs/architecture.md` — architecture, data flows, open points
  - `docs/architecture.excalidraw` — the master architecture diagram
  - `docs/analyse.png` — original whiteboard photo

### How LMU uses this package

`lmu_edutap_full_view` builds a Postgres "source of truth" (`heidi.local`) for
eduTAP pass creation. Two halves:

- **VZD side** (stays in the LMU repo): a VZD/LDAP webhook + spooler fills a
  `HeidiFullView` table (`PK EPPN`, `data JSON`).
- **Pass side** (this is where *we* come in): the LMU repo **references
  `edutap.webhook_heidi` as a dependency**. `heidi.cloud` triggers our
  `HeidiWebHook`, which enqueues the pass event. LMU then runs its own
  **`HeidiWebhookSpooler`** (LMU-specific, stays in the LMU repo) that reads our
  queue and writes a `PassState` table (`PK PassID`, `FK EPPN`, `PassTyp`,
  `State`, `WalletType`), `1:n` to `HeidiFullView`.

So the boundary is:

| Belongs to **this** package (`edutap.webhook_heidi`) | Belongs to the **consumer** (`lmu_edutap_full_view`) |
|---|---|
| `HeidiWebHook` FastAPI endpoint | `HeidiWebhookSpooler` (reads queue → `PassState`) |
| Pass-Queue + Postgres/Kafka/Redis backends | `PassState` table + all VZD/LDAP logic |
| Common pass-event data format | Everything downstream of the queue |

`EPPN := EduPersonPrincipalName : str` is the central person key across the
whole system; pass events will reference it.

---

## 3. CI/CD philosophy (adopted from zodb-pgjsonb)

The boilerplate was intentionally modelled on
[**bluedynamics/zodb-pgjsonb**](https://github.com/bluedynamics/zodb-pgjsonb).
Keep this philosophy as you extend the project:

- **Git tag = version.** `hatch-vcs` derives the version from the git tag;
  there is no hand-maintained version string. The build writes
  `src/edutap/webhook_heidi/_version.py` (gitignored).
- **Reusable workflows.** `ci.yaml` calls `qa.yaml` (ruff check + format) and
  `tests.yaml` (matrix Python 3.10–3.14, `uv`, coverage combine with a
  `fail_under = 90` gate). CI runs on every push to `main` and every PR.
- **`uv` everywhere** in CI (`astral-sh/setup-uv`), with dependency caching.
- **OIDC Trusted Publishing** in `release.yaml` — no API tokens:
  - On **green CI on `main`** → build + publish an in-dev version to **Test
    PyPI** (`release-test-pypi` environment). Continuous installable dev builds.
  - On a **published GitHub Release** → publish to **PyPI**
    (`release-pypi` environment).
  - Uses `hynek/build-and-inspect-python-package` to build once and reuse the
    artifact for both targets.
- **Changelog discipline.** Everything lands under `## unreleased` in
  `CHANGES.md`; a release promotes that heading to the version number.
- **Full process** is documented in [`RELEASE.md`](../RELEASE.md), including the
  one-time Test-PyPI/PyPI trusted-publisher setup and the two GitHub
  environments you must create (`release-test-pypi`, `release-pypi`).

> ⚠️ The reference project (`zodb-pgjsonb`) is a database library and spins up a
> Postgres **service** in `tests.yaml`. This package is a FastAPI service +
> queue; the scaffold's `tests.yaml` has **no DB service**. When you implement
> the Postgres/Kafka/Redis backends, add the matching service containers (or
> `testcontainers`) to the test workflow — mirror how `zodb-pgjsonb` does it.

---

## 4. eduTAP house conventions (keep these)

Derived from sibling packages `edutap.wallet_google` / `edutap.wallet_apple`:

- **Namespace layout:** `src/edutap/webhook_heidi/` (implicit namespace package
  under `edutap`). Wheel packages `["src/edutap"]`.
- **Distribution name:** `edutap.webhook-heidi` (dash); import name
  `edutap.webhook_heidi` (underscore).
- **License:** **EUPL 1.2** (`license = { text = "EUPL 1.2" }`).
- **Authors:** Alexander Loechel, Philipp Auersperg-Castell, Jens Klein,
  Robert Niederreiter (see `pyproject.toml`).
- **Python:** `>=3.10` (matches the other edutap packages).
- **Docs site:** `https://docs.edutap.eu/packages/edutap_webhook_heidi/index.html`
  (not created yet — wire it into the edutap docs aggregator later).
- **Stack for the implementation:** FastAPI + Pydantic v2 + pydantic-settings
  (already the core dependencies), matching the wallet packages' style.

---

## 5. Suggested first implementation steps

Roughly in order (adjust freely):

1. Define the **pass-event data model** (Pydantic) — the common queue format.
   Coordinate with `heidi.cloud` (what it sends) and the LMU `PassState` fields
   (`PassID`, `EPPN`, `PassTyp`, `State`, `WalletType`) as the initial field set.
2. Define a **`QueueBackend` interface** (enqueue / consume / ack) and a config
   surface (`pydantic-settings`) to select a backend.
3. Implement backends behind the extras that already exist in `pyproject.toml`:
   `[postgres]`, `[kafka]`, `[redis]`. Start with **one** (Postgres is the
   natural first, given the LMU consumer).
4. Implement the **`HeidiWebHook`** FastAPI router (validate event → enqueue),
   with auth on the endpoint (open question — see below).
5. Add tests per backend and wire the needed **service containers** into
   `tests.yaml`. Keep coverage ≥ 90%.
6. Provide a **consumer-facing API** (how a spooler reads/acks the queue) so
   `lmu_edutap_full_view` can drop its `HeidiWebhookSpooler` on top.

---

## 6. Open points (TBD)

- **Common queue data format / pass-event schema** — the central decision.
- **Backend selection & config** — env-driven via `pydantic-settings`; which
  backend is the default?
- **Webhook authentication** — how does `heidi.cloud` authenticate to
  `HeidiWebHook` (shared secret / signature / mTLS)?
- **Delivery semantics** — at-least-once vs exactly-once; ack/retry/dead-letter.
- **Which backend ships first** (recommend Postgres, to unblock the LMU
  consumer).
- **Docs integration** into `docs.edutap.eu`.

---

## 7. What is NOT done yet (finish this setup)

- [ ] Create the GitHub repo `edutap-eu/edutap.webhook_heidi` and push.
- [ ] Configure the two GitHub environments + trusted publishers (see
      `RELEASE.md`).
- [ ] `pre-commit install` and a first `uvx ruff format .` pass.
- [ ] Replace the smoke test with real tests as functionality lands.
- [ ] Everything in sections 5 & 6.
