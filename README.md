# Concurrency Guard Engine

> **SQLite Transactional Ledger, Idempotent Payments & Static Code Sentinel**
> *Engineered by Paul Trafford ([@elura172](https://github.com/elura172)) · Tokyo Relocation Pipeline Candidate*

[![Python](https://img.shields.io/badge/Python-3.11%20%7C%203.12%20%7C%203.13-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Evaluations](https://img.shields.io/badge/Concurrency%20Evals-10%2F10%20Passing%20(100%25)-brightgreen.svg)](#evaluation-benchmark)
[![Domain](https://img.shields.io/badge/Domain-PayPay%20Corporation%20%7C%20Core%20Backend-red.svg)](#the-problem-concurrency-at-paypay-scale)

---

## The Problem: Concurrency at PayPay Scale & AI-First Developer Speed

At nationwide financial scale (processing tens of millions of QR payment transactions per day across Japan), maintaining strict **at-most-once execution** and **zero balance drift** under high concurrency is paramount.

Simultaneously, engineering organizations are mandating **"AI-first development"**, encouraging developers to leverage LLMs, Copilots, and autonomous coding agents. While this accelerates velocity, AI-generated backend code frequently introduces subtle financial race conditions:

1. **Non-Atomic Balance Checks:** Reading wallet balances without pessimistic row-level locks (`SELECT ... FOR UPDATE`), leading to double-spend vulnerabilities under concurrent checkout bursts.
2. **Missing Endpoint Idempotency:** Payment endpoints lacking strict `Idempotency-Key` validation, causing duplicate debits during network retries or double QR code scans.
3. **Thundering Herd Retries:** Naive fixed-interval retry loops (`time.sleep(1)`) overwhelming downstream payment switches.
4. **Unrolled Exceptions:** Catch blocks swallowing database exceptions without issuing explicit rollbacks, leaking dirty session states.

**Concurrency Guard Engine** is a runnable reference prototype for these concerns: a SQLite-backed transactional ledger and a static code sentinel that flags selected concurrency anti-patterns. The sentinel is a local analyzer; wiring it into pull request CI and validating its findings against real repositories remain integration work.

---

## Architectural Overview

```mermaid
flowchart TD
    subgraph Client / Payment Layer
        QR[Mobile QR Code Scan]
        API[Payment REST API]
    end

    subgraph Transaction Core
        IDEM[Persistent Idempotency Key Registry]
        LOCK[SQLite or PostgreSQL Transaction]
        LEDGER[Atomic Mutation & Version Increment]
        AUDIT[Transactional Audit Records]
    end

    subgraph Static Developer Tooling
        PR[Developer Source Code]
        SENTINEL[Concurrency Static Analyzer & Sentinel]
        PATCH[Patch Suggestions]
    end

    QR --> API
    API --> IDEM
    IDEM -->|Fast-Path Replay| API
    IDEM -->|New Transaction| LOCK
    LOCK --> LEDGER
    LEDGER --> AUDIT

    PR --> SENTINEL
    SENTINEL -->|Flag Selected Patterns| PATCH
```

---

## Core Capabilities

### 1. High-Concurrency Transactional Ledger
* **Database Transactions:** Each payment uses a separate SQLite connection and a `BEGIN IMMEDIATE` transaction. The balance check, debit, idempotency record, and audit insert commit or roll back together.
* **SQLite:** Uses `BEGIN IMMEDIATE` and serializes writers database-wide for a dependency-free local run.
* **PostgreSQL:** Pass a PostgreSQL DSN to use `SELECT ... FOR UPDATE` for per-wallet concurrency and a transaction-scoped advisory lock for idempotency keys. Unrelated wallets can proceed independently.
* **Version Counter:** Settled debits increment a wallet version in the same transaction on either backend.
* **Contention Check:** The included 50-thread burst verifies that ten ¥1,000 debits settle from a ¥10,000 wallet and remaining requests are rejected without a negative balance.

### 2. Persistent Idempotency Registry
* **Replay:** Returns the stored prior result on duplicate requests without changing the wallet balance.
* **Tamper Detection:** Automatically rejects requests reusing an existing idempotency key with modified amounts as `CONFLICT`.

### 3. Static Code Sentinel
* **Static Analysis Prototype:** Scans supplied Python source text for selected financial anti-patterns:
  * `PAY-001`: Unguarded balance read (missing `FOR UPDATE`).
  * `PAY-002`: Missing `Idempotency-Key` validation on mutation endpoints.
  * `PAY-003`: Fixed-interval retries lacking exponential backoff and jitter.
  * `PAY-004`: Exception blocks lacking explicit `db.rollback()`.
* **Patch Suggestions:** Emits suggested snippets. These are not applied or syntax-checked as drop-in patches, and the analyzer is not yet integrated with a CI provider.

### Scope and limitations

This is a reference implementation, not a production payment system. The PostgreSQL adapter is still a small single-process application component; it does not supply connection pooling, migrations, API/authentication boundaries, or deployment controls. The audit table is append-only by convention in this application, not protected against privileged database edits. Production use needs broader fault and load testing.

---

## Evaluation Benchmark

The repository includes a 10-scenario canonical benchmark suite testing transaction integrity, contention bursts, and sentinel accuracy:

| Scenario | Objective | Status Detail | Result |
|---|---|---|---|
| `01_PARALLEL_BURST_CONTENTION_RACE` | 50 concurrent worker threads race on ¥10,000 balance | 10 settled / 40 rejected (balance: ¥0) | **PASS** |
| `02_IDEMPOTENT_DUPLICATE_QR_REPLAY` | Duplicate QR scan returns cached result without double debit | Verified prior settlement | **PASS** |
| `03_IDEMPOTENCY_PAYLOAD_TAMPER_CONFLICT` | Idempotency key reused with altered amount rejected | Rejected as CONFLICT | **PASS** |
| `04_INSUFFICIENT_FUNDS_SAFETY_REJECTION` | Overdraft attempt rejected cleanly without balance drift | Status: INSUFFICIENT_FUNDS | **PASS** |
| `05_SENTINEL_UNGUARDED_BALANCE_READ` | Static analyzer checks for missing `FOR UPDATE` lock | Caught Rule PAY-001 | **PASS** |
| `06_SENTINEL_NON_IDEMPOTENT_ENDPOINT` | Static analyzer checks endpoint idempotency header | Caught Rule PAY-002 | **PASS** |
| `07_SENTINEL_THUNDERING_HERD_RETRY` | Static analyzer checks retry loops for backoff and jitter | Caught Rule PAY-003 | **PASS** |
| `08_SENTINEL_MISSING_TRANSACTION_ROLLBACK` | Static analyzer checks exception handling for rollback | Caught Rule PAY-004 | **PASS** |
| `09_SENTINEL_CLEAN_DIFF_APPROVAL` | Static analyzer accepts its clean-code fixture | Status: safe_to_merge=True | **PASS** |
| `10_AUDIT_VERSIONING` | Debit writes wallet version and SHA-256 payload hash to audit row | Verified version and audit record | **PASS** |

**Canonical demo benchmark:** **10 / 10 scenarios passed** in the local SQLite run. This is a functional check, not a production load benchmark.

---

## Quickstart

### Prerequisites
* Python 3.11+
* [`uv`](https://github.com/astral-sh/uv) (recommended) or `pip`
* SQLite (included with Python)
* PostgreSQL 14+ and `uv sync --extra postgres` (optional PostgreSQL backend)

### 1. Run Interactive Terminal Demo
Experience a 50-thread contention burst, a simulated source scan, and the benchmark suite:
```bash
git clone https://github.com/elura172/concurrency-guard-engine.git
cd concurrency-guard-engine
uv run demo.py
```

### 2. Run Test Suite
```bash
uv run pytest -v
```

The engine uses a temporary SQLite database by default. Pass a path to preserve state across engine instances:

```python
from src.engine.ledger import ConcurrencyPaymentEngine

engine = ConcurrencyPaymentEngine("ledger.sqlite3")
```

For PostgreSQL, install the optional driver and pass a DSN:

```bash
uv sync --extra postgres
```

```python
engine = ConcurrencyPaymentEngine("postgresql://user:password@localhost:5432/ledger")
```

PostgreSQL integration coverage runs when `CGE_TEST_POSTGRES_DSN` is set before pytest. Point it at a disposable test database; the test creates uniquely named wallet and audit rows.

---

## Project Structure

```
concurrency-guard-engine/
├── demo.py                   # Rich terminal demo showing live 50-thread burst & sentinel
├── pyproject.toml            # Project specification & uv dependencies
├── README.md                 # System documentation & benchmark scorecard
├── src/
│   ├── models/
│   │   └── types.py          # Wallet schemas, payment transactions, vulnerability rules
│   ├── engine/
│   │   └── ledger.py         # SQLite transactions, idempotency & audit records
│   ├── sentinel/
│   │   └── analyzer.py       # Static code analyzer, concurrency rules & automated patches
│   └── evals/
│       └── harness.py        # 10-scenario canonical evaluation suite
└── tests/
    └── test_concurrency.py   # Unit and integration pytest suite
```

---

## Author & Contact

**Paul Trafford**  
*AI Systems & Backend Infrastructure Engineer*  
* **GitHub:** [github.com/elura172](https://github.com/elura172)  
* **Email:** [paul.trafford@protonmail.com](mailto:paul.trafford@protonmail.com)  
* **Target:** Tokyo Relocation Pipeline / PayPay Core Backend Engineering
