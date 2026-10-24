"""
Unit & Integration Test Suite for Concurrency Guard Engine & Sentinel.
"""
import pytest
import concurrent.futures
import sqlite3
import os
import time
import uuid
from src.models.types import PaymentRequest, TransactionStatus, VulnerabilityType
from src.engine.ledger import ConcurrencyPaymentEngine
from src.sentinel.analyzer import ConcurrencySentinel


@pytest.fixture
def engine():
    return ConcurrencyPaymentEngine()


@pytest.fixture
def sentinel():
    return ConcurrencySentinel()


def test_wallet_creation_and_balance(engine):
    wallet = engine.create_wallet("w-1", "user-1", 5000)
    assert wallet.wallet_id == "w-1"
    assert wallet.balance_jpy == 5000
    assert wallet.version == 0


def test_atomic_payment_success(engine):
    engine.create_wallet("w-2", "user-2", 10000)
    req = PaymentRequest(
        transaction_id="tx-01",
        idempotency_key="key-01",
        payer_wallet_id="w-2",
        merchant_id="m-1",
        amount_jpy=3500,
    )
    res = engine.process_payment(req)
    assert res.status == TransactionStatus.SETTLED
    assert res.balance_after_jpy == 6500
    assert engine.get_wallet("w-2").version == 1


def test_insufficient_funds_rejection(engine):
    engine.create_wallet("w-3", "user-3", 1000)
    req = PaymentRequest(
        transaction_id="tx-02",
        idempotency_key="key-02",
        payer_wallet_id="w-3",
        merchant_id="m-1",
        amount_jpy=1500,
    )
    res = engine.process_payment(req)
    assert res.status == TransactionStatus.INSUFFICIENT_FUNDS
    assert res.balance_after_jpy == 1000


def test_idempotent_replay(engine):
    engine.create_wallet("w-4", "user-4", 8000)
    req = PaymentRequest(
        transaction_id="tx-03",
        idempotency_key="key-03",
        payer_wallet_id="w-4",
        merchant_id="m-1",
        amount_jpy=2000,
    )
    res1 = engine.process_payment(req)
    assert res1.status == TransactionStatus.SETTLED
    assert res1.balance_after_jpy == 6000

    # Repeat with same key
    res2 = engine.process_payment(req)
    assert res2.status == TransactionStatus.REPLAY_IDEMPOTENT
    assert res2.balance_after_jpy == 6000
    assert engine.get_wallet("w-4").balance_jpy == 6000


def test_idempotent_conflict(engine):
    engine.create_wallet("w-5", "user-5", 5000)
    req1 = PaymentRequest(
        transaction_id="tx-04",
        idempotency_key="key-04",
        payer_wallet_id="w-5",
        merchant_id="m-1",
        amount_jpy=1000,
    )
    engine.process_payment(req1)

    req2 = PaymentRequest(
        transaction_id="tx-04-tampered",
        idempotency_key="key-04",
        payer_wallet_id="w-5",
        merchant_id="m-1",
        amount_jpy=2500, # Altered amount!
    )
    res2 = engine.process_payment(req2)
    assert res2.status == TransactionStatus.CONFLICT


def test_concurrency_race_condition_burst(engine):
    engine.create_wallet("w-race", "user-race", 10000)
    burst = engine.execute_contention_burst("w-race", total_requests=50, amount_per_request=1000)
    assert burst["settled_count"] == 10
    assert burst["insufficient_funds_count"] == 40
    assert burst["final_balance_jpy"] == 0
    assert burst["final_wallet_version"] == 10


def test_sqlite_wallet_and_idempotency_survive_engine_restart(tmp_path):
    db_path = tmp_path / "ledger.sqlite3"
    first_engine = ConcurrencyPaymentEngine(str(db_path))
    first_engine.create_wallet("w-durable", "user-durable", 5000)
    req = PaymentRequest(
        transaction_id="tx-durable",
        idempotency_key="key-durable",
        payer_wallet_id="w-durable",
        merchant_id="m-1",
        amount_jpy=1200,
    )
    assert first_engine.process_payment(req).status == TransactionStatus.SETTLED

    restarted_engine = ConcurrencyPaymentEngine(str(db_path))
    assert restarted_engine.get_wallet("w-durable").balance_jpy == 3800
    replay = restarted_engine.process_payment(req)
    assert replay.status == TransactionStatus.REPLAY_IDEMPOTENT
    assert restarted_engine.get_wallet("w-durable").version == 1
    assert len(restarted_engine._audit_ledger) == 1


def test_concurrent_retries_with_same_key_debit_once(engine):
    engine.create_wallet("w-same-key", "user-same-key", 5000)
    req = PaymentRequest(
        transaction_id="tx-same-key",
        idempotency_key="key-same-key",
        payer_wallet_id="w-same-key",
        merchant_id="m-1",
        amount_jpy=700,
    )
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
        results = list(pool.map(lambda _: engine.process_payment(req), range(40)))

    assert sum(r.status == TransactionStatus.SETTLED for r in results) == 1
    assert sum(r.status == TransactionStatus.REPLAY_IDEMPOTENT for r in results) == 39
    assert engine.get_wallet("w-same-key").balance_jpy == 4300
    assert engine.get_wallet("w-same-key").version == 1
    assert len(engine._audit_ledger) == 1


def test_audit_insert_failure_rolls_back_debit_and_idempotency(engine):
    engine.create_wallet("w-rollback", "user-rollback", 5000)
    first = PaymentRequest(
        transaction_id="tx-unique-audit",
        idempotency_key="key-first",
        payer_wallet_id="w-rollback",
        merchant_id="m-1",
        amount_jpy=600,
    )
    second = first.model_copy(update={"idempotency_key": "key-second"})
    engine.process_payment(first)

    # Duplicate tx IDs violate the audit table's UNIQUE constraint after the
    # wallet update and idempotency insert have run inside the transaction.
    with pytest.raises(sqlite3.IntegrityError):
        engine.process_payment(second)
    assert engine.get_wallet("w-rollback").balance_jpy == 4400
    assert engine.get_wallet("w-rollback").version == 1
    assert len(engine._audit_ledger) == 1
    retry_after_rollback = second.model_copy(update={"transaction_id": "tx-after-rollback"})
    assert engine.process_payment(retry_after_rollback).status == TransactionStatus.SETTLED
    assert engine.get_wallet("w-rollback").balance_jpy == 3800
    assert engine.get_wallet("w-rollback").version == 2
    assert len(engine._audit_ledger) == 2


def test_postgresql_parallel_contention_and_idempotency():
    dsn = os.getenv("CGE_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("set CGE_TEST_POSTGRES_DSN to run PostgreSQL integration coverage")
    engine = ConcurrencyPaymentEngine(dsn)
    unique = uuid.uuid4().hex
    engine.create_wallet(f"w-pg-idem-{unique}", "user-pg", 5000)
    same_request = PaymentRequest(
        transaction_id=f"tx-pg-idem-{unique}",
        idempotency_key=f"key-pg-idem-{unique}",
        payer_wallet_id=f"w-pg-idem-{unique}",
        merchant_id="m-1",
        amount_jpy=700,
    )
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
        duplicate_results = list(pool.map(lambda _: engine.process_payment(same_request), range(40)))
    assert sum(r.status == TransactionStatus.SETTLED for r in duplicate_results) == 1
    assert sum(r.status == TransactionStatus.REPLAY_IDEMPOTENT for r in duplicate_results) == 39
    assert engine.get_wallet(same_request.payer_wallet_id).balance_jpy == 4300
    assert engine.get_wallet(same_request.payer_wallet_id).version == 1
    restarted_engine = ConcurrencyPaymentEngine(dsn)
    assert restarted_engine.process_payment(same_request).status == TransactionStatus.REPLAY_IDEMPOTENT
    assert restarted_engine.get_wallet(same_request.payer_wallet_id).balance_jpy == 4300

    race_wallet = f"w-pg-race-{unique}"
    engine.create_wallet(race_wallet, "user-pg", 10000)
    burst = engine.execute_contention_burst(race_wallet, total_requests=50, amount_per_request=1000)
    assert burst["settled_count"] == 10
    assert burst["insufficient_funds_count"] == 40
    assert burst["final_balance_jpy"] == 0
    assert burst["final_wallet_version"] == 10


def test_postgresql_locks_only_the_wallet_being_debited():
    dsn = os.getenv("CGE_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("set CGE_TEST_POSTGRES_DSN to run PostgreSQL integration coverage")
    engine = ConcurrencyPaymentEngine(dsn)
    unique = uuid.uuid4().hex
    locked_wallet = f"w-pg-locked-{unique}"
    free_wallet = f"w-pg-free-{unique}"
    engine.create_wallet(locked_wallet, "user-pg", 2000)
    engine.create_wallet(free_wallet, "user-pg", 2000)
    blocker = engine._connect()
    try:
        engine._begin(blocker)
        engine._execute(
            blocker,
            "SELECT wallet_id FROM wallets WHERE wallet_id = ? FOR UPDATE",
            (locked_wallet,),
        )
        make_request = lambda wallet, key: PaymentRequest(
            transaction_id=f"tx-{key}",
            idempotency_key=key,
            payer_wallet_id=wallet,
            merchant_id="m-1",
            amount_jpy=500,
        )
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            unrelated = pool.submit(engine.process_payment, make_request(free_wallet, f"key-free-{unique}"))
            assert unrelated.result(timeout=3).status == TransactionStatus.SETTLED

            blocked = pool.submit(engine.process_payment, make_request(locked_wallet, f"key-locked-{unique}"))
            time.sleep(0.1)
            assert not blocked.done(), "debit should wait while another transaction holds this wallet row lock"
            engine._commit(blocker)
            assert blocked.result(timeout=3).status == TransactionStatus.SETTLED
    finally:
        blocker.close()


def test_sentinel_catches_unguarded_balance_read(sentinel):
    code = "wallet = db.execute('SELECT balance FROM wallets WHERE id = 1')"
    report = sentinel.scan_code(code)
    assert not report.safe_to_merge
    assert any(v.vulnerability_type == VulnerabilityType.UNGUARDED_BALANCE_MUTATION for v in report.vulnerabilities)


def test_sentinel_catches_non_idempotent_endpoint(sentinel):
    code = "@app.post('/api/v1/payments/charge')\ndef handler(): pass"
    report = sentinel.scan_code(code)
    assert any(v.vulnerability_type == VulnerabilityType.NON_IDEMPOTENT_ENDPOINT for v in report.vulnerabilities)


def test_sentinel_approves_clean_code(sentinel):
    code = """
    @app.post('/api/v1/payments/charge')
    def handler(idempotency_key: str):
        wallet = db.execute('SELECT balance FROM wallets WHERE id = 1 FOR UPDATE')
    """
    report = sentinel.scan_code(code)
    assert report.safe_to_merge is True
    assert len(report.vulnerabilities) == 0
