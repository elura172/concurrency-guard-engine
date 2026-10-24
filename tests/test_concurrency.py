"""
Unit & Integration Test Suite for Concurrency Guard Engine & Sentinel.
"""
import pytest
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
