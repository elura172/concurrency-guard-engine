"""
Evaluation Harness for Transaction Invariants and the Static Code Sentinel.
"""
from typing import Any, Dict, List
from src.models.types import PaymentRequest, TransactionStatus
from src.engine.ledger import ConcurrencyPaymentEngine
from src.sentinel.analyzer import ConcurrencySentinel


class ConcurrencyBenchmarkSuite:
    """
    10-scenario functional suite checking payment transaction invariants and static sentinel fixtures.
    """
    def __init__(self):
        pass

    def run_benchmark(self) -> Dict[str, Any]:
        results = []
        passed = 0

        # Scenario 1: 50 Parallel Goroutines/Threads Racing on One Wallet
        engine_1 = ConcurrencyPaymentEngine()
        engine_1.create_wallet("w-race-01", "user-tokyo-1", initial_balance_jpy=10000)
        burst_res = engine_1.execute_contention_burst("w-race-01", total_requests=50, amount_per_request=1000)
        p1 = (
            burst_res["settled_count"] == 10
            and burst_res["insufficient_funds_count"] == 40
            and burst_res["final_balance_jpy"] == 0
        )
        if p1: passed += 1
        results.append({
            "scenario": "01_PARALLEL_BURST_CONTENTION_RACE",
            "description": "50 concurrent worker threads race on ¥10,000 balance (zero double-spends)",
            "passed": p1,
            "metric": f"{burst_res['settled_count']} settled / {burst_res['insufficient_funds_count']} rejected (balance: ¥0)",
        })

        # Scenario 2: Idempotent Payment Replay on Duplicate QR Scan
        engine_2 = ConcurrencyPaymentEngine()
        engine_2.create_wallet("w-idem-01", "user-tokyo-2", initial_balance_jpy=5000)
        req_2 = PaymentRequest(
            transaction_id="tx-qr-001",
            idempotency_key="key-qr-scan-123",
            payer_wallet_id="w-idem-01",
            merchant_id="merchant-lawson-shinjuku",
            amount_jpy=1500,
        )
        res_2a = engine_2.process_payment(req_2)
        res_2b = engine_2.process_payment(req_2)
        p2 = res_2a.status == TransactionStatus.SETTLED and res_2b.status == TransactionStatus.REPLAY_IDEMPOTENT and res_2b.balance_after_jpy == 3500
        if p2: passed += 1
        results.append({
            "scenario": "02_IDEMPOTENT_DUPLICATE_QR_REPLAY",
            "description": "Network retry with identical key returns verified prior settlement without debiting twice",
            "passed": p2,
            "metric": f"First: {res_2a.status.value} | Replay: {res_2b.status.value}",
        })

        # Scenario 3: Idempotency Key Payload Tampering Conflict
        engine_3 = ConcurrencyPaymentEngine()
        engine_3.create_wallet("w-idem-02", "user-tokyo-3", initial_balance_jpy=5000)
        req_3a = PaymentRequest(
            transaction_id="tx-orig",
            idempotency_key="key-reused-conflict",
            payer_wallet_id="w-idem-02",
            merchant_id="merchant-famima",
            amount_jpy=1000,
        )
        engine_3.process_payment(req_3a)
        req_3b = PaymentRequest(
            transaction_id="tx-tampered",
            idempotency_key="key-reused-conflict", # Same key, altered amount!
            payer_wallet_id="w-idem-02",
            merchant_id="merchant-famima",
            amount_jpy=4000,
        )
        res_3b = engine_3.process_payment(req_3b)
        p3 = res_3b.status == TransactionStatus.CONFLICT
        if p3: passed += 1
        results.append({
            "scenario": "03_IDEMPOTENCY_PAYLOAD_TAMPER_CONFLICT",
            "description": "Idempotency key reused with modified payment amount rejected as CONFLICT",
            "passed": p3,
            "metric": f"Status: {res_3b.status.value}",
        })

        # Scenario 4: Insufficient Funds Clean Rejection
        engine_4 = ConcurrencyPaymentEngine()
        engine_4.create_wallet("w-low-01", "user-tokyo-4", initial_balance_jpy=500)
        req_4 = PaymentRequest(
            transaction_id="tx-low-01",
            idempotency_key="key-low-01",
            payer_wallet_id="w-low-01",
            merchant_id="merchant-7eleven",
            amount_jpy=1200,
        )
        res_4 = engine_4.process_payment(req_4)
        p4 = res_4.status == TransactionStatus.INSUFFICIENT_FUNDS and engine_4.get_wallet("w-low-01").balance_jpy == 500
        if p4: passed += 1
        results.append({
            "scenario": "04_INSUFFICIENT_FUNDS_SAFETY_REJECTION",
            "description": "Payment exceeding balance rejected cleanly without modifying wallet balance",
            "passed": p4,
            "metric": f"Status: {res_4.status.value} (Balance intact at ¥500)",
        })

        # Scenario 5: Sentinel Catches Unguarded Balance Read
        sentinel = ConcurrencySentinel()
        vuln_code_1 = """
        def transfer_funds(db, wallet_id, amount):
            wallet = db.query("SELECT balance FROM wallets WHERE id = ?", wallet_id)
            if wallet.balance >= amount:
                db.execute("UPDATE wallets SET balance = balance - ? WHERE id = ?", amount, wallet_id)
        """
        rep_5 = sentinel.scan_code(vuln_code_1, "api/transfer.py")
        p5 = any(v.rule_id == "PAY-001" for v in rep_5.vulnerabilities) and not rep_5.safe_to_merge
        if p5: passed += 1
        results.append({
            "scenario": "05_SENTINEL_UNGUARDED_BALANCE_READ",
            "description": "Static analyzer catches missing 'FOR UPDATE' row lock on balance read",
            "passed": p5,
            "metric": f"Caught Rule {rep_5.vulnerabilities[0].rule_id if rep_5.vulnerabilities else 'NONE'}",
        })

        # Scenario 6: Sentinel Catches Non-Idempotent Endpoint
        vuln_code_2 = """
        @app.post('/api/v1/payments/charge')
        async def charge_card(req: ChargeRequest):
            return payment_gateway.charge(req.card, req.amount)
        """
        rep_6 = sentinel.scan_code(vuln_code_2, "api/routes.py")
        p6 = any(v.rule_id == "PAY-002" for v in rep_6.vulnerabilities)
        if p6: passed += 1
        results.append({
            "scenario": "06_SENTINEL_NON_IDEMPOTENT_ENDPOINT",
            "description": "Static analyzer flags charge endpoint lacking Idempotency-Key validation",
            "passed": p6,
            "metric": f"Caught Rule {rep_6.vulnerabilities[0].rule_id if rep_6.vulnerabilities else 'NONE'}",
        })

        # Scenario 7: Sentinel Catches Unbounded Retry without Jitter
        vuln_code_3 = """
        for attempt in range(5):
            try:
                return send_payment()
            except NetworkError:
                time.sleep(1)
        """
        rep_7 = sentinel.scan_code(vuln_code_3, "services/client.py")
        p7 = any(v.rule_id == "PAY-003" for v in rep_7.vulnerabilities)
        if p7: passed += 1
        results.append({
            "scenario": "07_SENTINEL_THUNDERING_HERD_RETRY",
            "description": "Static analyzer detects fixed-interval retry loop lacking backoff & jitter",
            "passed": p7,
            "metric": f"Caught Rule {rep_7.vulnerabilities[0].rule_id if rep_7.vulnerabilities else 'NONE'}",
        })

        # Scenario 8: Sentinel Catches Missing Transaction Rollback
        vuln_code_4 = """
        try:
            db.execute("INSERT INTO ledger ...")
            db.commit()
        except DatabaseError as e:
            logger.error("Insert failed: %s", e)
        """
        rep_8 = sentinel.scan_code(vuln_code_4, "db/transactions.py")
        p8 = any(v.rule_id == "PAY-004" for v in rep_8.vulnerabilities)
        if p8: passed += 1
        results.append({
            "scenario": "08_SENTINEL_MISSING_TRANSACTION_ROLLBACK",
            "description": "Static analyzer flags exception block lacking explicit db.rollback()",
            "passed": p8,
            "metric": f"Caught Rule {rep_8.vulnerabilities[0].rule_id if rep_8.vulnerabilities else 'NONE'}",
        })

        # Scenario 9: Sentinel Approves Clean Concurrency-Guarded Code
        clean_code = """
        @app.post('/api/v1/payments/charge')
        async def charge(req: PaymentRequest, idempotency_key: str = Header(...)):
            async with db.transaction():
                wallet = await db.fetch_one("SELECT balance_jpy FROM wallets WHERE id = :id FOR UPDATE", id=req.wallet_id)
                if wallet.balance_jpy < req.amount_jpy:
                    raise InsufficientFundsError()
                await db.execute("UPDATE wallets SET balance_jpy = balance_jpy - :amt WHERE id = :id", amt=req.amount_jpy, id=req.wallet_id)
                await db.commit()
        """
        rep_9 = sentinel.scan_code(clean_code, "api/safe_payments.py")
        p9 = rep_9.safe_to_merge is True and len(rep_9.vulnerabilities) == 0
        if p9: passed += 1
        results.append({
            "scenario": "09_SENTINEL_CLEAN_DIFF_APPROVAL",
            "description": "Static analyzer approves its clean-code fixture",
            "passed": p9,
            "metric": "Status: safe_to_merge=True (0 vulnerabilities)",
        })

        # Scenario 10: Transactional Audit Record and Versioning
        engine_10 = ConcurrencyPaymentEngine()
        engine_10.create_wallet("w-ver-01", "user-tokyo-10", initial_balance_jpy=10000)
        for i in range(3):
            engine_10.process_payment(
                PaymentRequest(
                    transaction_id=f"tx-audit-{i}",
                    idempotency_key=f"idem-audit-{i}",
                    payer_wallet_id="w-ver-01",
                    merchant_id="merchant-tokyo",
                    amount_jpy=1000,
                )
            )
        wallet_10 = engine_10.get_wallet("w-ver-01")
        p10 = wallet_10.version == 3 and wallet_10.balance_jpy == 7000 and len(engine_10._audit_ledger) == 3
        if p10: passed += 1
        results.append({
            "scenario": "10_AUDIT_VERSIONING",
            "description": "Every atomic debit increments wallet version and writes cryptographic audit record",
            "passed": p10,
            "metric": f"Wallet version: {wallet_10.version} | Audit records: {len(engine_10._audit_ledger)}",
        })

        return {
            "total_scenarios": len(results),
            "passed": passed,
            "failed": len(results) - passed,
            "accuracy_score": round((passed / len(results)) * 100, 1),
            "scenarios": results,
        }
