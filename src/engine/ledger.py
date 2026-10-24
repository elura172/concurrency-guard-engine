"""
High-Concurrency Payment Ledger Engine with Lock-Guarded Idempotency and Race Condition Elimination.
"""
import concurrent.futures
import hashlib
import json
import threading
import time
from typing import Dict, List, Optional, Tuple
from src.models.types import (
    PaymentRequest,
    PaymentResult,
    TransactionStatus,
    Wallet,
)


class ConcurrencyPaymentEngine:
    """
    High-throughput transactional engine handling wallet debits, distributed mutex locks, and idempotent replay.
    """
    def __init__(self):
        # In-memory wallet datastore
        self._wallets: Dict[str, Wallet] = {}
        # Fine-grained per-wallet lock registry
        self._wallet_locks: Dict[str, threading.Lock] = {}
        self._meta_lock = threading.Lock()

        # Idempotency store: idempotency_key -> {payload_hash, result}
        self._idempotency_store: Dict[str, Dict[str, any]] = {}
        self._idempotency_lock = threading.Lock()

        # Immutable audit ledger
        self._audit_ledger: List[Dict[str, any]] = []

    def create_wallet(self, wallet_id: str, user_id: str, initial_balance_jpy: int) -> Wallet:
        with self._meta_lock:
            wallet = Wallet(wallet_id=wallet_id, user_id=user_id, balance_jpy=initial_balance_jpy)
            self._wallets[wallet_id] = wallet
            self._wallet_locks[wallet_id] = threading.Lock()
            return wallet

    def get_wallet(self, wallet_id: str) -> Optional[Wallet]:
        return self._wallets.get(wallet_id)

    @staticmethod
    def compute_payload_hash(req: PaymentRequest) -> str:
        data = {
            "payer": req.payer_wallet_id,
            "merchant": req.merchant_id,
            "amount": req.amount_jpy,
            "qr": req.qr_code_ref,
        }
        raw = json.dumps(data, sort_keys=True)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def process_payment(self, req: PaymentRequest) -> PaymentResult:
        start_time = time.perf_counter()
        payload_hash = self.compute_payload_hash(req)

        # 1. Fast-Path Idempotency Check
        with self._idempotency_lock:
            if req.idempotency_key in self._idempotency_store:
                cached = self._idempotency_store[req.idempotency_key]
                if cached["payload_hash"] != payload_hash:
                    return PaymentResult(
                        transaction_id=req.transaction_id,
                        idempotency_key=req.idempotency_key,
                        status=TransactionStatus.CONFLICT,
                        amount_jpy=req.amount_jpy,
                        payer_wallet_id=req.payer_wallet_id,
                        balance_before_jpy=0,
                        balance_after_jpy=0,
                        payload_hash=payload_hash,
                        latency_ms=(time.perf_counter() - start_time) * 1000,
                        message="Idempotency key reused with mismatched transaction payload",
                    )
                
                cached_res: PaymentResult = cached["result"]
                return PaymentResult(
                    transaction_id=req.transaction_id,
                    idempotency_key=req.idempotency_key,
                    status=TransactionStatus.REPLAY_IDEMPOTENT,
                    amount_jpy=cached_res.amount_jpy,
                    payer_wallet_id=cached_res.payer_wallet_id,
                    balance_before_jpy=cached_res.balance_before_jpy,
                    balance_after_jpy=cached_res.balance_after_jpy,
                    payload_hash=payload_hash,
                    latency_ms=(time.perf_counter() - start_time) * 1000,
                    message="Idempotent replay: transaction previously settled",
                )

        # 2. Acquire Fine-Grained Per-Wallet Mutex Lock
        with self._meta_lock:
            if req.payer_wallet_id not in self._wallets:
                raise KeyError(f"Wallet {req.payer_wallet_id} does not exist")
            wallet_lock = self._wallet_locks[req.payer_wallet_id]

        with wallet_lock:
            wallet = self._wallets[req.payer_wallet_id]
            balance_before = wallet.balance_jpy

            # Check Insufficient Funds
            if wallet.balance_jpy < req.amount_jpy:
                result = PaymentResult(
                    transaction_id=req.transaction_id,
                    idempotency_key=req.idempotency_key,
                    status=TransactionStatus.INSUFFICIENT_FUNDS,
                    amount_jpy=req.amount_jpy,
                    payer_wallet_id=req.payer_wallet_id,
                    balance_before_jpy=balance_before,
                    balance_after_jpy=balance_before,
                    payload_hash=payload_hash,
                    latency_ms=(time.perf_counter() - start_time) * 1000,
                    message=f"Insufficient funds: available ¥{balance_before:,}, required ¥{req.amount_jpy:,}",
                )
                return result

            # Atomic Mutation
            wallet.balance_jpy -= req.amount_jpy
            wallet.version += 1
            balance_after = wallet.balance_jpy

            result = PaymentResult(
                transaction_id=req.transaction_id,
                idempotency_key=req.idempotency_key,
                status=TransactionStatus.SETTLED,
                amount_jpy=req.amount_jpy,
                payer_wallet_id=req.payer_wallet_id,
                balance_before_jpy=balance_before,
                balance_after_jpy=balance_after,
                payload_hash=payload_hash,
                latency_ms=(time.perf_counter() - start_time) * 1000,
                message="Transaction settled atomically",
            )

            # Record in idempotency registry & audit ledger
            with self._idempotency_lock:
                self._idempotency_store[req.idempotency_key] = {
                    "payload_hash": payload_hash,
                    "result": result,
                }
                self._audit_ledger.append({
                    "tx_id": req.transaction_id,
                    "wallet_id": req.payer_wallet_id,
                    "amount": req.amount_jpy,
                    "balance_after": balance_after,
                    "version": wallet.version,
                    "hash": payload_hash,
                    "timestamp": time.time(),
                })

            return result

    def execute_contention_burst(
        self,
        payer_wallet_id: str,
        total_requests: int = 50,
        amount_per_request: int = 1000,
    ) -> Dict[str, any]:
        """
        Executes a high-concurrency race condition test with parallel worker threads hitting the exact same wallet simultaneously.
        """
        results: List[PaymentResult] = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
            futures = [
                executor.submit(
                    self.process_payment,
                    PaymentRequest(
                        transaction_id=f"tx-burst-{i:03d}",
                        idempotency_key=f"idem-burst-{i:03d}",
                        payer_wallet_id=payer_wallet_id,
                        merchant_id="merchant-shibuya-store",
                        amount_jpy=amount_per_request,
                    ),
                )
                for i in range(total_requests)
            ]
            for future in concurrent.futures.as_completed(futures):
                results.append(future.result())

        settled = sum(1 for r in results if r.status == TransactionStatus.SETTLED)
        insufficient = sum(1 for r in results if r.status == TransactionStatus.INSUFFICIENT_FUNDS)
        wallet = self.get_wallet(payer_wallet_id)

        return {
            "total_requests": total_requests,
            "settled_count": settled,
            "insufficient_funds_count": insufficient,
            "final_balance_jpy": wallet.balance_jpy if wallet else 0,
            "final_wallet_version": wallet.version if wallet else 0,
            "audit_ledger_records": len(self._audit_ledger),
        }
