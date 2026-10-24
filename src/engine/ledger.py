"""Transactional payment ledger for SQLite and PostgreSQL."""
import concurrent.futures
from contextlib import closing
import hashlib
import json
import sqlite3
import tempfile
import time
import uuid
from pathlib import Path
from typing import Dict, List, Optional

from src.models.types import PaymentRequest, PaymentResult, TransactionStatus, Wallet


class ConcurrencyPaymentEngine:
    """Persist wallets, idempotency records, and audit entries in a SQL database.

    Pass a SQLite path or PostgreSQL DSN. SQLite uses ``BEGIN IMMEDIATE`` to
    serialize writers; PostgreSQL locks only the affected wallet row.
    """

    def __init__(self, db_path: Optional[str] = None):
        self._temporary_directory = None
        if db_path is None:
            self._temporary_directory = tempfile.TemporaryDirectory(prefix="cge-")
            db_path = str(Path(self._temporary_directory.name) / "ledger.sqlite3")
        self.database_url = str(db_path)
        self.db_path = self.database_url  # Backward-compatible SQLite attribute.
        self.backend = "postgresql" if self.database_url.startswith(("postgresql://", "postgres://")) else "sqlite"
        self._initialize_database()

    def _connect(self):
        if self.backend == "postgresql":
            try:
                import psycopg
                from psycopg.rows import dict_row
            except ImportError as exc:
                raise RuntimeError(
                    "PostgreSQL support requires installing concurrency-guard-engine[postgres]"
                ) from exc
            return psycopg.connect(self.database_url, autocommit=True, row_factory=dict_row)
        connection = sqlite3.connect(self.database_url, timeout=30, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
        return connection

    def _execute(self, connection, query: str, params=()):
        if self.backend == "postgresql":
            query = query.replace("?", "%s")
        return connection.execute(query, params)

    def _begin(self, connection) -> None:
        connection.execute("BEGIN IMMEDIATE" if self.backend == "sqlite" else "BEGIN")

    def _commit(self, connection) -> None:
        if self.backend == "postgresql":
            connection.execute("COMMIT")
        else:
            connection.commit()

    def _rollback(self, connection) -> None:
        if self.backend == "postgresql":
            connection.execute("ROLLBACK")
        else:
            connection.rollback()

    def _initialize_database(self) -> None:
        if self.backend == "sqlite":
            Path(self.database_url).parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as connection:
            if self.backend == "sqlite":
                connection.execute("PRAGMA journal_mode = WAL")
                connection.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS wallets (
                        wallet_id TEXT PRIMARY KEY,
                        user_id TEXT NOT NULL,
                        balance_jpy INTEGER NOT NULL CHECK (balance_jpy >= 0),
                        version INTEGER NOT NULL DEFAULT 0 CHECK (version >= 0)
                    );
                    CREATE TABLE IF NOT EXISTS idempotency_records (
                        idempotency_key TEXT PRIMARY KEY,
                        payload_hash TEXT NOT NULL,
                        result_json TEXT NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS audit_ledger (
                        sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                        tx_id TEXT NOT NULL UNIQUE,
                        wallet_id TEXT NOT NULL REFERENCES wallets(wallet_id),
                        amount_jpy INTEGER NOT NULL CHECK (amount_jpy > 0),
                        balance_after_jpy INTEGER NOT NULL CHECK (balance_after_jpy >= 0),
                        version INTEGER NOT NULL,
                        payload_hash TEXT NOT NULL,
                        created_at REAL NOT NULL
                    );
                    """
                )
            else:
                connection.execute(
                    """CREATE TABLE IF NOT EXISTS wallets (
                        wallet_id TEXT PRIMARY KEY,
                        user_id TEXT NOT NULL,
                        balance_jpy BIGINT NOT NULL CHECK (balance_jpy >= 0),
                        version BIGINT NOT NULL DEFAULT 0 CHECK (version >= 0)
                    )"""
                )
                connection.execute(
                    """CREATE TABLE IF NOT EXISTS idempotency_records (
                        idempotency_key TEXT PRIMARY KEY,
                        payload_hash TEXT NOT NULL,
                        result_json TEXT NOT NULL
                    )"""
                )
                connection.execute(
                    """CREATE TABLE IF NOT EXISTS audit_ledger (
                        sequence BIGSERIAL PRIMARY KEY,
                        tx_id TEXT NOT NULL UNIQUE,
                        wallet_id TEXT NOT NULL REFERENCES wallets(wallet_id),
                        amount_jpy BIGINT NOT NULL CHECK (amount_jpy > 0),
                        balance_after_jpy BIGINT NOT NULL CHECK (balance_after_jpy >= 0),
                        version BIGINT NOT NULL,
                        payload_hash TEXT NOT NULL,
                        created_at DOUBLE PRECISION NOT NULL
                    )"""
                )

    @property
    def _audit_ledger(self) -> List[Dict[str, object]]:
        """Compatibility view used by the demo and evaluation harness."""
        with closing(self._connect()) as connection:
            rows = self._execute(
                connection,
                "SELECT tx_id, wallet_id, amount_jpy, balance_after_jpy, version, "
                "payload_hash, created_at FROM audit_ledger ORDER BY sequence",
            ).fetchall()
        return [dict(row) for row in rows]

    def create_wallet(self, wallet_id: str, user_id: str, initial_balance_jpy: int) -> Wallet:
        wallet = Wallet(wallet_id=wallet_id, user_id=user_id, balance_jpy=initial_balance_jpy)
        with closing(self._connect()) as connection:
            self._begin(connection)
            try:
                self._execute(
                    connection,
                    "INSERT INTO wallets(wallet_id, user_id, balance_jpy, version) VALUES (?, ?, ?, 0)",
                    (wallet.wallet_id, wallet.user_id, wallet.balance_jpy),
                )
                self._commit(connection)
            except Exception:
                self._rollback(connection)
                raise
        return wallet

    def get_wallet(self, wallet_id: str) -> Optional[Wallet]:
        with closing(self._connect()) as connection:
            row = self._execute(
                connection,
                "SELECT wallet_id, user_id, balance_jpy, version FROM wallets WHERE wallet_id = ?",
                (wallet_id,),
            ).fetchone()
        return Wallet(**dict(row)) if row else None

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

    @staticmethod
    def _result_from_json(value: str) -> PaymentResult:
        return PaymentResult(**json.loads(value))

    def process_payment(self, req: PaymentRequest) -> PaymentResult:
        start_time = time.perf_counter()
        payload_hash = self.compute_payload_hash(req)
        connection = self._connect()
        try:
            # Idempotency lookup, balance check, debit, and audit write share one
            # transaction. PostgreSQL's wallet row lock permits unrelated wallets
            # to progress independently while preventing a same-wallet lost update.
            self._begin(connection)
            if self.backend == "postgresql":
                # Serialize requests sharing a key even when they target different
                # wallets. The lock is scoped to this transaction and released at
                # commit/rollback; the signed hash is just its 64-bit lock identity.
                key_lock = int.from_bytes(
                    hashlib.sha256(req.idempotency_key.encode("utf-8")).digest()[:8],
                    byteorder="big",
                    signed=True,
                )
                self._execute(connection, "SELECT pg_advisory_xact_lock(?)", (key_lock,))
            cached = self._execute(
                connection,
                "SELECT payload_hash, result_json FROM idempotency_records WHERE idempotency_key = ?",
                (req.idempotency_key,),
            ).fetchone()
            if cached:
                if cached["payload_hash"] != payload_hash:
                    result = PaymentResult(
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
                    self._commit(connection)
                    return result
                prior = self._result_from_json(cached["result_json"])
                result = prior.model_copy(update={
                    "transaction_id": req.transaction_id,
                    "status": TransactionStatus.REPLAY_IDEMPOTENT,
                    "latency_ms": (time.perf_counter() - start_time) * 1000,
                    "message": "Idempotent replay: transaction previously settled",
                })
                self._commit(connection)
                return result

            wallet_query = "SELECT wallet_id, user_id, balance_jpy, version FROM wallets WHERE wallet_id = ?"
            if self.backend == "postgresql":
                wallet_query += " FOR UPDATE"
            wallet_row = self._execute(connection, wallet_query, (req.payer_wallet_id,)).fetchone()
            if wallet_row is None:
                raise KeyError(f"Wallet {req.payer_wallet_id} does not exist")
            wallet = Wallet(**dict(wallet_row))
            balance_before = wallet.balance_jpy
            if balance_before < req.amount_jpy:
                self._commit(connection)
                return PaymentResult(
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

            balance_after = balance_before - req.amount_jpy
            version = wallet.version + 1
            self._execute(
                connection,
                "UPDATE wallets SET balance_jpy = ?, version = ? WHERE wallet_id = ?",
                (balance_after, version, req.payer_wallet_id),
            )
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
            self._execute(
                connection,
                "INSERT INTO idempotency_records(idempotency_key, payload_hash, result_json) VALUES (?, ?, ?)",
                (req.idempotency_key, payload_hash, result.model_dump_json()),
            )
            self._execute(
                connection,
                "INSERT INTO audit_ledger(tx_id, wallet_id, amount_jpy, balance_after_jpy, version, payload_hash, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (req.transaction_id, req.payer_wallet_id, req.amount_jpy, balance_after, version, payload_hash, time.time()),
            )
            self._commit(connection)
            return result
        except Exception:
            self._rollback(connection)
            raise
        finally:
            connection.close()

    def execute_contention_burst(
        self,
        payer_wallet_id: str,
        total_requests: int = 50,
        amount_per_request: int = 1000,
    ) -> Dict[str, int]:
        """Run concurrent requests through independent database connections."""
        burst_id = uuid.uuid4().hex
        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
            futures = [
                executor.submit(
                    self.process_payment,
                    PaymentRequest(
                        transaction_id=f"tx-burst-{burst_id}-{i:03d}",
                        idempotency_key=f"idem-burst-{burst_id}-{i:03d}",
                        payer_wallet_id=payer_wallet_id,
                        merchant_id="merchant-shibuya-store",
                        amount_jpy=amount_per_request,
                    ),
                )
                for i in range(total_requests)
            ]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]

        wallet = self.get_wallet(payer_wallet_id)
        return {
            "total_requests": total_requests,
            "settled_count": sum(r.status == TransactionStatus.SETTLED for r in results),
            "insufficient_funds_count": sum(r.status == TransactionStatus.INSUFFICIENT_FUNDS for r in results),
            "final_balance_jpy": wallet.balance_jpy if wallet else 0,
            "final_wallet_version": wallet.version if wallet else 0,
            "audit_ledger_records": len(self._audit_ledger),
        }
