"""
Domain models and schema definitions for high-concurrency financial ledgers and AI-augmented code sentinels.
"""
from enum import Enum
from typing import Dict, List, Optional
from pydantic import BaseModel, Field


class TransactionStatus(str, Enum):
    SETTLED = "SETTLED"                          # Transaction successfully completed and ledger updated
    REPLAY_IDEMPOTENT = "REPLAY_IDEMPOTENT"      # Duplicate key detected; returned verified prior result
    INSUFFICIENT_FUNDS = "INSUFFICIENT_FUNDS"    # Balance below requested debit
    LOCK_CONTENTION = "LOCK_CONTENTION"          # Concurrent lock timeout / retry needed
    CONFLICT = "CONFLICT"                        # Idempotency key reused with mismatched payload
    FAILED = "FAILED"                            # Transaction failed and rolled back


class VulnerabilityType(str, Enum):
    UNGUARDED_BALANCE_MUTATION = "UNGUARDED_BALANCE_MUTATION"  # Modifying balance without SELECT FOR UPDATE / lock
    NON_IDEMPOTENT_ENDPOINT = "NON_IDEMPOTENT_ENDPOINT"        # Financial API missing Idempotency-Key header check
    UNBOUNDED_RETRY = "UNBOUNDED_RETRY"                        # Retry loop lacking exponential backoff / jitter
    MISSING_ROLLBACK = "MISSING_ROLLBACK"                      # Database transaction lacking explicit rollback in except block


class Wallet(BaseModel):
    wallet_id: str
    user_id: str
    balance_jpy: int = Field(ge=0)
    version: int = 0  # Optimistic concurrency lock version


class PaymentRequest(BaseModel):
    transaction_id: str
    idempotency_key: str
    payer_wallet_id: str
    merchant_id: str
    amount_jpy: int = Field(gt=0)
    qr_code_ref: Optional[str] = None


class PaymentResult(BaseModel):
    transaction_id: str
    idempotency_key: str
    status: TransactionStatus
    amount_jpy: int
    payer_wallet_id: str
    balance_before_jpy: int
    balance_after_jpy: int
    payload_hash: str
    latency_ms: float = 0.0
    message: Optional[str] = None


class ConcurrencyVulnerability(BaseModel):
    rule_id: str
    vulnerability_type: VulnerabilityType
    severity: str  # HIGH, CRITICAL
    description: str
    line_number: Optional[int] = None
    problematic_code: str
    suggested_patch: str


class SentinelReport(BaseModel):
    target_file: str
    vulnerabilities: List[ConcurrencyVulnerability]
    safe_to_merge: bool
    summary: str
