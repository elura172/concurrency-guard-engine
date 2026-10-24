"""
AI-Augmented Code Sentinel: Static Concurrency Pattern Analysis & Automated Remediation.
Inspects PR diffs and backend queries to prevent race conditions in AI-generated code.
"""
import re
from typing import List, Optional
from src.models.types import (
    ConcurrencyVulnerability,
    SentinelReport,
    VulnerabilityType,
)


class ConcurrencySentinel:
    """
    Automated developer sentinel designed for CI/CD pipelines to catch subtle financial race conditions in pull requests.
    """
    def __init__(self):
        pass

    def scan_code(self, source_code: str, file_path: str = "api/payments.py") -> SentinelReport:
        vulnerabilities: List[ConcurrencyVulnerability] = []
        lines = source_code.splitlines()

        # Rule 1: Unguarded Balance Read (Missing SELECT FOR UPDATE or lock)
        for idx, line in enumerate(lines, 1):
            if re.search(r"SELECT\s+.*balance.*FROM\s+wallets", line, re.IGNORECASE) and not re.search(r"FOR\s+UPDATE", line, re.IGNORECASE):
                vulnerabilities.append(
                    ConcurrencyVulnerability(
                        rule_id="PAY-001",
                        vulnerability_type=VulnerabilityType.UNGUARDED_BALANCE_MUTATION,
                        severity="CRITICAL",
                        description="Wallet balance read without pessimistic row lock (SELECT ... FOR UPDATE). Subject to concurrent race conditions.",
                        line_number=idx,
                        problematic_code=line.strip(),
                        suggested_patch="SELECT balance_jpy FROM wallets WHERE id = :wallet_id FOR UPDATE;",
                    )
                )

        # Rule 2: Payment Endpoint Missing Idempotency Key
        if re.search(r"@app\.(post|put)\([\"']/api/v1/payments", source_code, re.IGNORECASE):
            if "Idempotency-Key" not in source_code and "idempotency_key" not in source_code:
                vulnerabilities.append(
                    ConcurrencyVulnerability(
                        rule_id="PAY-002",
                        vulnerability_type=VulnerabilityType.NON_IDEMPOTENT_ENDPOINT,
                        severity="CRITICAL",
                        description="Payment mutation endpoint does not enforce or validate 'Idempotency-Key' header. Subject to duplicate billing.",
                        line_number=None,
                        problematic_code="@app.post('/api/v1/payments/charge')",
                        suggested_patch="@app.post('/api/v1/payments/charge')\nasync def charge(req: PaymentRequest, idempotency_key: str = Header(...)):",
                    )
                )

        # Rule 3: Unbounded Retry Loop without Jitter
        if re.search(r"while\s+True:", source_code) or re.search(r"for\s+\w+\s+in\s+range\(\d+\):", source_code):
            if "time.sleep" in source_code and "random" not in source_code and "jitter" not in source_code:
                vulnerabilities.append(
                    ConcurrencyVulnerability(
                        rule_id="PAY-003",
                        vulnerability_type=VulnerabilityType.UNBOUNDED_RETRY,
                        severity="HIGH",
                        description="Retry loop detected without exponential backoff and randomized jitter. Subject to thundering herd collapse.",
                        line_number=None,
                        problematic_code="time.sleep(1)",
                        suggested_patch="await asyncio.sleep(min(max_backoff, base_delay * (2 ** attempt)) + random.uniform(0, jitter))",
                    )
                )

        # Rule 4: Exception Block Missing Explicit Rollback
        if "except " in source_code and "db.commit()" in source_code:
            if "db.rollback()" not in source_code and "session.rollback()" not in source_code:
                vulnerabilities.append(
                    ConcurrencyVulnerability(
                        rule_id="PAY-004",
                        vulnerability_type=VulnerabilityType.MISSING_ROLLBACK,
                        severity="HIGH",
                        description="Exception block catches database errors without explicitly rolling back transaction state.",
                        line_number=None,
                        problematic_code="except Exception:\n    logger.error('failed')",
                        suggested_patch="except Exception as e:\n    db.rollback()\n    logger.error('Transaction rolled back: %s', e)\n    raise",
                    )
                )

        safe = len(vulnerabilities) == 0
        summary = (
            "✅ All concurrency guards passed. Safe to merge into production financial backends."
            if safe
            else f"⚠️ Found {len(vulnerabilities)} concurrency vulnerabilities in pull request diff."
        )

        return SentinelReport(
            target_file=file_path,
            vulnerabilities=vulnerabilities,
            safe_to_merge=safe,
            summary=summary,
        )
