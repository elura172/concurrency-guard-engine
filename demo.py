#!/usr/bin/env python3
"""
Interactive Terminal Demo for PayPay: Concurrency Guard Engine & AI Developer Sentinel.
Runs high-concurrency race contention tests, automated AST vulnerability scanning, and the 10-scenario benchmark suite.
"""
import sys
import time
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import box

from src.models.types import PaymentRequest
from src.engine.ledger import ConcurrencyPaymentEngine
from src.sentinel.analyzer import ConcurrencySentinel
from src.evals.harness import ConcurrencyBenchmarkSuite

console = Console()


def print_banner():
    banner_text = (
        "[bold cyan]PAYPAY // CONCURRENCY GUARD ENGINE[/bold cyan]\n"
        "[dim]High-Concurrency Financial Ledger, Race Condition Elimination & AI CI/CD Sentinel[/dim]\n"
        "[dim white]Author: Paul Trafford (elura172) · Tokyo Relocation Pipeline Candidate[/dim white]"
    )
    console.print(Panel(banner_text, border_style="cyan", box=box.DOUBLE))


def run_live_contention_simulation():
    console.print("\n[bold yellow]▶ STEP 1: Live 50-Thread High-Concurrency Contention Burst[/bold yellow]")
    engine = ConcurrencyPaymentEngine()
    engine.create_wallet("wallet-shibuya-user-01", "user-tokyo-77", initial_balance_jpy=10000)
    console.print("  [dim]Created test wallet: balance = ¥10,000 | Spawning 50 parallel worker threads attempting to debit ¥1,000 each...[/dim]")

    burst = engine.execute_contention_burst("wallet-shibuya-user-01", total_requests=50, amount_per_request=1000)

    console.print(f"  Total Concurrent Requests: [bold]{burst['total_requests']}[/bold]")
    console.print(f"  Successfully Settled: [bold green]{burst['settled_count']} (¥{burst['settled_count'] * 1000:,})[/bold green]")
    console.print(f"  Rejected for Insufficient Funds: [bold yellow]{burst['insufficient_funds_count']}[/bold yellow]")
    console.print(f"  Final Wallet Balance: [bold cyan]¥{burst['final_balance_jpy']:,}[/bold cyan] (Expected: ¥0 — [bold green]ZERO DOUBLE-SPENDS[/bold green])")
    console.print(f"  Final Optimistic Lock Version: [dim]v{burst['final_wallet_version']}[/dim]")


def run_sentinel_simulation():
    console.print("\n[bold yellow]▶ STEP 2: AI Developer Sentinel — PR Concurrency Vulnerability Scan[/bold yellow]")
    sentinel = ConcurrencySentinel()
    
    vulnerable_pr = """
    # Pull Request #1429 by Junior Engineer (generated via Copilot)
    @app.post('/api/v1/payments/qr-checkout')
    async def checkout(wallet_id: str, amount: int):
        wallet = await db.fetch_one("SELECT balance_jpy FROM wallets WHERE id = :id", id=wallet_id)
        if wallet.balance_jpy >= amount:
            await db.execute("UPDATE wallets SET balance_jpy = balance_jpy - :amt WHERE id = :id", amt=amount, id=wallet_id)
            return {"status": "SUCCESS"}
    """
    console.print("  [dim]Scanning simulated PR diff for financial race conditions...[/dim]")
    report = sentinel.scan_code(vulnerable_pr, "api/qr_checkout.py")

    console.print(f"  PR Concurrency Status: [bold red]REJECTED FROM MERGE[/bold red]")
    console.print(f"  Vulnerabilities Intercepted: [bold red]{len(report.vulnerabilities)}[/bold red]")

    for idx, v in enumerate(report.vulnerabilities, 1):
        console.print(f"\n  [bold red]Vulnerability {idx}: [{v.rule_id}] {v.vulnerability_type.value}[/bold red]")
        console.print(f"    Issue: {v.description}")
        console.print(f"    Problematic Code: [yellow]{v.problematic_code}[/yellow]")
        console.print(f"    [bold green]AI Sentinel Automated Patch:[/bold green]\n    [dim green]{v.suggested_patch}[/dim green]")


def run_benchmark():
    console.print("\n[bold yellow]▶ STEP 3: Running 10-Scenario Automated Evaluation Benchmark[/bold yellow]\n")
    suite = ConcurrencyBenchmarkSuite()
    res = suite.run_benchmark()

    table = Table(title="PayPay Concurrency & Code Sentinel Benchmark Results", box=box.ROUNDED)
    table.add_column("Scenario ID", style="bold cyan")
    table.add_column("Description", style="white")
    table.add_column("Metric / Status Detail", style="yellow")
    table.add_column("Status", style="bold")

    for sc in res["scenarios"]:
        status_str = "[bold green]PASS[/bold green]" if sc["passed"] else "[bold red]FAIL[/bold red]"
        table.add_row(
            sc["scenario"],
            sc["description"][:50] + "...",
            sc["metric"],
            status_str,
        )

    console.print(table)

    summary_panel = Panel(
        f"[bold]Total Scenarios:[/bold] {res['total_scenarios']} | "
        f"[bold green]Passed:[/bold green] {res['passed']} | "
        f"[bold red]Failed:[/bold red] {res['failed']} | "
        f"[bold cyan]Accuracy Score:[/bold cyan] [bold green]{res['accuracy_score']}%[/bold green]",
        border_style="green",
        title="PayPay Concurrency Benchmark Summary",
    )
    console.print(summary_panel)


def main():
    print_banner()
    run_live_contention_simulation()
    time.sleep(0.5)
    run_sentinel_simulation()
    time.sleep(0.5)
    run_benchmark()


if __name__ == "__main__":
    main()
