#!/usr/bin/env python3
"""
scripts/ledger_integrity.py
Offline ledger integrity checker for the Lending & Collateral platform.

Verifies double-entry accounting invariants:
  1. Journal pair balance (debit == credit per journal_id)
  2. Net zero across COA (total debits == total credits)
  3. No orphan journal entries (every entry references a valid account)
  4. Loan status history consistency
  5. Collateral status history consistency
  6. No negative balances on asset accounts
  7. Loan principal consistency (LOANS_RECEIVABLE vs loan records)
  8. Interest accrual continuity (no gaps for active loans)

Usage:
    DATABASE_URL=postgresql://lending:s3cr3t@localhost:5432/lending_db \
        python scripts/ledger_integrity.py

    python scripts/ledger_integrity.py --report-only
"""

import argparse
import os
import sys
from decimal import Decimal

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    print("psycopg2 not installed. Run: pip install psycopg2-binary")
    sys.exit(1)


DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://lending:s3cr3t@localhost:5432/lending_db",
)

PASS = "\033[32m[PASS]\033[0m"
FAIL = "\033[31m[FAIL]\033[0m"
BOLD = "\033[1m"
RESET = "\033[0m"

TOLERANCE = Decimal("0.000000000000000001")


def _connect():
    """Return a psycopg2 connection from DATABASE_URL."""
    dsn = DATABASE_URL.replace(
        "postgresql+psycopg2://", "postgresql://"
    )
    return psycopg2.connect(dsn)


def check_journal_pair_balance(cur):
    """Check 1: For every journal_id, SUM(debit) == SUM(credit)."""
    cur.execute("""
        SELECT journal_id,
               COALESCE(SUM(debit), 0)  AS total_debit,
               COALESCE(SUM(credit), 0) AS total_credit
        FROM journal_entries
        GROUP BY journal_id
        HAVING ABS(SUM(debit) - SUM(credit)) > 0.000000000000000001
    """)
    rows = cur.fetchall()
    if rows:
        details = [
            f"journal_id={r[0]} debit={r[1]} credit={r[2]} "
            f"diff={Decimal(str(r[1])) - Decimal(str(r[2]))}"
            for r in rows
        ]
        msg = (
            f"{len(rows)} unbalanced journal(s): "
            + "; ".join(details[:5])
        )
        if len(rows) > 5:
            msg += f" ... and {len(rows) - 5} more"
        return False, msg

    cur.execute(
        "SELECT COUNT(DISTINCT journal_id) FROM journal_entries"
    )
    count = cur.fetchone()[0]
    return True, f"All {count} journals are balanced."


def check_net_zero_across_coa(cur):
    """Check 2: Total debits across all accounts == total credits."""
    cur.execute("""
        SELECT COALESCE(SUM(debit), 0)  AS total_debit,
               COALESCE(SUM(credit), 0) AS total_credit
        FROM journal_entries
    """)
    row = cur.fetchone()
    total_debit = Decimal(str(row[0]))
    total_credit = Decimal(str(row[1]))
    diff = abs(total_debit - total_credit)

    if diff > TOLERANCE:
        return (
            False,
            f"Net imbalance: total_debit={total_debit} "
            f"total_credit={total_credit} diff={diff}",
        )
    return (
        True,
        f"Net zero confirmed: debit={total_debit} "
        f"credit={total_credit}",
    )


def check_no_orphan_journal_entries(cur):
    """Check 3: Every journal entry references a valid account_id."""
    cur.execute("""
        SELECT je.id, je.account_id
        FROM journal_entries je
        LEFT JOIN accounts a ON a.id = je.account_id
        WHERE a.id IS NULL
    """)
    orphans = cur.fetchall()
    if orphans:
        ids = [str(r[0]) for r in orphans[:5]]
        msg = (
            f"{len(orphans)} orphan entries (no matching account): "
            + ", ".join(ids)
        )
        if len(orphans) > 5:
            msg += f" ... and {len(orphans) - 5} more"
        return False, msg
    return True, "No orphan journal entries found."


def check_loan_status_history(cur):
    """Check 4: Every loan's status matches its latest status history row."""
    cur.execute("""
        SELECT l.id, l.loan_ref, l.status AS loan_status,
               lcs.status AS history_status
        FROM loans l
        LEFT JOIN LATERAL (
            SELECT lsh.status
            FROM loan_status_history lsh
            WHERE lsh.loan_id = l.id
            ORDER BY lsh.created_at DESC
            LIMIT 1
        ) lcs ON TRUE
        WHERE lcs.status IS NOT NULL
          AND l.status::text != lcs.status
    """)
    mismatches = cur.fetchall()
    if mismatches:
        details = [
            f"loan={r[1]} current={r[2]} history={r[3]}"
            for r in mismatches[:5]
        ]
        msg = (
            f"{len(mismatches)} loan status mismatch(es): "
            + "; ".join(details)
        )
        if len(mismatches) > 5:
            msg += f" ... and {len(mismatches) - 5} more"
        return False, msg

    cur.execute("SELECT COUNT(*) FROM loans")
    count = cur.fetchone()[0]
    return (
        True,
        f"All {count} loan(s) consistent with status history.",
    )


def check_collateral_status_history(cur):
    """Check 5: Every collateral position's status matches latest history."""
    cur.execute("""
        SELECT cp.id, cp.collateral_ref,
               cp.status AS position_status,
               ccs.status AS history_status
        FROM collateral_positions cp
        LEFT JOIN LATERAL (
            SELECT csh.status
            FROM collateral_status_history csh
            WHERE csh.collateral_id = cp.id
            ORDER BY csh.created_at DESC
            LIMIT 1
        ) ccs ON TRUE
        WHERE ccs.status IS NOT NULL
          AND cp.status::text != ccs.status
    """)
    mismatches = cur.fetchall()
    if mismatches:
        details = [
            f"collateral={r[1]} current={r[2]} history={r[3]}"
            for r in mismatches[:5]
        ]
        msg = (
            f"{len(mismatches)} collateral status mismatch(es): "
            + "; ".join(details)
        )
        if len(mismatches) > 5:
            msg += f" ... and {len(mismatches) - 5} more"
        return False, msg

    cur.execute("SELECT COUNT(*) FROM collateral_positions")
    count = cur.fetchone()[0]
    return (
        True,
        f"All {count} collateral position(s) consistent "
        f"with status history.",
    )


def check_no_negative_asset_balances(cur):
    """Check 6: LENDER_POOL and COLLATERAL_CUSTODY balances >= 0."""
    cur.execute("""
        SELECT account_id, coa_code, currency,
               SUM(debit) AS total_debit,
               SUM(credit) AS total_credit
        FROM journal_entries
        WHERE coa_code IN (
            'LENDER_POOL', 'COLLATERAL_CUSTODY'
        )
        GROUP BY account_id, coa_code, currency
    """)
    rows = cur.fetchall()
    negatives = []
    for r in rows:
        coa_code = r[1]
        total_debit = Decimal(str(r[3]))
        total_credit = Decimal(str(r[4]))
        if coa_code == "LENDER_POOL":
            balance = total_credit - total_debit
        else:
            balance = total_debit - total_credit
        if balance < -TOLERANCE:
            negatives.append(
                f"account={r[0]} coa={coa_code} "
                f"currency={r[2]} balance={balance}"
            )

    if negatives:
        msg = (
            f"{len(negatives)} negative asset balance(s): "
            + "; ".join(negatives[:5])
        )
        if len(negatives) > 5:
            msg += f" ... and {len(negatives) - 5} more"
        return False, msg
    return (
        True,
        f"No negative balances on LENDER_POOL or "
        f"COLLATERAL_CUSTODY ({len(rows)} entries checked).",
    )


def check_loan_principal_consistency(cur):
    """Check 7: LOANS_RECEIVABLE ledger balance matches loan principal."""
    cur.execute("""
        SELECT l.id, l.loan_ref, l.principal,
               COALESCE(lr.net_receivable, 0) AS ledger_balance
        FROM loans l
        LEFT JOIN LATERAL (
            SELECT SUM(je.debit) - SUM(je.credit)
                   AS net_receivable
            FROM journal_entries je
            WHERE je.coa_code = 'LOANS_RECEIVABLE'
              AND je.reference_id = l.id
        ) lr ON TRUE
        WHERE l.status IN ('active', 'margin_call', 'liquidating')
          AND ABS(l.principal - COALESCE(lr.net_receivable, 0))
              > 0.000000000000000001
    """)
    mismatches = cur.fetchall()
    if mismatches:
        details = [
            f"loan={r[1]} principal={r[2]} ledger={r[3]}"
            for r in mismatches[:5]
        ]
        msg = (
            f"{len(mismatches)} principal/ledger mismatch(es): "
            + "; ".join(details)
        )
        if len(mismatches) > 5:
            msg += f" ... and {len(mismatches) - 5} more"
        return False, msg

    cur.execute("""
        SELECT COUNT(*) FROM loans
        WHERE status IN ('active', 'margin_call', 'liquidating')
    """)
    count = cur.fetchone()[0]
    return (
        True,
        f"All {count} active loan(s) match LOANS_RECEIVABLE "
        f"ledger entries.",
    )


def check_interest_accrual_continuity(cur):
    """Check 8: No gaps in daily accrual records for active loans."""
    cur.execute("""
        SELECT l.id, l.loan_ref,
               l.disbursed_at::date AS start_date,
               CURRENT_DATE AS end_date
        FROM loans l
        WHERE l.status IN ('active', 'margin_call', 'liquidating')
          AND l.disbursed_at IS NOT NULL
    """)
    active_loans = cur.fetchall()

    gaps_found = []
    for loan in active_loans:
        loan_id = loan[0]
        loan_ref = loan[1]
        start_date = loan[2]
        end_date = loan[3]

        cur.execute("""
            SELECT accrual_date
            FROM interest_accrual_events
            WHERE loan_id = %s
            ORDER BY accrual_date
        """, (str(loan_id),))
        accrual_dates = {r[0] for r in cur.fetchall()}

        if not accrual_dates:
            gaps_found.append(
                f"loan={loan_ref} has NO accrual records "
                f"(active since {start_date})"
            )
            continue

        cur.execute("""
            SELECT d::date
            FROM generate_series(%s::date, %s::date, '1 day') d
        """, (start_date, end_date))
        expected_dates = {r[0] for r in cur.fetchall()}
        missing = expected_dates - accrual_dates
        if missing:
            sorted_missing = sorted(missing)
            sample = [str(d) for d in sorted_missing[:3]]
            gaps_found.append(
                f"loan={loan_ref} missing {len(missing)} "
                f"accrual date(s): {', '.join(sample)}"
                + (
                    f" ... and {len(missing) - 3} more"
                    if len(missing) > 3
                    else ""
                )
            )

    if gaps_found:
        msg = (
            f"{len(gaps_found)} loan(s) with accrual gaps: "
            + "; ".join(gaps_found[:5])
        )
        if len(gaps_found) > 5:
            msg += f" ... and {len(gaps_found) - 5} more"
        return False, msg
    return (
        True,
        f"All {len(active_loans)} active loan(s) have "
        f"continuous accrual records.",
    )


ALL_CHECKS = [
    (
        "Journal pair balance (debit == credit per journal_id)",
        check_journal_pair_balance,
    ),
    (
        "Net zero across COA (total debits == total credits)",
        check_net_zero_across_coa,
    ),
    (
        "No orphan journal entries (valid account_id)",
        check_no_orphan_journal_entries,
    ),
    (
        "Loan status history consistency",
        check_loan_status_history,
    ),
    (
        "Collateral status history consistency",
        check_collateral_status_history,
    ),
    (
        "No negative balances on asset accounts",
        check_no_negative_asset_balances,
    ),
    (
        "Loan principal vs LOANS_RECEIVABLE consistency",
        check_loan_principal_consistency,
    ),
    (
        "Interest accrual continuity for active loans",
        check_interest_accrual_continuity,
    ),
]


def main():
    parser = argparse.ArgumentParser(
        description="Lending ledger integrity checker"
    )
    parser.add_argument(
        "--report-only",
        action="store_true",
        help="Print report and exit 0 even if issues found",
    )
    args = parser.parse_args()

    print(f"\n{BOLD}Lending & Collateral Ledger Integrity Checker{RESET}")
    print(f"Database: {DATABASE_URL.split('@')[-1]}")
    print("-" * 60)

    conn = _connect()
    cur = conn.cursor()

    checks_passed = 0
    checks_failed = 0
    issues = []

    try:
        for i, (label, check_fn) in enumerate(ALL_CHECKS, start=1):
            print(
                f"\n{BOLD}Check {i}: {label}{RESET}"
            )
            passed, message = check_fn(cur)
            if passed:
                checks_passed += 1
                print(f"  {PASS}  {message}")
            else:
                checks_failed += 1
                issues.append(f"Check {i}: {message}")
                print(f"  {FAIL}  {message}")
    finally:
        cur.close()
        conn.close()

    print(f"\n{'-' * 60}")
    print(f"  Checks passed : {BOLD}{checks_passed}{RESET}")
    print(f"  Checks failed : {BOLD}{checks_failed}{RESET}")

    if issues:
        print(f"\n{BOLD}Issues:{RESET}")
        for issue in issues:
            print(f"    {FAIL}  {issue}")

    if issues and not args.report_only:
        print(
            f"\n{BOLD}\033[31mLEDGER INTEGRITY FAILED"
            f" -- {len(issues)} issue(s) require "
            f"investigation.\033[0m\n"
        )
        sys.exit(1)
    else:
        print(f"\n{BOLD}\033[32mLedger integrity OK.\033[0m\n")


if __name__ == "__main__":
    main()
