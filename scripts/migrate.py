"""
scripts/migrate.py -- Lightweight schema migration runner.

Applies sequential SQL migration files from init/postgres/ in filename order.
Tracks applied migrations in a `schema_migrations` table.

Usage:
    DATABASE_URL=postgresql://lending:s3cr3t@localhost:5432/lending_db \
        python scripts/migrate.py

    python scripts/migrate.py --dry-run
"""

import argparse
import os
import sys
from pathlib import Path

try:
    import psycopg2
except ImportError:
    print(
        "psycopg2 not installed. Run: pip install psycopg2-binary"
    )
    sys.exit(1)


MIGRATIONS_DIR = Path(__file__).parent.parent / "init" / "postgres"
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://lending:s3cr3t@localhost:5432/lending_db",
)


def _dsn():
    """Convert SQLAlchemy-style URL to psycopg2 DSN if needed."""
    return DATABASE_URL.replace(
        "postgresql+psycopg2://", "postgresql://"
    )


def ensure_migrations_table(cur):
    """Create the schema_migrations tracking table if absent."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            id          SERIAL PRIMARY KEY,
            filename    VARCHAR(255) UNIQUE NOT NULL,
            applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
    """)


def get_applied(cur):
    """Return set of already-applied migration filenames."""
    cur.execute(
        "SELECT filename FROM schema_migrations ORDER BY filename"
    )
    return {row[0] for row in cur.fetchall()}


def run(dry_run=False):
    """Discover and apply pending migrations."""
    sql_files = sorted(MIGRATIONS_DIR.glob("*.sql"))
    if not sql_files:
        print("No migration files found in", MIGRATIONS_DIR)
        return

    conn = psycopg2.connect(_dsn())
    conn.autocommit = False
    cur = conn.cursor()

    try:
        ensure_migrations_table(cur)
        conn.commit()
        applied = get_applied(cur)

        pending = [f for f in sql_files if f.name not in applied]
        if not pending:
            print("All migrations already applied.")
            return

        print(f"Pending migrations: {len(pending)}")
        for f in pending:
            print(f"\n  -> {f.name}")
            sql = f.read_text()

            if dry_run:
                print("    [DRY RUN -- not applied]")
                preview = sql[:200].replace("\n", "\n    ")
                print(f"    {preview}...")
                continue

            cur.execute(sql)
            cur.execute(
                "INSERT INTO schema_migrations (filename) "
                "VALUES (%s)",
                (f.name,),
            )
            conn.commit()
            print("    Applied.")

        if not dry_run:
            print(
                f"\n{len(pending)} migration(s) applied "
                f"successfully."
            )

    except Exception as exc:
        conn.rollback()
        print(f"\nMigration failed: {exc}")
        sys.exit(1)
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Lending schema migration runner"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview without applying",
    )
    args = parser.parse_args()
    run(dry_run=args.dry_run)
