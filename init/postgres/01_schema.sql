-- =====================================================================================
-- Institutional Lending & Collateral Management — PostgreSQL Schema
-- Covers: crypto-backed loans, collateral custody, margin calls, liquidations,
--         interest accrual, price feeds, double-entry accounting
-- =====================================================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- --- Enums --------------------------------------------------------------------------

CREATE TYPE account_type       AS ENUM ('institutional', 'custodian', 'lending_pool', 'system');
CREATE TYPE loan_status        AS ENUM ('pending', 'approved', 'active', 'margin_call', 'liquidating', 'repaid', 'defaulted', 'closed');
CREATE TYPE collateral_status  AS ENUM ('pending_deposit', 'active', 'margin_hold', 'liquidating', 'released', 'seized');
CREATE TYPE margin_call_status AS ENUM ('triggered', 'met', 'expired', 'liquidation_initiated');
CREATE TYPE liquidation_status AS ENUM ('initiated', 'executing', 'completed', 'failed', 'partial');
CREATE TYPE price_source       AS ENUM ('coinbase', 'binance', 'kraken', 'chainlink', 'internal', 'aggregated');
CREATE TYPE asset_type         AS ENUM ('BTC', 'ETH', 'SOL', 'AVAX');
CREATE TYPE fiat_currency      AS ENUM ('USD', 'EUR', 'GBP');
CREATE TYPE interest_method    AS ENUM ('simple', 'compound_daily', 'compound_monthly');

-- --- Accounts -----------------------------------------------------------------------

CREATE TABLE accounts (
    id                      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_name             VARCHAR(255) NOT NULL,
    account_type            account_type NOT NULL,
    lei                     VARCHAR(20) UNIQUE,
    is_active               BOOLEAN NOT NULL DEFAULT TRUE,
    kyc_verified            BOOLEAN NOT NULL DEFAULT FALSE,
    aml_cleared             BOOLEAN NOT NULL DEFAULT FALSE,
    risk_tier               SMALLINT NOT NULL DEFAULT 3 CHECK (risk_tier BETWEEN 1 AND 5),
    metadata                JSONB NOT NULL DEFAULT '{}',
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ
);

CREATE INDEX idx_accounts_lei ON accounts(lei) WHERE lei IS NOT NULL;

-- --- Chart of Accounts (double-entry COA) -------------------------------------------

CREATE TABLE chart_of_accounts (
    code                    VARCHAR(30) PRIMARY KEY,
    name                    VARCHAR(255) NOT NULL,
    account_type            VARCHAR(20) NOT NULL,
    normal_balance          VARCHAR(10) NOT NULL
);

-- --- Journal Entries (immutable double-entry ledger) --------------------------------

CREATE TABLE journal_entries (
    id                      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    journal_id              UUID NOT NULL,
    account_id              UUID NOT NULL REFERENCES accounts(id),
    coa_code                VARCHAR(30) NOT NULL REFERENCES chart_of_accounts(code),
    currency                VARCHAR(10) NOT NULL,
    debit                   NUMERIC(38, 18) NOT NULL DEFAULT 0,
    credit                  NUMERIC(38, 18) NOT NULL DEFAULT 0,
    entry_type              VARCHAR(50) NOT NULL,
    reference_id            UUID,
    narrative               TEXT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_journal_entries_journal   ON journal_entries(journal_id);
CREATE INDEX idx_journal_entries_account   ON journal_entries(account_id, created_at DESC);
CREATE INDEX idx_journal_entries_reference ON journal_entries(reference_id);

-- --- Outbox Events (transactional outbox for Kafka relay) ---------------------------

CREATE TABLE outbox_events (
    id                      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    aggregate_id            VARCHAR(255) NOT NULL,
    event_type              VARCHAR(255) NOT NULL,
    payload                 JSONB NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at            TIMESTAMPTZ
);

CREATE INDEX idx_outbox_unpublished ON outbox_events(published_at) WHERE published_at IS NULL;

-- --- Escrow Holds -------------------------------------------------------------------

CREATE TABLE escrow_holds (
    id                      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    hold_ref                VARCHAR(64) NOT NULL,
    account_id              UUID NOT NULL REFERENCES accounts(id),
    currency                VARCHAR(10) NOT NULL,
    amount                  NUMERIC(38, 18) NOT NULL,
    hold_type               VARCHAR(10) NOT NULL,
    related_entity_type     VARCHAR(50),
    related_entity_id       UUID,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_escrow_holds_ref     ON escrow_holds(hold_ref);
CREATE INDEX idx_escrow_holds_account ON escrow_holds(account_id);

-- --- Loans --------------------------------------------------------------------------

CREATE TABLE loans (
    id                      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    loan_ref                VARCHAR(64) UNIQUE NOT NULL,
    borrower_id             UUID NOT NULL REFERENCES accounts(id),
    lender_pool_id          UUID NOT NULL REFERENCES accounts(id),
    currency                VARCHAR(10) NOT NULL DEFAULT 'USD',
    principal               NUMERIC(38, 18) NOT NULL CHECK (principal > 0),
    interest_rate_bps       INTEGER NOT NULL CHECK (interest_rate_bps >= 0),
    initial_ltv_pct         NUMERIC(8, 4) NOT NULL DEFAULT 65.0,
    maintenance_ltv_pct     NUMERIC(8, 4) NOT NULL DEFAULT 75.0,
    liquidation_ltv_pct     NUMERIC(8, 4) NOT NULL DEFAULT 85.0,
    interest_method         VARCHAR(20) NOT NULL DEFAULT 'compound_daily',
    origination_fee_bps     INTEGER NOT NULL DEFAULT 50,
    status                  loan_status NOT NULL DEFAULT 'pending',
    disbursed_at            TIMESTAMPTZ,
    maturity_date           TIMESTAMPTZ,
    closed_at               TIMESTAMPTZ,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata                JSONB NOT NULL DEFAULT '{}'
);

CREATE INDEX idx_loans_loan_ref    ON loans(loan_ref);
CREATE INDEX idx_loans_borrower    ON loans(borrower_id);
CREATE INDEX idx_loans_status      ON loans(status);

-- --- Collateral Positions -----------------------------------------------------------

CREATE TABLE collateral_positions (
    id                      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    collateral_ref          VARCHAR(64) UNIQUE NOT NULL,
    loan_id                 UUID NOT NULL REFERENCES loans(id),
    asset_type              VARCHAR(10) NOT NULL,
    quantity                NUMERIC(28, 8) NOT NULL CHECK (quantity > 0),
    haircut_pct             NUMERIC(8, 4) NOT NULL DEFAULT 10.0,
    custodian_id            UUID REFERENCES accounts(id),
    status                  collateral_status NOT NULL DEFAULT 'pending_deposit',
    deposited_at            TIMESTAMPTZ,
    released_at             TIMESTAMPTZ,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_collateral_loan   ON collateral_positions(loan_id);
CREATE INDEX idx_collateral_status ON collateral_positions(status);

-- --- Price Feeds (immutable time-series) --------------------------------------------

CREATE TABLE price_feeds (
    id                      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    asset_type              VARCHAR(10) NOT NULL,
    price_usd               NUMERIC(28, 8) NOT NULL CHECK (price_usd > 0),
    source                  VARCHAR(50) NOT NULL,
    volume_24h              NUMERIC(28, 8),
    is_valid                BOOLEAN NOT NULL DEFAULT TRUE,
    recorded_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_price_feeds_asset ON price_feeds(asset_type, recorded_at DESC);

-- --- Margin Calls -------------------------------------------------------------------

CREATE TABLE margin_calls (
    id                              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    margin_call_ref                 VARCHAR(64) UNIQUE NOT NULL,
    loan_id                         UUID NOT NULL REFERENCES loans(id),
    triggered_ltv                   NUMERIC(8, 4) NOT NULL,
    current_collateral_value        NUMERIC(38, 18) NOT NULL,
    required_additional_collateral  NUMERIC(38, 18) NOT NULL,
    deadline                        TIMESTAMPTZ NOT NULL,
    status                          margin_call_status NOT NULL DEFAULT 'triggered',
    met_at                          TIMESTAMPTZ,
    expired_at                      TIMESTAMPTZ,
    created_at                      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_margin_calls_loan   ON margin_calls(loan_id);
CREATE INDEX idx_margin_calls_status ON margin_calls(status);

-- --- Liquidation Events -------------------------------------------------------------

CREATE TABLE liquidation_events (
    id                      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    liquidation_ref         VARCHAR(64) UNIQUE NOT NULL,
    loan_id                 UUID NOT NULL REFERENCES loans(id),
    trigger_ltv             NUMERIC(8, 4) NOT NULL,
    collateral_sold_qty     NUMERIC(28, 8),
    collateral_sold_asset   VARCHAR(10),
    sale_price_usd          NUMERIC(28, 8),
    total_proceeds          NUMERIC(38, 18),
    interest_recovered      NUMERIC(38, 18) NOT NULL DEFAULT 0,
    principal_recovered     NUMERIC(38, 18) NOT NULL DEFAULT 0,
    fees_recovered          NUMERIC(38, 18) NOT NULL DEFAULT 0,
    borrower_remainder      NUMERIC(38, 18) NOT NULL DEFAULT 0,
    status                  liquidation_status NOT NULL DEFAULT 'initiated',
    executed_at             TIMESTAMPTZ,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_liquidation_loan ON liquidation_events(loan_id);

-- --- Interest Accrual Events (immutable daily log) ----------------------------------

CREATE TABLE interest_accrual_events (
    id                      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    loan_id                 UUID NOT NULL REFERENCES loans(id),
    accrual_date            DATE NOT NULL,
    principal_balance       NUMERIC(38, 18) NOT NULL,
    daily_rate              NUMERIC(28, 18) NOT NULL,
    accrued_amount          NUMERIC(38, 18) NOT NULL,
    cumulative_interest     NUMERIC(38, 18) NOT NULL,
    journal_id              UUID,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (loan_id, accrual_date)
);

CREATE INDEX idx_interest_accrual_loan ON interest_accrual_events(loan_id, accrual_date);

-- --- Status History Tables (append-only audit trails) --------------------------------

CREATE TABLE loan_status_history (
    id                      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    loan_id                 UUID NOT NULL REFERENCES loans(id),
    status                  VARCHAR(20) NOT NULL,
    detail                  JSONB,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_loan_status_history_loan ON loan_status_history(loan_id);

CREATE TABLE collateral_status_history (
    id                      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    collateral_id           UUID NOT NULL REFERENCES collateral_positions(id),
    status                  VARCHAR(20) NOT NULL,
    detail                  JSONB,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_collateral_status_history ON collateral_status_history(collateral_id);

CREATE TABLE margin_call_status_history (
    id                      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    margin_call_id          UUID NOT NULL REFERENCES margin_calls(id),
    status                  VARCHAR(20) NOT NULL,
    detail                  JSONB,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_margin_call_status_history ON margin_call_status_history(margin_call_id);

CREATE TABLE liquidation_status_history (
    id                      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    liquidation_id          UUID NOT NULL REFERENCES liquidation_events(id),
    status                  VARCHAR(20) NOT NULL,
    detail                  JSONB,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_liquidation_status_history ON liquidation_status_history(liquidation_id);

-- --- Compliance Events --------------------------------------------------------------

CREATE TABLE compliance_events (
    id                      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_type             VARCHAR(50) NOT NULL,
    entity_id               UUID NOT NULL,
    event_type              VARCHAR(50) NOT NULL,
    result                  VARCHAR(20) NOT NULL,
    score                   NUMERIC(5, 2),
    details                 JSONB NOT NULL DEFAULT '{}',
    checked_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_compliance_entity ON compliance_events(entity_type, entity_id);

-- --- Chart of Accounts Seed Data ----------------------------------------------------

INSERT INTO chart_of_accounts (code, name, account_type, normal_balance) VALUES
    ('LENDER_POOL',          'Lender Pool Capital',       'liability', 'credit'),
    ('LOANS_RECEIVABLE',     'Outstanding Loans',         'asset',     'debit'),
    ('INTEREST_RECEIVABLE',  'Accrued Interest',          'asset',     'debit'),
    ('INTEREST_REVENUE',     'Interest Revenue',          'revenue',   'credit'),
    ('COLLATERAL_CUSTODY',   'Collateral in Custody',     'asset',     'debit'),
    ('BORROWER_COLLATERAL',  'Borrower Collateral Claim', 'liability', 'credit'),
    ('LIQUIDATION_PROCEEDS', 'Liquidation Proceeds',      'asset',     'debit'),
    ('FEE_REVENUE',          'Fee Revenue',               'revenue',   'credit'),
    ('INSURANCE_RESERVE',    'Insurance Reserve',          'liability', 'credit'),
    ('BORROWER_LIABILITY',   'Borrower Loan Liability',   'liability', 'credit');

-- =====================================================================================
-- Immutability Triggers — reject UPDATE/DELETE on append-only tables
-- =====================================================================================

CREATE OR REPLACE FUNCTION reject_modification()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION
        'Modifications to % are not allowed — append-only table',
        TG_TABLE_NAME;
END;
$$ LANGUAGE plpgsql;

-- journal_entries: fully immutable
CREATE TRIGGER trg_journal_entries_immutable
    BEFORE UPDATE OR DELETE ON journal_entries
    FOR EACH ROW EXECUTE FUNCTION reject_modification();

-- price_feeds: fully immutable
CREATE TRIGGER trg_price_feeds_immutable
    BEFORE UPDATE OR DELETE ON price_feeds
    FOR EACH ROW EXECUTE FUNCTION reject_modification();

-- interest_accrual_events: fully immutable
CREATE TRIGGER trg_interest_accrual_immutable
    BEFORE UPDATE OR DELETE ON interest_accrual_events
    FOR EACH ROW EXECUTE FUNCTION reject_modification();

-- escrow_holds: fully immutable
CREATE TRIGGER trg_escrow_holds_immutable
    BEFORE UPDATE OR DELETE ON escrow_holds
    FOR EACH ROW EXECUTE FUNCTION reject_modification();

-- loan_status_history: fully immutable
CREATE TRIGGER trg_loan_status_history_immutable
    BEFORE UPDATE OR DELETE ON loan_status_history
    FOR EACH ROW EXECUTE FUNCTION reject_modification();

-- collateral_status_history: fully immutable
CREATE TRIGGER trg_collateral_status_history_immutable
    BEFORE UPDATE OR DELETE ON collateral_status_history
    FOR EACH ROW EXECUTE FUNCTION reject_modification();

-- margin_call_status_history: fully immutable
CREATE TRIGGER trg_margin_call_status_history_immutable
    BEFORE UPDATE OR DELETE ON margin_call_status_history
    FOR EACH ROW EXECUTE FUNCTION reject_modification();

-- liquidation_status_history: fully immutable
CREATE TRIGGER trg_liquidation_status_history_immutable
    BEFORE UPDATE OR DELETE ON liquidation_status_history
    FOR EACH ROW EXECUTE FUNCTION reject_modification();

-- outbox_events: allow UPDATE only on published_at
CREATE OR REPLACE FUNCTION reject_outbox_modification()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'Deletes on outbox_events are not allowed';
    END IF;
    IF TG_OP = 'UPDATE' THEN
        IF OLD.aggregate_id IS DISTINCT FROM NEW.aggregate_id
           OR OLD.event_type IS DISTINCT FROM NEW.event_type
           OR OLD.payload IS DISTINCT FROM NEW.payload THEN
            RAISE EXCEPTION
                'Only published_at may be updated on outbox_events';
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_outbox_events_immutable
    BEFORE UPDATE OR DELETE ON outbox_events
    FOR EACH ROW EXECUTE FUNCTION reject_outbox_modification();

-- =====================================================================================
-- Status-Sync Triggers — AFTER INSERT on history tables updates parent status
-- =====================================================================================

CREATE OR REPLACE FUNCTION sync_loan_status()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE loans
       SET status = NEW.status::loan_status
     WHERE id = NEW.loan_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_sync_loan_status
    AFTER INSERT ON loan_status_history
    FOR EACH ROW EXECUTE FUNCTION sync_loan_status();

CREATE OR REPLACE FUNCTION sync_collateral_status()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE collateral_positions
       SET status = NEW.status::collateral_status
     WHERE id = NEW.collateral_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_sync_collateral_status
    AFTER INSERT ON collateral_status_history
    FOR EACH ROW EXECUTE FUNCTION sync_collateral_status();

CREATE OR REPLACE FUNCTION sync_margin_call_status()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE margin_calls
       SET status = NEW.status::margin_call_status
     WHERE id = NEW.margin_call_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_sync_margin_call_status
    AFTER INSERT ON margin_call_status_history
    FOR EACH ROW EXECUTE FUNCTION sync_margin_call_status();

CREATE OR REPLACE FUNCTION sync_liquidation_status()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE liquidation_events
       SET status = NEW.status::liquidation_status
     WHERE id = NEW.liquidation_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_sync_liquidation_status
    AFTER INSERT ON liquidation_status_history
    FOR EACH ROW EXECUTE FUNCTION sync_liquidation_status();

-- =====================================================================================
-- Deferred Balance Constraint — ensures journal entries balance within a transaction
-- =====================================================================================

CREATE OR REPLACE FUNCTION check_journal_balance()
RETURNS TRIGGER AS $$
DECLARE
    total_debit  NUMERIC(38, 18);
    total_credit NUMERIC(38, 18);
BEGIN
    SELECT COALESCE(SUM(debit), 0), COALESCE(SUM(credit), 0)
      INTO total_debit, total_credit
      FROM journal_entries
     WHERE journal_id = NEW.journal_id;

    IF ABS(total_debit - total_credit) > 0.000000000000000001 THEN
        RAISE EXCEPTION
            'Journal % is unbalanced: debit=% credit=%',
            NEW.journal_id, total_debit, total_credit;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER trg_check_journal_balance
    AFTER INSERT ON journal_entries
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION check_journal_balance();

-- =====================================================================================
-- Views
-- =====================================================================================

-- Account balances derived from the double-entry journal
CREATE VIEW account_balances AS
SELECT
    je.account_id,
    je.coa_code,
    je.currency,
    SUM(je.credit) - SUM(je.debit) AS balance
FROM journal_entries je
GROUP BY je.account_id, je.coa_code, je.currency;

-- Net escrow holds per account (reserve minus release)
CREATE VIEW active_collateral_holds AS
SELECT
    eh.account_id,
    eh.currency,
    SUM(CASE WHEN eh.hold_type = 'reserve' THEN eh.amount ELSE 0 END)
  - SUM(CASE WHEN eh.hold_type = 'release' THEN eh.amount ELSE 0 END) AS held_amount
FROM escrow_holds eh
GROUP BY eh.account_id, eh.currency;

-- Available balances = account balance minus active holds
CREATE VIEW account_available_balances AS
SELECT
    ab.account_id,
    ab.coa_code,
    ab.currency,
    ab.balance,
    COALESCE(ach.held_amount, 0) AS held_amount,
    ab.balance - COALESCE(ach.held_amount, 0) AS available_balance
FROM account_balances ab
LEFT JOIN active_collateral_holds ach
    ON ach.account_id = ab.account_id
   AND ach.currency = ab.currency;

-- Current loan status from the latest history entry
CREATE VIEW loan_current_status AS
SELECT DISTINCT ON (lsh.loan_id)
    lsh.loan_id,
    lsh.status,
    lsh.detail,
    lsh.created_at
FROM loan_status_history lsh
ORDER BY lsh.loan_id, lsh.created_at DESC;

-- Current collateral status from the latest history entry
CREATE VIEW collateral_current_status AS
SELECT DISTINCT ON (csh.collateral_id)
    csh.collateral_id,
    csh.status,
    csh.detail,
    csh.created_at
FROM collateral_status_history csh
ORDER BY csh.collateral_id, csh.created_at DESC;

-- Latest valid price per asset
CREATE VIEW latest_prices AS
SELECT DISTINCT ON (pf.asset_type)
    pf.asset_type,
    pf.price_usd,
    pf.source,
    pf.volume_24h,
    pf.recorded_at
FROM price_feeds pf
WHERE pf.is_valid = TRUE
ORDER BY pf.asset_type, pf.recorded_at DESC;

-- Loan collateral valuation with current LTV
CREATE VIEW loan_collateral_valuation AS
SELECT
    l.id                    AS loan_id,
    l.loan_ref,
    l.principal,
    l.currency,
    l.status                AS loan_status,
    l.initial_ltv_pct,
    l.maintenance_ltv_pct,
    l.liquidation_ltv_pct,
    cp.id                   AS collateral_id,
    cp.collateral_ref,
    cp.asset_type,
    cp.quantity,
    cp.haircut_pct,
    cp.status               AS collateral_status,
    lp.price_usd,
    lp.recorded_at          AS price_recorded_at,
    (cp.quantity * lp.price_usd)                           AS gross_collateral_value,
    (cp.quantity * lp.price_usd * (1 - cp.haircut_pct / 100)) AS net_collateral_value,
    CASE
        WHEN (cp.quantity * lp.price_usd * (1 - cp.haircut_pct / 100)) > 0
        THEN (l.principal / (cp.quantity * lp.price_usd * (1 - cp.haircut_pct / 100))) * 100
        ELSE NULL
    END                     AS current_ltv_pct
FROM loans l
JOIN collateral_positions cp ON cp.loan_id = l.id
LEFT JOIN latest_prices lp   ON lp.asset_type = cp.asset_type;

-- =====================================================================================
-- Seed: System Accounts
-- =====================================================================================

INSERT INTO accounts (id, entity_name, account_type, kyc_verified, aml_cleared, risk_tier)
VALUES
    ('00000000-0000-0000-0000-000000000001', 'SYSTEM_LENDING_POOL', 'lending_pool', TRUE, TRUE, 1),
    ('00000000-0000-0000-0000-000000000002', 'SYSTEM_CUSTODY',      'custodian',    TRUE, TRUE, 1),
    ('00000000-0000-0000-0000-000000000003', 'SYSTEM_INSURANCE',    'system',       TRUE, TRUE, 1);
