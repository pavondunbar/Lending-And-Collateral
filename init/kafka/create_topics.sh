#!/bin/bash
set -e

KAFKA="kafka:9092"
REPLICATION=1
WAIT_SEC=20

echo "Waiting ${WAIT_SEC}s for Kafka to be fully ready..."
sleep $WAIT_SEC

create_topic() {
    local TOPIC=$1
    local PARTITIONS=${2:-4}
    local RETENTION_MS=${3:-604800000}   # 7 days default

    kafka-topics --bootstrap-server "$KAFKA" \
        --create --if-not-exists \
        --topic "$TOPIC" \
        --partitions "$PARTITIONS" \
        --replication-factor "$REPLICATION" \
        --config retention.ms="$RETENTION_MS" \
        --config cleanup.policy=delete \
        --config compression.type=lz4
    echo "Topic created: $TOPIC"
}

# -- Loan Lifecycle -------------------------------------------------------------------
create_topic "loan.originated"            4
create_topic "loan.disbursed"             4
create_topic "loan.repayment.received"    4
create_topic "loan.repayment.completed"   4
create_topic "loan.closed"                2
create_topic "loan.interest.accrued"      8   # high volume

# -- Collateral -----------------------------------------------------------------------
create_topic "collateral.deposited"       4
create_topic "collateral.withdrawn"       4
create_topic "collateral.substituted"     4
create_topic "collateral.valued"          8

# -- Margin Calls ---------------------------------------------------------------------
create_topic "margin.call.triggered"      4
create_topic "margin.call.met"            4
create_topic "margin.call.expired"        2   86400000   # 1 day retention

# -- Liquidation ----------------------------------------------------------------------
create_topic "liquidation.initiated"      4
create_topic "liquidation.executed"       4
create_topic "liquidation.completed"      4

# -- Price Feed -----------------------------------------------------------------------
create_topic "price.feed.updated"         4

# -- Compliance & Audit ---------------------------------------------------------------
create_topic "compliance.event"           4   2592000000  # 30 days
create_topic "audit.trail"                8   2592000000  # 30 days

# -- Signing --------------------------------------------------------------------------
create_topic "signing.request"            2
create_topic "signing.completed"          2

# -- Dead Letter Queues (DLQ) ----------------------------------------------------------
create_topic "loan.originated.dlq"            2 2592000000
create_topic "loan.disbursed.dlq"             2 2592000000
create_topic "loan.repayment.received.dlq"    2 2592000000
create_topic "loan.repayment.completed.dlq"   2 2592000000
create_topic "loan.closed.dlq"                2 2592000000
create_topic "loan.interest.accrued.dlq"      2 2592000000
create_topic "collateral.deposited.dlq"       2 2592000000
create_topic "collateral.withdrawn.dlq"       2 2592000000
create_topic "collateral.substituted.dlq"     2 2592000000
create_topic "collateral.valued.dlq"          2 2592000000
create_topic "margin.call.triggered.dlq"      2 2592000000
create_topic "margin.call.met.dlq"            2 2592000000
create_topic "margin.call.expired.dlq"        2 2592000000
create_topic "liquidation.initiated.dlq"      2 2592000000
create_topic "liquidation.executed.dlq"       2 2592000000
create_topic "liquidation.completed.dlq"      2 2592000000
create_topic "price.feed.updated.dlq"         2 2592000000
create_topic "compliance.event.dlq"           2 2592000000
create_topic "audit.trail.dlq"                2 2592000000
create_topic "signing.request.dlq"            2 2592000000
create_topic "signing.completed.dlq"          2 2592000000

# -- Settlement -------------------------------------------------------------------------
create_topic "settlement.created"             4 2592000000
create_topic "settlement.approved"            4 2592000000
create_topic "settlement.signed"              4 2592000000
create_topic "settlement.broadcasted"         4 2592000000
create_topic "settlement.confirmed"           4 2592000000
create_topic "settlement.created.dlq"         2 2592000000
create_topic "settlement.approved.dlq"        2 2592000000
create_topic "settlement.signed.dlq"          2 2592000000
create_topic "settlement.broadcasted.dlq"     2 2592000000
create_topic "settlement.confirmed.dlq"       2 2592000000

echo ""
echo "All Kafka topics created successfully."
kafka-topics --bootstrap-server "$KAFKA" --list
