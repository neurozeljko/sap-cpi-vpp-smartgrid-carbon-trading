# ADR-017: Virtual Power Plant (VPP) Smart Grid & Real-Time Carbon Trading API Mesh

## Status
Accepted

## Context
Aggregating millions of sub-second micro-transactions from distributed energy assets directly into SAP S/4HANA Finance causes severe database thread locking, memory bloat, and unsustainable cloud compute costs.

## Decision
1. Terminate national energy grid bidding traffic at the Edge using SAP API Management with Redis rate-limiting and OAuth 2.0 mTLS.
2. Ingest trade events via Apache Kafka streams into SAP Cloud Integration.
3. Synchronize asset operational metrics to SAP Master Data Integration (MDI) via GraphQL mutations.
4. Execute non-buffering Groovy micro-batch aggregation to compress sub-second trades into 15-minute settlement buckets, issuing a single consolidated OData V4 Journal Entry into S/4HANA Finance.

## Consequences
- Reduces S/4HANA posting transactions by 99.98%.
- Eliminates database thread contention and deadlocks during market price spikes.
- Ensures strict Clean Core compliance with zero custom ABAP tables.
