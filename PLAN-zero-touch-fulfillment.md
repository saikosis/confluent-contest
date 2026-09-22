# Zero-Touch Fulfillment (Touchless Order Flow)

## Business impact narrative
Amazon-style fulfillment: the overwhelming majority of orders should go from
"click" to "picked in a warehouse" with **zero human touch** — a person only
gets involved for the genuine exceptions (no stock anywhere, or a risky order).
Today that routing/approval logic usually runs in nightly batch ERP jobs, so
orders sit for hours before anyone even knows a warehouse is out of stock.
Streaming it collapses that to seconds: faster "your order is on its way"
confirmations, fewer backorders that surface too late, and ops headcount that
scales with the *exception rate*, not with total order volume.

**Headline metrics to report:** zero-touch rate (% auto-approved with no human
step), order → warehouse-dispatch latency, time-to-detect backorder.

## Architecture
```
Datagen Source (orders)          Datagen Source (inventory)
        │                                 │
        ▼                                 ▼
topic: orders.placed             topic: inventory.snapshot
  (Schema Registry: Avro)          (Schema Registry: Avro)
        │                                 │
        └────────────┬────────────────────┘
                      ▼
        Flink SQL Job 1 — Availability & Routing
        (temporal join order.sku → inventory by warehouse,
         pick nearest warehouse with qty_on_hand >= qty)
                      │
          ┌───────────┴───────────┐
          ▼                       ▼
  topic: orders.routed     topic: orders.backordered
  (stock found)             (no warehouse has stock → EXCEPTION)
          │
          ▼
        Flink SQL Job 2 — Auto-Approval Gate
        (rule: order_value > $X AND account_age < N days
               OR billing/shipping mismatch → review;
               else → auto-approve)
                     │
               ┌──────┴──────┐
               ▼             ▼
            topic:        topic:
            orders.approved   orders.review   (EXCEPTION — human touch)
               |                 |
               │                 ▼
               ├──▶ HTTP Sink Connector → webhook (pick/pack/ship trigger or Human-Review)
               
 ```