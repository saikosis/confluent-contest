# Zero-Touch Fulfillment — Schemas, Sample Data & Partitioning
### Companion to PLAN-zero-touch-fulfillment.md

## Topics, keys & partition counts

| Topic | Key | Partitions | Why |
|---|---|---|---|
| `orders.placed` | `order_id` | 6 (default) | Key by `order_id` so every event for one order lands on the same partition — preserves per-order ordering through the pipeline. |
| `inventory.snapshot` | `sku` | 6 (default) | Key by `sku` so all stock updates for a SKU are ordered — required for the Flink temporal join to see a consistent view. |
| `orders.routed` / `orders.backordered` | `order_id` | 6 (default) | Carry the same key forward from `orders.placed`. |
| `orders.approved` / `orders.review` | `order_id` | 6 (default) | Same reasoning — one order's lifecycle stays traceable via its key. |

**Take the default (6) for all of them.** At this data volume (a Datagen demo,
low events/sec) 6 partitions is already more parallelism than you'll use.
Bump partition count only if you later push sustained throughput near
~10 MB/s per partition, or explicitly raise Flink job parallelism past 6 and
need the extra consumer parallelism to matter. Kafka partition *count* doesn't
need to match across topics for a Flink SQL join (Flink repartitions
internally) — the *key* consistency is what matters, not the number.

Schema Registry subject naming: leave default `TopicNameStrategy`
(`<topic>-value`) — no reason to hand-roll subject names for this scope.

## 1. `orders.placed` — Datagen custom schema
Avro Random Generator schema (paste into the Datagen connector's "Data" →
custom schema field). Event time comes from the Kafka record timestamp — no
need for a `ts` field in the payload (Flink reads it via `METADATA FROM
'timestamp'`).
```json
{
  "namespace": "contest.fulfillment",
  "name": "orders_placed",
  "type": "record",
  "fields": [
    { "name": "order_id", "type": { "type": "string", "arg.properties": { "options": ["ORD-000123","ORD-000456","ORD-000789","ORD-001011","ORD-001213","ORD-001415","ORD-001617","ORD-001819"] } } },
    { "name": "customer_id", "type": { "type": "string", "arg.properties": { "options": ["CUST-0001","CUST-0002","CUST-0003","CUST-0004","CUST-0005","CUST-0006","CUST-0007","CUST-0008"] } } },
    { "name": "sku", "type": { "type": "string", "arg.properties": { "options": ["SKU-1001","SKU-1002","SKU-1003","SKU-1004","SKU-1005","SKU-1006","SKU-1007","SKU-1008","SKU-1009","SKU-1010"] } } },
    { "name": "qty", "type": { "type": "int", "arg.properties": { "range": { "min": 1, "max": 5 } } } },
    { "name": "order_value", "type": { "type": "double", "arg.properties": { "range": { "min": 10.0, "max": 999.99 } } } },
    { "name": "account_age_days", "type": { "type": "int", "arg.properties": { "range": { "min": 0, "max": 2000 } } } },
    { "name": "billing_zip", "type": { "type": "string", "arg.properties": { "options": ["10001","94105","60601","30301","75201","98101","33101","80202"] } } },
    { "name": "shipping_zip", "type": { "type": "string", "arg.properties": { "options": ["10001","94105","60601","30301","75201","98101","33101","80202"] } } }
  ]
}
```
Datagen connector config: topic `orders.placed`, key format Avro, **Schema
key field** (`schema.keyfield`) set to `order_id`, `max.interval=1000` (1
record/sec is plenty for a demo).

Sample generated record:
```json
{"order_id":"ORD-482913","customer_id":"CUST-7741","sku":"SKU-1003","qty":2,
 "order_value":247.50,"account_age_days":12,"billing_zip":"94105","shipping_zip":"10001"}
```

## 2. `inventory.snapshot` — Datagen custom schema
Uses the **same `sku` value pool** as `orders.placed` so the join actually
matches.
```json
{
  "namespace": "contest.fulfillment",
  "name": "inventory_snapshot",
  "type": "record",
  "fields": [
    { "name": "warehouse_id", "type": { "type": "string", "arg.properties": { "options": ["WH-EAST","WH-WEST","WH-CENTRAL"] } } },
    { "name": "sku", "type": { "type": "string", "arg.properties": { "options": ["SKU-1001","SKU-1002","SKU-1003","SKU-1004","SKU-1005","SKU-1006","SKU-1007","SKU-1008","SKU-1009","SKU-1010"] } } },
    { "name": "qty_on_hand", "type": { "type": "int", "arg.properties": { "range": { "min": 0, "max": 500 } } } },
    { "name": "zip", "type": { "type": "string", "arg.properties": { "options": ["10001","94105","60601","30301","75201","98101","33101","80202"] } } }
  ]
}
```
Sample generated record:
```json
{"warehouse_id":"WH-EAST","sku":"SKU-1003","qty_on_hand":42,"zip":"10001"}
```
Datagen connector config: topic `inventory.snapshot`, key format Avro,
**Schema key field** (`schema.keyfield`) set to `sku`.


