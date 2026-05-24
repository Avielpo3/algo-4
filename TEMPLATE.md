# Inbound API Gateway

<!--
Business-side service definition. This is a non-technical, business-first
description of what the service does, how it fits into store operations, and
what data flows in and out of it.

Keep section headings and subsection structure stable — AI agents cross-read
these docs to detect overlaps, orphan events (emitted but never consumed),
and missing data dependencies. Renaming or removing top-level sections will
break those checks.

Add content under each section. Leave a section in place even if it's empty —
write "_None._" rather than deleting the heading.
-->

**Owner team:** Algo 4 Platform
**Status:** Draft
**Last updated:** 2026-05-24

---

## 1. Purpose

The Inbound API Gateway is the single entry point through which **external systems** push events into the Algo 4 cluster — brand POS, vendor KDS, brand Labor Management, brand VIP / loyalty, cloud order publishers, third-party holding-cabinet hardware, in-store vision cameras, and in-store GPS trackers. For every inbound call it authenticates the caller, captures the raw payload for audit, validates, normalizes the source-specific shape into a canonical Algo event, and publishes it onto the internal EventBus. POS-style flows keep their legacy `200 OK + POS response body` contract so Algo 3 brand POS clients keep working unchanged. On the POS order-entry path the gateway also makes two synchronous calls — to Order Management (commit) and AI Promise Time (promise time) — folds both into the POS response, and then publishes a single **enriched** canonical event for the rest of the cluster to consume async.

Menu Management is intentionally **out of scope** for this iteration.

## 2. Scope

### In Scope

- Terminate every external HTTP `POST` / `PUT` / `PATCH` Algo 3 receives today (`/PosInsert*`, v2 `/api/order*`, `/api/items/*`, `/UpdateOrderItemAnalysis*`, `/CarrierLocation`, `/updateOrderPayment`, `/api/order/vip*`, etc.).
- Terminate the external RabbitMQ queues the cluster consumes today (`{routingKey}-cabinet`, `{routingKey}-cloud-orders`) and convert them into inbound events of the same shape as the HTTP path.
- Capture raw request body, headers, transport, source identity, and timestamps for every inbound message.
- Authenticate callers through the (TBD) Auth Service and attach `sourceSystem` identity to the canonical event.
- Validate header / size / schema / store-scoping rules before publishing.
- Normalize into the canonical Algo event envelope and publish to EventBus business topics with stable partition keys.
- Honor the POS-compatible HTTP response contract (`200 OK` with POS body; `415` only on `/PosInsertNotification` missing `application/json`).
- Track each message through `RECEIVED → RAW_STORED → VALIDATED → NORMALIZED → PUBLISHED`, route `REJECTED` / `PUBLISH_FAILED` to audit / retry / DLQ.
- On `POST /PosInsertOrders`: sync-call Order Management *and* AI Promise Time, combine results into the POS body, then publish the **enriched** canonical event.
- Sync-call Order Management on other POS-compatible flows that fold OM state into the body (`GetOrderUpdatesForPos`, `GetPosNotification`).
- Enforce per-source idempotency (`Idempotency-Key` header, `sourceEventId`, or `(sourceSystem, storeId, eventType, sourceEventId)` tuple).

### Out of Scope

- **Menu Management** — deferred to a later iteration (`kfcmenuservice` webhooks, `PosInsertItemProperties`, `combos`).
- Any HTTP route where Algo's own components call Algo's own backend (makeline FE, Process Center, line-management, dispatch UI, support tooling) — those become in-cluster RPC or direct event subscription, not gateway events.
- The business logic of any downstream consumer (Quote, Customer, Employee, Order Management, AI Promise Time, Cabinet UI, vision, etc.).
- Mobile devices, DaaS Gateway, Admin Panel UI — owned by the Outbound (Cloud) Gateway.
- Async aggregator callbacks (DoorDash / Uber / Wolt) — those enter through the Outbound Gateway's DaaS EventBus consumer and republish on the same internal topics.
- The GPS / Traffilog *pull* loop (`IgnitionEvent.go`) — Algo polls an external server; nothing is pushed in.
- Long-poll / SSE / WebSocket fan-out to in-store screens — owned by the makeline / FE service.
- Calling Quote, Customer, Employee, or Couriers synchronously — they are bus consumers, never sync collaborators.
- Final auth scheme, raw-storage tech, retention policy, final topic naming — tracked as open decisions.

## 3. Role in Store Operations

### Order Entry

Front door for orders entering the store from outside: brand POS `POST /PosInsertOrders`, the cloud-order RabbitMQ queue (`{brand}-{country}-{externalStoreId}-cloud-orders`), POS reconciliation polls (`/GetOrderUpdatesForPos`, `/GetPosNotification`), VIP `/api/order/vip*`, and payment updates (`/updateOrderPayment`). On the POS-sync write path the gateway captures raw, validates, sync-calls Order Management *and* AI Promise Time, folds both into the POS body, returns `200 OK`, and *then* publishes the **enriched** canonical event so async consumers (Quote, Customer, Employee, Couriers, makeline projection, Ticket Service, Order Sequencer, Delivery Management, Cloud Gateway) see the same view OM did. The cloud-order queue path has no caller to respond to, so the sync calls are skipped and OM consumes the event off the bus like everyone else. Only the brand POS ever sees the promise time in the response.

### Order Modification / Cancellation

Same `/PosInsertOrders` channel with the order id reused, the `/GetOrderUpdatesForPos` poll, and the vendor-KDS v2 routes that report kitchen-side lifecycle (`POST /api/order/actions`, `POST /api/orders/{orderID}`, `PATCH /api/orders/{orderID}`, `PATCH /api/items/{itemID}`, multi-order `POST /api/orders/actions`). Aggregator-driven cancellations come through the Outbound Gateway, not here.

### Kitchen Preparation & Bumping

Vendor KDS over the v2 JWT API (`X-Station-ID` header): per-order actions, multi-order batch actions, order patches (`onMakeLine`, `slot`, `line`), item patches (`waiting | onMakeLine | ready | canceled | parked`). Also the POS→KDS push `POST /PosInsertNotification` (the one route that enforces `Content-Type: application/json` and returns `415` otherwise). Cabinet hardware publishes on its RabbitMQ queue and is consumed there. Pack-station, makeline-matcher, and table-cleanliness cameras each have their own HTTP routes. Legacy Algo makeline FE routes are internal traffic and excluded.

### Delivery / Handoff

Not the dispatch boundary — driver-side, customer-side, and DaaS-side delivery traffic flow through the Outbound Gateway. The only delivery-relevant external traffic seen here is in-store hardware: GPS / Traffilog trackers (`POST /CarrierLocation`) and POS vehicle assignment (`PUT /PosVehicleAssignment`).

### Day Open / Day Close

No dedicated route. The day-start signal is implicit: brand POS sends an initial `fullLoad=true` `PosInsertOrders` / `PosInsertEmployees` / `PosInsertEmployeesScheduling` / `PosInsertPermissionGroups`. The gateway forwards each as a normal inbound event; downstream services treat `fullLoad` as the snapshot signal. Recovery after restart relies on EventBus consumer-group offsets and the raw audit store for replay.

### Exception Flows

- Brand POS retries `PosInsertOrders` / `updateOrderPayment` — handled by the idempotency layer.
- Vendor KDS sends a stale `If-Match` — preserved on the canonical event; HTTP edge returns `409`.
- Bad camera grade on `/UpdateOrderItemAnalysis*` — published verbatim, no interpretation at the gateway.
- `415` on missing JSON `Content-Type` is enforced *only* on `/PosInsertNotification`; everywhere else, validation failures return `200 OK` with a POS-failure `status` (`validationError`, `parseError`, `readError`, `dbError`, `internalError`, `unexpectedError`).
- POS outage is invisible to the gateway — no inbound traffic from that POS for a while; downstream services notice via heartbeats.

## 4. Events Consumed (Triggers)

<!--
Every event below maps to an HTTP route or RabbitMQ queue where the SENDER is
outside the Algo cluster. Full per-route detail lives in
docs/inbound-api-gateway/README.md.
-->

### `orders.intake.received`

**Source service:** Brand POS (XML or JSON), cloud order publishers.
**Why we consume it:** Single ingest path for every order entering the store, whether typed at the till or pushed from a cloud aggregator.
**Action on receipt:** Capture raw, validate `storeNo` + shape, normalize each order keyed by `(storeNo, orderId)`. POS-sync path: sync-call Order Management *and* AI Promise Time, fold both into the POS body, return `200 OK`, then publish the **enriched** canonical event. Cloud-order RabbitMQ path: publish the canonical event directly.
**Notes:** Idempotent on `(storeNo, orderId, sourceEventId)`. `fullLoad=true` is the day-open snapshot.

### `admin.notification.posted`

**Source service:** Brand POS pushing an operator message to selected KDS stations.
**Action on receipt:** Capture, validate, publish for the makeline / FE projection to fan out over WebSocket.
**Notes:** **Only** inbound route that strictly enforces `Content-Type: application/json` — missing or wrong content type returns `415`. Preserve verbatim.

### `orders.payment.updated`

**Source service:** Brand POS / payment terminal bridge (`POST /updateOrderPayment`).
**Action on receipt:** Normalize keyed by `(storeId, orderId, paymentId)` and publish for Order Management. Idempotent on `paymentId`.

### `customer.vip.linked` / `customer.vip.unlinked`

**Source service:** Brand VIP / loyalty integrations (`POST|DELETE /api/order/vip` and `/api/order/vip/*`).
**Action on receipt:** Publish VIP-link / unlink keyed by `customerId` (fall back to `vipKey`). Customer Service and Order Management subscribe.

### `courier.profile.upserted`, `courier.schedule.upserted`

**Source service:** Brand Labor Management (`POST /PosInsertEmployees`, `POST /PosInsertEmployeesScheduling`).
**Action on receipt:** Normalize per-courier (and per-day shift) keyed by `(storeId, courierId)`. `FullLoad=true` is a roster replace.

### `admin.permission_group.upserted`

**Source service:** Brand labor / IT (`POST /PosInsertPermissionGroups`).
**Action on receipt:** Publish permission-group definitions for Employee Service / Auth.
**Notes:** Today's only inbound surface is the POS bulk snapshot — no granular permission-group-management API exists yet (see §9).

### `orders.kitchen.action_recorded` and `orders.kitchen.batch_action_recorded`

**Source service:** Vendor KDS via API v2 (`POST /api/order/actions`, `POST /api/orders/{orderID}`, multi-order `POST /api/orders/actions`).
**Action on receipt:** One canonical event per `(orderId, action)` partitioned by `orderId`; multi-order requests fan out into per-order events plus a thin batch correlation record.
**Notes:** `If-Match` is honored at the HTTP edge and carried as `expectedVersion` on the canonical event.

### `orders.kitchen.placement_changed`

**Source service:** Vendor KDS / dispatch (`PATCH /api/orders/{orderID}`).
**Action on receipt:** Publish placement change keyed by `orderId`.

### `orders.kitchen.item_status_changed`

**Source service:** Vendor KDS / kitchen automation (`PATCH /api/items/{itemID}`).
**Action on receipt:** Publish per-item status keyed by `orderId` (with `itemId` in payload).

### `orders.cabinet.status_changed`

**Source service:** Holding-cabinet hardware via the PUC bridge, AMQP on `{routingKey}-cabinet`.
**Action on receipt:** Publish keyed by `orderId` (fall back to `(storeId, cabinetOrderId)` until linked).

### `orders.kitchen.item_analyzed` and `orders.kitchen.matcher_observed`

**Source service:** Pack-station camera (`/UpdateOrderItemAnalysis`), makeline-matcher camera (`/UpdateOrderItemAnalysisMakeLine`).
**Action on receipt:** Publish per-item / matcher events keyed by `orderId`; image bytes go to raw-store, the canonical event carries only `rawRequestRef`.

### `store.cleanliness.observed`

**Source service:** Bench / table cleanliness camera (`POST /handler/{storeId}/tableCleanliness/events`).
**Action on receipt:** Publish keyed by `storeId` — observation is about the environment, not an order.

### `courier.location.updated` and `courier.vehicle.assigned`

**Source service:** In-store GPS / Traffilog tracker (`POST /CarrierLocation`); brand POS / fleet ops UI (`PUT /PosVehicleAssignment`).
**Action on receipt:** Publish waypoint / assignment keyed by `courierId`. `sourceSystem` distinguishes hardware-tracker from mobile-sourced GPS pings on the same topic.

### Query-shaped inbound (not facts, no canonical event)

- `POST /GetOrderUpdatesForPos` and `GET|POST /GetPosNotification` are RPC against Order Management — sync read, return a `posResponseByType` body, do **not** publish a canonical event.

### Not in-scope for this gateway

Legacy makeline FE / Process Center / line-management / dispatch UI / Auth surface / back-office / KDS read APIs / internal compute / aggregator callbacks / telematics pull / QA-only / Menu Management. See `docs/inbound-api-gateway/README.md` §4 for the full exclusion list.

## 5. Events Emitted

<!--
Every emitted event is a canonical Algo event published on the internal
EventBus using the envelope defined in CLAUDE.md.
-->

### `orders.intake.received`

**Emitted when:** `POST /PosInsertOrders` has been captured, validated, normalized, OM has committed, and AI Promise Time has returned — the published event is the **enriched** version. Or when a message has been pulled from `{routingKey}-cloud-orders` (no sync calls; raw inbound only).
**Payload summary:** Per-order envelope: `storeId`, `orderId`, `saleType`, addresses, `items[]`, payments, `fullLoad` flag, `sourceEventId`, `rawRequestRef`, plus (POS-sync only) `orderManagementResult` and `promiseTime`.
**Expected consumers (all async):** Order Management, Customer Service (PII), Employee Service (PII), Couriers / Vehicle Service (PII), Quote Service, AI Promise Time (workload signal), makeline / FE projection, Ticket Service, Order Sequencer, Delivery Management, Cloud Gateway.
**Notes:** Partition key `orderId`. Idempotent on `(storeId, orderId, sourceEventId)`.

### `admin.notification.posted`

**Emitted when:** `POST /PosInsertNotification` accepted.
**Payload summary:** `storeId`, `kdsList[]`, `notification`, `time`.
**Expected consumers:** Makeline / FE projection (WebSocket fan-out).

### `orders.payment.updated`

**Emitted when:** `POST /updateOrderPayment` accepted.
**Payload summary:** `storeId`, `orderId`, `paymentId`, payment shape.
**Expected consumers:** Order Management.

### `customer.vip.linked` / `customer.vip.unlinked`

**Expected consumers:** Customer Service / VIP projection, Order Management.

### `courier.profile.upserted`, `courier.schedule.upserted`

**Expected consumers:** Employee / Courier Service.

### `admin.permission_group.upserted`

**Expected consumers:** Auth Service, Employee Service.

### `orders.kitchen.action_recorded`

**Payload summary:** `storeId`, `orderId`, `action`, `stationId`, optional `slot`, optional `expectedVersion` (`If-Match`).
**Expected consumers:** Order Management (authoritative state), makeline projection, AI Promise Time.
**Notes:** Partition key `orderId`.

### `orders.kitchen.batch_action_recorded`

**Payload summary:** `batchId`, `storeId`, `action`, `orderIds[]`.
**Expected consumers:** Audit / replay only — per-order events on `orders.kitchen.action_recorded` are the source of truth.

### `orders.kitchen.placement_changed`, `orders.kitchen.item_status_changed`

**Expected consumers:** Order Management, makeline projection.

### `orders.cabinet.status_changed`

**Expected consumers:** Order Management, Cabinet UI / customer-collect projection.

### `orders.kitchen.item_analyzed`, `orders.kitchen.matcher_observed`

**Expected consumers:** Vision / QA projection, Order Management.

### `store.cleanliness.observed`

**Expected consumers:** Vision / QA projection, Admin Panel.

### `courier.location.updated`, `courier.vehicle.assigned`

**Expected consumers:** Telematics projection, Dispatch.

### Audit lifecycle (cross-cutting platform topic)

**Emitted when:** Every lifecycle transition (`RECEIVED`, `RAW_STORED`, `VALIDATED`, `REJECTED`, `NORMALIZED`, `PUBLISHED`, `PUBLISH_FAILED`, `DEAD_LETTERED`).
**Payload summary:** `eventId`, `correlationId`, `sourceSystem`, status, `rawRequestRef`, timestamps, error details.
**Expected consumers:** Audit Service.
**Notes:** Platform / observability topic — does not appear in the entity-topic catalog.

## 6. Data Dependencies (External Reads)

### Store / brand / country routing context

**Owning service:** Store Service.
**When fetched:** On every inbound event — needed to build `brandId` / `country` / canonical `storeId` and the partition key.
**Why needed:** Inbound payloads usually carry only `storeNo` / `storeId`; topics partition on `(brand, country, storeId)`.
**How fetched:** TBD — likely a cached lookup refreshed every N minutes; cache miss falls back to "unknown" and marks `REJECTED` if the store can't be resolved.
**Notes:** Must survive Store Service downtime via the cache.

### Caller identity → source system mapping

**Owning service:** Auth Service (TBD).
**When fetched:** On every inbound request.
**Why needed:** Sets `sourceSystem` (`brand-pos`, `vendor-kds`, `labor-management`, `cabinet-puc`, `vision-pack`, `vision-makeline`, `cleanliness-camera`, `gps-tracker`, `cloud-order-publisher`, `vip-loyalty`, …) and drives per-source rate limits / topic ACLs.
**How fetched:** Sync auth call; cacheable per credential for short windows.
**Notes:** **Open** — Auth Service contract is TBD. Interim: `X-Station-ID` / `Bearer` / brand-shared API keys.

### Order Management sync API (POS-sync flows)

**Owning service:** Order Management.
**When fetched:** Synchronously on `POST /PosInsertOrders` (commit + data for `rowCount` / `orders[]` in the POS body) and on every `POST /GetOrderUpdatesForPos` / `GetPosNotification` poll (read-only).
**Why needed:** POS-compatible response body must reflect real OM state, not just the gateway's intent.
**How fetched:** Sync HTTP / gRPC with a tight timeout. On timeout, fall back to a "still processing" POS body; OM still receives the event from the bus.
**Notes:** OM is the **only** cluster service called synchronously on the write path besides Promise Time. Latency budget here is the most important non-functional requirement.

### AI Promise Time sync HTTP API (POS order-entry only)

**Owning service:** AI Promise Time.
**When fetched:** Synchronously on `POST /PosInsertOrders` for POS-originated orders.
**Why needed:** The POS response must carry the promised pickup / delivery time so the brand POS can show it to the cashier / customer.
**How fetched:** Plain HTTP `POST` with a tight timeout (target single-digit hundreds of ms). On timeout / 5xx, return `200 OK` with a fallback / sentinel promise marker.
**Notes:** Sync HTTP call is restricted to the POS order-entry path. KDS / cabinet / camera / GPS / cloud-order responses never carry a promise time. Promise Time also subscribes async on the bus for non-POS flows. Exact POS-body field name is TBD (see §9).

### Other cluster services (Quote, Customer, Employee, Couriers, makeline / FE projection, Ticket Service, Order Sequencer, Delivery Management, Cloud Gateway)

**Owning service:** Each respective Algo 4 service.
**When fetched:** Never synchronously — they subscribe to the bus.
**Notes:** Customer, Employee, and Couriers are PII boundaries; the gateway scrubs / partitions per their contract before publishing but does not query them on the request path.

### Idempotency-key store

**Owning service:** Self-owned (Redis / DynamoDB / Postgres TBD).
**When fetched:** On every inbound event before publishing.
**Why needed:** Reject duplicates from external retries (`sourceEventId`, `Idempotency-Key`, or full tuple).

### Raw payload store

**Owning service:** Self-owned (S3 / blob / object store TBD).
**When fetched:** Written on every inbound message; read only by Audit Service replay tooling.
**Why needed:** Audit, debug, replay — the canonical event only carries `rawRequestRef`.
**Notes:** Retention and PII handling rules are TBD.

## 7. API Exposed to Other Services

### External-facing inbound HTTP surface

**Purpose:** Be wire-compatible with every external POST that targets Algo 3 today, so brand POS / vendor KDS / Labor Management / cabinet / camera / GPS / VIP clients keep their existing URLs and payload shapes.
**Known consumers:** Brand POS, brand cloud order publishers, vendor KDS, brand Labor Management, brand VIP / loyalty, third-party holding-cabinet PUC, pack-station / makeline / cleanliness cameras, in-store GPS / Traffilog trackers.
**Inputs:** Every route listed in §4 with its current payload shape.
**Returns:** POS-compatible body `{ time, status, rowCount, errDescription, validationErrors, warnings, orders }` for POS routes; on the POS order-entry path the body also carries the promise time (field name TBD). v2 routes return their existing JSON shape with `ETag` on reads and `code` + `message` on errors. The only non-`200` produced at the HTTP layer is `415` on `/PosInsertNotification` when `Content-Type` is missing.
**Notes:** Response contract is fixed and must be preserved verbatim. Auth is per the (TBD) Auth Service; interim is brand-shared API keys + `X-Station-ID` + JWT `Bearer` on `/api/*`.

### Internal canonical-event publishing surface

**Purpose:** Publish the canonical Algo event envelope to EventBus topics listed in §5. On the POS order-entry path the published event is the **enriched** version (raw inbound + OM commit result + promise time).
**Known consumers:** Order Management, Quote, Customer (PII), Employee (PII), Couriers / Vehicle (PII), AI Promise Time (async workload signal), makeline / FE projection, Ticket Service, Order Sequencer, Recommendation Service, Delivery Management, Cabinet UI, vision projection, telematics, Cloud Gateway, Audit Service.
**Inputs:** Internally derived from each inbound event; POS-sync path also augmented with OM and Promise Time responses.
**Returns:** Async — consumers subscribe; the gateway does not block on consumption.
**Notes:** Partition keys follow the entity (`orderId`, `courierId`, `customerId` / `vipKey`, `storeId`). Schema versioning via the `schemaVersion` envelope field.

### Inbound audit query API

**Purpose:** Look up the lifecycle of any inbound message by `eventId`, `correlationId`, or `(sourceSystem, sourceEventId)`.
**Known consumers:** Audit Service tooling, on-call engineers, replay tooling.
**Inputs:** One of the keys above.
**Returns:** Lifecycle row (`RECEIVED → … → PUBLISHED | DEAD_LETTERED`) plus the `rawRequestRef`.
**Notes:** Thin read API for operational tooling only.

## 8. Operational Assumptions

- Auth Service exists and can identify the calling external system on every request. Until it does, the gateway falls back to per-source API keys / `X-Station-ID` and tags `sourceSystem` accordingly.
- Store Service exposes a cached `(externalStoreId) → (brand, country, canonicalStoreId)` lookup fast enough to sit on the hot path.
- Order Management exposes a sync API fast enough for the brand POS timeout (low hundreds of ms).
- AI Promise Time exposes a sync HTTP endpoint that fits in the same brand POS timeout budget. Timeout / 5xx degrades to a fallback marker in the POS body, never to an HTTP error.
- Every external POS / vendor KDS / camera / GPS / cabinet posting today is willing to send the same payload to the new gateway URL — only the host changes.
- The brand POS is willing to receive the promise time as an additional field in the existing POS response body (or already reads the field name we settle on).
- The raw-store, idempotency-store, retry topic, and DLQ topic are operationally provisioned before the gateway accepts traffic.
- Menu Management is intentionally not wired up in this iteration.
- Cabinet hardware stays on RabbitMQ — the gateway runs an AMQP consumer; migrating the cabinet vendor to HTTP is out of scope.
- Aggregator callbacks, DaaS provider events, and mobile-driver events do **not** enter through this gateway — they enter through the Outbound (Cloud) Gateway and are republished on the same internal topics.
- Calls from Algo's own makeline FE / Process Center / line-management / dispatch UIs are internal traffic and do not enter through this gateway.
- The gateway never holds the POS HTTP connection open waiting for an async event response. Sync into the cluster is restricted to **two** services: Order Management (POS-sync write + read paths) and AI Promise Time (POS order-entry only).
- `Content-Type: application/json` enforcement (`415` on miss) applies only to `/PosInsertNotification`. Extending it elsewhere is a breaking change requiring brand sign-off.

## 9. Open Questions

- **Auth Service contract.** Final scheme (API key / HMAC / OAuth / mTLS) and how `sourceSystem` identity is encoded. Tracked in `CLAUDE.md`.
- **Menu Management deferred.** Need a separate design pass for the `kfcmenuservice` webhook chain, `POST /PosInsertItemProperties`, `POST /combos`.
- **Permission Group Management API.** No granular inbound surface today — only the POS bulk snapshot. Need product to define routes, payload, source of truth, identity model, and per-action canonical event split (`admin.permission_group.created` / `updated` / `deleted` + courier-side `courier.permission_group.assigned` / `revoked`).
- **AI Promise Time on the POS sync path.** Confirm (a) Promise Time meets the end-to-end brand POS timeout, (b) the exact field name / shape inside the POS body, (c) the fallback contract on timeout / 5xx.
- **`POST /GetOrderUpdatesForPos`** is query-shaped — confirm we are happy treating it as RPC against Order Management.
- **GPS telematics pull.** Decide if the vendor should switch to a push model and become a first-class inbound event, or stay as a separate adapter.
- **`POST /UpdateOrderItemAnalysisMakeLine` carries images.** Confirm size limit, blob destination, and that the canonical event only carries `rawRequestRef`.
- **Cabinet RabbitMQ.** Confirm we are happy running an AMQP consumer inside the gateway, vs. a dedicated AMQP-to-EventBus bridge.
- **Multi-order `POST /api/orders/actions`** fan-out into per-order events plus a thin batch record (not its own top-level topic).
- **`If-Match` semantics** on `PATCH`/`POST /api/orders/{orderID}` — confirm the canonical event carries `expectedVersion` and OM owns the optimistic-concurrency check.
- **Idempotency key strategy.** Default: prefer `Idempotency-Key` header, fall back to `sourceEventId`, then the full tuple, then a hash of stable raw fields.
- **Retention of raw payloads.** PII implications for `PosInsertOrders` (address) and `UpdateOrderItemAnalysisMakeLine` (food / staff images). Policy needed before production.
- **Topic-naming standard.** Names follow `docs/event-taxonomy.md` and `docs/topics-overview.md` (six entity domains, `snake_case`, no transport / source prefixes).
- **Per-source rate limits.** Brand POS bursts (especially day-open `fullLoad=true`) need a concrete budget.
