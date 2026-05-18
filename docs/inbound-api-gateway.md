# Inbound API Gateway

<!--
Business-side service definition. Filled from a sweep of the legacy Algo 3
codebase (bitbucket.org/dragontailcom/algo).

SCOPE OF "EVENTS" IN THIS DOC:
Only events that cross the External Systems -> Algo boundary. Concretely:
the sender is a system that is NOT part of the Algo cluster - brand POS,
vendor KDS, brand Labor Management, brand loyalty/VIP, third-party
cabinet hardware, third-party vision cameras, in-store GPS trackers,
cloud order publishers.

Menu Management is intentionally out of scope for now and will be picked
up in a follow-up iteration of this doc.

Calls from Algo's own components into Algo's own backend (legacy makeline
FE, Process Center station UI, line-management UI, dispatch UI, support
tooling, internal pull loops) are explicitly NOT inbound events for this
gateway. They are listed in the "Not in-scope" subsection at the end of
section 4 for traceability.
-->

**Owner team:** Algo 4 Platform
**Status:** Draft
**Last updated:** 2026-05-17

---

## 1. Purpose

The Inbound API Gateway is the single entry point through which **external systems** push events into the Algo 4 cluster. It receives traffic from store-side brand POS clients, vendor KDS devices, brand Labor Management feeds, brand VIP / loyalty integrations, cloud order publishers, third-party holding-cabinet hardware, third-party vision / camera services, and in-store GPS trackers. The gateway authenticates each call, persists the raw payload for audit, validates the request, normalizes the source-specific shape into a canonical Algo event, and publishes that event onto the internal Kafka EventBus. For POS-style flows it preserves the legacy `200 OK + POS response body` contract so existing brand POS clients keep working unchanged during the Algo 3 → Algo 4 migration, and on POS order-entry the gateway also synchronously calls AI Promise Time so the promised pickup / delivery time can be folded into the same POS response body.

Menu Management is intentionally **out of scope for now** and is omitted from this iteration of the doc.

## 2. Scope

### In Scope

- Terminate every external HTTP `POST` / `PUT` / `PATCH` that an outside system targets today (`/PosInsert*`, the v2 `/api/order*`, `/api/items/*`, `/UpdateOrderItemAnalysis*`, `/CarrierLocation`, `/updateOrderPayment`, `/api/order/vip*`, etc.).
- Terminate every external RabbitMQ-delivered event the cluster consumes today (`{routingKey}-cabinet`, `{routingKey}-cloud-orders`) and convert it into an inbound event of the same shape as the HTTP path.
- Capture the raw request body, headers, transport, source identity, and timestamps for every inbound message.
- Authenticate callers through the (TBD) Auth Service and attach a `sourceSystem` identity to the canonical event.
- Validate header / size / schema / store-scoping rules before publishing.
- Normalize source payloads into the canonical Algo event envelope defined in `CLAUDE.md`.
- Publish canonical events to Kafka business topics with stable partition keys (brand-country-store).
- Honor the POS-compatible HTTP response contract (`200 OK` with `time` / `status` / `rowCount` / `errDescription` / `validationErrors` / `warnings` / `orders`, plus `415` on missing `application/json`).
- Track each inbound message through the `RECEIVED → RAW_STORED → VALIDATED → NORMALIZED → PUBLISHED` lifecycle, and route `REJECTED` / `PUBLISH_FAILED` cases to the audit store and retry / DLQ topics.
- Synchronously call Order Management for POS order-management flows that need an immediate POS-compatible response body, and fold its result into that response.
- Synchronously call AI Promise Time on the POS order-entry path and fold the promised pickup / delivery time into the same POS response body. KDS / cabinet / camera / GPS callers do **not** get a promise time in their response.
- Enforce per-source idempotency before publishing (`Idempotency-Key` header, `sourceEventId`, or `sourceSystem + storeId + eventType + sourceEventId` tuple).

### Out of Scope

- **Menu Management.** All menu-publish, item-property, and combo-publish events are deferred to a later iteration of this doc. The `kfcmenuservice` webhook chain, `POST /PosInsertItemProperties`, and `POST /combos` are not covered here.
- Any HTTP route that Algo's own components call into Algo's own backend (makeline FE, Process Center station UI, line-management UI, dispatch UI, support tooling). Those are *internal* traffic and become in-cluster RPC or direct event subscription in Algo 4 — not gateway events.
- Implementing the business logic of any downstream consumer (Quote, Customer, Employee, Order Management, AI Promise Time, Cabinet UI, etc.).
- Talking to mobile devices, DaaS Gateway, or the Admin Panel UI — those are the Outbound (Cloud) Gateway's surfaces.
- Async aggregator callbacks (DoorDash / Uber / Wolt / etc.). Those arrive over the Outbound (Cloud) Gateway's DaaS Kafka consumer in Algo 4 and are republished onto the same internal topics — they do **not** enter through this gateway.
- The GPS / Traffilog telematics pull loop (`IgnitionEvent.go`). Algo today *polls* an external GPS server; nothing is pushed in. If that becomes a push channel later it can be added here.
- Long-polling / SSE / WebSocket fan-out to in-store screens (`publishStationsByWs`, `GET /handler/{storeId}/make` long-poll, `GET /api/events` SSE). Those continue to be owned by the makeline / FE service that subscribes to the bus.
- Picking the final auth scheme, raw-storage technology, retention policy, or final topic naming — those are open decisions tracked in `CLAUDE.md`.

## 3. Role in Store Operations

### Order Entry

The gateway is the front door for orders entering the store from outside. Today this happens through:

- A brand POS pushing `POST /PosInsertOrders` with a `PosOrders` envelope (`storeNo`, `fullLoad`, `orders[]`, `orderItems[]`, `translations[]`, `time`) in JSON or `OrdersXml` / `ItemsXml` form fields.
- A cloud order publisher dropping the same payload onto the `{brand}-{country}-{externalStoreId}-cloud-orders` RabbitMQ queue (`Payload` struct in `CloudOrderEvent.go`).
- A POS polling `POST /GetOrderUpdatesForPos` for incremental updates, and `GET|POST /GetPosNotification` for queued POS-bound notifications.
- Brand VIP / loyalty integrations hitting `POST /api/order/vip` and the related VIP routes.
- Payment events arriving from the POS / payment terminal on `POST /updateOrderPayment`.

For each of these the gateway captures the raw payload, validates it, emits a single canonical inbound event onto the bus (`algo.pos.order.received.v1` and friends), and — for the POS-sync variants — synchronously asks Order Management to commit the order **and** synchronously asks AI Promise Time for the promised pickup / delivery time, folding both into the POS-compatible response body before returning to the caller. AI Promise Time is reached over a plain HTTP sync call (not the Kafka bus) on this path; only the brand POS receives the promise time in the response — KDS / vendor callers never do.

### Order Modification / Cancellation

Modifications from the outside arrive on the same `/PosInsertOrders` channel (with the order id reused), on the `/GetOrderUpdatesForPos` poll, and through the vendor-KDS v2 routes that report kitchen-side lifecycle (`POST /api/order/actions`, `POST /api/orders/{orderID}`, `PATCH /api/orders/{orderID}`, `PATCH /api/items/{itemID}`, multi-order `POST /api/orders/actions`). Customer-driven cancellations from aggregators do not enter here — they come through the Outbound (Cloud) Gateway's DaaS Kafka path and land on the same internal topics.

### Kitchen Preparation & Bumping

The gateway accepts kitchen-side lifecycle from **third-party / vendor KDS devices** over the v2 JWT API (`X-Station-ID` header). Concretely: per-order actions (`POST /api/order/actions`, `POST /api/orders/{orderID}`), multi-order actions (`POST /api/orders/actions` — `updateRTML`, `reshuffle`), order patches (`PATCH /api/orders/{orderID}` — `status: onMakeLine`, `slot`, `line`), and item patches (`PATCH /api/items/{itemID}` — `waiting | onMakeLine | ready | canceled | parked`). It also accepts the POS-to-KDS push (`POST /PosInsertNotification`, the one route that strictly enforces `Content-Type: application/json` and returns `415` otherwise). Cabinet hardware does not use HTTP — it publishes on `{routingKey}-cabinet` and the gateway picks it up there. The pack-station camera, makeline matcher camera, and table-cleanliness camera each push on their own HTTP routes (`POST /UpdateOrderItemAnalysis`, `POST /UpdateOrderItemAnalysisMakeLine`, `POST /handler/{storeId}/tableCleanliness/events`).

The legacy Algo makeline FE endpoints (`POST /handler/{storeId}/make`, `setOrdersReachedTheMakeLine`, `printOrder`, `updateMatcherMetaData`, line-lock) are Algo's own UI talking to Algo's own backend — they are **not** inbound events for this gateway.

### Delivery / Handoff

The gateway is *not* the dispatch boundary. Driver-side, customer-side, and DaaS-side delivery traffic flows through the Outbound (Cloud) Gateway. The only delivery-relevant external traffic this gateway sees is in-store hardware: GPS / Traffilog trackers pushing waypoints (`POST /CarrierLocation`), and the POS assigning a vehicle to a carrier (`PUT /PosVehicleAssignment`).

### Day Open / Day Close

There is no dedicated day-open or day-close HTTP route. The day-start signal is implicit: the brand POS sends an initial `fullLoad=true` `PosInsertOrders` / `PosInsertEmployees` / `PosInsertEmployeesScheduling` / `PosInsertPermissionGroups`. The gateway forwards each as a normal inbound event; downstream services treat `fullLoad` as the day-open snapshot signal. Recovery after a gateway restart relies on Kafka consumer-group offsets in downstream services and on the raw audit store for replay. (Menu-publish at day-open is deferred — see §2.)

### Exception Flows

External-facing exceptions visible to this gateway:

- **Brand POS retries** the same `PosInsertOrders` / `updateOrderPayment` request — handled by the idempotency layer.
- **Vendor KDS sends a stale `If-Match`** on `PATCH /api/orders/{orderID}` — the gateway preserves the header and lets Order Management decide; the HTTP edge returns `409`.
- **Bad camera grade** arrives on `POST /UpdateOrderItemAnalysis*` — published verbatim, no interpretation at the gateway.
- **`415` on missing JSON Content-Type** on `/PosInsertNotification` is the only enforced HTTP-level rejection; all other validation failures return `200 OK` with a POS-failure status (`validationError`, `parseError`, `readError`, `dbError`, `internalError`, `unexpectedError`).
- **POS outage** is invisible to the gateway — there is simply no inbound traffic from that POS for a while. Downstream services notice via heartbeats.

## 4. Events Consumed (Triggers)

<!--
Every event below maps to an HTTP route or a RabbitMQ queue where the
SENDER is outside the Algo cluster. The "Trigger today" field carries the
wire-level shape from the Algo 3 / kfcmenuservice codebases. The canonical
Algo 4 event name in the heading is the *proposed* name on the internal bus.
Payload field lists are top-level only.
-->

### POS — order entry & lifecycle

#### `algo.pos.order.received.v1`

**Source service:** Brand POS (XML or JSON), cloud order publishers.
**Trigger today:** `POST /PosInsertOrders` (`PosServices.go`, payload `PosOrders`), or `Payload` consumed from `{brand}-{country}-{externalStoreId}-cloud-orders` (`CloudOrderEvent.go`).
**Payload shape:** `storeNo` (string), `fullLoad` (bool), `orders[]` (`PosOrder`), `orderItems[]` (`PosOrderItem`), `translations[]`, `time` (`YYYYMMDDHHMMSS`), `idle` flag. Per-order: `orderId`, addresses, `saleType`, VIP flags, payments, aggregator batch flags (`IsAggBatch`, `AggCarrierID`). Form variant carries `OrdersXml` / `ItemsXml` / `StoreNo` / `FullLoad`.
**Why we consume it:** Single ingest path for every order entering the store, whether typed at the till or pushed from a cloud aggregator.
**Action on receipt:** Capture raw, validate `storeNo` + shape, normalize each `PosOrder` into one canonical event keyed by `(storeNo, orderId)`, publish to `algo.pos.order.received.v1`. POS-sync flows go to Order Management synchronously, the response is folded into the POS body, then the event is still published for downstream fan-out.
**Notes:** Idempotency on `(storeNo, orderId, sourceEventId)` because brand POS retries are common. `fullLoad=true` is the day-open snapshot — downstream services must treat it as a replace-all, not a delta.

#### `algo.pos.order.updates.requested.v1`

**Source service:** Brand POS polling loop.
**Trigger today:** `POST /GetOrderUpdatesForPos` (form or raw JSON, fields `UpdateStamp`, `CarrierId`, `StoreNo`).
**Why we consume it:** Lets the POS reconcile what the store knows since `UpdateStamp` without holding state on its own.
**Action on receipt:** This is a *query*, not a state change. The gateway calls the Order Management read API synchronously and returns a `posResponseByType` body. We do **not** publish a canonical event for the read itself; we only emit `algo.pos.poll.observed.v1` for observability.
**Notes:** This is the one inbound that breaks the "publish-then-respond" pattern. Treat as RPC-shaped from day one.

#### `algo.pos.notification.pull.v1`

**Source service:** Brand POS.
**Trigger today:** `GET /GetPosNotification`, `POST /GetPosNotification` (`PullNotificationGet` / `PullNotificationPost`).
**Why we consume it:** POS pulls queued back-channel notifications that Algo built up while the POS was offline.
**Action on receipt:** Query-shaped, same pattern as the previous one — synchronously read Order Management's notification buffer and return.

#### `algo.pos.notification.push.v1`

**Source service:** Brand POS pushing an operator message to the kitchen.
**Trigger today:** `POST /PosInsertNotification` (`PosInsertNotification.go`, payload `PosInsertNotificationRequest`: `storeNo`, `kdsList`, `notification`, `time`).
**Why we consume it:** Operator messages typed at the POS that must surface on selected KDS stations.
**Action on receipt:** Capture, validate, publish to `algo.pos.notification.push.v1`. The KDS / FE projection fans out over WebSocket.
**Notes:** **The only inbound HTTP route that strictly enforces `Content-Type: application/json`** — missing or wrong content type returns `415 Unsupported Media Type` with a POS-failure body. Preserve verbatim in Algo 4.

#### `algo.pos.payment.updated.v1`

**Source service:** Brand POS / payment terminal bridge.
**Trigger today:** `POST /updateOrderPayment` (`endpoints.go` → `updateOrderPaymentHandler`).
**Action on receipt:** Normalize to a payment-update event keyed by `(storeId, orderId, paymentId)`; publish for Order Management.
**Notes:** Idempotent on `paymentId`.

#### `algo.pos.vip.requested.v1` / `algo.pos.vip.deleted.v1`

**Source service:** Brand VIP / loyalty integrations.
**Trigger today:** `POST /api/order/vip`, `DELETE /api/order/vip`, plus the `/api/order/vip/*` sub-routes (`VIPCreateJson` / `VIPDeleteJson`).
**Action on receipt:** Publish VIP-create / VIP-delete events keyed by `(storeId, vipKey)`. Order Management subscribes.

### Labor Management

#### `algo.labor.employees.upserted.v1`

**Source service:** Brand labor management (today: POS-shaped clients).
**Trigger today:** `POST /PosInsertEmployees` (form `CarriersXml` / `EmployeesXml`, `StoreNo`, `FullLoad`, or raw JSON).
**Why we consume it:** Carriers / employees roster snapshot or delta.
**Action on receipt:** Normalize each carrier into one event keyed by `(storeId, employeeId)`. Employee Service is the primary consumer.
**Notes:** `FullLoad=true` is a roster replace.

#### `algo.labor.schedule.upserted.v1`

**Source service:** Brand labor management.
**Trigger today:** `POST /PosInsertEmployeesScheduling` (`EmployeesXml`, `FullDayLoad`, JSON variant).
**Action on receipt:** Normalize per-employee per-day shift entries; publish.

#### `algo.labor.permission_groups.upserted.v1`

**Source service:** Brand labor / IT.
**Trigger today:** `POST /PosInsertPermissionGroups` (`PermissionGroupsXml` or JSON).
**Action on receipt:** Publish permission-group definitions for Employee Service / Auth.

### KDS (vendor devices, v2 API)

> Scope note: only the v2 `/api/*` routes (JWT `Bearer` + `X-Station-ID` header) are inbound from outside Algo — those are designed for vendor KDS hardware/software. The legacy `POST /handler/{storeId}/make` family is Algo's own makeline FE talking to its own backend and is intentionally excluded.

#### `algo.kds.order.action.v1`

**Source service:** Vendor KDS using API v2.
**Trigger today:** `POST /api/order/actions` (payload `OrderActionRequest`: `action`, `station`, `orderId`, `params { items, shelfNumber, slot }`) and `POST /api/orders/{orderID}` (payload `PostOrderActionRequest`: `action`, `items[]` map `{ hold | prepare | startCook | stopCook | pack | remake }`, optional `slot`, `level`).
**Why we consume it:** Per-order action surface from vendor KDS — bump, cook, pack, remake, undo, prioritize, seen.
**Action on receipt:** Publish one canonical event per `(storeId, orderId, action)` partitioned by `storeId`. Order Management and the makeline projection subscribe.
**Notes:** Optimistic concurrency via `If-Match` on the order hash. We honor that at the HTTP edge; the canonical event carries the `If-Match` value as `expectedVersion` so consumers can detect lost updates.

#### `algo.kds.orders.action.v1`

**Source service:** Vendor KDS.
**Trigger today:** `POST /api/orders/actions` (payload `OrdersActionRequest`: `action`, `station`, `orderIds[]`, `params`). Currently handles `updateRTML` (orders reached the makeline, the "seen" semantic) and `reshuffle`.
**Action on receipt:** Fan out one event per `orderId` into `algo.kds.order.action.v1`, preserving the batch correlation id. Emit a lightweight `algo.kds.orders.action.v1` batch record on its own topic for audit / replay only.

#### `algo.kds.order.patched.v1`

**Source service:** Vendor KDS / dispatch.
**Trigger today:** `PATCH /api/orders/{orderID}` (payload `PatchOrderRequest`: `status`, `slot`, `line`, address & coords for dispatch).
**Why we consume it:** Reports that an order is now on the makeline (`status: onMakeLine`), occupies a shelf slot, or moved to a different makeline line.
**Action on receipt:** Publish a structured order-patch event keyed by `(storeId, orderId)`.
**Notes:** `If-Match` applies at the HTTP edge.

#### `algo.kds.item.patched.v1`

**Source service:** Vendor KDS / kitchen automation.
**Trigger today:** `PATCH /api/items/{itemID}` (payload `PatchItemRequest`: `status` ∈ {`waiting`, `onMakeLine`, `ready`, `canceled`, `parked`}, optional `readyTime`).
**Action on receipt:** Publish an item-patch event keyed by `(storeId, orderId, itemId)`.

### Cabinet (third-party holding-cabinet hardware)

#### `algo.cabinet.order.updated.v1`

**Source service:** Holding-cabinet hardware via the PUC bridge.
**Trigger today:** AMQP consumer on `{brand}-{country}-{externalStoreId}-cabinet`, handler `OnPucMessage` (`PucAPI.go`), payload `entity.CabinetOrderResponse`: `id`, `qr_code`, `load_code`, `lockers { heated, ambient }`, `status`.
**Why we consume it:** Cabinet reports loaded / collected slots so Order Management knows the order is in the holding cabinet (and when it's been collected).
**Action on receipt:** Publish `algo.cabinet.order.updated.v1` keyed by `(storeId, cabinetOrderId)`. The legacy code's `SendPosNotifications` on `status == "done"` becomes a downstream subscriber's job.
**Notes:** Manual ack today, malformed messages rejected. Ordering is "broker + single consumer per queue" — preserve at least partition-per-store in Kafka.

### Vision / Camera (third-party in-store hardware)

#### `algo.kitchen.item.analysis.v1`

**Source service:** Pack-station camera / vision service.
**Trigger today:** `POST /UpdateOrderItemAnalysis` (`FrontEndServiceMakeLine.go`, payload `helper.OrderItemsAnalysis`: `storeId`, `items[]`, optional `version`).
**Action on receipt:** Publish per-item analysis event keyed by `(storeId, orderId, itemPosition)`.

#### `algo.kitchen.makeline.matcher.v1`

**Source service:** Makeline camera / matcher appliance.
**Trigger today:** `POST /UpdateOrderItemAnalysisMakeLine` (payload `MakeLineMatcherData`: `version`, `storeNo`, `match` `OrderMatch`, `capturingTime`, `analysis` `ImageAnalysis`, `image` `ImageData`, `stationId`).
**Action on receipt:** Publish matcher event with image reference (image bytes go to raw-store / blob; the canonical event carries only a `rawRequestRef`).
**Notes:** Large payload (base64 image). Enforce a payload size limit and store the image in raw storage rather than inlining it on the bus.

#### `algo.kitchen.table_cleanliness.event.v1`

**Source service:** Bench / table cleanliness camera.
**Trigger today:** `POST /handler/{storeId}/tableCleanliness/events` (`camera.go`, payload `TableCleanliness`: `sessionId`, `storeId`, `eventTime`, `isCleaningEvent`, `isStainingEvent`).
**Action on receipt:** Publish a hygiene event keyed by `(storeId, sessionId, eventTime)`.

### Fleet (in-store GPS hardware, POS-driven assignment)

#### `algo.fleet.carrier.location.v1`

**Source service:** In-store GPS / Traffilog tracker hardware.
**Trigger today:** `POST /CarrierLocation` (`SetCarrierLocationJson`).
**Action on receipt:** Publish waypoint event keyed by `(storeId, carrierId)`. Telematics consumers subscribe.

#### `algo.fleet.vehicle.assigned.v1`

**Source service:** Brand POS / fleet ops UI.
**Trigger today:** `PUT /PosVehicleAssignment` (`AssignVehicleToCarrierHandler`).
**Action on receipt:** Publish vehicle-assignment event keyed by `(storeId, carrierId, vehicleId)`.

---

### Not inbound events for this gateway (deliberately excluded)

The following arrive over HTTP into Algo today but are **not** external — the caller is one of Algo's own components and the call becomes in-cluster RPC or direct event subscription in Algo 4, not a gateway event.

- **Legacy makeline FE → Algo backend:** `POST /handler/{storeId}/make` (`UpdateOrderMakeStatusJson`), `POST /handler/{storeId}/setOrdersReachedTheMakeLine`, `POST /handler/{storeId}/printOrder`, `POST /handler/{storeId}/updateMatcherMetaData`, `POST /handler/{storeId}/clockIn|clockOut|deleteCarrier|editCarrier|createCarrier`, `POST /handler/{storeId}/algoCapture`, `GET /handler/{storeId}/newAnalysis`, `GET /handler/{storeId}/make` long-poll.
- **Algo's own line-management UI:** `POST /api/lines/lock`, `POST /api/lines/unlock`, `GET|PATCH /api/lines`, `GET|PATCH /api/occassions`, `POST /api/occassions/reset`, `GET|POST /api/{storeID}/{station}/ui_mode`.
- **Algo's Process Center station:** `POST /processCenter/packOrder/{orderId}`, `POST /processCenter/remakeOrder/{orderId}`, `POST /processCenter/printOrder/{orderId}`, `POST /processCenter/stageOrderKitchen/{orderId}/{kitchenId}`.
- **Algo's dispatch / FE aggregator UI:** `POST /handler/{storeId}/callAgg|cancelAgg|aggregatorsActivity`, `POST /api/stores/{storeID}/deliveries/{deliveryID}/callAggregator|callDaas` — these become *outbound* events from internal services, picked up by the **Outbound (Cloud) Gateway**, not inbound here.
- **Auth surface:** `POST /login`, `Handle /pinCode`, `POST /api/auth/token`, `POST /api/auth/refresh`, `POST /api/getAuthorized`, `POST /register`, `/forgotPassword`, `Handle /changePassword` — owned by Auth Service.
- **Operator / dashboard / back-office:** `GET /dashboard/settings`, `POST /dashboard`, `POST /dashboard/*`, `Handle /backoffice/*`, `POST /getPolygons` — owned by the Admin Panel UI surface of the **Outbound (Cloud) Gateway**.
- **KDS read APIs:** `GET /api/orders`, `GET /api/orders/bumped`, `GET /api/orders/{orderID}`, `GET /api/vehicles`, SSE `GET /api/events` — served by downstream projections directly.
- **Internal compute calls:** `POST /SendRequestToRulingAgent`, `POST /OptimizeOrdersFromJson`, `POST /AvgTimePerKilometerJson`, `POST /VehicleLocationAlertJson`, `POST /GetCarrierRoutesDistance*`, `Handle /AddressLocation`, `POST /EventStartRaise`, `POST /CallTraffilog`, `POST /SetLocation` — internal compute between Algo 3 components, not external.
- **Async aggregator callbacks** (`OnAggMessage`, `OnETAmessage` on the aggregator RabbitMQ queues): these arrive via the **Outbound (Cloud) Gateway's DaaS Kafka consumer** in Algo 4, not through this gateway.
- **Telematics GPS pull** (`IgnitionEvent.go` `GetGppNotificationsFromServer`): Algo *polls* an external server; nothing is pushed in. Not an inbound event.
- **QA-only:** `POST /demo/*` behind `AllowQaCommands` — do not migrate.
- **Menu Management (deferred):** the `kfcmenuservice` webhook chain (`POST /menu`), `POST /PosInsertItemProperties`, `POST /combos`, and any other menu-publish surface are not covered in this iteration. They will be added back once the menu integration is designed.

## 5. Events Emitted

<!--
Every emitted event is a canonical Algo event published on the internal
Kafka bus, with the envelope defined in CLAUDE.md (eventId, correlationId,
eventType, sourceSystem, brandId, country, storeId, receivedAt,
rawRequestRef, data{}).
-->

### `algo.pos.order.received.v1`
**Emitted when:** A `POST /PosInsertOrders` request has been captured, validated and normalized, **or** a message has been pulled from `{routingKey}-cloud-orders`.
**Payload summary:** Per-order envelope with `storeId`, `orderId`, `saleType`, addresses, `items[]`, payments, `fullLoad` flag, `sourceEventId`, `rawRequestRef`.
**Expected consumers:** Order Management (authoritative), Customer Service (address dedupe), AI Promise Time (workload signal for non-POS flows; on the POS-sync path Promise Time is already reached over sync HTTP — see §3 / §6), Quote Service (price recomputation when items change).
**Notes:** Partition key `storeId`. Idempotent on `(storeId, orderId, sourceEventId)`.

### `algo.pos.poll.observed.v1`
**Emitted when:** A `POST /GetOrderUpdatesForPos` arrives. Metrics-only.
**Payload summary:** `storeId`, `carrierId`, `updateStamp`, `receivedAt`.
**Expected consumers:** Observability pipeline only.

### `algo.pos.notification.push.v1`
**Emitted when:** A `POST /PosInsertNotification` is accepted (`Content-Type: application/json` enforced).
**Payload summary:** `storeId`, `kdsList[]`, `notification`, `time`.
**Expected consumers:** Makeline / FE projection (WebSocket fan-out).

### `algo.pos.payment.updated.v1`
**Emitted when:** A `POST /updateOrderPayment` is accepted.
**Payload summary:** `storeId`, `orderId`, `paymentId`, payment shape.
**Expected consumers:** Order Management.

### `algo.pos.vip.requested.v1` / `algo.pos.vip.deleted.v1`
**Expected consumers:** Order Management / VIP projection.

### `algo.labor.employees.upserted.v1`, `algo.labor.schedule.upserted.v1`, `algo.labor.permission_groups.upserted.v1`
**Expected consumers:** Employee Service. Auth service for permission groups.

### `algo.kds.order.action.v1`
**Emitted when:** Any v2 KDS action arrives (`POST /api/order/actions`, `POST /api/orders/{orderID}`, fan-out from the multi-order `POST /api/orders/actions`).
**Payload summary:** `storeId`, `orderId`, `action`, `stationId`, optional `slot`, optional `expectedVersion` (`If-Match`).
**Expected consumers:** Order Management (authoritative state), makeline projection, AI Promise Time.
**Notes:** Partition key `storeId` to keep per-store ordering.

### `algo.kds.orders.action.v1` (batch correlation only)
**Emitted when:** A multi-order action arrives (`POST /api/orders/actions`).
**Payload summary:** `batchId`, `storeId`, `action`, `orderIds[]`.
**Expected consumers:** Audit / replay only — per-order events on `algo.kds.order.action.v1` are the source of truth.

### `algo.kds.order.patched.v1`
**Expected consumers:** Order Management.

### `algo.kds.item.patched.v1`
**Expected consumers:** Order Management / makeline projection.

### `algo.cabinet.order.updated.v1`
**Emitted when:** A message arrives on `{routingKey}-cabinet`.
**Expected consumers:** Order Management, Cabinet UI / customer-collect projection.

### `algo.kitchen.item.analysis.v1`, `algo.kitchen.makeline.matcher.v1`, `algo.kitchen.table_cleanliness.event.v1`
**Expected consumers:** Vision / QA projection.

### `algo.fleet.carrier.location.v1`, `algo.fleet.vehicle.assigned.v1`
**Expected consumers:** Telematics projection, dispatch.

### `algo.inbound.audit.v1` (cross-cutting)
**Emitted when:** Every lifecycle transition (`RECEIVED`, `RAW_STORED`, `VALIDATED`, `REJECTED`, `NORMALIZED`, `PUBLISHED`, `PUBLISH_FAILED`, `DEAD_LETTERED`).
**Payload summary:** `eventId`, `correlationId`, `sourceSystem`, status, `rawRequestRef`, timestamps, error details.
**Expected consumers:** Audit Service.
**Notes:** Single observability stream feeding the table in §6 of `CLAUDE.md`.

## 6. Data Dependencies (External Reads)

### Store / brand / country routing context
**Owning service:** Store Service (today: rows in Algo SQLite, accessed via `Stores.getQueueRoutingKey()`).
**When fetched:** On every inbound event — needed to build the canonical envelope (`brandId`, `country`, `storeId` and the Kafka partition key).
**Why needed:** Inbound payloads usually carry only `storeNo` / `storeId` / `storeNumber`. The canonical envelope demands brand + country + canonical store id; downstream topics partition on `(brand, country, storeId)`.
**How fetched:** TBD — likely a cached lookup against Store Service refreshed every N minutes. Cache miss must not block ingestion; fall back to "unknown brand/country" and mark `REJECTED` if the store can't be resolved.
**Notes:** Staleness tolerance is "minutes" — store routing rarely changes. Must survive Store Service downtime via the cache.

### Caller identity → source system mapping
**Owning service:** Auth Service (TBD).
**When fetched:** On every inbound request.
**Why needed:** Sets `sourceSystem` (`brand-pos`, `vendor-kds`, `labor-management`, `cabinet-puc`, `vision-pack`, `vision-makeline`, `cleanliness-camera`, `gps-tracker`, `cloud-order-publisher`, `vip-loyalty`, etc.) and tenancy scoping. Drives per-source rate limits and topic ACLs.
**How fetched:** Sync auth call on every request. Cacheable per credential / token for short windows.
**Notes:** **Open** — the Auth Service contract is TBD per `CLAUDE.md`. The gateway must handle the "no Auth Service yet" interim case by reading `X-Station-ID` / `Bearer` / brand-shared API keys.

### Order Management read API (POS-sync flows)
**Owning service:** Order Management.
**When fetched:** Synchronously on `POST /PosInsertOrders` (when the POS expects a `200 OK` with `rowCount` / `orders[]` in the body) and on every `POST /GetOrderUpdatesForPos` / `GetPosNotification` poll.
**Why needed:** The POS-compatible response body must be built from real Order Management state, not just from the gateway's intent.
**How fetched:** Sync HTTP / gRPC call with a tight timeout. On timeout the gateway falls back to a "still processing" POS body so the caller does not block.
**Notes:** Latency budget on the sync path is the most important non-functional requirement of the gateway.

### AI Promise Time sync HTTP API (POS-sync flows)
**Owning service:** AI Promise Time.
**When fetched:** Synchronously on `POST /PosInsertOrders` for POS-originated orders, after Order Management has committed the order.
**Why needed:** The POS response body must carry the promised pickup / delivery time for the new order so the brand POS can show it to the cashier / customer in the same screen.
**How fetched:** Plain HTTP `POST` against AI Promise Time's sync endpoint with a tight timeout (target single-digit hundreds of ms). On timeout or 5xx, the gateway returns a `200 OK` POS body with a `still-computing` / fallback promise marker so the caller does not block.
**Notes:** Promise Time also subscribes asynchronously to `algo.pos.order.received.v1` on the Kafka bus for non-POS flows (cloud orders, KDS-originated changes) and for workload signaling. The sync HTTP call is only on the POS-sync path; KDS / cabinet / camera / GPS responses do **not** carry a promise time. Exact field name in the POS body is TBD — see §9.

### Idempotency-key store
**Owning service:** Self-owned (Redis / DynamoDB / Postgres TBD).
**When fetched:** On every inbound event, before publishing.
**Why needed:** Reject duplicates from external retries (`sourceEventId`, `Idempotency-Key`, or `(sourceSystem, storeId, eventType, sourceEventId)` tuple).
**Notes:** Final tech is TBD.

### Raw payload store
**Owning service:** Self-owned (S3 / blob / object store TBD).
**When fetched:** Written on every inbound message, read only by Audit Service replay tooling.
**Why needed:** Audit, debug, replay. The canonical event only carries a `rawRequestRef`.
**Notes:** Retention and PII handling rules are TBD per `CLAUDE.md`.

## 7. API Exposed to Other Services

### External-facing inbound HTTP surface

**Purpose:** Be wire-compatible with every external POST that targets Algo 3 today, so brand POS / vendor KDS / Labor Management / cabinet / camera / GPS / VIP clients can keep their existing URLs and payload shapes.
**Known consumers:** Brand POS, brand cloud order publishers, vendor KDS, brand Labor Management, brand VIP / loyalty integrations, third-party holding-cabinet PUC, pack-station / makeline / cleanliness cameras, in-store GPS / Traffilog trackers.
**Inputs:** Every route listed in §4 with its current payload shape.
**Returns:** POS-compatible body: `{ time, status, rowCount, errDescription, validationErrors, warnings, orders }` for POS routes; on the POS order-entry path the body also carries the promise time returned by AI Promise Time (exact field name TBD — see §9). v2 routes return their existing JSON shape with `ETag` on reads and `code` + `message` on errors and never carry a promise time. The only non-`200` status produced at the HTTP layer is `415 Unsupported Media Type` on `/PosInsertNotification` when `Content-Type` is missing.
**Notes:** Per §2 of `CLAUDE.md`, the response contract is fixed and must be preserved verbatim. Auth is per the (TBD) Auth Service; today's behavior is brand-shared API keys + `X-Station-ID` + JWT `Bearer` on `/api/*`.

### Internal canonical-event publishing surface

**Purpose:** Publish the canonical Algo event envelope (`CLAUDE.md` §"Canonical Algo Event") to Kafka topics listed in §5.
**Known consumers:** Order Management, Customer Service, Employee Service, AI Promise Time, Quote Service, makeline projection, Cabinet UI, vision projection, telematics, Audit Service.
**Inputs:** Internally derived from each inbound event.
**Returns:** Async — consumers subscribe; the gateway does not block on consumption.
**Notes:** Partition keys are `storeId` for store-scoped topics and finer keys (`cabinetOrderId`, `carrierId`) where ordering is per-entity. Schema versioning via the `schemaVersion` envelope field.

### Inbound audit query API

**Purpose:** Look up the lifecycle of any inbound message by `eventId`, `correlationId`, or `(sourceSystem, sourceEventId)`.
**Known consumers:** Audit Service tooling, on-call engineers, replay tooling.
**Inputs:** One of the keys above.
**Returns:** Lifecycle row (`RECEIVED → RAW_STORED → … → PUBLISHED | DEAD_LETTERED`) plus the `rawRequestRef`.
**Notes:** Thin read API for operational tooling only.

## 8. Operational Assumptions

- The Auth Service exists and can identify the calling external system on every inbound request (`sourceSystem`). Until it does, the gateway falls back to per-source API keys / `X-Station-ID` headers and tags `sourceSystem` accordingly.
- Store Service exposes a cached lookup from `(externalStoreId)` → `(brand, country, canonicalStoreId)` and stays fast enough to be on the hot path.
- Order Management exposes a sync API fast enough to satisfy the POS-compatible response-body contract within the brand POS's timeout (low hundreds of ms).
- AI Promise Time exposes a sync HTTP endpoint fast enough to be called on the POS order-entry path within the same brand POS timeout budget as the Order Management call. A timeout / 5xx degrades to a fallback marker in the POS body, never to an HTTP-level error.
- Every external POS / vendor KDS / camera / GPS / cabinet that posts today is willing to send the same payload to the new gateway URL — i.e. only the host changes, not the body shape, headers, or status codes.
- The brand POS is willing to receive the promise time as an additional field in the existing POS response body without a contract change on its side (or, equivalently, the brand POS already reads the field name we settle on).
- The raw-store, idempotency-store, retry topic, and DLQ topic are operationally provisioned before the gateway accepts traffic.
- Menu Management is intentionally not wired up in this iteration. When it is added back, the gateway will get a dedicated Menu Management subsection in §4 / §5 and a corresponding open question on push vs. pull.
- Cabinet hardware continues to use RabbitMQ as its transport even after Algo 4 — the gateway runs an AMQP consumer to terminate that queue and republish onto Kafka. Migrating the cabinet vendor to HTTP is out of scope for this gateway.
- Aggregator callbacks, DaaS provider events, and mobile-driver events do **not** enter through this gateway — they enter through the Outbound (Cloud) Gateway's inbound consumer paths and are republished on the same internal topics.
- Calls from Algo's own makeline FE, Process Center, line-management, and dispatch UIs are **internal** traffic and do not enter through this gateway. They become in-cluster RPC or direct event subscription in Algo 4.
- The gateway never holds the POS HTTP connection open waiting for an async event response. Sync is restricted to the Order Management and AI Promise Time calls described in §3 / §6.
- `Content-Type: application/json` enforcement (`415` on miss) applies only on `/PosInsertNotification` today. Extending it to other routes is a breaking change and requires brand sign-off.

## 9. Open Questions

- **Auth Service contract is missing.** Final scheme (API key / HMAC / OAuth / mTLS) and how `sourceSystem` identity is encoded on the request. Tracked in `CLAUDE.md` Open Decisions.
- **Menu Management is deferred.** All menu-publish flows (the `kfcmenuservice` webhook chain, `POST /PosInsertItemProperties`, `POST /combos`) have been pulled out of this iteration. Need a separate design pass to decide how Menu Management feeds the gateway and which canonical events it emits.
- **AI Promise Time on the POS sync path.** Confirm (a) Promise Time exposes a sync HTTP API that meets the brand POS timeout budget end-to-end (gateway → Order Management → gateway → Promise Time → gateway), (b) the exact field name and shape the promise time takes inside the existing POS response body (`{ time, status, rowCount, errDescription, validationErrors, warnings, orders }`), (c) the fallback contract when Promise Time times out or 5xx's (return a sentinel like `promiseTime: null` with `status: "ok"`, or a soft failure status). KDS / vendor callers must not receive a promise time even on the same internal event.
- **`POST /GetOrderUpdatesForPos` is query-shaped.** It is the one inbound HTTP that does not fit "capture → publish → respond". Confirm we are happy treating it as RPC against Order Management.
- **GPS telematics pull (`IgnitionEvent.go`)** is currently an Algo-initiated pull against an external GPS notification server. Listed under "Not inbound events" today. Decide if the telematics vendor should switch to a push model and become a first-class inbound event, or stay as a separate adapter.
- **`POST /UpdateOrderItemAnalysisMakeLine` carries images.** Confirm payload-size limit, blob-storage destination, and that the canonical event only carries a `rawRequestRef`.
- **Cabinet RabbitMQ queue staying on RabbitMQ.** Confirm we are happy running an AMQP consumer inside the gateway, or do we route it via a dedicated AMQP-to-Kafka bridge?
- **Multi-order `POST /api/orders/actions`.** Confirm fan-out into per-order events on `algo.kds.order.action.v1` plus a thin batch record on `algo.kds.orders.action.v1`, not its own top-level topic.
- **`If-Match` semantics on `PATCH /api/orders/{orderID}` / `POST /api/orders/{orderID}`.** Confirm the canonical event carries `expectedVersion` and Order Management is responsible for the optimistic-concurrency check, not the gateway.
- **Idempotency key strategy is TBD.** Default proposal: prefer `Idempotency-Key` header, fall back to `sourceEventId`, fall back to `(sourceSystem, storeId, eventType, sourceEventId)` tuple, fall back to a hash of stable raw fields.
- **Retention of raw payloads.** PII implications for `PosInsertOrders` (customer address) and `UpdateOrderItemAnalysisMakeLine` (image of food / staff). Retention and access-control policy must be decided before production.
- **Topic-naming standard.** Names in §5 are proposed — needs alignment with the Outbound (Cloud) Gateway and the broader Algo 4 topic taxonomy.
- **Per-source rate limits.** Brand POS bursts (especially on day-open `fullLoad=true`) need a concrete budget.
