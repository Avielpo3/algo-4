# Inbound API Gateway

<!--
Business-side service definition. Filled from a sweep of the legacy Algo 3
codebase (bitbucket.org/dragontailcom/algo).

SCOPE OF "EVENTS" IN THIS DOC:
Only events that cross the External Systems -> Algo boundary. Concretely:
the sender is a system that is NOT part of the Algo cluster - brand POS,
brand Labor Management, brand loyalty/VIP, third-party cabinet hardware,
cloud order publishers.

Vendor KDS devices (v2 JWT API), third-party vision / camera services
(POST /UpdateOrderItemAnalysis, POST /UpdateOrderItemAnalysisMakeLine,
POST /handler/{storeId}/tableCleanliness/events), in-store GPS /
Traffilog trackers (POST /CarrierLocation), and the POS
vehicle-assignment route (PUT /PosVehicleAssignment) are intentionally
out of scope for this iteration.

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

The Inbound API Gateway is the single entry point through which **external systems** push events into the Algo 4 cluster. It receives traffic from store-side brand POS clients, brand Labor Management feeds, brand VIP / loyalty integrations, cloud order publishers, and third-party holding-cabinet hardware. The gateway authenticates each call, persists the raw payload for audit, validates the request, normalizes the source-specific shape into a canonical Algo event, and publishes that event onto the internal EventBus. For POS-style flows it preserves the legacy `200 OK + POS response body` contract so existing brand POS clients keep working unchanged during the Algo 3 → Algo 4 migration. On the POS order-entry path the gateway makes **two synchronous calls** — to **Order Management** (to commit the order) and to **AI Promise Time** (to compute the promised pickup / delivery time) — combines both results into the POS response body, and only then publishes an **enriched** canonical event (raw inbound + OM commit result + promise time) onto the Event Bus, where the rest of the cluster (Quote, Customer, Employee, Couriers, makeline / FE projection, Ticket Service, Order Sequencer, Delivery Management, Cloud Gateway) consumes it asynchronously.

Menu Management is intentionally **out of scope for now** and is omitted from this iteration of the doc.

## 2. Scope

### In Scope

- Terminate every external HTTP `POST` / `PUT` / `PATCH` that an outside system targets today (`/PosInsert*`, `/updateOrderPayment`, `/api/order/vip*`, etc.).
- Terminate every external RabbitMQ-delivered event the cluster consumes today (`{routingKey}-cabinet`, `{routingKey}-cloud-orders`) and convert it into an inbound event of the same shape as the HTTP path.
- Capture the raw request body, headers, transport, source identity, and timestamps for every inbound message.
- Authenticate callers through the (TBD) Auth Service and attach a `sourceSystem` identity to the canonical event.
- Validate header / size / schema / store-scoping rules before publishing.
- Normalize source payloads into the canonical Algo event envelope defined in `CLAUDE.md`.
- Publish canonical events to EventBus business topics with stable partition keys (brand-country-store).
- Honor the POS-compatible HTTP response contract (`200 OK` with `time` / `status` / `rowCount` / `errDescription` / `validationErrors` / `warnings` / `orders`, plus `415` on missing `application/json`).
- Track each inbound message through the `RECEIVED → RAW_STORED → VALIDATED → NORMALIZED → PUBLISHED` lifecycle, and route `REJECTED` / `PUBLISH_FAILED` cases to the audit store and retry / DLQ topics.
- On the **POS order-entry path** (`POST /PosInsertOrders`): synchronously call **Order Management** to commit the order **and** synchronously call **AI Promise Time** to compute the promised pickup / delivery time, combine both results into the POS-compatible response body, return `200 OK` to the POS, and then publish a single **enriched** canonical event (raw inbound + OM commit result + promise time) onto the Event Bus.
- On the **POS reconciliation / status-poll paths** (`POST /GetOrderUpdatesForPos`, `GET|POST /GetPosNotification`): synchronously call **Order Management** for a read-only lookup of current order state, fold the result into the POS-compatible `posResponseByType` body, return `200 OK`, and **do not publish a canonical event** (the call is a query, not a fact).
- Never call Quote, Customer, Employee, or Couriers (Vehicle) Service synchronously — they are **bus consumers** of the published event, not sync collaborators of the gateway. Cabinet callers do **not** get a promise time in their response.
- Enforce per-source idempotency before publishing (`Idempotency-Key` header, `sourceEventId`, or `sourceSystem + storeId + eventType + sourceEventId` tuple).

### Out of Scope

- **Menu Management.** All menu-publish, item-property, and combo-publish events are deferred to a later iteration of this doc. The `kfcmenuservice` webhook chain, `POST /PosInsertItemProperties`, and `POST /combos` are not covered here.
- **Vendor KDS v2 API.** The JWT + `X-Station-ID` routes (`POST /api/order/actions`, `POST /api/orders/{orderID}`, `POST /api/orders/actions`, `PATCH /api/orders/{orderID}`, `PATCH /api/items/{itemID}`) are out of scope for this iteration of the gateway.
- **Third-party vision / camera services.** The pack-station vision route (`POST /UpdateOrderItemAnalysis`), the makeline matcher / camera route (`POST /UpdateOrderItemAnalysisMakeLine`, which carries a base64 image), and the bench / table cleanliness camera route (`POST /handler/{storeId}/tableCleanliness/events`) are out of scope for this iteration. They will be picked up in a follow-up iteration that decides image storage, payload-size enforcement, and the canonical-event split for vision facts vs. environment facts (`orders.kitchen.*` vs. `store.*`).
- **In-store GPS / Traffilog tracker push.** `POST /CarrierLocation` (`SetCarrierLocationJson`) from in-store hardware is out of scope. Mobile-sourced GPS pings still enter through the Outbound (Cloud) Gateway as today.
- **POS vehicle assignment.** `PUT /PosVehicleAssignment` (`AssignVehicleToCarrierHandler`) is out of scope; vehicle assignment will be revisited as part of a dedicated fleet design pass.
- Any HTTP route that Algo's own components call into Algo's own backend (makeline FE, Process Center station UI, line-management UI, dispatch UI, support tooling). Those are *internal* traffic and become in-cluster RPC or direct event subscription in Algo 4 — not gateway events.
- Implementing the business logic of any downstream consumer (Quote, Customer, Employee, Order Management, AI Promise Time, Cabinet UI, etc.).
- Talking to mobile devices, DaaS Gateway, or the Admin Panel UI — those are the Outbound (Cloud) Gateway's surfaces.
- Async aggregator callbacks (DoorDash / Uber / Wolt / etc.). Those arrive over the Outbound (Cloud) Gateway's DaaS EventBus consumer in Algo 4 and are republished onto the same internal topics — they do **not** enter through this gateway.
- The GPS / Traffilog telematics pull loop (`IgnitionEvent.go`). Algo today *polls* an external GPS server; nothing is pushed in. If that becomes a push channel later it can be added here.
- Long-polling / SSE / WebSocket fan-out to in-store screens (`publishStationsByWs`, `GET /handler/{storeId}/make` long-poll, `GET /api/events` SSE). Those continue to be owned by the makeline / FE service that subscribes to the bus.
- Picking the final auth scheme, raw-storage technology, retention policy, or final topic naming — those are open decisions tracked in `CLAUDE.md`.

## 3. Role in Store Operations

### Order Entry

The gateway is the front door for orders entering the store from outside. Today this happens through:

- A brand POS pushing `POST /PosInsertOrders` with a `PosOrders` envelope (`storeNo`, `fullLoad`, `orders[]`, `orderItems[]`, `translations[]`, `time`) in JSON or `OrdersXml` / `ItemsXml` form fields.
- A cloud order publisher dropping the same payload onto the `{brand}-{country}-{externalStoreId}-cloud-orders` RabbitMQ queue (`Payload` struct in `CloudOrderEvent.go`).
- Brand VIP / loyalty integrations hitting `POST /api/order/vip` and the related VIP routes.
- Payment events arriving from the POS / payment terminal on `POST /updateOrderPayment`.

For each of these the gateway captures the raw payload and validates it. On the **POS-sync variants** (`/PosInsertOrders`, `/updateOrderPayment`, `/api/order/vip`) the gateway then does the two sync calls *first*: it calls **Order Management** over a sync API to commit the order and it calls **AI Promise Time** over a sync HTTP API to compute the promised pickup / delivery time. Both calls happen in-flight before any response is returned to the POS. The gateway folds both results into the POS-compatible response body and returns `200 OK` to the caller. *After* the response is sent, the gateway publishes a single **enriched** canonical event onto the Event Bus (`orders.intake.received` and friends) carrying the raw inbound payload, the OM commit result, and the promise time, so async consumers (Quote, Customer, Employee Service, Couriers (Vehicle) Service, AI Promise Time for workload signal, makeline / FE projection, Ticket Service, Order Sequencer, Delivery Management, Cloud Gateway) see the same enriched view OM did. For the **non-POS-sync variants** (the cloud-order RabbitMQ queue) there is no caller to respond to, so the gateway skips the sync calls and publishes the canonical event directly; Order Management consumes it from the bus like any other downstream service. Neither Quote, Customer, Employee Service, nor Couriers (Vehicle) Service is ever called synchronously by the gateway — they subscribe to the bus.

### Order Modification / Cancellation

Modifications from the outside arrive on the same `/PosInsertOrders` channel (with the order id reused). The brand POS also reconciles its local order state against Algo by polling `POST /GetOrderUpdatesForPos` and `GET|POST /GetPosNotification` — those are **query-shaped reads**: the gateway sync-calls Order Management, folds the result into the POS-compatible `posResponseByType` body, and returns `200 OK` without publishing anything to the Event Bus (see §4 "Query-shaped inbound"). Customer-driven cancellations from aggregators do not enter here — they come through the Outbound (Cloud) Gateway's DaaS EventBus path and land on the same internal topics. Vendor-KDS-driven kitchen-side modifications are out of scope for this iteration (see §2).

### Kitchen Preparation & Bumping

The gateway accepts the POS-to-KDS push (`POST /PosInsertNotification`, the one route that strictly enforces `Content-Type: application/json` and returns `415` otherwise). Cabinet hardware does not use HTTP — it publishes on `{routingKey}-cabinet` and the gateway picks it up there. Vendor-KDS-driven kitchen lifecycle (the v2 JWT API) and third-party vision / camera routes (pack-station, makeline matcher, table-cleanliness) are out of scope for this iteration (see §2).

The legacy Algo makeline FE endpoints (`POST /handler/{storeId}/make`, `setOrdersReachedTheMakeLine`, `printOrder`, `updateMatcherMetaData`, line-lock) are Algo's own UI talking to Algo's own backend — they are **not** inbound events for this gateway.

### Delivery / Handoff

The gateway is *not* the dispatch boundary. Driver-side, customer-side, and DaaS-side delivery traffic flows through the Outbound (Cloud) Gateway, and there is no in-store hardware delivery traffic in scope for this iteration — in-store GPS / Traffilog tracker push (`POST /CarrierLocation`) and POS vehicle assignment (`PUT /PosVehicleAssignment`) are out of scope (see §2).

### Day Open / Day Close

There is no dedicated day-open or day-close HTTP route. The day-start signal is implicit: the brand POS sends an initial `fullLoad=true` `PosInsertOrders` / `PosInsertEmployees` / `PosInsertEmployeesScheduling` / `PosInsertPermissionGroups`. The gateway forwards each as a normal inbound event; downstream services treat `fullLoad` as the day-open snapshot signal. Recovery after a gateway restart relies on EventBus consumer-group offsets in downstream services and on the raw audit store for replay. (Menu-publish at day-open is deferred — see §2.)

### Exception Flows

External-facing exceptions visible to this gateway:

- **Brand POS retries** the same `PosInsertOrders` / `updateOrderPayment` request — handled by the idempotency layer.
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

#### `orders.intake.received`

Triggered by `POST /PosInsertOrders` from the brand POS (`PosServices.go`, payload `PosOrders`) or a message on `{brand}-{country}-{externalStoreId}-cloud-orders` from a cloud order publisher (`CloudOrderEvent.go`). On the POS-sync path the gateway calls Order Management and AI Promise Time synchronously, folds both results into the POS response body, returns `200 OK`, then publishes the **enriched** canonical event (raw + OM commit result + promise time) keyed by `(storeNo, orderId)`; the cloud-order path skips the sync calls and publishes the raw-only event directly. Idempotent on `(storeNo, orderId, sourceEventId)`, and `fullLoad=true` is the day-open snapshot that downstream services treat as a replace-all.

#### `pos.notification.posted`

Triggered by `POST /PosInsertNotification` (`PosInsertNotificationRequest`: `storeNo`, `kdsList`, `notification`, `time`) — operator messages typed at the POS that must surface on selected KDS stations. The gateway captures, validates, and publishes the event for the makeline / FE projection to fan out over WebSocket. **This is the only inbound HTTP route that strictly enforces `Content-Type: application/json`** and returns `415 Unsupported Media Type` on miss.

#### `orders.payment.updated`

Triggered by `POST /updateOrderPayment` from the POS / payment terminal bridge. Normalize to a payment-update event keyed by `(storeId, orderId, paymentId)` and publish for Order Management. Idempotent on `paymentId`.

#### `customer.vip.linked` / `customer.vip.unlinked`

Triggered by `POST /api/order/vip`, `DELETE /api/order/vip`, and the `/api/order/vip/*` sub-routes from brand VIP / loyalty integrations. Publish VIP-link / unlink events keyed by `customerId` (or `vipKey` as a fallback when no customer record exists yet); Customer Service and Order Management subscribe. VIP is a **customer**-domain fact, not an order fact — it persists across many orders.

### Labor Management (publishes to `employee.*`)

> Naming note: `employee.*` covers the **roster / HR** entity that every store worker shares — profile, schedule, permission-group membership. The delivery-only operational facts (status, GPS location, session, vehicle, available-orders) live on `courier.*` and represent the courier-role projection of an employee (i.e. `courierId == employeeId` for employees whose role is courier). Owner mapping: `employee.*` → Employee Service; `courier.*` → Couriers (Vehicle) Service.

#### `employee.profile.upserted`

Triggered by `POST /PosInsertEmployees` from brand labor management (form `CarriersXml` / `EmployeesXml` + `StoreNo` + `FullLoad`, or raw JSON) — the roster snapshot or delta for every store worker (couriers, kitchen, managers). Normalize each employee into one event keyed by `(storeId, employeeId)`; `FullLoad=true` is a roster replace. Employee Service is the primary consumer; the Couriers (Vehicle) Service joins on `employeeId` for employees whose role is courier.

#### `employee.schedule.upserted`

Triggered by `POST /PosInsertEmployeesScheduling` from brand labor management (`EmployeesXml`, `FullDayLoad`, or JSON variant). Normalize per-employee per-day shift entries and publish keyed by `(storeId, employeeId)`.

#### `employee.permission_group.upserted`

Triggered by `POST /PosInsertPermissionGroups` (`PermissionGroupsXml` or JSON) from brand labor / IT. Publish permission-group definitions for Employee Service / Auth — permission groups are HR-domain authorization rules that describe what an employee role can do, which is why they live on `employee.*` rather than the generic `admin.*` bucket. **Gap:** today the only inbound surface for permission groups is this POS bulk snapshot; there is no granular create / update / delete / assign / revoke API — see §9 Open Questions.

### Cabinet (third-party holding-cabinet hardware — publishes to `orders.cabinet.*`)

#### `orders.cabinet.status_changed`

Triggered by AMQP messages on `{brand}-{country}-{externalStoreId}-cabinet` from holding-cabinet hardware via the PUC bridge (handler `OnPucMessage`, payload `entity.CabinetOrderResponse`: `id`, `qr_code`, `load_code`, `lockers { heated, ambient }`, `status`) — the cabinet reports loaded / collected slots so Order Management knows when the order is in the cabinet and when it's been collected. Publish keyed by `orderId`, falling back to `(storeId, cabinetOrderId)` while the cabinet id is not yet linked to a system order. Manual ack today, malformed messages rejected, and at least partition-per-store ordering must be preserved on EventBus.

### Query-shaped inbound (POS reconciliation polls — not facts, no canonical event)

The following routes look like inbound POSTs but are **queries**, not facts. The brand POS uses them to reconcile its local order state against Algo. The gateway terminates the HTTP, sync-calls Order Management for a read-only lookup, folds the result into the legacy POS-compatible response body, and **does not publish a canonical event** to the Event Bus — there is nothing to fan out, the caller just wanted to read.

#### `POST /GetOrderUpdatesForPos`

POS reconciliation poll: "what has changed for my store since the last poll?" The gateway sync-calls Order Management for the delta and returns the legacy `posResponseByType` body (`{ time, status, rowCount, errDescription, validationErrors, warnings, orders }`) populated with the changed orders. No canonical event is emitted. On OM timeout, return `200 OK` with a `still-processing` POS body so the POS keeps polling.

#### `GET|POST /GetPosNotification`

POS notification poll: "is there an operator notification waiting for me?" The gateway sync-calls Order Management for queued notifications and returns the same legacy `posResponseByType` body shape. No canonical event is emitted. Operator notifications generated by Algo itself ride on `pos.notification.posted` for the makeline projection; this poll is just the brand POS pulling them back over its native channel.

**Why these aren't events.** Every other entry in §4 represents a fact about something that happened in the outside world (an order arrived, a payment posted, a cabinet door closed). A reconciliation poll is the **brand POS asking a question** — Order Management already owns the answer. Publishing a "POS asked a question" event onto the bus would create noise without a useful consumer. The gateway therefore treats both routes as **read-through RPC** against Order Management on the POS-compatible wire contract, and §6 lists them under the OM sync API as a read path.

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
- **Async aggregator callbacks** (`OnAggMessage`, `OnETAmessage` on the aggregator RabbitMQ queues): these arrive via the **Outbound (Cloud) Gateway's DaaS EventBus consumer** in Algo 4, not through this gateway.
- **Telematics GPS pull** (`IgnitionEvent.go` `GetGppNotificationsFromServer`): Algo *polls* an external server; nothing is pushed in. Not an inbound event.
- **QA-only:** `POST /demo/*` behind `AllowQaCommands` — do not migrate.
- **Menu Management (deferred):** the `kfcmenuservice` webhook chain (`POST /menu`), `POST /PosInsertItemProperties`, `POST /combos`, and any other menu-publish surface are not covered in this iteration. They will be added back once the menu integration is designed.

## 5. Events Emitted

<!--
Every emitted event is a canonical Algo event published on the internal
EventBus bus, with the envelope defined in CLAUDE.md (eventId, correlationId,
eventType, sourceSystem, brandId, country, storeId, receivedAt,
rawRequestRef, data{}).
-->

### `orders.intake.received`
**Emitted when:** A `POST /PosInsertOrders` request has been captured, validated, normalized, and (on the POS-sync path) **after** OM has committed the order and AI Promise Time has returned a promise time — i.e. the event published to the bus is the **enriched** version that carries the OM commit result and the promise time alongside the raw inbound. **Or** when a message has been pulled from `{routingKey}-cloud-orders` (no sync calls; the event carries only the raw inbound).
**Payload summary:** Per-order envelope with `storeId`, `orderId`, `saleType`, addresses, `items[]`, payments, `fullLoad` flag, `sourceEventId`, `rawRequestRef`, plus (on the POS-sync path only) `orderManagementResult` and `promiseTime`.
**Expected consumers (all async, on the Event Bus):** Order Management (authoritative state for non-POS-sync flows — on the POS-sync path OM has already been called sync and treats the bus event as a confirmation), Customer Service (address dedupe — PII boundary), Employee Service (employee correlation — PII boundary), Couriers (Vehicle) Service (courier-side context for delivery orders — PII boundary), Quote Service (price recomputation when items change), AI Promise Time (workload signal for non-POS flows; on the POS-sync path Promise Time has already been called sync), makeline / FE projection, Ticket Service, Order Sequencer, Delivery Management, Cloud Gateway (outbound mirror to DaaS / mobile).
**Notes:** Partition key `orderId`. Idempotent on `(storeId, orderId, sourceEventId)`. The gateway never calls Quote, Customer, Employee Service, or Couriers (Vehicle) Service synchronously — they receive their copy off the bus.

### `pos.notification.posted`
**Emitted when:** A `POST /PosInsertNotification` is accepted (`Content-Type: application/json` enforced).
**Payload summary:** `storeId`, `kdsList[]`, `notification`, `time`.
**Expected consumers:** Makeline / FE projection (WebSocket fan-out).

### `orders.payment.updated`
**Emitted when:** A `POST /updateOrderPayment` is accepted.
**Payload summary:** `storeId`, `orderId`, `paymentId`, payment shape.
**Expected consumers:** Order Management.

### `customer.vip.linked` / `customer.vip.unlinked`
**Expected consumers:** Customer Service / VIP projection, Order Management.

### `employee.profile.upserted`, `employee.schedule.upserted`
**Expected consumers:** Employee Service (primary); Couriers (Vehicle) Service joins on `employeeId` for courier-role employees.

### `employee.permission_group.upserted`
**Expected consumers:** Auth Service, Employee Service.

### `orders.cabinet.status_changed`
**Emitted when:** A message arrives on `{routingKey}-cabinet`.
**Expected consumers:** Order Management, Cabinet UI / customer-collect projection.

### Audit lifecycle (cross-cutting platform topic)
**Emitted when:** Every lifecycle transition (`RECEIVED`, `RAW_STORED`, `VALIDATED`, `REJECTED`, `NORMALIZED`, `PUBLISHED`, `PUBLISH_FAILED`, `DEAD_LETTERED`).
**Payload summary:** `eventId`, `correlationId`, `sourceSystem`, status, `rawRequestRef`, timestamps, error details.
**Expected consumers:** Audit Service.
**Notes:** This is a platform / observability topic, not an entity-domain event. It does not appear in the entity-topic catalog because it carries metadata about all events. Single observability stream feeding the table in §6 of `CLAUDE.md`.

## 6. Data Dependencies (External Reads)

### Store / brand / country routing context
**Owning service:** Store Service (today: rows in Algo SQLite, accessed via `Stores.getQueueRoutingKey()`).
**When fetched:** On every inbound event — needed to build the canonical envelope (`brandId`, `country`, `storeId` and the EventBus partition key).
**Why needed:** Inbound payloads usually carry only `storeNo` / `storeId` / `storeNumber`. The canonical envelope demands brand + country + canonical store id; downstream topics partition on `(brand, country, storeId)`.
**How fetched:** TBD — likely a cached lookup against Store Service refreshed every N minutes. Cache miss must not block ingestion; fall back to "unknown brand/country" and mark `REJECTED` if the store can't be resolved.
**Notes:** Staleness tolerance is "minutes" — store routing rarely changes. Must survive Store Service downtime via the cache.

### Caller identity → source system mapping
**Owning service:** Auth Service (TBD).
**When fetched:** On every inbound request.
**Why needed:** Sets `sourceSystem` (`brand-pos`, `vendor-kds`, `labor-management`, `cabinet-puc`, `gps-tracker`, `cloud-order-publisher`, `vip-loyalty`, etc.) and tenancy scoping. Drives per-source rate limits and topic ACLs.
**How fetched:** Sync auth call on every request. Cacheable per credential / token for short windows.
**Notes:** **Open** — the Auth Service contract is TBD per `CLAUDE.md`. The gateway must handle the "no Auth Service yet" interim case by reading `X-Station-ID` / `Bearer` / brand-shared API keys.

### Order Management sync API (POS-sync flows — both write and read)
**Owning service:** Order Management.
**When fetched:**
  - **Write path —** Synchronously on `POST /PosInsertOrders` to commit the order and obtain the data needed for the `rowCount` / `orders[]` portion of the POS body.
  - **Read path —** Synchronously on every `POST /GetOrderUpdatesForPos` and `GET|POST /GetPosNotification` reconciliation poll (read-only lookup; no bus publish — see §4 "Query-shaped inbound").
**Why needed:** The POS-compatible response body must reflect real Order Management state, not just the gateway's intent. On `PosInsertOrders` the gateway combines OM's commit result with AI Promise Time's response and only then returns to the POS. On the reconciliation polls, OM is the source of truth for what the POS is asking about.
**How fetched:** Sync HTTP / gRPC call with a tight timeout. On the write path, an OM timeout falls back to a "still processing" POS body so the caller does not block, and Order Management still receives the event from the bus once the gateway publishes. On the read paths, an OM timeout returns an empty `posResponseByType` body with a soft failure status so the brand POS retries on its next poll cycle.
**Notes:** OM is the **only** Algo 4 cluster service the gateway calls synchronously on the inbound write path, and the **only** cluster service called synchronously on the read paths. All other cluster services (Quote, Customer, Employee Service, Couriers (Vehicle) Service, AI Promise Time for non-POS flows, makeline projection, Ticket Service, Order Sequencer, Delivery Management, Cloud Gateway) are bus consumers, never sync collaborators. Latency budget on the sync path is the most important non-functional requirement of the gateway.

### AI Promise Time sync HTTP API (POS order-entry only)
**Owning service:** AI Promise Time.
**When fetched:** Synchronously on `POST /PosInsertOrders` for POS-originated orders. The gateway issues the OM commit call and the Promise Time call in-flight, then combines both results into the POS response body before returning.
**Why needed:** The POS response body must carry the promised pickup / delivery time for the new order so the brand POS can show it to the cashier / customer on the same screen.
**How fetched:** Plain HTTP `POST` against AI Promise Time's sync endpoint with a tight timeout (target single-digit hundreds of ms). On timeout or 5xx, the gateway returns a `200 OK` POS body with a `still-computing` / fallback promise marker so the caller does not block.
**Notes:** Promise Time also subscribes asynchronously to `orders.intake.received` on the bus — for cloud orders (no POS-sync caller) and for general workload signaling. The sync HTTP call is restricted to the POS order-entry path; cabinet / cloud-order responses do **not** carry a promise time. Exact field name in the POS body is TBD — see §9.

### Other cluster services (Quote, Customer, Employee Service, Couriers (Vehicle) Service, makeline / FE projection, Ticket Service, Order Sequencer, Delivery Management, Cloud Gateway)
**Owning service:** Each respective service in the Algo 4 cluster.
**When fetched:** Never synchronously. They subscribe to the Event Bus and consume the enriched canonical event the gateway publishes.
**Why noted here:** The BYTE Kitchen and Fleet architecture diagram shows direct lines between the API Gateway and Quote / Customer / Employee Service. Those lines represent **cluster adjacency**, not sync calls. The gateway's only sync collaborators on the write path are Order Management and AI Promise Time.
**Notes:** Customer, Employee Service, and Couriers (Vehicle) Service are PII boundaries — the gateway scrubs / partitions per their contract before publishing, but does not query them on the request path.

### Idempotency-key store
**Owning service:** Self-owned (Redis / DynamoDB / Postgres TBD).
**When fetched:** On every inbound event, before publishing.
**Why needed:** Reject duplicates from external retries (`sourceEventId`, `Idempotency-Key`, or `(sourceSystem, storeId, eventType, sourceEventId)` tuple).
**Notes:** Final tech is TBD.

### Raw payload store
**Owning service:** Self-owned (S3 / blob / object store TBD), **private to this gateway**.
**When fetched:** Written on every inbound message. Read **only by the gateway itself** for replay flows it initiates and by out-of-band operator / support tooling. **No other Algo service reads this store directly.**
**Why needed:** Audit, debug, replay. The canonical event only carries a `rawRequestRef`; consumers that want the raw bytes must request a re-publish (operator-driven), not query the store themselves.
**Notes:** Retention and PII handling rules are TBD per `CLAUDE.md`. Treated as a private dependency, not an exposed surface (see §7 "Not exposed").

## 7. API Exposed to Other Services

> **Architecture rule:** the gateway exposes **no database** and **no internal API** (HTTP, gRPC, query, lookup, replay) to any other service inside the Algo cluster. The only surface other Algo services see is the **canonical EventBus topics in §5**. Everything else listed below is either external-facing (for non-Algo callers) or internal to the gateway itself. If a downstream service needs to know whether a message was received, it consumes the bus; if it needs the raw payload, the bus event carries `rawRequestRef` and the raw store is operated as a private blob store accessed only by the gateway and offline replay tooling — not by another Algo service at runtime.

### External-facing inbound HTTP surface (for non-Algo callers only)

**Purpose:** Be wire-compatible with every external POST that targets Algo 3 today, so brand POS / Labor Management / cabinet / VIP clients can keep their existing URLs and payload shapes.
**Known callers:** Brand POS, brand cloud order publishers, brand Labor Management, brand VIP / loyalty integrations, third-party holding-cabinet PUC. **No service inside the Algo cluster calls this surface** — Algo-internal callers (legacy makeline FE, Process Center, line-management, dispatch UI) are the "Not inbound events" list at the end of §4.
**Inputs:** Every route listed in §4 with its current payload shape.
**Returns:** POS-compatible body: `{ time, status, rowCount, errDescription, validationErrors, warnings, orders }` for POS routes; on the POS order-entry path the body also carries the promise time returned by AI Promise Time (exact field name TBD — see §9). The only non-`200` status produced at the HTTP layer is `415 Unsupported Media Type` on `/PosInsertNotification` when `Content-Type` is missing.
**Notes:** Per §2 of `CLAUDE.md`, the response contract is fixed and must be preserved verbatim. Auth is per the (TBD) Auth Service; today's behavior is brand-shared API keys.

### Internal canonical-event publishing surface (the only surface to other Algo services)

**Purpose:** Publish the canonical Algo event envelope (`CLAUDE.md` §"Canonical Algo Event") to EventBus topics listed in §5. On the POS order-entry path the published event is the **enriched** version that carries the OM commit result and the promise time alongside the raw inbound payload. **This is the only way the gateway exposes anything to other Algo services.**
**Known consumers (inside Algo, per the Kitchen and Fleet cluster diagram):** Order Management, Quote Service, Customer Service (PII — owns `customer.*`), Employee Service (PII — owns `employee.*`), Couriers (Vehicle) Service (PII — owns `courier.*`, joins on `employeeId`), AI Promise Time (async workload signal), Ticket Service (routing + line management + item sequencing), Order Sequencer (prioritization), Recommendation Service (batches), Delivery Management (order + driver assignment), Notification Dispatcher, Gateway UI (KDS — Makeline / Pack / Expo, and Dispatch), Cloud Gateway (outbound mirror to DaaS / mobile / Tracker).
**Inputs:** Internally derived from each inbound event; on the POS-sync path also augmented with the OM commit response and the AI Promise Time response.
**Returns:** Async — consumers subscribe; the gateway does not block on consumption.
**Notes:** Partition keys follow the entity: `orderId` for `orders.*`, `employeeId` for `employee.*`, `courierId` for `courier.*` (= `employeeId` for courier-role employees), `customerId` (or `vipKey`) for `customer.*`, `storeId` for `store.*` / `admin.*` / `delivery.provider.*`. Schema versioning via the `schemaVersion` envelope field.

### Not exposed (deliberately)

- **No internal HTTP / gRPC / query API for other Algo services.** No "give me the raw payload by `eventId`" endpoint. No "look up lifecycle by `correlationId`" endpoint. If a downstream service needs that, it must keep its own state from bus events.
- **No shared database access.** The gateway's idempotency store and raw payload store (§6) are private to the gateway. Other Algo services do not connect to them and do not read them. Offline replay / support tooling exists, but it is operator-driven and out-of-band, not a service-to-service surface.
- **No sync RPC into the gateway.** The two sync calls on the POS-write path go *out* of the gateway (gateway → Order Management, gateway → AI Promise Time). Nothing inside the cluster calls *into* the gateway.

## 8. Operational Assumptions

- The Auth Service exists and can identify the calling external system on every inbound request (`sourceSystem`). Until it does, the gateway falls back to per-source API keys / `X-Station-ID` headers and tags `sourceSystem` accordingly.
- Store Service exposes a cached lookup from `(externalStoreId)` → `(brand, country, canonicalStoreId)` and stays fast enough to be on the hot path.
- Order Management exposes a sync API fast enough to satisfy the POS-compatible response-body contract within the brand POS's timeout (low hundreds of ms).
- AI Promise Time exposes a sync HTTP endpoint fast enough to be called on the POS order-entry path within the same brand POS timeout budget as the Order Management call. A timeout / 5xx degrades to a fallback marker in the POS body, never to an HTTP-level error.
- Every external POS / cabinet that posts today is willing to send the same payload to the new gateway URL — i.e. only the host changes, not the body shape, headers, or status codes.
- The brand POS is willing to receive the promise time as an additional field in the existing POS response body without a contract change on its side (or, equivalently, the brand POS already reads the field name we settle on).
- The raw-store, idempotency-store, retry topic, and DLQ topic are operationally provisioned before the gateway accepts traffic.
- Menu Management is intentionally not wired up in this iteration. When it is added back, the gateway will get a dedicated Menu Management subsection in §4 / §5 and a corresponding open question on push vs. pull.
- Cabinet hardware continues to use RabbitMQ as its transport even after Algo 4 — the gateway runs an AMQP consumer to terminate that queue and republish onto EventBus. Migrating the cabinet vendor to HTTP is out of scope for this gateway.
- Aggregator callbacks, DaaS provider events, and mobile-driver events do **not** enter through this gateway — they enter through the Outbound (Cloud) Gateway's inbound consumer paths and are republished on the same internal topics.
- Calls from Algo's own makeline FE, Process Center, line-management, and dispatch UIs are **internal** traffic and do not enter through this gateway. They become in-cluster RPC or direct event subscription in Algo 4.
- The gateway never holds the POS HTTP connection open waiting for an async event response. Sync from the gateway into the Algo 4 cluster is restricted to **two** services: Order Management (on POS-sync write and read paths) and AI Promise Time (on the POS order-entry path only). Every other cluster service — Quote, Customer, Employee Service, Couriers (Vehicle) Service, makeline / FE projection, Ticket Service, Order Sequencer, Recommendation Service, Delivery Management, Cabinet UI, telematics, Cloud Gateway — is reached **only** via the Event Bus.
- `Content-Type: application/json` enforcement (`415` on miss) applies only on `/PosInsertNotification` today. Extending it to other routes is a breaking change and requires brand sign-off.

## 9. Open Questions

- **Auth Service contract is missing.** Final scheme (API key / HMAC / OAuth / mTLS) and how `sourceSystem` identity is encoded on the request. Tracked in `CLAUDE.md` Open Decisions.
- **Menu Management is deferred.** All menu-publish flows (the `kfcmenuservice` webhook chain, `POST /PosInsertItemProperties`, `POST /combos`) have been pulled out of this iteration. Need a separate design pass to decide how Menu Management feeds the gateway and which canonical events it emits.
- **Permission Group Management API is missing.** Today permission groups only enter through the POS bulk snapshot (`POST /PosInsertPermissionGroups` → `employee.permission_group.upserted`). There is no granular inbound surface for creating / updating / deleting a single group, assigning a group to an employee, or revoking a permission. Need product to define: (a) the routes and payload shape for a Permission Group Management API, (b) the source of truth (brand POS vs. Admin Panel vs. a new labor / IT system), (c) the identity model (who can call it, per store vs. per brand), (d) the canonical event split — likely `employee.permission_group.created` / `updated` / `deleted` plus per-assignment `employee.permission_group.assigned` / `revoked` keyed by `employeeId` — so we stop overloading `employee.permission_group.upserted` with everything.
- **AI Promise Time on the POS sync path.** Confirm (a) Promise Time exposes a sync HTTP API that meets the brand POS timeout budget end-to-end (gateway → Order Management → gateway → Promise Time → gateway), (b) the exact field name and shape the promise time takes inside the existing POS response body (`{ time, status, rowCount, errDescription, validationErrors, warnings, orders }`), (c) the fallback contract when Promise Time times out or 5xx's (return a sentinel like `promiseTime: null` with `status: "ok"`, or a soft failure status).
- **Per-order tracker URL (`trackUrl`) in the POS response body.** Algo 3 returns a per-order `trackUrl` field (XML `TrackUrl`, JSON `trackUrl`) inside each entry of `PosResponse.Orders[]` on `POST /PosInsertOrders` — see `PosServices.go` `OrderResponse` (line 397) populated via `buildUrlTacker(storeId, orderId)` in `ProxyClient.go` (line 4598). The URL is `{store.CarrierFollowUpLink}/?UID={EncodeTrackUID({storeId, orderId})}`, computed from a per-store config row (`Stores.CarrierFollowUpLink`, with a per-account override at `Stores.MessagesEmailAccountStruct.CarrierFollowUpLink`) plus an encrypted UID. The brand POS reads this field and the same URL is later interpolated into customer-facing SMS / email via the `{{TrackerUrl}}` template (`ProxyClient.go` lines 4332, 4354). Decide for Algo 4: (a) who computes `trackUrl` on the POS-sync path — the gateway (so it can fold it into the response body alongside the OM commit result + promise time), Order Management as part of its commit result, or a dedicated Tracker / Notification service called sync; (b) where the per-store `CarrierFollowUpLink` config lives — Store Service, a new Tracker Service, or stays in the Algo SQLite replacement; (c) where the `EncodeTrackUID` secret lives (key rotation, per-brand vs. global); (d) whether the URL is included in the **enriched** bus event payload so Notification Dispatcher and Cloud Gateway can reuse it for SMS / email / push without recomputing.
- **GPS telematics pull (`IgnitionEvent.go`)** is currently an Algo-initiated pull against an external GPS notification server. Listed under "Not inbound events" today. Decide if the telematics vendor should switch to a push model and become a first-class inbound event, or stay as a separate adapter.
- **Cabinet RabbitMQ queue staying on RabbitMQ.** Confirm we are happy running an AMQP consumer inside the gateway, or do we route it via a dedicated AMQP-to-EventBus bridge?
- **POS notification on post-response failure.** Once the gateway has returned `200 OK` to the brand POS (sync OM commit + Promise Time succeeded), the POS connection is gone. If the subsequent bus publish, retry, or any downstream consumer (Quote, Customer, makeline projection, Cloud Gateway) later fails, **the POS is never told** — exactly mirrors Algo 3 today, where `PosServices.go` deliberately schedules post-response work with `go func(){...}` and `time.AfterFunc(time.Second*5, ...)` (see comment *"wait 5 seconds in order to make sure that the posInsertOrder response is sent first"*) and the only signal on failure is `logs.Error`. There is no `CallbackUrl` / `webhookUrl` field in the POS contract. Decide whether Algo 4 keeps the same silent-fail model (and recovery is purely operator-driven via the retry topic, DLQ, and raw-store replay), or introduces an optional async failure callback to the POS — and if the latter, who defines that contract per brand.
- **Idempotency key strategy is TBD.** Default proposal: prefer `Idempotency-Key` header, fall back to `sourceEventId`, fall back to `(sourceSystem, storeId, eventType, sourceEventId)` tuple, fall back to a hash of stable raw fields.
- **Retention of raw payloads.** PII implications for `PosInsertOrders` (customer address). Retention and access-control policy must be decided before production.
- **Topic-naming standard.** Names in §5 follow the canonical taxonomy in `docs/event-taxonomy.md` and the visual guide in `docs/topics-overview.md` (six entity domains, `snake_case`, no transport / source-system prefixes).
- **Per-source rate limits.** Brand POS bursts (especially on day-open `fullLoad=true`) need a concrete budget.
