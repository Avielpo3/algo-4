# Outbound (Cloud) Gateway

<!--
Business-side service definition. Filled from a sweep of three legacy
codebases:

- bitbucket.org/dragontailcom/algo            (today's monolith: ProxyClient.go,
                                               AggregatorsAPI.go, BackOffice.go,
                                               rabbitmq.go, daas_bridge.go, …)
- bitbucket.org/dragontailcom/daas-gateway    (Dragontail's central DaaS / 3PL
                                               gateway: HTTP-in, RabbitMQ-out)
- bitbucket.org/dragontailcom/proxy           (per-region proxy that owns the
                                               WebSocket to mobile devices and
                                               the Dragon Track tracker UI)

SCOPE OF "EVENTS" IN THIS DOC:
Only events that cross the Algo cluster -> Cloud boundary. Concretely, the
peer on the other side of the boundary is one of:

  * the Dragontail DaaS Gateway, which itself talks to DoorDash / UberDirect /
    Stuart / Wolt / Urbanpiper / etc.,
  * the Proxy service, which fans out over WebSocket to courier mobile devices
    and the Dragon Track customer-facing tracker UI,
  * the cloud Admin Panel UI (backoffice / dashboards / polygon editor).

Calls from external POS / KDS / Menu Management / Labor Management / cabinet /
vision / GPS hardware into Algo enter through the **Inbound API Gateway**, not
this one. Aggregator order pushes that look like cloud orders also enter
through the Inbound API Gateway. See `docs/inbound-api-gateway.md` for that
boundary.
-->

**Owner team:** Algo 4 Platform
**Status:** Draft
**Last updated:** 2026-05-17

---

## 1. Purpose

The Outbound (Cloud) Gateway is the single egress point through which the Algo 4 cluster talks to the **cloud-side world**: the Dragontail DaaS Gateway (which fronts every third-party delivery provider — DoorDash, UberDirect, Stuart, Wolt, Urbanpiper, Pandago, Grab, Cargo, Skip, Skedadel, Shipday, Magicpin, Mensajerosurbanos, Leta, Deliveroo), the per-region Proxy service (which fans out over WebSocket to courier mobile devices and the Dragon Track customer-facing tracker UI), and the cloud Admin Panel UI (backoffice, dashboards, polygon editor, store self-service). It subscribes to internal EventBus business events, translates them into the right external transport (HTTP to DaaS Gateway, EventBus to Proxy, HTTPS REST to the Admin Panel), and — in the reverse direction — consumes DaaS Gateway provider callbacks, Proxy mobile/tracker events, and Admin Panel write requests, and republishes them onto the internal EventBus as canonical Algo events. It replaces today's `algo/ProxyClient.go` WebSocket-to-Proxy, `algo/AggregatorsAPI.go` direct HTTP-to-DaaS, `algo/rabbitmq.go` aggregator / quote / ETA consumers, and `algo/BackOffice.go` admin-facing handlers so that nothing inside the cluster talks to the outside cloud directly.

## 2. Scope

### In Scope

- Subscribe to internal "delivery intent" EventBus events (quote requested, delivery created / updated / canceled, ETA requested, rating posted) and translate them into the DaaS Gateway HTTP contract used today (`POST /api/v2/deliveries/quotes`, `POST /api/v2/deliveries`, `POST /api/v2/deliveries/eta`, `POST /api/v2/deliveries/rate`, `DELETE /api/v2/deliveries`, plus `POST /mp/{provider}/order/status` and `POST /mp/{provider}/order/switch` for marketplace decisions). Paths/structs are the ones defined in `algo/pkg/consts/constValues.go` (`EtaPath`, `QuotePath`, `DeliveryPath`, `CarrierRatingPath`, `SwitchOrderToMPPath`, `UpdateMPOrderStatusPath`) and in `daas-gateway/api/handler/router.go`.
- Subscribe to internal "device-bound" entity EventBus events (`orders.delivery.status_changed`, `orders.delivery.bid_offered`, `courier.session.configured`, `courier.session.cleared`, `courier.available_orders.updated`, `courier.message.posted`, `courier.message.expired`, …) and translate each one in code into the device wire format `helper.PsEvent` (Order, Carrier, Location, AppData, ClearData, ClearNotifications, PushNotify, TrackOrder, Geofence, Checklist, StoreStatus, BidRequest, Panic, AvailableOrders, MobileParameters, …) on the Proxy ingress channel, replacing the Algo→Proxy WebSocket `sendEventsToProxy` loop in `algo/ProxyClient.go` (the inner `eventName` set is the `SuppotredEvents` array sent on `LoginToProxy`).
- Consume DaaS Gateway provider callbacks on EventBus and republish them as canonical Algo events on the internal bus. Today these arrive over RabbitMQ as the aggregator callback queue (`{routingKey}` → `OnAggMessage` in `algo/AggregatorsEvents.go`), the ETA queue (`{routingKey}-eta-v2` → `OnETAmessage`), and the quote queue (`{routingKey}-quote` → `OnMultiDaasQuote`). The wire shape matches `daas-gateway/api/handler/callback.go` (`AlgoCallback { providerID, targets[], courier{} }`) and the `AggMessage` shape Algo unmarshals today.
- Consume Proxy mobile/tracker events on EventBus and republish them onto the internal bus: courier location pings (`POST /setCarrierLocation` → `locationHandler` in `proxy/DragonTrack.go`, plus telematics on RabbitMQ `{routingKey}-courier-location`), per-event replies and acks from devices (`PsEvent.Reply`), and the tracker-service v2 events (`forwardeventstotrackerv2` — `carrier`, `order`, `location` in `proxy/eventForwarders.go`). Today these flow over the bidirectional WebSocket between Algo's `ProxyClient.go` and the Proxy `conn.go`.
- Expose the cloud Admin Panel UI's HTTPS REST surface. Today the same endpoints live inside Algo (`algo/BackOffice.go`, `algo/endpoints.go` `registerBORoutes` — `/dashboard/settings`, `POST /dashboard`, `POST /dashboard/*`, `POST /getPolygons`, `POST /backoffice/getPolygons`, `/bo*`, `/backofficeHeader*`, `/backofficeNav*`, `/api/stores/self`, `/getSingleStoreNo`, `/register`, `/changePassword`, `/forgotPassword`, plus the `/api/v2/stores/{id}/daas` provider-status read from `algo/daas_bridge.go`). In Algo 4 the Admin Panel becomes a cloud-hosted UI talking to this gateway, which fronts the underlying store / dashboard / polygon services.
- Honor the existing DaaS Gateway response contract on synchronous calls (the JSON envelopes in `daas-gateway/api/response/json.go` and the per-provider `AggResponse{ status, data, message }` shape in `algo/AggregatorsAPI.go`).
- Track every outbound message through a `REQUESTED → DISPATCHED → ACKED | FAILED | DEAD_LETTERED` lifecycle, with retries and DLQ for both the HTTP-to-DaaS leg and the EventBus-to-Proxy leg (mirroring the `RECEIVED → … → PUBLISHED | DEAD_LETTERED` lifecycle of the Inbound API Gateway).
- Enforce per-provider rate limits (`AvailableCalls` / `CallLimit` from `entity.AggregatorSettings`, today read in `algo/AggregatorsAPI.go` and `algo/daas_bridge.go`) and per-device rate limits / whitelists (`StoresWhiteList`, `CarriersWhiteList` from `proxy/main.go` `setEventForwardingWhitelists`).
- Hold the cloud-side credentials: DaaS Gateway basic-auth / bearer / per-provider keys, Proxy bearer (`proxyServiceAuthToken`, `X-StoreNo`, `X-Market` headers from `algo/ProxyClient.go` `ConnectToProxyMicroService`), Admin Panel UI JWT.

### Out of Scope

- Every push from outside Algo into Algo — POS, vendor KDS, brand Menu Management, brand Labor Management, brand VIP, cabinet hardware, vision cameras, in-store GPS, cloud order publishers — that surface is owned by the **Inbound API Gateway** (`docs/inbound-api-gateway.md`).
- The DaaS Gateway's own provider plumbing — actually talking to DoorDash, UberDirect, Stuart, Wolt, Urbanpiper, etc. — is owned by `daas-gateway` and is opaque to us; we only see the gateway's normalized HTTP surface and its normalized callback queues.
- The Proxy service's own WebSocket lifecycle, device authentication, device approval / registration, MAC/UID handling, SSL termination on the tracker port, ACME, and the per-region routing in `proxy/main.go` / `proxy/AuthServer.go` / `proxy/conn.go` / `proxy/hub.go` are owned by the Proxy service. Algo 4 only emits / consumes Proxy-bound EventBus events; it does not talk to phones.
- Mobile-app store URLs, app versioning, and `CheckVersion` content negotiation (`proxy/v2Handlers.go`).
- The Dragon Track HTML pages (`proxy/home.html`, `proxy/homeS.html`, `proxy/Dockerfile`) and the customer-facing tracker UI itself.
- SSH reverse-tunnel remote support (`algo/CloudRemoteAccess.go`, `RemoteAccessQueue` on RabbitMQ). Today this rides on the same broker as the aggregator callbacks; in Algo 4 it should remain a separate operational channel and not flow through this gateway.
- The legacy backoffice CRUD plumbing (`crudhnd.TableHandler`, `crudhnd.BackofficeStructureHandler`, `crudhnd.BackofficeNavHandler` in `algo/endpoints.go` `registerBORoutes`) is *exposed* through this gateway but the actual table-editing logic belongs to whichever domain service owns each table.
- The aggregator → cloud-orders push that arrives as `{routingKey}-cloud-orders` (handler `ProccessCloudOrdersEvent`, struct `Payload` in `algo/CloudOrderEvent.go`). That is *order intake* and is owned by the **Inbound API Gateway**, even though it rides on RabbitMQ today; calling it "cloud" is a transport accident.
- Auth Service contract (same TBD as in the Inbound API Gateway). Until decided, the gateway falls back to today's per-leg credentials.
- DLQ / replay tooling, raw-payload retention policy, EventBus technology choice, exact topic-naming standard.

## 3. Role in Store Operations

### Order Entry

The gateway is **not** the order-entry boundary. Orders enter the cluster through the Inbound API Gateway (POS push, cloud-orders RabbitMQ, vendor-KDS v2). What this gateway sees during order entry is the *follow-on* fan-out:

- When a new order is accepted internally, an entity event on `orders.delivery.status_changed` (status: `assigned`) is published onto the internal bus; this gateway translates it into an `Order` PsEvent and forwards it via the Proxy ingress EventBus topic to the Proxy service, which delivers it over WebSocket to the assigned courier's phone. This is the algo-4 replacement for `algo/ProxyClient.go`'s `sendEventsToProxy` + `proxy/conn.go` `ForwardEventToDevice`.
- When Delivery Management's decision is "use a 3PL carrier" the gateway picks up a quote / create-delivery intent off the bus and calls the DaaS Gateway (`POST /api/v2/deliveries/quotes` → `POST /api/v2/deliveries`), with the body shape currently built by `AggregatorsAPI.go` `AggRequest` (storeId, brand, country, location, targets[], tzdata, pickupReadyAt, isCash, isAlcoholic, providerCouriersCount, …).

### Order Modification / Cancellation

- Internal cancel / re-route decisions become outbound calls: `DELETE /api/v2/deliveries` on the DaaS Gateway (the `proxy_helper.Delivery` cancellation path in `daas-gateway/api/handler/proxy.go`), and an `Order { status: canceled }` PsEvent forwarded through the Proxy EventBus leg so the courier's phone updates.
- Marketplace re-routing decisions (today `SwitchOrderToMPCarrier` / `stayWithStoreCarrier` in `algo/AggregatorsAPI.go`) become outbound `POST /mp/{provider}/order/switch` / `POST /mp/{provider}/order/status` calls (the `MarketplaceProxy` handler in `daas-gateway/api/handler/marketplaceProxy.go`).
- DaaS provider-driven cancellations (driver couldn't deliver, customer canceled at aggregator) arrive as provider callbacks on the inbound leg and are republished as `delivery.provider.callback` on the internal bus — Order Management decides what to do with them.

### Kitchen Preparation & Bumping

- The gateway is not involved in kitchen lifecycle as a destination. Kitchen "show / prep / cook / bump" events that need to be visible on a courier's phone (status overlays, pickup-ready signals) are forwarded as `Order` PsEvents through the Proxy EventBus leg.
- Holding-cabinet, makeline-camera, table-cleanliness, and POS-side notification routes are all **inbound** and owned by the Inbound API Gateway.

### Delivery / Handoff

This is the gateway's busiest flow. Two halves:

- **Outbound (Algo → cloud):**
  - `delivery.provider.quote_requested` → `POST /api/v2/deliveries/quotes` on DaaS Gateway (today `AggregatorsAPI.go` → `consts.QuotePath`).
  - `delivery.provider.create_requested` → `POST /api/v2/deliveries` (today `AggregatorsAPI.go` → `consts.DeliveryPath`).
  - `delivery.provider.eta_requested` → `POST /api/v2/deliveries/eta` (today `consts.EtaPath`; daas-gateway hard-routes to Urbanpiper).
  - `delivery.provider.cancel_requested` → `DELETE /api/v2/deliveries`.
  - `delivery.provider.rate_requested` → `POST /api/v2/deliveries/rate` (today `consts.CarrierRatingPath`).
  - `orders.marketplace.switched` → `POST /mp/{provider}/order/switch`; `orders.marketplace.status_changed` → `POST /mp/{provider}/order/status`.
  - **No transport topic for device-bound events.** CGW subscribes to entity topics (`orders.delivery.status_changed`, `orders.delivery.bid_offered`, `courier.session.configured`, `courier.session.cleared`, `courier.available_orders.updated`, `courier.message.posted`, `courier.message.expired`, …) and translates each one in code into a device-shaped `PsEvent` (Order, Carrier, Location, ClearData, PushNotify, TrackOrder, …) on the Proxy ingress channel, replacing `ProxyClient.go`'s outbound WebSocket frames.
- **Inbound (cloud → Algo):**
  - DaaS provider callbacks for each delivery (`Assigned`, `Accepted`, `EnrouteToPickup`, `ArrivedAtStore`, `PickedUp`, `Nearby`, `ArrivedToCustomer`, `Delivered`, `Cancelled`, `CouldNotDeliver`) → `delivery.provider.callback`. Wire shape `AlgoCallback` from `daas-gateway/api/handler/callback.go` and `AggMessage` from `algo/AggregatorsEvents.go`.
  - DaaS provider callback `CourierLogin` (Stuart-style fleet on/off-shift) → `courier.status.changed` (status: `logged_in` / `logged_out`).
  - Quote responses (today `QuoteResponse` returned on `consts.QuotePath` synchronously for single-provider, or aggregated by `proxy_helper.GetQuoteResponse` in `daas-gateway/api/handler/proxy.go` and republished on `{routingKey}-quote` for multi-DaaS) → `delivery.provider.quote_received` / `delivery.provider.quote_failed`.
  - ETA updates (today `OnETAmessage` on `{routingKey}-eta-v2`) → `delivery.provider.eta_updated`.
  - DaaS create response → `delivery.provider.created` on accept, `delivery.provider.create_failed` on `4xx` / `5xx`.
  - DaaS cancel ack on `DELETE /api/v2/deliveries` → `delivery.provider.canceled`; the cancellation audit feed (`sendCancellationAuditMessage` in daas-gateway) → `delivery.provider.cancellation_audited`.
  - DaaS marketplace ack on `POST /mp/{provider}/order/status` and `POST /mp/{provider}/order/switch` → `orders.marketplace.acked`.
  - Mobile-side carrier waypoints (`Location` PsEvent from `proxy/conn.go readPump`, `POST /setCarrierLocation` from Dragon Track UI to `proxy/DragonTrack.go locationHandler`, and Mouters telematics RabbitMQ feed) → `courier.location.updated`.
  - Mobile-side carrier login / logout, `inStore` / `outOfStore` carrier acks, "at customer address" geofence transitions, panic events, push-notification acks → `courier.status.changed`, `courier.message.acknowledged`. `Order` / `Carrier` acks → `orders.delivery.status_changed`. (Every `PsEvent` whose direction is device → backend, today `ProxyClient.go` `readPump` → `helper.PsEvent` with empty `CarrierId` on the envelope.)
  - Tracker-service v2 ingestion is **outbound only** today (`forwardeventstotrackerv2` in `proxy/eventForwarders.go`). Listed for completeness; if tracker-v2 ever produces callbacks they enter through this gateway.

### Day Open / Day Close

- No dedicated day-open / day-close call on the cloud side. At store boot, the gateway re-establishes its DaaS Gateway HTTP session and opens its Proxy ingress / egress EventBus consumers.
- Mobile devices treat their own reconnect as the day-open signal; the gateway just forwards whatever events flow.
- Admin Panel UI sessions are long-lived; nothing special at day boundaries.

### Exception Flows

- **DaaS Gateway returns 5xx / times out** on a quote or create: retry with the same idempotency key, then DLQ. Folded into the audit stream so dispatch can fall back to a different provider or to the in-store fleet.
- **DaaS Gateway provider callback never arrives** for an order we created: `GetMissingEventsPath` / `GetMissingEventsPathForMP` (the `GetMissingEvents` handler in `daas-gateway/api/handler/proxy.go` and `algo/AggregatorsAPI.go`) is the retry pull; preserve it in Algo 4 as a scheduled internal job that the gateway runs against DaaS Gateway.
- **Provider callback arrives for an unknown delivery** (race vs. `CountAggregatorOrder` in `parseCallbacks`): publish to a `quarantine` topic and audit, do not drop.
- **Proxy is unreachable**: queue device-bound events on the Proxy ingress EventBus topic; mobile delivery will catch up when Proxy reconnects.
- **Mobile device offline** (no socket in Proxy's hub): per-event TTL on the Proxy ingress topic; events with carrier-side delivery expectations expire and emit `courier.message.expired`.
- **Whitelist drops** (event filtered by `StoresWhiteList` / `CarriersWhiteList` per `proxy/eventForwarders.go`): record in audit, do not fail the source bus event.
- **Admin Panel auth failure**: standard `401` / `403`, no canonical event.
- **`POST /api/v2/deliveries/quotes` returns "no successful quote"** (today `proxy_helper.ErrNoSuccessfulQuoteFound` in `daas-gateway/api/handler/proxy.go`): publish `delivery.provider.quote_failed` with the reason; Delivery Management consumes that to decide store-carrier fallback (`stayWithStoreCarrier`).

## 4. Events — Listen vs Publish

This gateway is **bidirectional** at the event level. The catalog below is split first by direction (Listen vs Publish) and then by event family (DaaS / Proxy). The two indexes in §4.1 and §4.2 are the quick reference; §4.3 and §4.4 give the per-event detail.

> The cloud Admin Panel UI talks to this gateway over HTTPS REST, not over the bus. It does **not** subscribe to or publish bus events. Its REST surface is documented in §7 and is intentionally not modelled as an event family here.

### 4.0 Family map: DaaS vs Proxy

The Cloud Gateway owns two distinct event families. Each family has its own external peer, its own transport today, and its own canonical bus topics. **The shape of the event tells you the family:**

| Family | What it covers | External peer | Transport today | Transport in Algo 4 | Canonical topics |
| --- | --- | --- | --- | --- | --- |
| **DaaS** (delivery / 3PL / aggregators / marketplace) | Quoting, creating, ETA, cancelling, rating 3PL deliveries, marketplace order routing, and the corresponding provider callbacks. Every event is keyed by `(storeId, providerId, aggOrderId)` or `(storeId, orderId)`. | DaaS Gateway (`bitbucket.org/dragontailcom/daas-gateway`), which fronts every 3PL: DoorDash, UberDirect, Stuart, Wolt, Urbanpiper, Pandago, Grab, Cargo, Skip, Skedadel, Shipday, Magicpin, Mensajerosurbanos, Leta, Deliveroo. The gateway picks the provider via `aggProvider` (free-text DB lookup against `agg_services.name`) — `urbanpiper` is hard-forced for `/eta` and `/marketplace/*`. | **Out:** synchronous HTTP `POST` / `DELETE` / `GET` to DaaS Gateway. **In:** RabbitMQ queues `{brand}-{country}-{externalStoreId}-v2` (callbacks), `…-v2-quote` (multi-DaaS async quote), `…-v2-eta-v2` (rider ETA stream), plus daas-gateway-internal audit queues `provider_callbacks` / `store_orders` / `provider_quotes` / `engine_decisions`. | **Out:** HTTP unchanged. **In:** EventBus topic published by daas-gateway (preferred) or daas-gateway → AMQP-bridge → EventBus during transition. | `delivery.provider.*`, `orders.marketplace.*` |
| **Proxy** (mobile devices / TrackerV2 service / in-house fleet) | Mobile-courier WebSocket traffic. Every event is a `helper.PsEvent` keyed by `(storeId, carrierId)`. The same `PsEvent.EventName` flows in both directions; direction is determined by `event.CarrierId != ""` (Algo→mobile) vs empty (mobile→Algo). | Proxy service (`bitbucket.org/dragontailcom/proxy`), which holds the WebSocket to phones (`/ws`), the `POST /setCarrierLocation` Dragon Track UI courier-location endpoint, the TrackerV2 forwarder, and the per-store / per-carrier whitelist. | **Out:** WebSocket Algo→Proxy (`/stores/ws` or `/ws`) carrying a JSON array of `PsEvent`. **In:** WebSocket Proxy→Algo on the same connection; Dragon Track UI HTTP `POST` to Proxy; per-store telematics RabbitMQ `…-v2-courier-location`. | **Out:** EventBus topic Algo→Proxy (CGW is the only writer). **In:** EventBus topic Proxy→Algo (CGW is the only reader); CGW decodes each `PsEvent` and routes to the right entity topic. | `courier.*`, `orders.delivery.status_changed` (mobile half), `orders.delivery.bid_offered` / `bid_canceled` |

**Rule of thumb.** If the event names a delivery / 3PL / aggregator / marketplace order it is **DaaS**. If it names a mobile courier, a tracker UI action, or an in-house fleet device it is **Proxy**. Anything operator-facing that does not fit either of those is part of the Admin Panel UI HTTPS REST surface (§7), not an event.

A small number of canonical topics carry both flavours (`courier.location.updated`, `orders.delivery.status_changed`). Direction and provenance are disambiguated by the envelope:
- `sourceSystem ∈ { daas, proxy_mobile, dragon_track_ui, mouters, algo }`
- `value.status` (DaaS callback statuses are different from mobile-driven `orders.delivery.status_changed` statuses).

### 4.1 Listen Index (Internal Bus → Outbound action)

Topics CGW **subscribes** to. Each row is detailed in §4.3 (DaaS) or §4.4 (Proxy).

| Family | Internal bus topic (subscribe) | Outbound action | Today's wire path |
| --- | --- | --- | --- |
| DaaS | `delivery.provider.quote_requested` | `POST /api/v2/deliveries/quotes` → DaaS Gateway (sync single-provider OR async multi-DaaS via 202) | `algo/AggregatorsAPI.go` → `AggRequest.SendQuote` → `consts.QuotePath` |
| DaaS | `delivery.provider.create_requested` | `POST /api/v2/deliveries` → DaaS Gateway | `algo/AggregatorsAPI.go` → `AggRequest.SendDelivery` → `consts.DeliveryPath` |
| DaaS | `delivery.provider.eta_requested` | `POST /api/v2/deliveries/eta` → DaaS Gateway (always Urbanpiper-backed) | `algo/AggregatorsAPI.go` → `AggRequest.SendEtaRequest` → `consts.EtaPath` |
| DaaS | `delivery.provider.cancel_requested` | `DELETE /api/v2/deliveries` → DaaS Gateway | `algo/AggregatorsAPI.go` → `CancelDelivery` → `AggRequest.SendDelete` |
| DaaS | `delivery.provider.rate_requested` | `POST /api/v2/deliveries/rate` → DaaS Gateway | `algo/AggregatorsAPI.go` → `SendAggCarrierRating` → `consts.CarrierRatingPath` |
| DaaS | `delivery.provider.missed_events_requested` _(deprecated — not used anymore)_ | `GET /api/v1/deliveries/{storeId}/{orderId}/{providerName}/{aggId}` or `GET /api/v1/marketplace/{brand}/{country}/{storeNo}/{orderId}` | `algo/AggregatorsAPI.go` → `GetMissingEvents` (timer after create / MP-ETA) |
| DaaS | `orders.marketplace.status_changed` | `POST /mp/{provider}/order/status` → DaaS Gateway | `algo/AggregatorsAPI.go` → `stayWithStoreCarrier` → `updateMarketPlaceWithDecision` |
| DaaS | `orders.marketplace.switched` | `POST /mp/{provider}/order/switch` → DaaS Gateway | `algo/AggregatorsAPI.go` → `switchOrderToMPCarrier` → `updateMarketPlaceWithDecision` |
| Proxy | `orders.delivery.status_changed` (`assigned` / `canceled`) | EventBus → Proxy as `PsEvent { EventName: Order, value: ProxyOrder }` | `algo/ProxyClient.go` → `sendEventsToProxy` (today: WS frame) |
| Proxy | `orders.delivery.bid_offered` | EventBus → Proxy as `PsEvent { EventName: BidRequest, value: defs.Bid }` | `algo/pool.go` → `sendBidRequestToProxy` |
| Proxy | `orders.delivery.bid_canceled` | EventBus → Proxy as `PsEvent { EventName: CancelBid, value: defs.StatusBid }` | `algo/pool.go` → `sendCancelBidRequest` |
| Proxy | `courier.session.configured` | EventBus → Proxy as `PsEvent { EventName: Login | AppData | StartUpOptions | MobileParameters }` | `algo/ProxyClient.go` → `LoginToProxy`, `InsertCarrierActivity*` |
| Proxy | `courier.session.cleared` | EventBus → Proxy as `PsEvent { EventName: ClearData }` | `algo/ProxyClient.go` → `InsertCarrierActivityClearData`, `SendClearDataEventToAllCarriers` |
| Proxy | `courier.available_orders.updated` | EventBus → Proxy as `PsEvent { EventName: AvailableOrders }` | `algo/ProxyClient.go` reply path to mobile poll |
| Proxy | `courier.message.posted` | EventBus → Proxy as `PsEvent { EventName: PushNotify | PushNotification | TrackOrder }` | `algo/ProxyClient.go` → `InsertCarrierActivityPushAlertNotification`, `InsertCarrierActivityPushNotification` |
| Proxy | `courier.message.expired` | EventBus → Proxy as `PsEvent { EventName: ClearNotifications }` (TTL-driven; carved out per `event-taxonomy.md`) | New in Algo 4 — no equivalent today |
| Proxy | `courier.status.changed` (Algo-side decisions, e.g. `borrowedTo`) | EventBus → Proxy as `PsEvent { EventName: Carrier, value: CarrierEventStatus }` | `algo/ProxyClient.go` → `BuildCarrierStatusWithPerformanceJson` |
| Proxy | `store.shared_carriers.updated` | EventBus → Proxy as `PsEvent { EventName: SharedCarriers }` | `algo/SharedDrivers.go` |
| Proxy | `store.status.changed` | EventBus → Proxy as `PsEvent { EventName: StoreStatus, value: StoreStatusEvent }` | `algo/SharedDrivers.go` |
| Proxy | `courier.device.approval_requested` | EventBus → Proxy as `PsEvent { EventName: ApproveDevice }` | `algo/FrontEndService.go` → `ApproveDeviceByEventId` |
| Proxy | (Tracker v2 forward — see §4.4.6) | EventBus → Proxy with `target: trackerV2` discriminator | `proxy/eventForwarders.go` → `ForwardToTrackerServiceV2` |

### 4.2 Publish Index (External → Internal Bus)

Topics CGW **publishes** when an external peer pushes something into the cluster. Each row is detailed in §4.3 (DaaS) or §4.4 (Proxy).

| Family | External trigger | Internal bus topic | Today's wire path |
| --- | --- | --- | --- |
| DaaS | DaaS Gateway provider callback (delivery lifecycle) | `delivery.provider.callback` | RabbitMQ `{brand}-{country}-{externalStoreId}-v2` → `algo/rabbitmq.go` → `OnAggMessage` → `parseCallbacks` |
| DaaS | DaaS Gateway provider callback (`DeliveryStatus.CourierLogin`) | `courier.status.changed` (status: `logged_in` / `logged_out`) | Same callback queue, switch case `CourierLogin` in `parseCallbacks` |
| DaaS | DaaS Gateway quote response (sync single-provider) | `delivery.provider.quote_received` | HTTP `200 OK` synchronous reply on `/quotes` → `GetAggregatorCarrier` |
| DaaS | DaaS Gateway quote response (multi-DaaS async) | `delivery.provider.quote_received` | RabbitMQ `…-v2-quote` → `OnMultiDaasQuote` (only when `store.multiDaasEnabled()`) |
| DaaS | DaaS Gateway "no successful quote" / `JSONResp.status = error` | `delivery.provider.quote_failed` | Sync HTTP error or async on `…-v2-quote`; `proxy_helper.ErrNoSuccessfulQuoteFound` in daas-gateway |
| DaaS | DaaS Gateway create accept (delivery created, provider assigned) | `delivery.provider.created` | Folded into the synchronous `200 OK` body of `/api/v2/deliveries`; today reflected through the first `Assigned` callback — **TBD: sync reply or async bus event in Algo 4?** |
| DaaS | DaaS Gateway create reject (`4xx` / `5xx`) | `delivery.provider.create_failed` | Sync HTTP error path → `UpdateAggError`; today no separate bus event — **TBD: sync reply or async bus event in Algo 4?** |
| DaaS | DaaS Gateway ETA response (synchronous) | `delivery.provider.eta_received` | HTTP `200 OK` reply on `/eta` (Urban Piper–specific — daas-gateway hard-routes `/eta` to the `urbanpiper` provider; only fired by Algo's `CheckForNewMarketPlaceOrders` for MP sources like Zomato/Swiggy, operationally India-only today) |
| DaaS | DaaS Gateway rider-ETA stream | `delivery.provider.eta_updated` | RabbitMQ `…-v2-eta-v2` → `OnETAmessage`; `quoteId == "rider_eta"` |
| DaaS | DaaS Gateway cancel ack (DELETE) | `delivery.provider.canceled` | Sync HTTP ack + synthetic `Cancelled` callback published by daas-gateway `sendCancellationAuditMessage` |
| DaaS | DaaS Gateway cancellation audit (audit-only) | `delivery.provider.cancellation_audited` | RabbitMQ `provider_callbacks` audit queue inside daas-gateway — audit-callbacks flow for order-level visibility, not a business signal |
| DaaS | DaaS Gateway marketplace ack | `orders.marketplace.acked` | Sync HTTP ack on `/mp/{provider}/order/status` and `/mp/{provider}/order/switch`; today no bus event |
| Proxy | Mobile `Login` `PsEvent` | (consumed locally; CGW emits `courier.status.changed: logged_in` after Algo accepts the login) | `proxy/PsEvents.go` → `ProcessLoginEvent` → store WS |
| Proxy | Mobile `Logout` `PsEvent` | `courier.status.changed` (status: `logged_out`) | Pass-through `Logout` PsEvent |
| Proxy | Mobile `Location` `PsEvent` and Dragon Track UI `POST /setCarrierLocation` | `courier.location.updated` | `proxy/PsEvents.go` `ProcessLocationEvent`; `proxy/DragonTrack.go` `locationHandler` |
| Proxy | Mouters telematics RabbitMQ feed | `courier.location.updated` (sourceSystem `mouters`) | RabbitMQ `…-v2-courier-location` → `algo/mouters.go` → `ProcessMouterEvent` |
| Proxy | Mobile `Panic` `PsEvent` | `courier.status.changed` (status: `panic` / `panic_off`) or dedicated `courier.panic.triggered` | `proxy/PsEvents.go` → `ProcessPanicEvent` |
| Proxy | Mobile `Order` ack (status: `delivered` / `undelivered` / `notHome` / `getItemsList` / `printInvoice`) | `orders.delivery.status_changed` | `proxy/PsEvents.go` → `ProcessOrderEvent` |
| Proxy | Mobile `Carrier` ack (status: `inStore` / `outOfStore` / `LoggedOut` / `borrowedTo` / `borrowedFrom` / `cancelMoveRequest` / `cancelPending`) | `courier.status.changed` | `proxy/PsEvents.go` → `ProcessCarrierEvent` |
| Proxy | Mobile `PushNotification` ack | `courier.message.acknowledged` | `proxy/PsEvents.go` → ack reply |
| Proxy | Mobile `ClearNotifications` ack | `courier.message.acknowledged` | `proxy/PsEvents.go` → `ProcessClearNotificationsEvent` |
| Proxy | Mobile `TrackOrder` reply (UI-initiated) | `courier.tracking.requested` (audit) | `proxy/PsEvents.go` → `ProcessTrackOrderEvent` |
| Proxy | Mobile `CheckList` ack | `courier.checklist.submitted` | `proxy/PsEvents.go` → `ProcessChecklistEvent` |
| Proxy | Mobile `AvailableOrders` request/response | `courier.available_orders.updated` (echo / audit) | `proxy/PsEvents.go` → `ProcessAvailableOrdersEvent` |
| Proxy | Mobile `StatsByRange` / `HistoricOrderList` | `courier.stats.queried` / `courier.orders.history_queried` (audit-only sync read-through) | `proxy/PsEvents.go` |
| Proxy | Mobile `StartUpOptions` | `courier.session.configured` (echo as ack) | `proxy/PsEvents.go` → `ProcessStartUpOptionsEvent` |
| Proxy | Device `ApproveDevice` reply | `courier.device.approval_result` | `proxy/PsEvents.go` → `ProcessReply` |

### 4.3 DaaS family — per-event detail

All `delivery.provider.*` and `orders.marketplace.*` topics. The Cloud Gateway is the **only** writer to DaaS Gateway HTTP and the **only** reader of the DaaS callback / quote / ETA queues.

**Routing-key construction** (`daas-gateway/pkg/proxy_helper.GetRoutingKey`):
- `franchisee == ""` → `{brand}-{country}-{storeNo}-v2{suffix}`
- `franchisee != ""` → `{brand}:{franchisee}-{country}-{storeNo}-v2{suffix}`

**Provider routing inside daas-gateway** (`daas-gateway/api/handler/proxy.go` `Proxy`):
- `provider := delivery.AggProvider`; if empty, `provider = delivery.ProviderID`.
- If the path ends with `/eta`, **`provider = "urbanpiper"`** (hardcoded).
- DB lookup against `agg_services.name`; reverse-proxy to `agg_services.url`.

**Supported providers** are DB rows in `agg_services` plus engine-eligible providers for multi-quote: DoorDash, UberDirect, Stuart, Wolt, Urbanpiper, Pandago, Grab, Cargo, Skip, Skedadel, Shipday, Magicpin, Mensajerosurbanos, Leta, Deliveroo. New providers are added by inserting a row, not by changing code.

#### 4.3.1 (Listen) `delivery.provider.quote_requested`

**Source service:** Quote Service / Delivery Management.
**Outbound action:** `POST /api/v2/deliveries/quotes` on DaaS Gateway. Two modes:
1. **Single-provider sync** — body has `aggProvider` set. Synchronous HTTP `200 OK` with the quote; daas-gateway reverse-proxies to that one provider.
2. **Multi-DaaS async** — body has `aggProvider == ""` and only `providerID`. daas-gateway returns `202 Accepted` with empty body, fans out in parallel to the engine-selected provider list, and publishes the chosen quote on RabbitMQ `…-v2-quote`.

**Wire shape (request):** `proxy_helper.Delivery { aggProvider, providerID, brand, storeBrand, storeId, tzdata { name, offset }, pickupReadyAt, isCash, isAlcoholic, providerCouriersCount, targets[] { customer { location { lat, lng } }, details { id } } }`. Algo today builds the richer `algo/AggregatorsAPI.go AggRequest` (adds `storeName`, `storePhone`, `address`, `pickupDeadLineAt`, `cancelForBatch`, `isNotPaid`, `carrierId`, `rating`, `reason`, `comment`); only the subset above is the daas-gateway contract.
**Wire shape (response, sync):** `JSONResp { status, data: ProviderResponse.Data = QuoteResponse, message }` where `QuoteResponse = { providerID, createdAt, expiresAt, fee, currency, dropoffETA, pickupETA, providerConf { cancellationDisabled, batchingEnabled, autoEnrouteOnAggPickup, callLimit }, validations{}, orderIDs[] }`.
**Wire shape (response, async multi-DaaS):** HTTP `202 { "status": "success", "data": "" }`; final body arrives on RabbitMQ `{routingKey}-v2-quote` carrying `JSONResp { QuoteResponse }` on success or `JSONResp { status: "error", data: []orderIDs, message }` on failure. Custom queue name via `?queue_name=` query param.

**Possible values:**
- HTTP: `200 OK` (sync), `202 Accepted` (async multi-DaaS), `4xx` / `5xx` (validation / upstream error).
- `JSONResp.status ∈ { "success", "error" }` (sync constants `StatusSuccess` / `StatusError` in `algo/AggregatorsEvents.go`).
- `providerID` value space — any `agg_services.name` row.

**How consumed today:** `algo/AggregatorsAPI.go` → `AggRequest.SendQuote()` from `GetAggregatorCarrier` and `CheckAggForOrders`. Sync replies are unmarshalled inline. Async replies are consumed in `algo/rabbitmq.go` → `OnMultiDaasQuote`, registered only when `store.multiDaasEnabled()`.
**Algo 4 behaviour:** Publish `delivery.provider.quote_received` on success or `delivery.provider.quote_failed` on `JSONResp.status == "error"` / `proxy_helper.ErrNoSuccessfulQuoteFound`.

#### 4.3.2 (Listen) `delivery.provider.create_requested`

**Source service:** Delivery Management.
**Outbound action:** `POST /api/v2/deliveries` on DaaS Gateway.
**Wire shape (request):** Same `proxy_helper.Delivery` shape as quote, with `aggProvider` set and `targets[]` populated.
**Wire shape (response):** `JSONResp { status, data: AggMessage, message }` where `AggMessage` is the full delivery snapshot (`id`, `providerId`, `createdAt`, `updatedAt`, `quoteId`, `courier {…}`, `fee`, `currency`, `targets[]`).

**Possible values:**
- HTTP `200 OK` = created.
- `JSONResp.status ∈ { "success", "error" }`.
- Provider-side errors surface in `JSONResp.message`; `algo/AggregatorsAPI.go` converts these into `UpdateAggError` and audit log lines.

**How consumed today:** `algo/AggregatorsAPI.go` → `AggRequest.SendDelivery()` from `sendDeliverySetCarrier()`. On success: `SetOrderAggCarrierId`, `setFakeAggCarrier`, then `checkAndFetchMissingEvents` is scheduled. On error: `UpdateAggError`. There is no separate `delivery.provider.created` bus event today — the Algo monolith waits for the first `Assigned` callback.
**Algo 4 behaviour:** Publish `delivery.provider.created` on `200 OK`, `delivery.provider.create_failed` on `4xx` / `5xx` or `JSONResp.status == "error"`.

#### 4.3.3 (Listen) `delivery.provider.eta_requested`

**Source service:** AI Promise Time / Delivery Management.
**Outbound action:** `POST /api/v2/deliveries/eta` on DaaS Gateway. **Always routed to Urbanpiper** by daas-gateway (`if strings.HasSuffix(req.URL.Path, "/eta") { provider = urbanpiperID }` in `daas-gateway/api/handler/proxy.go`) — no fan-out, no engine involvement, no RabbitMQ publish.
**Wire shape (request):** Same `proxy_helper.Delivery` shape. Algo today sets `externalProviderId` to the marketplace `altOrderId` for MP orders.
**Wire shape (response):** `JSONResp` passthrough from Urbanpiper.
**How consumed today:** `algo/AggregatorsAPI.go` → `AggRequest.SendEtaRequest()` from `CheckForNewMarketPlaceOrders`. Sets a fake aggregator carrier and starts `checkAndFetchMissingEvents(..., MarketplaceOrder)`.
**Algo 4 behaviour:** Publish `delivery.provider.eta_received` from the synchronous reply.

#### 4.3.4 (Listen) `delivery.provider.cancel_requested`

**Source service:** Delivery Management / Order Management.
**Outbound action:** `DELETE /api/v2/deliveries` on DaaS Gateway.
**Wire shape (request):** `proxy_helper.Delivery` (subset populated from order: `aggProvider`, `storeId`, `targets[].details.id`).
**Wire shape (response):** `JSONResp` with `data` typed as `AggDeleteData { id, canceledAt, status }` in algo's local model.
**Side-effect on success:** daas-gateway publishes a synthetic `Cancelled` callback to the default callback queue and an audit message to `provider_callbacks` (`sendCancellationAuditMessage`). The synthetic callback body is:

```
AlgoCallback {
  ProviderID: provider,
  Targets:    [{ OrderID: metadata.OrderIDs[0], Status: 5 (Cancelled) }],
  Courier:    { Status: 0 (Unassigned) }
}
```

**Possible values:**
- HTTP `400` on cancel → `algo/AggregatorsAPI.go ErrAggLateCancel`.

**How consumed today:** `algo/AggregatorsAPI.go` → `CancelDelivery`, `CancelOrdersInBatch`, `CancelLastWaitingAggOrder`. The eventual provider-side confirmation arrives through the regular callback queue as `DeliveryStatus.Cancelled`.
**Algo 4 behaviour:** Publish `delivery.provider.canceled` on the `200 OK` ack; the callback queue still drives the authoritative state via `delivery.provider.callback`.

#### 4.3.5 (Listen) `delivery.provider.rate_requested`

**Source service:** Operator UI / post-delivery feedback.
**Outbound action:** `POST /api/v2/deliveries/rate` on DaaS Gateway.
**Wire shape (request):** `proxy_helper.Delivery` with `carrierId`, `rating`, `reason`, `comment`, `targets[0].details.id == orderId`. Algo today builds it from `RateCarrierData { carrierId, orderId, provider, reason, comment, rating, storeNo }`.
**How consumed today:** `algo/FrontEndService.go` → `RateCarrier()` → `algo/AggregatorsAPI.go SendAggCarrierRating()` → `consts.CarrierRatingPath`.
**Algo 4 behaviour:** Fire-and-forget; no separate response event. Audit-only on the bus.

#### 4.3.6 (Listen) `delivery.provider.missed_events_requested`

> **Note:** Deprecated — this topic is no longer used. The flow is kept here for historical reference only.

**Source service:** Internal recovery loop.
**Outbound action:**
- Aggregator orders → `GET /api/v1/deliveries/{storeId}/{orderId}/{providerName}/{aggId}?lastStatus=&courierStatus=`
- Marketplace orders → `GET /api/v1/marketplace/{brand}/{country}/{storeNo}/{orderId}` (always Urbanpiper-backed)

**Wire shape (request):** Path parameters; query params `lastStatus` and `courierStatus` are integer enums (`DeliveryStatus` and `CourierStatus`, see §4.4.1).
**Wire shape (response):** `JSONResp { data: []AggMessage }` — list of missed callbacks.
**How consumed today:** `algo/AggregatorsAPI.go GetMissingEvents()` is called by `checkAndFetchMissingEvents` after a create or MP-ETA. Each returned `AggMessage` is fed back through `parseCallbacks("GetMissingEvents", …)`.
**Algo 4 behaviour:** Re-emit each fetched message onto the bus as `delivery.provider.callback` with `metadata.recovery: true`.

#### 4.3.7 (Listen) `orders.marketplace.status_changed`

**Source service:** Delivery Management (`stayWithStoreCarrier` decision).
**Outbound action:** `POST /mp/{provider}/order/status` on DaaS Gateway.
**Wire shape (request):** `OrderStatusUpdate { aggOrderId, brand, country, orderId, orderStatus, storeNo }`.
**Possible values for `orderStatus`:** Today the only value Algo sends is `"NewOrderMakeline"`. The marketplace backend is service-owned (`mp-{provider}` row in daas-gateway `services` table) and the body is reverse-proxied without daas-gateway-side validation.
**How consumed today:** `algo/AggregatorsAPI.go updateMarketPlaceWithDecision()`, reading `MarketPlace / retryMaxAttempts` and `MarketPlace / retryMaxSeconds`. On accept, sets `OrderFlowAlerts_StayWithStoreCarrier`.
**Algo 4 behaviour:** Publish `orders.marketplace.acked` on success.

#### 4.3.8 (Listen) `orders.marketplace.switched`

**Source service:** Delivery Management (`switchOrderToMPCarrier` decision).
**Outbound action:** `POST /mp/{provider}/order/switch` on DaaS Gateway.
**Wire shape (request):** `SwitchOrder { aggOrderId, brand, country, storeNo }`.
**How consumed today:** `algo/AggregatorsAPI.go updateMarketPlaceWithDecision()`. On accept, sets `OrderFlowAlerts_SwitchToMPCarrier`, sale type → carry-out.
**Algo 4 behaviour:** Publish `orders.marketplace.acked` on success.

---

#### 4.3.9 (Publish) `delivery.provider.callback`

**External trigger:** Provider callback published by daas-gateway. Every 3PL eventually flows through `daas-gateway/api/handler/callback.go Callback` on `POST /callbacks/{brand}/{country}/{storeNo}` or `POST /callbacks/{brand}/{franchisee}/{country}/{storeNo}`. Returns `202 Accepted` to the 3PL and republishes verbatim to RabbitMQ.
**Today's transport:** RabbitMQ queue `{brand}-{country}-{externalStoreId}-v2` (no suffix), TTL 15 min, declared with DLQ at `{queue}_DLQ`.
**Wire shape:** `AlgoCallback { providerID, targets[] { id, orderID, status }, courier { status } }`. Algo's richer view is `AggMessage { id, providerId, createdAt, updatedAt, quoteId, courier, fee, currency, targets[] }` with nested `Courier { id, aggregator, status, firstName, lastName, vehicleType, phone, location { lat, lng }, rating, loginStatus }` and `AggOrder { id, orderId, status, quotedDeliveryTime, quotedPickupTime, pickupDuration, dropoffDuration, dropoffETA, pickupETA, actualDeliveryTime, actualPickupTime, aggCustomID, deliveryValidation }`.

**Possible values — `DeliveryStatus` (`AggOrder.status`, integer on the wire):**

| Value | Label | Meaning |
| --- | --- | --- |
| 0 | Waiting | Order accepted by 3PL but not yet scheduled (some callbacks reuse this for "location-only" updates with no status change) |
| 1 | Scheduled | 3PL scheduled the courier |
| 2 | Assigned | Courier assigned (this is the first authoritative "we have a courier") |
| 3 | PickedUp | Courier picked up at the store |
| 4 | Delivered | Courier completed delivery |
| 5 | Cancelled | Order cancelled (3PL-side or via DELETE) |
| 6 | CouldNotDeliver | Order could not be delivered |
| 7 | CourierLogin | Stuart-style fleet on/off-shift signal — published as `courier.status.changed` instead of `delivery.provider.callback` |

**Possible values — `CourierStatus` (`Courier.status`, integer on the wire):**

| Value | Label |
| --- | --- |
| 0 | Unassigned |
| 1 | Accepted |
| 2 | EnrouteToPickup (Enroute to store) |
| 3 | ArrivedAtStore |
| 4 | CourierPickedUp |
| 5 | EnrouteToDropoff (Enroute to customer) |
| 6 | ArrivedToCustomer |
| 7 | DroppedOff |
| 8 | Nearby |

**Possible values — `Courier.loginStatus`:** `"online"`, `"offline"` (case-insensitive). Used only when `DeliveryStatus.CourierLogin` is set.
**Possible values — `quoteId` reserved markers:** `"rider_eta"` (ETA-update marker, see §4.3.12), `"mp"` (marketplace-flow marker).

**How consumed today:** `algo/rabbitmq.go` → `OnAggMessage` → `algo/AggregatorsEvents.go parseCallbacks()`. The `switch` handles `CourierLogin`, `Assigned`, `PickedUp`, `Delivered`, `Cancelled` plus nested courier sub-statuses. `Scheduled` / `Waiting` skip the switch but still update location and audit via `CarrierLocation()`.
**Algo 4 behaviour:** Publish `delivery.provider.callback` partition-keyed by `storeId`. Idempotency tuple: `(storeId, aggOrderId, deliveryStatus, courierStatus, updatedAt)`. The `CourierLogin` path is split off into `courier.status.changed` per `event-taxonomy.md`. `Cancelled` callbacks emitted as a side-effect of DELETE also drive `delivery.provider.canceled` for the synchronous-cancel correlation.

#### 4.3.10 (Publish) `delivery.provider.quote_received` / `delivery.provider.quote_failed`

**External trigger:** DaaS Gateway response on `POST /api/v2/deliveries/quotes`.

- **Sync single-provider:** HTTP `200 OK` with `JSONResp { status: "success", data: QuoteResponse }`.
- **Multi-DaaS async:** RabbitMQ `{brand}-{country}-{externalStoreId}-v2-quote` (or the queue name supplied via `?queue_name=`). Body is `JSONResp` wrapping `QuoteResponse` on success or `[]string` order IDs on error.

**Wire shape:** `QuoteResponse { providerID, createdAt, expiresAt, fee, currency, dropoffETA, pickupETA, providerConf { cancellationDisabled, batchingEnabled, autoEnrouteOnAggPickup, callLimit }, validations{}, orderIDs[] }`.
**Possible values:**
- `JSONResp.status ∈ { "success", "error" }`.
- HTTP `200` (sync), `202` (async).
- `providerConf.callLimit = -1` means unlimited.

**How consumed today:** Sync replies → `algo/AggregatorsAPI.go GetAggregatorCarrier`. Async replies → `algo/rabbitmq.go OnMultiDaasQuote` (only when `store.multiDaasEnabled()`). On `OnMultiDaasQuote`: updates `aggProvider`, pickup ETA, provider config cache, signals `QuoteConsumedFromQueue`.
**Algo 4 behaviour:** Publish `delivery.provider.quote_received` on success keyed by `(storeId, orderIDs[0])`. On error or `proxy_helper.ErrNoSuccessfulQuoteFound`, publish `delivery.provider.quote_failed` carrying `reason` so Delivery Management can fall back via `stayWithStoreCarrier`.

#### 4.3.11 (Publish) `delivery.provider.created` / `delivery.provider.create_failed`

**External trigger:** DaaS Gateway response on `POST /api/v2/deliveries`.
**Wire shape:** Same `JSONResp { data: AggMessage }` as the create reply (see §4.3.2).
**Possible values:** HTTP `200` = created; `4xx` / `5xx` or `JSONResp.status == "error"` = failed.
**How consumed today:** No distinct bus event — algo `sendDeliverySetCarrier` writes the result to local DB and waits for the first `delivery.provider.callback` with `DeliveryStatus.Assigned`.
**Algo 4 behaviour:** Publish a focused success / failure event so Order Management and Delivery Management can react before the first callback arrives. **TBD: stay synchronous on the HTTP reply (Delivery Management waits for `200 OK` / error inline) or async via these bus events?** Affects how Delivery Management is structured — if async, the original requester must correlate via `correlationId` and accept that the create result arrives out-of-band.

#### 4.3.12 (Publish) `delivery.provider.eta_received` / `delivery.provider.eta_updated`

**External trigger:**
- `eta_received`: synchronous response on `POST /api/v2/deliveries/eta` (Urbanpiper-backed).
- `eta_updated`: provider-pushed rider ETA stream — `AggMessage` with `quoteId == "rider_eta"` published on RabbitMQ `{brand}-{country}-{externalStoreId}-v2-eta-v2`.

**Wire shape:** `AggMessage` (see §4.3.9), single order in `targets[0]`.
**How consumed today:** `algo/rabbitmq.go OnETAmessage` (registered when MP sources are configured) → `UpdateOrderQuotePickUpTime`, `CreateAggUpdate`. Sync ETA reply consumed inline in `algo/AggregatorsAPI.go`.
**Algo 4 behaviour:** Publish a focused ETA-update event keyed by `(storeId, aggOrderId)`.

#### 4.3.13 (Publish) `delivery.provider.canceled` / `delivery.provider.cancellation_audited`

**External trigger:**
- `canceled`: synchronous `200 OK` on `DELETE /api/v2/deliveries`, plus the synthetic `Cancelled` callback published by daas-gateway `sendCancellationAuditMessage` to the default callback queue.
- `cancellation_audited`: audit-only side-channel — daas-gateway publishes `ProviderCallbackAuditMessage { Payload, CancelStatus, Brand, Country, StoreNo }` to the fixed `provider_callbacks` queue (TTL configurable via `cfg.Audit.CallbacksExpiration`, default 15 min). Specific to the audit-callbacks flow so support tooling has order-level visibility into every cancel; not a business signal for the cluster.

**Wire shape (canceled):** Same `AggMessage` / `AlgoCallback` flow as the regular callback, with `DeliveryStatus.Cancelled (5)` and `CourierStatus.Unassigned (0)`.
**Wire shape (cancellation_audited):** `ProviderCallbackAuditMessage { Payload []byte (raw callback body), CancelStatus *ResponseStatus (SUCCESS|FAIL on DELETE synthetic cancel), Brand, Country, StoreNo }`.
**How consumed today:** Sync HTTP success path inside `algo/AggregatorsAPI.go CancelDelivery`. Synthetic callback flows through `OnAggMessage`. The audit queue is consumed only by daas-gateway tooling, not by algo.
**Algo 4 behaviour:** `delivery.provider.canceled` is the authoritative cancel-acknowledged event for Order Management. `delivery.provider.cancellation_audited` is audit-only (no internal domain consumer).

#### 4.3.14 (Publish) `orders.marketplace.acked`

**External trigger:** DaaS Gateway sync `200 OK` on `POST /mp/{provider}/order/status` or `POST /mp/{provider}/order/switch`. Daas-gateway is a pure reverse-proxy here — body shapes are owned by the `mp-{provider}` backend service.
**How consumed today:** No bus event — algo treats the HTTP success as terminal and writes `OrderFlowAlerts_*`.
**Algo 4 behaviour:** Publish so Delivery Management's decision propagates and `OrderFlowAlerts` updates are bus-driven.

---

#### Out-of-band DaaS audit queues

These are daas-gateway-owned audit feeds. They are NOT subscribed to by algo today and are NOT in scope for the Cloud Gateway in Algo 4 (they belong to daas-gateway operations):

- `provider_callbacks` — audit copy of every callback + DELETE synthetic cancel (see §4.3.13).
- `store_orders` — `StoreOrderAuditMessage { Payload, Status: SUCCESS|FAIL, ErrorFromProvider }` published on every successful POST when `cfg.Audit.OrdersEnabled`.
- `provider_quotes` — `ProviderQuotesAuditMessage { Metadata: DeliveryMetaData, Quotes[]: QuoteResponse, Errors{}, DueDate }` from the multi-DaaS quote fan-out (`cfg.Audit.QuotesEnabled`).
- `engine_decisions` — `EngineDecisionsAuditMessage { Metadata, Decisions[]: { Provider, Chosen, Reason, Value, Limit } }`.

### 4.4 Proxy family — per-event detail

All `courier.*`, `orders.delivery.*` (mobile-driven half), and a few `store.*` topics. The Cloud Gateway is the **only** writer to the Proxy ingress channel and the **only** reader of the Proxy egress channel. Proxy itself is a router, not a schema authority — most `PsEvent.value` payloads are opaque to it.

**Wire envelope:** `helper.PsEvent { eventName, eventId, proxyEventId, value, reply, storeNo, alphaNumericStoreNo, carrierId, language, secondsPassed, orders[], market, eventInLocalTime }`.

**Wire transport today:**
- Algo↔Proxy: WebSocket `wss://{Proxy.Address}/ws` or `…/stores/ws`. JSON array of `PsEvent` per text frame.
- Proxy↔Mobile: WebSocket `/ws` per device.
- Proxy↔TrackerV2: HTTP `POST {trackerV2Url}/{eventName}` (see §4.4.6).
- Dragon Track UI ↔ Proxy: HTTP `POST /setCarrierLocation` under `trackerListener` for courier-location pings. (The remaining Dragon Track UI endpoints — `/deliveryRoute`, `/messageDriver`, `/drivethrough`, `/track/*` — have moved to the Tracker Service; see §4.5.)

**Direction rule** (`proxy/PsEvents.go ProcessDefaultEvent`): if incoming `event.CarrierId != ""` → Algo→Mobile; else set `event.CarrierId = c.CarrierId` and Mobile→Algo.

**Auth** (today):
- Algo→Proxy WS: bearer from `algo/AuthServiceClient.go` `POST {auth}/stores/auth` with `AlgoCreds { market, storeNo, pwd, macAddress }`. Headers `proxyServiceAuthToken`, `X-StoreNo`, `X-Market`.
- Mobile→Proxy WS: `AuthorizeConnection` → `authorizeDeviceConnection`; per-store password via `helper.PwForStore(storeNo)`.

**Whitelist controls** (`proxy/main.go setEventForwardingWhitelists`, in-memory only):
- `AllowedStores`: empty or `*` = all; else comma-separated store numbers.
- `AllowedCarriers`: empty or `*` = all; else lower-case carrier IDs.

The whitelist filters tracker-v2 forwarding only today — the device WebSocket path is unfiltered.

**Login `SuppotredEvents` array** (advertised by algo on connect, `algo/ProxyClient.go LoginToProxy`):

```
Login, AppData, Geofence, Location, Order, Carrier, ClearData, SurveyData,
PushNotify, ClearNotifications, TrackOrder, CheckList, StoreStatus,
BidRequest, Panic, AvailableOrders
```

Plus runtime additions: `MobileParameters`, `Relogin`, `Skipped`, `WakeDevice`, `StartUpOptions`, `ApproveDevice`, `SharedCarriers`, `KdsLogin`, `BidStatus`, `CancelBid`, `KeepAlive`, `Register`, `ListClients`, `GetUpdates`, `Logout`, `PushNotification`, `StatsByRange`, `HistoricOrderList`, `driveThrough`, `DeviceLog`.

#### 4.4.1 (Listen) `orders.delivery.status_changed` (assigned / canceled)

**Source service:** Order Management.
**Outbound action:** Publish to Proxy ingress as `PsEvent { eventName: Order, value: ProxyOrder, carrierId }`. Proxy decodes the `CarrierId` and forwards to that mobile device.
**Wire shape (`ProxyOrder`):** `orderId`, `carrierId`, `orderStatus`, `status`, `timeWaiting`, `customerName`, `customerPhone`, `address`, `lat`, `lng`, `nav1Address`, `nav1Icon`, `nav2Address`, `nav2Icon`, `comments`, `orderInRoute`, `secondsSinceOrdered`, `secondsToPromisedTime`, `items[]`, `paymentMethod`, `paymentType`, `orderTotal`, `dailyNo`, `allowSetPosition`, `setPosition`, `SecondsSinceLastLocation`, `accuracy`, `deliveryNotification`, `exitGeofence`, `enterGeofence`, `tmpExitGeofence`, `deliveryValidation`, `forgettableItemsCount`, `batchNumber`.

**Possible values for `status` (algo→mobile, sourced from `orders.delivery.status_changed`):**
- `assigned` (Algo decision)
- `canceled` (system / operator)

The remaining values (`arrived_to_store`, `enroute`, `delivered`, `unable_to_deliver`) come from the device, see §4.4.10.

**Possible values for `orderStatus` (algo internal integer set, in PsEvent body):**

| Int | Label | String alias |
| --- | --- | --- |
| -1 | waiting | `waiting` |
| 0 | NewOrderMakeline | `onMakeLine` |
| 1 | onShelf | `onShelf` |
| 2 | cooking | `cooking` |
| 3 | packing | `packing` |
| 4 | ready | `ready` |
| 5 | enRoute | `enRoute` |
| 6 | delivered | `delivered` |
| 7 | unDelivered | `unDelivered` |
| 8 / 98 / 99 | canceled variants | `canceled`, `pendingCancellation`, `betweenCourses` |
| 100 | prioritize | — |

**How consumed today:** `algo/ProxyClient.go sendEventsToProxy` from the `CarrierActivities` table (`PsEventActionType_Order`). Proxy `ProcessDefaultEvent` → `ForwardEventToDevice`.

#### 4.4.2 (Listen) `orders.delivery.bid_offered`

**Source service:** Order Management / Pool dispatch.
**Outbound action:** Publish to Proxy ingress as `PsEvent { eventName: BidRequest, value: defs.Bid, carrierId }`.
**Wire shape (`defs.Bid`):** `storeNo`, `storeUuid`, `chainId`, `legs[] { id, lng, lat, type, address }`, `sourceTrace`, `priority`, `routeSeconds`, `readyForPickupSeconds`, `uuid`.
**Possible values for `priority`:** `"normal"`, `"high"`.
**How consumed today:** `algo/pool.go sendBidRequestToProxy`.

#### 4.4.3 (Listen) `orders.delivery.bid_canceled`

**Source service:** Order Management.
**Outbound action:** Publish to Proxy ingress as `PsEvent { eventName: CancelBid, value: defs.StatusBid }`.
**Wire shape:** `defs.StatusBid { bidUUID }`.
**How consumed today:** `algo/pool.go sendCancelBidRequest`.

#### 4.4.4 (Listen) `courier.session.configured`

**Source service:** Order Management (in reply to a mobile `Login` it accepted; also at gateway boot per store).
**Outbound action:** Publish to Proxy ingress as one of:
- `PsEvent { eventName: Login, value: helper.Login }` — store-level WS login (gateway → Proxy).
- `PsEvent { eventName: AppData, value: opaque JSON }` — per-courier app data.
- `PsEvent { eventName: StartUpOptions, value: helper.Login }` — pre-login push to a device.
- `PsEvent { eventName: MobileParameters, value: MobileParameters }` — post-login mobile config push.

**Wire shape (`helper.Login`):** `username`, `password`, `storeNo`, `suppotredEvents[]`, `version`, `reconnect`, `lastLoginId`, `assets`, `vehicleId`, `noRefresh`, `external`, `storeName`, `location: LocationData`, `newApp`.
**Wire shape (`MobileParameters`):** `printInvoice`, `surveyQuestions[]`, `useAnalytics`, `locationTickSeconds`, `lockScreen`, `lockCPU`, `accuracyThreashhold`, `allowOffline`, `eventSounds`, `bgLoc`, `notHome`, `navigateBack`, `uiOptions`.

**How consumed today:** `algo/ProxyClient.go ConnectToProxyServer` → `LoginToProxy` (for the Algo↔Proxy login). Per-courier session config flows via `InsertCarrierActivity*`. Proxy itself produces `MobileParameters` post-login via `SendMobileParametersToMobile` (`proxy/MobileServices.go`).

#### 4.4.5 (Listen) `courier.session.cleared`

**Source service:** Order Management (shift reset / clear-data command).
**Outbound action:** Publish to Proxy ingress as `PsEvent { eventName: ClearData, value: opaque JSON }`.
**How consumed today:** `algo/ProxyClient.go InsertCarrierActivityClearData`, `SendClearDataEventToAllCarriers`.

#### 4.4.6 (Listen) `courier.available_orders.updated`

**Source service:** Order Management (when a courier's offerable-orders list changes).
**Outbound action:** Publish to Proxy ingress as `PsEvent { eventName: AvailableOrders, value: { availableOrdersExist: bool } }`.
**How consumed today:** Reply to mobile poll path in `algo/ProxyClient.go`.

#### 4.4.7 (Listen) `courier.message.posted`

**Source service:** Notification Dispatcher / Operator.
**Outbound action:** Publish to Proxy ingress as one of:
- `PsEvent { eventName: PushNotify, value: PushNotify }` — alert-style notification: `message`, `sound`, `reloginAfter`, `popup`, `nag`, `loginAs`. Deliver as WebSocket frame.
- `PsEvent { eventName: PushNotification, value: PushNotification }` — system push: `notificationText { body, title }`, `notificationData { reloginAfter, loginAs }`. Deliver via Proxy `processPushNotifcationEvent` → HTTP `POST {PushNotifcationURL}` with device UUID.
- `PsEvent { eventName: TrackOrder, value: helper.TrackOrder }` — tracker-driven message: `type`, `orderId`, `lastLocationId`, `fullDetails`, `lastStatus`, `message`, `requestIp`, `source`, `tester`, `campaign`, `altorderid`.

**How consumed today:** `algo/ProxyClient.go InsertCarrierActivityPushAlertNotification`, `InsertCarrierActivityPushNotification`. (Tracker UI's `POST /messageDriver` used to also land in this topic; that path has moved to the Tracker Service — see §4.5.)

#### 4.4.8 (Listen) `courier.message.expired`

**Source service:** Cloud Gateway itself (TTL housekeeping).
**Outbound action:** Publish to Proxy ingress as `PsEvent { eventName: ClearNotifications }` so the device clears any UI overlay tied to the expired message.
**How consumed today:** No equivalent today — events that don't reach a device simply linger in `CarrierActivities`. New in Algo 4.

#### 4.4.9 (Listen) `courier.status.changed` (Algo-side)

**Source service:** Order Management / Couriers Service when Algo wants to push a status to the device (e.g. `borrowedTo`, `borrowedFrom`, `cancelMoveRequest`, `cancelPending`, performance JSON).
**Outbound action:** Publish to Proxy ingress as `PsEvent { eventName: Carrier, value: CarrierEventStatus }`.
**Wire shape (`structures.CarrierEventStatus`):** `status`, `deliver[]`, `lat`, `lng`, `estimateReturnMinutes`, `clearNotifications`, `storeNo`, `borrowedCarrierId`, `loginAsCarrierId`, `storeName`, `info`.
**Possible values for `status` (Algo→mobile):** `borrowedTo`, `borrowedFrom`, `cancelMoveRequest`, `cancelPending`, plus performance-only payloads via `BuildCarrierStatusWithPerformanceJson`.
**How consumed today:** `algo/ProxyClient.go BuildCarrierStatusWithPerformanceJson`.

#### 4.4.10 (Listen) `store.shared_carriers.updated` / `store.status.changed` / `courier.device.approval_requested`

**Source services:** Algo `SharedDrivers.go`, `FrontEndService.go ApproveDeviceByEventId`.
**Outbound action:** Publish to Proxy ingress as `PsEvent { eventName: SharedCarriers | StoreStatus | ApproveDevice }`.
**How consumed today:** Various Algo paths; Proxy handlers `ProcessSharedCarriersEvent`, `ProcessStoreStatusEvent`, `ProcessApproveDeviceEvent`.

#### 4.4.11 Tracker v2 forward (Listen, transport-level)

**Source service:** Anything the gateway forwards as `Carrier`, `Order`, or `Location` PsEvent — by Proxy config (`AppConf.ForwardEvents.Registrations`):

```
"Carrier":  ["forwardEventsToTrackerV2"],
"Location": ["forwardEventsToTrackerV2"],
"Order":    ["forwardEventsToTrackerV2"]
```

**Outbound action:** Publish on the Proxy ingress channel with a `target: trackerV2` discriminator so Proxy forwards via its tracker-v2 path:
- `Carrier` events → `POST {TrackerV2Service.Url}/carrier`. Skip if `value` contains `"loggedout"` (case-insensitive).
- `Order` events → `POST .../order`. Skip if no active mobile WS for `(storeId, carrierId)`. Maintains `carrierOrders` cmap; adds on non-`remove`/non-`delivered` `OrderCarrierStatus`, removes on `remove` or `delivered`.
- `Location` events → `POST .../location`. Requires `StoreNo > 0`. Sets `event.Orders = getCarrierOrders(carrierId)` for enrichment.

**Headers:** `Content-Type: application/json`, `Authorization: Bearer {AppConf.TrackerV2Service.CurrentToken}` (proxy-issued JWT, issuer `proxy`, audience `dragontail.com`).

**`OrderCarrierStatus` enum** (`proxy/types/types.go`): `getItemsList`, `remove`, `dispatched`, `delivered`.
**`OrderStatus` enum used in tracker v2 parse** (`proxy/types/types.go`): `1` Created, `2` InOven/Pack, `3` Ready, `4` EnRoute, `98` Cancelled, `99` Failed.

**How consumed today:** `proxy/eventForwarders.go ForwardToTrackerServiceV2`.

---

#### 4.4.12 (Publish) `courier.location.updated`

**External trigger:** Three sources fold into this one canonical topic. The `sourceSystem` envelope field discriminates.

| sourceSystem | Wire path today | Wire shape |
| --- | --- | --- |
| `proxy_mobile` | `PsEvent { eventName: Location, value: LocationData }` over WebSocket | `LocationData { lat, lng, gpsStatus, accurate, accuracy, altitude, altitudeAccuracy, heading, speed, watchid, localtime, demoMode, idle, secondsPassed }` |
| `dragon_track_ui` | `POST /setCarrierLocation` to Proxy | `CarrierLocation { uid, storeNo, carrierId, eventId, lat, lng, source, accuracy, secondsPassed }` |
| `mouters` (telematics) | RabbitMQ `{brand}-{country}-{externalStoreId}-v2-courier-location` | `MouterVehicle { id, lat, lng }` joined to a carrier via vehicle mapping |

**Possible values:**
- `LocationData.gpsStatus`: `0` OK, `1` Permission denied, `2` Permission unavailable, `3` Timeout.
- `LocationData.action` (when set): `1` TagIn, `2` TagOut, `3` RecommendedDelivery.

**How consumed today:**
- `proxy_mobile` → `proxy/PsEvents.go ProcessLocationEvent` (proxy-side filter: updates `LastAcurateLocationEvent*` only when `accuracy ≤ 100` and `gpsStatus == 0`) → forwarded to algo.
- `dragon_track_ui` → `proxy/DragonTrack.go locationHandler` → forwarded to algo as a `Location` PsEvent.
- `mouters` → `algo/mouters.go ProcessMouterEvent` → maps vehicle → carrier → `updateCarrierStatusLocationAndOrders(..., MoutersEventSource)`.

**Algo 4 behaviour:** Single canonical topic partitioned by `(storeId, carrierId)` with a high partition count. Enrich location events with the carrier's current order list (mirrors `proxy/eventForwarders.go getCarrierOrders` for tracker v2 today).

#### 4.4.13 (Publish) `courier.status.changed` (mobile-driven)

**External trigger:** Several `PsEvent.eventName` values from the device.

**Possible values for the canonical topic's `status` field:**

| Status | Triggered by | Source PsEvent shape |
| --- | --- | --- |
| `logged_in` | Mobile `Login` accepted by Algo OR DaaS callback `DeliveryStatus.CourierLogin (7)` with `Courier.loginStatus == "online"` | `helper.Login` from device, or `AggMessage.courier.loginStatus` from DaaS |
| `logged_out` | Mobile `Logout` PsEvent OR `Carrier { status: LoggedOut }` ack OR DaaS callback `CourierLogin` with `loginStatus == "offline"` | `Logout` PsEvent (opaque), `CarrierEventStatus { status: "LoggedOut" }`, `AggMessage.courier.loginStatus` |
| `in_store` | Mobile `Carrier` ack `{ status: inStore }` | `CarrierEventStatus { status: "inStore" }` |
| `out_of_store` | Mobile `Carrier` ack `{ status: outOfStore }` | `CarrierEventStatus { status: "outOfStore" }` |
| `panic` / `panic_off` | Mobile `Panic` PsEvent with `PanicEventStatus { status, panicId }` | `structures.PanicEventStatus { status: "off" \| other, panicId }` |
| `borrowed_to` / `borrowed_from` / `cancel_move_request` / `cancel_pending` | Mobile `Carrier` ack | `CarrierEventStatus.status ∈ { borrowedTo, borrowedFrom, cancelMoveRequest, cancelPending }` |

**How consumed today:** `proxy/PsEvents.go` handlers `ProcessLoginEvent`, `ProcessCarrierEvent`, `ProcessPanicEvent` forward the PsEvent to algo; algo updates carrier state in `Carriers`-related tables.
**Algo 4 behaviour:** Single canonical topic partitioned by `courierId`. CGW decodes the `PsEvent.eventName` and maps to the `status` enum above.

#### 4.4.14 (Publish) `orders.delivery.status_changed` (mobile-driven)

**External trigger:** Mobile `Order` ack PsEvent.
**Wire shape:** `ProxyOrder` (see §4.4.1) plus a status string in `value.status`.
**Possible values for mobile-set `status`** (from `proxy/PsEvents.go` `ProcessOrderEvent` and observed values):

| Mobile status | Canonical bus status (per `event-taxonomy.md`) |
| --- | --- |
| `getItemsList` | (sub-flow, no canonical state change) |
| `notHome` | `unable_to_deliver` (with `reason: customer_not_home`) |
| `undelivered` | `unable_to_deliver` |
| `delivered` | `delivered` |
| `deliveredOffline` | `delivered` (offline-buffered) |
| `deliveryOffline` | `delivered` (offline-buffered) |
| `printInvoice` | (sub-flow, no canonical state change) |

The full canonical enum on the bus is `{ assigned, arrived_to_store, enroute, delivered, unable_to_deliver, canceled }` per `event-taxonomy.md` §6.

**How consumed today:** `proxy/PsEvents.go ProcessOrderEvent` → forwarded to algo → updates order in DB.
**Algo 4 behaviour:** Same canonical topic as §4.4.1; CGW publishes with `sourceSystem: proxy_mobile` and the canonical mapping above.

#### 4.4.15 (Publish) `courier.message.acknowledged`

**External trigger:** Mobile reply to a `PushNotification` push, or a `ClearNotifications` ack.
**How consumed today:** `proxy/PsEvents.go ProcessClearNotificationsEvent` and the per-event ack reply path.
**Algo 4 behaviour:** Used by Notification Dispatcher to confirm delivery.

#### 4.4.16 (Publish) `courier.tracking.requested`

**External trigger:** Mobile `TrackOrder` PsEvent reply (typically when a device asks the tracker UI for live drive info).
**Wire shape:** `helper.TrackOrder { type, orderId, lastLocationId, fullDetails, lastStatus, message, requestIp, source, tester, campaign, altorderid }`.
**How consumed today:** `proxy/PsEvents.go ProcessTrackOrderEvent`. Some flows fill an HTTP-waiter channel for the Dragon Track UI (synchronous reply to the browser).

#### 4.4.17 (Publish) `courier.checklist.submitted`

**External trigger:** Mobile `CheckList` PsEvent.
**How consumed today:** `proxy/PsEvents.go ProcessChecklistEvent`.

#### 4.4.18 (Publish) `courier.available_orders.updated` (mobile pull)

**External trigger:** Mobile `AvailableOrders` PsEvent (the device asking for the current list).
**How consumed today:** `proxy/PsEvents.go ProcessAvailableOrdersEvent`. The corresponding push from Algo is the same canonical topic with `sourceSystem: algo` (§4.4.6).

#### 4.4.19 (Publish) Dragon Track UI HTTP → bus

The Dragon Track tracker UI talks to Proxy over HTTP (`trackerListener` / `proxy/DragonTrack.go`). One route still translates into a `PsEvent` that this gateway forwards onto the bus:

| HTTP route | Internal bus topic | Wire shape | Consumer today |
| --- | --- | --- | --- |
| `POST /setCarrierLocation` | `courier.location.updated` (sourceSystem `dragon_track_ui`) | `CarrierLocation` (§4.4.12) | algo via store WS |

The remaining Dragon Track UI routes (`/deliveryRoute`, `/messageDriver`, `/drivethrough`, plus the UI assets `/track/*` and `/`) have moved to the **Tracker Service** and are no longer in CGW's scope — see §4.5.

#### 4.4.20 (Publish) `courier.device.approval_result` and other audit-only mobile events

`StatsByRange` / `HistoricOrderList` / `Register` / `KeepAlive` / `GetUpdates` / `KdsLogin` are sync read-throughs handled inside Proxy; the gateway audits them but does not necessarily fan them out to domain services. Each gets a focused audit topic only if a real consumer exists.

### 4.5 Moved to Tracker Service

The Dragon Track tracker UI HTTP surface used to be served by Proxy (`proxy/DragonTrack.go`) and reach the cluster through this gateway. The routes below have moved to the **Tracker Service**, which now owns both the tracker UI and the synchronous responses to the browser. They are **out of scope** for the Cloud Gateway.

| Former route | Former internal bus topic | Wire shape | Now owned by |
| --- | --- | --- | --- |
| `GET /deliveryRoute` (header `token: @customerRoute`) | `orders.delivery.route_planned` | Query → `helper.TrackOrder { type: "deliveryRoute", … }`; synchronous reply was `helper.DriveInfo` (status, `OrderDetails`, `Coordinates[]`, `DriveThroughSettings`, …) | Tracker Service |
| `POST /messageDriver` (header `token: @customerRoute`) | `courier.message.posted` (sourceSystem `dragon_track_ui`) | Form `UID`, `message` → `helper.TrackOrder { type: "messageDriver", … }`; synchronous reply to the browser | Tracker Service |
| `POST /drivethrough` | `orders.delivery.drivethrough_observed` | `helper.DriveThroughData { isArrived, uid, orderID, fields, receivedTS }`; PsEvent `eventName: driveThrough` | Tracker Service |
| `GET /track/*`, `GET /` | (UI assets, not bus) | Static / `home.html` / `homeS.html` | Tracker Service |

Implications:
- The bus topics `orders.delivery.route_planned` and `orders.delivery.drivethrough_observed` are no longer published by CGW.
- `courier.message.posted` is still published by CGW but only from the algo-side push paths (§4.4.7) — the `dragon_track_ui` source path is gone.
- Anything in Algo 4 that wants to react to tracker-UI actions now subscribes to topics published by the Tracker Service (out of scope for this document).

---

### 4.6 Not in scope (deliberately excluded)

These cross the algo↔cloud boundary at the wire level today but belong to a different gateway or are operational concerns:

- **POS / KDS / Menu / Labor / VIP / cabinet / vision / GPS push paths** — owned by the **Inbound API Gateway**. See `docs/inbound-api-gateway/README.md`.
- **Cloud-orders RabbitMQ queue** (`{routingKey}-cloud-orders` → `ProccessCloudOrdersEvent` in `algo/CloudOrderEvent.go`) — *order intake*, owned by the Inbound API Gateway as `orders.intake.received`. The transport is RabbitMQ today, but it is not a cloud-gateway concern.
- **Remote-access SSH tunnel** (`{routingKey}-remote-access` → `ProccessRemoteAccessEvent` in `algo/CloudRemoteAccess.go`) — operational support channel, stays separate.
- **Cabinet hardware** (`{routingKey}-cabinet` → `OnPucMessage`) — third-party hardware push, owned by the Inbound API Gateway.
- **Fleet vehicles update** (`{routingKey}-vehicles-updates` → `ProcessVehiclesUpdatesEvent`) — vehicle registry sync, owned by the Inbound API Gateway as `courier.vehicle.assigned` (data source: brand POS / fleet ops).
- **Internal compute** (`OptimizeOrdersFromJson`, `AvgTimePerKilometerJson`, `VehicleLocationAlertJson`, `GetCarrierRoutesDistance*`, `EventStartRaise`, `CallTraffilog`, `SetLocation`) — internal between Algo 3 components, not a cloud egress.
- **Maps proxy** (`/maps/*` in `algo/maps.go` and `algo/endpoints.go`) — server-rendered map tiles; stays internal.
- **Direct algo→cloud HTTP calls bypassing the shared client** (`algo/CabinetsAPI.go`, `algo/Analysis.go`, `algo/SMSMechanism.go`, `algo/IgnitionEvent.go` Traffilog pull) — listed in `algo/communication-component-hld.md` "Outbound Scope". Default in Algo 4: every cloud-bound call routes through this gateway, but the per-integration cutover is decided individually.
- **Auth surface** (`POST /login`, `Handle /pinCode`, `/api/auth/token`, `/api/auth/refresh`, `/api/auth/authorize`, `POST /api/getAuthorized`) — owned by Auth Service.
- **Per-store WebSocket / SSE fan-out inside the store** (`/ws`, `/wsHome`, `/api/events`) — owned by the makeline / FE projection.
- **`/handler/*` legacy makeline FE → backend** (`algo/nethandler.go`, `NetHandlerStyle`) — internal traffic in Algo 3, becomes in-cluster RPC in Algo 4.
- **Daas-gateway-internal audit queues** (`provider_callbacks`, `store_orders`, `provider_quotes`, `engine_decisions`) — daas-gateway operational telemetry, not a cluster-facing event family.

## 5. Internal Bus — Expected Consumers Per Topic

§4 defined the wire shapes, possible values, and today's consumption paths. This section names the **expected consumers inside the Algo cluster** for each topic the gateway publishes onto the internal bus, so service owners can see at a glance which topics they need to subscribe to.

> **"Expected consumers (inside Algo)"** lists only services inside the Algo cluster (the Kitchen and Fleet box: Order Management, Delivery Management, AI Promise Time, Quote Service, Customer Service, Employee Service, Couriers (Vehicle) Service, Notification Dispatcher, Order Sequencer, Ticket Service, Recommendation Service, Gateway UI). External destinations (DaaS Gateway, Proxy, mobile devices, tracker UI, Admin Panel UI, audit log) live in §7 "API Exposed to Other Services" and are not repeated here. Where no internal domain consumer exists today, the entry says "no internal domain consumer (audit-only)" — please don't fill in a guess.

### 5.1 DaaS family consumers

| Topic | Expected consumers (inside Algo) | Notes |
| --- | --- | --- |
| `delivery.provider.quote_received` | Delivery Management | Single-provider sync OR multi-DaaS async folded into one topic |
| `delivery.provider.quote_failed` | Delivery Management (`stayWithStoreCarrier` decision) | Carries `reason` |
| `delivery.provider.created` | Order Management, Delivery Management | New in Algo 4 — today the first `Assigned` callback is the create signal |
| `delivery.provider.create_failed` | Order Management, Delivery Management | New in Algo 4 |
| `delivery.provider.eta_received` | AI Promise Time, Delivery Management | Synchronous reply from `/eta` (Urban Piper–specific — MP-only flow; daas-gateway hard-routes `/eta` to `urbanpiper`) |
| `delivery.provider.eta_updated` | AI Promise Time | Provider-pushed `rider_eta` stream |
| `delivery.provider.canceled` | Order Management, Delivery Management | Authoritative cancel-acknowledged event |
| `delivery.provider.cancellation_audited` | No internal domain consumer (audit-only) | Daas-gateway-side audit echo |
| `delivery.provider.callback` | Order Management (authoritative), AI Promise Time, Delivery Management, Customer Service | Partition key `storeId`. Idempotent on `(storeId, aggOrderId, deliveryStatus, courierStatus, updatedAt)` |
| `orders.marketplace.acked` | Delivery Management (so the decision propagates and `OrderFlowAlerts` updates) | Today no bus event; algo treats sync HTTP success as terminal |

### 5.2 Proxy family consumers

| Topic | Expected consumers (inside Algo) | Notes |
| --- | --- | --- |
| `courier.location.updated` | Couriers (Vehicle) Service, AI Promise Time, Delivery Management | Three sources — `proxy_mobile`, `dragon_track_ui`, `mouters` — disambiguated by `sourceSystem`. High-volume; partition by `(storeId, carrierId)` with high partition count |
| `courier.status.changed` | Couriers (Vehicle) Service, Delivery Management, Order Management, Employee Service | Statuses include `logged_in`, `logged_out`, `in_store`, `out_of_store`, `panic`, `borrowed_to`, `borrowed_from`, `cancel_move_request`, `cancel_pending` |
| `orders.delivery.status_changed` | Order Management (`Order` / `Carrier` / `TrackOrder` acks), Customer Service, Delivery Management, AI Promise Time | Mobile-sourced statuses are `arrived_to_store`, `enroute`, `delivered`, `unable_to_deliver`. Algo-sourced are `assigned`, `canceled` (see §4.4.1 / §4.4.14) |
| `courier.message.posted` | Notification Dispatcher | Algo-side push only (tracker-UI `messageDriver` source moved to Tracker Service — see §4.5) |
| `courier.message.acknowledged` | Notification Dispatcher | Mobile reply to push or `ClearNotifications` ack |
| `courier.message.expired` | Notification Dispatcher, Delivery Management | New in Algo 4 — TTL-driven |
| `courier.session.configured` (echo) | No internal domain consumer (audit-only) | Bus-side echo when CGW pushes a fresh `MobileParameters` / `StartUpOptions` to a device |
| `courier.session.cleared` (echo) | No internal domain consumer (audit-only) | |
| `courier.available_orders.updated` (mobile pull) | No internal domain consumer (audit-only) | The Algo-side push is the authoritative variant |
| `courier.tracking.requested` | No internal domain consumer (audit-only) | UI-initiated `TrackOrder` |
| `courier.checklist.submitted` | No internal domain consumer (audit-only) | |
| `courier.device.approval_result` | No internal domain consumer (audit-only) | |

## 6. Data Dependencies (External Reads)

### Store / brand / country routing context
**Owning service:** Store Service.
**When fetched:** On every internal bus event that needs an external call (to build `routingKey`, partition key, DaaS Gateway URL parameters).
**Why needed:** Today `algo/Stores.go` `getQueueRoutingKey()` / `getMPQueueRoutingKey()` and `daas-gateway/pkg/proxy_helper.GetRoutingKey` both build `{brand}-{country}-{externalStoreId}` from the store record. The gateway needs the same lookup to construct the right DaaS callback path (`/callbacks/{brand}/{country}/{storeNo}` or `/callbacks/{brand}/{franchisee}/{country}/{storeNo}`).
**How fetched:** Cached lookup, refreshed every N minutes; cache miss falls back to the latest known value and emits an audit warning.

### DaaS provider catalog
**Owning service:** DaaS Gateway (`daas-gateway/entity` `AggService { id, name, url, cancellationDisabled, batchingEnabled, autoEnrouteOnAggPickup, callLimit }`, exposed through `/aggregator-services` CRUD).
**When fetched:** On startup and on a configurable refresh cadence; also after any DaaS provider config write coming through the Admin Panel UI HTTPS REST surface (§7).
**Why needed:** Pick the right provider URL, batching policy, cancellation policy, call limit, and authentication for every `POST /api/v2/deliveries{,/quotes,/eta,/rate}` call. Today `algo/AggregatorsAPI.go` `GetAggregator` queries the local SQLite mirror; in Algo 4 the source of truth is DaaS Gateway.

### Multi-DaaS / aggregator settings per store
**Owning service:** Store Service.
**When fetched:** On Delivery Management decisions.
**Why needed:** Today `algo/daas_bridge.go` `loadDaasProviders` joins `GetAggregatorsSettings` + `GetMultiDaasActivity` + `optionlist.FloatValue("Aggregators","CancellationDisabled")` to compute `DaasProvidersResponse { active, providers[] }`. The gateway needs the same to know whether to call quotes synchronously or to fan out over the multi-DaaS queue.

### Per-provider runtime credentials
**Owning service:** Secrets store (TBD; today configured in DaaS Gateway env / `cfg.Auth` per `daas-gateway/api/handler/router.go` `middleware.BasicAuth`).
**Why needed:** Sign every outbound call to DaaS Gateway.

### Proxy bearer token + market
**Owning service:** Auth Service (today: the algo→proxy token is fetched by `algo/ProxyClient.go` `getTokenToConnectProxyMs(market, proxyAddress, protocol)`; cached in `proxyServiceAuthToken`).
**When fetched:** Once per gateway boot, refreshed on `401`. Also on every Proxy EventBus batch the gateway must set `X-StoreNo` / `X-Market` headers to satisfy `proxy/MobileServices.go` and `proxy/eventForwarders.go`.
**Why needed:** The Proxy enforces auth on its ingress.

### Order ↔ delivery correlation map
**Owning service:** Order Management.
**When fetched:** On every inbound provider callback to translate `aggOrderId` ↔ `(storeId, orderId)`. Today this is the `CountAggregatorOrder` script in `algo/AggregatorsEvents.go` `parseCallbacks`.
**How fetched:** Sync API call with low timeout; cache the mapping for the order's expected lifetime.
**Notes:** If lookup fails the gateway publishes to a `quarantine` topic and audits — it does not drop the callback.

### Carrier → active orders map
**Owning service:** Self-owned (today: `proxy/eventForwarders.go` `carrierOrders` `cmap`). The gateway needs to enrich outbound `location` tracker-v2 events with the carrier's current order list.
**Notes:** Updated on every `Order { status: dispatched | delivered | remove }` event flowing through the gateway.

### Idempotency-key store
**Owning service:** Self-owned (Redis / Postgres TBD — same tech choice as the Inbound API Gateway).
**Why needed:** Provider callbacks are retried at the DaaS Gateway layer; mobile devices retry their PsEvents.

### Audit / raw payload store
**Owning service:** Self-owned (S3 / blob TBD).
**Why needed:** Every outbound call body, every provider callback body, every Admin Panel write must be persisted by `correlationId` for support / replay.

### Whitelist configuration
**Owning service:** Self-owned (today `proxy/main.go` `AppConf.ForwardEvents.StoresWhiteList` / `CarriersWhiteList`, persisted in PostgreSQL via `proxy/dataAccess/queries`).
**When fetched:** Hot path on every outbound mobile event.
**Why needed:** Drop events for stores / carriers not in the active rollout cohort.

## 7. API Exposed to Other Services

### Internal canonical-event subscription surface
**Purpose:** Subscribe to every internal EventBus topic listed in §4 "Internal bus → outbound …" and translate to the right external call.
**Known consumers:** N/A — this is the gateway's input. Listed for symmetry with the Inbound API Gateway's "internal canonical-event publishing surface".
**Inputs:** Canonical Algo events with the `CLAUDE.md` envelope.
**Returns:** Async; no synchronous reply on the bus (replies come back as separate inbound canonical events — e.g. `delivery.provider.quote_received` after `delivery.provider.quote_requested`).
**Notes:** Partition keys per topic are chosen so per-`storeId` ordering and per-`(storeId, carrierId)` ordering are preserved end-to-end.

### Outbound HTTP surface to DaaS Gateway
**Purpose:** Be wire-compatible with the DaaS Gateway routes the cluster uses today so the DaaS Gateway does not need to change for the Algo 4 cutover.
**Known consumers:** DaaS Gateway is the consumer of the outbound calls; ultimately each call lands on a 3PL provider behind the DaaS Gateway.
**Inputs:** `AggRequest`, `OrderStatusUpdate`, `SwitchOrder`, rating payload, ETA payload — exact shapes as currently produced by `algo/AggregatorsAPI.go`.
**Returns:** `AggResponse { status, data, message }`, `AggQuoteData`, `AggDeleteData` — exact shapes as consumed today.
**Notes:** Auth scheme matches what DaaS Gateway expects (per-provider keys + the basic-auth `cfg.Auth` for the `/aggregator-services` admin routes).

### Outbound EventBus surface to Proxy
**Purpose:** Single ingress EventBus topic to the Proxy service carrying every device-bound `PsEvent` and tracker-v2 forward.
**Known consumers:** Proxy service consumers, which then deliver via WebSocket (`proxy/conn.go`) or via tracker-v2 HTTP (`proxy/eventForwarders.go` `ForwardToTrackerServiceV2`).
**Inputs:** `PsEvent` body wrapped in the canonical envelope; `target` discriminator (`device`, `tracker-v2`).
**Returns:** Async; per-event delivery acks land back on the corresponding entity topic (`courier.message.acknowledged`, `orders.delivery.status_changed`, etc. — the same topic the original device-bound event was carved out of).
**Notes:** Partition key `(storeId, carrierId)`. Whitelist enforcement happens at the gateway boundary, not in the Proxy.

### Admin Panel UI HTTPS REST surface
**Purpose:** Replace the cluster of admin routes mounted today inside Algo (`algo/endpoints.go` `registerBORoutes`, `processCenterRoutes`, parts of `frontendRoutes`) with a single cloud-hosted endpoint the Admin Panel UI calls.
**Known consumers:** Cloud-hosted Admin Panel UI.
**Inputs:** The exact route surface from `algo/endpoints.go` `registerBORoutes`:
  - `GET /dashboard/settings`
  - `POST /dashboard`, `POST /dashboard/*`
  - `POST /getReportFromDashboard`
  - `POST /getPolygons`, `POST /backoffice/getPolygons`
  - `Handle /bo`, `Handle /bo/*`, `Handle /backofficeHeader`, `Handle /backofficeHeader/*`, `Handle /backofficeNav`, `Handle /backofficeNav/*`
  - `POST /register`, `Handle /changePassword`, `Handle /forgotPassword`
  - `GET /api/stores/self`, `GET /getSingleStoreNo`, `GET /api/stores/{storeID}/daas`, `GET /api/stores/{storeID}/qrcode`
**Returns:** JSON / HTML matching today's shapes.
**Notes:** Auth via JWT (today `v2auth.JWTAuthMiddleware` in `algo/endpoints.go` `frontendRoutes`). The gateway terminates the JWT and forwards the call to the appropriate domain service.

### Inbound DaaS callback receiver
**Purpose:** Receive provider callbacks from DaaS Gateway (today over RabbitMQ; Algo 4 over DaaS-published EventBus or a dedicated callback EventBus topic — open question §9).
**Known consumers:** N/A — gateway-internal.
**Inputs:** `AlgoCallback { providerID, targets[], courier{} }`, `AggMessage`, audit messages.
**Returns:** Canonical events on the internal bus.

### Inbound Proxy event receiver
**Purpose:** Receive every device-originated event from Proxy, plus the `POST /setCarrierLocation` Dragon Track UI courier-location pings.
**Known consumers:** N/A — gateway-internal.
**Inputs:** `PsEvent` envelope, `CarrierLocation` envelope.
**Returns:** Canonical events on the internal bus.
**Notes:** Tracker-UI HTTP routes other than `/setCarrierLocation` (`/deliveryRoute`, `/messageDriver`, `/drivethrough`) are owned by the Tracker Service — see §4.5.

### Outbound audit query API
**Purpose:** Look up the lifecycle of any outbound message by `eventId`, `correlationId`, `(storeId, orderId)`, `(storeId, carrierId)`, or `aggOrderId`.
**Known consumers:** Audit Service tooling, on-call engineers, support tooling.
**Returns:** Lifecycle row plus the `rawRequestRef` to the captured body.

## 8. Operational Assumptions

- Every internal service that today produces a `helper.PsEvent` and shoves it onto `con.sendChan` in `algo/ProxyClient.go` will instead publish a canonical bus event that this gateway consumes. The gateway is the **only** writer to the Proxy ingress EventBus topic.
- Every internal service that today calls a DaaS Gateway HTTP endpoint directly (the four call sites in `algo/AggregatorsAPI.go`, the marketplace decisions in `SwitchOrderToMPCarrier` / `stayWithStoreCarrier`) will instead publish a canonical bus event. The gateway is the **only** writer to DaaS Gateway HTTP.
- DaaS Gateway is willing to publish provider callbacks to a EventBus topic the gateway consumes (or, transitionally, the gateway continues to consume the existing RabbitMQ aggregator / quote / ETA queues — see §9).
- Proxy is willing to consume from a EventBus topic for ingress instead of holding a WebSocket from Algo, and to publish to a EventBus topic for egress. If Proxy stays WebSocket-only in v1, this gateway runs a single WebSocket client on behalf of the whole cluster and bridges to EventBus internally — i.e. the Algo→Proxy WebSocket moves *into* the gateway, no other service holds one.
- Mobile devices do not need to know that anything changed. The same `PsEvent` shape over the same WebSocket Proxy continues to be served by the same Proxy service.
- DaaS Gateway's `/aggregator-services` admin surface stays where it is (`daas-gateway/api/handler/router.go` `MakeHandlers` with `middleware.BasicAuth`). The Algo 4 Admin Panel UI either calls DaaS Gateway directly for that or through this gateway as a transparent reverse-proxy — TBD §9.
- The Admin Panel UI is cloud-hosted and reaches the gateway over HTTPS with a JWT. The current in-store-bundled admin pages (`algo/FrontEnd/` SPA mounts under `/dashboard`, `/backoffice`, etc.) keep working for backwards compatibility but new sessions land on the cloud Admin Panel.
- Per-provider rate limits (`AvailableCalls` / `CallLimit`), retry budgets (`MarketPlace / retryMaxAttempts`), and per-event TTLs are configured before the gateway is allowed to accept traffic.
- The DaaS Gateway, the Proxy, and the Admin Panel UI are reachable from the gateway over the public internet with TLS — the gateway is the egress chokepoint and the only place that holds those credentials.
- `CourierLogin` is Stuart-specific. Other 3PLs may add fleet on/off-shift signals later; the gateway must not assume the set of `DeliveryStatus` values is fixed forever.
- `RemoteAccess` (`{routingKey}-remote-access` queue, `ProccessRemoteAccessEvent`) is not migrated into this gateway — it stays as a separate operational support channel.
- The gateway does not own delivery semantics. If Order Management decides a callback is invalid (wrong store, unknown order, stale `updatedAt`), the gateway has already published the canonical event and Order Management is the authoritative dropper.
- `kfcmenuservice`-style flows do **not** flow through this gateway in either direction; they are owned by the Inbound API Gateway.

## 9. Open Questions

- **DaaS Gateway provider-callback transport.** Today provider callbacks land on RabbitMQ queues consumed by Algo directly (`OnAggMessage`, `OnETAmessage`, `OnMultiDaasQuote`, `OnPucMessage`). For Algo 4 we have three options: (a) the gateway runs a RabbitMQ consumer on the same queues and republishes to EventBus, (b) DaaS Gateway is modified to publish to a callback EventBus topic the gateway consumes, (c) DaaS Gateway calls a callback HTTP endpoint on the gateway (mirroring `daas-gateway/api/handler/callback.go` Callback). Recommendation: option (b), with option (a) as the transition path.
- **Proxy ingress/egress transport.** Same question for the Algo↔Proxy WebSocket. Three options: (a) the gateway is the only WebSocket client to Proxy and bridges to EventBus internally (no Proxy change), (b) Proxy gains EventBus ingress/egress, (c) Proxy itself becomes the cloud gateway role, absorbing this service. Recommendation: option (a) for v1, with option (b) as the target.
- **Admin Panel UI reverse-proxying.** Should the gateway transparently reverse-proxy admin routes to underlying domain services (single hop), or should each domain service be EventBus-driven and the gateway compose answers (event-sourced)? Recommendation: reverse-proxy for v1, event-sourced for write paths in v2.
- **Tracker-service v2 callback path.** Today the tracker-v2 flow is outbound only (`forwardeventstotrackerv2`). If the tracker service starts sending callbacks (delivery viewed, customer messaged), they will arrive over a new HTTP route — confirm that route is added to this gateway, not to Proxy.
- **Tracker UI launched from the Algo dispatch view.** Today the operator dispatch view in Algo's frontend can open a customer-facing tracker UI for a selected order (the Dragon Track surface). The bulk of that surface (`/deliveryRoute`, `/messageDriver`, `/drivethrough`, `/track/*`) has moved to the Tracker Service — see §4.5. The one remaining route in CGW's scope is `POST /setCarrierLocation` (§4.4.12 / §4.4.19). Confirm: (a) which service owns the link / URL generation from the dispatch view in Algo 4 — Order Management, Delivery Management, the Admin Panel UI, or the Tracker Service? (b) does the operator-side dispatch view embed the cloud Tracker Service UI directly, or proxy through CGW?
- **Mobile `BidRequest` / `CancelBid` reply events.** `proxy/PsEvents.go` exposes `ProcessBidRequestedReply` (mobile `BidStatus` reply to a `BidRequest`) and `ProcessCancelBid` (mobile reply to a `CancelBid`). It is unclear whether these are still produced by current courier apps and, if so, who consumes them on the Algo side. Removed from the inbound table (§4.2) and §4.4 detail sections pending clarification. Confirm: (a) do courier devices in production still emit `BidStatus` / `CancelBid` replies, or has the bid-accept/decline flow been replaced (e.g. by `Order` ack)? (b) if they are still emitted, which Algo service consumes the reply (Delivery Management / `algo/pool.go`)? (c) if they are still emitted, should the canonical bus topic be `orders.delivery.bid_offered` (terminal-state on accept) or a dedicated `orders.delivery.bid_replied`? Until answered, the gateway should drop these PsEvents on the floor (audit-log only).
- **Offline / disconnected-store behavior.** Today Algo runs in-store and continues to operate when cloud connectivity is lost — orders are processed, couriers are dispatched, and mobile devices keep working through the in-store Proxy. The gateway, by contrast, is the egress chokepoint to **cloud-side** DaaS Gateway, cloud Proxy, and Admin Panel UI. Confirm: (a) when cloud connectivity drops, does the gateway buffer outbound events on the bus (and for how long) or fail-fast and rely on retry/DLQ? (b) which event families are safe to drop on the floor when offline vs. must be queued until the link is restored (e.g. `delivery.provider.create_requested` must queue, `courier.location.updated` can be dropped beyond TTL)? (c) does the gateway expose an `online/offline` health signal that in-store services can react to (e.g. to fall back to in-house fleet dispatch when DaaS Gateway is unreachable)? (d) on reconnect, is there an explicit "drain" / replay path, and how does it interact with idempotency keys and per-event TTLs?
- **`X-Market` header propagation.** Today `algo/ProxyClient.go` `ConnectToProxyMicroService` and `proxy/eventForwarders.go` use a per-event `Market` (falling back to proxy-level `AppConf.Market`). Confirm the canonical envelope carries `market` so the gateway can fill the header on outbound calls.
- **Multi-DaaS quote semantics.** `algo/rabbitmq.go` only registers `OnMultiDaasQuote` on the quote queue when `store.multiDaasEnabled()`. Confirm the gateway uses the same condition — single-DaaS replies synchronously on the HTTP response; multi-DaaS reads the quote off the response EventBus topic.
- **Idempotency on provider callbacks.** Today there is no explicit idempotency key on `AggMessage`; dedup relies on Order Management's `CountAggregatorOrder` check plus the (rare) RabbitMQ at-least-once semantics. With EventBus at-least-once, we need an explicit dedup tuple. Default proposal: `(storeId, aggOrderId, deliveryStatus, courierStatus, updatedAt)`.
- **Retry / DLQ policy on outbound HTTP.** `updateMarketPlaceWithDecision` has its own retry loop (`MarketPlace / retryMaxAttempts`, `MarketPlace / retryMaxSeconds`). Confirm the gateway-wide policy supersedes per-call-site retry loops.
- **Whitelist scope.** Today `setEventForwardingWhitelists` filters tracker-v2 forwarding only. Should it also filter the device-bound WebSocket path? Today no. Confirm we keep that asymmetry or unify.
- **`/aggregator-services` admin routes.** The DaaS Gateway exposes its own admin CRUD (`r.Post("/", CreateAggService)` etc. in `daas-gateway/api/handler/router.go`). Either expose it through this gateway or have the Admin Panel call it directly. Decide before MVP.
- **Remote-access tunnel.** Confirm `ProccessRemoteAccessEvent` and `CloudRemoteAccessQueue` stay outside this gateway. Probably yes; it is an operational support concern, not a business event.
- **Per-store identity to DaaS Gateway.** Today every store posts with the same shared credentials in `consts`. Algo 4 should attach a store-scoped identity (mirroring §6 of the Inbound API Gateway). Tracked against the Auth Service decision in `CLAUDE.md`.
- **Mobile event TTLs.** Default per-`eventName` TTLs (Location: seconds; Order: minutes; ClearData: minutes; PushNotify: hours) — needs product input.
- **Topic-naming standard.** The canonical topic naming convention is defined in `docs/event-taxonomy.md`.
- **Cabinet `OnPucMessage`** is correctly *not* in scope here, but it shares the same RabbitMQ broker as the aggregator callbacks. Confirm the inbound-side cabinet handler in the Inbound API Gateway also runs an AMQP consumer, and that this gateway's AMQP consumer does not accidentally pick up cabinet messages.
