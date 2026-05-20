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

The Outbound (Cloud) Gateway is the single egress point through which the Algo 4 cluster talks to the **cloud-side world**: the Dragontail DaaS Gateway (which fronts every third-party delivery provider — DoorDash, UberDirect, Stuart, Wolt, Urbanpiper, etc.), the per-region Proxy service (which fans out over WebSocket to courier mobile devices and the Dragon Track customer-facing tracker UI), and the cloud Admin Panel UI (backoffice, dashboards, polygon editor, store self-service). It subscribes to internal Kafka business events, translates them into the right external transport (HTTP to DaaS Gateway, Kafka to Proxy, HTTPS REST to the Admin Panel), and — in the reverse direction — consumes DaaS Gateway provider callbacks, Proxy mobile/tracker events, and Admin Panel write requests, and republishes them onto the internal Kafka EventBus as canonical Algo events. It replaces today's `algo/ProxyClient.go` WebSocket-to-Proxy, `algo/AggregatorsAPI.go` direct HTTP-to-DaaS, `algo/rabbitmq.go` aggregator / quote / ETA / cabinet / cloud-orders consumers, and `algo/BackOffice.go` admin-facing handlers so that nothing inside the cluster talks to the outside cloud directly.

## 2. Scope

### In Scope

- Subscribe to internal "delivery intent" Kafka events (quote requested, delivery created / updated / canceled, ETA requested, rating posted, discovery requested) and translate them into the DaaS Gateway HTTP contract used today (`POST /api/v2/deliveries/quotes`, `POST /api/v2/deliveries`, `POST /api/v2/deliveries/eta`, `POST /api/v2/deliveries/rate`, `DELETE /api/v2/deliveries`, `POST /api/v1/discovery`, plus `POST /mp/{provider}/order/status` and `POST /mp/{provider}/order/switch` for marketplace decisions). Paths/structs are the ones defined in `algo/pkg/consts/constValues.go` (`EtaPath`, `QuotePath`, `DeliveryPath`, `CarrierRatingPath`, `DiscoveryPath`, `SwitchOrderToMPPath`, `UpdateMPOrderStatusPath`) and in `daas-gateway/api/handler/router.go`.
- Subscribe to internal "device-bound" Kafka events (Order, Carrier, Location, AppData, ClearData, ClearNotifications, PushNotify, TrackOrder, Geofence, Checklist, StoreStatus, BidRequest, PanicEvent, AvailableOrders, MobileParameters, …) and republish them onto the Proxy ingress topic, replacing the Algo→Proxy WebSocket `sendEventsToProxy` loop in `algo/ProxyClient.go` (the events listed are the `SuppotredEvents` array sent on `LoginToProxy`).
- Consume DaaS Gateway provider callbacks on Kafka and republish them as canonical Algo events on the internal bus. Today these arrive over RabbitMQ as the aggregator callback queue (`{routingKey}` → `OnAggMessage` in `algo/AggregatorsEvents.go`), the ETA queue (`{routingKey}-eta-v2` → `OnETAmessage`), and the quote queue (`{routingKey}-quote` → `OnMultiDaasQuote`). The wire shape matches `daas-gateway/api/handler/callback.go` (`AlgoCallback { providerID, targets[], courier{} }`) and the `AggMessage` shape Algo unmarshals today.
- Consume Proxy mobile/tracker events on Kafka and republish them onto the internal bus: courier location pings (`POST /setCarrierLocation` → `locationHandler` in `proxy/DragonTrack.go`), per-event replies and acks from devices (`PsEvent.Reply`), and the tracker-service v2 events (`forwardeventstotrackerv2` — `carrier`, `order`, `location` in `proxy/eventForwarders.go`). Today these flow over the bidirectional WebSocket between Algo's `ProxyClient.go` and the Proxy `conn.go`.
- Expose the cloud Admin Panel UI's HTTPS REST surface. Today the same endpoints live inside Algo (`algo/BackOffice.go`, `algo/endpoints.go` `registerBORoutes` — `/dashboard/settings`, `POST /dashboard`, `POST /dashboard/*`, `POST /getPolygons`, `POST /backoffice/getPolygons`, `/bo*`, `/backofficeHeader*`, `/backofficeNav*`, `/api/stores/self`, `/getSingleStoreNo`, `/register`, `/changePassword`, `/forgotPassword`, plus the `/api/v2/stores/{id}/daas` provider-status read from `algo/daas_bridge.go`). In Algo 4 the Admin Panel becomes a cloud-hosted UI talking to this gateway, which fronts the underlying store / dashboard / polygon services.
- Honor the existing DaaS Gateway response contract on synchronous calls (the JSON envelopes in `daas-gateway/api/response/json.go` and the per-provider `AggResponse{ status, data, message }` shape in `algo/AggregatorsAPI.go`).
- Track every outbound message through a `REQUESTED → DISPATCHED → ACKED | FAILED | DEAD_LETTERED` lifecycle, with retries and DLQ for both the HTTP-to-DaaS leg and the Kafka-to-Proxy leg (mirroring the `RECEIVED → … → PUBLISHED | DEAD_LETTERED` lifecycle of the Inbound API Gateway).
- Enforce per-provider rate limits (`AvailableCalls` / `CallLimit` from `entity.AggregatorSettings`, today read in `algo/AggregatorsAPI.go` and `algo/daas_bridge.go`) and per-device rate limits / whitelists (`StoresWhiteList`, `CarriersWhiteList` from `proxy/main.go` `setEventForwardingWhitelists`).
- Hold the cloud-side credentials: DaaS Gateway basic-auth / bearer / per-provider keys, Proxy bearer (`proxyServiceAuthToken`, `X-StoreNo`, `X-Market` headers from `algo/ProxyClient.go` `ConnectToProxyMicroService`), Admin Panel UI JWT.

### Out of Scope

- Every push from outside Algo into Algo — POS, vendor KDS, brand Menu Management, brand Labor Management, brand VIP, cabinet hardware, vision cameras, in-store GPS, cloud order publishers — that surface is owned by the **Inbound API Gateway** (`docs/inbound-api-gateway.md`).
- The DaaS Gateway's own provider plumbing — actually talking to DoorDash, UberDirect, Stuart, Wolt, Urbanpiper, etc. — is owned by `daas-gateway` and is opaque to us; we only see the gateway's normalized HTTP surface and its normalized Kafka callbacks.
- The Proxy service's own WebSocket lifecycle, device authentication, device approval / registration, MAC/UID handling, SSL termination on the tracker port, ACME, and the per-region routing in `proxy/main.go` / `proxy/AuthServer.go` / `proxy/conn.go` / `proxy/hub.go` are owned by the Proxy service. Algo 4 only emits / consumes Proxy-bound Kafka events; it does not talk to phones.
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

- When a new order is accepted internally, a "device-bound" event (`Order` PsEvent with `status` and `items[]`) is published onto the internal bus and this gateway forwards it via Kafka to the Proxy service, which delivers it over WebSocket to the assigned courier's phone. This is the algo-4 replacement for `algo/ProxyClient.go`'s `sendEventsToProxy` + `proxy/conn.go` `ForwardEventToDevice`.
- When the dispatch decision is "use a 3PL carrier" the gateway picks up a quote / create-delivery intent off the bus and calls the DaaS Gateway (`POST /api/v2/deliveries/quotes` → `POST /api/v2/deliveries`), with the body shape currently built by `AggregatorsAPI.go` `AggRequest` (storeId, brand, country, location, targets[], tzdata, pickupReadyAt, isCash, isAlcoholic, providerCouriersCount, …).

### Order Modification / Cancellation

- Internal cancel / re-route decisions become outbound calls: `DELETE /api/v2/deliveries` on the DaaS Gateway (the `proxy_helper.Delivery` cancellation path in `daas-gateway/api/handler/proxy.go`), and an `Order { status: Cancelled }` PsEvent forwarded through the Proxy Kafka leg so the courier's phone updates.
- Marketplace re-routing decisions (today `SwitchOrderToMPCarrier` / `stayWithStoreCarrier` in `algo/AggregatorsAPI.go`) become outbound `POST /mp/{provider}/order/switch` / `POST /mp/{provider}/order/status` calls (the `MarketplaceProxy` handler in `daas-gateway/api/handler/marketplaceProxy.go`).
- DaaS provider-driven cancellations (driver couldn't deliver, customer canceled at aggregator) arrive as provider callbacks on the inbound leg and are republished as `algo.delivery.provider.callback.v1` on the internal bus — Order Management decides what to do with them.

### Kitchen Preparation & Bumping

- The gateway is not involved in kitchen lifecycle as a destination. Kitchen "show / prep / cook / bump" events that need to be visible on a courier's phone (status overlays, pickup-ready signals) are forwarded as `Order` PsEvents through the Proxy Kafka leg.
- Holding-cabinet, makeline-camera, table-cleanliness, and POS-side notification routes are all **inbound** and owned by the Inbound API Gateway.

### Delivery / Handoff

This is the gateway's busiest flow. Two halves:

- **Outbound (Algo → cloud):**
  - `algo.delivery.quote.requested.v1` → `POST /api/v2/deliveries/quotes` on DaaS Gateway (today `AggregatorsAPI.go` → `consts.QuotePath`).
  - `algo.delivery.created.v1` → `POST /api/v2/deliveries` (today `AggregatorsAPI.go` → `consts.DeliveryPath`).
  - `algo.delivery.eta.requested.v1` → `POST /api/v2/deliveries/eta` (today `consts.EtaPath`).
  - `algo.delivery.canceled.v1` → `DELETE /api/v2/deliveries`.
  - `algo.delivery.rated.v1` → `POST /api/v2/deliveries/rate` (today `consts.CarrierRatingPath`).
  - `algo.delivery.discovery.requested.v1` → `POST /api/v1/discovery` (today `sendAggServiceDiscovery` in `algo/AggregatorsAPI.go`).
  - `algo.marketplace.order.switched.v1` → `POST /mp/{provider}/order/switch`; `algo.marketplace.order.status_changed.v1` → `POST /mp/{provider}/order/status`.
  - `algo.mobile.event.dispatched.v1` (Order, Carrier, Location, ClearData, PushNotify, TrackOrder, …) → Proxy ingress Kafka topic, replacing `ProxyClient.go`'s outbound WebSocket frames.
- **Inbound (cloud → Algo):**
  - Provider callbacks for each delivery (`Assigned`, `Accepted`, `EnrouteToPickup`, `ArrivedAtStore`, `PickedUp`, `Nearby`, `ArrivedToCustomer`, `Delivered`, `Cancelled`, `CourierLogin`) — wire shape `AlgoCallback` from `daas-gateway/api/handler/callback.go` and `AggMessage` from `algo/AggregatorsEvents.go`.
  - Quote responses (today `AggQuoteData` returned on `consts.QuotePath` and aggregated by `proxy_helper.GetQuoteResponse` in `daas-gateway/api/handler/proxy.go` for the multi-DaaS scenario).
  - ETA updates (today `OnETAmessage` on `{routingKey}-eta-v2`).
  - Multi-DaaS quote results (today `OnMultiDaasQuote` on `{routingKey}-quote`).
  - Mobile-side carrier waypoints (`Location` PsEvent from `proxy/DragonTrack.go` `locationHandler`).
  - Mobile-side carrier login / logout, "in store / out of store" geofence transitions, "at customer address" geofence transitions, panic events, available-orders pulls, push-notification acks — every `PsEvent` whose value direction is device → backend (`ProxyClient.go` `readPump` → `helper.PsEvent` with non-empty `Value`).
  - Tracker-service v2 ingestion (`forwardeventstotrackerv2` in `proxy/eventForwarders.go` for `carrier`, `order`, `location` events that the Dragon Track UI consumes).

### Day Open / Day Close

- No dedicated day-open / day-close call on the cloud side. At store boot, the gateway re-establishes its DaaS Gateway HTTP session, opens its Proxy ingress / egress Kafka consumers, and runs `algo.delivery.discovery.requested.v1` to refresh the per-provider configuration (the `sendAggServiceDiscovery` loop in `algo/AggregatorsAPI.go`).
- Mobile devices treat their own reconnect as the day-open signal; the gateway just forwards whatever events flow.
- Admin Panel UI sessions are long-lived; nothing special at day boundaries.

### Exception Flows

- **DaaS Gateway returns 5xx / times out** on a quote or create: retry with the same idempotency key, then DLQ. Folded into the audit stream so dispatch can fall back to a different provider or to the in-store fleet.
- **DaaS Gateway provider callback never arrives** for an order we created: `GetMissingEventsPath` / `GetMissingEventsPathForMP` (the `GetMissingEvents` handler in `daas-gateway/api/handler/proxy.go` and `algo/AggregatorsAPI.go`) is the retry pull; preserve it in Algo 4 as a scheduled internal job that the gateway runs against DaaS Gateway.
- **Provider callback arrives for an unknown delivery** (race vs. `CountAggregatorOrder` in `parseCallbacks`): publish to a `quarantine` topic and audit, do not drop.
- **Proxy is unreachable**: queue device-bound events on the Proxy ingress Kafka topic; mobile delivery will catch up when Proxy reconnects.
- **Mobile device offline** (no socket in Proxy's hub): per-event TTL on the Proxy ingress topic; events with carrier-side delivery expectations expire and emit `algo.mobile.delivery.expired.v1`.
- **Whitelist drops** (event filtered by `StoresWhiteList` / `CarriersWhiteList` per `proxy/eventForwarders.go`): record in audit, do not fail the source bus event.
- **Admin Panel auth failure**: standard `401` / `403`, no canonical event.
- **`POST /api/v2/deliveries/quotes` returns "no successful quote"** (today `proxy_helper.ErrNoSuccessfulQuoteFound` in `daas-gateway/api/handler/proxy.go`): publish `algo.delivery.quote.failed.v1` with the reason; dispatch consumes that to decide store-carrier fallback (`stayWithStoreCarrier`).

## 4. Events Consumed (Triggers)

<!--
This gateway has two kinds of triggers:

  A) Internal Kafka bus events that the gateway must translate into an
     external call (HTTP to DaaS Gateway, Kafka to Proxy, REST to Admin
     Panel UI).
  B) External pushes that the gateway must translate into a canonical Algo
     event on the internal bus (DaaS Gateway provider callbacks, Proxy
     mobile/tracker events, Admin Panel write requests).

The canonical event name in each H4 heading is a proposed topic on the
internal bus — the topic naming convention is defined in
`docs/event-taxonomy.md`. The "Trigger today" field is the wire-level shape
from the algo / daas-gateway / proxy codebases.
-->

### Internal bus → outbound HTTP to DaaS Gateway

#### `algo.delivery.discovery.requested.v1`

**Source service:** Quote / Dispatch service (internal).
**Trigger today:** `sendAggServiceDiscovery` in `algo/AggregatorsAPI.go` POSTs `AggDiscoveryRequest { storeID, country, brandID, providerID }` to `consts.DiscoveryPath` = `/api/v1/discovery` on the DaaS Gateway (`daas-gateway/api/handler/router.go` → `r.Post("/*", Proxy)` under `/api/v1`).
**Why we consume it:** Discover the active per-provider configuration (cancellation, batching, call limits, auto-enroute, alcohol policy) for a store.
**Action on receipt:** Build the discovery payload from the bus event, call DaaS Gateway, fold the `AggResponse { status, data, message }` reply into `algo.delivery.discovery.received.v1`. Cache per store for the duration in `Aggregators / DiscoveryRetrySeconds`.
**Notes:** Retried indefinitely today via `time.AfterFunc`; keep the cadence in Algo 4 but back off after N consecutive failures.

#### `algo.delivery.quote.requested.v1`

**Source service:** Quote / Dispatch service.
**Trigger today:** Multiple callers in `algo/AggregatorsAPI.go` build an `AggRequest` and POST to `consts.QuotePath` = `/api/v2/deliveries/quotes` (`daas-gateway/api/handler/router.go` → `r.Post("/quotes", HandleQuote)`).
**Payload shape:** `AggRequest { aggProvider, externalProviderId, brand, storeBrand, storeId, storeName, storePhone, location, address, tzdata, pickupReadyAt, pickupDeadLineAt, targets[]{ customer, details }, cancelForBatch, isCash, isAlcoholic, providerCouriersCount{}, isNotPaid, carrierId, rating, reason, comment }`.
**Action on receipt:** POST to DaaS Gateway. For multi-DaaS (`store.multiDaasEnabled()`), DaaS Gateway answers asynchronously through the `{routingKey}-quote` RabbitMQ queue today — in Algo 4 that becomes a Kafka response that the gateway re-emits as `algo.delivery.quote.received.v1`. For single-provider, the response body is folded into the bus event reply directly.
**Notes:** Partition key `(storeId, providerId)`. Idempotent on the gateway-assigned `correlationId`; the same `quoteId` from DaaS Gateway short-circuits duplicate downstream fan-out.

#### `algo.delivery.created.v1`

**Source service:** Dispatch service.
**Trigger today:** `algo/AggregatorsAPI.go` POST to `consts.DeliveryPath` = `/api/v2/deliveries` (`daas-gateway/api/handler/router.go` → `r.Post("/", Proxy)` under `/v2/deliveries`).
**Payload shape:** Same `AggRequest` shape, with `targets[]` populated and the chosen `aggProvider`.
**Action on receipt:** POST to DaaS Gateway; record the returned `id` (DaaS-side delivery id) and link it to `(storeId, orderId)` so the provider callback can be correlated back. Publish `algo.delivery.created.acked.v1` on success or `algo.delivery.created.failed.v1` on `4xx` / `5xx`.
**Notes:** Idempotent on `(storeId, orderId, providerId)`. The DaaS Gateway's `proxy_helper.ConvertDeliveryToMetadata` (`daas-gateway/api/handler/proxy.go`) extracts the same key.

#### `algo.delivery.eta.requested.v1`

**Source service:** Dispatch service / AI Promise Time.
**Trigger today:** POST to `consts.EtaPath` = `/api/v2/deliveries/eta` (handled by `r.Post("/eta", Proxy)` under `/v2/deliveries`, which always routes to `urbanpiperID` per `daas-gateway/api/handler/proxy.go`).
**Action on receipt:** POST to DaaS Gateway; re-emit response as `algo.delivery.eta.received.v1` on the bus.

#### `algo.delivery.canceled.v1`

**Source service:** Dispatch / Order Management.
**Trigger today:** `DELETE` on `consts.DeliveryPath` (`r.Delete("/", Proxy)` under `/v2/deliveries`). On success DaaS Gateway emits a cancellation audit message (`sendCancellationAuditMessage` in `daas-gateway/api/handler/proxy.go`).
**Action on receipt:** Issue the cancel; the eventual provider-side confirmation comes back through the inbound provider-callback path as `Status: Cancelled`.

#### `algo.delivery.rated.v1`

**Source service:** Operator UI / post-delivery feedback.
**Trigger today:** POST to `consts.CarrierRatingPath` = `/api/v2/deliveries/rate` (`r.Post("/rate", Proxy)` under `/v2/deliveries`).
**Action on receipt:** POST to DaaS Gateway, fire-and-forget. Audit only.

#### `algo.marketplace.order.status_changed.v1` / `algo.marketplace.order.switched.v1`

**Source service:** Dispatch service (the `SwitchOrderToMPCarrier` and `stayWithStoreCarrier` decisions in `algo/AggregatorsAPI.go`).
**Trigger today:** `POST /mp/{provider}/order/status` and `POST /mp/{provider}/order/switch` on the DaaS Gateway (`daas-gateway/api/handler/router.go` `MakeMpProxyHandler` → `r.Post("/order/status", MarketplaceProxy)` / `r.Post("/order/switch", MarketplaceProxy)` / `r.Post("/order/events", MarketplaceProxy)` / `r.Post("/events/locations", MarketplaceProxy)`).
**Payload shape:** `OrderStatusUpdate { aggOrderId, brand, country, orderId, orderStatus, storeNo }` and `SwitchOrder { aggOrderId, brand, country, storeNo }` from `algo/AggregatorsAPI.go`.
**Action on receipt:** Forward to DaaS Gateway, retry per `MarketPlace / retryMaxAttempts` (today read by `updateMarketPlaceWithDecision`). Audit-only on the bus.

#### `algo.delivery.missing_events.requested.v1`

**Source service:** Internal recovery loop (the `GetMissingEvents` retry today in `algo/AggregatorsAPI.go`).
**Trigger today:** `GET /api/v1/deliveries/{brand}/{country}/{storeNo}/{orderId}` and `GET /api/v1/marketplace/{brand}/{country}/{storeNo}/{orderId}` (`GetMissingEvents` / `GetProviderCallbacks` handlers in `daas-gateway/api/handler/proxy.go`; paths from `consts.GetMissingEventsPath` / `consts.GetMissingEventsPathForMP`).
**Action on receipt:** Pull missed callbacks from DaaS Gateway and re-emit them onto the bus as if they had arrived live — same shape as the inbound provider-callback events below. Mark them with `algo.delivery.provider.callback.v1` + `recovery: true`.

### Internal bus → outbound Kafka to Proxy (replaces today's Algo→Proxy WebSocket)

#### `algo.mobile.event.dispatched.v1`

**Source service:** Any internal service that today writes a `helper.PsEvent` onto `con.sendChan` in `algo/ProxyClient.go`.
**Trigger today:** `ProxyClient.go` opens a WebSocket to the Proxy (`ConnectToProxyServer` → `/stores/ws` or `/ws`), logs in with `Login { username: storeNo, password: helper.PwForStore(storeNo), suppotredEvents: ["Login","AppData","Geofence","Location","Order","Carrier","ClearData","SurveyData","PushNotify","ClearNotifications","TrackOrder","CheckList","StoreStatus","BidRequest","Panic","AvailableOrders"], storeName, … }`, then pumps `PsEvent { eventName, value, storeNo, carrierId, market, eventInLocalTime, … }` over `writePump`.
**Payload shape:** Canonical envelope with `data` = the original `PsEvent` body. Inner `eventName` is one of the values above (compounded by `EventName_PushNotification`, `EventName_AvailableOrders`, `EventName_StatsByRange`, `EventName_HistoricOrderList`, `EventName_MobileParameters` constants from `algo/ProxyClient.go` and `proxy/MobileServices.go`).
**Why we consume it:** Replaces the Algo→Proxy WebSocket as the single egress path from the cluster to mobile devices and the tracker UI.
**Action on receipt:** Publish to the Proxy ingress Kafka topic, partition-keyed by `(storeId, carrierId)` so per-carrier ordering matches today's per-connection ordering. Honor `StoresWhiteList` / `CarriersWhiteList` from `proxy/main.go` `setEventForwardingWhitelists` at the gateway boundary so the Proxy receives only allowed events.
**Notes:** This event covers **all** of: `Login`, `AppData`, `Geofence`, `Location`, `Order`, `Carrier`, `ClearData`, `SurveyData`, `PushNotify`, `ClearNotifications`, `TrackOrder`, `CheckList`, `StoreStatus`, `BidRequest`, `Panic`, `AvailableOrders`, `MobileParameters`, `Relogin` (`event := &PsEvent{EventName: "Relogin"}` in `proxy/PsEvents.go`), and per-event acks. We deliberately keep one canonical envelope so the Proxy does not need a topic-per-event-type.

#### `algo.mobile.tracker_v2.dispatched.v1`

**Source service:** Anything internal that today writes `carrier` / `order` / `location` PsEvents that `proxy/eventForwarders.go` `ForwardToTrackerServiceV2` forwards to the tracker v2 service (`TrackerV2Service`).
**Trigger today:** `proxy/eventForwarders.go` `ForwardToTrackerServiceV2` does a `POST {trackerV2Url}/{eventName}` with `Authorization: Bearer {proxy-jwt}`, with `eventName ∈ { carrier, order, location }`. The proxy enriches `order` events with `OrderCarrierStatus` (`getItemsList | remove | dispatched | delivered`) from `proxy/types/types.go`.
**Action on receipt:** Republish on the Proxy ingress Kafka topic with a `target: trackerV2` discriminator so the Proxy knows to forward via its tracker-v2 path instead of the device WebSocket path.

### Internal bus → outbound REST to Admin Panel UI

The Admin Panel UI does not consume bus events directly; it polls / writes through the gateway's REST surface (§7). The internal events involved here are the *responses* the gateway emits back into the bus when a backoffice write needs domain validation — for example, `algo.admin.polygon.updated.v1` published after a successful `POST /backoffice/getPolygons` save round-trip.

### External → internal bus (provider callbacks from DaaS Gateway)

#### `algo.delivery.provider.callback.v1`

**Source service:** DaaS Gateway provider callback (every 3PL eventually flows through `daas-gateway/api/handler/callback.go` `Callback` on `POST /callbacks/{brand}/{country}/{storeNo}` or `POST /callbacks/{brand}/{franchisee}/{country}/{storeNo}`).
**Trigger today:** DaaS Gateway publishes the callback onto a RabbitMQ queue keyed by `proxy_helper.GetRoutingKey(brand, franchisee, country, storeNo, "")`. Algo consumes it through `algo/rabbitmq.go` → `OnAggMessage` in `algo/AggregatorsEvents.go`. Wire shape: `AlgoCallback { providerID, targets[]{ id, orderID, status }, courier{ status } }`.
**Payload shape (canonical):** `storeId`, `brand`, `country`, `aggregatorId`, `orderId`, `aggOrderId`, `deliveryStatus` ∈ `{ Waiting, Scheduled, Assigned, PickedUp, Delivered, Cancelled, CouldNotDeliver, CourierLogin }`, `courierStatus` ∈ `{ Unassigned, Accepted, EnrouteToPickup, ArrivedAtStore, CourierPickedUp, EnrouteToDropoff, ArrivedToCustomer, DroppedOff, Nearby }`, `courier { id, firstName, lastName, phone, vehicleType, location { lat, lng }, rating }`, `pickupETA`, `updatedAt`, `deliveryValidation`.
**Why we consume it:** This is the cluster's eyes on what the 3PL fleet is doing — Order Management, dispatch, Promise Time, and the courier UI all need it.
**Action on receipt:** Idempotency check against `(storeId, aggOrderId, deliveryStatus, courierStatus, updatedAt)`. Publish to `algo.delivery.provider.callback.v1` partition-keyed by `storeId`. Order Management, Customer Service, AI Promise Time, and the makeline / dispatch projection subscribe. Audit-write to the provider-callbacks audit topic (today `provider_callbacks` queue per `daas-gateway/api/handler/callback.go`).
**Notes:** Today the broker reorders are rare because there is a single consumer per queue per store; in Algo 4 we keep ordering per `(storeId, aggOrderId)` via the Kafka partition key. The `CourierLogin` status carries Stuart-style fleet on/off-shift signals — preserve as-is.

#### `algo.delivery.quote.received.v1`

**Source service:** DaaS Gateway aggregated quote response (multi-DaaS).
**Trigger today:** `daas-gateway/api/handler/proxy.go` `HandleQuote` calls `proxy_helper.GetQuoteResponse` and publishes the chosen `AggQuoteData` onto the `{routingKey}-quote` RabbitMQ queue. Algo consumes via `algo/rabbitmq.go` → `client.OnMultiDaasQuote` (only registered when `store.multiDaasEnabled()`).
**Payload shape:** `AggQuoteData { providerID, id, createdAt, expiresAt, fee, currency, pickupDuration, dropoffDuration, dropoffETA, pickupETA, providerConf, orderIDs[] }`.
**Action on receipt:** Publish `algo.delivery.quote.received.v1` keyed by `(storeId, orderIDs[0])`. Dispatch consumes.

#### `algo.delivery.eta.updated.v1`

**Source service:** DaaS Gateway ETA updates (Urbanpiper-style "rider_eta" stream).
**Trigger today:** Algo consumes `{getMPQueueRoutingKey()}-eta-v2` via `algo/rabbitmq.go` → `OnETAmessage` in `algo/AggregatorsEvents.go`, fed by `daas-gateway/api/handler/proxy.go` `Proxy` when `req.URL.Path` ends with `/eta` (Urbanpiper).
**Payload shape:** Same `AggMessage` shape with `isUpdatedEta = "rider_eta"`.
**Action on receipt:** Publish a focused ETA-update event keyed by `(storeId, aggOrderId)`.

#### `algo.delivery.cancellation.audit.v1`

**Source service:** DaaS Gateway cancellation audit feed (today `daas-gateway/api/handler/proxy.go` `sendCancellationAuditMessage` writes `ProviderCallbackAuditMessage` to the `provider_callbacks` queue).
**Action on receipt:** Republish as an audit-only event; not authoritative for Order Management state.

### External → internal bus (mobile / tracker events from Proxy)

#### `algo.mobile.event.received.v1`

**Source service:** Courier mobile device, via Proxy.
**Trigger today:** The Proxy WebSocket `readPump` in `proxy/conn.go` decodes `PsEvent { eventName, value, eventId, proxyEventId, … }` from the device, runs `ProcessInputData` (`proxy/PsEvents.go`), and forwards device→backend events back to Algo on the same WebSocket. In Algo 4 the Proxy publishes them on a Kafka topic the gateway consumes.
**Payload shape (canonical):** `storeId`, `carrierId`, `deviceId`, `eventName` ∈ {`Location`, `Order`, `Carrier`, `Login`, `Logout`, `GetUpdates`, `PushNotification`, `BidRequest`, `Panic`, `Geofence`, `ClearData` ack, `AppData`, `MobileParameters` ack, `TrackOrder`, `CheckList`, `StoreStatus`, `Relogin`, …}, `proxyEventId`, `asResponseToRecId`, `eventInLocalTime`, `value{}` (the device-side body).
**Why we consume it:** Every change a courier reports (waypoints, status acks, panic, order acceptance) must reach Order Management, dispatch, telematics, and the makeline projection.
**Action on receipt:** Publish per `eventName` partitioned by `(storeId, carrierId)`. Order Management, telematics, dispatch, makeline projection consume.

#### `algo.mobile.location.received.v1`

**Source service:** Courier mobile device location stream (the high-volume cousin of the generic mobile event above; called out separately because the volume / ordering / dedup logic is different).
**Trigger today:** Either as a `PsEvent { eventName: "Location" }` over WebSocket or as `POST /setCarrierLocation` to the Proxy (`proxy/DragonTrack.go` `locationHandler`). The Proxy decodes `CarrierLocation { uid, storeNo, carrierId, eventId, lat, lng, source, accuracy, secondsPassed }` and forwards to Algo.
**Payload shape (canonical):** `storeId`, `carrierId`, `lat`, `lng`, `accuracy`, `source`, `secondsPassed`, `gpsStatus`, `accurate`, `altitude`, `heading`, `speed`, `localtime`, `idle` (the union of `LocationData` from `proxy/PsEvents.go` and `CarrierLocation` from `proxy/DragonTrack.go`).
**Action on receipt:** Publish keyed by `(storeId, carrierId)` with a high partition count so location streams don't head-of-line block other mobile events.
**Notes:** Also enrich with the carrier's current order list (today `proxy/eventForwarders.go` `getCarrierOrders` does that for the `location` event before forwarding to tracker v2).

#### `algo.mobile.tracker.deliveryRoute.v1` / `algo.mobile.tracker.messageDriver.v1` / `algo.mobile.tracker.driveThrough.v1`

**Source service:** Dragon Track customer-facing tracker UI and operator messaging UI.
**Trigger today:** `proxy/main.go` `trackerListener` routes `/deliveryRoute`, `/messageDriver`, `/drivethrough`, and `/track/*` to the corresponding handlers in `proxy/DragonTrack.go`.
**Action on receipt:** Republish on the bus so dispatch / customer-comms services can pick them up.

#### `algo.mobile.tracker_v2.received.v1`

**Source service:** Tracker-service v2 `forwardeventstotrackerv2` callbacks (the Proxy itself produces these into the tracker service).
**Trigger today:** None inbound *to* Algo today — the tracker v2 flow is outbound only (`proxy/eventForwarders.go` `ForwardToTrackerServiceV2`). Listed here for completeness: if the tracker v2 service ever produces callbacks they would enter through this gateway.
**Notes:** No-op in v1.

### External → internal bus (Admin Panel UI writes)

#### `algo.admin.dashboard.requested.v1`

**Source service:** Admin Panel UI.
**Trigger today:** `algo/endpoints.go` `registerBORoutes` — `POST /dashboard`, `POST /dashboard/*`, `GET /dashboard/settings`, `POST /getReportFromDashboard` (the last one served by `processCenterRoutes`).
**Action on receipt:** Stateless read; the gateway proxies to the dashboard backend. Audit-only event on the bus for traceability.

#### `algo.admin.backoffice.crud.v1`

**Source service:** Admin Panel UI.
**Trigger today:** `algo/endpoints.go` `registerBORoutes` — `/bo*`, `/backofficeHeader*`, `/backofficeNav*` (mounted on `crudhnd.TableHandler` / `crudhnd.BackofficeStructureHandler` / `crudhnd.BackofficeNavHandler`).
**Action on receipt:** The gateway calls the appropriate domain service (Store / Employee / Permission / VIP / …); on success publish a domain-specific event (`algo.store.updated.v1`, `algo.employee.updated.v1`, etc.) for cluster-wide observability.

#### `algo.admin.polygon.updated.v1`

**Source service:** Admin Panel polygon editor.
**Trigger today:** `POST /getPolygons` and `POST /backoffice/getPolygons` (`algo/endpoints.go`).
**Action on receipt:** Route to the geo / trade-area service, publish the resulting polygon update for any subscriber that caches polygons.

#### `algo.admin.store.self.requested.v1` / `algo.admin.store.qrcode.requested.v1` / `algo.admin.store.daas.requested.v1`

**Source service:** Admin Panel UI.
**Trigger today:** `GET /api/stores/self` (`algo/storeDetails.go` `StoreDetailsHandler`), `GET /getSingleStoreNo`, and `GET /api/stores/{storeID}/daas` (`algo/daas_bridge.go` `loadDaasProviders`, returning `DaasProvidersResponse { active, providers[]{ name, enabled, active, cancellationDisabled } }`).
**Action on receipt:** Stateless read passthrough; audit-only.

#### `algo.admin.auth.user.v1`

**Source service:** Admin Panel UI.
**Trigger today:** `POST /register`, `Handle /changePassword`, `Handle /forgotPassword` (`algo/endpoints.go` `registerBORoutes`).
**Action on receipt:** Forward to Auth Service; no canonical event on the bus.

---

### Not outbound / inbound events for this gateway (deliberately excluded)

- **POS / KDS / Menu / Labor / VIP / cabinet / vision / GPS push paths** — owned by the **Inbound API Gateway**. See `docs/inbound-api-gateway.md`.
- **Cloud-orders RabbitMQ queue** (`{routingKey}-cloud-orders` → `ProccessCloudOrdersEvent` in `algo/CloudOrderEvent.go`) — this is *order intake* and belongs to the Inbound API Gateway as `algo.pos.order.received.v1`, even though it lands on RabbitMQ today.
- **Remote-access SSH tunnel** (`{routingKey}-remote-access` → `ProccessRemoteAccessEvent` in `algo/CloudRemoteAccess.go`) — operational support channel, stays separate.
- **Cabinet hardware** (`{routingKey}-cabinet` → `OnPucMessage`) — third-party hardware push, owned by the Inbound API Gateway.
- **Internal compute** (`OptimizeOrdersFromJson`, `AvgTimePerKilometerJson`, `VehicleLocationAlertJson`, `GetCarrierRoutesDistance*`, `EventStartRaise`, `CallTraffilog`, `SetLocation`) — internal between Algo 3 components, not a cloud egress.
- **Maps proxy** (`/maps/*` in `algo/maps.go` and `algo/endpoints.go`) — server-rendered map tiles; stays internal.
- **Direct algo→cloud HTTP calls bypassing the shared client** (`algo/CabinetsAPI.go`, `algo/Analysis.go`, `algo/SMSMechanism.go`, `algo/IgnitionEvent.go` Traffilog pull) — listed in `algo/communication-component-hld.md` "Outbound Scope"; whether each moves through this gateway is decided per integration. Default in Algo 4: every cloud-bound call routes through here.
- **Auth surface** (`POST /login`, `Handle /pinCode`, `/api/auth/token`, `/api/auth/refresh`, `/api/auth/authorize`) — owned by Auth Service.
- **Per-store WebSocket / SSE fan-out inside the store** (`/ws`, `/wsHome`, `/api/events`) — owned by the makeline / FE projection.
- **`/handler/*` legacy makeline FE → backend** (`algo/nethandler.go`, `NetHandlerStyle`) — internal traffic in Algo 3, becomes in-cluster RPC in Algo 4.

## 5. Events Emitted

<!--
Two halves: events emitted onto the internal Kafka bus (canonical Algo
events, same envelope as CLAUDE.md), and outbound calls to external systems
(HTTP to DaaS Gateway, Kafka to Proxy, REST to Admin Panel UI).

We list bus events here; external calls are described under §7 "API Exposed
to Other Services" because, from this gateway's point of view, they ARE
the public surface offered to the rest of the cluster.
-->

### `algo.delivery.discovery.received.v1`
**Emitted when:** A `POST /api/v1/discovery` response is received from DaaS Gateway.
**Payload summary:** `storeId`, `providerId`, `providerConf { cancellationDisabled, batchingEnabled, autoEnrouteOnAggPickup, callLimit, … }`.
**Expected consumers:** Dispatch / Quote service.
**Notes:** Cached per store.

### `algo.delivery.quote.received.v1`
**Emitted when:** A DaaS Gateway quote arrives — either synchronously on `POST /api/v2/deliveries/quotes` or asynchronously over the `{routingKey}-quote` queue (multi-DaaS).
**Payload summary:** `AggQuoteData { providerID, id, createdAt, expiresAt, fee, currency, pickupDuration, dropoffDuration, dropoffETA, pickupETA, providerConf, orderIDs[] }`.
**Expected consumers:** Dispatch service.

### `algo.delivery.quote.failed.v1`
**Emitted when:** DaaS Gateway responded with `proxy_helper.ErrNoSuccessfulQuoteFound` or every provider failed.
**Expected consumers:** Dispatch service (`stayWithStoreCarrier` decision).

### `algo.delivery.created.acked.v1` / `algo.delivery.created.failed.v1`
**Emitted when:** `POST /api/v2/deliveries` was accepted / rejected by DaaS Gateway.
**Expected consumers:** Order Management, dispatch.

### `algo.delivery.eta.received.v1`
**Emitted when:** Response to `POST /api/v2/deliveries/eta` arrives.
**Expected consumers:** AI Promise Time, dispatch.

### `algo.delivery.eta.updated.v1`
**Emitted when:** Rider ETA update arrives on `{routingKey}-eta-v2` (today `OnETAmessage`).
**Expected consumers:** AI Promise Time, customer-comms.

### `algo.delivery.canceled.acked.v1`
**Emitted when:** `DELETE /api/v2/deliveries` succeeded.
**Expected consumers:** Order Management.

### `algo.delivery.provider.callback.v1`
**Emitted when:** Any provider callback arrives (RabbitMQ aggregator queue today; DaaS Gateway Kafka in Algo 4).
**Payload summary:** Canonical envelope wrapping `AggMessage` — `storeId`, `aggregatorId`, `aggOrderId`, `orderId`, `deliveryStatus`, `courier { status, location, … }`, `updatedAt`, `pickupETA`, `deliveryValidation`.
**Expected consumers:** Order Management (authoritative), AI Promise Time, dispatch, customer-comms.
**Notes:** Partition key `storeId`. Idempotent on `(storeId, aggOrderId, deliveryStatus, courierStatus, updatedAt)`.

### `algo.delivery.courier_login.v1`
**Emitted when:** Stuart-style courier `CourierLogin` callback arrives — fleet on/off-shift.
**Expected consumers:** Dispatch, Employee Service.

### `algo.delivery.cancellation.audit.v1`
**Emitted when:** DaaS Gateway emits its cancellation audit message (`sendCancellationAuditMessage`).
**Expected consumers:** Audit Service.

### `algo.marketplace.order.acked.v1`
**Emitted when:** `POST /mp/{provider}/order/status` or `POST /mp/{provider}/order/switch` succeeds.
**Expected consumers:** Dispatch (so the decision propagates and `OrderFlowAlerts` updates per `algo/AggregatorsAPI.go`).

### `algo.mobile.event.received.v1`
**Emitted when:** Proxy forwards a device-originated `PsEvent` (any `eventName` in §4 inbound table).
**Payload summary:** `storeId`, `carrierId`, `deviceId`, `eventName`, `proxyEventId`, `asResponseToRecId`, `eventInLocalTime`, `value{}`.
**Expected consumers:** Order Management (Order / Carrier acks), telematics (Location, Geofence), dispatch (BidRequest, Panic), makeline projection (TrackOrder), notifications (PushNotification ack).
**Notes:** Partition key `(storeId, carrierId)`.

### `algo.mobile.location.received.v1`
**Emitted when:** A `Location` PsEvent or `POST /setCarrierLocation` arrives via Proxy.
**Expected consumers:** Telematics projection, dispatch.

### `algo.mobile.tracker.deliveryRoute.v1` / `algo.mobile.tracker.messageDriver.v1` / `algo.mobile.tracker.driveThrough.v1`
**Emitted when:** Dragon Track UI / RoadRunner posts to `/deliveryRoute`, `/messageDriver`, `/drivethrough` via Proxy.
**Expected consumers:** Dispatch, customer-comms.

### `algo.mobile.delivery.expired.v1`
**Emitted when:** A `algo.mobile.event.dispatched.v1` could not be delivered to a device within the configured TTL (offline phone).
**Expected consumers:** Notifications service, dispatch.

### `algo.admin.dashboard.requested.v1` / `algo.admin.backoffice.crud.v1` / `algo.admin.polygon.updated.v1` / `algo.admin.store.self.requested.v1` / `algo.admin.store.qrcode.requested.v1` / `algo.admin.store.daas.requested.v1` / `algo.admin.auth.user.v1`
**Emitted when:** The corresponding Admin Panel UI REST call completes.
**Expected consumers:** Mostly Audit Service; polygon updates have real domain consumers (geo / trade-area).

### `algo.outbound.audit.v1` (cross-cutting)
**Emitted when:** Every lifecycle transition (`REQUESTED → DISPATCHED → ACKED | FAILED | DEAD_LETTERED`).
**Payload summary:** `eventId`, `correlationId`, `direction` ∈ {`to-daas`, `to-proxy`, `to-admin-ui`, `from-daas`, `from-proxy`, `from-admin-ui`}, `targetSystem`, `status`, `rawRequestRef`, timestamps, error details, retry count.
**Expected consumers:** Audit Service.

## 6. Data Dependencies (External Reads)

### Store / brand / country routing context
**Owning service:** Store Service.
**When fetched:** On every internal bus event that needs an external call (to build `routingKey`, partition key, DaaS Gateway URL parameters).
**Why needed:** Today `algo/Stores.go` `getQueueRoutingKey()` / `getMPQueueRoutingKey()` and `daas-gateway/pkg/proxy_helper.GetRoutingKey` both build `{brand}-{country}-{externalStoreId}` from the store record. The gateway needs the same lookup to construct the right DaaS callback path (`/callbacks/{brand}/{country}/{storeNo}` or `/callbacks/{brand}/{franchisee}/{country}/{storeNo}`).
**How fetched:** Cached lookup, refreshed every N minutes; cache miss falls back to the latest known value and emits an audit warning.

### DaaS provider catalog
**Owning service:** DaaS Gateway (`daas-gateway/entity` `AggService { id, name, url, cancellationDisabled, batchingEnabled, autoEnrouteOnAggPickup, callLimit }`, exposed through `/aggregator-services` CRUD).
**When fetched:** On startup and on a configurable refresh cadence; also after any `algo.admin.store.daas.requested.v1` write.
**Why needed:** Pick the right provider URL, batching policy, cancellation policy, call limit, and authentication for every `POST /api/v2/deliveries{,/quotes,/eta,/rate}` call. Today `algo/AggregatorsAPI.go` `GetAggregator` queries the local SQLite mirror; in Algo 4 the source of truth is DaaS Gateway.

### Multi-DaaS / aggregator settings per store
**Owning service:** Store Service.
**When fetched:** On dispatch decisions and on `algo.delivery.discovery.requested.v1`.
**Why needed:** Today `algo/daas_bridge.go` `loadDaasProviders` joins `GetAggregatorsSettings` + `GetMultiDaasActivity` + `optionlist.FloatValue("Aggregators","CancellationDisabled")` to compute `DaasProvidersResponse { active, providers[] }`. The gateway needs the same to know whether to call quotes synchronously or to fan out over the multi-DaaS queue.

### Per-provider runtime credentials
**Owning service:** Secrets store (TBD; today configured in DaaS Gateway env / `cfg.Auth` per `daas-gateway/api/handler/router.go` `middleware.BasicAuth`).
**Why needed:** Sign every outbound call to DaaS Gateway.

### Proxy bearer token + market
**Owning service:** Auth Service (today: the algo→proxy token is fetched by `algo/ProxyClient.go` `getTokenToConnectProxyMs(market, proxyAddress, protocol)`; cached in `proxyServiceAuthToken`).
**When fetched:** Once per gateway boot, refreshed on `401`. Also on every Proxy Kafka batch the gateway must set `X-StoreNo` / `X-Market` headers to satisfy `proxy/MobileServices.go` and `proxy/eventForwarders.go`.
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
**Purpose:** Subscribe to every internal Kafka topic listed in §4 "Internal bus → outbound …" and translate to the right external call.
**Known consumers:** N/A — this is the gateway's input. Listed for symmetry with the Inbound API Gateway's "internal canonical-event publishing surface".
**Inputs:** Canonical Algo events with the `CLAUDE.md` envelope.
**Returns:** Async; no synchronous reply on the bus (replies come back as separate inbound canonical events — e.g. `algo.delivery.quote.received.v1` after `algo.delivery.quote.requested.v1`).
**Notes:** Partition keys per topic are chosen so per-`storeId` ordering and per-`(storeId, carrierId)` ordering are preserved end-to-end.

### Outbound HTTP surface to DaaS Gateway
**Purpose:** Be wire-compatible with the DaaS Gateway routes the cluster uses today so the DaaS Gateway does not need to change for the Algo 4 cutover.
**Known consumers:** DaaS Gateway is the consumer of the outbound calls; ultimately each call lands on a 3PL provider behind the DaaS Gateway.
**Inputs:** `AggRequest`, `AggDiscoveryRequest`, `OrderStatusUpdate`, `SwitchOrder`, rating payload, ETA payload — exact shapes as currently produced by `algo/AggregatorsAPI.go`.
**Returns:** `AggResponse { status, data, message }`, `AggQuoteData`, `AggDeleteData` — exact shapes as consumed today.
**Notes:** Auth scheme matches what DaaS Gateway expects (per-provider keys + the basic-auth `cfg.Auth` for the `/aggregator-services` admin routes).

### Outbound Kafka surface to Proxy
**Purpose:** Single ingress Kafka topic to the Proxy service carrying every device-bound `PsEvent` and tracker-v2 forward.
**Known consumers:** Proxy service consumers, which then deliver via WebSocket (`proxy/conn.go`) or via tracker-v2 HTTP (`proxy/eventForwarders.go` `ForwardToTrackerServiceV2`).
**Inputs:** `PsEvent` body wrapped in the canonical envelope; `target` discriminator (`device`, `tracker-v2`).
**Returns:** Async; per-event delivery acks land back on `algo.mobile.event.received.v1`.
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
**Purpose:** Receive provider callbacks from DaaS Gateway (today over RabbitMQ; Algo 4 over DaaS-published Kafka or a dedicated callback Kafka topic — open question §9).
**Known consumers:** N/A — gateway-internal.
**Inputs:** `AlgoCallback { providerID, targets[], courier{} }`, `AggMessage`, audit messages.
**Returns:** Canonical events on the internal bus.

### Inbound Proxy event receiver
**Purpose:** Receive every device-originated and tracker-UI-originated event from Proxy.
**Known consumers:** N/A — gateway-internal.
**Inputs:** `PsEvent` envelope, `CarrierLocation` envelope, `DriveThroughData`, deliveryRoute / messageDriver payloads.
**Returns:** Canonical events on the internal bus.

### Outbound audit query API
**Purpose:** Look up the lifecycle of any outbound message by `eventId`, `correlationId`, `(storeId, orderId)`, `(storeId, carrierId)`, or `aggOrderId`.
**Known consumers:** Audit Service tooling, on-call engineers, support tooling.
**Returns:** Lifecycle row plus the `rawRequestRef` to the captured body.

## 8. Operational Assumptions

- Every internal service that today produces a `helper.PsEvent` and shoves it onto `con.sendChan` in `algo/ProxyClient.go` will instead publish a canonical bus event that this gateway consumes. The gateway is the **only** writer to the Proxy ingress Kafka topic.
- Every internal service that today calls a DaaS Gateway HTTP endpoint directly (the four call sites in `algo/AggregatorsAPI.go`, the marketplace decisions in `SwitchOrderToMPCarrier` / `stayWithStoreCarrier`, the discovery loop) will instead publish a canonical bus event. The gateway is the **only** writer to DaaS Gateway HTTP.
- DaaS Gateway is willing to publish provider callbacks to a Kafka topic the gateway consumes (or, transitionally, the gateway continues to consume the existing RabbitMQ aggregator / quote / ETA queues — see §9).
- Proxy is willing to consume from a Kafka topic for ingress instead of holding a WebSocket from Algo, and to publish to a Kafka topic for egress. If Proxy stays WebSocket-only in v1, this gateway runs a single WebSocket client on behalf of the whole cluster and bridges to Kafka internally — i.e. the Algo→Proxy WebSocket moves *into* the gateway, no other service holds one.
- Mobile devices do not need to know that anything changed. The same `PsEvent` shape over the same WebSocket Proxy continues to be served by the same Proxy service.
- DaaS Gateway's `/aggregator-services` admin surface stays where it is (`daas-gateway/api/handler/router.go` `MakeHandlers` with `middleware.BasicAuth`). The Algo 4 Admin Panel UI either calls DaaS Gateway directly for that or through this gateway as a transparent reverse-proxy — TBD §9.
- The Admin Panel UI is cloud-hosted and reaches the gateway over HTTPS with a JWT. The current in-store-bundled admin pages (`algo/FrontEnd/` SPA mounts under `/dashboard`, `/backoffice`, etc.) keep working for backwards compatibility but new sessions land on the cloud Admin Panel.
- Per-provider rate limits (`AvailableCalls` / `CallLimit`), retry budgets (`MarketPlace / retryMaxAttempts`, `Aggregators / DiscoveryRetrySeconds`), and per-event TTLs are configured before the gateway is allowed to accept traffic.
- The DaaS Gateway, the Proxy, and the Admin Panel UI are reachable from the gateway over the public internet with TLS — the gateway is the egress chokepoint and the only place that holds those credentials.
- `CourierLogin` is Stuart-specific. Other 3PLs may add fleet on/off-shift signals later; the gateway must not assume the set of `DeliveryStatus` values is fixed forever.
- `RemoteAccess` (`{routingKey}-remote-access` queue, `ProccessRemoteAccessEvent`) is not migrated into this gateway — it stays as a separate operational support channel.
- The gateway does not own delivery semantics. If Order Management decides a callback is invalid (wrong store, unknown order, stale `updatedAt`), the gateway has already published the canonical event and Order Management is the authoritative dropper.
- `kfcmenuservice`-style flows do **not** flow through this gateway in either direction; they are owned by the Inbound API Gateway.

## 9. Open Questions

- **DaaS Gateway provider-callback transport.** Today provider callbacks land on RabbitMQ queues consumed by Algo directly (`OnAggMessage`, `OnETAmessage`, `OnMultiDaasQuote`, `OnPucMessage`). For Algo 4 we have three options: (a) the gateway runs a RabbitMQ consumer on the same queues and republishes to Kafka, (b) DaaS Gateway is modified to publish to a callback Kafka topic the gateway consumes, (c) DaaS Gateway calls a callback HTTP endpoint on the gateway (mirroring `daas-gateway/api/handler/callback.go` Callback). Recommendation: option (b), with option (a) as the transition path.
- **Proxy ingress/egress transport.** Same question for the Algo↔Proxy WebSocket. Three options: (a) the gateway is the only WebSocket client to Proxy and bridges to Kafka internally (no Proxy change), (b) Proxy gains Kafka ingress/egress, (c) Proxy itself becomes the cloud gateway role, absorbing this service. Recommendation: option (a) for v1, with option (b) as the target.
- **Admin Panel UI reverse-proxying.** Should the gateway transparently reverse-proxy admin routes to underlying domain services (single hop), or should each domain service be Kafka-driven and the gateway compose answers (event-sourced)? Recommendation: reverse-proxy for v1, event-sourced for write paths in v2.
- **Tracker-service v2 callback path.** Today the tracker-v2 flow is outbound only (`forwardeventstotrackerv2`). If the tracker service starts sending callbacks (delivery viewed, customer messaged), they will arrive over a new HTTP route — confirm that route is added to this gateway, not to Proxy.
- **`X-Market` header propagation.** Today `algo/ProxyClient.go` `ConnectToProxyMicroService` and `proxy/eventForwarders.go` use a per-event `Market` (falling back to proxy-level `AppConf.Market`). Confirm the canonical envelope carries `market` so the gateway can fill the header on outbound calls.
- **Multi-DaaS quote semantics.** `algo/rabbitmq.go` only registers `OnMultiDaasQuote` on the quote queue when `store.multiDaasEnabled()`. Confirm the gateway uses the same condition — single-DaaS replies synchronously on the HTTP response; multi-DaaS reads the quote off the response Kafka topic.
- **Idempotency on provider callbacks.** Today there is no explicit idempotency key on `AggMessage`; dedup relies on Order Management's `CountAggregatorOrder` check plus the (rare) RabbitMQ at-least-once semantics. With Kafka at-least-once, we need an explicit dedup tuple. Default proposal: `(storeId, aggOrderId, deliveryStatus, courierStatus, updatedAt)`.
- **Retry / DLQ policy on outbound HTTP.** `updateMarketPlaceWithDecision` has its own retry loop (`MarketPlace / retryMaxAttempts`, `MarketPlace / retryMaxSeconds`). Confirm the gateway-wide policy supersedes per-call-site retry loops.
- **Whitelist scope.** Today `setEventForwardingWhitelists` filters tracker-v2 forwarding only. Should it also filter the device-bound WebSocket path? Today no. Confirm we keep that asymmetry or unify.
- **`/aggregator-services` admin routes.** The DaaS Gateway exposes its own admin CRUD (`r.Post("/", CreateAggService)` etc. in `daas-gateway/api/handler/router.go`). Either expose it through this gateway or have the Admin Panel call it directly. Decide before MVP.
- **Remote-access tunnel.** Confirm `ProccessRemoteAccessEvent` and `CloudRemoteAccessQueue` stay outside this gateway. Probably yes; it is an operational support concern, not a business event.
- **Per-store identity to DaaS Gateway.** Today every store posts with the same shared credentials in `consts`. Algo 4 should attach a store-scoped identity (mirroring §6 of the Inbound API Gateway). Tracked against the Auth Service decision in `CLAUDE.md`.
- **Mobile event TTLs.** Default per-`eventName` TTLs (Location: seconds; Order: minutes; ClearData: minutes; PushNotify: hours) — needs product input.
- **Topic-naming standard.** The canonical topic naming convention is defined in `docs/event-taxonomy.md`.
- **Cabinet `OnPucMessage`** is correctly *not* in scope here, but it shares the same RabbitMQ broker as the aggregator callbacks. Confirm the inbound-side cabinet handler in the Inbound API Gateway also runs an AMQP consumer, and that this gateway's AMQP consumer does not accidentally pick up cabinet messages.
