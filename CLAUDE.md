# Algo 4 - Inbound API Gateway HLD

## Purpose

Algo 4 needs an inbound API Gateway that receives external events from Menu Management, Labor Management, and KDS, captures the original request, normalizes the data into a canonical Algo event, and publishes it to the internal EventBus.

The Gateway is responsible for the inbound boundary only. Internal services should subscribe to EventBus topics rather than being called directly by the Gateway.

## Scope

### In Scope

- Receive external `POST` requests.
- Authenticate callers through the Auth Service, with the exact method still TBD.
- Store raw incoming payloads exactly as received.
- Store request metadata for audit, traceability, and debugging.
- Validate incoming requests internally.
- Normalize source-specific payloads through an adapter.
- Produce canonical Algo events.
- Publish events to the internal EventBus.
- Track validation and publishing outcomes.
- Support retry and dead-letter behavior for failed publishing or failed processing.

### Out Of Scope For Now

- Detailed source-specific contract definitions.
- Final Auth Service method.
- Final EventBus technology.
- Final raw storage technology.
- Detailed downstream service implementation.

## Current Design Summary

```mermaid
flowchart LR
  %% Algo 4 Inbound API Gateway - Executive Architecture

  subgraph External["External Systems"]
    MenuManagement["Menu Management"]
    LaborManagement["Labor Management"]
    KDS["KDS"]
  end

  subgraph Gateway["Algo 4 Inbound API Gateway"]
    EventIntake["Event Intake API<br/>Inbound POST Events"]
    AuthService["Auth Service<br/>Source Identity + Trust"]
    Capture["Request Capture<br/>Metadata + Correlation ID"]
    Validator["Request Validator"]
    Adapter["Source Adapter"]
    Envelope["Canonical Algo Event<br/>JSON Envelope"]
  end

  subgraph Audit["Audit And Reliability"]
    RawStore[("Immutable Raw Request Store")]
    Rejected["Rejected Event Topic"]
    FailedNormalization["Failed Normalization Topic"]
    Retry["Retry Queue"]
    DLQ["Dead Letter Queue"]
  end

  subgraph Messaging["Internal EventBus"]
    EventBus[("Business Event Topics")]
  end

  subgraph Consumers["Subscribed Internal Services"]
    Quote["Quote Service"]
    Customer["Customer Service"]
    Employee["Employee Service"]
    PromiseTime["AI Promise Time"]
    Orders["Order Management"]
  end

  MenuManagement -->|"POST event"| EventIntake
  LaborManagement -->|"POST event"| EventIntake
  KDS -->|"POST event"| EventIntake

  EventIntake --> AuthService
  AuthService --> Capture
  Capture --> RawStore
  Capture --> Validator
  Validator -->|"valid"| Adapter
  Validator -->|"invalid"| Rejected
  Adapter -->|"normalized"| Envelope
  Adapter -->|"normalization failed"| FailedNormalization
  Envelope -->|"publish"| EventBus
  Envelope -->|"publish failed"| Retry
  Retry -->|"retry publish"| EventBus
  Retry -->|"retries exhausted"| DLQ

  EventBus --> Quote
  EventBus --> Customer
  EventBus --> Employee
  EventBus --> PromiseTime
  EventBus --> Orders

  classDef external fill:#E3F2FD,stroke:#1565C0,stroke-width:1px,color:#0D47A1
  classDef gateway fill:#E8F5E9,stroke:#2E7D32,stroke-width:1px,color:#1B5E20
  classDef audit fill:#FFF3E0,stroke:#EF6C00,stroke-width:1px,color:#E65100
  classDef bus fill:#F3E5F5,stroke:#6A1B9A,stroke-width:1px,color:#4A148C
  classDef consumer fill:#ECEFF1,stroke:#455A64,stroke-width:1px,color:#263238

  class MenuManagement,LaborManagement,KDS external
  class EventIntake,AuthService,Capture,Validator,Adapter,Envelope gateway
  class RawStore,Rejected,FailedNormalization,Retry,DLQ audit
  class EventBus bus
  class Quote,Customer,Employee,PromiseTime,Orders consumer
```

## Gateway Responsibilities

### 1. Receive External Requests

The Gateway receives `POST` requests from Menu Management, Labor Management, and KDS. These requests trigger internal processing by publishing events into the cluster.

The external caller should receive a simple acknowledgement. At this stage, either `200 OK` or `202 Accepted` is acceptable. The important design decision is that internal validation or processing failures should not expose detailed internal errors to the caller.

### 2. Auth Service

The Auth Service contract is still missing and should remain an explicit design gap.

Possible options:

- API key.
- HMAC request signature.
- OAuth client credentials.
- mTLS.
- IP allowlist combined with one of the above.

The chosen Auth Service method should identify the source system and allow the Gateway to attach that identity to request metadata.

### 3. Raw Request Storage

The Gateway must store the incoming payload exactly as received, without transformation.

Raw storage should include:

- Raw request body.
- Source system.
- Request headers.
- Request path.
- HTTP method.
- Received timestamp.
- Caller identity, when known.
- Client IP, if allowed by privacy/security rules.
- Correlation ID.
- Validation status.
- Publish status.
- Raw request storage ID or object key.

The raw payload should be treated as immutable audit data.

### 4. Validation

The Gateway validates requests internally. Validation failures should be recorded, but should not leak detailed internal errors to the caller.

Validation can include:

- Required headers.
- Required payload fields.
- Source-specific schema checks.
- Payload size limits.
- Event type support.
- Store or tenant routing context.
- Duplicate detection inputs.

Invalid requests should still be saved as raw data. The system should then either publish a rejected event or keep the rejected request as an audit-only record, depending on the final decision.

### 5. Source Adapter

The Gateway contains an internal adapter layer that converts payload formats from Menu Management, Labor Management, and KDS into a canonical Algo event.

Each source can have its own adapter, but all adapters should produce the same envelope shape. This keeps downstream services decoupled from source-specific payload formats.

### 6. Event Publishing

After normalization, the Gateway publishes the canonical Algo event to the EventBus.

The Gateway should publish business events. It should not know or directly call every downstream service.

Downstream services subscribe to the topics they care about:

- Quote Service.
- Customer Service.
- Employee Service.
- AI Promise Time.
- Order Management.

## Validation Outcome Model

Each inbound request should be tracked through clear internal statuses.

| Status | Meaning |
| --- | --- |
| `RECEIVED` | Gateway accepted the HTTP request. |
| `RAW_STORED` | Raw payload and metadata were persisted. |
| `VALIDATED` | Request passed validation. |
| `REJECTED` | Request failed validation but was recorded. |
| `NORMALIZED` | Adapter created a canonical Algo event. |
| `PUBLISHED` | Event was sent to EventBus. |
| `PUBLISH_FAILED` | EventBus publish failed and retry is required. |
| `DEAD_LETTERED` | Retries were exhausted or the event cannot be processed. |

## Canonical Algo Event

The existing Algo codebase uses JSON-based message payloads and RabbitMQ-style routing conventions. Examples include aggregator messages, cloud order events, cabinet events, and queue suffixes such as `quote`, `eta`, `cloud-orders`, and `cabinet`.

Algo 4 should follow a JSON-first envelope unless another platform decision requires a different format.

### Proposed Event Envelope

```json
{
  "eventId": "uuid",
  "correlationId": "uuid-or-source-correlation-id",
  "eventType": "algo.menu.updated",
  "schemaVersion": "v1",
  "sourceSystem": "menu-management",
  "sourceEventId": "external-id-if-provided",
  "brandId": "brand-or-tenant",
  "country": "country-code",
  "storeId": "external-store-id",
  "receivedAt": "RFC3339 timestamp",
  "occurredAt": "RFC3339 timestamp if provided by source",
  "rawRequestRef": "raw-store-key-or-id",
  "validationStatus": "VALIDATED",
  "data": {}
}
```

### Required Fields

- `eventId`: unique ID generated by Algo 4.
- `correlationId`: ID used for tracing the request across services.
- `eventType`: business event name.
- `schemaVersion`: version of the envelope schema.
- `sourceSystem`: source external system, such as Menu Management, Labor Management, or KDS.
- `receivedAt`: timestamp when Gateway received the request.
- `rawRequestRef`: reference to immutable raw payload storage.
- `data`: normalized event payload.

### Recommended Fields

- `sourceEventId`: external event ID when provided by the source.
- `brandId`: brand or tenant routing context.
- `country`: country routing context.
- `storeId`: external store ID.
- `occurredAt`: timestamp from the source event when available.
- `validationStatus`: latest validation outcome.

## Event Topics

Prefer business-event topics over service-target topics.

Candidate logical topics:

- `algo.inbound.received`
- `algo.inbound.rejected`
- `algo.menu.updated`
- `algo.labor.updated`
- `algo.kds.order_status.updated`
- `algo.quote.requested`
- `algo.promise_time.requested`

If the final EventBus uses RabbitMQ-style routing, these logical topics can be mapped to routing keys that include brand, country, store, and version context.

Example routing context from the existing Algo system:

```text
{brand}-{country}-{externalStoreId}-v2
{brand}-{country}-{externalStoreId}-eta-v2
{brand}-{country}-{externalStoreId}-quote
{brand}-{country}-{externalStoreId}-cloud-orders
```

## Request Flow

```mermaid
sequenceDiagram
  participant Source as "Menu Management / Labor Management / KDS"
  participant Gateway as "Inbound API Gateway"
  participant RawStore as "Raw Request Store"
  participant Adapter as "Source Adapter"
  participant Bus as "EventBus"
  participant Service as "Subscribed Internal Services"

  Source->>Gateway: "POST event"
  Gateway->>RawStore: "Store raw payload and metadata"
  Gateway->>Gateway: "Validate request"
  Gateway->>Adapter: "Normalize source payload"
  Adapter-->>Gateway: "Canonical Algo event"
  Gateway->>Bus: "Publish event"
  Bus-->>Service: "Deliver by topic subscription"
  Gateway-->>Source: "200 OK or 202 Accepted"
```

## Failure Handling

### Validation Failure

If validation fails:

- Store raw payload and metadata.
- Mark status as `REJECTED`.
- Do not expose detailed validation errors to the caller.
- Optionally publish to `algo.inbound.rejected`.

### Normalization Failure

If adapter normalization fails:

- Keep raw payload.
- Mark status as failed normalization.
- Publish or store a failed-normalization record.
- Include enough metadata for replay after adapter fixes.

### EventBus Publish Failure

If publishing fails:

- Mark status as `PUBLISH_FAILED`.
- Retry according to configured retry policy.
- Move to DLQ after retry exhaustion.
- Preserve the raw request reference and canonical event attempt.

### Downstream Failure

Downstream service failures should be handled by each subscriber using retries and DLQs. The Gateway should not be responsible for direct downstream recovery after the event has been published.

## Idempotency

External systems may retry requests. The Gateway needs an idempotency strategy to avoid duplicate business processing.

Possible idempotency keys:

- `sourceEventId` from the source system.
- Explicit `Idempotency-Key` header.
- Source system + store ID + event type + source event ID.
- Hash of selected stable raw request fields if no source ID exists.

The deduplication decision should happen before publishing the canonical event.

## Observability

The Gateway should emit structured logs, metrics, and traces using `eventId` and `correlationId`.

Recommended metrics:

- Requests received.
- Requests by source system.
- Raw store success/failure.
- Validation success/failure.
- Normalization success/failure.
- Publish success/failure.
- Retry count.
- DLQ count.
- End-to-end latency from request received to event published.

Recommended log fields:

- `eventId`
- `correlationId`
- `sourceSystem`
- `eventType`
- `storeId`
- `validationStatus`
- `publishStatus`
- `rawRequestRef`

## Security And Privacy

Security details are intentionally low detail for now, except for the explicit Auth Service gap.

Minimum design considerations:

- Auth Service method must be selected before production.
- Payload size limits should be enforced.
- Rate limits should be configured per source system.
- Raw payload storage may contain PII and should have retention, access control, and encryption rules.
- Logs should avoid dumping full raw payloads.
- Secrets or auth tokens from headers should not be stored in plain text.

## Open Decisions

| Decision | Current Status |
| --- | --- |
| Auth Service method | Missing / TBD |
| HTTP acknowledgement status | `200 OK` or `202 Accepted`, either is acceptable for now |
| Raw storage technology | TBD |
| EventBus technology | TBD |
| Topic naming standard | Proposed but not final |
| Invalid request behavior | Decide between rejected-event publication or audit-only |
| Retention policy for raw data | TBD |
| Idempotency key source | TBD |

## Recommended Next Steps

1. Choose the Auth Service method for external systems.
2. Choose raw request storage and retention policy.
3. Finalize the canonical event envelope.
4. Finalize the first set of business event types.
5. Define retry and DLQ policy.
6. Define the first source adapter contract.
7. Create one sequence diagram per critical business flow.

## Notes From Existing Algo Code

The current Algo repository suggests these conventions:

- Messaging is JSON-based.
- RabbitMQ routing keys include brand, country, external store ID, and version context.
- Existing message bodies are source-specific, with no single universal body.
- Existing queue suffixes include `quote`, `eta`, `cloud-orders`, `cabinet`, `courier-location`, and `vehicles-updates`.
- A stable envelope with variable `data` payload is a good fit for Algo 4 because it preserves source flexibility while giving internal services consistent metadata.
