# {Service Name}

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

**Owner team:** {team name}
**Status:** Draft | In Review | Approved
**Last updated:** YYYY-MM-DD

---

## 1. Purpose

<!--
One paragraph (3–6 sentences) describing what this service is for in
business terms. Avoid implementation details. A non-engineer reading this
section alone should understand why the service exists and what problem it
solves for store operations.
-->

## 2. Scope

### In Scope

<!--
Bulleted list of responsibilities this service owns. Be specific — "manages
deliveries" is too vague; "assigns couriers to deliveries and tracks the
courier-side lifecycle from pickup to drop-off" is better.
-->

- …

### Out of Scope

<!--
Bulleted list of things this service explicitly does NOT do, especially
where another service might be expected to handle it. This is the most
important section for catching overlaps between teams.
-->

- …

## 3. Role in Store Operations

<!--
Describe how this service participates in the day-to-day store flows below.
Use plain language — describe the business behavior, not the wire protocol.
If a flow doesn't touch this service, write "_Not involved._"
-->

### Order Entry

<!-- What happens in this service when a new order enters the store? -->

### Order Modification / Cancellation

<!-- Order edits, item changes, customer cancellations, store cancellations. -->

### Kitchen Preparation & Bumping

<!-- Item show, prep, cook, bump events. What does this service do (if anything) as items move through the kitchen? -->

### Delivery / Handoff

<!-- Courier assignment, dispatch, customer-side outcomes (delivered, failed). For dine-in/pickup, describe handoff to the customer. -->

### Day Open / Day Close

<!-- Startup state, recovery after restart, end-of-day reconciliation. -->

### Exception Flows

<!-- Remakes, refunds, lost orders, courier failure, POS outage, store closures. -->

## 4. Events Consumed (Triggers)

<!--
One H3 subsection per event the service listens to. If the service consumes
no events, write "_None._" under this heading and skip the subsections.

For each event, describe what the service does on receipt in business
terms — what changes, what gets recomputed, what gets emitted downstream.
Do not describe the wire format here; that lives in the technical design.
-->

### `{event.name.here}`

**Source service:** {service that emits this event}
**Why we consume it:** {what business decision/action this event drives}
**Action on receipt:** {what this service does in response}
**Notes:** {idempotency, ordering assumptions, edge cases}

### `{event.name.here}`

**Source service:**
**Why we consume it:**
**Action on receipt:**
**Notes:**

## 5. Events Emitted

<!--
One H3 subsection per event the service publishes. If the service emits no
events, write "_None._"

State the event as a fact (e.g., "delivery.dispatched") not a command
("dispatch_delivery"). The "Expected consumers" line is what AI cross-check
agents use to find orphan events — list known consumer services, or
"_Unknown — to be discovered_" if the team hasn't identified them yet.
-->

### `{event.name.here}`

**Emitted when:** {business condition that triggers this event}
**Payload summary:** {key fields, in plain language — full schema lives in event contracts}
**Expected consumers:** {services known/expected to consume this}
**Notes:** {ordering guarantees, frequency, dedup keys}

### `{event.name.here}`

**Emitted when:**
**Payload summary:**
**Expected consumers:**
**Notes:**

## 6. Data Dependencies (External Reads)

<!--
The most important section for catching loose ends. List every piece of
information the service needs that does NOT arrive in a consumed event.

Example: an "order.placed" event might carry the order_id but not the line
items. If the service needs items to do its job, that's a data dependency
on Order Management.

One H3 subsection per piece of data. If none, write "_None — all required
data arrives in consumed events._"
-->

### {Data name — e.g., "Order line items"}

**Owning service:** {service that holds this data}
**When fetched:** {on each event / cached on startup / refreshed every N minutes}
**Why needed:** {what the service can't do without it}
**How fetched:** {sync API call / subscription / batch load — leave blank if undecided}
**Notes:** {staleness tolerance, fallback if owning service is down}

### {Data name}

**Owning service:**
**When fetched:**
**Why needed:**
**How fetched:**
**Notes:**

## 7. API Exposed to Other Services

<!--
One H3 subsection per operation this service offers to peers. Describe the
business capability, not the HTTP verb/path. "Get order line items by
order_id" is a business capability; "GET /orders/{id}/items" is an
implementation choice.

If no API surface (event-only service), write "_None — interaction is
event-driven only._"
-->

### {Operation name — e.g., "Get order line items"}

**Purpose:** {what business question this answers}
**Known consumers:** {services expected to call this}
**Inputs:** {what the caller must supply}
**Returns:** {what the caller gets back, in plain language}
**Notes:** {auth, rate-limit expectations, latency budget if known}

### {Operation name}

**Purpose:**
**Known consumers:**
**Inputs:**
**Returns:**
**Notes:**

## 8. Operational Assumptions

<!--
What must be true in the wider system for this service to function?
Examples:
- "Order Management is the sole emitter of order.status.changed"
- "Menu data is loaded into Menu Service before the store opens for the day"
- "Every order has at least one item by the time order.placed is emitted"

These are the assumptions other teams need to know about and not violate.
-->

- …

## 9. Open Questions

<!--
Anything the team is unsure about. Each question should be specific enough
that another team can either answer it or escalate. Reference DECISIONS.md
Q-IDs if a question has been logged there.
-->

- …
