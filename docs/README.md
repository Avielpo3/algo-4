# Algo 4 — Documentation Index

This folder holds the business-side design docs and diagrams for Algo 4. Code
lives elsewhere; this is the source of truth for **what** each service does,
**why** it exists, and **how** events flow between services.

---

## Start here

| If you want to … | Read |
|---|---|
| Understand the topic model in 30 seconds (the "what & why") | [`topics-overview.md`](topics-overview.md) |
| Look up the exact name / payload / partition key of a topic | [`event-taxonomy.md`](event-taxonomy.md) |
| Author a new service doc | [`../TEMPLATE.md`](../TEMPLATE.md) |
| Understand the project's HLD intent | [`../CLAUDE.md`](../CLAUDE.md) |

---

## Folder layout

```text
docs/
├── README.md                              ← you are here
├── topics-overview.md                     ← cross-cutting: topic model, decisions, anti-patterns
├── event-taxonomy.md                      ← cross-cutting: topic catalog (every name, every status)
│
├── inbound-api-gateway/                   ← external systems → Algo
│   ├── README.md                          ← service doc (Purpose, Scope, Events, Data, API)
│   ├── architecture.mmd                   ← top-level architecture
│   ├── flow-pos-order-insert.mmd          ← POS order insert sync flow (OM + AI Promise Time)
│   └── flow-pos-reconciliation.mmd        ← POS reconciliation / status-poll read-through (no bus event)
│
├── outbound-cloud-gateway/                ← Algo → external systems
│   ├── README.md                          ← service doc
│   ├── hld.md                             ← HLD for Cloud Gateway
│   └── architecture.mmd                   ← top-level architecture
│
└── plans/                                 ← dated implementation plans
    └── 2026-05-14-inbound-api-gateway.md
```

---

## Services

| Service | Direction | Folder |
|---|---|---|
| Inbound API Gateway | external → Algo (POS, Labor, VIP, Cabinet, Cloud Orders) | [`inbound-api-gateway/`](inbound-api-gateway/) |
| Outbound Cloud Gateway | Algo → external (DaaS providers, DragonDrive mobile, Admin Panel) | [`outbound-cloud-gateway/`](outbound-cloud-gateway/) |

---

## Conventions

- **One folder per service.** All prose, architecture, and flow diagrams for a
  service live in its own folder.
- **`README.md` is the canonical service doc.** It follows
  [`../TEMPLATE.md`](../TEMPLATE.md) and is the entry point when a folder is
  opened.
- **`architecture.mmd`** is the top-level diagram for that service.
- **`flow-*.mmd`** files show concrete cross-service flows (one flow per file).
- **Cross-cutting docs** (topic model, taxonomy, plans) live at the root of
  `docs/`, not inside a service folder.
- **Topic names** follow `{domain}.{lifecycle}.{event-name}`, **`snake_case`**
  within each level. Six entity domains: `orders`, `courier`, `delivery`,
  `customer`, `store`, `admin`. See [`topics-overview.md`](topics-overview.md)
  for the model and [`event-taxonomy.md`](event-taxonomy.md) for the full
  catalog.

---

## Adding a new service

1. Create `docs/{service-name}/`.
2. Copy [`../TEMPLATE.md`](../TEMPLATE.md) to `docs/{service-name}/README.md`
   and fill it in.
3. Add an `architecture.mmd` for the service's top-level diagram.
4. Add `flow-*.mmd` files for each cross-service flow worth illustrating.
5. Add the service to the **Services** table above.
6. If the service introduces new topics, add them to
   [`event-taxonomy.md`](event-taxonomy.md) §6 (the catalog) and verify they
   follow the model described in [`topics-overview.md`](topics-overview.md).
