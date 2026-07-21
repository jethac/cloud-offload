# Declarative providers

Cloud Offload can talk to a cloud it has never heard of without anyone writing
Python. You describe the provider's REST API in a JSON **spec**; the built-in
`DeclarativeRestConnector` reads that spec, issues the requests, and maps the
responses onto the two shapes the rest of the system understands — the offer
dict and the `Instance` dataclass.

Vast.ai is the proof this is real, not a toy: it is served entirely by
`cloud_offload/providers/specs/vast.json`. It used to have a hand-written
connector; that was deleted, and `tests/test_declarative.py` asserts the spec
still reproduces its output request for request and field for field, against
golden values captured from it at cutover.

## Where specs live

| Location | Purpose |
| --- | --- |
| `cloud_offload/providers/specs/*.json` | Shipped with the package, registered by default. |
| `~/.cloud-offload/providers/*.json` | Yours. Dropped in by hand or written by the settings UI. |

Both are loaded by `register_declarative_providers()`, which the plugin loader
calls at startup. Built-ins load first; a user spec with the same name wins.
Invalid files are reported and skipped — one bad spec never stops the
coordinator from starting.

A spec is **refused** rather than allowed to shadow a coded connector of the same
name, so a stray `runpod.json` cannot silently replace the real RunPod
connector.

## Read this first: what a spec cannot do

The declarative path deliberately covers only the common case. If your provider
needs any of the following, write a `CloudConnector` subclass instead — that is
exactly why RunPod has one and always will:

- **GraphQL.** JSON over REST only.
- **Multi-step provisioning.** One request per operation, plus the optional
  `wait_for` polling loop. "Reserve, then configure, then start" needs code.
- **Custom request signing.** Bearer, custom header, query parameter and HTTP
  Basic only. AWS SigV4 and friends need code.
- **Pagination**, retry/backoff policies, websockets or streaming.
- **Arbitrary response transformation.** You get scale, unit, type and default —
  nothing that amounts to a scripting language.

Credentials are never part of a spec. They come from
`config.api_key_for(<name>)`, so a spec file is safe to share or commit.

## Spec reference

```jsonc
{
  "spec_version": 1,
  "name": "acme",                       // canonical connector name
  "aliases": ["acme-gpu"],              // optional registry aliases
  "display_name": "Acme GPU",
  "base_url": "https://api.acme.dev/v1",
  "base_url_config_field": "acme_api_url",  // optional CloudConfig field to honour
  "auth": {"type": "bearer"},
  "headers": {"User-Agent": "cloud-offload/0.1"},
  "timeout": 30,
  "client_filter": true,                // post-filter offers against the caller's caps
  "include_raw": true,                  // keep the provider payload on each offer
  "status_map": {"provisioning": "pending", "active": "running"},
  "default_status": "unknown",
  "settings_schema": [ ... ],           // optional; rendered by the settings UI
  "endpoints": { ... }
}
```

### Auth

| `type` | Effect |
| --- | --- |
| `bearer` (default) | `Authorization: Bearer <key>`. `name` and `prefix` are overridable. |
| `header` | `<name>: <prefix><key>`. `name` required. |
| `query` | `?<name>=<key>`. `name` required. |
| `basic` | `Authorization: Basic …`. `in` is `username` (default) or `password`; the other half comes from `username`. |
| `none` | No credential required at all. |

### Endpoints

`offers` is mandatory; `launch`, `get`, `list`, `terminate` and `balance` are
optional and their absence is reported clearly when called.

```jsonc
"offers": {
  "method": "GET",
  "path": "bundles",                 // joined to base_url, may contain {{vars}}
  "query": {"owner": "me"},          // templated query parameters
  "body": { ... },                   // templated JSON body
  "items": "$.offers",               // where the collection lives
  // ...or, to tolerate a 200 that omits it entirely:
  // "items": {"path": "$.offers", "default": []},
  "select": {"where": "$.state", "in": ["active"]},
  "map": { ... },
  "on_error": "empty"
}
```

### Template variables

`{{offer_id}}`, `{{instance_id}}`, `{{docker_image}}`, `{{env_vars}}`,
`{{startup_script}}`, `{{gpu_type}}`, `{{min_gpu_ram}}`, `{{max_hourly_rate}}`,
`{{provider}}`.

A string that is *exactly* one placeholder keeps the value's type
(`"{{min_gpu_ram}}"` yields the number `24`); a placeholder inside a longer
string is stringified. Unknown variable names are a validation error, so typos
surface before a request is ever made.

Three directives shape structured values:

| Directive | Meaning |
| --- | --- |
| `{"$json": {...}}` | Render the inner value, then JSON-encode it. This is how Vast.ai's filter DSL is passed as `?q=<json>`. |
| `{"$when": "min_gpu_ram", ...}` | Drop the whole containing object unless that variable is truthy. |
| `{"$value": "min_gpu_ram", "scale": 1024, "type": "int", "omit_if": "empty"}` | Emit a typed, optionally scaled value. `omit_if` is `null` (default), `empty` or `never`. |

Together they express "add this filter only if the caller asked for it, in the
provider's units":

```jsonc
"gpu_ram": {"$when": "min_gpu_ram", "gte": {"$value": "min_gpu_ram", "scale": 1024}}
```

### Mapping responses

Paths are a small JSONPath subset implemented in-tree — no new dependencies:
`$.data.items`, `$.gpu.vram_gb`, `$.offers[0].id`, or plain `data.items`.

A map entry is a path string or an object:

```jsonc
"gpu_ram_gb": {"path": "$.gpu_ram", "unit": "MB->GB", "default": 0}
"hourly_rate": {"path": "$.price_cents", "scale": 0.01, "type": "float"}
"id":          {"path": "$.id", "type": "str", "required": true}
"kind":        {"const": "on-demand"}
```

- `scale` multiplies a numeric value; `unit` is sugar for a known scale
  (`MB->GB`, `GB->MB`, `bytes->GB`, `cents->USD`, `per_minute->per_hour`, …).
- `type` coerces to `str` / `int` / `float` / `bool`.
- A missing value or JSON `null` falls back to `default`; with no default the key
  is present and `None`, so mapped shapes stay stable.
- `required: true` turns a missing value into a clear error instead of silent
  `None` — use it for IDs.

For `get`/`list`/`launch` the map names `Instance` fields (`id`, `status`,
`gpu_type`, `gpu_count`, `hourly_rate`, `ip_address`, `ssh_port`) plus a nested
`metadata` sub-map. `status` is the raw provider value; `status_map` normalizes
it to `pending` / `running` / `stopped` / `terminated`, falling back to
`default_status`.

### Selecting from a collection

Some providers have no fetch-by-id route — Vast.ai does not. `select` picks one
entry out of a listing, and filters `list` results:

```jsonc
"get":  {"items": "$.instances",
         "select": {"where": "$.id", "equals": "{{instance_id}}", "type": "str"}}
"list": {"items": "$.instances",
         "select": {"where": "$.actual_status", "in": ["running", "loading"]}}
```

### Waiting for readiness

Many providers return `provisioning` and flip to `running` seconds later. Put
`wait_for` on `launch` and the connector polls the `get` endpoint for you:

```jsonc
"wait_for": {"status": "running", "timeout_seconds": 300, "interval_seconds": 5}
```

It raises `TimeoutError` if the deadline passes. A `get` endpoint is required;
validation says so if you forget.

### Error policy

Errors are loud by default — a failed request, a non-JSON body or a missing
`required` field raises with a message naming the operation, method and URL.
Read endpoints may opt into degrading instead, which is how most hand-written
connectors behave so that one unreachable provider does not abort routing across
all of them:

| Endpoint | `on_error` | Result |
| --- | --- | --- |
| `offers`, `list` | `empty` | Log a warning, return `[]`. |
| `get` | `null` | Log a warning, return `None`. |

`terminate` always reports failure as `False`. `balance` always raises.

## Validating and debugging a spec

```python
from cloud_offload.providers.declarative import validate_spec, dry_run_spec

validate_spec(spec)                       # [] means valid; otherwise a list of problems
dry_run_spec(spec, api_key="…")           # issues ONE read-only offers request
```

`dry_run_spec` returns `{"ok", "offer_count", "sample", "error", "problems"}`.
It never provisions anything and never spends money — it is there so you can
debug a spec without renting a GPU. Pass `http=` to inject a client in tests.

## Worked example

The shipped Vast.ai spec is the worked example. To start a new provider from
it, copy it under your own name and edit — a spec may not shadow a provider
that already exists, so rename it first:

```bash
cp cloud_offload/providers/specs/vast.json ~/.cloud-offload/providers/acme.json
# edit: set "name": "acme", drop "aliases", point base_url at your provider
export CLOUD_OFFLOAD_ACME_API_KEY=…
```

It exercises every primitive at once: bearer auth, a JSON-encoded query
parameter carrying a filter DSL, conditional filters, an MB→GB unit conversion,
a templated `PUT` path, optional body fields that vanish when empty, readiness
polling after launch, select-from-collection because there is no by-id route,
and a status map.
