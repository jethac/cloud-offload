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

## Authoring a spec over HTTP

Everything above is also reachable through the coordinator, so a spec can be
written, checked and installed without hand-editing files or restarting.

| Route | Does |
| --- | --- |
| `GET /api/providers/specs` | List user specs: `name`, `display_name`, `source`, `valid`, `problems`, `registered`, plus the `directory` they live in and the `auth_types` the engine supports. |
| `GET /api/providers/specs/{name}` | Return one spec's JSON. Falls back to the built-in spec of that name, marked `"builtin": true, "editable": false`. |
| `PUT /api/providers/specs/{name}` | Create or replace a user spec, then re-register it. |
| `DELETE /api/providers/specs/{name}` | Remove a user spec. |
| `POST /api/providers/specs/validate` | `{"valid": bool, "problems": [...]}`. Writes nothing, contacts nobody. |
| `POST /api/providers/specs/dry-run` | `{"spec": {...}, "api_key": "…"}` → the `dry_run_spec` result. |

The write route is deliberately unforgiving, in this order:

1. The name is canonicalized with `normalize_provider_name` — the same call the
   credentials, settings and test routes use, so `vast` means `vast.ai` here too
   — and must then be a bare file stem: lowercase alphanumerics plus `.`, `-`
   and `_`, 64 characters at most. Anything with a path separator, a leading dot
   or a space is a **400**, so a name off the wire can never address a file
   outside the spec directory.
2. The spec's own `name` must match the one in the URL.
3. `validate_spec` runs **before** anything touches disk, so an invalid spec is a
   **400** carrying `error.details.problems` and is never persisted to be
   rediscovered at next startup. That includes carrying a credential-shaped key
   (`api_key`, `token`, `Authorization`, …) anywhere in the spec: specs are
   shareable *because* they hold no secrets, and the API key belongs in
   `POST /api/providers/{provider}/credentials`. Because the rule lives in
   `validate_spec` rather than in the route, validate, save and the startup
   loader all give the same answer.
4. A spec that would shadow a coded connector is a **409** — the same rule
   `register_declarative_providers` applies at load time, exposed as
   `shadow_conflict()` so there is one copy of it rather than two.

On success the spec is written and `register_declarative_providers()` runs, so
the provider is routable through `create_connector()` immediately; the response
says whether it `registered`. Deletion is the asymmetric case: the registry has
no unregister, so a deleted spec keeps serving from memory and the response sets
`restart_required` rather than pretending the provider vanished. Built-in specs
are readable but not deletable (**409**).

`api_key` on the dry-run route is used for that one read-only probe. It is never
written to the credential file and never echoed back — if it appears in a
transport error message it is replaced with `***`. Omit it to reuse whatever
`config.api_key_for(<spec name>)` already resolves to.

## Authoring a spec from ComfyUI

The node pack's **Cloud Offload: Manage providers** command (command palette, or
the Cloud Offload settings category) lists the specs already installed and
carries an **Add REST provider** form: name, display name, base URL, auth type,
and a JSON textarea for the endpoint table and field mapping, prefilled from the
shipped Vast.ai spec so you start from a provider that actually works rather than
an empty box. The prefill is fetched live from `GET /api/providers/specs/vast.ai`
and the auth-type list from `GET /api/providers/specs`, so neither can drift from
what the engine actually supports.

**Validate** shows the problem list inline, **Dry run** reports the offer count
and the mapped sample, and **Save** writes the spec. The browser never talks to
the coordinator directly: ComfyUI proxies these under `/cloud_offload/providers/specs`
so the bearer token stays server-side, and nothing — credential or probe key — is
written to `comfy.settings.json`.

The form states, and means, that this covers REST/JSON providers only. A GraphQL
API, request signing or multi-step provisioning needs a connector plugin instead;
see the limits above.

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
