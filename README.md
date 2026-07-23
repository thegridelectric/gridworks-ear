# Gridworks Ear

[![Tests](https://github.com/thegridelectric/gridworks-ear/workflows/Tests/badge.svg)][tests]

[tests]: https://github.com/thegridelectric/gridworks-ear/actions?workflow=Tests

Every message on a GridWorks RabbitMQ broker is sent by an actor — a
[GNode](https://gnr.electricity.works/docs) — and names its payload's
[Sema](https://schemas.electricity.works) TypeName in the routing key. The
ear is the audit witness for that traffic: it binds a queue with routing
key `#` to one exchange and writes every message it hears, byte for byte,
into an S3-compatible object store. It reads exactly two things from each
routing key — who spoke and what type they claimed — and deliberately
validates nothing. Meaning lives in Sema, not here: the store's job is to
preserve what was actually said, and an archive that kept only
schema-valid messages would discard exactly the utterances an audit exists
to catch. Because TypeNames are versioned and declared externally, the
archive stays interpretable as the schemas — and the system's
understanding — evolve.

The ear has no state beyond a local retry cache: its output *is* the
store.

## The routing-key pattern

GridWorks routing keys embed the sender and the type. They come in a few
categories with different shapes — the full grammar, the routing-class
table, and the exchange/binding mechanics live in the gridworks-base
README's [Message transport](https://github.com/thegridelectric/gridworks-base#message-transport)
section. The broadcast (`rjb`) shape:

```
rjb.<from-alias>.<from-rc>.<type-name>[.<radio-channel>]
rjb.hw1-isone-me-versant-keene.mm.latest-price
```

GNode aliases and TypeNames are dotted words, and RabbitMQ reserves the
dot as its topic separator — so within a routing-key word, dots render as
dashes. The ear restores them: the example above is the MarketMaker GNode
`hw1.isone.me.versant.keene` broadcasting a `latest.price`. Those two
recovered strings — `from_alias` and `type_name` — become part of the
object key below; the payload itself passes through untouched.

One category varies the shape: `gw` messages, MQTT-bridged from scadas —
e.g. `gw.hw1-isone-me-versant-keene-beech-scada.to.ltn.power-watts`, the
beech scada sending `power.watts` up to its LTN. The third word is the
literal `to`, and the body is an outer envelope type carrying a header
and a payload. The TypeName in the routing key — and therefore in the
ear's object key — is the **inner** payload's TypeName, while the stored
bytes are the whole envelope.

## The instances

Two production instances run from this one codebase, distinguished only by
their `.env`:

| instance | exchange | store |
|---|---|---|
| **universal ear** | `ear_tx` (everything on the broker) | `gwdev` bucket, AWS S3 |
| **gnr seed ear** | `gnr_ear_tx` (the Grid Node Registry slice: create/re-parent commands, ack/nack verdicts, forest broadcasts) | `gw-seedstore` bucket, Backblaze B2 |

## How it stores data

One message becomes one object. The body is stored **verbatim** — the ear
does not parse, re-serialize, or wrap the payload; the bytes published on
the broker are the bytes in the store.

Object key grammar:

```
<world_instance_alias>/eventstore/<YYYYMMDD>/<from_alias>-<type_name>-<epoch_ms>-<service_alias>.json
```

- `world_instance_alias` — the world this ear witnesses (`hw1__1` in
  production); constant for the life of an instance.
- `YYYYMMDD` — UTC day folder; the ear rolls to a new folder at the UTC
  day boundary.
- `from_alias` — the publisher's GNode alias, recovered from the routing
  key.
- `type_name` — the payload's claimed Sema TypeName, recovered from the
  routing key, dots preserved.
- `epoch_ms` — the ear's receipt time in unix milliseconds.
- `service_alias` — the witnessing ear's own service alias
  (`EAR_G_NODE_ALIAS`): `hw1.ear` for the universal ear, `hw1.gnr.ear`
  for the seed ear. One identity per instance, independent of which
  machine it runs on — which ear wrote an object is readable from the key
  alone. (Objects written before 2026-07 carry the witness's fqdn,
  `ear.electricity.works`, in this slot — same grammar, earlier identity
  convention.)
- Extension is `.json` for JSON-serialized message categories; the legacy
  `RabbitGwSerial` category gets `.txt`.

Illustrative keys (one from each instance):

```
hw1__1/eventstore/20260722/hw1.isone.me.versant.keene.scada-report-1753142400123-hw1.ear.json
hw1__1/eventstore/20260722/hw1.isone-g.node.create.cmd-1753142398021-hw1.gnr.ear.json
```

The object body of the second is exactly the `g.node.create.cmd` JSON the
registry client published — a reader needs no ear-specific decoding, just
the sema type.

If an S3 put fails, the message is written instead to the local cache
(`~/.local/share/gridworks/ear/output/need_to_put/<world_instance_alias>/`)
so it is not silently dropped; recovery from that cache is a manual
operator step.

## Configuration

Settings come from the environment and/or a `.env` found by walking up
from the working directory. Prefix `EAR_`, nested delimiter `__`. The main
wires:

| variable | default | meaning |
|---|---|---|
| `EAR_RABBIT__URL` | local dev broker | AMQP url of the broker to witness |
| `EAR_CONSUME_EXCHANGE` | `ear_tx` | the exchange the queue binds (`#`) |
| `EAR_G_NODE_ALIAS` | `d1.ear` | this witness's service alias (left-right-dot): drives the queue name and is the last segment of every object key, so co-resident instances must differ |
| `EAR_WORLD_INSTANCE_ALIAS` | `d1__1` | first segment of every object key |
| `EAR_S3__BUCKET_NAME` | `gwdev` | target bucket |
| `EAR_S3__ENDPOINT_URL` | empty | empty = AWS S3; set to aim the same boto3 client at any S3-compatible host (e.g. `https://s3.us-east-005.backblazeb2.com`) |
| `EAR_S3__PROFILE_NAME` | `default` | credentials profile in the login's `~/.aws/credentials` |

Filled-in templates for both production instances are in
[`service/`](service/).

## CLI

- `ear config` — print the resolved settings and directories.
- `ear listen` — run the ear (`--no-s3` to consume without storing).
- `ear dummy` — publish periodic test status messages (dev broker only).

Logs: `~/.local/state/gridworks/ear/log/state.txt` (service state) and
`message.txt` (per-message), both rotating.

## Development

```
uv sync
uv run pre-commit run --all-files
uv run pytest
```

The test suite needs a local dev RabbitMQ broker; set one up per the
[gridworks-base README](https://github.com/thegridelectric/gridworks-base?tab=readme-ov-file#dev-rabbit-broker).
Without a broker the suite self-skips.

## Deployment

One login per instance, each with its own clone, `.env`, venv, logs, and
aliases:

| login | unit | aliases |
|---|---|---|
| `ear` | [`service/ear.service`](service/ear.service) | [`service/ear_bash_aliases`](service/ear_bash_aliases) (`earstart` `earstop` `earrestart` `earstatus` `earlog`) |
| `gnrear` | [`service/gnr-ear.service`](service/gnr-ear.service) | [`service/gnr_ear_bash_aliases`](service/gnr_ear_bash_aliases) (`gnrearstart` … `gnrearlog`) |

Per instance: clone at `~/gridworks-ear` (clean pushed SHA only), `uv sync
--frozen`, copy the unit to `/etc/systemd/system/`, `.env` from the
matching `service/template.*.env`, S3 credentials in the login's
`~/.aws/credentials`, aliases sourced from `~/.bashrc` with a matching
narrow sudoers drop-in. Which box, DNS, and secrets are operational
matters recorded in the private `gridworks-infra` repo, not here.

## License

Distributed under the terms of the [MIT license][license],
_Gridworks Ear_ is free and open source software.

[license]: https://github.com/thegridelectric/gridworks-ear/blob/main/LICENSE
