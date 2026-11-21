# autobee-rs

A Rust port of npm [`autobee`](https://www.npmjs.com/package/autobee) **2.12.1**
— multi-writer views over hypercores — on [`hyperbee-rs`](../hyperbee-rs)
(hyperbee2 **2.21.4**), [`hypercore-rs`](../hypercore-rs) and
[`corestore-rs`](../corestore-rs).

One `Autobee` is one writer's materialised state: its local oplog, its own
`system` and `view` cores under `autobee/<local>/view/...`, the remote writers
it follows, and the drain loop that applies their batches in the reference's
deterministic order. Multi-value appends enter the local writer queue atomically,
so the drain cannot split a batch. Only the driver task drains, as the
reference's detached `_draining` promise does: every append, wakeup and update
bumps one counter and awaits the drain that consumes it, and dropping such a
future never aborts a drain halfway. Hints arriving on their own start it too.
A drain error other than an application `interrupt()` closes the session, as
the reference does: the failed batch is neither persisted nor resumed, so
reopen to recover. An `interrupt()` keeps the session open on every path:
`interrupted()` returns its reason, `last_error()` reports `Apply interrupted`,
the interrupted call returns that error and no later drain runs. `close()`
closes the sessions before it waits for the driver, as `_teardown` does, so a
drain parked on a block no peer serves fails and ends.

Peers interoperate live: the system info, view rows and oplog bytes converge
across the two implementations, a reopened peer resumes its session, and
either implementation can own a channel the other writes to. The oplog blocks,
system blocks, encodings, topology order, fast-forward landing and writer
records are byte-identical to the reference.

## Modules

The module layout follows the reference's files:

| module | reference |
| --- | --- |
| `base` | `index.js`: `Autobee`, the session and its drain loop |
| `apply_view` | `lib/apply-view.js` and the `Handlers` trait an application implements |
| `apply_calls` | `lib/apply-calls.js`: the host an apply handler calls back through |
| `writers` | `lib/writers.js`: active writers, pending queue, coupler |
| `system` | `lib/system.js`: system tree, writer records, system info |
| `topo` | `lib/topo.js`: deterministic ordering and replay of oplog batches |
| `witness` | `lib/witness.js`: witness weight resolution |
| `promotions` | `lib/promotions.js`: grant hints and pending promotions |
| `boot` | `lib/boot.js`: local writer, bootstrap core, stored boot record |
| `fast_forward` | `lib/fast-forward.js`: the boot-time landing search |
| `trusted` | `lib/trusted.js`: the fast-forward trust cache |
| `migrations` | `lib/migrations.js`, reduced to the boot-time decision |
| `encryption` | `lib/encryption.js`: system, view and writer encryption |
| `encoding` | `lib/encoding.js` and `encoding/legacy.js` |
| `order_hash` | `advanceHash` from `lib/system.js` |
| `updates` | `lib/updates.js` |
| `types` | the objects the modules pass around |
| `constants` | `lib/constants.js` |
| `core_coupler` | `core-coupler`: coupled-core announcements |
| `wakeup` | `autobee-wakeup`: one wakeup session per capability |

## Compatibility evidence

Every fixture is a recording of npm autobee 2.12.1 on `hyperbee2` 2.21.4,
`corestore` 7.16.1 and `hypercore` 11.40.3, not a Rust-side expectation.

- `fixtures/encoding-reference.json`: all 97 schema types at every version the
  reference can encode, plus 57 exported helper calls (value padding and
  envelopes, the legacy decode-only types).
- `fixtures/encryption-reference.json`: the constants, 122 encryption cases
  across the system, view and writer modes, 10 padding/envelope anchors and 60
  order-hash transitions.
- `fixtures/boot-reference.json`: 13 boot scenarios.
- `fixtures/append-batch-reference.json`: one `append([a, b, c])` call, its
  three oplog blocks and the batch coordinates the handler receives.
- `fixtures/units-reference.json`: 15 trigger steps, 3 genesis and 15 trust
  steps, 3 change scenarios and 10 apply-call steps. The trigger steps drive
  `Writer::waiting` and `ActiveWriters::notify_batch`, where `writers` folds
  `lib/triggers.js`.
- `fixtures/traces/`: 7 recorded sessions (`single-writer`,
  `concurrent-writers`, `add-remove`, `optimistic`, `reorder`, `late-joiner`,
  `fast-forward-landing`) holding 169 flushed batches, 169 hash transitions,
  169 apply calls and 30 topo calls. They are replayed as live sessions over
  in-process streams (`tests/autobee_traces.rs`), as system flushes with their
  KV operations and physical blocks including recorded undos and a boot onto
  another peer's core (`tests/system_traces.rs`), as topo sort/linearize calls
  (`tests/topo_traces.rs`), and as the fast-forward landing, its download
  presence and the landed state (`tests/fast_forward.rs`).

Two tests drive the reference's own JavaScript against this crate:

- `tests/interop.rs`: npm autobee 2.12.1 and this crate as peers on one local
  HyperDHT testnet. The JS owner grants the Rust writer, both append, and both
  sides converge on the system heads and hash, the view rows and the oplog
  bytes; the Rust side then reopens the same storage and resumes, and finally
  the roles swap.
- `tests/wakeup.rs`: npm `autobee-wakeup` 1.2.1 and `wakeup` on one
  capability; each side answers the other's lookup and the answer lands in the
  other's hint sink.

Both are `#[ignore]` because they need `node` and `npm install` in `tests/js`;
they run under `--include-ignored`.

## Not ported

Deliberate cuts, refused rather than half-ported:

- Legacy autobase storage: a system tree holding pre-v3 info is rejected as
  `LEGACY_AUTOBASE_STORAGE` instead of migrated.
- Named sessions and `signedLength`: peers announce `{key, length}`; the JS
  session names and the autobase-compat `signedLength` field are not exposed.
- `_bootFromNetwork`: booting a legacy manifest by waiting for its head to
  arrive over the network.
- `setLocal`: a session has one local writer for its lifetime.
- `compactMaybe`/reindexing: every core this crate opens uses manifest version
  3, so the compaction path is unreachable.
- `moveTo`/`replay`: neither is on the writable path.
- Custom encryption provider: `Autobee::open` refuses a session that asks for
  encryption. The encryption codecs themselves are ported and fixture-tested.

## Deviations from npm

Each one bounds hostile input the reference accepts and then hangs or
starves on, or adopts a later upstream fix. None changes the bytes or the
order of a block the reference applies.

- Batch coordinates are checked at ingestion: the i-th block of a batch must
  say `batch.start == i` and the `batch.end` values must count down to 0. A
  block that lies freezes its writer, as a decode error does. The reference
  applies it and later hangs in the reorder walk (`core.get` before the oplog
  start); a system history that still names an unreadable batch is a drain
  error here, not an endless loop.
- Fast-forward candidates are resolved concurrently, at most 64 per search
  round; the rest wait for the next round. The reference resolves every
  announced candidate at once.
- `refresh` resets the local writer's `processed` count, as autobee 2.13
  `reset()` does: local nodes applied before a landing in the same drain are
  re-applied to the landed system before they are flushed. 2.12.1 flushes
  them with views that point at the replaced system.
- `close()` closes every session this crate opened but not the caller's
  `Corestore`; the reference closes the store it was given.
- A read-only store opens remote writer cores without joining the wakeup
  group, as the bootstrap core already does.
- Apply removing a writer (`removeWriter`) forgets that writer's cached
  fast-forward trust answer, so its next landing is judged again. The
  reference clears the trust cache only on an undo.

## Known reference flaws

Reproduced on purpose: a Rust-only fix would fork the views from JS peers.

- Approvals are folded into the system before `canApply` runs. A stranger's
  optimistic block that approves its own key, or a removed writer's block,
  admits that key as a writer (autobee 2.12.1 `index.js` `_applyBatch`,
  `lib/system.js` `addWriter`). The application `apply` must check the
  writer's role itself: drop a non-optimistic node whose `weight` is below 1
  and treat a removed writer as not writable.

## Fixtures and tests

Regenerate the fixtures from the npm reference (Node 22 or later):

```sh
cd fixtures/gen && npm install
node boot.mjs && node encoding.mjs && node encryption.mjs && node units.mjs
node append-batch.mjs && node record/run.mjs
```

`record/run.mjs` writes `fixtures/traces/*.json`, `append-batch.mjs` writes
the multi-append reference; the other scripts write the `*-reference.json`
files. The interop tests need their own install:

```sh
cd tests/js && npm install
```

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test                          # no node needed
cargo test -- --include-ignored     # adds the two live JS interop tests
```

The owner/writer convergence barrier requires each round's appended view rows, not just equal
peer snapshots: both peers may briefly agree on the old state while an append waits on its links.
