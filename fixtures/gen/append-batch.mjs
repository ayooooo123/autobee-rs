// Records npm autobee 2.12.1 appending three values in one `append()` call, so
// the Rust port can compare its oplog bytes and the batch coordinates its
// handler sees. Run with `npm install && node append-batch.mjs`; the Rust test
// in tests/autobee_traces.rs replays the same store identity and clock.

import fs from 'node:fs'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join, dirname } from 'node:path'
import { fileURLToPath } from 'node:url'
import { createRequire } from 'node:module'

const require = createRequire(import.meta.url)
const Corestore = require('corestore')
const Autobee = require('autobee')

const hex = (b) => (b == null ? null : Buffer.from(b).toString('hex'))
const OUT = join(dirname(fileURLToPath(import.meta.url)), '..', 'append-batch-reference.json')
const PRIMARY_KEY = Buffer.alloc(32, 'a'.charCodeAt(0))
const CLOCK = 1700000001000

await (async () => {
  const root = await mkdtemp(join(tmpdir(), 'autobee-append-'))
  const store = new Corestore(join(root, 'a'), { primaryKey: PRIMARY_KEY, unsafe: true })
  const applies = []
  const db = new Autobee(store, null, {
    name: 'a',
    optimistic: true,
    now: () => CLOCK,
    async apply (batch, view) {
      applies.push(batch.map((n) => ({
        key: hex(n.key),
        length: n.length,
        batch: { start: n.batch.start, end: n.batch.end },
        value: hex(n.value)
      })))
      const w = view.write()
      for (const node of batch) {
        const op = JSON.parse(node.value.toString())
        w.tryPut(Buffer.from(op.key), Buffer.from(op.value))
      }
      await w.flush()
    }
  })
  try {
    await db.ready()
    await db.append([
      Buffer.from(JSON.stringify({ type: 'put', key: 'k1', value: 'v1' })),
      Buffer.from(JSON.stringify({ type: 'put', key: 'k2', value: 'v2' })),
      Buffer.from(JSON.stringify({ type: 'put', key: 'k3', value: 'v3' }))
    ])
    await db.flush()

    const blocks = []
    for (let i = 0; i < db.local.length; i++) blocks.push(hex(await db.local.get(i)))

    const out = {
      schema: 1,
      scenario: 'multi-append',
      primaryKey: hex(PRIMARY_KEY),
      clock: CLOCK,
      writer: hex(db.local.key),
      oplog: { key: hex(db.local.key), blocks },
      apply: applies
    }
    fs.writeFileSync(OUT, JSON.stringify(out, null, 2) + '\n')
    console.log('wrote', OUT, blocks.length, 'blocks')
  } finally {
    await db.close()
    await store.close()
    await rm(root, { recursive: true, force: true })
  }
})()
