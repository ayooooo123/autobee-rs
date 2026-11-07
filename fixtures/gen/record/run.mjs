import { createRequire } from 'node:module'
import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join, dirname } from 'node:path'
import { fileURLToPath } from 'node:url'
import assert from 'node:assert/strict'

const require = createRequire(import.meta.url)
const Corestore = require('corestore')
const topo = require('autobee/lib/topo.js')
const System = require('autobee/lib/system.js')
const FastForward = require('autobee/lib/fast-forward.js')
const TrustedPeers = require('autobee/lib/trusted.js')
const encoding = require('autobee/lib/encoding.js')
const Autobee = require('autobee')
const out = join(dirname(fileURLToPath(import.meta.url)), '../../traces')
let clock = 1700000000000
Date.now = () => clock
let current
const hex = b => b == null ? null : Buffer.from(b).toString('hex')
const clean = x => {
  if (x == null || typeof x !== 'object') return x
  if (Buffer.isBuffer(x) || x instanceof Uint8Array) return hex(x)
  if (Array.isArray(x)) return x.map(clean)
  const result = {}
  for (const [k, v] of Object.entries(x)) if (!['core', 'from'].includes(k) && typeof v !== 'function') result[k] = clean(v)
  return result
}
const node = n => n === null ? null : clean(Object.fromEntries(Object.entries(n).filter(([k]) => ['key', 'length', 'weight', 'version', 'timestamp', 'links', 'batch', 'views', 'value', 'hash', 'witness', 'optimistic', 'approvals'].includes(k))))
const nodes = batch => batch.map(node)
const activeCandidates = new WeakMap()
const isTrusted = TrustedPeers.prototype.isTrusted
TrustedPeers.prototype.isTrusted = async function (key, reference) {
  const candidate = activeCandidates.get(this.auto)
  const trusted = await isTrusted.call(this, key, reference)
  if (candidate) candidate.decisions.push({ key: hex(key), trusted })
  return trusted
}
const fromHeads = FastForward.fromHeads
FastForward.fromHeads = async function (auto, heads, options) {
  const candidate = current.fastForward && { peer: auto.name, heads: clean(heads), decisions: [], selected: null }
  if (candidate) {
    current.fastForward.candidates.push(candidate)
    activeCandidates.set(auto, candidate)
  }
  let result
  try { result = await fromHeads.call(this, auto, heads, options) } finally {
    if (candidate) activeCandidates.delete(auto)
  }
  if (candidate) candidate.selected = result ? clean(result.head) : null
  return result
}
const originalSort = topo.sort
const originalLinearize = topo.linearize
topo.sort = async (auto, batch) => {
  const input = nodes(batch)
  const result = await originalSort(auto, batch)
  current.topology.push({ peer: auto.name, method: 'sort', batches: [input], undo: clean(result.undo), view: clean(result.view), list: result.tip.map(nodes) })
  return result
}
topo.linearize = batches => {
  const input = batches.map(nodes)
  const result = originalLinearize(batches)
  current.topology.push({ peer: null, method: 'linearize', batches: input, undo: null, view: null, list: result.map(nodes) })
  return result
}
const addNode = System.prototype.addNode
System.prototype.addNode = function (n) {
  const prev = hex(this.hash)
  const input = node(n)
  const result = addNode.call(this, n)
  current.hashes.push({ peer: this.auto.name, prev, node: input, result: hex(this.hash) })
  return result
}
async function blocks (core) {
  const result = []
  for (let i = 0; i < core.length; i++) result.push(hex(await core.get(i)))
  return { key: hex(core.key), blocks: result }
}
const flush = System.prototype.flush
System.prototype.flush = async function (batch, bee) {
  const puts = []
  const preHead = clean(this.bee.head())
  const updates = clean([...this.updates.values()])
  const write = this.bee.write
  this.bee.write = function (...args) {
    const w = write.apply(this, args)
    for (const method of ['tryPut', 'tryDelete']) {
      const original = w[method]
      w[method] = function (key, value) {
        puts.push({ op: method === 'tryPut' ? 'put' : 'delete', key: hex(key), ...(method === 'tryPut' ? { value: hex(value) } : {}) })
        return original.apply(this, arguments)
      }
    }
    return w
  }
  try {
    const result = await flush.call(this, batch, bee)
    const info = await this.bee.get(Buffer.from([0]))
    current.flushes.push({ peer: this.auto.name, batch: nodes(batch), preHead, updates, puts, info: info ? clean(encoding.decodeSystemInfo(info.value)) : null, system: await blocks(this.bee.context.local), view: await blocks(bee.context.local), systemHead: clean(this.bee.head()), viewHead: clean(bee.head()) })
    return result
  } finally { this.bee.write = write }
}

async function scenario (name, run) {
  clock = 1700000000000
  current = { schema: 1, scenario: name, peers: {}, steps: [], topology: [], hashes: [], flushes: [], apply: [] }
  const root = await mkdtemp(join(tmpdir(), 'autobee-trace-'))
  const peers = []
  const streams = []
  try {
    async function peer (id, bootstrap = null, options = {}) {
      const store = new Corestore(join(root, id), { primaryKey: Buffer.alloc(32, id.charCodeAt(0)), unsafe: true })
      const db = new Autobee(store, bootstrap && bootstrap.db.key, {
        name: id, fastForward: options.fastForward ?? false, optimistic: true, now: () => clock,
        ...(options.trustOwner ? { async isTrusted (key, reference) {
          const writer = hex(key)
          let trusted = writer === hex(db.key)
          if (!trusted && reference) {
            const revoked = await reference.get(Buffer.from('revoked/' + writer), { timeout: 5000 })
            const record = await reference.get(Buffer.from('writer/' + writer), { timeout: 5000 })
            trusted = !revoked && !!record && record.value.toString() === 'owner'
          }
          current.fastForward.trust.push({ peer: id, key: writer, trusted })
          return trusted
        } } : {}),
        async apply (batch, view, host) {
          const call = { peer: id, nodes: nodes(batch), host: [] }
          current.apply.push(call)
          const w = view.write()
          for (const n of batch) {
            const op = JSON.parse(n.value.toString())
            if (hex(n.key) === hex(db.key) && op.type === 'grant') {
              call.host.push({ method: 'addWriter', key: op.writer, options: { isIndexer: false } })
              await host.addWriter(Buffer.from(op.writer, 'hex'), { isIndexer: false })
              w.tryPut(Buffer.from('writer/' + op.writer), Buffer.from('writer'))
            } else if (hex(n.key) === hex(db.key) && op.type === 'revoke') {
              call.host.push({ method: 'removeWriter', key: op.writer })
              await host.removeWriter(Buffer.from(op.writer, 'hex'))
              w.tryDelete(Buffer.from('writer/' + op.writer))
            } else if (op.type === 'put') {
              w.tryPut(Buffer.from(op.key), Buffer.from(op.value))
            }
          }
          await w.flush()
        }
      })
      await db.ready()
      const p = { id, db, store }
      peers.push(p)
      current.peers[id] = { writer: hex(db.local.key) }
      return p
    }
    async function append (p, op, optimistic = false) {
      clock += 1000
      current.steps.push({ action: 'append', peer: p.id, op, optimistic })
      await p.db.append(Buffer.from(JSON.stringify(op)), { optimistic })
      await p.db.flush()
    }
    async function connect (a, b) {
      current.steps.push({ action: 'connect', peers: [a.id, b.id] })
      const x = a.store.replicate(true); const y = b.store.replicate(false)
      x.pipe(y).pipe(x); streams.push(x, y)
    }
    async function sync (a, b) {
      current.steps.push({ action: 'sync', peers: [a.id, b.id] })
      for (const [source, target] of [[a, b], [b, a]]) {
        if (!source.db.local.length) continue
        const core = target.store.get({ key: source.db.local.key })
        await core.ready()
        await core.get(source.db.local.length - 1, { timeout: 10000 })
        await core.close()
        await target.db.wakeup({ key: source.db.local.key, length: source.db.local.length })
        await target.db.update()
      }
      await a.db.update(); await b.db.update()
      await a.db.flush(); await b.db.flush()
    }
    function disconnect () {
      current.steps.push({ action: 'disconnect' })
      for (const s of streams.splice(0)) s.destroy()
    }
    await run({ peer, append, connect, sync, disconnect })
    for (const p of peers) {
      current.peers[p.id].oplog = await blocks(p.db.local)
      current.peers[p.id].stats = { undos: p.db.stats.undos, fastForwards: p.db.stats.fastForwards }
    }
    for (const field of ['topology', 'hashes', 'flushes', 'apply']) {
      current[field].sort((a, b) => (a.peer || '').localeCompare(b.peer || ''))
    }
    const text = JSON.stringify(current, null, 2) + '\n'
    assert(Buffer.byteLength(text) < 2 * 1024 * 1024)
    await writeFile(join(out, name + '.json'), text)
  } finally {
    for (const s of streams) s.destroy()
    for (const p of peers) { await p.db.close(); await p.store.close() }
    await rm(root, { recursive: true, force: true })
  }
}
const put = (key, value) => ({ type: 'put', key, value })
const grant = p => ({ type: 'grant', writer: hex(p.db.local.key) })
const revoke = p => ({ type: 'revoke', writer: hex(p.db.local.key) })
await scenario('single-writer', async ({ peer, append }) => {
  const a = await peer('a'); await append(a, put('one', '1')); await append(a, put('two', '2')); await append(a, put('one', '3'))
})
await scenario('concurrent-writers', async ({ peer, append, connect, sync, disconnect }) => {
  const a = await peer('a'); const b = await peer('b', a)
  await append(a, grant(b)); await connect(a, b); await sync(a, b); disconnect()
  await append(a, put('a', '1')); await append(b, put('b', '1'))
  await connect(a, b); await sync(a, b); disconnect()
  await append(a, put('a', '2')); await append(b, put('b', '2'))
  await connect(a, b); await sync(a, b)
})
await scenario('add-remove', async ({ peer, append, connect, sync, disconnect }) => {
  const a = await peer('a'); const b = await peer('b', a); const c = await peer('c', a)
  await append(a, grant(b)); await connect(a, b); await sync(a, b); disconnect()
  await append(b, put('b', '1')); await append(a, grant(c))
  await connect(a, b); await sync(a, b); disconnect()
  await connect(a, c); await sync(a, c); disconnect()
  await append(c, put('c', '1')); await connect(a, c); await sync(a, c); disconnect()
  await append(a, revoke(b)); await connect(a, b); await sync(a, b)
})
await scenario('optimistic', async ({ peer, append, connect, sync, disconnect }) => {
  const a = await peer('a'); const b = await peer('b', a)
  await append(b, put('optimistic', 'before-grant'), true)
  await connect(a, b); await sync(a, b); disconnect()
  await append(a, grant(b)); await connect(a, b); await sync(a, b); disconnect()
  await append(b, put('confirmed', 'after-grant')); await connect(a, b); await sync(a, b)
})
await scenario('reorder', async ({ peer, append, connect, sync, disconnect }) => {
  const a = await peer('a'); const b = await peer('b', a)
  await append(a, grant(b)); await connect(a, b); await sync(a, b); disconnect()
  await append(b, put('older', 'b')); await append(a, put('later', 'a1')); await append(a, put('latest', 'a2'))
  await connect(a, b); await sync(a, b)
  assert(current.topology.some(t => t.undo), 'scenario must actually undo')
})
await scenario('late-joiner', async ({ peer, append, connect, sync }) => {
  const a = await peer('a')
  for (let i = 0; i < 48; i++) await append(a, put('item/' + i, 'value/' + i))
  const b = await peer('b', a); await connect(a, b); await sync(a, b)
  let entry = null
  for (let i = 0; i < 200 && !entry; i++) {
    await b.db.update()
    entry = await b.db.view.get(Buffer.from('item/47'))
    if (!entry) await new Promise(resolve => setTimeout(resolve, 10))
  }
  assert.equal(entry?.value.toString(), 'value/47')
})

await scenario('fast-forward-landing', async ({ peer, append, connect, sync }) => {
  current.fastForward = { candidates: [], trust: [], landing: null }
  const a = await peer('a')
  const b = await peer('b', a, { fastForward: {}, trustOwner: true })
  await append(a, grant(b))
  for (let i = 0; i < 24; i++) await append(a, put('history/' + i, 'value/' + i))
  let release
  b.db._preApplied = false
  b.db._preapply = () => new Promise(resolve => { release = resolve })
  await connect(a, b)
  const head = { key: a.db.local.key, length: a.db.local.length }
  current.steps.push({ action: 'fast-forward-search', peer: 'b', heads: [clean(head)] })
  b.db._queueFastForward([head])
  for (let i = 0; i < 1500 && (!b.db.fastForwardTo || !release); i++) {
    await new Promise(resolve => setTimeout(resolve, 10))
  }
  assert(b.db.fastForwardTo && release, 'trusted landing must be ready before replay')
  release()
  for (let i = 0; i < 1500 && !b.db.stats.fastForwards; i++) {
    await new Promise(resolve => setTimeout(resolve, 10))
  }
  assert.equal(b.db.stats.fastForwards, 1)
  const systemHead = b.db.system.bee.head()
  const viewHead = b.db.system.view
  const downloaded = []
  for (const [kind, h] of [['oplog', head], ['system', systemHead], ['view', viewHead]]) {
    const core = b.store.get({ key: h.key })
    await core.ready()
    const present = []
    for (let i = 0; i < h.length; i++) if (await core.has(i)) present.push(i)
    downloaded.push({ kind, key: hex(h.key), length: core.length, targetLength: h.length, present })
    await core.close()
  }
  const system = []
  for await (const row of b.db.system.bee.createReadStream()) system.push({ key: hex(row.key), value: hex(row.value) })
  const view = []
  for await (const row of b.db.view.createReadStream()) view.push({ key: hex(row.key), value: hex(row.value) })
  const info = await b.db.system.bee.get(Buffer.from([0]))
  current.fastForward.landing = { systemHead: clean(systemHead), viewHead: clean(viewHead), downloaded, info: clean(encoding.decodeSystemInfo(info.value)), system, view }
  assert.equal((await b.db.view.get(Buffer.from('history/23'))).value.toString(), 'value/23')
  await append(b, put('after-landing', 'from-b'))
  await sync(b, a)
  let result = null
  for (let i = 0; i < 200 && !result; i++) {
    await a.db.update()
    result = await a.db.view.get(Buffer.from('after-landing'))
    if (!result) await new Promise(resolve => setTimeout(resolve, 10))
  }
  assert.equal(result?.value.toString(), 'from-b')
  current.fastForward.seenByOwner = { key: hex(result.key), value: hex(result.value) }
})
