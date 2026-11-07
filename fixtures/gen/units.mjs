// Records the pure-module behaviour of autobee 2.12.1 for the Rust unit tests:
// `lib/triggers.js`, `lib/trusted.js`, `lib/updates.js` and
// `lib/apply-calls.js`, driven with stub writers, handlers and autobee objects.
//
// Run with `npm install && node units.mjs`.

import fs from 'fs'
import path from 'path'
import { createRequire } from 'module'

const require = createRequire(import.meta.url)
const b4a = require('b4a')
const Triggers = require('autobee/lib/triggers.js')
const TrustedPeers = require('autobee/lib/trusted.js')
const UpdateChanges = require('autobee/lib/updates.js')
const ApplyCalls = require('autobee/lib/apply-calls.js')

const ROOT = path.join(import.meta.dirname, '..', '..')
const hex = (buf) => (buf ? b4a.toString(buf, 'hex') : null)
const key = (byte) => b4a.alloc(32, byte)

// --- triggers ------------------------------------------------------------

function recordTriggers () {
  const triggers = new Triggers()
  const writers = new Map()
  const bumps = []
  let entry = null

  const writer = (id) => {
    if (writers.has(id)) return writers.get(id)
    const w = {
      id,
      waiting: null,
      bump () {
        bumps.push(id)
      }
    }
    writers.set(id, w)
    return w
  }

  const waitingState = () => {
    const out = {}
    for (const [id, w] of writers) {
      if (!w.waiting) continue
      out[id] = { id: hex(w.waiting.id), index: w.waiting.index, length: w.waiting.length }
    }
    return out
  }

  const steps = []
  const step = (op, result) => {
    const snapshot = { ...result }
    if (entry !== null) snapshot.entry = { id: hex(entry.id), index: entry.index, length: entry.length }
    steps.push({ op, entry: snapshot.entry ?? null, bumps: bumps.splice(0), waiting: waitingState() })
  }

  const add = (writerId, id, length) => {
    entry = triggers.add(id, length, writer(writerId))
    writer(writerId).waiting = entry
    step({ add: { writer: writerId, id: hex(id), length } })
  }

  const trigger = (id, length) => {
    triggers.trigger(id, length)
    step({ trigger: { id: hex(id), length } })
  }

  const drop = (writerId) => {
    const w = writer(writerId)
    triggers.remove(w.waiting)
    step({ remove: { writer: writerId } })
  }

  const A = key(0xa1)
  const B = key(0xb2)
  const C = key(0xc3)

  add('w1', A, 1)
  add('w1', A, 2)
  add('w2', A, 1)
  add('w1', B, 5)
  trigger(A, 0)
  trigger(A, 1)
  add('w3', C, 3)
  add('w1', A, 4)
  add('w2', A, 2)
  trigger(A, 2)
  trigger(B, 5)
  add('w1', B, 6)
  drop('w1')
  drop('w1')
  trigger(B, 10)

  return { steps }
}

// --- trusted -------------------------------------------------------------

async function recordTrusted () {
  const autoKey = key(0x01)
  const indexer = key(0x02)
  const yes = key(0x03)
  const no = key(0x04)
  const slow = key(0x05)
  const genesis = { isGenesis: true, key: hex(autoKey), indexers: [hex(indexer)] }

  const answers = new Map([[hex(yes), true], [hex(no), false], [hex(slow), true]])
  let calls = 0

  const hooks = {
    isTrusted: async (k) => {
      calls++
      const answer = answers.get(hex(k))
      if (hex(k) === hex(slow)) await new Promise((resolve) => setTimeout(resolve, 10))
      return answer === true
    }
  }

  const auto = {
    key: autoKey,
    system: { isGenesis: () => true, indexers: [{ key: indexer, weight: 1 }] }
  }

  const trusted = new TrustedPeers(auto, hooks)
  const steps = []

  const probe = async (label, k) => {
    const before = calls
    const result = await trusted.isTrusted(k, null)
    steps.push({ kind: 'isTrusted', label, key: hex(k), result, hookCalls: calls - before, calls })
  }

  await probe('genesis', autoKey)
  await probe('indexer', indexer)
  await probe('yes', yes)
  await probe('yes-cached', yes)
  await probe('no', no)
  await probe('no-again', no)

  const beforeSlow = calls
  const [first, second] = await Promise.all([trusted.isTrusted(slow, null), trusted.isTrusted(slow, null)])
  steps.push({ kind: 'concurrent', key: hex(slow), results: [first, second], hookCalls: calls - beforeSlow, calls })

  trusted.clear()
  steps.push({ kind: 'clear' })
  await probe('after-clear', yes)

  steps.push({ kind: 'mostRecentTrusted', result: (await trusted.mostRecentTrusted({}, null)) ?? null })
  steps.push({ kind: 'read', input: true, result: trusted.read(true) })
  steps.push({ kind: 'read', input: false, result: trusted.read(false) })
  steps.push({ kind: 'read', input: null, result: trusted.read(null) })

  // no hooks at all: isTrusted falls back to !mostRecentTrusted
  const bare = new TrustedPeers(auto, {})
  steps.push({ kind: 'bare', key: hex(key(0x06)), result: await bare.isTrusted(key(0x06), null) })

  // a mostRecentTrusted hook returns the pinned head
  const head = { key: key(0x07), length: 9 }
  const withRecent = new TrustedPeers(auto, {
    isTrusted: async () => true,
    mostRecentTrusted: async () => head
  })
  steps.push({
    kind: 'mostRecentTrustedHook',
    result: describe(await withRecent.mostRecentTrusted({}, null))
  })

  return { genesis, steps }
}

// --- updates -------------------------------------------------------------

function head (byte, length) {
  return { key: key(byte), length }
}

function describe (value) {
  return value ? { key: hex(value.key), length: value.length } : null
}

function recordUpdates () {
  const scenarios = []

  const run = (name, { fromView, fromSystem, flushes, toView, toSystem, finalFlushes, fastForward, track = true }) => {
    // the reference keeps the live bee objects from track(), so their heads are
    // read again at finalise; model one bee per tree whose head changes
    let viewHead = fromView
    let systemHead = fromSystem
    const auto = {
      _workingBee: { head: () => viewHead },
      system: { bee: { head: () => systemHead }, flushes }
    }
    const changes = new UpdateChanges(auto)
    if (track) changes.track()
    viewHead = toView
    systemHead = toSystem
    auto.system.flushes = finalFlushes
    const mapping = changes.finalise(fastForward)
    const out = {}
    for (const key of ['view', 'system']) {
      const change = mapping.get(key)
      out[key] = change
        ? {
            flushes: change.flushes,
            from: describe(change.from),
            to: describe(change.to),
            incremental: change.incremental
          }
        : null
    }
    scenarios.push({
      name,
      track: track ? { view: describe(fromView), system: describe(fromSystem), flushes } : null,
      finalise: {
        view: describe(toView),
        system: describe(toSystem),
        flushes: finalFlushes,
        fastForward
      },
      expect: out
    })
  }

  run('incremental', {
    fromView: head(1, 4),
    fromSystem: head(2, 3),
    flushes: 10,
    toView: head(1, 9),
    toSystem: head(2, 3),
    finalFlushes: 12,
    fastForward: false
  })

  run('fast-forward', {
    fromView: head(1, 4),
    fromSystem: head(2, 3),
    flushes: 10,
    toView: head(3, 7),
    toSystem: head(4, 8),
    finalFlushes: 11,
    fastForward: true
  })

  run('untracked', {
    fromView: null,
    fromSystem: null,
    flushes: 0,
    toView: head(1, 2),
    toSystem: head(2, 2),
    finalFlushes: 1,
    fastForward: false,
    track: false
  })

  return { scenarios }
}

// --- apply calls ---------------------------------------------------------

async function recordApplyCalls () {
  const calls = []
  const auto = {
    id: 'id-1',
    key: key(0x11),
    discoveryKey: key(0x22),
    name: 'test-name',
    system: {
      flushes: 7,
      isGenesis: () => true,
      addWriter: async (key, opts) => {
        calls.push({
          method: 'addWriter',
          key: hex(key),
          opts: {
            weight: opts.weight,
            isGenesis: opts.isGenesis,
            coord: opts.coord ? { key: hex(opts.coord.key), length: opts.coord.length } : null,
            carrier: opts.carrier
          }
        })
      },
      removeWriter: async (key) => {
        calls.push({ method: 'removeWriter', key: hex(key) })
      }
    },
    interrupt (reason) {
      calls.push({ method: 'interrupt', reason: reason ?? null })
    },
    createAnchor: async (k, length) => ({ key: k, length })
  }

  const host = new ApplyCalls(auto)
  const steps = []
  const flush = (extra) => {
    const recorded = calls.splice(0)
    steps.push({ ...extra, calls: recorded })
  }

  await host.addWriter(key(0x31), {})
  flush({ op: 'addWriter-default' })

  await host.addWriter(key(0x32), { isIndexer: false })
  flush({ op: 'addWriter-writer' })

  await host.addWriter(key(0x33), { weight: 5 })
  flush({ op: 'addWriter-weight' })

  await host.removeWriter(key(0x32))
  flush({ op: 'removeWriter' })

  const validId = hex(key(0x34))
  await host.addWriter(validId, {})
  flush({ op: 'addWriter-id' })

  try {
    await host.addWriter('not-a-key', {})
  } catch (err) {
    steps.push({ op: 'addWriter-invalid', error: err.message })
  }

  try {
    await host.removeWriter('zzz')
  } catch (err) {
    steps.push({ op: 'removeWriter-invalid', error: err.message })
  }

  const applyingKey = key(0x35)
  host.applying = [{ key: applyingKey, length: 3, weight: 4 }]
  await host.addWriter(applyingKey, {})
  flush({ op: 'addWriter-applying' })
  host.applying = null

  host.interrupt('stop')
  flush({ op: 'interrupt' })

  steps.push({
    op: 'accessors',
    id: host.id,
    key: hex(host.key),
    discoveryKey: hex(host.discoveryKey),
    name: host.name,
    clock: host.clock,
    genesis: host.genesis,
    removeable: host.removeable(key(0x36))
  })

  return { steps }
}

const out = {
  schema: 1,
  triggers: recordTriggers(),
  trusted: await recordTrusted(),
  updates: recordUpdates(),
  applyCalls: await recordApplyCalls()
}

fs.writeFileSync(path.join(ROOT, 'fixtures/units-reference.json'), JSON.stringify(out, null, 1) + '\n')
