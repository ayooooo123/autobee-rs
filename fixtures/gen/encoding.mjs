// Records reference encodings for the encoding_fixtures test.
//
// Part 1 walks every type of the autobee schema and, for every version the npm
// codec can encode, records a deterministic value with its bytes.
// Part 2 exercises the exported helpers of `autobee/lib/encoding.js`, the
// legacy decoders in `autobee/encoding/legacy.js` (decode-only in the reference,
// so their inputs are built here from the same wire layout) and the value
// padding / encryption envelope of `encodeValue`.
//
// Run with `npm install && node encoding.mjs`.

import fs from 'fs'
import path from 'path'
import { createRequire } from 'module'

const require = createRequire(import.meta.url)
const b4a = require('b4a')
const c = require('compact-encoding')

const ROOT = path.join(import.meta.dirname, '..', '..')
const autobeeRoot = path.dirname(require.resolve('autobee'))
const codec = require(path.join(autobeeRoot, 'encoding/spec/autobee/index.js'))
const E = require(path.join(autobeeRoot, 'lib/encoding.js'))

const SCHEMA = JSON.parse(fs.readFileSync(path.join(ROOT, 'spec/autobee/schema.json'), 'utf8'))
const TYPES = new Map(SCHEMA.schema.map((t) => [(t.namespace ? '@' + t.namespace + '/' : '') + t.name, t]))

const VARIANTS = ['full', 'sparse', 'mixed']
const PRIMITIVES = new Set([
  'uint', 'uint8', 'uint16', 'uint24', 'uint32', 'uint40', 'uint48', 'uint56', 'uint64',
  'int', 'int8', 'int16', 'int24', 'int32', 'int40', 'int48', 'int56', 'int64',
  'bool', 'buffer', 'optionalBuffer', 'fixed8', 'fixed16', 'fixed24', 'fixed32', 'fixed64'
])

function fqnOf(t) {
  return (t.namespace ? '@' + t.namespace + '/' : '') + t.name
}

function kindOf(t) {
  if (t.alias) return 'alias'
  if (t.enum) return 'enum'
  if (t.array) return 'array'
  if (t.record) return 'record'
  if (t.external) return 'external'
  if (t.versions) return 'versioned'
  return 'struct'
}

function makeRng(seed) {
  let s = seed >>> 0
  return () => {
    s = (s + 0x6d2b79f5) >>> 0
    let t = s
    t = Math.imul(t ^ (t >>> 15), t | 1)
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61)
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}

function hash(text) {
  let h = 2166136261
  for (let i = 0; i < text.length; i++) h = Math.imul(h ^ text.charCodeAt(i), 16777619)
  return h >>> 0
}

function pick(values, r) {
  return values[Math.floor(r() * values.length)]
}

function randUint(bits, r) {
  return Math.floor(r() * Math.min(2 ** bits, 2 ** 53 - 1))
}

function randInt(bits, r) {
  const max = 2 ** (bits - 1) - 1
  return Math.floor(r() * 2 * max) - max
}

// ---------------------------------------------------------------------------
// schema value sampling

function presentOptional(index, ctx) {
  if (ctx.variant === 'sparse') return false
  if (ctx.variant === 'full') return true
  return index % 2 === 0
}

function sampleStruct(def, ctx, types) {
  const out = {}
  ctx.depth = (ctx.depth || 0) + 1
  def.fields.forEach((f, i) => {
    const required = !!f.required || (!f.array && f.type === 'bool')
    if (!required && (ctx.depth > 2 || !presentOptional(i, ctx))) {
      out[f.name] = null
      return
    }
    out[f.name] = f.array ? sampleArray(f.type, ctx, types) : sampleType(f.type, ctx, types)
  })
  ctx.depth--
  return out
}

function sampleArray(name, ctx, types) {
  const len = ctx.depth > 2 ? 0 : ctx.variant === 'sparse' ? 0 : ctx.variant === 'mixed' ? 1 : 3
  const out = []
  for (let i = 0; i < len; i++) out.push(sampleType(name, ctx, types))
  return out
}

function sampleType(name, ctx, types) {
  if (PRIMITIVES.has(name)) return samplePrim(name, ctx)
  const def = types.get(name)
  if (!def) throw new Error('cannot sample unknown type ' + name)
  switch (kindOf(def)) {
    case 'array':
      return sampleArray(def.type, ctx, types)
    case 'struct':
      return sampleStruct(def, ctx, types)
    case 'versioned': {
      const inner = def.versions[def.versions.length - 1]
      return Object.assign({ version: inner.version }, sampleType(inner.type, ctx, types))
    }
    case 'external':
      return null
    default:
      throw new Error('cannot sample ' + name)
  }
}

function samplePrim(name, ctx) {
  const r = ctx.rand
  const extreme = ctx.variant === 'full'
  switch (name) {
    case 'uint':
      return extreme ? 0xffffffff + 1 : pick([0, 1, 0xfc, 0xfd, 0x1000, 0x123456], r)
    case 'uint8':
      return extreme ? 0xff : randUint(8, r)
    case 'uint16':
      return extreme ? 0xffff : randUint(16, r)
    case 'uint24':
      return extreme ? 0xffffff : randUint(24, r)
    case 'uint32':
      return extreme ? 0xffffffff : randUint(32, r)
    case 'uint40':
      return extreme ? 2 ** 40 - 1 : randUint(40, r)
    case 'uint48':
      return extreme ? 2 ** 48 - 1 : randUint(48, r)
    case 'uint56':
    case 'uint64':
      return extreme ? 2 ** 53 - 1 : randUint(53, r)
    case 'int':
      return extreme ? -(2 ** 52 - 1) : r() < 0.5 ? -Math.floor(r() * 1e6) : Math.floor(r() * 1e6)
    case 'int8':
      return extreme ? -128 : randInt(8, r)
    case 'int16':
      return extreme ? -32768 : randInt(16, r)
    case 'int24':
      return extreme ? -(2 ** 23) : randInt(24, r)
    case 'int32':
      return extreme ? -(2 ** 31) : randInt(32, r)
    case 'int40':
      return extreme ? -(2 ** 39) : randInt(40, r)
    case 'int48':
      return extreme ? -(2 ** 47) : randInt(48, r)
    case 'int56':
    case 'int64':
      return extreme ? -(2 ** 52 - 1) : randInt(52, r)
    case 'bool':
      return ctx.variant === 'sparse' ? false : r() < 0.5
    case 'buffer':
      return extreme ? Buffer.alloc(40, 0xab) : Buffer.from([1, 2, 3])
    case 'optionalBuffer':
      return r() < 0.25 ? null : extreme ? Buffer.alloc(9, 7) : Buffer.from([9, 8])
    case 'fixed8':
      return Buffer.alloc(8, 0x11)
    case 'fixed16':
      return Buffer.alloc(16, 0x22)
    case 'fixed24':
      return Buffer.alloc(24, 0x33)
    case 'fixed32':
      return Buffer.alloc(32, 0x44)
    case 'fixed64':
      return Buffer.alloc(64, 0x55)
    default:
      throw new Error('cannot sample primitive ' + name)
  }
}

// ---------------------------------------------------------------------------
// schema cases

function touchesExternal(t, seen) {
  if (t.array) return touchesExternal(TYPES.get(t.type), seen)
  const kind = kindOf(t)
  if (kind === 'external') return true
  if (kind !== 'struct') return false
  const fqn = fqnOf(t)
  if (seen.has(fqn)) return false
  seen.add(fqn)
  return t.fields.some((f) => {
    const ref = TYPES.get(f.type)
    return ref ? touchesExternal(ref, seen) : false
  })
}

function encodeCase(fqn, value, version) {
  try {
    const bytes = codec.encode(fqn, value, version)
    const decoded = codec.decode(fqn, bytes, version)
    // A version with a `map` decodes to a shape the raw type cannot re-encode.
    if (!codec.encode(fqn, decoded, version).equals(bytes)) return null
    return { hex: bytes.toString('hex'), decoded }
  } catch {
    return null
  }
}

function schemaCases() {
  const cases = []
  for (const t of SCHEMA.schema) {
    const fqn = fqnOf(t)
    const kind = kindOf(t)
    if (kind === 'external') continue

    if (kind === 'versioned') {
      for (const def of t.versions) {
        const inner = TYPES.get(def.type)
        if (!inner || kindOf(inner) === 'external') continue
        const ctx = { variant: 'full', rand: makeRng(hash(fqn) + def.version) }
        const value = Object.assign({ version: def.version }, sampleType(def.type, ctx, TYPES))
        const rec = encodeCase(fqn, value, SCHEMA.version)
        if (!rec) continue
        cases.push({
          name: fqn,
          version: SCHEMA.version,
          hex: rec.hex,
          value: Object.assign({ version: def.version }, jsonSafe(rec.decoded))
        })
      }
      continue
    }

    if (touchesExternal(t, new Set())) continue

    for (const variant of VARIANTS) {
      const ctx = { variant, rand: makeRng(hash(fqn)) }
      const value = sampleType(fqn, ctx, TYPES)
      const rec = encodeCase(fqn, value, SCHEMA.version)
      if (!rec) continue
      cases.push({
        name: fqn,
        version: SCHEMA.version,
        hex: rec.hex,
        value: jsonSafe(rec.decoded)
      })
    }
  }
  return cases
}

// ---------------------------------------------------------------------------
// helper cases

const K1 = b4a.alloc(32, 0x11)
const K2 = b4a.alloc(32, 0x22)
const UINT_MAX = 0xffffffff + 1

function jsonSafe(value) {
  return JSON.parse(
    JSON.stringify(value, (key, v) => {
      if (typeof v === 'bigint') return Number(v)
      if (v && typeof v === 'object' && v.type === 'Buffer' && Array.isArray(v.data)) {
        return Buffer.from(v.data).toString('hex')
      }
      return v
    })
  )
}

function link(key, length) {
  return { key, length }
}

function clock() {
  return [{ key: K1, length: 1 }, { key: K2, length: 2 }]
}

// The reference legacy codecs only decode; their inputs are assembled from the
// same layouts with compact-encoding primitives.
function encodeCheckout(m) {
  const state = { start: 0, end: 0, buffer: null }
  c.fixed32.preencode(state, m.key)
  c.uint.preencode(state, m.length)
  state.buffer = b4a.alloc(state.end)
  state.start = 0
  c.fixed32.encode(state, m.key)
  c.uint.encode(state, m.length)
  return state.buffer
}

function encodeClock(items) {
  return b4a.concat([c.encode(c.uint, items.length), ...items.map(encodeCheckout)])
}

function encodeNode(m) {
  const state = { start: 0, end: 0, buffer: null }
  const heads = encodeClock(m.heads)
  c.uint.preencode(state, m.batch)
  c.optionalBuffer.preencode(state, m.value)
  state.buffer = b4a.alloc(state.end)
  state.start = 0
  c.uint.encode(state, m.batch)
  c.optionalBuffer.encode(state, m.value)
  return b4a.concat([heads, state.buffer])
}

function encodeCheckpointer(idx) {
  const parts = [c.encode(c.uint, idx.checkpointer)]
  if (idx.checkpoint !== null) {
    parts.push(c.encode(c.fixed64, idx.checkpoint.signature), c.encode(c.uint, idx.checkpoint.length))
  }
  return b4a.concat(parts)
}

function checkpointerArray(items) {
  return b4a.concat([c.encode(c.uint, items.length), ...items.map(encodeCheckpointer)])
}

function encodeAdditional() {
  return c.encode(c.uint, 1) // pointer != 0, no additional data
}

function legacyOplogV0(node, maxSupportedVersion) {
  return b4a.concat([
    c.encode(c.uint, 0),
    c.encode(c.uint, 0), // flags: no digest, no checkpoint
    encodeNode(node),
    encodeAdditional(),
    c.encode(c.uint, maxSupportedVersion)
  ])
}

function legacyOplogV1({ node, maxSupportedVersion, optimistic }) {
  return b4a.concat([
    c.encode(c.uint, 1),
    c.encode(c.uint, maxSupportedVersion),
    c.encode(c.uint, optimistic ? 2 : 0),
    encodeNode(node)
  ])
}

function legacyBootRecordV0(indexed, heads) {
  return b4a.concat([
    c.encode(c.uint, 0),
    encodeCheckout(indexed),
    encodeClock(heads)
  ])
}

function legacySystemWriter(version, length) {
  return b4a.concat([c.encode(c.uint, version), c.encode(c.uint, length)])
}

async function calls() {
  const out = []

  // boot record (a plain link)
  const bootRecord = link(K1, 42)
  out.push({
    fn: 'encode_boot_record',
    value: jsonSafe(bootRecord),
    hex: E.encodeBootRecord(bootRecord).toString('hex')
  })
  out.push({
    fn: 'decode_boot_record',
    hex: E.encodeBootRecord(bootRecord).toString('hex'),
    decoded: jsonSafe(E.decodeBootRecord(E.encodeBootRecord(bootRecord)))
  })

  // previous drain
  out.push({ fn: 'encode_previous_drain', value: UINT_MAX, hex: E.encodePreviousDrain(UINT_MAX).toString('hex') })
  out.push({ fn: 'decode_previous_drain', hex: E.encodePreviousDrain(UINT_MAX).toString('hex'), decoded: UINT_MAX })

  // autobase boot record: v3 encodable, v0 legacy decode-only
  const br3 = {
    version: 3,
    key: K2,
    systemLength: 7,
    indexersUpdated: true,
    fastForwarding: false,
    recoveries: 3
  }
  const br3bytes = codec.encode('@autobase-compat/boot-record', br3, SCHEMA.version)
  out.push({ fn: 'decode_autobase_boot_record', hex: br3bytes.toString('hex'), decoded: jsonSafe(E.decodeAutobaseBootRecord(br3bytes)) })
  const br0bytes = legacyBootRecordV0(link(K1, 5), clock())
  out.push({ fn: 'decode_autobase_boot_record', hex: br0bytes.toString('hex'), decoded: jsonSafe(E.decodeAutobaseBootRecord(br0bytes)) })

  // system keys and pending entries
  out.push({ fn: 'encode_system_writer_key', key: K1.toString('hex'), hex: E.encodeSystemWriterKey(K1).toString('hex') })
  out.push({ fn: 'encode_system_pending_key', key: K2.toString('hex'), hex: E.encodeSystemPendingKey(K2).toString('hex') })
  const pendingKey = E.encodeSystemPendingKey(K2)
  out.push({ fn: 'decode_system_pending_key', hex: pendingKey.toString('hex'), decoded: jsonSafe(E.decodeSystemPendingKey(pendingKey)) })
  const range = E.encodeSystemPendingRange()
  out.push({ fn: 'encode_system_pending_range', gte: range.gte.toString('hex'), lt: range.lt.toString('hex') })
  for (const weight of [0, 300, 2 ** 32]) {
    const bytes = E.encodeSystemPending({ weight })
    out.push({ fn: 'encode_system_pending', value: weight, hex: bytes.toString('hex') })
    out.push({ fn: 'decode_system_pending', hex: bytes.toString('hex'), decoded: jsonSafe(E.decodeSystemPending(bytes)) })
  }

  // grants
  out.push({ fn: 'encode_system_grant_key', key: K2.toString('hex'), hex: E.encodeSystemGrantKey(K2).toString('hex') })
  const hint = { key: K1, length: 9, weight: 2 }
  const hintBytes = E.encodeSystemGrantHint(hint)
  out.push({ fn: 'encode_system_grant_hint', value: jsonSafe(hint), hex: hintBytes.toString('hex') })
  out.push({ fn: 'decode_system_grant_hint', hex: hintBytes.toString('hex'), decoded: jsonSafe(E.decodeSystemGrantHint(hintBytes)) })

  // system writers: v4/v5 encode, v0-v3 are legacy decode-only
  const writerKey = E.encodeSystemWriterKey(K1)
  for (const value of [
    { version: 4, isRemoved: false, isOplog: true, weight: 2, maxWeight: 2, length: 11, clock: 3, isGenesis: true, isAnchor: false, timestamp: -5 },
    { version: 5, isRemoved: true, isOplog: false, isGenesis: false, weight: 1, maxWeight: 1, length: 0, timestamp: UINT_MAX }
  ]) {
    const bytes = E.encodeSystemWriter(value)
    out.push({ fn: 'encode_system_writer', key: writerKey.toString('hex'), value: jsonSafe(value), hex: bytes.toString('hex') })
    out.push({
      fn: 'decode_system_writer',
      key: writerKey.toString('hex'),
      hex: bytes.toString('hex'),
      decoded: { key: K1.toString('hex'), ...jsonSafe(E.decodeSystemWriter(writerKey, bytes)) }
    })
  }
  for (const version of [0, 1, 2, 3]) {
    const bytes = legacySystemWriter(version, 21)
    out.push({
      fn: 'decode_system_writer',
      key: writerKey.toString('hex'),
      hex: bytes.toString('hex'),
      decoded: { key: K1.toString('hex'), ...jsonSafe(E.decodeSystemWriter(writerKey, bytes)) }
    })
  }

  // system info: v1 (mapped on decode), v2, v3
  const infos = [
    {
      version: 1,
      members: 2,
      pendingIndexers: [K1],
      indexers: clock(),
      heads: clock(),
      views: []
    },
    {
      version: 2,
      members: 3,
      pendingIndexers: [K1, K2],
      indexers: clock(),
      heads: clock(),
      views: clock(),
      encryptionLength: 4,
      entropy: b4a.alloc(32, 0x33)
    },
    {
      version: 3,
      timestamp: 1700000000000,
      flushes: 5,
      view: link(K1, 6),
      heads: [link(K2, 7)],
      indexers: [link(K1, 8)],
      pending: [true, false],
      hash: b4a.alloc(8, 0x55)
    }
  ]
  for (const value of infos) {
    const bytes = E.encodeSystemInfo(value)
    out.push({ fn: 'encode_system_info', value: jsonSafe(value), hex: bytes.toString('hex') })
    out.push({ fn: 'decode_system_info', hex: bytes.toString('hex'), decoded: jsonSafe(E.decodeSystemInfo(bytes)) })
  }

  // oplogs: v4 and v3 encode, v2 encodes through the versioned wrapper, v0/v1
  // are legacy decode-only
  const oplogV4 = {
    version: 4,
    timestamp: 1700000000001,
    links: [link(K1, 3)],
    batch: { start: 0, end: 2 },
    views: { flushes: 1, system: { key: K1, start: 0, length: 3 }, view: { key: K2, start: 1, length: 2 } },
    trusted: [link(K2, 9)],
    witness: { weight: 2, link: link(K1, 1) },
    approvals: [{ key: K1, weight: 2 }],
    optimistic: true,
    value: Buffer.from('v4 value'),
    hash: b4a.alloc(8, 0x66),
    weightHint: 4,
    drift: 1
  }
  const oplogV3 = {
    version: 3,
    timestamp: 1700000000002,
    links: [link(K2, 4)],
    batch: { start: 1, end: 5 },
    views: { flushes: 2, system: { key: K2, start: 0, length: 4 }, view: null },
    optimistic: true,
    value: Buffer.from('v3 value'),
    witness: {
      weight: 3,
      backer: { key: K1, length: 2, signature: b4a.alloc(64, 0x77), manifest: Buffer.from('manifest') }
    },
    attestations: [{ key: K2, weight: 1, signature: b4a.alloc(64, 0x88) }],
    trusted: [{ key: K1, length: 5, flushes: 2 }]
  }
  const oplogV2 = {
    version: 2,
    node: { heads: clock(), batch: 2, value: Buffer.from('v2 value') },
    checkpoint: null,
    digest: null,
    optimistic: false,
    trace: null
  }
  for (const value of [oplogV4, oplogV3, oplogV2]) {
    const bytes = E.encodeOplog(structuredClone(value))
    out.push({ fn: 'encode_oplog', value: jsonSafe(value), hex: bytes.toString('hex') })
    out.push({ fn: 'decode_oplog', hex: bytes.toString('hex'), decoded: jsonSafe(E.decodeOplog(bytes)) })
    out.push({ fn: 'decode_raw_oplog', hex: bytes.toString('hex'), decoded: jsonSafe(E.decodeRawOplog(bytes)) })
  }
  // a zeroed batch is written as absent
  const zeroBatch = { version: 4, timestamp: 1700000000003, links: [], batch: { start: 0, end: 0 }, optimistic: false }
  const zeroBytes = E.encodeOplog(structuredClone(zeroBatch))
  out.push({ fn: 'encode_oplog', value: jsonSafe(zeroBatch), hex: zeroBytes.toString('hex') })
  out.push({ fn: 'decode_oplog', hex: zeroBytes.toString('hex'), decoded: jsonSafe(E.decodeOplog(zeroBytes)) })

  const node = { heads: clock(), batch: 3, value: Buffer.from('legacy') }
  for (const bytes of [
    legacyOplogV0(node, 4),
    legacyOplogV1({ node, maxSupportedVersion: 4, optimistic: true })
  ]) {
    out.push({ fn: 'decode_oplog', hex: bytes.toString('hex'), decoded: jsonSafe(E.decodeOplog(bytes)) })
    out.push({ fn: 'decode_raw_oplog', hex: bytes.toString('hex'), decoded: jsonSafe(E.decodeRawOplog(bytes)) })
  }

  // values: default, legacy, padding and encrypted envelopes
  const payload = Buffer.from('hello autobee')
  const valueCases = [
    { value: payload, opts: { timestamp: 1700000000004 } },
    { value: payload, opts: { version: 3, timestamp: 1700000000005, optimistic: true, links: [link(K1, 1)] } },
    { value: payload, opts: { timestamp: 1700000000006, padding: 16 } },
    { value: payload, opts: { timestamp: 1700000000007, optimistic: true, encrypted: true } },
    { value: payload, opts: { legacy: true, optimistic: true, heads: clock() } }
  ]
  for (const { value, opts } of valueCases) {
    const bytes = E.encodeValue(value, opts)
    out.push({ fn: 'encode_value', value: value.toString('hex'), opts: jsonSafe(opts), hex: bytes.toString('hex') })
    let decoded
    try {
      decoded = await E.decodeValue(bytes)
    } catch {
      continue // an encrypted envelope needs a key; the reference throws
    }
    out.push({ fn: 'decode_value', hex: bytes.toString('hex'), decoded: decoded ? decoded.toString('hex') : null })
  }

  return out
}

const out = {
  source: 'autobee@2.12.1',
  version: SCHEMA.version,
  types: schemaCases(),
  calls: await calls()
}
if (out.types.some((t) => t.name.includes('system-writer')) === false) throw new Error('no system-writer cases')

fs.writeFileSync(path.join(ROOT, 'fixtures/encoding-reference.json'), JSON.stringify(out, null, 1) + '\n')
console.log(`types: ${out.types.length}, calls: ${out.calls.length}`)
