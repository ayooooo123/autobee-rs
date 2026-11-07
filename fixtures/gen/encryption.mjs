// Generates ../encryption-reference.json by running the real npm autobee
// encryption classes (2.12.1) and the system order hash from lib/system.js.
import { createHash } from 'node:crypto'
import { createRequire } from 'node:module'

const require = createRequire(import.meta.url)

const c = require('compact-encoding')
const b4a = require('b4a')
const sodium = require('sodium-native')
const DefaultEncryption = require('hypercore/lib/default-encryption.js')
const { AutobeeEncryption, ViewEncryption, WriterEncryption } = require('autobee/lib/encryption.js')
const { ManifestData } = require('autobee/lib/encoding.js')
const { DEFAULT_MANIFEST_VERSION } = require('autobee/lib/constants.js')

const PADDING = AutobeeEncryption.PADDING
const hex = (b) => b4a.toString(b, 'hex')

// Deterministic stand-in for random data: BLAKE2b of a label.
function bytes(label, n = 32) {
  const out = b4a.alloc(n)
  let off = 0
  let i = 0
  while (off < n) {
    const d = createHash('blake2b512').update(`${label}@${i++}`).digest()
    d.copy(out, off, 0, Math.min(d.length, n - off))
    off += d.length
  }
  return out
}

const key = (label) => bytes(label)
const pad = (data) => b4a.concat([b4a.alloc(PADDING), data])

function provider({ core, name, bootstrap, encryptionKey, version, coreKey, userData }) {
  const auto = { key: bootstrap, encryptionKey }
  const ctx = { key: coreKey, manifest: { version, userData: userData || null } }
  if (core === 'writer') return { enc: new WriterEncryption(auto), ctx }
  return { enc: new ViewEncryption(auto, name), ctx }
}

// Fixture fields shared by every case.
function base(spec) {
  return {
    core: spec.core,
    name: spec.name ?? null,
    bootstrap: hex(spec.bootstrap),
    encryption_key: hex(spec.encryptionKey),
    manifest_version: spec.version,
    core_key: hex(spec.coreKey),
    user_data: spec.userData ? hex(spec.userData) : null,
    index: spec.index,
    fork: spec.fork
  }
}

const cases = []

async function cipherCase(spec) {
  const { enc, ctx } = provider(spec)
  const plaintext = bytes(spec.label, spec.size)
  const input = pad(plaintext)
  const buf = b4a.from(input)
  await enc.encrypt(spec.index, buf, spec.fork, ctx)
  const copy = b4a.from(buf)
  const returned = await enc.decrypt(spec.index, copy, ctx)
  // decrypt mutates in place for ciphertext and only returns the payload for type 0.
  const decrypted = returned === undefined ? copy.subarray(PADDING) : returned
  if (!b4a.equals(decrypted, plaintext)) throw new Error(`roundtrip failed: ${spec.label}`)

  cases.push({
    mode: 'encrypt',
    ...base(spec),
    plaintext: hex(plaintext),
    block: hex(input),
    encrypted: hex(buf),
    expected: hex(decrypted)
  })
}

// Compat view core: data is encrypted by hypercore with the compat block key,
// autobee only ever decrypts it (its view encrypt refuses pre-v3 manifests).
function compatViewCase(spec) {
  const { enc, ctx } = provider(spec)
  const plaintext = bytes(spec.label, spec.size)
  const block = pad(plaintext)
  const compat = enc.compatKeys()
  DefaultEncryption.encrypt(spec.index, block, spec.fork, compat.block, compat.blinding)

  cases.push({
    mode: 'decrypt-only',
    ...base(spec),
    plaintext: hex(plaintext),
    block: hex(block),
    encrypted: null,
    expected: hex(plaintext)
  })
}

// Compat writer core: both sides derive the same hypercore keys.
async function compatWriterCase(spec) {
  const { enc, ctx } = provider(spec)
  const plaintext = bytes(spec.label, spec.size)
  const input = pad(plaintext)
  const buf = b4a.from(input)
  await enc.encrypt(spec.index, buf, spec.fork, ctx)

  const derived = DefaultEncryption.deriveKeys(spec.encryptionKey, spec.coreKey)
  const reference = b4a.from(input)
  DefaultEncryption.encrypt(spec.index, reference, spec.fork, derived.block, derived.blinding)
  if (!b4a.equals(buf, reference)) throw new Error(`compat keys differ: ${spec.label}`)

  cases.push({
    mode: 'encrypt',
    ...base(spec),
    plaintext: hex(plaintext),
    block: hex(input),
    encrypted: hex(buf),
    expected: hex(plaintext)
  })
}

// Type 0 padding: block contents are stored as-is.
async function plaintextCase(spec) {
  const { enc, ctx } = provider(spec)
  const plaintext = bytes(spec.label, spec.size)
  const padding = bytes(`${spec.label}-padding`, PADDING)
  padding[0] = 0
  const block = b4a.concat([padding, plaintext])
  const copy = b4a.from(block)
  const returned = await enc.decrypt(spec.index, copy, ctx)
  const decrypted = returned === undefined ? copy.subarray(PADDING) : returned
  if (!b4a.equals(decrypted, plaintext)) throw new Error(`plain decrypt failed: ${spec.label}`)

  cases.push({
    mode: 'plaintext',
    ...base(spec),
    plaintext: hex(plaintext),
    block: hex(block),
    encrypted: null,
    expected: hex(decrypted)
  })
}

function userData(namespace, legacyBlocks) {
  return c.encode(ManifestData, { version: 0, legacyBlocks, namespace })
}

const sizes = [0, 1, 7, 8, 32, 100, 512, 1024]

// View cores: version 3 and above, alternating names.
let n = 0
for (let i = 0; i < 15; i++) {
  for (let j = 0; j < 4; j++) {
    const label = `view-${i}-${j}`
    await cipherCase({
      core: 'view',
      name: i % 2 === 0 ? null : `view-${i}`,
      label,
      bootstrap: key(`bootstrap-${i}`),
      encryptionKey: key(`encryption-${i}`),
      version: i % 3 === 0 ? 3 : i % 3 === 1 ? 4 : 5,
      coreKey: key(`core-${i}-${j}`),
      userData: null,
      index: [0, 1, j * 3 + 1, 1 << (j + 12)][j],
      fork: [0, 1, 7, 2 ** 32][j],
      size: sizes[(i * 4 + j) % sizes.length]
    })
    n++
  }
}

// System core: always named `_system`.
for (let i = 0; i < 10; i++) {
  await cipherCase({
    core: 'view',
    name: '_system',
    label: `system-${i}`,
    bootstrap: key(`system-bootstrap-${i}`),
    encryptionKey: key(`system-encryption-${i}`),
    version: DEFAULT_MANIFEST_VERSION,
    coreKey: key(`system-core-${i}`),
    userData: null,
    index: i * 5,
    fork: i % 3,
    size: sizes[i % sizes.length]
  })
  n++
}

// Writer cores: userData decides whether the namespace or the core key is used.
for (let i = 0; i < 10; i++) {
  for (let j = 0; j < 3; j++) {
    const namespace = j === 0 ? null : key(`writer-namespace-${i}-${j}`)
    await cipherCase({
      core: 'writer',
      label: `writer-${i}-${j}`,
      bootstrap: key(`writer-bootstrap-${i}`),
      encryptionKey: key(`writer-encryption-${i}`),
      version: 3,
      coreKey: key(`writer-core-${i}-${j}`),
      userData: namespace ? userData(namespace, j === 2 ? 9 : 0) : null,
      index: j * 1000 + i,
      fork: j,
      size: sizes[(i + j) % sizes.length]
    })
    n++
  }
}

// Compat cores, manifest version <= 1.
for (let i = 0; i < 8; i++) {
  await compatWriterCase({
    core: 'writer',
    label: `compat-writer-${i}`,
    bootstrap: key(`compat-bootstrap-${i}`),
    encryptionKey: key(`compat-encryption-${i}`),
    version: i % 2 === 0 ? 1 : 0,
    coreKey: key(`compat-core-${i}`),
    userData: null,
    index: i * 3,
    fork: i % 2,
    size: sizes[i % sizes.length]
  })
  n++
}

for (let i = 0; i < 8; i++) {
  compatViewCase({
    core: 'view',
    name: '_system',
    label: `compat-view-${i}`,
    bootstrap: key(`compat-view-bootstrap-${i}`),
    encryptionKey: key(`compat-view-encryption-${i}`),
    version: 1,
    coreKey: key(`compat-view-core-${i}`),
    userData: null,
    index: i * 7,
    fork: i % 3,
    size: sizes[i % sizes.length]
  })
  n++
}

// Type 0 blocks.
for (let i = 0; i < 6; i++) {
  await plaintextCase({
    core: i % 2 === 0 ? 'view' : 'writer',
    name: i % 2 === 0 ? '_system' : null,
    label: `plaintext-${i}`,
    bootstrap: key(`plain-bootstrap-${i}`),
    encryptionKey: key(`plain-encryption-${i}`),
    version: 3,
    coreKey: key(`plain-core-${i}`),
    userData: null,
    index: i,
    fork: 0,
    size: sizes[i % sizes.length]
  })
  n++
}

// Anchors (autobee/index.js:1151): index 0 and block id 0 with a namespace key.
const anchors = []
for (let i = 0; i < 10; i++) {
  const bootstrap = key(`anchor-bootstrap-${i}`)
  const encryptionKey = key(`anchor-encryption-${i}`)
  const namespace = key(`anchor-namespace-${i}`)
  const plaintext = bytes(`anchor-${i}`, sizes[i % sizes.length])
  const block = pad(plaintext)
  AutobeeEncryption.encryptAnchor(block, bootstrap, encryptionKey, namespace)
  anchors.push({
    bootstrap: hex(bootstrap),
    encryption_key: hex(encryptionKey),
    namespace: hex(namespace),
    plaintext: hex(plaintext),
    block: hex(pad(plaintext)),
    encrypted: hex(block)
  })
}

// System order hash, copied verbatim from autobee/lib/system.js:503-522.
const HASH = c.fixed(sodium.crypto_shorthash_BYTES)
const HASH_KEY = b4a.from('autobee-orderkey')
const HASH_SEED = b4a.alloc(sodium.crypto_shorthash_BYTES)

function advanceHash(prev, node) {
  const state = { start: 0, end: 0, buffer: null }

  HASH.preencode(state, prev || HASH_SEED)
  c.fixed32.preencode(state, node.key)
  c.uint.preencode(state, node.length)
  c.uint.preencode(state, node.weight)

  state.buffer = b4a.allocUnsafe(state.end)

  HASH.encode(state, prev || HASH_SEED)
  c.fixed32.encode(state, node.key)
  c.uint.encode(state, node.length)
  c.uint.encode(state, node.weight)

  const hash = b4a.allocUnsafe(sodium.crypto_shorthash_BYTES)
  sodium.crypto_shorthash(hash, state.buffer, HASH_KEY)

  return hash
}

const lengths = [0, 1, 2, 251, 252, 253, 254, 255, 256, 65534, 65535, 65536, 2 ** 32 - 1, 2 ** 32, 2 ** 40, 2 ** 51, 2 ** 52]
const weights = [0, 1, 2, 3, 7, 252, 253, 65535, 65536, 2 ** 32 - 1, 2 ** 32, 2 ** 52]

const orderHash = []
for (let i = 0; i < 60; i++) {
  const prev = i % 5 === 0 ? null : orderHash[i - 1].hash
  const node = {
    key: key(`order-key-${i}`),
    length: lengths[i % lengths.length],
    weight: weights[(i * 7 + 3) % weights.length]
  }
  const hash = advanceHash(prev ? b4a.from(prev, 'hex') : null, node)
  orderHash.push({
    prev,
    key: hex(node.key),
    length: node.length,
    weight: node.weight,
    hash: hex(hash)
  })
}

const out = {
  constants: {
    padding: PADDING,
    default_manifest_version: DEFAULT_MANIFEST_VERSION
  },
  cases,
  anchors,
  order_hash: orderHash
}

if (n !== cases.length) throw new Error('case counter drift')
process.stdout.write(JSON.stringify(out, null, 2) + '\n')
