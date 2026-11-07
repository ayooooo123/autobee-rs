// Records `autobee/lib/boot.js` results for the boot_apply test.
//
// A scenario sets up a store (its own temporary directory, a fixed public
// primary key), optional preset user data, then runs boot once and records the
// resolved cores, the stored user data and the decoded boot record. The Rust
// side replays the same primary key and asserts the same bytes.
//
// Run with `npm install && node boot.mjs`.

import fs from 'fs'
import os from 'os'
import path from 'path'
import { createRequire } from 'module'

const require = createRequire(import.meta.url)
const b4a = require('b4a')
const Corestore = require('corestore')
const boot = require('autobee/lib/boot.js')
const E = require('autobee/lib/encoding.js')

const ROOT = path.join(import.meta.dirname, '..', '..')

const USER_DATA = [
  'referrer',
  'autobee/local',
  'autobee/head',
  'autobee/encryption',
  'autobee/previous-drain',
  'autobase/local',
  'autobase/boot'
]

const hex = (buf) => (buf ? b4a.toString(buf, 'hex') : null)

async function userData (core) {
  const out = {}
  for (const key of USER_DATA) out[key] = hex(await core.getUserData(key))
  return out
}

function describeHead (head) {
  return head ? { key: hex(head.key), length: head.length } : null
}

async function preset (scenario, core, values, open) {
  scenario.preset.push({ key: hex(core.key), ...open, userData: values })
  for (const [key, value] of Object.entries(values)) {
    await core.setUserData(key, value === null ? null : b4a.from(value, 'hex'))
  }
}

async function run (name, config, primaryKey) {
  const manifestVersion = config.manifestVersion || 1
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'autobee-boot-'))
  const store = new Corestore(dir, { primaryKey, manifestVersion, unsafe: true })
  await store.ready()

  const scenario = { name, primaryKey: hex(primaryKey), manifestVersion, preset: [] }

  let key = null
  if (config.setup) {
    const result = await config.setup(store, scenario)
    if (result && result.key) key = result.key
  }

  if (config.preBoot) {
    const first = await boot(store, null, {})
    await first.local.close()
    await first.bootstrap.close()
  }

  const keyPair = config.keyPairName ? await store.createKeyPair(config.keyPairName) : null
  const encryptionKey = config.encryptionKey ? b4a.from(config.encryptionKey, 'hex') : null
  const options = { keyPair, encryptionKey, encrypt: config.encrypt === true }

  scenario.boot = {
    key: hex(key),
    keyPairName: config.keyPairName || null,
    keyPairPublicKey: keyPair ? hex(keyPair.publicKey) : null,
    encryptionKey: config.encryptionKey || null,
    encrypt: config.encrypt === true
  }

  let result = null
  let error = null
  try {
    result = await boot(store, key, options)
  } catch (err) {
    error = err.message
  }

  if (error) {
    scenario.error = error
  } else {
    scenario.expect = {
      key: hex(result.key),
      localKey: hex(result.local.key),
      bootstrapKey: hex(result.bootstrap.key),
      encryptionKey: hex(result.encryptionKey),
      system: describeHead(result.system),
      previousDrain: result.previousDrain,
      localUserData: await userData(result.local),
      bootstrapUserData: await userData(result.bootstrap)
    }
  }

  await store.close()
  fs.rmSync(dir, { recursive: true, force: true })

  return scenario
}

async function coreByKeyPair (store, scenario, name, values = {}) {
  const core = store.get({ keyPair: await store.createKeyPair(name), active: false })
  await core.ready()
  await preset(scenario, core, values, { keyPairName: name })
  return core
}

const scenarios = [
  // a brand new store: local is the namespace local core, bootstrap is a
  // session of it
  (primaryKey) => run('fresh-local', {}, primaryKey),

  // a remote channel: the key core is already held writable, local is the
  // namespace local core
  (primaryKey) => run('remote-key', {
    setup: async (store, scenario) => {
      const core = await coreByKeyPair(store, scenario, 'bootstrap')
      return { key: core.key }
    }
  }, primaryKey),

  // a writer whose local core is recorded on the bootstrap core: the record
  // wins over the namespace local core
  (primaryKey) => run('local-pointer', {
    setup: async (store, scenario) => {
      const local = await coreByKeyPair(store, scenario, 'recorded-local')
      const bootstrap = await coreByKeyPair(store, scenario, 'bootstrap', { 'autobee/local': hex(local.key) })
      return { key: bootstrap.key }
    }
  }, primaryKey),

  // a founder: local is the core the key pair names, bootstrap is a session
  (primaryKey) => run('keypair-v1', { keyPairName: 'fixture' }, primaryKey),

  // same, on a store that signs manifest version 3
  (primaryKey) => run('keypair-v3', { manifestVersion: 3, keyPairName: 'fixture' }, primaryKey),

  // a caller-supplied encryption key is stored
  (primaryKey) => run('encryption-key', { encryptionKey: '07'.repeat(32) }, primaryKey),

  // encrypt with no key: the encryption key pair is derived from the store
  (primaryKey) => run('encryption-generated', { encrypt: true }, primaryKey),

  // a stored encryption key is read back and not overwritten
  (primaryKey) => run('encryption-stored', {
    setup: async (store, scenario) => {
      const local = store.get({ name: 'local', active: false })
      await local.ready()
      await preset(scenario, local, { 'autobee/encryption': '09'.repeat(32) }, { name: 'local' })
    }
  }, primaryKey),

  // the previous drain offset round-trips through its stored form
  (primaryKey) => run('previous-drain', {
    setup: async (store, scenario) => {
      const local = store.get({ name: 'local', active: false })
      await local.ready()
      await preset(scenario, local, { 'autobee/previous-drain': hex(E.encodePreviousDrain(1234)) }, { name: 'local' })
    }
  }, primaryKey),

  // the stored boot record decodes into the system head, byte for byte
  (primaryKey) => run('system-head', {
    setup: async (store, scenario) => {
      const local = store.get({ name: 'local', active: false })
      await local.ready()
      const record = E.encodeBootRecord({ key: Buffer.alloc(32, 3), length: 42 })
      await preset(scenario, local, { 'autobee/head': hex(record) }, { name: 'local' })
    }
  }, primaryKey),

  // legacy autobase storage is refused, not migrated
  (primaryKey) => run('legacy-autobase', {
    setup: async (store, scenario) => {
      const local = store.get({ name: 'local', active: false })
      await local.ready()
      await preset(scenario, local, { 'autobase/boot': '0102' }, { name: 'local' })
    }
  }, primaryKey),

  // a local core that remembers its channel: the referrer names the bootstrap
  (primaryKey) => run('referrer', {
    setup: async (store, scenario) => {
      const bootstrap = await coreByKeyPair(store, scenario, 'bootstrap')
      const local = store.get({ name: 'local', active: false })
      await local.ready()
      await preset(scenario, local, { referrer: hex(bootstrap.key) }, { name: 'local' })
    }
  }, primaryKey),

  // a second boot reads the referrer the first one stored
  (primaryKey) => run('reboot', { preBoot: true }, primaryKey)
]

const out = { schema: 1, scenarios: [] }
for (const [i, make] of scenarios.entries()) {
  out.scenarios.push(await make(Buffer.alloc(32, 0x40 + i)))
}

fs.writeFileSync(path.join(ROOT, 'fixtures/boot-reference.json'), JSON.stringify(out, null, 1) + '\n')
