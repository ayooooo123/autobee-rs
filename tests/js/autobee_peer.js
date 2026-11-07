// npm autobee 2.12.1 peer for tests/interop.rs.
//
//   node autobee_peer.js owner <dir> <bootstrap-port> [fast-forward]
//   node autobee_peer.js peer  <dir> <bootstrap-port> <channel-hex>
//
// Handlers mirror PearTube: the owner grants and revokes writers and applies
// `put` ops; the update hook is a no-op. stdin drives appends, hints and state
// dumps; stdout carries one `event payload` line per command.
const Corestore = require('corestore')
const Autobee = require('autobee')
const Hyperswarm = require('hyperswarm')
const crypto = require('hypercore-crypto')
const encoding = require('autobee/lib/encoding.js')
const readline = require('readline')
const fs = require('fs')
const path = require('path')

const log = (line) => process.stdout.write(line + '\n')
const hex = (b) => (b == null ? null : Buffer.from(b).toString('hex'))
const [mode, ...args] = process.argv.slice(2)
const ffFlag = mode === 'owner' ? args[2] : args[3]

let store = null
let db = null
let swarm = null

function handlers() {
  return {
    optimistic: true,
    fastForward: ffFlag === 'fast-forward' ? {} : false,
    async apply(batch, view, host) {
      const w = view.write()
      for (const n of batch) {
        if (!n.value || n.value.length === 0) continue
        let op = null
        try {
          op = JSON.parse(n.value.toString())
        } catch {
          continue
        }
        const isOwner = Buffer.compare(n.key, db.key) === 0
        if (isOwner && op.type === 'grant') {
          await host.addWriter(Buffer.from(op.writer, 'hex'), { isIndexer: false })
          w.tryPut(Buffer.from('writer/' + op.writer), Buffer.from('writer'))
        } else if (isOwner && op.type === 'revoke') {
          await host.removeWriter(Buffer.from(op.writer, 'hex'))
          w.tryDelete(Buffer.from('writer/' + op.writer))
        } else if (op.type === 'put') {
          w.tryPut(Buffer.from(op.key), Buffer.from(op.value))
        }
      }
      await w.flush()
    },
    update() {}
  }
}

async function rows(bee) {
  const out = []
  for await (const row of bee.createReadStream()) {
    out.push({ key: hex(row.key), value: hex(row.value) })
  }
  return out
}

async function blocks(core) {
  const out = []
  for (let i = 0; i < core.length; i++) out.push(hex(await core.get(i)))
  return out
}

async function state() {
  const info = await db.system.getInfo()
  const head = db.system.bee.head()
  const view = db.system.view
  return {
    key: hex(db.key),
    localKey: hex(db.local.key),
    localLength: db.local.length,
    systemHead: head && { key: hex(head.key), length: head.length },
    viewHead: view && { key: hex(view.key), length: view.length },
    info: info && {
      version: info.version,
      timestamp: info.timestamp,
      flushes: info.flushes,
      heads: info.heads.map((h) => ({ key: hex(h.key), length: h.length })),
      hash: hex(info.hash),
      pending: info.pending || null
    },
    system: await rows(db.system.bee),
    view: await rows(db.view),
    localBlocks: await blocks(db.local)
  }
}

async function main() {
  const [dir, port, channel] = args
  const bootstrap = [{ host: '127.0.0.1', port: Number(port) }]
  const primaryKey = Buffer.alloc(32, mode === 'owner' ? 0x6f : 0x6a)
  store = new Corestore(fs.mkdtempSync(path.join(dir, mode + '-')), { primaryKey, unsafe: true })

  db =
    mode === 'owner'
      ? new Autobee(store, handlers())
      : new Autobee(store, Buffer.from(channel, 'hex'), handlers())
  await db.ready()
  log('ready ' + JSON.stringify({ key: hex(db.key), localKey: hex(db.local.key) }))

  swarm = new Hyperswarm({ bootstrap })
  swarm.on('connection', (conn) => {
    db.replicate(conn)
  })
  await swarm.join(crypto.discoveryKey(db.key), { server: true, client: true }).flushed()
  log('joined')

  const lines = readline.createInterface({ input: process.stdin })
  for await (const line of lines) {
    const trimmed = line.trim()
    if (!trimmed) continue
    const [command, ...rest] = trimmed.split(' ')
    try {
      if (command === 'append') {
        const op = rest.join(' ')
        await db.append(Buffer.from(op))
        await db.flush()
        log('appended ' + db.local.length)
      } else if (command === 'grant' || command === 'revoke') {
        await db.append(Buffer.from(JSON.stringify({ type: command, writer: rest[0] })))
        await db.flush()
        log('appended ' + db.local.length)
      } else if (command === 'hint') {
        await db.wakeup({ key: Buffer.from(rest[0], 'hex'), length: Number(rest[1]) })
        log('hinted')
      } else if (command === 'writable') {
        log('writable ' + (db.writable ? 'true' : 'false'))
      } else if (command === 'state') {
        log('state ' + JSON.stringify(await state()))
      } else if (command === 'blocks') {
        const core = store.get({ key: Buffer.from(rest[0], 'hex'), active: false })
        await core.ready()
        const out = { key: rest[0], blocks: await blocks(core) }
        await core.close()
        log('blocks ' + JSON.stringify(out))
      } else if (command === 'close') {
        await db.close()
        await store.close()
        log('closed')
        process.exit(0)
      }
    } catch (err) {
      log('error ' + (err && err.stack ? err.stack : String(err)))
    }
  }
  process.exit(0)
}

main().catch((err) => {
  log('error ' + (err && err.stack ? err.stack : String(err)))
  process.exit(1)
})
