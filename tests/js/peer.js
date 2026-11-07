// HyperDHT testnet + npm autobee-wakeup peer for tests/wakeup.rs.
//
// node peer.js testnet
//   runs a 3-node local testnet, prints "bootstrap <port>", exits on stdin end.
// node peer.js peer <bootstrap-port> <capability-hex>
//   joins the topic, prints "joined writers <key>:<length>", then serves stdin:
//   lookup (asks the Rust peer for its writers), close, quit.
const createTestnet = require('hyperdht/testnet')
const Hyperswarm = require('hyperswarm')
const AutobeeWakeup = require('autobee-wakeup')
const Corestore = require('corestore')
const crypto = require('hypercore-crypto')
const readline = require('readline')
const fs = require('fs')
const os = require('os')
const path = require('path')

const log = (s) => process.stdout.write(s + '\n')
const [mode, ...args] = process.argv.slice(2)
const hex = (b) => b.toString('hex')
const fmt = (list) => list.map(({ key, length }) => hex(key) + ':' + length).join(',')

async function testnet() {
  const t = await createTestnet(3, { host: '127.0.0.1' })
  log('bootstrap ' + t.bootstrap[0].port)
  process.stdin.resume()
  process.stdin.on('end', () => process.exit(0))
}

async function peer() {
  const [port, capHex] = args
  const capability = Buffer.from(capHex, 'hex')
  const discoveryKey = crypto.discoveryKey(capability)
  const store = new Corestore(fs.mkdtempSync(path.join(os.tmpdir(), 'autobee-wakeup-')))

  // A stand-in bee: autobee-wakeup needs the bootstrap core, the active
  // writers map and bumpSoon. The writer core carries the announced length.
  const bootstrap = store.get({ name: 'bootstrap' })
  await bootstrap.ready()
  const writer = store.get({ name: 'writer' })
  await writer.append(['a', 'b', 'c'])
  const auto = {
    bootstrap,
    writers: { active: new Map([['writer', { isPending: false, core: writer }]]) },
    bumpSoon () {}
  }

  const wakeup = new AutobeeWakeup(auto)
  wakeup.setCapability(capability, discoveryKey)

  const swarm = new Hyperswarm({ bootstrap: [{ host: '127.0.0.1', port: Number(port) }] })
  swarm.on('connection', (conn) => {
    log('connection')
    store.replicate(conn)
    wakeup.addStream(conn)
  })
  await swarm.join(discoveryKey, { server: true, client: false }).flushed()
  log('joined writers ' + fmt([{ key: writer.key, length: writer.length }]))

  const lines = readline.createInterface({ input: process.stdin })
  for await (const line of lines) {
    const [cmd] = line.trim().split(' ')
    if (cmd === 'lookup') {
      const target = wakeup._session && wakeup._session.peers[0]
      if (!target) {
        log('no-peer')
        continue
      }
      wakeup._session.lookup(target, { hash: null })
      log('sent-lookup')
      // The answer arrives on the hint sink; hand it back to the test.
      const deadline = Date.now() + 20000
      while (wakeup._hints.size === 0 && Date.now() < deadline) {
        await new Promise((resolve) => setTimeout(resolve, 50))
      }
      const hints = [...wakeup.flush().entries()].map(([key, length]) => ({
        key: Buffer.from(key, 'hex'),
        length
      }))
      log('hints ' + fmt(hints))
    } else if (cmd === 'close') {
      await swarm.destroy()
      await wakeup.close()
      await store.close()
      log('closed')
      process.exit(0)
    } else if (cmd === 'quit') {
      process.exit(0)
    }
  }
}

;({ testnet, peer })[mode]().catch((err) => {
  log('error ' + err.stack)
  process.exit(1)
})
