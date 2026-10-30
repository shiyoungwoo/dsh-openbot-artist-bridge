// Host-side tests for runtime.mjs startBridge(ctx, config).
//
// The bridge is exercised over a real AF_UNIX socket. Only the DSH host
// boundary is faked: ctx.connection, ctx.sessionController, and the
// ctx.on('session/event') callback (captured so the test drives committed
// session events exactly as the host would). No real Bot, DSH process, or
// network is involved.
import { test } from 'node:test'
import assert from 'node:assert/strict'
import net from 'node:net'
import { once } from 'node:events'
import { mkdtemp, rm, lstat, chmod, writeFile } from 'node:fs/promises'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { startBridge } from '../runtime.mjs'

const TESTS_DIR = dirname(fileURLToPath(import.meta.url))

// --------------------------------------------------------------------------- //
// Fake host boundary
// --------------------------------------------------------------------------- //

const BOT = { id: 'bot-1', name: 'Artist', sessionId: 'sess-1' }

function defaultBotBody() {
  return { ok: true, value: { readOnly: false, bots: [BOT] } }
}

/** Build a fake ctx plus the captured session/event emitter and call log. */
function createHarness(options = {}) {
  const listeners = []
  const peers = []
  const promptCalls = []
  const fetchCalls = []
  let offCount = 0

  function emitEvent(session, event) {
    for (const { cb } of listeners) cb(session, event)
  }

  const ctx = {
    connection: {
      createPeer(opts) {
        const peer = {
          opts,
          disposed: false,
          async dispose() { peer.disposed = true },
        }
        peers.push(peer)
        return peer
      },
      createSharedFetchHandler(prefix) {
        return {
          async fetch(request, peer) {
            fetchCalls.push({ request, peer, prefix })
            const result = options.respond
              ? await options.respond({ request, peer })
              : { status: 200, body: defaultBotBody() }
            return new Response(JSON.stringify(result.body ?? {}), {
              status: result.status ?? 200,
              headers: { 'content-type': 'application/json' },
            })
          },
        }
      },
    },
    sessionController: {
      async prompt(request, signal) {
        promptCalls.push({ request, signal })
        if (options.onPrompt) return options.onPrompt(request, { emit: emitEvent })
        simpleOnPrompt(request, { emit: emitEvent })
      },
    },
    on(name, cb) {
      assert.equal(name, 'session/event', 'bridge must only observe session/event')
      const entry = { cb }
      listeners.push(entry)
      return () => {
        offCount += 1
        const index = listeners.indexOf(entry)
        if (index >= 0) listeners.splice(index, 1)
      }
    },
  }

  return {
    ctx,
    peers,
    promptCalls,
    fetchCalls,
    emitEvent,
    get offCount() { return offCount },
    get listenerCount() { return listeners.length },
  }
}

/** Emit one committed session event to the captured callback. */
function ev(harness, sessionId, type, data) {
  harness.emitEvent({ id: sessionId }, { type, data })
}

/** Emit a normal completed turn for the given prompt request. */
function simpleOnPrompt(request, { emit }) {
  const text = `reply:${request.content[0].text}`
  const turn = 1
  emit({ id: request.sessionId }, { type: 'turn/start', data: { turn } })
  emit({ id: request.sessionId }, { type: 'user/message', data: { turn, source: { rpcId: request.requestId } } })
  emit({ id: request.sessionId }, { type: 'assistant/message', data: { turn, message: { content: [{ type: 'text', text }] } } })
  emit({ id: request.sessionId }, { type: 'turn/end', data: { turn, reason: { kind: 'completed' } } })
}

// --------------------------------------------------------------------------- //
// Real AF_UNIX client helpers
// --------------------------------------------------------------------------- //

function makeConfig(socketPath, overrides = {}) {
  return {
    botId: 'bot-1',
    socketPath,
    timeZone: 'Asia/Shanghai',
    lookupTimeoutMs: 5000,
    promptTimeoutMs: 5000,
    ...overrides,
  }
}

async function makeTempDir() {
  return mkdtemp(join(TESTS_DIR, 'tmp-'))
}

/** Open a socket and write one newline-delimited JSON request on connect. */
function openClient(socketPath, payload) {
  const socket = net.createConnection(socketPath)
  socket.setEncoding('utf8')
  const frames = []
  let buffer = ''
  let settled = false
  const done = new Promise((resolve, reject) => {
    const finish = (err) => {
      if (settled) return
      settled = true
      socket.destroy()
      if (err) reject(err)
      else resolve(frames)
    }
    socket.on('connect', () => socket.write(JSON.stringify(payload) + '\n'))
    socket.on('data', chunk => {
      buffer += chunk
      let index
      while ((index = buffer.indexOf('\n')) >= 0) {
        const line = buffer.slice(0, index)
        buffer = buffer.slice(index + 1)
        frames.push(JSON.parse(line))
      }
    })
    socket.on('end', () => finish())
    socket.on('close', () => finish())
    socket.on('error', err => finish(err))
  })
  return { socket, frames, done }
}

/** Send one request and collect every frame until the server ends the socket. */
async function request(socketPath, payload, timeoutMs = 4000) {
  const client = openClient(socketPath, payload)
  return withTimeout(client.done, timeoutMs, 'request')
}

function withTimeout(promise, ms, label) {
  let timer
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms)
    timer.unref?.()
  })
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer))
}

const delay = ms => new Promise(resolve => { const t = setTimeout(resolve, ms); t.unref?.() })

/** Start a bridge against a fresh temp dir; returns the harness and teardown. */
async function setup(options = {}) {
  const dir = await makeTempDir()
  const socketPath = join(dir, 'bridge.sock')
  const harness = createHarness(options)
  const teardown = await startBridge(harness.ctx, makeConfig(socketPath, options.config))
  const close = async () => {
    try {
      await teardown()
    } finally {
      await rm(dir, { recursive: true, force: true })
    }
  }
  return { dir, socketPath, harness, teardown, close }
}

// --------------------------------------------------------------------------- //
// Tests
// --------------------------------------------------------------------------- //

test('status returns the configured bot from a single host fetch', async () => {
  const { socketPath, harness, close } = await setup()
  try {
    const frames = await request(socketPath, { op: 'status' })

    assert.deepEqual(frames, [{ bot: BOT }])
    assert.equal(harness.fetchCalls.length, 1)
    const { request: fetchRequest, prefix } = harness.fetchCalls[0]
    assert.equal(prefix, '/api')
    assert.equal(fetchRequest.method, 'POST')
    assert.equal(fetchRequest.url, 'http://localhost/api/bot')

    // Peer was created exactly as the owner-only host principal expects.
    assert.equal(harness.peers.length, 1)
    assert.deepEqual(harness.peers[0].opts, { principal: { kind: 'owner' }, interactions: 'none' })
  } finally {
    await close()
  }
})

test('queue processes concurrent prompts strictly in order', async () => {
  const { socketPath, harness, close } = await setup({
    onPrompt: async (request, { emit }) => {
      const text = request.content[0].text
      if (text === 'A') await delay(80)
      simpleOnPrompt(request, { emit })
    },
  })
  try {
    // A connects and sends first; B follows before A can finish.
    const a = openClient(socketPath, { op: 'prompt', text: 'A', requestId: 'client-a' })
    await once(a.socket, 'connect')
    const b = openClient(socketPath, { op: 'prompt', text: 'B', requestId: 'client-b' })
    await once(b.socket, 'connect')

    const order = []
    a.done.then(() => order.push('A'))
    b.done.then(() => order.push('B'))

    const [aFrames, bFrames] = await Promise.all([
      withTimeout(a.done, 4000, 'prompt A'),
      withTimeout(b.done, 4000, 'prompt B'),
    ])

    // The slow first turn must fully finish before the second prompt starts.
    assert.deepEqual(order, ['A', 'B'])
    assert.deepEqual(aFrames, [{ type: 'text', text: 'reply:A' }, { type: 'done', stopReason: 'end_turn' }])
    assert.deepEqual(bFrames, [{ type: 'text', text: 'reply:B' }, { type: 'done', stopReason: 'end_turn' }])
    assert.equal(harness.promptCalls.length, 2)
    assert.deepEqual(harness.promptCalls.map(call => call.request.content[0].text), ['A', 'B'])
  } finally {
    await close()
  }
})

test('correlates events by rpcId and ignores an unrelated turn', async () => {
  const { socketPath, close } = await setup({
    onPrompt: (request, { emit }) => {
      // An unrelated turn that must not leak into this prompt's reply.
      ev({ emitEvent: emit }, 'sess-1', 'turn/start', { turn: 1 })
      ev({ emitEvent: emit }, 'sess-1', 'user/message', { turn: 1, source: { rpcId: 'someone-else' } })
      ev({ emitEvent: emit }, 'sess-1', 'assistant/message', { turn: 1, message: { content: [{ type: 'text', text: 'NOISE' }] } })
      ev({ emitEvent: emit }, 'sess-1', 'turn/end', { turn: 1, reason: { kind: 'completed' } })

      // The real turn: only this rpcId matches.
      ev({ emitEvent: emit }, 'sess-1', 'turn/start', { turn: 2 })
      ev({ emitEvent: emit }, 'sess-1', 'user/message', { turn: 2, source: { rpcId: request.requestId } })
      ev({ emitEvent: emit }, 'sess-1', 'assistant/message', { turn: 2, message: { content: [{ type: 'text', text: 'REAL' }] } })
      ev({ emitEvent: emit }, 'sess-1', 'turn/end', { turn: 2, reason: { kind: 'completed' } })
    },
  })
  try {
    const frames = await request(socketPath, { op: 'prompt', text: 'hi', requestId: 'r-1' })
    assert.deepEqual(frames, [{ type: 'text', text: 'REAL' }, { type: 'done', stopReason: 'end_turn' }])
  } finally {
    await close()
  }
})

test('rejects when a foreign user joins the matched turn', async () => {
  const { socketPath, close } = await setup({
    onPrompt: (request, { emit }) => {
      const sessionId = 'sess-1'
      ev({ emitEvent: emit }, sessionId, 'turn/start', { turn: 1 })
      ev({ emitEvent: emit }, sessionId, 'user/message', { turn: 1, source: { rpcId: request.requestId } })
      // Another person's message joins the same turn after ours.
      ev({ emitEvent: emit }, sessionId, 'user/message', { turn: 1, source: { rpcId: 'foreign-user' } })
      ev({ emitEvent: emit }, sessionId, 'assistant/message', { turn: 1, message: { content: [{ type: 'text', text: 'combined' }] } })
      ev({ emitEvent: emit }, sessionId, 'turn/end', { turn: 1, reason: { kind: 'completed' } })
    },
  })
  try {
    const frames = await request(socketPath, { op: 'prompt', text: 'hi', requestId: 'r-1' })
    assert.deepEqual(frames, [{ type: 'error', message: 'Another message joined this turn; read the combined reply in DSH' }])
  } finally {
    await close()
  }
})

test('detects a foreign user that preceded the matched message in the same turn', async () => {
  const { socketPath, close } = await setup({
    onPrompt: (request, { emit }) => {
      const sessionId = 'sess-1'
      ev({ emitEvent: emit }, sessionId, 'turn/start', { turn: 1 })
      // Foreign message arrives first, our own message second.
      ev({ emitEvent: emit }, sessionId, 'user/message', { turn: 1, source: { rpcId: 'foreign-user' } })
      ev({ emitEvent: emit }, sessionId, 'user/message', { turn: 1, source: { rpcId: request.requestId } })
      ev({ emitEvent: emit }, sessionId, 'assistant/message', { turn: 1, message: { content: [{ type: 'text', text: 'combined' }] } })
      ev({ emitEvent: emit }, sessionId, 'turn/end', { turn: 1, reason: { kind: 'completed' } })
    },
  })
  try {
    const frames = await request(socketPath, { op: 'prompt', text: 'hi', requestId: 'r-1' })
    assert.deepEqual(frames, [{ type: 'error', message: 'Another message joined this turn; read the combined reply in DSH' }])
  } finally {
    await close()
  }
})

test('ignores runtime/time/agent-instruction user messages, but still flags a real foreign user', async () => {
  const { socketPath, harness, close } = await setup({
    onPrompt: (request, { emit }) => {
      const sessionId = 'sess-1'
      const turn = 1
      const mode = request.content[0].text
      ev({ emitEvent: emit }, sessionId, 'turn/start', { turn })
      ev({ emitEvent: emit }, sessionId, 'user/message', { turn, source: { kind: 'user', rpcId: request.requestId } })

      if (mode === 'foreign') {
        // A genuine second user must still be detected as ambiguity.
        ev({ emitEvent: emit }, sessionId, 'user/message', { turn, source: { kind: 'user', rpcId: 'foreign-user' } })
        ev({ emitEvent: emit }, sessionId, 'assistant/message', { turn, message: { content: [{ type: 'text', text: 'combined' }] } })
        ev({ emitEvent: emit }, sessionId, 'turn/end', { turn, reason: { kind: 'completed' } })
        return
      }

      // Automatic host sources arrive as user/message after the real prompt is
      // matched; they must not be mistaken for another person joining the turn.
      for (const kind of ['runtime-context', 'time-context', 'agent-instructions']) {
        ev({ emitEvent: emit }, sessionId, 'user/message', { turn, source: { kind, rpcId: `auto-${kind}` } })
      }
      ev({ emitEvent: emit }, sessionId, 'assistant/message', { turn, message: { content: [{ type: 'text', text: 'clean reply' }] } })
      ev({ emitEvent: emit }, sessionId, 'turn/end', { turn, reason: { kind: 'completed' } })
    },
  })
  try {
    // Regression: automatic sources no longer produce a false ambiguity error.
    const clean = await request(socketPath, { op: 'prompt', text: 'auto', requestId: 'r-auto' })
    assert.deepEqual(clean, [{ type: 'text', text: 'clean reply' }, { type: 'done', stopReason: 'end_turn' }])

    // Existing behaviour: a real foreign user in the matched turn is still flagged.
    const flagged = await request(socketPath, { op: 'prompt', text: 'foreign', requestId: 'r-foreign' })
    assert.deepEqual(flagged, [{ type: 'error', message: 'Another message joined this turn; read the combined reply in DSH' }])

    assert.equal(harness.promptCalls.length, 2)
  } finally {
    await close()
  }
})

test('collects only text blocks and ignores reasoning and tool blocks', async () => {
  const { socketPath, close } = await setup({
    onPrompt: (request, { emit }) => {
      const sessionId = 'sess-1'
      ev({ emitEvent: emit }, sessionId, 'turn/start', { turn: 1 })
      ev({ emitEvent: emit }, sessionId, 'user/message', { turn: 1, source: { rpcId: request.requestId } })
      ev({ emitEvent: emit }, sessionId, 'assistant/message', {
        turn: 1,
        message: {
          content: [
            { type: 'reasoning', text: 'SECRET-THOUGHT' },
            { type: 'text', text: 'Hello ' },
            { type: 'tool_use', id: 't1', name: 'search', input: { q: 'x' } },
            { type: 'text', text: 'world' },
            { type: 'tool_result', id: 't1', content: 'TOOL-OUTPUT' },
          ],
        },
      })
      ev({ emitEvent: emit }, sessionId, 'turn/end', { turn: 1, reason: { kind: 'completed' } })
    },
  })
  try {
    const frames = await request(socketPath, { op: 'prompt', text: 'hi', requestId: 'r-1' })
    assert.deepEqual(frames, [{ type: 'text', text: 'Hello world' }, { type: 'done', stopReason: 'end_turn' }])
    const serialized = JSON.stringify(frames)
    assert.ok(!serialized.includes('SECRET-THOUGHT'))
    assert.ok(!serialized.includes('TOOL-OUTPUT'))
  } finally {
    await close()
  }
})

test('reports a non-completed turn as an error instead of a reply', async () => {
  const { socketPath, close } = await setup({
    onPrompt: (request, { emit }) => {
      const sessionId = 'sess-1'
      ev({ emitEvent: emit }, sessionId, 'turn/start', { turn: 1 })
      ev({ emitEvent: emit }, sessionId, 'user/message', { turn: 1, source: { rpcId: request.requestId } })
      ev({ emitEvent: emit }, sessionId, 'assistant/message', { turn: 1, message: { content: [{ type: 'text', text: 'partial' }] } })
      ev({ emitEvent: emit }, sessionId, 'turn/end', { turn: 1, reason: { kind: 'failed', error: { message: 'boom' } } })
    },
  })
  try {
    const frames = await request(socketPath, { op: 'prompt', text: 'hi', requestId: 'r-1' })
    assert.deepEqual(frames, [{ type: 'error', message: 'Artist turn ended: failed: boom' }])
  } finally {
    await close()
  }
})

test('reports a bot state HTTP failure as an error', async () => {
  const { socketPath, harness, close } = await setup({
    respond: () => ({ status: 500, body: { error: 'down' } }),
  })
  try {
    const frames = await request(socketPath, { op: 'prompt', text: 'hi', requestId: 'r-1' })
    assert.deepEqual(frames, [{ type: 'error', message: 'DS Bot state returned HTTP 500' }])
    // The failure happens before any session prompt is admitted.
    assert.equal(harness.promptCalls.length, 0)
  } finally {
    await close()
  }
})

test('rejects a read-only bot runtime as an error', async () => {
  const { socketPath, close } = await setup({
    respond: () => ({ status: 200, body: { ok: true, value: { readOnly: true, bots: [BOT] } } }),
  })
  try {
    const frames = await request(socketPath, { op: 'prompt', text: 'hi', requestId: 'r-1' })
    assert.deepEqual(frames, [{ type: 'error', message: 'DS Bot runtime is read-only' }])
  } finally {
    await close()
  }
})

test('rejects an unknown bridge operation', async () => {
  const { socketPath, close } = await setup()
  try {
    const frames = await request(socketPath, { op: 'bogus' })
    assert.deepEqual(frames, [{ type: 'error', message: 'Unknown bridge operation' }])
  } finally {
    await close()
  }
})

test('disposal removes the listener, peer, socket file, and pending attempt', async () => {
  const { socketPath, dir, harness, teardown } = await setup({
    onPrompt: (request, { emit }) => {
      // A turn that starts but never ends; the prompt stays pending.
      ev({ emitEvent: emit }, 'sess-1', 'turn/start', { turn: 1 })
      ev({ emitEvent: emit }, 'sess-1', 'user/message', { turn: 1, source: { rpcId: request.requestId } })
    },
  })
  try {
    const client = openClient(socketPath, { op: 'prompt', text: 'hang', requestId: 'r-hang' })
    await once(client.socket, 'connect')
    await delay(30) // let the bridge admit the prompt and register the active attempt
    assert.equal(harness.promptCalls.length, 1)
    assert.equal(harness.listenerCount, 1)

    const closed = once(client.socket, 'close')
    await teardown() // must resolve even with a pending attempt, not hang
    await withTimeout(closed, 2000, 'client close')

    assert.equal(harness.offCount, 1, 'session/event listener must be detached')
    assert.equal(harness.listenerCount, 0)
    assert.equal(harness.peers.length, 1)
    assert.equal(harness.peers[0].disposed, true, 'peer must be disposed')

    // The listener is gone: a fresh connection is refused.
    await assert.rejects(new Promise((resolve, reject) => {
      const probe = net.createConnection(socketPath)
      probe.on('connect', () => { probe.destroy(); resolve() })
      probe.on('error', reject)
    }))

    // Node unlinks the AF_UNIX socket on server close.
    await assert.rejects(lstat(socketPath), error => error.code === 'ENOENT')
  } finally {
    await rm(dir, { recursive: true, force: true })
  }
})

test('rejects a relative socket path', async () => {
  const harness = createHarness()
  await assert.rejects(
    startBridge(harness.ctx, makeConfig('relative.sock')),
    /socketPath must be absolute/,
  )
})

test('rejects when the socket path already exists', async () => {
  const dir = await makeTempDir()
  try {
    const socketPath = join(dir, 'bridge.sock')
    await writeFile(socketPath, '')
    const harness = createHarness()
    await assert.rejects(
      startBridge(harness.ctx, makeConfig(socketPath)),
      /Socket already exists/,
    )
  } finally {
    await rm(dir, { recursive: true, force: true })
  }
})

test('rejects a socket directory accessible to group or others', async () => {
  const dir = await makeTempDir()
  try {
    await chmod(dir, 0o755)
    const socketPath = join(dir, 'bridge.sock')
    const harness = createHarness()
    await assert.rejects(
      startBridge(harness.ctx, makeConfig(socketPath)),
      /Socket directory must be owned by this user with mode 0700/,
    )
  } finally {
    await rm(dir, { recursive: true, force: true })
  }
})
