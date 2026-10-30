import net from 'node:net'
import { mkdir, lstat, chmod } from 'node:fs/promises'
import { dirname, isAbsolute } from 'node:path'
import { randomUUID } from 'node:crypto'

const MAX_FRAME = 8 * 1024 * 1024

/** Start the local owner-only socket and observe committed replies from one Bot.
 * @param {object} ctx - Host plugin Context.
 * @param {object} config - Validated socket, Bot, and timeout settings.
 * @returns {Promise<Function>} asynchronous teardown.
 */
export async function startBridge(ctx, config) {
  if (!isAbsolute(config.socketPath)) throw new Error('socketPath must be absolute')
  const parent = dirname(config.socketPath)
  await mkdir(parent, { recursive: true, mode: 0o700 })
  const dir = await lstat(parent)
  if (!dir.isDirectory() || dir.isSymbolicLink() || dir.uid !== process.getuid() || (dir.mode & 0o077)) {
    throw new Error('Socket directory must be owned by this user with mode 0700')
  }
  try {
    await lstat(config.socketPath)
    throw new Error(`Socket already exists: ${config.socketPath}; stop its owner before removing a stale socket`)
  } catch (error) {
    if (error.code !== 'ENOENT') throw error
  }
  const peer = ctx.connection.createPeer({ principal: { kind: 'owner' }, interactions: 'none' })
  const handler = ctx.connection.createSharedFetchHandler('/api')
  let active
  let queue = Promise.resolve()
  let disposed = false
  const sockets = new Set()

  async function bot() {
    const response = await handler.fetch(new Request('http://localhost/api/bot', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ endpoint: 'state', payload: {} }),
      signal: AbortSignal.timeout(config.lookupTimeoutMs),
    }), peer)
    if (!response.ok) throw new Error(`DS Bot state returned HTTP ${response.status}`)
    const result = await response.json()
    if (!result.ok) throw new Error(result.error?.message || 'DS Bot state failed')
    if (result.value.readOnly) throw new Error('DS Bot runtime is read-only')
    const selected = result.value.bots.find(item => item.id === config.botId)
    if (!selected?.sessionId) throw new Error(`Configured Bot is unavailable: ${config.botId}`)
    return { id: selected.id, name: selected.name, sessionId: selected.sessionId }
  }

  const off = ctx.on('session/event', (session, event) => {
    const task = active
    if (!task || session.id !== task.sessionId) return
    const data = event.data
    if (event.type === 'turn/start') task.currentTurn = data.turn
    if (event.type === 'user/message' && !['runtime-context', 'time-context', 'agent-instructions'].includes(data.source?.kind)) {
      const own = data.source?.rpcId === task.rpcId
      if (own) {
        task.turn = task.currentTurn
        task.matched = true
        if (task.usersInTurn) task.ambiguous = true
      } else {
        if (task.matched && task.currentTurn === task.turn) task.ambiguous = true
        task.usersInTurn = true
      }
    }
    if (event.type === 'turn/start') task.usersInTurn = false
    if (event.type === 'assistant/message' && task.matched && data.turn === task.turn) {
      const text = data.message.content.filter(block => block.type === 'text').map(block => block.text).join('')
      if (text) task.texts.push(text)
    }
    if (event.type === 'turn/end' && task.matched && data.turn === task.turn) {
      if (task.ambiguous) task.reject(new Error('Another message joined this turn; read the combined reply in DSH'))
      else if (data.reason.kind === 'completed') task.resolve(task.texts)
      else task.reject(new Error(`Artist turn ended: ${data.reason.kind}${data.reason.error?.message ? `: ${data.reason.error.message}` : ''}`))
    }
  })

  function send(socket, frame, end = false) {
    if (socket.destroyed) return
    const value = JSON.stringify(frame) + '\n'
    if (end) socket.end(value)
    else socket.write(value)
  }

  async function prompt(socket, request) {
    if (disposed || socket.destroyed) return
    const selected = await bot()
    const rpcId = `openbot-artist-${randomUUID()}`
    let timer
    const completion = new Promise((resolve, reject) => {
      active = { sessionId: selected.sessionId, rpcId, texts: [], matched: false, resolve, reject }
      timer = setTimeout(() => reject(new Error('Timed out waiting for Artist; check DSH before retrying')), config.promptTimeoutMs)
    })
    // Observe a rejection immediately while admission is still awaiting a resumed Agent.
    completion.catch(() => {})
    try {
      await ctx.sessionController.prompt({ requestId: rpcId, sessionId: selected.sessionId,
        mode: 'queue', content: [{ type: 'text', text: request.text }], clientTimeZone: config.timeZone,
      }, AbortSignal.timeout(config.lookupTimeoutMs))
      const texts = await completion
      for (const text of texts) send(socket, { type: 'text', text })
      send(socket, { type: 'done', stopReason: 'end_turn' }, true)
    } finally {
      clearTimeout(timer)
      active = undefined
    }
  }

  const server = net.createServer(socket => {
    sockets.add(socket)
    socket.on('close', () => sockets.delete(socket))
    socket.on('error', () => { /* A disconnected ACP reader does not cancel the shared Bot. */ })
    socket.setEncoding('utf8')
    socket.setTimeout(config.promptTimeoutMs, () => socket.destroy())
    let buffer = ''
    const receive = chunk => {
      buffer += chunk
      if (Buffer.byteLength(buffer) > MAX_FRAME) { socket.destroy(); return }
      const newline = buffer.indexOf('\n')
      if (newline < 0) return
      socket.off('data', receive)
      let request
      try {
        request = JSON.parse(buffer.slice(0, newline))
        if (!request || typeof request !== 'object') throw new Error('Request must be an object')
        if (request.op === 'status') {
          bot().then(value => send(socket, { bot: value }, true)).catch(error => send(socket, { type: 'error', message: error.message }, true))
        } else if (request.op === 'prompt') {
          if (typeof request.text !== 'string' || !request.text.trim() || typeof request.requestId !== 'string') throw new Error('prompt requires text and requestId')
          queue = queue.then(() => prompt(socket, request)).catch(error => send(socket, { type: 'error', message: error.message }, true))
        } else throw new Error('Unknown bridge operation')
      } catch (error) { send(socket, { type: 'error', message: error.message }, true) }
    }
    socket.on('data', receive)
  })
  try {
    await new Promise((resolve, reject) => {
      server.once('error', reject)
      server.listen(config.socketPath, () => { server.off('error', reject); resolve() })
    })
    await chmod(config.socketPath, 0o600)
  } catch (error) {
    off()
    server.close()
    await peer.dispose()
    throw error
  }
  // Node removes the Unix socket on close; no manual unlink can remove another generation's socket.
  return async () => {
    disposed = true
    off()
    active?.reject(new Error('DSH Artist bridge stopped'))
    for (const socket of sockets) socket.destroy()
    await new Promise(resolve => server.close(resolve))
    await queue
    await peer.dispose()
  }
}
