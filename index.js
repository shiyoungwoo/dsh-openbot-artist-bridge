import Schema from '@deepseek-ai/schemastery'
import { startBridge } from './runtime.mjs'

export const name = 'openbot-artist-bridge'
export const inject = ['connection', 'sessionController']
export const Config = Schema.object({
  botId: Schema.string().required(),
  socketPath: Schema.string().required(),
  timeZone: Schema.string().default('Asia/Shanghai'),
  lookupTimeoutMs: Schema.number().min(1000).default(15000),
  promptTimeoutMs: Schema.number().min(1000).default(3600000),
})

/** Mount the local ACP bridge with its socket and observation lifecycle.
 * @param {object} ctx - Host Context with connection and Session services.
 * @param {object} config - Validated bridge configuration.
 */
export function apply(ctx, config) {
  ctx.effect(() => startBridge(ctx, config))
}
