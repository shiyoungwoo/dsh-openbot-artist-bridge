# dsh-openbot-artist-bridge

Connect an **OpenBot** chat to an existing **DS Bot** inside DeepSeek Harness (DSH). OpenBot drives the Bot over the Agent Client Protocol (ACP); this bridge forwards that conversation into a DS Bot session you already run in DSH, so the Bot keeps one identity, one memory, and one workspace across both apps.

[中文说明](#中文说明)

## What it is, and what it is not

- It is a **front-end adapter**. It does not create a Bot, does not start its own DSH process, and does not take ownership of any session.
- The Bot it reaches must already exist in DSH (the DS Bot plugin). The bridge only relays prompts and streams the reply back.

## Architecture

```
OpenBot Desktop
   │  ACP over stdin/stdout (newline-delimited JSON-RPC)
   ▼
acp_bridge.py  ──┐
                 │  AF_UNIX socket, newline-delimited JSON
                 ▼
runtime.mjs  (host half inside DSH)
   │  shared fetch /api/bot  +  sessionController.prompt
   ▼
DS Bot session in DSH
```

- `runtime.mjs` owns an owner-only AF_UNIX socket (parent directory `0700`, socket `0600`). It resolves the configured Bot over DSH's shared connection, sends prompts with `mode: 'queue'`, and correlates each `session/event` back to its request by `rpcId`.
- `acp_bridge.py` is a standalone ACP frontend that OpenBot launches. It connects to the socket as a client, advertises only what it supports (text and `resource_link` prompts), and cancels a prompt by closing that prompt's own socket — never a global cancel, so unrelated Bot work keeps running.

## Requirements

- DSH `>=0.2.1-rc.3 <0.3.0` with the DS Bot plugin and at least one Bot session.
- Node `>=22.19`.
- Python 3.9+ for the ACP frontend.
- macOS or Linux (AF_UNIX sockets).

## Setup

1. **Install the host half** into a DSH profile:

   ```sh
   pnpm add link:/path/to/dsh-openbot-artist-bridge
   # or: pnpm add github:shiyoungwoo/dsh-openbot-artist-bridge
   ```

2. **Configure it.** Copy [`cordis.patch.yml`](cordis.patch.yml) into your profile's bundle list, or override the row in your own profile patch:

   ```yaml
   - id: local-openbot-artist-bridge
     name: '@local/dsh-openbot-artist-bridge'
     config:
       botId: session-your-bot-session-id
       socketPath: /absolute/path/to/dsh-openbot-artist-bridge/runtime/bridge.sock
       timeZone: Asia/Shanghai
   ```

   Optional: `lookupTimeoutMs` (default `15000`), `promptTimeoutMs` (default `3600000`).

3. **Register the ACP frontend in OpenBot** as a custom agent. It runs over stdin/stdout, so the command is:

   ```json
   {
     "id": "dsh",
     "name": "DeepSeek Harness",
     "command": "/usr/bin/python3",
     "args": ["/absolute/path/to/dsh-openbot-artist-bridge/acp_bridge.py"]
   }
   ```

   Pass `--socket <path>` to `acp_bridge.py` if you changed `socketPath`; otherwise it defaults to `runtime/bridge.sock` beside the script.

4. **Restart DSH** (or toggle the bundle in the Plugins page), then talk to the Bot from OpenBot.

## Security notes

- The socket is owner-only. A prompt from another DSH user that joins the same turn is detected and rejected with a readable message instead of being merged silently.
- Frames are capped at 8 MiB.
- Configuration values (`botId`, `socketPath`) identify one machine's Bot; keep your real values in a profile override, not in a shared repository.

## Tests

```sh
npm test        # host half: real AF_UNIX socket, queueing, rpcId correlation, security cases
npm run test:acp  # ACP frontend: real stdin/stdout against a fake socket server
```

## Files

- `index.js` — plugin entry; declares config and starts the bridge in an effect.
- `runtime.mjs` — host half: socket server, Bot resolution, prompt relay.
- `acp_bridge.py` — standalone ACP frontend for OpenBot.
- `cordis.patch.yml` — bundle patch with placeholder config.
- `locale/*.json`, `icon.svg` — plugin metadata.
- `tests/` — host (Node) and ACP (Python) test suites.

## 中文说明

这个插件把 OpenBot 的对话接到 DSH 里已经存在的某个 DS Bot 上，让两边共用同一个 Bot、同一份记忆和工作区。它只是前端适配层：不创建 Bot、不启动自己的 DSH 进程、不接管会话。

- `runtime.mjs`（DSH 内 host 半）：创建仅属主可访问的 AF_UNIX socket（父目录 0700、socket 0600），通过 DSH 共享连接找到配置的 Bot，用 `mode: 'queue'` 投递提示，并按 `rpcId` 把 `session/event` 对回请求。
- `acp_bridge.py`（OpenBot 启动的独立进程）：在 stdin/stdout 上讲 ACP，作为客户端连到上面的 socket；只声明支持文本与 `resource_link`；取消一条提示靠断开它自己的连接，不影响 Bot 的其他工作。

配置项：`botId`（必填）、`socketPath`（必填，绝对路径）、`timeZone`（默认 Asia/Shanghai）。请把真实值写在你自己的 profile 补丁里，不要提交到仓库。

## License

MIT
