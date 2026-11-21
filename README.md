# dsh-opensandbox

OpenSandbox executor for DeepSeek Harness (DSH). Every `bash` tool call runs inside an OpenSandbox sandbox through its execd API — same commands, same tool cards, but the process runs on the sandbox side instead of the harness host.

```text
agent ── bash tool ──→ ctx.shell ──→ dsh-opensandbox ──HTTP──→ execd (inside OpenSandbox)
```

## What it is

`dsh-opensandbox` is an alternative implementation of the `ctx.shell` capability seam. It registers an executor that maps shell commands onto execd's command API:

- `POST /command` (SSE) streams stdout/stderr; the sandbox enforces the request deadline server-side;
- a deadline kill is reported as `CommandTimeout` and classified as `timedOut`;
- caller aborts interrupt the run via `DELETE /command?id=…` and are classified as `aborted`;
- `X-EXECD-ACCESS-TOKEN` is attached when the sandbox has secure access enabled.

Composition picks exactly one bash executor, so mount this **instead of** `bash-local` / `bash-sandbox`. The model-facing `bash` tool and background jobs work on top of it unchanged.

## Install

```bash
dsh plugin --profile web add dsh-opensandbox
```

Then point it at your sandbox's execd endpoint in `cordis.yml`:

```yaml
plugins:
  dsh-opensandbox:
    baseUrl: http://10.0.0.7:44772
    accessToken: <X-EXECD-ACCESS-TOKEN when secure access is on>
    cwd: /workspace
    timeoutMs: 120000
    maxTimeoutMs: 600000
    maxOutputBytes: 8388608
```

## Configuration

| Key | Meaning |
| --- | --- |
| `baseUrl` | execd base URL of the target sandbox (required) |
| `accessToken` | `X-EXECD-ACCESS-TOKEN` value for sandboxes with secure access |
| `cwd` | default working directory inside the sandbox; empty uses the sandbox's own default |
| `timeoutMs` | default foreground timeout; the sandbox enforces it server-side |
| `maxTimeoutMs` | upper bound for per-call timeout overrides |
| `maxOutputBytes` | per-stream in-memory capture cap; overflow keeps the tail and flags truncation |

## Known limitations and deferred work

- **No stdin payloads**: execd's command API takes shell text, so `ShellExecRequest.stdin` is rejected with a clear error; commands that need input use shell syntax (pipes or heredocs).
- **No terminals**: `spawnTerminal`-style consumers are not served yet; the executor covers foreground and background command runs.
- **Output spill**: truncation keeps an in-memory tail and flags `lossy`; spill files on the sandbox side are not exported.
- **One sandbox per executor instance**: point `baseUrl` at the sandbox the composition should run in.

## Development

```bash
pnpm install
pnpm run typecheck
pnpm run test
pnpm run build
```

## License

Apache-2.0
