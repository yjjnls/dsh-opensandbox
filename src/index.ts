/**
 * OpenSandbox executor for the bash capability seam (`ctx.shell`). Every
 * command runs inside an OpenSandbox sandbox through its execd HTTP API:
 * `POST /command` streams the run (stdout/stderr/error/execution_complete),
 * `DELETE /command?id=…` interrupts it. The sandbox enforces the request
 * deadline server-side and reports a deadline kill as a `CommandTimeout`
 * error event, which this executor maps to the seam's first-cause `timedOut`
 * classification; a caller abort maps to `aborted` via an interrupt call.
 * There is no local process and no host environment: only the caller's
 * `env`/`dshEnv` entries are sent, so host credentials cannot leak into the
 * sandbox.
 * @module dsh-opensandbox
 */

import { Context } from '@deepseek-ai/cordis'
import type { Volatile } from '@deepseek-ai/cordis'
import z from '@deepseek-ai/schemastery'
import { ShellExecutor } from '@deepseek-ai/dsh-shell'
import type {
  CollectedOutput,
  ShellExecRequest,
  ShellExecSpec,
  ShellExecution,
  ShellProcess,
  ShellProcessRead,
  ShellRunResult,
} from '@deepseek-ai/dsh-shell'
import type { SubprocessOutputReader } from '@deepseek-ai/dsh-subprocess'

import { parseExecdEvents } from './sse.ts'

/** Validated plugin configuration. */
export interface Config {
  /** execd base URL of the target sandbox, for example `http://10.0.0.7:44772`. */
  baseUrl: Volatile<string>
  /** `X-EXECD-ACCESS-TOKEN` value when the sandbox has secure access enabled. */
  accessToken: Volatile<string>
  /** Default working directory inside the sandbox; empty means the sandbox's own default. */
  cwd: Volatile<string>
  /** Default foreground timeout in milliseconds. */
  timeoutMs: Volatile<number>
  /** Upper bound for per-call timeout overrides. */
  maxTimeoutMs: Volatile<number>
  /** Per-stream in-memory output cap in bytes; overflow keeps the tail and flags truncation. */
  maxOutputBytes: Volatile<number>
}

/** One append-only capture with a bounded in-memory tail. */
interface CaptureBuffer {
  append(text: string): void
  /** Snapshot for the settled result projection. */
  collected(): CollectedOutput
  /** Independent offset reader (the `observed` surface). */
  reader(): SubprocessOutputReader
}

/**
 * Create a bounded capture: the tail of at most `cap` bytes is kept,
 * overflow is dropped from the head and reported as truncation on every read.
 * @param cap - maximum retained bytes (approximated over UTF-16 units).
 * @returns the capture buffer.
 */
function createCapture(cap: number): CaptureBuffer {
  let text = ''
  let truncated = false
  return {
    append(chunk: string): void {
      if (chunk.length === 0) return
      text += chunk
      if (text.length > cap) {
        truncated = true
        text = text.slice(text.length - cap)
      }
    },
    collected(): CollectedOutput {
      return { text, truncated }
    },
    reader(): SubprocessOutputReader {
      return {
        readFrom(offset: number): { text: string; nextOffset: number; lossy: boolean } {
          const from = Math.max(0, Math.min(offset, text.length))
          return { text: text.slice(from), nextOffset: text.length, lossy: truncated }
        },
      }
    },
  }
}

/** Plugin config schema (live resolvable values). */
// eslint-disable-next-line @typescript-eslint/no-extraneous-class
export class OpenSandboxShellExecutor extends ShellExecutor {
  static Config = z.object({
    baseUrl: z.string().volatile(),
    accessToken: z.string().default('').volatile(),
    cwd: z.string().default('').volatile(),
    timeoutMs: z.number().default(120_000).volatile(),
    maxTimeoutMs: z.number().default(600_000).volatile(),
    maxOutputBytes: z.number().default(8 * 1024 * 1024).volatile(),
  })

  constructor(ctx: Context, readonly config: Config) {
    super(ctx)
  }

  /**
   * Apply configured defaults and caps: `workdir` from the request or the
   * configured sandbox cwd, `timeoutMs` defaulted and capped, and the stdout
   * capture budget defaulted to the executor's per-stream cap.
   * @param request - the caller's request.
   * @returns the fully-specified spec to execute.
   */
  resolve(request: ShellExecRequest): ShellExecSpec {
    const maxTimeoutMs = this.config.maxTimeoutMs.get()
    const timeoutMs = Math.min(request.timeoutMs ?? this.config.timeoutMs.get(), maxTimeoutMs)
    if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
      throw new Error('dsh-opensandbox: timeoutMs must be a positive finite number')
    }
    return {
      command: request.command,
      workdir: request.workdir ?? this.config.cwd.get() ?? '',
      timeoutMs,
      onExpiry: request.onExpiry ?? 'kill',
      stdoutMaxBytes: request.stdoutMaxBytes ?? this.config.maxOutputBytes.get(),
      signal: request.signal,
      stdin: request.stdin,
      env: request.env,
      dshEnv: request.dshEnv,
      sandboxPolicy: undefined,
    }
  }

  /**
   * Start the command in the sandbox and return its handle.
   * @param spec - a resolved spec from {@link resolve}.
   * @returns the live handle; foreground callers await `result()` on it.
   * @throws on request construction failure or a `/command` call that never produced a stream.
   */
  async execute(spec: ShellExecSpec): Promise<ShellExecution> {
    if (spec.stdin !== undefined) {
      throw new Error(
        'dsh-opensandbox: stdin payloads are not supported; use shell syntax (pipe or heredoc) in the command',
      )
    }
    const baseUrl = this.config.baseUrl.get().replace(/\/+$/, '')
    const headers: Record<string, string> = { 'content-type': 'application/json' }
    const token = this.config.accessToken.get() || undefined
    if (token) headers['x-execd-access-token'] = token

    const envs: Record<string, string> = { ...spec.env }
    if (spec.dshEnv) {
      for (const [key, value] of Object.entries(spec.dshEnv)) {
        if (value !== undefined) envs[key] = String(value)
      }
    }
    const body: Record<string, unknown> = { command: spec.command, background: false }
    if (spec.workdir !== '') body.cwd = spec.workdir
    // The sandbox enforces deadlines server-side; `none` arms no deadline at all.
    if (spec.onExpiry !== 'none') body.timeout = spec.timeoutMs
    if (Object.keys(envs).length > 0) body.envs = envs

    const maxOutputBytes = this.config.maxOutputBytes.get()
    const stdout = createCapture(Math.max(spec.stdoutMaxBytes, 1))
    const stderr = createCapture(maxOutputBytes)

    const response = await fetch(`${baseUrl}/command`, {
      method: 'POST',
      headers,
      body: JSON.stringify(body),
    })
    if (!response.ok || response.body === null) {
      throw new Error(`dsh-opensandbox: execd /command failed with HTTP ${response.status}`)
    }

    let sessionId: string | undefined
    let exitCode: number | null = null
    let killed = false
    let abortFired = false
    let timedOut = false
    let providerFailure: Error | undefined
    let settle: () => void = () => {}
    const done = new Promise<void>((resolve) => { settle = resolve })
    let offsetStdout = 0
    let offsetStderr = 0

    const interrupt = async (): Promise<void> => {
      if (sessionId === undefined) return
      try {
        const interruptHeaders: Record<string, string> = {}
        if (token) interruptHeaders['x-execd-access-token'] = token
        await fetch(`${baseUrl}/command?id=${encodeURIComponent(sessionId)}`, {
          method: 'DELETE',
          headers: interruptHeaders,
        })
      } catch (_bestEffortInterrupt) {
        // The stream outcome still settles the handle; a failed interrupt only delays the kill.
      }
    }

    const onAbort = (): void => {
      if (abortFired || killed) return
      abortFired = true
      void interrupt()
    }
    if (spec.signal) {
      if (spec.signal.aborted) onAbort()
      else spec.signal.addEventListener('abort', onAbort, { once: true })
    }

    let resultPromise: Promise<ShellRunResult> | undefined
    const handle: ShellExecution = {
      status: 'running',
      get exitCode(): number | null {
        return exitCode
      },
      signal: null,
      sandbox: undefined,
      done,
      readOutput: (): ShellProcessRead => {
        const out = stdout.reader().readFrom(offsetStdout)
        const err = stderr.reader().readFrom(offsetStderr)
        offsetStdout = out.nextOffset
        offsetStderr = err.nextOffset
        const separator = out.text.length > 0 && !out.text.endsWith('\n') ? '\n' : ''
        const delta = out.text + (err.text.length > 0 ? `${separator}[stderr]\n${err.text}` : '')
        return { delta, lossy: out.lossy || err.lossy }
      },
      observed: {
        stdout: stdout.reader(),
        stderr: stderr.reader(),
      },
      kill: (): boolean => {
        if (handle.status !== 'running') return false
        killed = true
        void interrupt()
        return true
      },
      result: (): Promise<ShellRunResult> => {
        resultPromise ??= completion.then(() => {
          if (providerFailure !== undefined) throw providerFailure
          return {
            exitCode,
            signal: null,
            timedOut,
            aborted: abortFired && !timedOut,
            timeoutMs: spec.timeoutMs,
            stdout: stdout.collected(),
            stderr: stderr.collected(),
          }
        })
        return resultPromise
      },
    }

    const completion = (async (): Promise<void> => {
      try {
        for await (const event of parseExecdEvents(response.body as ReadableStream<Uint8Array>)) {
          switch (event.type) {
            case 'init':
              sessionId = event.text
              if (abortFired) void interrupt()
              break
            case 'stdout':
              stdout.append(event.text ?? '')
              break
            case 'stderr':
              stderr.append(event.text ?? '')
              break
            case 'error':
              if (event.error?.ename === 'CommandTimeout') {
                // Server-side deadline kill: signal death, first-cause timeout.
                timedOut = exitCode === null
                exitCode = null
              } else if (event.error?.exit_code !== undefined) {
                exitCode = event.error.exit_code
              }
              break
            case 'execution_complete':
              exitCode = event.exit_code ?? exitCode ?? 0
              break
            default:
              // ping/status/result/execution_count carry nothing for the shell seam.
              break
          }
        }
      } catch (error) {
        providerFailure = error instanceof Error ? error : new Error(String(error))
      } finally {
        spec.signal?.removeEventListener('abort', onAbort)
        handle.status = killed || abortFired ? 'killed' : 'completed'
        settle()
      }
    })()

    return handle
  }
}

export default OpenSandboxShellExecutor
