import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { Context } from "@deepseek-ai/cordis";
import { OpenSandboxShellExecutor } from "../src/index.ts";

/** One execd SSE server scenario: scripted events + recorded requests. */
interface Recorded {
  method: string | undefined;
  url: string | undefined;
  body: unknown;
  headers: Record<string, string | string[] | undefined>;
}

async function startExecd(script: (reqId: number) => {
  status: number;
  events: Array<Record<string, unknown>>;
  delayMs?: number;
}): Promise<{ server: Server; url: string; requests: Recorded[] }> {
  const requests: Recorded[] = [];
  const server = createServer((req, res) => {
    let raw = "";
    req.on("data", (chunk: Buffer) => { raw += chunk.toString(); });
    req.on("end", () => {
      let body: unknown = undefined;
      try { body = JSON.parse(raw); } catch { body = raw; }
      const flatHeaders: Record<string, string | string[] | undefined> = {};
      for (const [key, value] of Object.entries(req.headers)) flatHeaders[key] = value;
      requests.push({ method: req.method, url: req.url, body, headers: flatHeaders });

      const scripted = script(requests.length);
      res.writeHead(scripted.status, {
        "content-type": "text/event-stream",
        connection: "close",
      });
      const frame = (event: Record<string, unknown>) =>
        `data: ${JSON.stringify(event)}\n\n`;
      const write = () => {
        for (const event of scripted.events) res.write(frame(event));
        res.end();
      };
      if (scripted.delayMs) setTimeout(write, scripted.delayMs);
      else write();
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const url = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  return { server, url, requests };
}

function executor(baseUrl: string, accessToken?: string): OpenSandboxShellExecutor {
  const ctx = new Context();
  return new OpenSandboxShellExecutor(ctx, {
    baseUrl: { get: () => baseUrl } as never,
    accessToken: { get: () => accessToken ?? "" } as never,
    cwd: { get: () => "/workspace" } as never,
    timeoutMs: { get: () => 5_000 } as never,
    maxTimeoutMs: { get: () => 30_000 } as never,
    maxOutputBytes: { get: () => 64 * 1024 } as never,
  });
}

describe("OpenSandboxShellExecutor", () => {
  const servers: Server[] = [];
  afterEach(() => {
    for (const server of servers.splice(0)) server.close();
  });

  it("resolves defaults and caps the timeout", () => {
    const exec = executor("http://127.0.0.1:1");
    const spec = exec.resolve({ command: "ls" });
    expect(spec.command).toBe("ls");
    expect(spec.workdir).toBe("/workspace");
    expect(spec.timeoutMs).toBe(5_000);
    const capped = exec.resolve({ command: "ls", timeoutMs: 999_999 });
    expect(capped.timeoutMs).toBe(30_000);
    expect(exec.resolve({ command: "ls", onExpiry: "none" }).timeoutMs).toBe(5_000);
  });

  it("streams foreground output and classifies a clean exit", async () => {
    const { server, url, requests } = await startExecd(() => ({
      status: 200,
      events: [
        { type: "init", text: "session-1" },
        { type: "stdout", text: "hello " },
        { type: "stdout", text: "world\n" },
        { type: "stderr", text: "warn\n" },
        { type: "execution_complete", exit_code: 0, execution_time: 12 },
      ],
    }));
    servers.push(server);

    const handle = await executor(url).execute({
      command: "echo hello world",
      workdir: "/workspace",
      timeoutMs: 5_000,
      onExpiry: "kill",
      stdoutMaxBytes: 64 * 1024,
      sandboxPolicy: undefined,
    });
    const result = await handle.result();

    expect(result.exitCode).toBe(0);
    expect(result.timedOut).toBe(false);
    expect(result.aborted).toBe(false);
    expect(result.stdout.text).toBe("hello world\n");
    expect(result.stderr.text).toBe("warn\n");
    expect(requests[0]?.method).toBe("POST");
    expect((requests[0]?.body as Record<string, unknown>).command).toBe("echo hello world");
    expect((requests[0]?.body as Record<string, unknown>).timeout).toBe(5_000);
  });

  it("maps a non-zero exit through the error event", async () => {
    const { server, url } = await startExecd(() => ({
      status: 200,
      events: [
        { type: "init", text: "session-2" },
        {
          type: "error",
          error: { ename: "CommandExecError", evalue: "2", exit_code: 2 },
        },
      ],
    }));
    servers.push(server);

    const handle = await executor(url).execute({
      command: "exit 2",
      workdir: "/workspace",
      timeoutMs: 5_000,
      onExpiry: "kill",
      stdoutMaxBytes: 64 * 1024,
      sandboxPolicy: undefined,
    });
    const result = await handle.result();

    expect(result.exitCode).toBe(2);
    expect(result.timedOut).toBe(false);
    expect(result.stdout.text).toBe("");
  });

  it("classifies a server-side deadline kill as timedOut with null exit", async () => {
    const { server, url } = await startExecd(() => ({
      status: 200,
      events: [
        { type: "init", text: "session-3" },
        { type: "stdout", text: "partial\n" },
        {
          type: "error",
          error: { ename: "CommandTimeout", evalue: "command timed out after 5s: killed", exit_code: -1 },
        },
      ],
    }));
    servers.push(server);

    const handle = await executor(url).execute({
      command: "sleep 60",
      workdir: "/workspace",
      timeoutMs: 5_000,
      onExpiry: "kill",
      stdoutMaxBytes: 64 * 1024,
      sandboxPolicy: undefined,
    });
    const result = await handle.result();

    expect(result.timedOut).toBe(true);
    expect(result.aborted).toBe(false);
    expect(result.exitCode).toBe(null);
    expect(handle.status).toBe("completed");
  });

  it("interrupts via DELETE /command when the caller aborts", async () => {
    const { server, url, requests } = await startExecd(() => ({
      status: 200,
      // First (POST) request hangs long enough for the abort to fire.
      events: [{ type: "init", text: "session-4" }],
      delayMs: 500,
    }));
    servers.push(server);

    const controller = new AbortController();
    const exec = executor(url);
    const handle = exec.execute({
      command: "sleep 60",
      workdir: "/workspace",
      timeoutMs: 5_000,
      onExpiry: "kill",
      stdoutMaxBytes: 64 * 1024,
      signal: controller.signal,
      sandboxPolicy: undefined,
    });
    // The handle resolves once prepared; abort now triggers the interrupt.
    const started = await handle;
    controller.abort();
    await started.result().catch(() => undefined);
    await started.done;

    // The interrupt is fire-and-forget; let its request land before asserting.
    await new Promise((resolve) => setTimeout(resolve, 100));
    const interrupt = requests.find((r) => r.method === "DELETE");
    expect(interrupt?.url).toContain("id=session-4");
  });

  it("rejects stdin payloads with a clear error", async () => {
    const { server, url } = await startExecd(() => ({ status: 200, events: [] }));
    servers.push(server);

    await expect(
      executor(url).execute({
        command: "cat",
        workdir: "/workspace",
        timeoutMs: 5_000,
        onExpiry: "kill",
        stdoutMaxBytes: 64 * 1024,
        stdin: "payload",
        sandboxPolicy: undefined,
      }),
    ).rejects.toThrow(/stdin/);
  });

  it("attaches the access token header when configured", async () => {
    const { server, url, requests } = await startExecd(() => ({
      status: 200,
      events: [{ type: "execution_complete", exit_code: 0 }],
    }));
    servers.push(server);

    const handle = await executor(url, "execd-token").execute({
      command: "true",
      workdir: "/workspace",
      timeoutMs: 5_000,
      onExpiry: "kill",
      stdoutMaxBytes: 64 * 1024,
      sandboxPolicy: undefined,
    });
    await handle.result();

    expect((requests[0]?.body as Record<string, unknown>).command).toBe("true");
    expect(requests[0]?.headers["x-execd-access-token"]).toBe("execd-token");
  });
});
