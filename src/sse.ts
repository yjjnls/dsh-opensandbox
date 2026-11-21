/**
 * Minimal parser for execd's Server-Sent Events stream (`text/event-stream`).
 * Each `data:` payload is one JSON `ServerStreamEvent`; `event:`/`id:`/comment
 * lines carry no information this executor needs.
 * @module dsh-opensandbox/sse
 */

/** One decoded execd stream event. */
export interface ExecdStreamEvent {
  /** Event discriminator (`init`, `stdout`, `stderr`, `error`, `execution_complete`, ...). */
  type: string
  /** Textual payload for `init` (the session id) and the stream events. */
  text?: string
  /** Successful foreground exit status, present on `execution_complete`. */
  exit_code?: number
  /** Execution duration in milliseconds. */
  execution_time?: number
  /** Error details for `error` events (`CommandTimeout` marks a server-side deadline kill). */
  error?: {
    ename?: string
    evalue?: string
    exit_code?: number
  }
}

/**
 * Decode an execd event stream into events. Blocks are separated by a blank
 * line; a trailing unterminated block is still emitted so a stream that ends
 * without a final blank line does not lose its last event.
 * @param body - the response body stream.
 * @returns the decoded events in order; unparsable data blocks are skipped.
 */
export async function* parseExecdEvents(
  body: ReadableStream<Uint8Array>,
): AsyncGenerator<ExecdStreamEvent> {
  const decoder = new TextDecoder()
  let buffered = ''
  for await (const chunk of body as unknown as AsyncIterable<Uint8Array>) {
    buffered += decoder.decode(chunk, { stream: true })
    // Normalize CRLF framing before splitting on blank lines.
    buffered = buffered.replaceAll('\r\n', '\n')
    let boundary = buffered.indexOf('\n\n')
    while (boundary >= 0) {
      const event = parseBlock(buffered.slice(0, boundary))
      buffered = buffered.slice(boundary + 2)
      if (event) yield event
      boundary = buffered.indexOf('\n\n')
    }
  }
  const tail = parseBlock(buffered)
  if (tail) yield tail
}

/**
 * Parse one SSE block into an event.
 * @param block - raw text between blank lines.
 * @returns the decoded event, or `undefined` when the block has no parsable data payload.
 */
function parseBlock(block: string): ExecdStreamEvent | undefined {
  const dataLines: string[] = []
  for (const line of block.split('\n')) {
    if (line.startsWith('data:')) dataLines.push(line.slice(5).trimStart())
  }
  if (dataLines.length === 0) return undefined
  try {
    const parsed: unknown = JSON.parse(dataLines.join('\n'))
    if (typeof parsed !== 'object' || parsed === null) return undefined
    const record = parsed as Record<string, unknown>
    if (typeof record.type !== 'string') return undefined
    return record as unknown as ExecdStreamEvent
  } catch (_unparsableData) {
    // A non-JSON data block carries nothing this executor can map; skip it.
    return undefined
  }
}
