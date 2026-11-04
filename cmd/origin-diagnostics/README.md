# Diagnose origin reads

This internal command reads one existing S3 object through three paths and writes
JSON lines to stdout. It never uploads, deletes, or changes an object. It does not
join TAG's cache ring or serve customer requests.

Build it for the host where the affected TAG process runs:

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o origin-diagnostics ./cmd/origin-diagnostics
```

Provide credentials through `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.
Keep them out of the configuration file and shell arguments. The configuration
contains the endpoint, bucket, and exactly one existing object key:

```json
{
  "Endpoint": "https://ACCOUNT.r2.cloudflarestorage.com",
  "Bucket": "example-bucket",
  "Keys": ["existing-object"]
}
```

Check access and metadata before transferring data:

```sh
./origin-diagnostics -config config.json -mode head -run access-check
```

Run each mode separately with a unique run label. Transfer modes accept objects
between 1 GB and 4 GiB and enforce a ten-minute command deadline.

| Mode | Behavior |
| --- | --- |
| `full` | One direct GET, streamed into SHA-256. |
| `ranges` | Direct 16 MiB ranges in batches of four, with a 120-second deadline per range and no retries or hedges. |
| `parallel` | TAG's origin reader, including its current concurrency, hedging, retry, and deadline policies. |

```sh
./origin-diagnostics -config config.json -mode full -run trial1-full > trial1-full.jsonl
./origin-diagnostics -config config.json -mode ranges -run trial1-ranges > trial1-ranges.jsonl
./origin-diagnostics -config config.json -mode parallel -run trial1-parallel > trial1-parallel.jsonl
```

Keep the ETag and SHA-256 identical across runs. Change the order between rounds
to reduce order bias. Reads can affect provider caches even though they do not
modify objects; these commands cannot guarantee a cold provider cache.

## Interpret the evidence

Each attempt records connection, DNS, TCP, TLS, request-write, and response-header
timings where available. Progress records bytes read into memory, with at most 32
recent samples. Gaps between reads include local processing and scheduling time.
Object bodies are never written to disk.

`cause` distinguishes attempt and body deadlines, caller cancellation, failed
reads, and losing hedges. `launch_denied` records which concurrency, memory-buffer,
attempt, or byte budget prevented another attempt. The reader's attempt deadline
is a total time limit, not an idle timeout: continuing progress does not reset it.

The first part reuses the initial GET body. Its `initial_stream` trace starts after
that request's headers; a separate `initial_headers_only` record covers the initial
request. `first_response_header_byte` measures response headers, not body progress.

Response records include Cloudflare Ray IDs or S3 request IDs when supplied.
Diagnostics omit object names, URLs, credentials, and raw error strings. The
object record includes a shortened key hash and ETag for comparing observations.

`pod_tcp_delta` reads Linux counters in the command's network namespace. Local
retransmission counters do not establish whether packets sent by the remote
server were lost. On systems without these counters, the map is empty.

The range control waits for each batch of four. TAG uses a moving window, so this
is a diagnostic comparison, not an isolated benchmark of retry overhead. Neither
mode exercises TAG's cache fill, client backpressure, or proxy handling. Record
host load and resource limits alongside results before drawing conclusions.

## Enable tracing in a diagnostic caller

Use `origin.WithDiagnostics(ctx, observer)` on a request context passed to
`Reader.Wrap`. The observer must be concurrency-safe and return promptly; it runs
on download goroutines. A slow observer changes the timings being measured.
Normal gateway requests do not install this observer. They allocate no trace
history or HTTP hooks, and retain the existing download policies.
