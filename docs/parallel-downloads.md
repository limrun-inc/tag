# Parallel origin downloads

TAG can retrieve a large object with four concurrent byte ranges while returning
one ordinary streaming response. It retries or races slow ranges, pins every
range to the initial response's strong ETag, and preserves whole-object caching.

The feature is off by default. Enable a canary with
`TAG_PARALLEL_DOWNLOADS=true` or `upstream.parallel_downloads: true`. Disable it
to restore the existing origin stream for subsequent requests. No cache format,
coordination mode, storage block setting, or client request format changes.

## Eligible reads

Signed full-object GETs qualify after the original origin response returns 200,
a known size greater than 16 MiB, a strong ETag, and `Accept-Ranges: bytes`.
The reader covers cold misses, changed revalidation bodies, background fills,
and authenticated forwarding paths, including presigned cache bypass.
Authentication remains the forwarder's responsibility. The routing/authentication
migration is a prerequisite for client request forms the deployed forwarder does
not already support; this feature does not grant new access.

Small objects, client Range requests, encoded bodies, unknown lengths, weak or
missing ETags, and unsupported query selectors retain the existing stream.
Selectors such as `versionId`, `partNumber`, and response-header overrides are
not rewritten. Transparent client requests retain their original signature;
this reader does not replace them with service-credential reads. Synthetic
signed background GETs can use the reader in either forwarding mode.

Warm whole-object cache hits continue to use the existing cache path.

## Initial policy

| Control | Setting |
| --- | --- |
| Range size | 16 MiB |
| Read-ahead window | At most four distinct ranges, including completed ranges waiting for output |
| Extra attempts | At most two active across one object |
| Attempts per range | At most three total, including the original, errors, and hedges |
| Hedge delay | At least two seconds between attempts |
| Slow-rate comparison | Less than one third of a completed peer's rate, with enough remaining time to justify starting again |
| No-progress detection | Two seconds; without a completed peer, only the leading stalled range can hedge |
| Attempt deadline | 30 seconds, including synthetic-request headers and body |
| Body deadline | Five minutes, bounded further by the caller's context |
| Range memory reservation | 96 MiB per admitted object, shared with cache-populate/block-staging memory |
| Aggregate range memory | Uses the existing staging half of the configured cache-buffer budget |
| Request admission | 64 primary reservations plus extra-attempt slots combined per forwarder |
| Extra connection pool | Separate reusable transport, at most 16 connections per host |
| Extra requested bytes per object | At most 25% of object size, with a one-range minimum and 64 MiB maximum |
| Extra requested bytes per forwarder | At most 25% of delivered bytes plus a 64 MiB initial burst |

The request ceiling covers admitted range downloads. Single-stream fallbacks
retain the existing server admission limit. The attempt and byte ceilings are
upper bounds. A request can receive fewer
attempts when a shared budget is full. Admission never waits: a new object keeps
its initial stream if the byte or request reservation cannot fit. An admitted
range whose original attempt fails and cannot obtain a retry budget fails the
response. Turning off the cache memory budget does not remove the range limit;
the reader then uses a separate budget with a 1 GiB staging cap.

The initial GET supplies headers and streams its leading range immediately.
TAG closes that body after the first range and obtains the remaining ranges
directly from the origin. Each synthetic response must match the captured ETag,
total size, exact range, and body length. A faster attempt can supply bytes once it passes the committed output offset.
A valid completed attempt wins; replacements never repeat bytes already delivered. Losing
requests are canceled, closed, and joined before releasing their reservations.
An overwrite, authentication failure, missing object, or invalid range response
fails the stream. TAG never falls back to mixing another object version.

The range reader samples progress while reading the origin into its bounded
buffers. Waiting for downstream consumption does not lower that measured rate.
When the feature is enabled, the broadcaster waits if every remaining client
queue is full. A fast cache writer cannot make the last client fall off the
stream. If another client has room, it can continue and a slower peer retains
the existing bounded-queue disconnect behavior. Cache writers remain best effort.

The first request's header wait keeps the existing HTTP transport timeout.
Range hedging begins after that request supplies the size and validator.
Concurrency is fixed at four in this version; it does not adapt to network
capacity. Uniformly slow transfers do not qualify as throughput outliers against
each other. Parallel reads cannot create bandwidth on a saturated shared link.

## Controlled results, 29 October 2026

The benchmark uses the production reader against a local HTTP origin on an
Apple M3 Pro, macOS arm64, Go 1.26.2. Both variants hash the complete output and
check its size. The baseline uses the unchanged single-response-body path.
Normal requests receive 32 MiB/s independently; the 1 GiB scenario uses 64 MiB/s.
Slow requests or byte regions receive 2 MiB/s. The shared-link scenario instead
caps all requests together at 16 MiB/s.

| Scenario | Single stream | Range reader | Measured transfers per variant |
| --- | ---: | ---: | ---: |
| Healthy 10 MiB | 0.315 s | 0.314 s | 1 |
| Healthy 64 MiB | 2.002 s | 0.530 s | 3 |
| Slow initial request, 64 MiB | 32.002 s | 2.533 s | 1 |
| Slow middle 16 MiB, 64 MiB object | 9.502 s | 2.524 s | 3 |
| Eight-second initial body stall, followed by an unpaced buffered response | 8.057 s | 2.544 s | 1 |
| Shared 16 MiB/s link, 64 MiB | 4.283 s | 4.014 s | 3 |
| Healthy 1 GiB | 16.002 s | 4.055 s | 1 |

A separate run started five distinct 64 MiB downloads together. Four initial
requests received 32 MiB/s and the fifth received 2 MiB/s. The slowest completion
fell from 32.00 seconds to 2.522 seconds. Median completion fell from 2.003 to
0.537 seconds. Total origin writes increased 1.6%, and the five readers reserved
480 MiB. All five output checksums matched. This is one batch per variant.
See the [concurrent run output](benchmarks/origin-20261029-concurrent.txt).

The three-transfer rows report means. These small samples do not estimate p95
or p99. Healthy 64 MiB time to first byte averaged 9.03 ms with the baseline and
8.88 ms with the range reader. The slow-middle case read and sent 1.078 times
the object size, about 8% extra traffic. Healthy runs read and sent 1.000 times
the object size. The shared-link result shows no parallelism-sized speedup.

Range-buffer reservations peaked at 96 MiB. Total Go allocations per 64 MiB
transfer were about 64.3 MiB without a hedge and 80.4 MiB with one. The 1 GiB
transfer allocated about 100 MiB in total, including metadata and channel
notifications, while retaining the same 96 MiB range-buffer ceiling. These
figures describe range buffering and allocations, not process RSS. Existing
broadcast queues, cache writers, HTTP buffers, and storage memory remain separate
contributors to RSS.

The origin-read counter measures bytes returned by HTTP body reads. The test
also records origin writes, but live transport buffering can discard unread
bytes when the initial full GET or a losing attempt is closed. The extra-request
quota does not account for those unread transport bytes. Measure actual origin
traffic during a canary as well as TAG's counters.

Raw outputs: [single-transfer cases](benchmarks/origin-20261029-single.txt) and
[repeated cases](benchmarks/origin-20261029-repeat.txt). Reproduce with:

```sh
go test -run '^$' -bench '^BenchmarkOriginFiveDownloads$' -benchtime=1x -count=1 -timeout 2m ./proxy/origin
go test -run '^$' -bench '^BenchmarkOriginDownload$' -benchtime=1x -count=1 -timeout 5m ./proxy/origin
go test -run '^$' -bench 'BenchmarkOriginDownload/(healthy_64MiB|slow_middle_64MiB|shared_link_64MiB)$' -benchtime=3x -count=1 -timeout 3m ./proxy/origin
```

These are controlled mechanism results, not measured R2 or production gains.
The HTTP service tests separately exercise signed cold reads, presigned cache
bypass, changed revalidation, background fills, warm hits, and a slow consumer.
They also check that a failed version-pinned read never publishes cache metadata.
Reader tests cover invalid responses, short bodies, three failed attempts,
cancellation, deadline cleanup without further consumer reads, request/memory
admission, and bounded read-ahead under backpressure. The full Go race suite,
local binary build, and `go vet` passed.

## Canary requirements

Keep the existing PVCs, pod placement, whole-object caching, and coordination
mode. This change is based on deployed R2 revision `59c2bf1` and must retain its
compatibility/publication fixes. It does not change deployment pins.

Before enabling a production canary, align the actual client idle and overall
deadlines with the two-second hedge opportunity, 30-second attempt deadline,
and five-minute body deadline. In particular, a five-second client idle timeout
can still cancel TAG before a replacement range completes. Client timeout and
recovery changes are not part of this TAG patch.

Compare cold, warm, revalidated, and bypassed reads separately for app downloads
and build-cache restores. Include concurrent traffic from both workloads. Use
actual R2 objects from tens of MiB through 1 GiB, retain downstream checksum
validation, and collect p50/p95/p99, timeouts, RSS, CPU, origin traffic, cache
population success, and throttling. A single-host result does not establish
fleet capacity or identify the cause of the original outliers.

New metrics are `tag_origin_range_events_total`, `tag_origin_range_bytes_total`,
`tag_origin_range_reserved_bytes`, `tag_origin_range_duration_seconds`, and
`tag_origin_range_output_wait_seconds_total`. Their labels contain no object
keys, URLs, or credentials. Read-byte and duration counters update when the
body closes. Continue watching existing broadcast slow-consumer, populate-shed,
and cache-locality metrics. Disable the flag if correctness, completion rates,
cache fills, RSS, or origin traffic regress.

The backpressure review also used the existing queue investigation as a lead,
then checked the deployed code. KB:
`concepts/tag-broadcast-queue-counts-64-kib-slots-not-bytes-so-16-kib-tls-reads-fill-it-at-a-quarter-of-nominal-and-a-mac-receive-window-caps-each-restore-stream`.
