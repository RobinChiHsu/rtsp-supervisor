# rtsp-supervisor

[![ci](https://github.com/RobinChiHsu/rtsp-supervisor/actions/workflows/ci.yml/badge.svg)](https://github.com/RobinChiHsu/rtsp-supervisor/actions/workflows/ci.yml)

Keep an RTSP camera (or any video source) flowing in an asyncio application, even when the network,
the camera or the decoder misbehaves.

The naive loop most projects start with looks like this:

```python
capture = cv2.VideoCapture(url)
while True:
    ok, frame = capture.read()
    process(frame)
```

It works on a desk and fails in the field:

- `read()` blocks, so it freezes the event loop, and it can hang forever when a camera stops sending
  packets without closing the connection.
- A dropped connection ends the stream for good, or turns into a tight reconnect loop that hammers
  the camera.
- When processing is slower than the frame rate, frames queue up and the "live" view drifts seconds
  or minutes behind reality.
- Credentials embedded in the URL leak into logs and exception messages.

`rtsp-supervisor` wraps a frame source in a small supervisor that deals with all of the above and
exposes frames as an async iterator.

## Install

```bash
pip install "rtsp-supervisor[opencv]"
```

The core package has no dependencies. The `opencv` extra adds the ready-made `OpenCVSource`.

## Usage

```python
import asyncio

from rtsp_supervisor import StreamSupervisor, SupervisorConfig
from rtsp_supervisor.opencv import OpenCVSource


async def main() -> None:
    url = "rtsp://user:password@192.168.1.20:554/stream1"
    supervisor = StreamSupervisor(
        lambda: OpenCVSource(url),
        config=SupervisorConfig(read_timeout=3.0, stable_after=15.0),
    )

    async with supervisor:
        async for frame in supervisor.frames():
            await analyse(frame.data)


asyncio.run(main())
```

Every frame carries a monotonically increasing `sequence` and the `captured_at` timestamp, so a
consumer can tell how many frames it skipped and how old the frame is.

Runtime information is available at any time:

```python
supervisor.state  # StreamState.STREAMING
supervisor.stats  # frames read, frames dropped, reconnect attempts, last error
supervisor.is_healthy(max_staleness=2)  # streaming and the last frame is at most 2 seconds old
```

`is_healthy` is meant to back a liveness or readiness probe. State changes can also be observed
with `on_state_change=lambda old, new: ...`.

See [`examples/monitor.py`](examples/monitor.py) for a runnable script.

## How it works

```
            open ok                read timeout / error
 CONNECTING ───────▶ STREAMING ─────────────────────────┐
     ▲   │                                              ▼
     │   └──── open timeout / error ───────────────▶ BACKOFF
     └───────────────────── delay elapsed ──────────────┘
```

**Blocking calls run off the event loop.** `open`, `read` and `close` run in worker threads through
`asyncio.to_thread`, each guarded by its own timeout. A camera that stops sending data is detected
by the read timeout rather than hanging the application.

**Latest frame wins.** Frames go into a single-slot buffer. If the consumer has not picked up the
previous frame yet, it is replaced and counted in `frames_dropped`. A slow consumer always works on
the newest image instead of a growing backlog, and memory use stays constant.

**Exponential backoff with jitter.** Reconnect delays double up to a ceiling, with random jitter so
many cameras recovering from the same outage do not reconnect in lockstep. The backoff only resets
after a connection has stayed up for `stable_after` seconds, so a camera that accepts a connection
and drops it immediately does not cause a reconnect storm.

**Sources are pluggable.** Anything with `open()`, `read()` and `close()` satisfies the
`FrameSource` protocol. The supervisor creates a fresh source through a factory on every attempt,
so no half-broken state survives a reconnect. This also makes the supervisor easy to test without
cameras: the test suite drives it with scripted fake sources and a fake clock.

**Credentials stay out of logs.** Errors raised by `OpenCVSource` pass the URL through
`redact_url`, which turns `rtsp://admin:secret@host` into `rtsp://admin:***@host`. The helper is
exported for use in your own logging.

**Clean shutdown.** `stop()` (or leaving the `async with` block) cancels the supervisor, closes the
current source and ends every `frames()` iterator. Cancelling the task that calls `stop()` is still
propagated to the caller.

## Configuration

| Option | Default | Meaning |
| --- | --- | --- |
| `open_timeout` | 10.0 | Seconds allowed for opening a source |
| `read_timeout` | 5.0 | Seconds without a frame before the stream is considered stalled |
| `close_timeout` | 2.0 | Seconds allowed for releasing a source |
| `stable_after` | 10.0 | Seconds a connection must survive before backoff resets |

Backoff is configured separately with `ExponentialBackoff(initial, maximum, multiplier, jitter)`.

## Limitations

- A Python thread cannot be killed. When a read times out, the supervisor closes the source to
  unblock the worker thread, which works for OpenCV and most network clients. A source whose
  `read` ignores `close` will keep its thread alive until the call returns.
- Video files are read as fast as they can be decoded, not at their native frame rate. They are
  handy for testing but do not simulate a live camera's timing.

## Development

```bash
uv sync
uv run pytest      # tests with branch coverage
uv run mypy        # strict type checking
uv run ruff check .
```

## License

MIT
