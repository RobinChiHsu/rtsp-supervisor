import argparse
import asyncio
import logging

import cv2

from rtsp_supervisor import StreamState, StreamSupervisor, SupervisorConfig
from rtsp_supervisor.opencv import OpenCVSource


def log_transition(old: StreamState, new: StreamState) -> None:
    logging.info("state %s -> %s", old, new)


async def report(supervisor: StreamSupervisor[object], interval: float) -> None:
    while True:
        await asyncio.sleep(interval)
        stats = supervisor.stats
        logging.info(
            "read=%d dropped=%d reconnects=%d healthy=%s",
            stats.frames_read,
            stats.frames_dropped,
            stats.reconnect_attempts,
            supervisor.is_healthy(max_staleness=2.0),
        )


async def main(url: str, duration: float) -> None:
    supervisor: StreamSupervisor[object] = StreamSupervisor(
        lambda: OpenCVSource(url, api_preference=cv2.CAP_FFMPEG),
        config=SupervisorConfig(read_timeout=3.0),
        on_state_change=log_transition,
    )
    async with supervisor:
        reporter = asyncio.create_task(report(supervisor, interval=1.0))
        try:
            async with asyncio.timeout(duration):
                async for frame in supervisor.frames():
                    await asyncio.sleep(0.1)
                    logging.debug("processed frame %d", frame.sequence)
        except TimeoutError:
            pass
        finally:
            reporter.cancel()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Watch a stream and print supervisor stats.")
    parser.add_argument("url", help="RTSP URL or path to a video file")
    parser.add_argument("--duration", type=float, default=30.0, help="seconds to run")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    asyncio.run(main(args.url, args.duration))
