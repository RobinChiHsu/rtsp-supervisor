from rtsp_supervisor import SystemClock


async def test_system_clock_sleep_advances_monotonic_time() -> None:
    clock = SystemClock()
    before = clock.monotonic()

    await clock.sleep(0.01)

    assert clock.monotonic() - before >= 0.01
