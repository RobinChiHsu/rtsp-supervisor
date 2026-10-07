import pytest
from hypothesis import given
from hypothesis import strategies as st

from rtsp_supervisor import ExponentialBackoff


def test_delays_grow_exponentially_without_jitter() -> None:
    backoff = ExponentialBackoff(initial=0.5, multiplier=2.0, maximum=60.0, jitter=0.0)

    assert [backoff.next_delay() for _ in range(4)] == [0.5, 1.0, 2.0, 4.0]


def test_delay_is_capped_at_maximum() -> None:
    backoff = ExponentialBackoff(initial=1.0, multiplier=3.0, maximum=5.0, jitter=0.0)

    assert [backoff.next_delay() for _ in range(4)] == [1.0, 3.0, 5.0, 5.0]


def test_reset_starts_over_from_initial_delay() -> None:
    backoff = ExponentialBackoff(initial=1.0, multiplier=2.0, maximum=60.0, jitter=0.0)
    backoff.next_delay()
    backoff.next_delay()

    backoff.reset()

    assert backoff.attempt == 0
    assert backoff.next_delay() == 1.0


@pytest.mark.parametrize(("roll", "expected"), [(0.0, 8.0), (0.5, 10.0), (1.0, 12.0)])
def test_jitter_spreads_delay_symmetrically(roll: float, expected: float) -> None:
    backoff = ExponentialBackoff(initial=10.0, maximum=60.0, jitter=0.2, rng=lambda: roll)

    assert backoff.next_delay() == pytest.approx(expected)


def test_jitter_never_exceeds_maximum() -> None:
    backoff = ExponentialBackoff(initial=10.0, maximum=10.0, jitter=0.5, rng=lambda: 1.0)

    assert backoff.next_delay() == 10.0


@pytest.mark.parametrize(
    "kwargs",
    [
        {"initial": 0.0},
        {"initial": 2.0, "maximum": 1.0},
        {"multiplier": 0.5},
        {"jitter": -0.1},
        {"jitter": 1.5},
    ],
)
def test_rejects_invalid_configuration(kwargs: dict[str, float]) -> None:
    with pytest.raises(ValueError):  # noqa: PT011
        ExponentialBackoff(**kwargs)


@given(
    initial=st.floats(min_value=0.01, max_value=10.0),
    extra=st.floats(min_value=0.0, max_value=100.0),
    multiplier=st.floats(min_value=1.0, max_value=10.0),
    jitter=st.floats(min_value=0.0, max_value=1.0),
    roll=st.floats(min_value=0.0, max_value=1.0),
    attempts=st.integers(min_value=1, max_value=2000),
)
def test_delay_always_stays_within_bounds(
    initial: float, extra: float, multiplier: float, jitter: float, roll: float, attempts: int
) -> None:
    maximum = initial + extra
    backoff = ExponentialBackoff(
        initial=initial, maximum=maximum, multiplier=multiplier, jitter=jitter, rng=lambda: roll
    )

    for _ in range(attempts):
        delay = backoff.next_delay()
        assert 0.0 <= delay <= maximum


@given(
    multiplier=st.floats(min_value=1.0, max_value=10.0),
    attempts=st.integers(min_value=2, max_value=200),
)
def test_delays_without_jitter_never_decrease(multiplier: float, attempts: int) -> None:
    backoff = ExponentialBackoff(initial=0.1, maximum=30.0, multiplier=multiplier, jitter=0.0)

    delays = [backoff.next_delay() for _ in range(attempts)]

    assert delays == sorted(delays)
