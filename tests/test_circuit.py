"""Тесты circuit breaker (День 5)."""

from __future__ import annotations

import pytest

from src.circuit import (
    STATE_CLOSED,
    STATE_HALF_OPEN,
    STATE_OPEN,
    CircuitBreaker,
)
from src.errors import (
    NetworkError,
    ParseError,
    PermanentError,
    ServerError,
)

URL = "http://example.com/page"
OTHER = "http://other.example/page"


class Clock:
    """Подменяемые монотонные часы: восстановление ждать незачем."""

    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def clock() -> Clock:
    return Clock()


def breaker(clock: Clock, **kwargs) -> CircuitBreaker:
    kwargs.setdefault("failure_threshold", 3)
    kwargs.setdefault("window", 60.0)
    kwargs.setdefault("recovery_time", 30.0)
    return CircuitBreaker(clock=clock, **kwargs)


def fail(circuit: CircuitBreaker, times: int, url: str = URL) -> None:
    for _ in range(times):
        circuit.record_failure(url, ServerError("HTTP 503", status=503))


# --- открытие цепи ----------------------------------------------------------


def test_closed_circuit_allows_everything(clock):
    circuit = breaker(clock)

    assert circuit.allow(URL) is True
    assert circuit.state_of(URL) == STATE_CLOSED


def test_threshold_opens_the_circuit(clock):
    circuit = breaker(clock)

    fail(circuit, 2)
    assert circuit.allow(URL) is True  # порог ещё не достигнут

    fail(circuit, 1)
    assert circuit.state_of(URL) == STATE_OPEN
    assert circuit.allow(URL) is False


def test_refused_requests_are_counted(clock):
    circuit = breaker(clock)
    fail(circuit, 3)

    assert circuit.allow(URL) is False
    assert circuit.allow(URL) is False

    assert circuit.get_stats()["refused"] == 2
    assert circuit.get_stats()["trips"] == 1


def test_old_failures_leave_the_window(clock):
    circuit = breaker(clock)

    fail(circuit, 2)
    clock.advance(61.0)  # прошлые ошибки больше не считаются
    fail(circuit, 2)

    assert circuit.failures_for(URL) == 2
    assert circuit.state_of(URL) == STATE_CLOSED


def test_success_resets_the_counter(clock):
    circuit = breaker(clock)

    fail(circuit, 2)
    circuit.record_success(URL)
    fail(circuit, 2)

    assert circuit.failures_for(URL) == 2
    assert circuit.allow(URL) is True


# --- восстановление ---------------------------------------------------------


def test_probe_is_allowed_after_recovery_time(clock):
    circuit = breaker(clock)
    fail(circuit, 3)

    clock.advance(29.0)
    assert circuit.allow(URL) is False

    clock.advance(2.0)
    assert circuit.state_of(URL) == STATE_HALF_OPEN
    assert circuit.allow(URL) is True
    # Пробный запрос один: остальные ждут его исхода.
    assert circuit.allow(URL) is False


def test_lost_probe_does_not_block_the_domain_forever(clock):
    """Если исход пробы не пришёл (задачу отменили), через время пробуем снова."""
    circuit = breaker(clock)
    fail(circuit, 3)
    clock.advance(31.0)
    assert circuit.allow(URL) is True

    assert circuit.allow(URL) is False
    clock.advance(31.0)
    assert circuit.allow(URL) is True


def test_successful_probe_closes_the_circuit(clock):
    circuit = breaker(clock)
    fail(circuit, 3)
    clock.advance(31.0)
    circuit.allow(URL)

    circuit.record_success(URL)

    assert circuit.state_of(URL) == STATE_CLOSED
    assert circuit.allow(URL) is True
    assert circuit.get_stats()["recoveries"] == 1


def test_failed_probe_opens_the_circuit_again(clock):
    circuit = breaker(clock)
    fail(circuit, 3)
    clock.advance(31.0)
    circuit.allow(URL)

    fail(circuit, 1)

    assert circuit.state_of(URL) == STATE_OPEN
    assert circuit.allow(URL) is False
    assert circuit.get_stats()["trips"] == 2


# --- что считается аварией --------------------------------------------------


def test_answered_probe_closes_the_circuit_even_with_404(clock):
    """Сервер ответил отказом — значит, он жив, и держать домен незачем."""
    circuit = breaker(clock)
    fail(circuit, 3)
    clock.advance(31.0)
    assert circuit.allow(URL) is True

    circuit.record_failure(URL, PermanentError("HTTP 404: Not Found", status=404))

    assert circuit.state_of(URL) == STATE_CLOSED
    assert circuit.allow(URL) is True
    assert circuit.failures_for(URL) == 0


@pytest.mark.parametrize(
    "error",
    [
        PermanentError("HTTP 404: Not Found", status=404),
        PermanentError("HTTP 403: Forbidden", status=403),
        ParseError("soup: ValueError"),
    ],
)
def test_permanent_errors_do_not_open_the_circuit(clock, error):
    """Битые ссылки — свойство сайта, а не признак аварии сервера."""
    circuit = breaker(clock)

    for _ in range(5):
        circuit.record_failure(URL, error)

    assert circuit.failures_for(URL) == 0
    assert circuit.allow(URL) is True


def test_network_errors_count_towards_the_threshold(clock):
    circuit = breaker(clock)

    for _ in range(3):
        circuit.record_failure(URL, NetworkError("DNS не ответил"))

    assert circuit.state_of(URL) == STATE_OPEN


def test_failure_without_error_object_counts(clock):
    circuit = breaker(clock)

    for _ in range(3):
        circuit.record_failure(URL)

    assert circuit.state_of(URL) == STATE_OPEN


# --- домены и статистика ----------------------------------------------------


def test_domains_are_independent(clock):
    circuit = breaker(clock)

    fail(circuit, 3)

    assert circuit.allow(URL) is False
    assert circuit.allow(OTHER) is True


def test_www_and_port_share_one_state(clock):
    """Ключ тот же, что у лимитера: это один и тот же сайт."""
    circuit = breaker(clock)

    fail(circuit, 3, "http://www.example.com:8080/a")

    assert circuit.allow(URL) is False


def test_stats_describe_settings_and_outcome(clock):
    circuit = breaker(clock, failure_threshold=2, window=30.0, recovery_time=10.0)
    fail(circuit, 2)
    circuit.allow(URL)

    stats = circuit.get_stats()

    assert stats["enabled"] is True
    assert stats["failure_threshold"] == 2
    assert stats["window"] == 30.0
    assert stats["recovery_time"] == 10.0
    assert stats["domains"] == 1
    assert stats["open"] == ["example.com"]
    assert stats["states"] == {"example.com": STATE_OPEN}


def test_domain_with_probe_in_flight_is_visible(clock):
    """Домен с пробой тоже никого не пускает — он не должен пропасть из отчёта."""
    circuit = breaker(clock)
    fail(circuit, 3)
    clock.advance(31.0)
    circuit.allow(URL)

    stats = circuit.get_stats()

    assert stats["open"] == []
    assert stats["half_open"] == ["example.com"]
    assert circuit.allow(URL) is False


def test_reset_stats_keeps_open_circuits(clock):
    """Счётчики — история, состояние — защита сайта: сбрасывается только первое."""
    circuit = breaker(clock)
    fail(circuit, 3)
    circuit.allow(URL)

    circuit.reset_stats()
    stats = circuit.get_stats()

    assert stats["trips"] == 0
    assert stats["refused"] == 0
    assert circuit.state_of(URL) == STATE_OPEN


def test_invalid_settings_are_rejected():
    with pytest.raises(ValueError):
        CircuitBreaker(failure_threshold=0)
    with pytest.raises(ValueError):
        CircuitBreaker(window=0)
    with pytest.raises(ValueError):
        CircuitBreaker(recovery_time=-1)
