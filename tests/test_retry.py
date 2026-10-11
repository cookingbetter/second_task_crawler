"""Тесты стратегии повторов (День 5)."""

from __future__ import annotations

import asyncio
import random

import pytest

from src.errors import (
    CrawlerTimeoutError,
    NetworkError,
    ParseError,
    PermanentError,
    RateLimitError,
    ServerError,
    TransientError,
)
from src.retry import RetryOutcome, RetryPolicy, RetryStrategy


class Sleeper:
    """Подменяет asyncio.sleep: тест видит задержки, но не ждёт их."""

    def __init__(self) -> None:
        self.delays: list[float] = []

    async def __call__(self, delay: float) -> None:
        self.delays.append(delay)


def strategy(sleeper: Sleeper, **kwargs) -> RetryStrategy:
    kwargs.setdefault("max_retries", 3)
    return RetryStrategy(sleep=sleeper, **kwargs)


def failing(error: BaseException, succeed_at: int | None = None):
    """Корутина, которая падает заданной ошибкой до нужной попытки."""
    calls = {"count": 0}

    async def call(*args, **kwargs):
        calls["count"] += 1
        if succeed_at is not None and calls["count"] >= succeed_at:
            return f"успех с {calls['count']}-й попытки"
        raise error

    call.calls = calls  # type: ignore[attr-defined]
    return call


# --- экспоненциальный backoff ----------------------------------------------


async def test_backoff_grows_exponentially():
    sleeper = Sleeper()
    retries = strategy(sleeper)
    call = failing(TransientError("перегрузка", status=408))

    with pytest.raises(TransientError):
        await retries.execute_with_retry(call)

    assert call.calls["count"] == 4  # первая попытка и три повтора
    assert sleeper.delays == [0.5, 1.0, 2.0]


async def test_backoff_is_capped_by_max_delay():
    sleeper = Sleeper()
    retries = strategy(sleeper, max_retries=4, base_delay=1.0, max_delay=3.0)

    with pytest.raises(TransientError):
        await retries.execute_with_retry(failing(TransientError("перегрузка")))

    assert sleeper.delays == [1.0, 2.0, 3.0, 3.0]


async def test_jitter_only_adds_time():
    sleeper = Sleeper()
    retries = strategy(
        sleeper, max_retries=2, jitter=0.5, rng=random.Random(0)
    )

    with pytest.raises(TransientError):
        await retries.execute_with_retry(failing(TransientError("перегрузка")))

    assert len(sleeper.delays) == 2
    assert 0.5 <= sleeper.delays[0] <= 1.0
    assert 1.0 <= sleeper.delays[1] <= 1.5


# --- что повторяется, а что нет --------------------------------------------


async def test_permanent_error_is_not_retried():
    sleeper = Sleeper()
    retries = strategy(sleeper)
    call = failing(PermanentError("HTTP 404: Not Found", status=404))

    with pytest.raises(PermanentError):
        await retries.execute_with_retry(call)

    assert call.calls["count"] == 1
    assert sleeper.delays == []
    assert retries.get_stats()["not_retried"] == 1


async def test_parse_error_is_not_retried():
    """Второй разбор того же HTML дал бы ту же ошибку."""
    sleeper = Sleeper()
    retries = strategy(sleeper)
    call = failing(ParseError("soup: ValueError"))

    with pytest.raises(ParseError):
        await retries.execute_with_retry(call)

    assert call.calls["count"] == 1


async def test_success_after_retries_returns_result():
    sleeper = Sleeper()
    retries = strategy(sleeper)
    outcome = RetryOutcome()

    result = await retries.execute_with_retry(
        failing(CrawlerTimeoutError("превышен таймаут"), succeed_at=3),
        outcome=outcome,
    )

    assert result == "успех с 3-й попытки"
    assert outcome.attempts == 3
    assert outcome.waited == pytest.approx(1.5)  # 0.5 + 1.0
    assert retries.get_stats()["retry_success"] == 1


async def test_custom_retry_on_accepts_foreign_exceptions():
    sleeper = Sleeper()
    retries = RetryStrategy(
        max_retries=2, retry_on=[ValueError], sleep=sleeper
    )
    call = failing(ValueError("своя ошибка"), succeed_at=2)

    result = await retries.execute_with_retry(call)

    assert result == "успех с 2-й попытки"
    assert sleeper.delays == [0.5]


async def test_retry_on_narrows_default_set():
    sleeper = Sleeper()
    retries = RetryStrategy(max_retries=2, retry_on=[NetworkError], sleep=sleeper)
    call = failing(ServerError("HTTP 503", status=503))

    with pytest.raises(ServerError):
        await retries.execute_with_retry(call)

    assert call.calls["count"] == 1


async def test_disabled_strategy_never_retries_even_429():
    """При max_retries=0 политика 429 не должна оживлять повторы."""
    sleeper = Sleeper()
    retries = RetryStrategy(max_retries=0, sleep=sleeper)
    call = failing(RateLimitError("HTTP 429", status=429))

    with pytest.raises(RateLimitError):
        await retries.execute_with_retry(call)

    assert call.calls["count"] == 1
    assert retries.enabled is False


# --- политики по типам ошибок ----------------------------------------------


async def test_server_error_gets_fewer_attempts_and_longer_pause():
    sleeper = Sleeper()
    retries = strategy(sleeper)
    call = failing(ServerError("HTTP 500", status=500))

    with pytest.raises(ServerError):
        await retries.execute_with_retry(call)

    # max_retries 3, но 5xx повторяется на один раз меньше и с базой 1.0 с.
    assert call.calls["count"] == 3
    assert sleeper.delays == [1.0, 2.0]


async def test_rate_limit_gets_extra_attempt_and_high_base():
    sleeper = Sleeper()
    retries = strategy(sleeper, max_retries=2)
    call = failing(RateLimitError("HTTP 429", status=429))

    with pytest.raises(RateLimitError):
        await retries.execute_with_retry(call)

    assert call.calls["count"] == 4  # на одну попытку больше обычных
    assert sleeper.delays == [5.0, 10.0, 20.0]


async def test_network_error_pause_starts_higher_than_transient():
    sleeper = Sleeper()
    retries = strategy(sleeper, max_retries=2)

    with pytest.raises(NetworkError):
        await retries.execute_with_retry(failing(NetworkError("DNS не ответил")))

    assert sleeper.delays == [1.0, 2.0]


async def test_explicit_policy_overrides_default():
    sleeper = Sleeper()
    retries = strategy(
        sleeper,
        policies={ServerError: RetryPolicy(1, 0.25, 2.0)},
    )
    call = failing(ServerError("HTTP 502", status=502))

    with pytest.raises(ServerError):
        await retries.execute_with_retry(call)

    assert call.calls["count"] == 2
    assert sleeper.delays == [0.25]


async def test_policy_is_found_through_class_hierarchy():
    """Неизвестный подвид временной ошибки берёт политику родителя."""

    class OddTransient(TransientError):
        pass

    retries = strategy(Sleeper())

    assert retries.policy_for(OddTransient("странно")).max_retries == 3
    assert retries.policy_for(ServerError("HTTP 500")).max_retries == 2


# --- Retry-After ------------------------------------------------------------


async def test_retry_after_wins_over_calculated_pause():
    sleeper = Sleeper()
    retries = strategy(
        sleeper, max_retries=1, policies={RateLimitError: RetryPolicy(1, 5.0, 2.0)}
    )
    error = RateLimitError("HTTP 429", status=429, retry_after=12.0)

    with pytest.raises(RateLimitError):
        await retries.execute_with_retry(failing(error))

    assert sleeper.delays == [12.0]


async def test_short_retry_after_does_not_speed_up_backoff():
    """Сервер разрешает прийти через 0.1 с, но свой backoff мы не сокращаем."""
    sleeper = Sleeper()
    retries = strategy(sleeper, max_retries=1)
    error = ServerError("HTTP 503", status=503, retry_after=0.1)

    with pytest.raises(ServerError):
        await retries.execute_with_retry(failing(error))

    assert sleeper.delays == [1.0]


async def test_retry_after_is_capped_by_max_delay():
    sleeper = Sleeper()
    retries = strategy(
        sleeper,
        max_retries=1,
        max_delay=5.0,
        policies={RateLimitError: RetryPolicy(1, 5.0, 2.0)},
    )
    error = RateLimitError("HTTP 429", status=429, retry_after=3600.0)

    with pytest.raises(RateLimitError):
        await retries.execute_with_retry(failing(error))

    assert sleeper.delays == [5.0]


# --- аргументы и контракты --------------------------------------------------


async def test_attempt_number_is_passed_to_the_call():
    seen: list[int] = []

    async def call(url: str, *, attempt: int = 1) -> str:
        seen.append(attempt)
        if attempt < 3:
            raise CrawlerTimeoutError("превышен таймаут")
        return url

    retries = strategy(Sleeper())
    result = await retries.execute_with_retry(
        call, "http://example.com", attempt_kwarg="attempt"
    )

    assert result == "http://example.com"
    assert seen == [1, 2, 3]


async def test_ready_coroutine_is_awaited_without_retries(caplog):
    """Готовый объект корутины одноразовый — повторить его нельзя."""

    async def call() -> str:
        return "один раз"

    retries = strategy(Sleeper())
    with caplog.at_level("WARNING"):
        result = await retries.execute_with_retry(call(), label="demo")

    assert result == "один раз"
    assert retries.get_stats()["attempts"] == 1
    assert "повторы для demo отключены" in caplog.text.lower()


async def test_not_callable_argument_is_rejected():
    retries = strategy(Sleeper())

    with pytest.raises(TypeError):
        await retries.execute_with_retry("не корутина")


async def test_cancellation_is_not_an_error():
    sleeper = Sleeper()
    retries = strategy(sleeper)

    async def call() -> None:
        raise asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        await retries.execute_with_retry(call)

    assert sleeper.delays == []
    assert retries.get_stats()["errors_total"] == 0


async def test_attempts_are_written_to_the_error():
    retries = strategy(Sleeper(), max_retries=2)

    with pytest.raises(ServerError) as exc_info:
        await retries.execute_with_retry(failing(ServerError("HTTP 500", status=500)))

    assert exc_info.value.attempts == 2


def test_invalid_settings_are_rejected():
    with pytest.raises(ValueError):
        RetryStrategy(max_retries=-1)
    with pytest.raises(ValueError):
        RetryStrategy(backoff_factor=0.5)
    with pytest.raises(ValueError):
        RetryStrategy(base_delay=-1.0)
    with pytest.raises(ValueError):
        RetryStrategy(jitter=-0.1)


# --- статистика -------------------------------------------------------------


async def test_stats_count_attempts_errors_and_waits():
    sleeper = Sleeper()
    retries = strategy(sleeper, max_retries=2)

    await retries.execute_with_retry(
        failing(CrawlerTimeoutError("превышен таймаут"), succeed_at=2)
    )
    with pytest.raises(PermanentError):
        await retries.execute_with_retry(failing(PermanentError("HTTP 404", status=404)))
    with pytest.raises(ServerError):
        await retries.execute_with_retry(failing(ServerError("HTTP 503", status=503)))

    stats = retries.get_stats()
    assert stats["calls"] == 3
    assert stats["attempts"] == 2 + 1 + 2
    assert stats["retries"] == 2
    assert stats["retry_success"] == 1
    assert stats["exhausted"] == 1
    assert stats["not_retried"] == 1
    assert stats["by_class"] == {
        "CrawlerTimeoutError": 1,
        "PermanentError": 1,
        "ServerError": 2,
    }
    assert stats["by_error_type"] == {"timeout": 1, "http": 3}
    assert stats["avg_wait"] == pytest.approx(stats["wait_total"] / 2)


async def test_reset_stats_keeps_settings():
    retries = strategy(Sleeper(), max_retries=1)
    with pytest.raises(ServerError):
        await retries.execute_with_retry(failing(ServerError("HTTP 500", status=500)))

    retries.reset_stats()
    stats = retries.get_stats()

    assert stats["calls"] == 0
    assert stats["attempts"] == 0
    assert stats["by_class"] == {}
    assert stats["max_retries"] == 1
    assert stats["enabled"] is True
