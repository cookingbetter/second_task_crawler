"""Тесты ограничения скорости запросов (День 4)."""

from __future__ import annotations

import asyncio
import random
import time

import pytest

from src.ratelimit import GLOBAL_KEY, RateLimiter

# Интервалы в тестах намеренно крошечные: проверяется соблюдение шага, а не
# конкретные числа, поэтому весь файл работает доли секунды.
FAST_RPS = 20.0
FAST_INTERVAL = 1.0 / FAST_RPS

URL_A = "http://a.example/page"
URL_B = "http://b.example/page"


async def elapsed_of(coro) -> float:
    started = time.perf_counter()
    await coro
    return time.perf_counter() - started


# --- шаг между запросами ----------------------------------------------------


async def test_first_request_does_not_wait():
    limiter = RateLimiter(FAST_RPS)

    assert await limiter.acquire(URL_A) == 0.0


async def test_requests_to_one_domain_are_paced():
    limiter = RateLimiter(FAST_RPS)
    started = time.perf_counter()

    for _ in range(3):
        await limiter.acquire(URL_A)

    # Первый запрос уходит сразу, значит ждём два шага.
    assert time.perf_counter() - started >= FAST_INTERVAL * 2


async def test_different_domains_do_not_wait_for_each_other():
    limiter = RateLimiter(FAST_RPS)
    await limiter.acquire(URL_A)

    assert await limiter.acquire(URL_B) == 0.0


async def test_global_limit_is_shared_between_domains():
    limiter = RateLimiter(FAST_RPS, per_domain=False)
    await limiter.acquire(URL_A)

    assert await limiter.acquire(URL_B) >= FAST_INTERVAL / 2


async def test_global_limit_uses_single_key():
    limiter = RateLimiter(FAST_RPS, per_domain=False)
    await limiter.acquire(URL_A)
    await limiter.acquire(URL_B)

    assert list(limiter.get_stats()["interval_by_domain"]) == [GLOBAL_KEY]


async def test_domain_key_ignores_www_and_port():
    limiter = RateLimiter(FAST_RPS)
    await limiter.acquire("http://example.com/a")

    waited = await limiter.acquire("http://www.example.com:80/b")

    assert waited > 0
    assert list(limiter.get_stats()["interval_by_domain"]) == ["example.com"]


async def test_no_limit_means_no_waiting():
    limiter = RateLimiter(None)

    for _ in range(5):
        assert await limiter.acquire(URL_A) == 0.0
    assert limiter.enabled is False


async def test_zero_rps_is_treated_as_no_limit():
    limiter = RateLimiter(0)

    assert limiter.base_interval == 0.0
    assert await elapsed_of(limiter.acquire(URL_A)) < FAST_INTERVAL


async def test_waiting_order_is_fifo():
    """Запросы уходят в том порядке, в котором их попросили."""
    limiter = RateLimiter(FAST_RPS)
    order: list[int] = []

    async def worker(number: int) -> None:
        await limiter.acquire(URL_A)
        order.append(number)

    # Задачи создаются по очереди, поэтому и лок достаётся им по очереди.
    tasks = [asyncio.create_task(worker(number)) for number in range(4)]
    await asyncio.gather(*tasks)

    assert order == [0, 1, 2, 3]


async def test_concurrent_requests_are_spread_in_time():
    limiter = RateLimiter(FAST_RPS)
    started = time.perf_counter()

    await asyncio.gather(*(limiter.acquire(URL_A) for _ in range(4)))

    assert time.perf_counter() - started >= FAST_INTERVAL * 3


# --- min_delay, Crawl-delay и jitter ----------------------------------------


async def test_min_delay_wins_over_faster_rps():
    limiter = RateLimiter(100.0, min_delay=0.2)

    assert limiter.interval_for(URL_A) == pytest.approx(0.2)


async def test_rps_wins_over_smaller_min_delay():
    limiter = RateLimiter(2.0, min_delay=0.1)

    assert limiter.interval_for(URL_A) == pytest.approx(0.5)


async def test_crawl_delay_from_robots_wins_over_rps():
    limiter = RateLimiter(10.0)
    limiter.set_crawl_delay(URL_A, 1.5)

    assert limiter.interval_for(URL_A) == pytest.approx(1.5)
    assert limiter.interval_for(URL_B) == pytest.approx(0.1)


async def test_crawl_delay_applies_only_to_its_domain():
    limiter = RateLimiter(None)
    limiter.set_crawl_delay("http://a.example/", 0.5)

    assert limiter.interval_for(URL_A) == pytest.approx(0.5)
    assert limiter.interval_for(URL_B) == 0.0
    assert limiter.enabled is True


async def test_zero_crawl_delay_removes_previous_value():
    limiter = RateLimiter(None)
    limiter.set_crawl_delay(URL_A, 0.5)
    limiter.set_crawl_delay(URL_A, 0.0)

    assert limiter.interval_for(URL_A) == 0.0


async def test_jitter_adds_random_tail_but_never_shortens():
    limiter = RateLimiter(10.0, jitter=0.2, rng=random.Random(0))

    delays = [limiter._delay("a.example") for _ in range(20)]

    assert all(0.1 <= delay <= 0.3 for delay in delays)
    assert len(set(delays)) > 1  # добавка действительно случайная
    # Сам шаг без разброса остаётся прежним.
    assert limiter.interval_for(URL_A) == pytest.approx(0.1)


async def test_jitter_is_deterministic_with_seeded_rng():
    first = RateLimiter(10.0, jitter=0.2, rng=random.Random(42))
    second = RateLimiter(10.0, jitter=0.2, rng=random.Random(42))

    assert first._delay("a.example") == second._delay("a.example")


# --- backoff после ошибок ---------------------------------------------------


async def test_penalize_grows_exponentially():
    limiter = RateLimiter(None, backoff_start=0.5, backoff_factor=2.0)

    assert limiter.penalize(URL_A) == pytest.approx(0.5)
    assert limiter.penalize(URL_A) == pytest.approx(1.0)
    assert limiter.penalize(URL_A) == pytest.approx(2.0)
    assert limiter.get_stats()["penalties"] == 3


async def test_penalty_is_capped_by_max_backoff():
    limiter = RateLimiter(None, backoff_start=1.0, max_backoff=2.0)

    for _ in range(10):
        limiter.penalize(URL_A)

    assert limiter.backoff_for(URL_A) == pytest.approx(2.0)


async def test_penalty_adds_to_the_interval():
    limiter = RateLimiter(2.0, backoff_start=1.0)
    limiter.penalize(URL_A)

    assert limiter.interval_for(URL_A) == pytest.approx(1.5)


async def test_penalty_is_per_domain():
    limiter = RateLimiter(None, backoff_start=1.0)
    limiter.penalize(URL_A)

    assert limiter.backoff_for(URL_B) == 0.0


async def test_reward_halves_the_penalty():
    limiter = RateLimiter(None, backoff_start=1.0)
    limiter.penalize(URL_A)

    limiter.reward(URL_A)

    assert limiter.backoff_for(URL_A) == pytest.approx(0.5)


async def test_reward_drops_negligible_penalty():
    limiter = RateLimiter(None, backoff_start=0.02)
    limiter.penalize(URL_A)

    limiter.reward(URL_A)
    limiter.reward(URL_A)

    assert limiter.backoff_for(URL_A) == 0.0
    assert limiter.get_stats()["backoff"] == {}


async def test_reward_without_penalty_does_nothing():
    limiter = RateLimiter(None)
    limiter.reward(URL_A)

    assert limiter.get_stats()["backoff"] == {}


async def test_penalty_applies_to_the_very_next_request():
    """Штраф должен сработать сразу, а не через запрос."""
    limiter = RateLimiter(FAST_RPS, backoff_start=0.2)
    await limiter.acquire(URL_A)

    limiter.penalize(URL_A)

    assert await limiter.acquire(URL_A) >= 0.2


async def test_crawl_delay_applies_to_the_very_next_request():
    """То же для `Crawl-delay`: он известен уже после загрузки robots.txt."""
    limiter = RateLimiter(FAST_RPS)
    await limiter.acquire(URL_A)

    limiter.set_crawl_delay(URL_A, 0.3)

    assert await limiter.acquire(URL_A) >= 0.25


# --- общий лимит и свойства отдельных сайтов --------------------------------


async def test_global_mode_keeps_crawl_delay_per_site():
    limiter = RateLimiter(FAST_RPS, per_domain=False)
    limiter.set_crawl_delay(URL_A, 0.3)

    assert limiter.get_stats()["crawl_delays"] == {"a.example": 0.3}
    assert limiter.interval_for(URL_A) == pytest.approx(0.3)
    assert limiter.interval_for(URL_B) == pytest.approx(FAST_INTERVAL)


async def test_global_mode_keeps_penalty_per_site():
    limiter = RateLimiter(FAST_RPS, per_domain=False, backoff_start=5.0)
    limiter.penalize(URL_A)

    assert limiter.backoff_for(URL_A) == pytest.approx(5.0)
    assert limiter.backoff_for(URL_B) == 0.0


async def test_global_mode_shares_the_clock_not_the_site_delays():
    """Общие часы — да, но ожидание медленного сайта на остальных не вешается."""
    limiter = RateLimiter(FAST_RPS, per_domain=False)
    limiter.set_crawl_delay(URL_A, 10.0)
    await limiter.acquire(URL_B)

    waited = await limiter.acquire(URL_B)

    assert waited >= FAST_INTERVAL / 2
    assert waited < 1.0


# --- статистика и чистка состояния ------------------------------------------


async def test_stats_count_requests_and_waiting():
    limiter = RateLimiter(FAST_RPS)
    for _ in range(3):
        await limiter.acquire(URL_A)

    stats = limiter.get_stats()

    assert stats["requests"] == 3
    assert stats["waited_total"] > 0
    assert stats["avg_wait"] == pytest.approx(stats["waited_total"] / 3)
    assert stats["max_wait"] >= stats["avg_wait"]
    # Между тремя запросами два промежутка, каждый не меньше шага.
    assert stats["avg_gap"] >= FAST_INTERVAL * 0.9
    assert stats["actual_rps"] > 0
    assert stats["enabled"] is True


async def test_stats_report_configuration():
    limiter = RateLimiter(4.0, per_domain=False, min_delay=0.1, jitter=0.05)

    stats = limiter.get_stats()

    assert stats["requests_per_second"] == 4.0
    assert stats["per_domain"] is False
    assert stats["base_interval"] == pytest.approx(0.25)
    assert stats["min_delay"] == 0.1
    assert stats["jitter"] == 0.05


async def test_reset_stats_keeps_pacing_state():
    limiter = RateLimiter(FAST_RPS)
    limiter.set_crawl_delay(URL_A, 0.3)
    limiter.penalize(URL_A)
    await limiter.acquire(URL_A)

    limiter.reset_stats()
    stats = limiter.get_stats()

    assert stats["requests"] == 0
    assert stats["waited_total"] == 0.0
    assert stats["penalties"] == 0
    # Задержки — состояние вежливости, а не история: они остаются.
    assert stats["crawl_delays"] == {"a.example": 0.3}
    assert stats["backoff"] == {"a.example": pytest.approx(1.0)}


async def test_locks_are_released_after_use():
    limiter = RateLimiter(FAST_RPS)
    await asyncio.gather(*(limiter.acquire(URL_A) for _ in range(3)))

    assert limiter._locks == {}
    assert limiter._refs == {}
    # Шаг между запросами при этом не теряется.
    assert await limiter.acquire(URL_A) > 0


async def test_cancelled_waiter_releases_its_lock_reference():
    limiter = RateLimiter(1.0)
    await limiter.acquire(URL_A)
    waiter = asyncio.create_task(limiter.acquire(URL_A))
    await asyncio.sleep(0)

    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter

    assert limiter._refs == {}
    assert limiter._locks == {}


# --- проверка аргументов ----------------------------------------------------


@pytest.mark.parametrize(
    "kwargs",
    [
        {"requests_per_second": -1},
        {"min_delay": -0.5},
        {"jitter": -0.1},
        {"backoff_factor": 0.5},
        {"max_backoff": -1},
        {"backoff_start": -1},
    ],
)
async def test_invalid_arguments_are_rejected(kwargs):
    with pytest.raises(ValueError):
        RateLimiter(**kwargs)
