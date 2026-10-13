"""Тесты вежливости в обходе: robots.txt, лимит скорости, задержки (День 4)."""

from __future__ import annotations

import time

import pytest

from src.crawler import AsyncCrawler
from src.main import format_robots_table, parse_args
from src.models import ERROR_ROBOTS
from src.sitemap import SITEMAP_AUTO
from src.utils import format_crawl_stats, format_progress, format_rate_stats

# /site/a1 обычный обход загружает, поэтому запрет на него хорошо виден.
BLOCK_A1 = "User-agent: *\nDisallow: /site/a1\n"
BLOCK_A1_WITH_DELAY = "User-agent: *\nDisallow: /site/a1\nCrawl-delay: 0.05\n"
BLOCK_FOR_MYBOT = "User-agent: MyBot\nDisallow: /site/b1\n"

FAST_RPS = 20.0
FAST_INTERVAL = 1.0 / FAST_RPS


def paths(urls) -> set[str]:
    return {"/" + url.split("/", 3)[3] for url in urls}


# --- соблюдение robots.txt --------------------------------------------------


async def test_crawl_skips_urls_blocked_by_robots(url, hits, robots_txt):
    robots_txt(BLOCK_A1)

    async with AsyncCrawler(max_concurrent=2, respect_robots=True) as crawler:
        await crawler.crawl([url("/site/")], max_pages=50, same_domain_only=True)

    assert "/site/a1" not in paths(crawler.processed_urls)
    assert hits["/site/a1"] == 0
    assert crawler.skipped["robots"] == 1
    assert crawler.blocked_urls == {url("/site/a1"): "Disallow: /site/a1"}


async def test_blocked_url_is_not_counted_as_failure(url, robots_txt):
    robots_txt(BLOCK_A1)

    async with AsyncCrawler(max_concurrent=2, respect_robots=True) as crawler:
        pages = await crawler.crawl(
            [url("/site/")], max_pages=50, same_domain_only=True
        )
        stats = crawler.get_crawl_stats()

    assert url("/site/a1") not in crawler.failed_urls
    assert all(page["url"] != url("/site/a1") for page in pages)
    assert stats["blocked"] == 1
    assert stats["robots"]["blocked"] == 1


async def test_blocked_url_does_not_spend_page_budget(url, robots_txt):
    """Запрещённая ссылка не должна вытеснять разрешённую страницу из лимита."""
    robots_txt(BLOCK_A1)

    async with AsyncCrawler(max_concurrent=1, respect_robots=True) as crawler:
        pages = await crawler.crawl(
            [url("/site/")], max_pages=4, same_domain_only=True
        )

    assert len(pages) == 4
    assert url("/site/a1") not in {page["url"] for page in pages}


async def test_crawl_without_respect_robots_ignores_rules(url, hits, robots_txt):
    robots_txt(BLOCK_A1)

    async with AsyncCrawler(max_concurrent=2) as crawler:
        await crawler.crawl([url("/site/")], max_pages=50, same_domain_only=True)

    assert hits["/site/a1"] == 1
    assert crawler.blocked_urls == {}
    assert crawler.get_crawl_stats()["robots"]["enabled"] is False


async def test_robots_txt_is_fetched_once_per_domain(url, agents, robots_txt):
    robots_txt(BLOCK_A1)

    async with AsyncCrawler(max_concurrent=4, respect_robots=True) as crawler:
        await crawler.crawl([url("/site/")], max_pages=50, same_domain_only=True)

    assert [path for path, _agent in agents].count("/robots.txt") == 1
    assert crawler.get_crawl_stats()["robots"]["fetched"] == 1


async def test_cached_robots_visible_when_fetched_before_crawl(url, robots_txt):
    """robots.txt читают и до обхода — ради карты сайта; это должно быть видно.

    Счётчик загрузок обнуляется в начале `crawl`, а правила берутся из кэша,
    поэтому одного `fetched` для отчёта не хватает.
    """
    robots_txt(f"User-agent: *\nSitemap: {url('/sitemap.xml')}\n")

    async with AsyncCrawler(max_concurrent=2, respect_robots=True) as crawler:
        seeds = await crawler.seeds_from_sitemaps(
            [SITEMAP_AUTO], base_urls=[url("/site/")], limit=2
        )
        await crawler.crawl(seeds, max_pages=2, same_domain_only=True)
        stats = crawler.get_crawl_stats()

    assert (stats["robots"]["fetched"], stats["robots"]["domains"]) == (0, 1)
    assert "файлов загружено 0 (в кэше 1)" in format_crawl_stats(stats)


async def test_missing_robots_txt_allows_whole_site(url, robots_txt):
    robots_txt(None)  # сервер отвечает 404

    async with AsyncCrawler(max_concurrent=2, respect_robots=True) as crawler:
        await crawler.crawl([url("/site/")], max_pages=50, same_domain_only=True)
        stats = crawler.get_crawl_stats()

    assert crawler.blocked_urls == {}
    assert stats["robots"]["missing"] == 1
    assert "/site/a1" in paths(crawler.processed_urls)


async def test_fetch_one_returns_robots_error_without_request(url, hits, robots_txt):
    robots_txt(BLOCK_A1)

    async with AsyncCrawler(respect_robots=True) as crawler:
        result = await crawler.fetch_one(url("/site/a1"))

    assert result.ok is False
    assert result.error_type == ERROR_ROBOTS
    assert "Disallow: /site/a1" in (result.error or "")
    assert hits["/site/a1"] == 0


async def test_fetch_one_allows_permitted_url(url, robots_txt):
    robots_txt(BLOCK_A1)

    async with AsyncCrawler(respect_robots=True) as crawler:
        result = await crawler.fetch_one(url("/site/b1"))

    assert result.ok is True


async def test_robots_rules_apply_to_the_request_user_agent(url, hits, robots_txt):
    robots_txt(BLOCK_FOR_MYBOT)

    async with AsyncCrawler(respect_robots=True, user_agent="MyBot/1.0") as blocked:
        assert (await blocked.fetch_one(url("/site/b1"))).error_type == ERROR_ROBOTS
    async with AsyncCrawler(respect_robots=True, user_agent="Other/1.0") as allowed:
        assert (await allowed.fetch_one(url("/site/b1"))).ok is True

    assert hits["/site/b1"] == 1


async def test_default_user_agent_is_not_matched_by_substring(url, robots_txt):
    """Группа выбирается по имени краулера, а не по подстроке во всём агенте.

    В User-Agent по умолчанию есть слово «bot» в ссылке, но это не делает нас
    краулером `bot` из robots.txt.
    """
    robots_txt("User-agent: bot\nDisallow: /\n")

    async with AsyncCrawler(respect_robots=True) as crawler:
        result = await crawler.fetch_one(url("/site/b1"))

    assert result.ok is True


async def test_no_robots_checks_after_the_budget_is_spent(url, robots_txt):
    """Исчерпан лимит страниц — спрашивать robots.txt уже не о чем."""
    robots_txt(BLOCK_A1)

    async with AsyncCrawler(max_concurrent=1, respect_robots=True) as crawler:
        await crawler.crawl([url("/site/")], max_pages=1, same_domain_only=True)
        stats = crawler.get_crawl_stats()

    assert stats["robots"]["checks"] == 1
    assert crawler.blocked_urls == {}


# --- лимит скорости в обходе ------------------------------------------------


async def test_crawl_respects_requests_per_second(url):
    async with AsyncCrawler(
        max_concurrent=4, requests_per_second=FAST_RPS
    ) as crawler:
        started = time.perf_counter()
        pages = await crawler.crawl(
            [url("/site/")], max_pages=4, same_domain_only=True
        )
        elapsed = time.perf_counter() - started
        stats = crawler.get_crawl_stats()

    assert len(pages) == 4
    # Первая страница уходит сразу, дальше — по шагу на каждую.
    assert elapsed >= FAST_INTERVAL * 3
    assert stats["rate_limit"]["requests"] == 4
    assert stats["rate_limit"]["waited_total"] > 0


async def test_min_delay_applies_without_rps(url):
    async with AsyncCrawler(max_concurrent=4, min_delay=FAST_INTERVAL) as crawler:
        started = time.perf_counter()
        await crawler.crawl([url("/site/")], max_pages=3, same_domain_only=True)
        elapsed = time.perf_counter() - started

    assert elapsed >= FAST_INTERVAL * 2


async def test_pages_record_how_long_they_waited(url):
    async with AsyncCrawler(
        max_concurrent=2, requests_per_second=FAST_RPS
    ) as crawler:
        pages = await crawler.crawl(
            [url("/site/")], max_pages=3, same_domain_only=True
        )

    assert pages[0]["waited"] == 0.0  # первая страница не ждала
    assert any(page["waited"] > 0 for page in pages[1:])


async def test_crawl_delay_from_robots_reaches_the_limiter(url, robots_txt):
    robots_txt(BLOCK_A1_WITH_DELAY)

    async with AsyncCrawler(max_concurrent=2, respect_robots=True) as crawler:
        await crawler.crawl([url("/site/")], max_pages=3, same_domain_only=True)
        stats = crawler.get_crawl_stats()

    assert crawler.rate_limiter.interval_for(url("/site/")) == pytest.approx(0.05)
    assert stats["robots"]["crawl_delays"]
    assert stats["rate_limit"]["enabled"] is True


async def test_crawl_delay_applies_to_the_first_page(url, robots_txt):
    """`Crawl-delay` известен сразу после robots.txt, значит ждёт уже первая страница."""
    delay = 0.15
    robots_txt(f"User-agent: *\nCrawl-delay: {delay}\n")

    async with AsyncCrawler(max_concurrent=1, respect_robots=True) as crawler:
        started = time.perf_counter()
        pages = await crawler.crawl(
            [url("/site/")], max_pages=2, same_domain_only=True
        )
        elapsed = time.perf_counter() - started

    # Шаг отсчитывается от запроса robots.txt, поэтому ждут обе страницы.
    assert pages[0]["waited"] >= delay * 0.9
    assert elapsed >= delay * 2 * 0.9


async def test_rate_limit_stats_are_reset_between_crawls(url):
    async with AsyncCrawler(
        max_concurrent=2, requests_per_second=FAST_RPS
    ) as crawler:
        await crawler.crawl([url("/site/")], max_pages=3, same_domain_only=True)
        first = crawler.get_crawl_stats()["rate_limit"]["requests"]

        await crawler.crawl([url("/site/a")], max_pages=1, same_domain_only=True)
        second = crawler.get_crawl_stats()["rate_limit"]["requests"]

    assert first == 3
    assert second == 1


async def test_robots_stats_are_reset_between_crawls(url, robots_txt):
    robots_txt(BLOCK_A1)

    async with AsyncCrawler(max_concurrent=2, respect_robots=True) as crawler:
        await crawler.crawl([url("/site/")], max_pages=50, same_domain_only=True)
        assert crawler.get_crawl_stats()["robots"]["blocked"] == 1

        await crawler.crawl([url("/site/b")], max_pages=1, same_domain_only=True)
        stats = crawler.get_crawl_stats()

    assert stats["robots"]["blocked"] == 0
    assert crawler.blocked_urls == {}
    # Правила остались в кэше: повторной загрузки robots.txt не было.
    assert stats["robots"]["fetched"] == 0


async def test_rate_limit_is_off_by_default(url):
    async with AsyncCrawler(max_concurrent=4) as crawler:
        await crawler.crawl([url("/site/")], max_pages=3, same_domain_only=True)
        stats = crawler.get_crawl_stats()

    assert stats["rate_limit"]["enabled"] is False
    assert stats["rate_limit"]["waited_total"] == 0.0


# --- backoff после ошибок ---------------------------------------------------


async def test_server_error_slows_down_the_domain(url):
    # Повторы дня 5 здесь не при чём: проверяется штраф лимитеру, и одна
    # попытка делает тест и быстрее, и точнее.
    async with AsyncCrawler(max_backoff=1.0, max_retries=0) as crawler:
        result = await crawler.fetch_one(url("/status/503"))

    assert result.ok is False
    assert crawler.rate_limiter.get_stats()["penalties"] == 1
    assert crawler.rate_limiter.backoff_for(url("/")) > 0


async def test_not_found_does_not_slow_down_the_domain(url):
    async with AsyncCrawler() as crawler:
        result = await crawler.fetch_one(url("/status/404"))

    assert result.ok is False
    assert crawler.rate_limiter.get_stats()["penalties"] == 0


async def test_successful_request_reduces_the_penalty(url):
    async with AsyncCrawler(max_backoff=1.0, max_retries=0) as crawler:
        await crawler.fetch_one(url("/status/503"))
        penalty = crawler.rate_limiter.backoff_for(url("/"))

        await crawler.fetch_one(url("/ok"))

    assert crawler.rate_limiter.backoff_for(url("/")) == pytest.approx(penalty / 2)


# --- User-Agent -------------------------------------------------------------


async def test_user_agent_is_sent_with_every_request(url, agents):
    async with AsyncCrawler(user_agent="SoloBot/1.0") as crawler:
        await crawler.crawl([url("/site/")], max_pages=3, same_domain_only=True)

    assert {agent for _path, agent in agents} == {"SoloBot/1.0"}


async def test_user_agents_are_rotated(url, agents):
    rotation = ["BotA/1.0", "BotB/1.0"]
    async with AsyncCrawler(max_concurrent=1, user_agents=rotation) as crawler:
        await crawler.crawl([url("/site/")], max_pages=4, same_domain_only=True)

    used = [agent for path, agent in agents if path != "/robots.txt"]
    assert used == rotation * 2


async def test_rotation_keeps_robots_check_in_sync(url, hits, agents, robots_txt):
    """Страница проверяется и грузится под одним и тем же агентом."""
    robots_txt(BLOCK_FOR_MYBOT)

    async with AsyncCrawler(
        max_concurrent=1,
        respect_robots=True,
        user_agents=["MyBot/1.0", "Other/1.0"],
    ) as crawler:
        # Первый обход достаётся MyBot, которому /site/b1 запрещён.
        await crawler.crawl([url("/site/b1")], max_pages=1, max_depth=0)
        assert crawler.blocked_urls == {url("/site/b1"): "Disallow: /site/b1"}
        assert hits["/site/b1"] == 0

        # Ротация пошла дальше, и для Other та же страница разрешена.
        await crawler.crawl([url("/site/b1")], max_pages=1, max_depth=0)
        assert paths(crawler.processed_urls) == {"/site/b1"}
        # Одна проверка на страницу: воркер спрашивает разрешение, а fetch_one
        # повторно его не запрашивает.
        assert crawler.get_crawl_stats()["robots"]["checks"] == 1

    assert ("/site/b1", "Other/1.0") in agents


# --- вывод статистики -------------------------------------------------------


async def test_progress_line_shows_politeness(url, robots_txt):
    robots_txt(BLOCK_A1)

    async with AsyncCrawler(
        max_concurrent=2, respect_robots=True, requests_per_second=FAST_RPS
    ) as crawler:
        await crawler.crawl([url("/site/")], max_pages=5, same_domain_only=True)
        line = format_progress(crawler.get_crawl_stats())

    assert "ожидание:" in line
    assert "robots: 1" in line


async def test_progress_line_stays_short_without_politeness(url):
    async with AsyncCrawler(max_concurrent=2) as crawler:
        await crawler.crawl([url("/site/")], max_pages=2, same_domain_only=True)
        line = format_progress(crawler.get_crawl_stats())

    assert "ожидание:" not in line
    assert "robots:" not in line


async def test_summary_includes_politeness_blocks(url, robots_txt):
    robots_txt(BLOCK_A1_WITH_DELAY)

    async with AsyncCrawler(
        max_concurrent=2, respect_robots=True, requests_per_second=FAST_RPS
    ) as crawler:
        await crawler.crawl([url("/site/")], max_pages=5, same_domain_only=True)
        summary = format_crawl_stats(crawler.get_crawl_stats())

    assert "Ограничение скорости" in summary
    assert "robots.txt: проверок" in summary
    assert "Crawl-delay" in summary
    assert "robots=1" in summary  # причина пропуска в строке пропусков


def test_rate_stats_report_no_limit():
    text = format_rate_stats({"requests_per_second": None, "per_domain": True})

    assert "без лимита" in text


async def test_rate_stats_name_the_global_limit_readably(url):
    """Общий лимит живёт под служебным ключом `*` — в отчёте он назван словами."""
    async with AsyncCrawler(
        max_concurrent=2, requests_per_second=FAST_RPS, per_domain_rate=False
    ) as crawler:
        await crawler.crawl([url("/site/")], max_pages=2, same_domain_only=True)
        text = format_rate_stats(crawler.get_crawl_stats()["rate_limit"])

    assert "все домены" in text
    assert "*:" not in text


def test_robots_table_marks_blocked_urls():
    table = format_robots_table(
        [
            {"url": "http://a/ok", "allowed": True, "rule": None},
            {"url": "http://a/no", "allowed": False, "rule": "Disallow: /no"},
        ]
    )

    assert "разрешён" in table
    assert "ЗАПРЕЩЁН" in table
    assert "Disallow: /no" in table


# --- CLI --------------------------------------------------------------------


@pytest.mark.parametrize(
    "argv",
    [
        ["--crawl", "--rps", "-1"],
        ["--crawl", "--min-delay", "-0.5"],
        ["--crawl", "--jitter", "-1"],
        ["--crawl", "--max-backoff", "-2"],
    ],
)
def test_negative_politeness_options_are_rejected(argv):
    with pytest.raises(SystemExit):
        parse_args(argv)


def test_politeness_options_are_off_by_default():
    args = parse_args([])

    assert args.rps == 0.0
    assert args.min_delay == 0.0
    assert args.jitter == 0.0
    assert args.respect_robots is False


def test_polite_preset_turns_everything_on():
    args = parse_args(["--polite"])

    assert args.crawl is True  # пресет сам включает режим обхода
    assert args.respect_robots is True
    assert args.rps == 2.0
    assert args.min_delay == 0.5
    assert args.jitter == 0.2


def test_explicit_option_overrides_the_preset():
    args = parse_args(["--polite", "--rps", "5", "--min-delay", "0"])

    assert args.rps == 5.0
    assert args.min_delay == 0.0


def test_rotate_ua_accepts_several_agents():
    args = parse_args(["--crawl", "--rotate-ua", "A/1.0", "B/1.0"])

    assert args.rotate_ua == ["A/1.0", "B/1.0"]
