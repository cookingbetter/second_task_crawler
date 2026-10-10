"""Тесты разбора robots.txt и проверки разрешений (День 4)."""

from __future__ import annotations

import asyncio

import pytest

from src.robots import (
    SOURCE_ERROR,
    SOURCE_MISSING,
    SOURCE_OK,
    RobotsParser,
    RobotsResponse,
    parse_robots,
)

SIMPLE = """
User-agent: *
Disallow: /private/
Crawl-delay: 1.5
"""

MULTI_GROUP = """
# Правила для всех
User-agent: *
Disallow: /admin
Crawl-delay: 2

User-agent: MyBot
User-agent: OtherBot
Disallow: /only-for-bots
Crawl-delay: 0.25

User-agent: HTTrack
Disallow: /
"""

URL = "http://example.com"


def fake_fetcher(
    text: str = "",
    *,
    status: int | None = 200,
    error: str | None = None,
    calls: list[str] | None = None,
    delay: float = 0.0,
):
    """Загрузчик robots.txt без сети: отдаёт заранее заданный ответ."""

    async def _fetch(url: str) -> RobotsResponse:
        if calls is not None:
            calls.append(url)
        if delay:
            await asyncio.sleep(delay)
        return RobotsResponse(status=status, text=text, error=error)

    return _fetch


# --- разбор правил ----------------------------------------------------------


def test_disallow_blocks_prefix():
    rules = parse_robots(SIMPLE)

    assert rules.decide("/private/secret", "AnyBot") == (
        False,
        "Disallow: /private/",
    )
    assert rules.decide("/public", "AnyBot") == (True, None)


def test_empty_robots_allows_everything():
    rules = parse_robots("")

    assert rules.groups == {}
    assert rules.decide("/anything", "AnyBot") == (True, None)


def test_empty_disallow_value_allows_everything():
    rules = parse_robots("User-agent: *\nDisallow:")

    assert rules.decide("/anything", "AnyBot") == (True, None)


def test_disallow_root_blocks_everything():
    rules = parse_robots("User-agent: *\nDisallow: /")

    assert rules.decide("/", "AnyBot")[0] is False
    assert rules.decide("/deep/page", "AnyBot")[0] is False


def test_longer_allow_wins_over_disallow():
    rules = parse_robots(
        "User-agent: *\nDisallow: /private/\nAllow: /private/public"
    )

    assert rules.decide("/private/public/page", "Bot") == (
        True,
        "Allow: /private/public",
    )
    assert rules.decide("/private/other", "Bot")[0] is False


def test_allow_wins_on_equal_length():
    rules = parse_robots("User-agent: *\nDisallow: /page\nAllow: /page")

    assert rules.decide("/page", "Bot") == (True, "Allow: /page")


def test_wildcard_matches_any_part_of_path():
    rules = parse_robots("User-agent: *\nDisallow: /*/private")

    assert rules.decide("/section/private", "Bot")[0] is False
    assert rules.decide("/a/b/private", "Bot")[0] is False
    assert rules.decide("/private", "Bot")[0] is True


def test_dollar_anchors_end_of_url():
    rules = parse_robots("User-agent: *\nDisallow: /*.pdf$")

    assert rules.decide("/docs/file.pdf", "Bot")[0] is False
    assert rules.decide("/docs/file.pdf.html", "Bot")[0] is True


def test_query_string_is_part_of_the_target():
    rules = parse_robots("User-agent: *\nDisallow: /*?")

    assert rules.decide("/search?q=1", "Bot")[0] is False
    assert rules.decide("/search", "Bot")[0] is True


def test_special_characters_are_not_treated_as_regex():
    rules = parse_robots("User-agent: *\nDisallow: /a+b(c)")

    assert rules.decide("/a+b(c)", "Bot")[0] is False
    assert rules.decide("/aab", "Bot")[0] is True


def test_comments_and_unknown_directives_are_ignored():
    rules = parse_robots(
        "Sitemap: http://example.com/sitemap.xml\n"
        "User-agent: *   # всем\n"
        "Disallow: /x  # кроме этого\n"
        "Host: example.com\n"
        "мусор без двоеточия\n"
    )

    assert sorted(rules.groups) == ["*"]
    assert rules.decide("/x", "Bot")[0] is False


def test_rules_before_any_user_agent_are_ignored():
    rules = parse_robots("Disallow: /early\nUser-agent: *\nDisallow: /late")

    assert rules.decide("/early", "Bot")[0] is True
    assert rules.decide("/late", "Bot")[0] is False


def test_directives_are_case_insensitive():
    rules = parse_robots("USER-AGENT: *\nDISALLOW: /x\nCRAWL-DELAY: 3")

    assert rules.decide("/x", "Bot")[0] is False
    assert rules.crawl_delay_for("Bot") == 3.0


def test_path_case_is_preserved():
    rules = parse_robots("User-agent: *\nDisallow: /Private")

    assert rules.decide("/Private", "Bot")[0] is False
    assert rules.decide("/private", "Bot")[0] is True


# --- группы User-agent ------------------------------------------------------


def test_consecutive_user_agents_share_rules():
    rules = parse_robots(MULTI_GROUP)

    assert rules.decide("/only-for-bots", "MyBot/1.0")[0] is False
    assert rules.decide("/only-for-bots", "OtherBot/2.0")[0] is False


def test_specific_group_replaces_wildcard_group():
    rules = parse_robots(MULTI_GROUP)

    # Для MyBot работает только его группа, запрет /admin из `*` не действует.
    assert rules.decide("/admin", "MyBot/1.0")[0] is True
    assert rules.decide("/admin", "Unknown/1.0")[0] is False


def test_group_matching_ignores_case():
    rules = parse_robots(MULTI_GROUP)

    assert rules.group_for("mybot/1.0").name == "mybot"
    assert rules.group_for("HTTRACK").name == "httrack"


def test_longest_matching_group_wins():
    rules = parse_robots(
        "User-agent: googlebot\nDisallow: /a\n\n"
        "User-agent: googlebot-news\nDisallow: /b\n"
    )

    assert rules.group_for("Googlebot-News/1.0").name == "googlebot-news"
    assert rules.group_for("Googlebot/2.1").name == "googlebot"


def test_group_is_chosen_by_product_token_only():
    """Имя краулера — это текст до первого `/`, остальное к группам не относится."""
    rules = parse_robots(
        "User-agent: bot\nDisallow: /\n\nUser-agent: *\nDisallow: /private/\n"
    )

    agent = "AsyncCrawler/1.0 (+https://example.com/bot)"

    assert rules.group_for(agent).name == "*"
    assert rules.decide("/page", agent)[0] is True
    assert rules.decide("/private/x", agent)[0] is False
    # А настоящий `bot` своей группой по-прежнему блокируется.
    assert rules.decide("/page", "bot/1.0")[0] is False


def test_group_is_none_without_wildcard_group():
    rules = parse_robots("User-agent: HTTrack\nDisallow: /")

    assert rules.group_for("FriendlyBot") is None
    assert rules.decide("/", "FriendlyBot") == (True, None)


def test_crawl_delay_is_per_group():
    rules = parse_robots(MULTI_GROUP)

    assert rules.crawl_delay_for("MyBot/1.0") == 0.25
    assert rules.crawl_delay_for("Unknown/1.0") == 2.0
    assert rules.crawl_delay_for("HTTrack") == 0.0


@pytest.mark.parametrize("value", ["быстро", "", "-1"])
def test_invalid_crawl_delay_is_ignored(value):
    rules = parse_robots(f"User-agent: *\nCrawl-delay: {value}")

    assert rules.crawl_delay_for("Bot") == 0.0


def test_summary_describes_selected_group():
    summary = parse_robots(MULTI_GROUP, status=200).summary("MyBot/1.0")

    assert summary["available"] is True
    assert summary["status"] == 200
    assert summary["group"] == "mybot"
    assert summary["rules"] == 1
    assert summary["crawl_delay"] == 0.25
    assert summary["groups"]["*"] == {"rules": 1, "crawl_delay": 2.0}


# --- загрузка и кэш --------------------------------------------------------


async def test_rules_are_fetched_once_per_domain():
    calls: list[str] = []
    parser = RobotsParser(fake_fetcher(SIMPLE, calls=calls))

    await parser.allows(f"{URL}/one")
    await parser.allows(f"{URL}/two")

    assert calls == [f"{URL}/robots.txt"]


async def test_concurrent_checks_trigger_single_fetch():
    calls: list[str] = []
    parser = RobotsParser(fake_fetcher(SIMPLE, calls=calls, delay=0.01))

    await asyncio.gather(*(parser.allows(f"{URL}/{i}") for i in range(5)))

    assert len(calls) == 1


async def test_each_domain_has_its_own_rules():
    calls: list[str] = []
    parser = RobotsParser(fake_fetcher(SIMPLE, calls=calls))

    await parser.allows("http://a.example/private/x")
    await parser.allows("http://b.example/private/x")

    assert len(calls) == 2
    assert parser.get_stats()["domains"] == 2


async def test_default_port_shares_one_cache_entry():
    calls: list[str] = []
    parser = RobotsParser(fake_fetcher(SIMPLE, calls=calls))

    await parser.allows("http://example.com/x")
    await parser.allows("http://example.com:80/x")

    assert len(calls) == 1


async def test_www_has_its_own_rules():
    """`www.example.com` — отдельный сервер со своим robots.txt."""
    calls: list[str] = []
    parser = RobotsParser(fake_fetcher(SIMPLE, calls=calls))

    await parser.allows("http://example.com/x")
    await parser.allows("http://www.example.com/x")

    assert len(calls) == 2


async def test_ports_have_their_own_rules():
    """Два сервиса на одном хосте запрещают разное — правила не смешиваются."""

    async def fetcher(url: str) -> RobotsResponse:
        text = "User-agent: *\nDisallow: /a/" if ":8001" in url else "User-agent: *\n"
        return RobotsResponse(status=200, text=text)

    parser = RobotsParser(fetcher)

    blocked, _rule = await parser.allows("http://host:8001/a/x")
    allowed, _rule = await parser.allows("http://host:8002/a/x")

    assert blocked is False
    assert allowed is True


async def test_blocked_url_is_reported_with_rule():
    parser = RobotsParser(fake_fetcher(SIMPLE))

    allowed, rule = await parser.allows(f"{URL}/private/page")

    assert allowed is False
    assert rule == "Disallow: /private/"


async def test_path_params_reach_the_rules():
    """`;params` — часть пути, и правило про идентификатор сессии срабатывает."""
    parser = RobotsParser(fake_fetcher("User-agent: *\nDisallow: /*;sessionid"))

    allowed, rule = await parser.allows(f"{URL}/page;sessionid=7?q=1")

    assert allowed is False
    assert rule == "Disallow: /*;sessionid"


async def test_missing_robots_allows_everything():
    parser = RobotsParser(fake_fetcher(status=404))

    allowed, _rule = await parser.allows(f"{URL}/anything")

    assert allowed is True
    assert parser.cached_rules(URL).source == SOURCE_MISSING
    assert parser.get_stats()["missing"] == 1


async def test_server_error_allows_by_default():
    parser = RobotsParser(fake_fetcher(status=503))

    allowed, _rule = await parser.allows(f"{URL}/anything")

    assert allowed is True
    assert parser.cached_rules(URL).source == SOURCE_ERROR
    assert parser.get_stats()["errors"] == 1


async def test_server_error_blocks_when_configured():
    parser = RobotsParser(fake_fetcher(status=503), allow_on_error=False)

    allowed, rule = await parser.allows(f"{URL}/anything")

    assert allowed is False
    assert rule == "Disallow: /"


async def test_too_many_requests_is_not_treated_as_missing():
    """429 — «сейчас не отдам», а не «файла нет»: правила остались неизвестны."""
    parser = RobotsParser(fake_fetcher(status=429))

    allowed, _rule = await parser.allows(f"{URL}/anything")

    assert allowed is True
    assert parser.cached_rules(URL).source == SOURCE_ERROR
    assert parser.get_stats()["errors"] == 1
    assert parser.get_stats()["missing"] == 0


async def test_too_many_requests_blocks_when_configured():
    parser = RobotsParser(fake_fetcher(status=429), allow_on_error=False)

    allowed, rule = await parser.allows(f"{URL}/anything")

    assert allowed is False
    assert rule == "Disallow: /"


async def test_network_error_is_handled():
    parser = RobotsParser(fake_fetcher(error="соединение не установлено"))

    allowed, _rule = await parser.allows(f"{URL}/anything")

    assert allowed is True
    assert parser.get_stats()["errors"] == 1


async def test_broken_fetcher_does_not_break_the_check():
    async def boom(url: str) -> RobotsResponse:
        raise RuntimeError("загрузчик упал")

    parser = RobotsParser(boom)

    assert await parser.allows(f"{URL}/anything") == (True, None)
    assert parser.get_stats()["errors"] == 1


async def test_fetch_robots_returns_summary():
    parser = RobotsParser(fake_fetcher(SIMPLE), user_agent="MyBot/1.0")

    summary = await parser.fetch_robots(f"{URL}/start")

    assert summary["origin"] == "http://example.com"
    assert summary["domain"] == "example.com"
    assert summary["url"] == f"{URL}/robots.txt"
    assert summary["user_agent"] == "MyBot/1.0"
    assert summary["source"] == SOURCE_OK
    assert summary["crawl_delay"] == 1.5


# --- синхронные обращения к кэшу -------------------------------------------


async def test_can_fetch_uses_loaded_rules():
    parser = RobotsParser(fake_fetcher(SIMPLE))
    await parser.fetch_robots(URL)

    assert parser.can_fetch(f"{URL}/private/x") is False
    assert parser.can_fetch(f"{URL}/open") is True


def test_can_fetch_allows_when_rules_are_not_loaded():
    parser = RobotsParser(fake_fetcher(SIMPLE))

    assert parser.can_fetch(f"{URL}/private/x") is True


async def test_can_fetch_respects_user_agent():
    parser = RobotsParser(fake_fetcher(MULTI_GROUP))
    await parser.fetch_robots(URL)

    assert parser.can_fetch(f"{URL}/admin", "Unknown/1.0") is False
    assert parser.can_fetch(f"{URL}/admin", "MyBot/1.0") is True


async def test_get_crawl_delay_for_single_cached_domain():
    parser = RobotsParser(fake_fetcher(SIMPLE))
    await parser.fetch_robots(URL)

    assert parser.get_crawl_delay() == 1.5


async def test_get_crawl_delay_needs_url_for_several_domains():
    parser = RobotsParser(fake_fetcher(SIMPLE))
    await parser.fetch_robots("http://a.example/")
    await parser.fetch_robots("http://b.example/")

    assert parser.get_crawl_delay() == 0.0
    assert parser.get_crawl_delay(url="http://a.example/page") == 1.5


async def test_get_crawl_delay_for_unknown_domain_is_zero():
    parser = RobotsParser(fake_fetcher(SIMPLE))

    assert parser.get_crawl_delay(url="http://unknown.example/") == 0.0


async def test_get_crawl_delay_respects_user_agent():
    parser = RobotsParser(fake_fetcher(MULTI_GROUP))
    await parser.fetch_robots(URL)

    assert parser.get_crawl_delay("MyBot/1.0") == 0.25
    assert parser.get_crawl_delay("Unknown/1.0") == 2.0


# --- статистика -------------------------------------------------------------


async def test_stats_count_checks_and_blocks():
    parser = RobotsParser(fake_fetcher(SIMPLE))
    await parser.allows(f"{URL}/private/a")
    await parser.allows(f"{URL}/private/b")
    await parser.allows(f"{URL}/open")

    stats = parser.get_stats()

    assert stats["checks"] == 3
    assert stats["blocked"] == 2
    assert stats["allowed"] == 1
    assert stats["blocked_by_domain"] == {"example.com": 2}
    assert stats["fetched"] == 1
    assert stats["crawl_delays"] == {"example.com": 1.5}


async def test_reset_stats_keeps_parsed_rules():
    calls: list[str] = []
    parser = RobotsParser(fake_fetcher(SIMPLE, calls=calls))
    await parser.allows(f"{URL}/private/a")

    parser.reset_stats()
    await parser.allows(f"{URL}/private/b")

    assert parser.get_stats()["checks"] == 1
    assert len(calls) == 1  # повторной загрузки не было
