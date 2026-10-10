"""Демо AsyncCrawler: параллельная загрузка (день 1) и парсинг страниц (день 2)."""

from __future__ import annotations

import argparse
import asyncio
import logging

from src.crawler import AsyncCrawler
from src.models import CrawlStats
from src.parser import HTMLParser
from src.utils import (
    format_page_summary,
    format_parse_stats,
    format_results_table,
    format_stats,
    setup_logging,
    timer,
)

logger = logging.getLogger(__name__)

DEFAULT_URLS = [
    "https://example.com",
    "https://httpbin.org/get",
    "https://httpbin.org/delay/1",
    "https://httpbin.org/delay/2",
    "https://httpbin.org/delay/3",
    "https://httpbin.org/html",
    "https://httpbin.org/status/404",
    "https://this-domain-does-not-exist-12345.invalid",
]

# Для режима парсинга нужны именно HTML-страницы, а не JSON-ответы.
DEFAULT_PARSE_URLS = [
    "https://example.com",
    "https://httpbin.org/html",
    "https://httpbin.org/links/10/0",
    "https://www.python.org/",
    "https://httpbin.org/status/404",
    "https://this-domain-does-not-exist-12345.invalid",
]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Демо асинхронного краулера (дни 1-2)"
    )
    parser.add_argument(
        "--urls",
        nargs="+",
        help="список URL для загрузки (по умолчанию — встроенный набор)",
    )
    parser.add_argument(
        "--max-concurrent",
        type=int,
        default=5,
        help="максимум одновременных запросов (по умолчанию 5)",
    )
    parser.add_argument(
        "--sequential",
        action="store_true",
        help="дополнительно прогнать последовательную загрузку и сравнить время",
    )
    parser.add_argument(
        "--parse",
        action="store_true",
        help="режим дня 2: загрузить страницы и разобрать их HTML",
    )
    parser.add_argument(
        "--same-domain",
        action="store_true",
        help="в режиме --parse оставлять только ссылки с того же домена",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="уровень логирования",
    )
    return parser.parse_args(argv)


async def run_parallel(urls: list[str], max_concurrent: int) -> CrawlStats:
    print(f"\n=== Параллельная загрузка (max_concurrent={max_concurrent}) ===")
    async with AsyncCrawler(max_concurrent=max_concurrent) as crawler:
        pages = await crawler.fetch_urls(urls)
        print(format_results_table(crawler.results))
        print(format_stats(crawler.stats, "Итог параллельно"))
        print(f"Загружено {len(pages)} страниц")
        return crawler.stats


async def run_sequential(urls: list[str]) -> CrawlStats:
    print("\n=== Последовательная загрузка (для сравнения) ===")
    async with AsyncCrawler(max_concurrent=1) as crawler:
        await crawler.fetch_urls_sequential(urls)
        print(format_results_table(crawler.results))
        print(format_stats(crawler.stats, "Итог последовательно"))
        return crawler.stats


async def run_parse(
    urls: list[str], max_concurrent: int, same_domain: bool
) -> CrawlStats:
    print(f"\n=== Загрузка с парсингом (max_concurrent={max_concurrent}) ===")
    html_parser = HTMLParser(same_domain_only=same_domain)
    async with AsyncCrawler(
        max_concurrent=max_concurrent, parser=html_parser
    ) as crawler:
        pages = await crawler.fetch_and_parse_many(urls)

        for page in pages:
            print()
            print(format_page_summary(page))

        print()
        print(format_parse_stats(pages))
        print(format_stats(crawler.stats, "Итог загрузки"))
        return crawler.stats


async def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    setup_logging(args.log_level)

    default_urls = DEFAULT_PARSE_URLS if args.parse else DEFAULT_URLS
    urls: list[str] = args.urls or default_urls
    print(f"URL в очереди: {len(urls)}")

    with timer() as total:
        if args.parse:
            if args.sequential:
                logger.warning(
                    "--sequential относится к режиму дня 1 и в --parse не действует"
                )
            stats = await run_parse(urls, args.max_concurrent, args.same_domain)
        else:
            stats = await run_parallel(urls, args.max_concurrent)

            if args.sequential:
                sequential = await run_sequential(urls)
                if stats.elapsed > 0:
                    speedup = sequential.elapsed / stats.elapsed
                    print(
                        f"\nУскорение: {sequential.elapsed:.2f} с -> "
                        f"{stats.elapsed:.2f} с (x{speedup:.1f})"
                    )

    print(f"Общее время работы: {total.seconds:.2f} с")

    if stats.succeeded == 0:
        print("\nНи одна страница не загружена — проверьте доступ в сеть.")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
