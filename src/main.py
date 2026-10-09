"""Демонстрация AsyncCrawler: параллельная загрузка против последовательной."""

from __future__ import annotations

import argparse
import asyncio
import logging

from src.crawler import AsyncCrawler
from src.models import CrawlStats
from src.utils import format_results_table, format_stats, setup_logging, timer

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


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Демо асинхронного краулера (День 1)")
    parser.add_argument(
        "--urls",
        nargs="+",
        default=DEFAULT_URLS,
        help="список URL для загрузки",
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


async def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    setup_logging(args.log_level)

    urls: list[str] = args.urls
    print(f"URL в очереди: {len(urls)}")

    with timer() as total:
        parallel = await run_parallel(urls, args.max_concurrent)

        if args.sequential:
            sequential = await run_sequential(urls)
            if parallel.elapsed > 0:
                speedup = sequential.elapsed / parallel.elapsed
                print(
                    f"\nУскорение: {sequential.elapsed:.2f} с -> "
                    f"{parallel.elapsed:.2f} с (x{speedup:.1f})"
                )

    print(f"Общее время работы: {total.seconds:.2f} с")

    if parallel.succeeded == 0:
        print("\nНи одна страница не загружена — проверьте доступ в сеть.")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
