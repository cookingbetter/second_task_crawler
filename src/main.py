"""Демо AsyncCrawler: загрузка (день 1), парсинг (день 2), обход (день 3),
соблюдение правил вежливости (день 4) и повторы при ошибках (день 5)."""

from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from src.crawler import DEFAULT_USER_AGENT, AsyncCrawler
from src.models import CrawlStats
from src.parser import HTMLParser
from src.utils import (
    format_circuit_stats,
    format_crawl_stats,
    format_error_report,
    format_page_summary,
    format_parse_stats,
    format_results_table,
    format_retry_stats,
    format_robots_stats,
    format_stats,
    print_progress,
    save_crawl_results,
    save_error_report,
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

# Сайт, специально сделанный для тренировки краулеров: много внутренних ссылок
# и настоящая глубина вложенности.
DEFAULT_CRAWL_URLS = ["https://quotes.toscrape.com/"]

# Для режима --robots нужны адреса с настоящими правилами: у python.org есть и
# запреты для всех агентов, и отдельные группы, а у quotes.toscrape.com
# robots.txt нет вовсе — видно оба случая сразу.
DEFAULT_ROBOTS_URLS = [
    "https://www.python.org/",
    "https://www.python.org/downloads/",
    "https://www.python.org/webstats/",
    "https://www.python.org/~guido/orlijn/index.html",
    "https://quotes.toscrape.com/page/2/",
]

# Набор для --retry: по одному адресу на каждый сценарий дня 5. 503 и 500
# повторяются, 429 просит подождать, 404 и 403 постоянны, /delay/5 не
# укладывается в таймаут режима, несуществующий домен даёт сетевую ошибку.
DEFAULT_RETRY_URLS = [
    "https://httpbin.org/status/503",
    "https://httpbin.org/status/500",
    "https://httpbin.org/status/429",
    "https://httpbin.org/status/404",
    "https://httpbin.org/status/403",
    "https://httpbin.org/delay/5",
    "https://this-domain-does-not-exist-12345.invalid",
    "https://example.com",
]

DEFAULT_OUTPUT_DIR = Path("data")

# Пресет --polite: умеренный темп, пауза между запросами и разброс, чтобы поток
# не выглядел машинным. Явно заданные флаги важнее пресета.
POLITE_PRESET = {"rps": 2.0, "min_delay": 0.5, "jitter": 0.2}
# Таймауты по умолчанию — те же, что у AsyncCrawler.
DEFAULT_TIMEOUTS = {
    "timeout_total": 15.0,
    "timeout_connect": 5.0,
    "timeout_read": 10.0,
}
# В режиме --retry таймауты короче: иначе медленный адрес просто дождался бы
# ответа, и повтор по таймауту показать было бы нечем. Потолок паузы тоже ниже:
# у 429 база 5 с, и с обычными 30 с демо ждало бы больше половины минуты.
RETRY_PRESET = {"timeout_total": 4.0, "timeout_read": 3.0, "retry_max_delay": 6.0}
# Значения по умолчанию для флагов, которые пресеты могут переопределить.
DEFAULT_RETRY_SETTINGS = {"retry_max_delay": 30.0}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Демо асинхронного краулера (дни 1-5)"
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
        help=(
            "в режиме --parse оставлять только ссылки с того же домена, "
            "в режиме --crawl — не выходить за домены стартовых URL"
        ),
    )
    parser.add_argument(
        "--crawl",
        action="store_true",
        help="режим дня 3: обход сайта по ссылкам с очередью и лимитами",
    )
    parser.add_argument(
        "--max-depth",
        type=int,
        default=2,
        help="максимальная глубина обхода в режиме --crawl (по умолчанию 2)",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=20,
        help="максимум загружаемых страниц в режиме --crawl (по умолчанию 20)",
    )
    parser.add_argument(
        "--max-per-domain",
        type=int,
        default=3,
        help="максимум одновременных запросов к одному домену (по умолчанию 3)",
    )
    parser.add_argument(
        "--include",
        nargs="+",
        metavar="REGEX",
        help="обходить только URL, подходящие хотя бы под один паттерн",
    )
    parser.add_argument(
        "--exclude",
        nargs="+",
        metavar="REGEX",
        help="не обходить URL, подходящие хотя бы под один паттерн",
    )
    parser.add_argument(
        "--output",
        help="файл для сохранения результатов обхода (по умолчанию data/crawl_<время>.json)",
    )
    parser.add_argument(
        "--no-save",
        action="store_true",
        help="не сохранять результаты обхода в файл",
    )
    parser.add_argument(
        "--progress-interval",
        type=float,
        default=0.5,
        help="как часто обновлять строку прогресса, с (0 — выключить)",
    )
    parser.add_argument(
        "--robots",
        action="store_true",
        help="режим дня 4: показать решения robots.txt для URL без обхода",
    )
    parser.add_argument(
        "--polite",
        action="store_true",
        help=(
            "режим дня 4: обход с robots.txt и лимитом скорости "
            f"({POLITE_PRESET['rps']} зап/с, пауза {POLITE_PRESET['min_delay']} с)"
        ),
    )
    parser.add_argument(
        "--rps",
        type=float,
        help="лимит скорости, запросов в секунду на домен (0 — без лимита)",
    )
    parser.add_argument(
        "--global-rate",
        action="store_true",
        help="считать лимит скорости общим на все домены, а не отдельным для каждого",
    )
    parser.add_argument(
        "--min-delay",
        type=float,
        help="минимальная пауза между запросами к одному домену, с",
    )
    parser.add_argument(
        "--jitter",
        type=float,
        help="случайная добавка к паузе, с (0 — без разброса)",
    )
    parser.add_argument(
        "--max-backoff",
        type=float,
        default=60.0,
        help="предел замедления домена после ошибок, с (по умолчанию 60)",
    )
    parser.add_argument(
        "--respect-robots",
        action="store_true",
        help="соблюдать robots.txt при обходе",
    )
    parser.add_argument(
        "--user-agent",
        default=DEFAULT_USER_AGENT,
        help="User-Agent запросов и ключ для правил robots.txt",
    )
    parser.add_argument(
        "--rotate-ua",
        nargs="+",
        metavar="UA",
        help="ротация User-Agent по кругу (правила robots.txt проверяются для каждого)",
    )
    parser.add_argument(
        "--retry",
        action="store_true",
        help="режим дня 5: загрузка адресов с разными ошибками и повторы",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=2,
        help="максимум повторов одного запроса (по умолчанию 2, 0 — без повторов)",
    )
    parser.add_argument(
        "--no-retry",
        action="store_true",
        help="выключить повторы совсем (то же, что --max-retries 0)",
    )
    parser.add_argument(
        "--retry-base-delay",
        type=float,
        default=0.5,
        help="базовая пауза перед первым повтором, с (по умолчанию 0.5)",
    )
    parser.add_argument(
        "--retry-backoff",
        type=float,
        default=2.0,
        help="во сколько раз растёт пауза с каждым повтором (по умолчанию 2)",
    )
    parser.add_argument(
        "--retry-max-delay",
        type=float,
        help=(
            "предел паузы перед повтором, с (по умолчанию "
            f"{DEFAULT_RETRY_SETTINGS['retry_max_delay']:.0f}, "
            f"в режиме --retry {RETRY_PRESET['retry_max_delay']:.0f})"
        ),
    )
    parser.add_argument(
        "--timeout-total",
        type=float,
        help=f"полный таймаут запроса, с (по умолчанию {DEFAULT_TIMEOUTS['timeout_total']:.0f})",
    )
    parser.add_argument(
        "--timeout-connect",
        type=float,
        help=f"таймаут соединения, с (по умолчанию {DEFAULT_TIMEOUTS['timeout_connect']:.0f})",
    )
    parser.add_argument(
        "--timeout-read",
        type=float,
        help=f"таймаут чтения ответа, с (по умолчанию {DEFAULT_TIMEOUTS['timeout_read']:.0f})",
    )
    parser.add_argument(
        "--timeout-growth",
        type=float,
        default=1.5,
        help="во сколько раз растут таймауты с каждой попыткой (1 — не растут)",
    )
    parser.add_argument(
        "--circuit-breaker",
        action="store_true",
        help="отключать домен, который отдаёт одни ошибки (по умолчанию выключено)",
    )
    parser.add_argument(
        "--circuit-threshold",
        type=int,
        default=5,
        help=(
            "сколько неудачных запросов к домену за минуту его отключают "
            "(по умолчанию 5; повторы одного URL считаются по отдельности)"
        ),
    )
    parser.add_argument(
        "--circuit-recovery",
        type=float,
        default=30.0,
        help="через сколько секунд пробовать отключённый домен снова (по умолчанию 30)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="уровень логирования",
    )

    args = parser.parse_args(argv)

    # Проверяем здесь, чтобы пользователь получил понятное сообщение argparse,
    # а не трейсбек ValueError из глубины краулера.
    for name, value, minimum in (
        ("--max-concurrent", args.max_concurrent, 1),
        ("--max-per-domain", args.max_per_domain, 1),
        ("--max-pages", args.max_pages, 1),
        ("--max-depth", args.max_depth, 0),
        ("--max-retries", args.max_retries, 0),
        ("--circuit-threshold", args.circuit_threshold, 1),
    ):
        if value < minimum:
            parser.error(f"{name} должен быть >= {minimum}, получено {value}")
    if args.progress_interval < 0:
        parser.error("--progress-interval не может быть отрицательным")
    if args.retry_backoff < 1:
        parser.error(f"--retry-backoff должен быть >= 1, получено {args.retry_backoff}")
    if args.timeout_growth < 1:
        parser.error(
            f"--timeout-growth должен быть >= 1, получено {args.timeout_growth}"
        )

    if args.polite:
        # Пресет сам по себе означает обход: отдельно включать --crawl не нужно.
        args.crawl = True
        args.respect_robots = True
        # Пресет только заполняет пропуски: явный флаг всегда сильнее.
        for name, value in POLITE_PRESET.items():
            if getattr(args, name) is None:
                setattr(args, name, value)

    if args.retry:
        # Короткие таймауты нужны самому режиму, но явный флаг сильнее пресета.
        for name, value in RETRY_PRESET.items():
            if getattr(args, name) is None:
                setattr(args, name, value)
    for name, value in {**DEFAULT_TIMEOUTS, **DEFAULT_RETRY_SETTINGS}.items():
        if getattr(args, name) is None:
            setattr(args, name, value)

    if args.no_retry:
        args.max_retries = 0

    for name, value in (
        ("--rps", args.rps),
        ("--min-delay", args.min_delay),
        ("--jitter", args.jitter),
        ("--max-backoff", args.max_backoff),
        ("--retry-base-delay", args.retry_base_delay),
        ("--retry-max-delay", args.retry_max_delay),
        ("--circuit-recovery", args.circuit_recovery),
        ("--timeout-total", args.timeout_total),
        ("--timeout-connect", args.timeout_connect),
        ("--timeout-read", args.timeout_read),
    ):
        if value is not None and value < 0:
            parser.error(f"{name} не может быть отрицательным, получено {value}")

    # Дальше по коду это просто числа: 0 означает «ограничения нет».
    args.rps = args.rps or 0.0
    args.min_delay = args.min_delay or 0.0
    args.jitter = args.jitter or 0.0

    return args


def _crawler_kwargs(args: argparse.Namespace) -> dict[str, Any]:
    """Настройки, общие для всех режимов: таймауты, повторы, circuit breaker."""
    return {
        "timeout_total": args.timeout_total,
        "timeout_connect": args.timeout_connect,
        "timeout_read": args.timeout_read,
        "timeout_growth": args.timeout_growth,
        "max_retries": args.max_retries,
        "retry_base_delay": args.retry_base_delay,
        "retry_backoff_factor": args.retry_backoff,
        "retry_max_delay": args.retry_max_delay,
        "circuit_breaker": args.circuit_breaker,
        "circuit_threshold": args.circuit_threshold,
        "circuit_recovery": args.circuit_recovery,
    }


async def run_parallel(urls: list[str], args: argparse.Namespace) -> CrawlStats:
    print(f"\n=== Параллельная загрузка (max_concurrent={args.max_concurrent}) ===")
    async with AsyncCrawler(
        max_concurrent=args.max_concurrent, **_crawler_kwargs(args)
    ) as crawler:
        pages = await crawler.fetch_urls(urls)
        print(format_results_table(crawler.results))
        print(format_stats(crawler.stats, "Итог параллельно"))
        print(f"Загружено {len(pages)} страниц")
        return crawler.stats


async def run_sequential(urls: list[str], args: argparse.Namespace) -> CrawlStats:
    print("\n=== Последовательная загрузка (для сравнения) ===")
    async with AsyncCrawler(max_concurrent=1, **_crawler_kwargs(args)) as crawler:
        await crawler.fetch_urls_sequential(urls)
        print(format_results_table(crawler.results))
        print(format_stats(crawler.stats, "Итог последовательно"))
        return crawler.stats


async def run_retry(urls: list[str], args: argparse.Namespace) -> CrawlStats:
    """Показывает повторы и классификацию ошибок на адресах, которые ломаются."""
    print(f"\n=== Повторы и ошибки (max_concurrent={args.max_concurrent}) ===")
    print(
        f"Повторы: до {args.max_retries}"
        f", backoff x{args.retry_backoff:.2f} от {args.retry_base_delay:.2f} с"
        f" (потолок {args.retry_max_delay:.2f} с)"
        f"\nТаймауты: всего {args.timeout_total:.1f} с"
        f", соединение {args.timeout_connect:.1f} с"
        f", чтение {args.timeout_read:.1f} с"
        f", рост x{args.timeout_growth:.2f} на попытку"
        f"\nCircuit breaker: "
        + (
            f"порог {args.circuit_threshold}, восстановление {args.circuit_recovery:.0f} с"
            if args.circuit_breaker
            else "выключен"
        )
    )

    async with AsyncCrawler(
        max_concurrent=args.max_concurrent, **_crawler_kwargs(args)
    ) as crawler:
        await crawler.fetch_urls(urls)

        print()
        print(format_results_table(crawler.results))
        print()
        print(format_retry_stats(crawler.retry_strategy.get_stats(), "Повторы"))
        if crawler.circuit is not None:
            print(format_circuit_stats(crawler.circuit.get_stats(), "Circuit breaker"))

        report = crawler.get_error_report()
        print()
        print(format_error_report(report))
        print(format_stats(crawler.stats, "Итог загрузки"))

        if not args.no_save:
            path = Path(args.output) if args.output else _default_error_path()
            saved = save_error_report(
                {
                    **report,
                    "retry": crawler.retry_strategy.get_stats(),
                    "circuit": (
                        crawler.circuit.get_stats()
                        if crawler.circuit is not None
                        else {"enabled": False}
                    ),
                    "failures": [
                        {
                            "url": result.url,
                            "status": result.status,
                            "error": result.error,
                            "error_type": result.error_type,
                            "error_class": result.error_class,
                            "attempts": result.attempts,
                            "retry_waited": result.retry_waited,
                        }
                        for result in crawler.results
                        if not result.ok
                    ],
                },
                path,
            )
            print(f"  Отчёт об ошибках сохранён: {saved}")

        return crawler.stats


async def run_parse(urls: list[str], args: argparse.Namespace) -> CrawlStats:
    print(f"\n=== Загрузка с парсингом (max_concurrent={args.max_concurrent}) ===")
    html_parser = HTMLParser(same_domain_only=args.same_domain)
    async with AsyncCrawler(
        max_concurrent=args.max_concurrent,
        parser=html_parser,
        **_crawler_kwargs(args),
    ) as crawler:
        pages = await crawler.fetch_and_parse_many(urls)

        for page in pages:
            print()
            print(format_page_summary(page))

        print()
        print(format_parse_stats(pages))
        print(format_stats(crawler.stats, "Итог загрузки"))
        return crawler.stats


async def run_crawl(urls: list[str], args: argparse.Namespace) -> CrawlStats:
    print(
        f"\n=== Обход сайта (глубина {args.max_depth}, страниц до {args.max_pages}, "
        f"конкурентность {args.max_concurrent}/{args.max_per_domain} на домен) ==="
    )
    print(
        f"Вежливость: лимит {args.rps or 'нет'} зап/с"
        f" ({'общий' if args.global_rate else 'на домен'})"
        f", пауза {args.min_delay:.2f} с, разброс {args.jitter:.2f} с"
        f", robots.txt {'учитывается' if args.respect_robots else 'игнорируется'}"
    )
    # Парсер без same_domain_only: ссылки на внешние сайты остаются в данных
    # страницы, а до очереди их не пускает фильтр обхода.
    show_progress = args.progress_interval > 0
    async with AsyncCrawler(
        max_concurrent=args.max_concurrent,
        max_depth=args.max_depth,
        max_per_domain=args.max_per_domain,
        requests_per_second=args.rps or None,
        per_domain_rate=not args.global_rate,
        min_delay=args.min_delay,
        jitter=args.jitter,
        max_backoff=args.max_backoff,
        respect_robots=args.respect_robots,
        user_agent=args.user_agent,
        user_agents=args.rotate_ua,
        parser=HTMLParser(),
        **_crawler_kwargs(args),
    ) as crawler:
        pages = await crawler.crawl(
            start_urls=urls,
            max_pages=args.max_pages,
            same_domain_only=args.same_domain,
            include_patterns=args.include,
            exclude_patterns=args.exclude,
            on_progress=print_progress if show_progress else None,
            progress_interval=args.progress_interval,
        )
        if show_progress:
            print()  # закрыть строку прогресса, которую печатали через \r

        crawl_stats = crawler.get_crawl_stats()
        print(format_depth_table(pages))
        print()
        print(format_crawl_stats(crawl_stats))

        if crawler.failed_urls:
            print(f"  Неудачные URL ({len(crawler.failed_urls)}):")
            for url, error in list(crawler.failed_urls.items())[:5]:
                print(f"    {url} -> {error}")

        if crawler.blocked_urls:
            print(f"  Запрещено robots.txt ({len(crawler.blocked_urls)}):")
            for url, rule in list(crawler.blocked_urls.items())[:5]:
                print(f"    {url} -> {rule}")

        if not args.no_save:
            path = Path(args.output) if args.output else _default_output_path()
            saved = save_crawl_results(pages, crawl_stats, path)
            print(f"  Данные сохранены: {saved}")

        return crawler.stats


async def run_robots(urls: list[str], args: argparse.Namespace) -> CrawlStats:
    """Показывает, что robots.txt разрешает, а что нет — без обхода сайта."""
    print(f"\n=== Проверка robots.txt (агент: {args.user_agent}) ===")

    async with AsyncCrawler(
        max_concurrent=args.max_concurrent,
        requests_per_second=args.rps or None,
        min_delay=args.min_delay,
        jitter=args.jitter,
        respect_robots=True,
        user_agent=args.user_agent,
        **_crawler_kwargs(args),
    ) as crawler:
        domains: dict[str, dict[str, Any]] = {}
        rows: list[dict[str, Any]] = []

        for url in urls:
            summary = await crawler.robots.fetch_robots(url)
            domains.setdefault(summary["domain"], summary)
            allowed, rule = await crawler.robots.allows(url)
            rows.append({"url": url, "allowed": allowed, "rule": rule})

        print("Домены:")
        for domain, summary in domains.items():
            if summary["available"]:
                print(
                    f"  {domain}: robots.txt {summary['status']}"
                    f", групп {len(summary['groups'])}"
                    f", подошла {summary['group'] or '—'}"
                    f", правил {summary['rules']}"
                    f", Crawl-delay {summary['crawl_delay']:.2f} с"
                )
            else:
                reason = (
                    f"нет файла ({summary['status']})"
                    if summary["source"] == "missing"
                    else "не удалось загрузить"
                )
                print(f"  {domain}: {reason} — разрешено всё")

        print()
        print(format_robots_table(rows))
        print()
        print(format_robots_stats(crawler.robots.get_stats(), "robots.txt"))

        reachable = sum(1 for item in domains.values() if item["source"] != "error")
        return CrawlStats(
            total=len(domains), succeeded=reachable, failed=len(domains) - reachable
        )


def format_robots_table(rows: list[dict[str, Any]]) -> str:
    """Таблица решений: какой URL разрешён и каким правилом это задано."""
    header = f"{'URL':<55} {'Решение':<12} Правило"
    lines = [header, "-" * len(header)]

    for row in rows:
        url = row["url"]
        if len(url) > 55:
            url = url[:52] + "..."
        verdict = "разрешён" if row["allowed"] else "ЗАПРЕЩЁН"
        lines.append(f"{url:<55} {verdict:<12} {row['rule'] or '—'}")

    return "\n".join(lines)


def format_depth_table(pages: list[dict[str, Any]]) -> str:
    """Сводка по уровням обхода: сколько страниц и ссылок нашлось на каждом."""
    header = f"{'Глубина':<9}{'Страниц':>9}{'Ошибок':>9}{'Ссылок':>9}"
    lines = [header, "-" * len(header)]

    by_depth: dict[int, list[dict[str, Any]]] = {}
    for page in pages:
        by_depth.setdefault(page.get("depth", 0), []).append(page)

    for depth in sorted(by_depth):
        group = by_depth[depth]
        ok = [page for page in group if page.get("ok")]
        links = sum(page.get("links_count", 0) for page in ok)
        lines.append(f"{depth:<9}{len(ok):>9}{len(group) - len(ok):>9}{links:>9}")

    return "\n".join(lines)


def _warn_unused_politeness(args: argparse.Namespace) -> None:
    """Предупреждает, что флаги дня 4 в режимах дней 1-2 ничего не делают."""
    if args.rps or args.min_delay or args.jitter or args.respect_robots or args.rotate_ua:
        logger.warning(
            "Флаги вежливости (--rps, --min-delay, --jitter, --respect-robots, "
            "--rotate-ua) действуют в режимах --crawl и --robots"
        )


def _default_output_path() -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return DEFAULT_OUTPUT_DIR / f"crawl_{stamp}.json"


def _default_error_path() -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return DEFAULT_OUTPUT_DIR / f"errors_{stamp}.json"


async def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    setup_logging(args.log_level)

    if args.robots:
        default_urls = DEFAULT_ROBOTS_URLS
    elif args.crawl:
        default_urls = DEFAULT_CRAWL_URLS
    elif args.retry:
        default_urls = DEFAULT_RETRY_URLS
    elif args.parse:
        default_urls = DEFAULT_PARSE_URLS
    else:
        default_urls = DEFAULT_URLS
    urls: list[str] = args.urls or default_urls

    if args.robots:
        print(f"URL для проверки: {len(urls)}")
    elif args.crawl:
        print(f"Стартовых URL: {len(urls)}")
    else:
        print(f"URL в очереди: {len(urls)}")

    with timer() as total:
        if args.robots:
            if args.crawl:
                logger.warning("--robots и --crawl вместе: выполняется только --robots")
            stats = await run_robots(urls, args)
        elif args.crawl:
            if args.parse:
                logger.warning("--parse в режиме --crawl избыточен: обход всегда парсит")
            if args.sequential:
                logger.warning("--sequential относится к режиму дня 1 и в --crawl не действует")
            if args.retry:
                logger.warning(
                    "--retry — отдельный режим демо; при обходе повторы работают "
                    "и без него, выполняется --crawl"
                )
            stats = await run_crawl(urls, args)
        elif args.retry:
            _warn_unused_politeness(args)
            stats = await run_retry(urls, args)
        elif args.parse:
            _warn_unused_politeness(args)
            if args.sequential:
                logger.warning(
                    "--sequential относится к режиму дня 1 и в --parse не действует"
                )
            stats = await run_parse(urls, args)
        else:
            _warn_unused_politeness(args)
            stats = await run_parallel(urls, args)

            if args.sequential:
                sequential = await run_sequential(urls, args)
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
