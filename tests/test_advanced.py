"""Тесты финальной интеграции и слияния CLI с конфигурацией (День 7)."""

from __future__ import annotations

import json

import pytest

from src.advanced import AdvancedCrawler
from src.config import CrawlerConfig
from src.main import config_from_args, default_mode_argv, parse_args
from src.sitemap import SITEMAP_AUTO
from src.storage import JSONStorage
from tests.conftest import SITEMAP_ORDER, SITEMAP_PATH


def config(url, **extra) -> CrawlerConfig:
    """Конфигурация на локальный сервер: быстрая и без вежливых пауз."""
    data = {
        "start_urls": [url("/site/")],
        "max_pages": 3,
        "max_depth": 1,
        "progress_interval": 0,
        **extra,
    }
    return CrawlerConfig.from_dict(data, source="тест")


# --- обход по конфигурации --------------------------------------------------


async def test_crawl_by_config(url):
    crawler = AdvancedCrawler(config(url))
    try:
        pages = await crawler.crawl()
    finally:
        await crawler.close()

    assert 1 <= len(pages) <= 3
    assert all(page["url"].startswith("http") for page in pages)


async def test_get_stats_uses_names_from_task(url):
    async with AdvancedCrawler(config(url)) as crawler:
        await crawler.crawl()
        stats = crawler.get_stats()

    assert stats["total_pages"] >= 1
    assert stats["successful"] + stats["failed"] == stats["total_pages"]
    assert stats["elapsed"] > 0


async def test_stats_remember_settings(url):
    """По отчёту должно быть понятно, какими настройками он получен."""
    async with AdvancedCrawler(config(url, max_pages=2)) as crawler:
        await crawler.crawl()

        assert crawler.stats.settings == crawler.config.to_dict()
        assert crawler.stats.settings["max_pages"] == 2


async def test_explicit_urls_win_over_config(url):
    async with AdvancedCrawler(config(url, max_pages=1, max_depth=0)) as crawler:
        pages = await crawler.crawl([url("/site/a")])

    assert [page["url"] for page in pages] == [url("/site/a")]


async def test_crawl_without_urls_is_error(url):
    cfg = config(url)
    cfg.start_urls = []  # после проверки конфигурации — как будто карта пуста
    async with AdvancedCrawler(cfg) as crawler:
        with pytest.raises(ValueError, match="нет стартовых URL"):
            await crawler.crawl()


# --- источник адресов -------------------------------------------------------


async def test_sitemap_is_preferred_source(url):
    cfg = config(url, sitemaps=[url(SITEMAP_PATH)], max_pages=2)
    async with AdvancedCrawler(cfg) as crawler:
        urls = await crawler.resolve_start_urls()

    # Адреса идут по <priority>, а не по порядку в конфигурации.
    assert urls == [url(SITEMAP_ORDER[0]), url(SITEMAP_ORDER[1])]


async def test_sitemap_auto_searches_by_start_urls(url, robots_txt):
    robots_txt(f"User-agent: *\nSitemap: {url(SITEMAP_PATH)}\n")
    cfg = config(url, sitemaps=[SITEMAP_AUTO], max_pages=1)

    async with AdvancedCrawler(cfg) as crawler:
        urls = await crawler.resolve_start_urls()

    assert urls == [url(SITEMAP_ORDER[0])]


async def test_empty_sitemap_falls_back_to_start_urls(url, robots_txt):
    """Карта не нашлась — это не повод заканчивать работу."""
    cfg = config(url, sitemaps=[url("/nowhere.xml")])

    async with AdvancedCrawler(cfg) as crawler:
        urls = await crawler.resolve_start_urls()

    assert urls == cfg.start_urls


async def test_without_sitemap_nothing_is_loaded(url):
    """Карты не просили — краулер за ней и не ходит."""
    async with AdvancedCrawler(config(url)) as crawler:
        assert await crawler.resolve_start_urls() == [url("/site/")]
        assert crawler.crawler.sitemap.get_stats()["files"] == 0


# --- хранилище --------------------------------------------------------------


async def test_storage_flushed_before_stats(tmp_path, url):
    """Статистика не должна показывать «записей 0», пока буфер не дослан."""
    storage = JSONStorage(tmp_path / "pages.json", batch_size=1000)
    cfg = config(url, max_pages=2)

    async with AdvancedCrawler(cfg, storage=storage) as crawler:
        pages = await crawler.crawl()
        storage_stats = crawler.stats.crawl["storage"]

    assert storage_stats["saved"] == len(pages)
    assert storage_stats["buffered"] == 0
    assert json.loads((tmp_path / "pages.json").read_text("utf-8"))


async def test_close_closes_storage(tmp_path, url):
    storage = JSONStorage(tmp_path / "pages.json")
    crawler = AdvancedCrawler(config(url, max_pages=1), storage=storage)

    await crawler.crawl()
    await crawler.close()

    assert storage.closed is True


async def test_storage_built_from_config(tmp_path, url):
    cfg = config(url, storage=["jsonl"], storage_path=str(tmp_path / "run"))

    async with AdvancedCrawler(cfg) as crawler:
        assert isinstance(crawler.storage, JSONStorage)
        assert str(crawler.storage.target).endswith("run.jsonl")


# --- отчёты -----------------------------------------------------------------


async def test_export_to_paths_from_config(tmp_path, url):
    cfg = config(
        url,
        max_pages=1,
        report_html=str(tmp_path / "report.html"),
        report_json=str(tmp_path / "stats.json"),
    )

    async with AdvancedCrawler(cfg) as crawler:
        await crawler.crawl()
        written = crawler.export_reports()

    assert sorted(path.name for path in written) == ["report.html", "stats.json"]
    assert "<!DOCTYPE html>" in (tmp_path / "report.html").read_text("utf-8")
    assert json.loads((tmp_path / "stats.json").read_text("utf-8"))["total_pages"] == 1


async def test_export_argument_wins_over_config(tmp_path, url):
    cfg = config(url, max_pages=1, report_json=str(tmp_path / "from_config.json"))

    async with AdvancedCrawler(cfg) as crawler:
        await crawler.crawl()
        target = crawler.export_to_json(tmp_path / "explicit.json")

    assert target.name == "explicit.json"
    assert not (tmp_path / "from_config.json").exists()


async def test_export_without_target_is_error(url):
    async with AdvancedCrawler(config(url, max_pages=1)) as crawler:
        with pytest.raises(ValueError, match="не указан файл отчёта"):
            crawler.export_to_html_report()


async def test_export_reports_writes_only_requested(tmp_path, url):
    cfg = config(url, max_pages=1, report_json=str(tmp_path / "stats.json"))

    async with AdvancedCrawler(cfg) as crawler:
        await crawler.crawl()
        written = crawler.export_reports()

    assert [path.name for path in written] == ["stats.json"]


# --- from_config ------------------------------------------------------------


def test_from_config_reads_file_and_sets_logging(tmp_path, monkeypatch):
    target = tmp_path / "c.yaml"
    target.write_text(
        "crawler:\n  urls: [https://example.com]\n  max_pages: 4\n"
        "logging:\n  log_level: WARNING\n",
        encoding="utf-8",
    )
    calls: list[tuple] = []
    monkeypatch.setattr(
        "src.advanced.setup_logging", lambda *args, **kwargs: calls.append((args, kwargs))
    )

    crawler = AdvancedCrawler.from_config(target)

    assert crawler.config.max_pages == 4
    assert calls and calls[0][0][0] == "WARNING"


def test_plain_constructor_leaves_logging_alone(url, monkeypatch):
    """Библиотека не настраивает корневой логгер — это дело приложения."""
    calls: list[tuple] = []
    monkeypatch.setattr(
        "src.advanced.setup_logging", lambda *args, **kwargs: calls.append(args)
    )

    AdvancedCrawler(config(url))

    assert calls == []


def test_from_config_can_skip_logging(tmp_path, monkeypatch):
    target = tmp_path / "c.json"
    target.write_text(json.dumps({"urls": ["https://example.com"]}), encoding="utf-8")
    calls: list[tuple] = []
    monkeypatch.setattr(
        "src.advanced.setup_logging", lambda *args, **kwargs: calls.append(args)
    )

    AdvancedCrawler.from_config(target, configure_logging=False)

    assert calls == []


def test_title_mentions_site(url):
    crawler = AdvancedCrawler(config(url))

    assert url("/site/") in crawler.stats.title


# --- слияние CLI с конфигурацией --------------------------------------------


def write_config(tmp_path, **settings) -> str:
    target = tmp_path / "c.json"
    data = {"start_urls": ["https://example.com"], **settings}
    target.write_text(json.dumps(data), encoding="utf-8")
    return str(target)


def test_config_fills_untouched_flags(tmp_path):
    path = write_config(tmp_path, max_pages=11, max_depth=4, storage=["csv"])

    args = parse_args(["--config", path])

    assert (args.max_pages, args.max_depth) == (11, 4)
    assert args.storage == ["csv"]
    assert args.urls == ["https://example.com"]


def test_explicit_flag_wins_over_config(tmp_path):
    path = write_config(tmp_path, max_pages=11)

    args = parse_args(["--config", path, "--max-pages", "7", "--urls", "https://b.example"])

    assert args.max_pages == 7
    assert args.urls == ["https://b.example"]


def test_config_keys_map_to_flag_names(tmp_path):
    path = write_config(
        tmp_path,
        sitemaps=["https://example.com/sitemap.xml"],
        rate_limit=3,
        report_html="data/r.html",
        report_json="data/s.json",
    )

    args = parse_args(["--config", path])

    assert args.sitemap == ["https://example.com/sitemap.xml"]
    assert args.rps == 3.0
    assert (args.report, args.stats_json) == ("data/r.html", "data/s.json")


def test_config_alone_means_advanced(tmp_path):
    args = parse_args(["--config", write_config(tmp_path)])

    assert args.advanced is True


@pytest.mark.parametrize("flag", ["--crawl", "--parse", "--store", "--robots", "--retry"])
def test_config_with_other_mode_is_just_settings(tmp_path, flag: str):
    args = parse_args(["--config", write_config(tmp_path, max_pages=11), flag])

    assert args.advanced is False
    assert args.max_pages == 11  # настройки всё равно применились


def test_broken_config_exits_with_message(tmp_path, capsys):
    target = tmp_path / "c.json"
    target.write_text(json.dumps({"max_page": 5}), encoding="utf-8")

    with pytest.raises(SystemExit):
        parse_args(["--config", str(target)])

    assert "--config" in capsys.readouterr().err


def test_missing_config_exits_with_message(tmp_path, capsys):
    with pytest.raises(SystemExit):
        parse_args(["--config", str(tmp_path / "нет.yaml")])

    assert "не найден" in capsys.readouterr().err


@pytest.mark.parametrize("flag", ["--rps", "--rate-limit"])
def test_rate_limit_is_alias_of_rps(flag: str):
    assert parse_args([flag, "2.5"]).rps == 2.5


def test_log_flags_parsed():
    args = parse_args(
        ["--log-file", "logs/run.log", "--log-max-bytes", "1000", "--log-backups", "1"]
    )

    assert (args.log_file, args.log_max_bytes, args.log_backups) == ("logs/run.log", 1000, 1)


@pytest.mark.parametrize(
    "argv",
    [
        ["--log-max-bytes", "-1"],
        ["--log-backups", "-1"],
        ["--benchmark-pages", "0"],
    ],
)
def test_bad_day7_flags_rejected(argv: list[str]):
    with pytest.raises(SystemExit):
        parse_args(argv)


@pytest.mark.parametrize("flag", ["--output", "--report", "--stats-json", "--log-file"])
def test_directory_instead_of_file_rejected(tmp_path, flag: str, capsys):
    with pytest.raises(SystemExit):
        parse_args([flag, str(tmp_path)])

    assert "каталог" in capsys.readouterr().err


# --- корневой crawler.py ----------------------------------------------------


def test_task_command_crawls_and_saves():
    """Команда из задания должна обходить сайт, а не загружать один адрес."""
    argv = default_mode_argv(
        ["--urls", "https://example.com", "--max-pages", "100", "--output", "r.json"]
    )
    args = parse_args(argv)

    assert args.advanced is True
    assert args.max_pages == 100
    assert args.output == "r.json"


def test_default_mode_added_without_flags():
    assert default_mode_argv([]) == ["--advanced"]


@pytest.mark.parametrize(
    "flag",
    ["--crawl", "--parse", "--store", "--robots", "--retry", "--benchmark",
     "--sequential", "--polite", "--advanced"],
)
def test_chosen_mode_left_alone(flag: str):
    assert default_mode_argv([flag, "--max-pages", "3"]) == [flag, "--max-pages", "3"]


def test_config_counts_as_chosen_mode(tmp_path):
    """У --config свой разбор режима: сам включит --advanced, если нужно."""
    path = write_config(tmp_path)

    assert default_mode_argv(["--config", path]) == ["--config", path]


def test_mode_with_equals_sign_recognized(tmp_path):
    path = write_config(tmp_path)

    assert default_mode_argv([f"--config={path}"]) == [f"--config={path}"]


# --- config_from_args -------------------------------------------------------


def test_config_from_args_matches_flags(tmp_path):
    args = parse_args(
        [
            "--advanced",
            "--max-pages", "5",
            "--rate-limit", "2",
            "--respect-robots",
            "--storage", "jsonl",
            "--storage-path", str(tmp_path / "run"),
            "--report", str(tmp_path / "r.html"),
            "--no-save",
        ]
    )

    cfg = config_from_args(args, ["https://example.com"])

    assert cfg.start_urls == ["https://example.com"]
    assert cfg.max_pages == 5
    assert cfg.requests_per_second == 2.0
    assert cfg.respect_robots is True
    assert cfg.storage == ["jsonl"]
    assert cfg.report_html == str(tmp_path / "r.html")
    assert cfg.output is None  # --no-save: файла с результатами не будет


def test_config_from_args_keeps_default_output():
    args = parse_args(["--advanced"])

    assert config_from_args(args, ["https://example.com"]).output.endswith(".json")


def test_config_from_args_validates():
    args = parse_args(["--advanced"])
    args.max_pages = 0  # argparse такое не пропустил бы, конфигурация тоже не должна

    with pytest.raises(ValueError, match="max_pages"):
        config_from_args(args, ["https://example.com"])
