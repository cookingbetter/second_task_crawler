"""Тесты расширенной статистики и отчётов (День 7)."""

from __future__ import annotations

import json
from datetime import datetime

import pytest

from src.models import FetchResult
from src.stats import NO_STATUS, PAGES_IN_REPORT, CrawlerStats


def result(
    url: str,
    *,
    status: int | None = 200,
    error: str | None = None,
    error_type: str | None = None,
    elapsed: float = 0.1,
    final_url: str = "",
) -> FetchResult:
    """Готовый результат загрузки: в тестах нужны только эти поля."""
    return FetchResult(
        url=url,
        status=status,
        text="<html></html>" if error is None else None,
        error=error,
        error_type=error_type,
        elapsed=elapsed,
        final_url=final_url,
    )


def page(url: str, *, depth: int = 0, content_type: str = "text/html") -> dict:
    return {
        "url": url,
        "depth": depth,
        "content_type": content_type,
        "title": f"Страница {url}",
        "links": [],
        "status": 200,
    }


RESULTS = [
    result("https://a.example/1"),
    result("https://a.example/2"),
    result("https://a.example/3", status=404, error="404", error_type="http_error"),
    result("https://b.example/1"),
    result("https://b.example/2", status=None, error="таймаут", error_type="timeout"),
]
PAGES = [
    page("https://a.example/1"),
    page("https://a.example/2", depth=1),
    page("https://b.example/1", depth=1, content_type="text/html; charset=utf-8"),
]


# --- сбор сводки ------------------------------------------------------------


def test_totals_count_successes_and_failures():
    stats = CrawlerStats.from_results(RESULTS, PAGES, elapsed=2.0)

    assert (stats.total_pages, stats.successful, stats.failed) == (5, 3, 2)
    assert stats.success_rate == pytest.approx(60.0)
    assert stats.pages_per_sec == pytest.approx(2.5)
    assert stats.avg_page_time == pytest.approx(0.4)


def test_empty_run_has_no_division_by_zero():
    stats = CrawlerStats.from_results([])

    assert (stats.total_pages, stats.pages_per_sec, stats.success_rate) == (0, 0.0, 0.0)
    assert stats.avg_page_time == 0.0


def test_status_distribution_includes_no_answer():
    stats = CrawlerStats.from_results(RESULTS)

    assert stats.by_status == {"200": 3, "404": 1, NO_STATUS: 1}


def test_error_types_counted():
    stats = CrawlerStats.from_results(RESULTS)

    assert stats.by_error_type == {"http_error": 1, "timeout": 1}


def test_top_domains_sorted_by_pages():
    stats = CrawlerStats.from_results(RESULTS)

    top = stats.top_domains()
    assert [row["domain"] for row in top] == ["a.example", "b.example"]
    assert top[0]["pages"] == 3
    assert top[0]["failed"] == 1
    assert top[0]["avg_time"] == pytest.approx(0.1)


def test_top_domains_limited():
    results = [result(f"https://d{n}.example/") for n in range(5)]

    assert len(CrawlerStats.from_results(results).top_domains(limit=2)) == 2


def test_domain_taken_after_redirect():
    """Страницу отдал конечный адрес — его домен и попадает в отчёт."""
    stats = CrawlerStats.from_results(
        [result("https://old.example/a", final_url="https://new.example/a")]
    )

    assert [row["domain"] for row in stats.domains] == ["new.example"]


def test_depth_and_content_type_from_pages():
    stats = CrawlerStats.from_results(RESULTS, PAGES)

    assert stats.by_depth == {0: 1, 1: 2}
    # charset отброшен: это тот же тип содержимого.
    assert stats.by_content_type == {"text/html": 3}


def test_elapsed_falls_back_to_crawl_stats():
    stats = CrawlerStats.from_results(RESULTS, PAGES, {"elapsed": 4.0})

    assert stats.elapsed == pytest.approx(4.0)
    assert stats.pages_per_sec == pytest.approx(1.25)


def test_get_stats_uses_names_from_task():
    stats = CrawlerStats.from_results(RESULTS, PAGES, elapsed=2.0)

    summary = stats.get_stats()
    assert summary["total_pages"] == 5
    assert summary["successful"] == 3
    assert summary["failed"] == 2
    assert summary["domains_total"] == 2


def test_recount_gives_same_result():
    """Агрегатор без состояния: второй подсчёт того же прогона совпадает."""
    first = CrawlerStats.from_results(RESULTS, PAGES, elapsed=2.0).get_stats()
    second = CrawlerStats.from_results(RESULTS, PAGES, elapsed=2.0).get_stats()

    first.pop("finished_at")
    second.pop("finished_at")
    assert first == second


# --- экспорт ----------------------------------------------------------------


def test_export_to_json_writes_readable_file(tmp_path):
    stats = CrawlerStats.from_results(
        RESULTS, PAGES, elapsed=2.0, started_at=datetime(2026, 1, 2, 3, 4, 5)
    )

    target = stats.export_to_json(tmp_path / "nested" / "stats.json")

    data = json.loads(target.read_text(encoding="utf-8"))
    assert data["total_pages"] == 5
    assert data["started_at"].startswith("2026-01-02T03:04:05")
    assert "pages" not in data  # сами страницы пишут хранилища, а не отчёт


def test_export_to_json_keeps_cyrillic_readable(tmp_path):
    stats = CrawlerStats.from_results(RESULTS, title="Отчёт о прогоне")

    text = stats.export_to_json(tmp_path / "stats.json").read_text(encoding="utf-8")

    assert "Отчёт о прогоне" in text


def test_export_to_html_report_is_self_contained(tmp_path):
    stats = CrawlerStats.from_results(RESULTS, PAGES, elapsed=2.0)

    target = stats.export_to_html_report(tmp_path / "report.html")

    html = target.read_text(encoding="utf-8")
    assert html.startswith("<!DOCTYPE html>")
    assert "<style>" in html
    # Отчёт должен открываться без сети: ни скриптов, ни внешних ссылок.
    assert "<script" not in html
    assert "http://cdn" not in html and "https://cdn" not in html


def test_html_report_has_all_sections(tmp_path):
    stats = CrawlerStats.from_results(
        RESULTS,
        PAGES,
        {
            "retry": {"retries": 2, "given_up": 1},
            "robots": {"enabled": True, "checks": 5, "disallowed": 1},
            "sitemap": {"enabled": True, "files": 1, "urls": 4},
            "storage": {"enabled": True, "name": "JSONStorage", "written": 3},
        },
        elapsed=2.0,
    )
    stats.settings = {"max_pages": 10, "storage": ["json"]}

    html = stats.export_to_html_report(tmp_path / "report.html").read_text("utf-8")

    for section in (
        "Статус-коды",
        "Топ доменов",
        "Глубина обхода",
        "Типы содержимого",
        "Ошибки",
        "Повторы",
        "robots.txt",
        "Карта сайта",
        "Сохранение данных",
        "Страницы",
        "Настройки",
    ):
        assert section in html


def test_html_report_skips_sections_without_data(tmp_path):
    stats = CrawlerStats.from_results([result("https://a.example/1")])

    html = stats.export_to_html_report(tmp_path / "report.html").read_text("utf-8")

    # Про ошибки отчёт пишет всегда — «ошибок не было» это тоже результат, —
    # а разделы про карту и хранилища появляются только когда их включали.
    assert "Ошибок не было" in html
    assert "Карта сайта" not in html
    assert "Сохранение данных" not in html


def test_html_report_escapes_page_data(tmp_path):
    """В заголовках страниц бывает что угодно, вплоть до чужого скрипта."""
    evil = page("https://a.example/<img>")
    evil["title"] = "<script>alert('xss')</script>"
    stats = CrawlerStats.from_results([result("https://a.example/1")], [evil])

    html = stats.export_to_html_report(tmp_path / "report.html").read_text("utf-8")

    assert "<script>alert" not in html
    assert "&lt;script&gt;" in html


def test_html_report_limits_page_table(tmp_path):
    pages = [page(f"https://a.example/{n}") for n in range(PAGES_IN_REPORT + 10)]
    stats = CrawlerStats.from_results([result("https://a.example/1")], pages)

    html = stats.export_to_html_report(tmp_path / "report.html").read_text("utf-8")

    assert html.count("<tr><td>https://a.example/") <= PAGES_IN_REPORT
    assert str(len(pages)) in html  # общее число страниц всё равно показано


def test_settings_keep_zeros_and_switches(tmp_path):
    """`respect_robots: нет` и `max_depth: 0` — настройки, а не пустые места."""
    stats = CrawlerStats.from_results(RESULTS, PAGES, elapsed=1.0)
    stats.settings = {
        "max_depth": 0,
        "progress_interval": 0.0,
        "respect_robots": False,
        "rotate_ua": [],
        "log_file": None,
    }

    html = stats.export_to_html_report(tmp_path / "report.html").read_text("utf-8")

    assert "max_depth" in html and "progress_interval" in html
    assert "respect_robots" in html and "нет" in html
    # А вот не заданного в отчёте быть не должно: объяснять нечего.
    assert "rotate_ua" not in html and "log_file" not in html


def test_report_without_crawl_does_not_invent_queue(tmp_path):
    """Режим загрузки с парсингом: очереди не было, но сохранение было."""
    stats = CrawlerStats.from_results(
        RESULTS,
        PAGES,
        {"storage": {"enabled": True, "storage": "JSONStorage", "target": "a.json",
                     "saved": 3}},
        elapsed=1.0,
    )

    html = stats.export_to_html_report(tmp_path / "report.html").read_text("utf-8")

    assert "обход по ссылкам не выполнялся" in html
    assert "найдено URL" not in html
    assert "Сохранение данных" in html


def test_html_report_renders_empty_run(tmp_path):
    target = CrawlerStats().export_to_html_report(tmp_path / "empty.html")

    assert "<!DOCTYPE html>" in target.read_text(encoding="utf-8")


# --- сборка из краулера -----------------------------------------------------


async def test_from_crawler_takes_results_and_crawl_stats(url):
    from src.crawler import AsyncCrawler
    from src.parser import HTMLParser

    async with AsyncCrawler(parser=HTMLParser(), max_depth=1) as crawler:
        pages = await crawler.crawl([url("/site/")], max_pages=3)
        stats = CrawlerStats.from_crawler(crawler, pages, title="Проверка")

    assert stats.title == "Проверка"
    assert stats.total_pages == len(crawler.results)
    assert stats.successful >= 1
    assert stats.elapsed > 0
    assert stats.crawl["visited"] >= 1
    assert "200" in stats.by_status
