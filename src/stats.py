"""Расширенная статистика прогона и отчёты по ней: JSON и HTML (День 7).

`CrawlStats` из `src/models.py` — итог одной загрузки: всего, успешно, ошибок,
время. `CrawlerStats` здесь — отчёт по целому прогону: распределение по
статус-кодам, топ доменов, глубина, ошибки, повторы, сохранение. Разделение не
случайно: первое живёт внутри краулера и считается после каждого запуска,
второе собирается один раз для человека.
"""

from __future__ import annotations

import json
import logging
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from html import escape
from pathlib import Path
from typing import TYPE_CHECKING, Any

from src.filters import domain_key
from src.models import ERROR_UNKNOWN, FetchResult
from src.utils import json_default

if TYPE_CHECKING:  # только для подсказок типов: модуль не зависит от краулера
    from src.crawler import AsyncCrawler

logger = logging.getLogger(__name__)

# Ответа не было вовсе: таймаут, сетевая ошибка, отказ circuit breaker.
NO_STATUS = "нет ответа"
TOP_LIMIT = 10
# Сколько страниц показывать в HTML-отчёте: таблица на тысячу строк не отчёт, а
# выгрузка, и для неё есть хранилища дня 6.
PAGES_IN_REPORT = 50


@dataclass(slots=True)
class CrawlerStats:
    """Сводка по прогону: собирается из результатов, страниц и статистики обхода.

    Своего состояния не ведёт и в обходе не участвует — это агрегатор, как
    `AsyncCrawler.get_error_report()`. Поэтому один и тот же прогон можно
    посчитать дважды и получить тот же результат.
    """

    total_pages: int = 0
    successful: int = 0
    failed: int = 0
    elapsed: float = 0.0
    started_at: datetime | None = None
    finished_at: datetime | None = None
    by_status: dict[str, int] = field(default_factory=dict)
    by_error_type: dict[str, int] = field(default_factory=dict)
    by_depth: dict[int, int] = field(default_factory=dict)
    by_content_type: dict[str, int] = field(default_factory=dict)
    domains: list[dict[str, Any]] = field(default_factory=list)
    pages: list[dict[str, Any]] = field(default_factory=list)
    crawl: dict[str, Any] = field(default_factory=dict)
    # Настройки прогона: по отчёту должно быть понятно, чем он получен.
    settings: dict[str, Any] = field(default_factory=dict)
    title: str = "Отчёт краулера"

    @property
    def pages_per_sec(self) -> float:
        """Средняя скорость обработки — так же, как её считает очередь."""
        return self.total_pages / self.elapsed if self.elapsed > 0 else 0.0

    @property
    def avg_page_time(self) -> float:
        """Среднее время на страницу: при конкурентности меньше 1/скорости."""
        return self.elapsed / self.total_pages if self.total_pages else 0.0

    @property
    def success_rate(self) -> float:
        return self.successful / self.total_pages * 100 if self.total_pages else 0.0

    @classmethod
    def from_results(
        cls,
        results: list[FetchResult],
        pages: list[dict[str, Any]] | None = None,
        crawl: dict[str, Any] | None = None,
        *,
        elapsed: float | None = None,
        started_at: datetime | None = None,
        title: str = "Отчёт краулера",
    ) -> CrawlerStats:
        """Считает сводку по результатам загрузки и разобранным страницам."""
        pages = pages or []
        crawl = dict(crawl or {})
        statuses: Counter[str] = Counter()
        error_types: Counter[str] = Counter()
        per_domain: dict[str, dict[str, Any]] = {}
        successful = 0

        for result in results:
            if result.ok:
                successful += 1
            else:
                error_types[result.error_type or ERROR_UNKNOWN] += 1
            statuses[str(result.status) if result.status else NO_STATUS] += 1

            # Домен считается по адресу после редиректов: именно он отдал
            # страницу, и в отчёте интересен он.
            domain = domain_key(result.final_url or result.url)
            row = per_domain.setdefault(
                domain, {"domain": domain, "pages": 0, "failed": 0, "time": 0.0}
            )
            row["pages"] += 1
            row["time"] += result.elapsed
            if not result.ok:
                row["failed"] += 1

        depths: Counter[int] = Counter()
        content_types: Counter[str] = Counter()
        for page in pages:
            depths[page.get("depth", 0)] += 1
            if content_type := page.get("content_type"):
                # «text/html; charset=utf-8» и «text/html» — один и тот же тип.
                content_types[str(content_type).split(";", 1)[0].strip()] += 1

        for row in per_domain.values():
            row["avg_time"] = row["time"] / row["pages"] if row["pages"] else 0.0

        total_elapsed = elapsed
        if total_elapsed is None:
            total_elapsed = float(crawl.get("elapsed") or 0.0)

        return cls(
            total_pages=len(results),
            successful=successful,
            failed=len(results) - successful,
            elapsed=total_elapsed,
            started_at=started_at,
            finished_at=datetime.now(),
            by_status=dict(statuses.most_common()),
            by_error_type=dict(error_types.most_common()),
            by_depth=dict(sorted(depths.items())),
            by_content_type=dict(content_types.most_common()),
            domains=sorted(
                per_domain.values(), key=lambda item: (-item["pages"], item["domain"])
            ),
            pages=pages,
            crawl=crawl,
            title=title,
        )

    @classmethod
    def from_crawler(
        cls,
        crawler: AsyncCrawler,
        pages: list[dict[str, Any]] | None = None,
        *,
        started_at: datetime | None = None,
        title: str = "Отчёт краулера",
    ) -> CrawlerStats:
        """То же, но краулер сам отдаёт и результаты, и статистику обхода."""
        return cls.from_results(
            crawler.results,
            pages,
            crawler.get_crawl_stats(),
            elapsed=crawler.stats.elapsed or None,
            started_at=started_at,
            title=title,
        )

    def top_domains(self, limit: int = TOP_LIMIT) -> list[dict[str, Any]]:
        """Домены по числу страниц — список уже отсортирован при сборке."""
        return self.domains[:limit]

    def get_stats(self) -> dict[str, Any]:
        """Сводка словарём: такой же набор уходит и в JSON-отчёт.

        Ключи `total_pages`, `successful` и `failed` названы как в задании: по
        ним демо-пример печатает итог.
        """
        return {
            "title": self.title,
            "total_pages": self.total_pages,
            "successful": self.successful,
            "failed": self.failed,
            "success_rate": round(self.success_rate, 1),
            "elapsed": round(self.elapsed, 3),
            "pages_per_sec": round(self.pages_per_sec, 2),
            "avg_page_time": round(self.avg_page_time, 3),
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "by_status": self.by_status,
            "by_error_type": self.by_error_type,
            "by_depth": self.by_depth,
            "by_content_type": self.by_content_type,
            "top_domains": self.top_domains(),
            "domains_total": len(self.domains),
            "settings": self.settings,
            "crawl": self.crawl,
        }

    def export_to_json(self, filename: str | Path) -> Path:
        """Сохраняет сводку в JSON.

        Страницы в файл не идут: отчёт — про прогон, а сами данные пишут
        хранилища дня 6. Время — через общий `json_default`, то есть в ISO.
        """
        target = Path(filename)
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("w", encoding="utf-8") as handle:
            json.dump(
                self.get_stats(),
                handle,
                ensure_ascii=False,
                indent=2,
                default=json_default,
            )

        logger.info("Статистика сохранена: %s", target)
        return target

    def export_to_html_report(self, filename: str | Path) -> Path:
        """Собирает HTML-отчёт: один самодостаточный файл без JS и сети."""
        target = Path(filename)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(self.render_html(), encoding="utf-8")

        logger.info("HTML-отчёт сохранён: %s", target)
        return target

    def render_html(self) -> str:
        """Разметка отчёта. Вынесена отдельно, чтобы её можно было проверить."""
        return _render(self)


# --- сборка HTML ------------------------------------------------------------

# Стили инлайном: отчёт должны открывать на машине без интернета, поэтому ни
# CDN, ни JS-библиотек. Диаграммы — полоски на div с процентной шириной: этого
# хватает, чтобы увидеть перекос по статусам или доменам.
_STYLE = """
:root { color-scheme: light dark; }
body { font-family: -apple-system, Segoe UI, Roboto, sans-serif; margin: 0 auto;
       max-width: 60rem; padding: 2rem 1rem; line-height: 1.5; }
h1 { margin-bottom: 0.2rem; }
.sub { color: #777; margin-top: 0; }
.cards { display: flex; flex-wrap: wrap; gap: 0.75rem; margin: 1.5rem 0; }
.card { border: 1px solid #8884; border-radius: 0.5rem; padding: 0.75rem 1rem;
        min-width: 8rem; }
.card .value { font-size: 1.6rem; font-weight: 600; }
.card .label { color: #777; font-size: 0.85rem; }
.card.bad .value { color: #c0392b; }
table { border-collapse: collapse; width: 100%; margin-bottom: 1.5rem; }
th, td { text-align: left; padding: 0.35rem 0.6rem; border-bottom: 1px solid #8883; }
th { font-weight: 600; }
td.num, th.num { text-align: right; font-variant-numeric: tabular-nums; }
.bar { background: #8882; border-radius: 0.2rem; height: 0.9rem; min-width: 2px; }
.bar.bad { background: #c0392b88; }
.url { font-family: ui-monospace, SFMono-Regular, monospace; font-size: 0.85rem;
       word-break: break-all; }
section { margin-bottom: 2rem; }
footer { color: #777; font-size: 0.85rem; border-top: 1px solid #8883;
         padding-top: 0.75rem; }
.empty { color: #777; }
"""


def _esc(value: Any) -> str:
    return escape(str(value), quote=True)


def _card(value: Any, label: str, *, bad: bool = False) -> str:
    return (
        f'<div class="card{" bad" if bad else ""}">'
        f'<div class="value">{_esc(value)}</div>'
        f'<div class="label">{_esc(label)}</div></div>'
    )


def _table(headers: list[str], rows: list[list[str]], *, numeric: set[int] = frozenset()) -> str:
    """Таблица с правым выравниванием числовых колонок."""
    if not rows:
        return '<p class="empty">Нет данных</p>'

    head = "".join(
        f'<th class="num">{_esc(name)}</th>' if index in numeric else f"<th>{_esc(name)}</th>"
        for index, name in enumerate(headers)
    )
    body = []
    for row in rows:
        cells = "".join(
            f'<td class="num">{cell}</td>' if index in numeric else f"<td>{cell}</td>"
            for index, cell in enumerate(row)
        )
        body.append(f"<tr>{cells}</tr>")
    return f"<table><thead><tr>{head}</tr></thead><tbody>{''.join(body)}</tbody></table>"


def _bar(count: int, total: int, *, bad: bool = False) -> str:
    """Полоска-диаграмма шириной в процентах от максимума."""
    width = count / total * 100 if total else 0
    classes = "bar bad" if bad else "bar"
    return f'<div class="{classes}" style="width: {width:.1f}%"></div>'


def _counts_table(
    counts: dict[Any, int], first_header: str, *, bad_keys: set[str] = frozenset()
) -> str:
    """Распределение: значение, количество, доля и полоска."""
    total = sum(counts.values())
    peak = max(counts.values(), default=0)
    rows = []
    for name, count in counts.items():
        share = count / total * 100 if total else 0
        rows.append(
            [
                _esc(name),
                str(count),
                f"{share:.1f}%",
                _bar(count, peak, bad=str(name) in bad_keys),
            ]
        )
    return _table([first_header, "Страниц", "Доля", ""], rows, numeric={1, 2})


def _is_bad_status(name: str) -> bool:
    """Код ответа, который считается плохим: 4xx, 5xx и отсутствие ответа."""
    return name == NO_STATUS or (name[:1].isdigit() and int(name) >= 400)


def _section(title: str, body: str) -> str:
    return f"<section><h2>{_esc(title)}</h2>{body}</section>"


def _optional_sections(stats: CrawlerStats) -> str:
    """Блоки, которые появляются только если соответствующий механизм работал."""
    crawl = stats.crawl
    blocks: list[str] = []

    retry: dict[str, Any] = crawl.get("retry") or {}
    if retry.get("retries") or retry.get("exhausted"):
        blocks.append(
            _section(
                "Повторы",
                _table(
                    ["Показатель", "Значение"],
                    [
                        ["Повторов", str(retry.get("retries", 0))],
                        ["Спасено повтором", str(retry.get("retry_success", 0))],
                        ["Исчерпано попыток", str(retry.get("exhausted", 0))],
                        ["Ожидание, с", f"{retry.get('wait_total', 0.0):.2f}"],
                    ],
                    numeric={1},
                ),
            )
        )

    robots: dict[str, Any] = crawl.get("robots") or {}
    if robots.get("enabled"):
        blocks.append(
            _section(
                "robots.txt",
                _table(
                    ["Показатель", "Значение"],
                    [
                        ["Проверок", str(robots.get("checks", 0))],
                        ["Запрещено", str(robots.get("blocked", 0))],
                        ["Файлов загружено", str(robots.get("fetched", 0))],
                        # Загрузок может быть 0 при обходе по готовому кэшу —
                        # например, когда robots.txt прочитали ради карты сайта.
                        ["Файлов в кэше", str(robots.get("domains", 0))],
                        ["Нет файла", str(robots.get("missing", 0))],
                    ],
                    numeric={1},
                ),
            )
        )

    sitemap: dict[str, Any] = crawl.get("sitemap") or {}
    if sitemap.get("enabled"):
        blocks.append(
            _section(
                "Карта сайта",
                _table(
                    ["Показатель", "Значение"],
                    [
                        ["Файлов разобрано", str(sitemap.get("files", 0))],
                        ["Индексных карт", str(sitemap.get("indexes", 0))],
                        ["Адресов получено", str(sitemap.get("urls", 0))],
                        ["Ошибок", str(sitemap.get("errors", 0))],
                        ["Оборвано лимитом", "да" if sitemap.get("truncated") else "нет"],
                    ],
                    numeric={1},
                ),
            )
        )

    storage: dict[str, Any] = crawl.get("storage") or {}
    if storage.get("enabled"):
        rows = [
            ["Хранилище", _esc(storage.get("storage", "—"))],
            ["Куда", f'<span class="url">{_esc(storage.get("target", "—"))}</span>'],
            ["Записей", str(storage.get("saved", 0))],
            ["Не записано", str(storage.get("failed", 0))],
            ["Время записи, с", f"{storage.get('write_time', 0.0):.2f}"],
        ]
        for child in storage.get("storages") or []:
            rows.append(
                [
                    f'<span class="url">{_esc(child.get("target", "—"))}</span>',
                    f"записей {child.get('saved', 0)}, ошибок {child.get('failed', 0)}",
                ]
            )
        blocks.append(_section("Сохранение данных", _table(["Показатель", "Значение"], rows)))

    return "".join(blocks)


def _errors_section(stats: CrawlerStats) -> str:
    errors: dict[str, Any] = stats.crawl.get("errors") or {}
    if not stats.failed and not errors.get("failed"):
        return _section("Ошибки", '<p class="empty">Ошибок не было</p>')

    blocks = []
    if stats.by_error_type:
        blocks.append(_counts_table(stats.by_error_type, "Тип ошибки"))
    if by_class := errors.get("by_class"):
        blocks.append(
            _table(
                ["Класс ошибки", "Случаев"],
                [[_esc(name), str(count)] for name, count in by_class.items()],
                numeric={1},
            )
        )
    if permanent := errors.get("permanent_urls"):
        blocks.append("<h3>Постоянные ошибки</h3>")
        blocks.append(
            _table(
                ["URL", "Ошибка"],
                [
                    [
                        f'<span class="url">{_esc(item.get("url"))}</span>',
                        _esc(item.get("error")),
                    ]
                    for item in permanent[:PAGES_IN_REPORT]
                ],
            )
        )
    return _section("Ошибки", "".join(blocks))


def _pages_section(stats: CrawlerStats) -> str:
    if not stats.pages:
        return ""

    rows = []
    for page in stats.pages[:PAGES_IN_REPORT]:
        rows.append(
            [
                f'<span class="url">{_esc(page.get("url", ""))}</span>',
                _esc(page.get("status") or "—"),
                _esc(page.get("depth", 0)),
                _esc(page.get("links_count", 0)),
                _esc(page.get("text_length", 0)),
                _esc(page.get("title") or ""),
            ]
        )

    body = _table(
        ["URL", "Статус", "Глубина", "Ссылок", "Текст", "Заголовок"],
        rows,
        numeric={1, 2, 3, 4},
    )
    if len(stats.pages) > PAGES_IN_REPORT:
        body += (
            f'<p class="empty">Показаны первые {PAGES_IN_REPORT} из '
            f"{len(stats.pages)} страниц</p>"
        )
    return _section("Страницы", body)


def _settings_value(value: Any) -> str:
    """Значение настройки для таблицы: список строкой, выключатель словом."""
    if isinstance(value, bool):
        # `respect_robots: False` — важная настройка, а не пустое место: по
        # отчёту должно быть видно, что robots.txt не учитывался.
        return "да" if value else "нет"
    if isinstance(value, (list, tuple)):
        return ", ".join(map(str, value))
    return str(value)


def _settings_section(stats: CrawlerStats) -> str:
    """Настройки прогона.

    Пропускается только то, чего не задавали: `None` и пустые списки. Нули и
    `False` остаются — `max_depth: 0` значит «только стартовые страницы», а
    `progress_interval: 0` — «без строки прогресса», и это не то же самое, что
    настройка, которой не было.
    """
    if not stats.settings:
        return ""

    rows = [
        [_esc(name), _esc(_settings_value(value))]
        for name, value in stats.settings.items()
        if value is not None and value != "" and value != [] and value != {}
    ]
    return _section("Настройки прогона", _table(["Настройка", "Значение"], rows))


def _render(stats: CrawlerStats) -> str:
    started = (
        stats.started_at.strftime("%d.%m.%Y %H:%M:%S") if stats.started_at else "—"
    )
    finished = (
        stats.finished_at.strftime("%d.%m.%Y %H:%M:%S") if stats.finished_at else "—"
    )
    crawl = stats.crawl

    cards = "".join(
        [
            _card(stats.total_pages, "страниц обработано"),
            _card(stats.successful, "успешно"),
            _card(stats.failed, "ошибок", bad=bool(stats.failed)),
            _card(f"{stats.success_rate:.0f}%", "доля успешных"),
            _card(f"{stats.pages_per_sec:.2f}", "страниц в секунду"),
            _card(f"{stats.elapsed:.2f} с", "время работы"),
        ]
    )

    sections = [
        _section(
            "Статус-коды",
            _counts_table(
                stats.by_status,
                "Код ответа",
                bad_keys={name for name in stats.by_status if _is_bad_status(name)},
            ),
        ),
        _section(
            "Топ доменов",
            _table(
                ["Домен", "Страниц", "Ошибок", "Среднее время, с", ""],
                [
                    [
                        _esc(row["domain"]),
                        str(row["pages"]),
                        str(row["failed"]),
                        f"{row['avg_time']:.2f}",
                        _bar(row["pages"], stats.domains[0]["pages"] if stats.domains else 0),
                    ]
                    for row in stats.top_domains()
                ],
                numeric={1, 2, 3},
            ),
        ),
    ]

    if stats.by_depth:
        sections.append(_section("Глубина обхода", _counts_table(stats.by_depth, "Глубина")))
    if stats.by_content_type:
        sections.append(
            _section("Типы содержимого", _counts_table(stats.by_content_type, "Content-Type"))
        )

    sections.append(_errors_section(stats))
    sections.append(_optional_sections(stats))
    sections.append(_pages_section(stats))
    sections.append(_settings_section(stats))

    # Про очередь пишем, только если она работала: у режима загрузки с
    # парсингом статистика обхода не пустая (там есть сохранение), но «найдено
    # URL 0» для него было бы неправдой.
    queue_line = (
        f"найдено URL {crawl['seen']}"
        f", в очереди осталось {crawl.get('pending', 0)}"
        f", максимальная глубина {crawl.get('max_depth_reached', 0)}"
        if "seen" in crawl
        else "обход по ссылкам не выполнялся"
    )

    return (
        "<!DOCTYPE html>\n"
        '<html lang="ru"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1">'
        f"<title>{_esc(stats.title)}</title>"
        f"<style>{_STYLE}</style></head><body>"
        f"<h1>{_esc(stats.title)}</h1>"
        f'<p class="sub">Начало: {_esc(started)} | окончание: {_esc(finished)}'
        f" | {_esc(queue_line)}</p>"
        f'<div class="cards">{cards}</div>'
        f"{''.join(sections)}"
        f"<footer>Отчёт сформирован краулером, "
        f"{_esc(datetime.now().strftime('%d.%m.%Y %H:%M:%S'))}</footer>"
        "</body></html>\n"
    )
