"""Конфигурация краулера из файла YAML или JSON (День 7).

Имена настроек совпадают с именами флагов CLI: у пользователя один словарь
понятий, а перевод в аргументы `AsyncCrawler` собран в одном месте —
`to_crawler_kwargs()`. Поэтому `config.yaml` читается как записанная командная
строка, а не как второй, параллельный набор названий.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field, fields
from datetime import datetime
from pathlib import Path
from typing import Any

from src.crawler import DEFAULT_USER_AGENT
from src.sitemap import SITEMAP_AUTO
from src.storage import (
    CSVStorage,
    DataStorage,
    JSONStorage,
    MultiStorage,
    SQLiteStorage,
)

try:  # pyyaml есть в requirements, но без него JSON-конфиги должны работать
    import yaml
except ImportError:  # pragma: no cover - зависит от окружения, не от кода
    yaml = None

logger = logging.getLogger(__name__)

# Форматы сохранения (день 6) и расширения их файлов.
STORAGE_SUFFIXES = {
    "json": ".json",
    "jsonl": ".jsonl",
    "csv": ".csv",
    "sqlite": ".db",
}
# Батчи по умолчанию: файлу незачем копить страницы, а базе — есть зачем.
DEFAULT_BATCH_SIZES = {"json": 1, "jsonl": 1, "csv": 1, "sqlite": 50}

DEFAULT_OUTPUT_DIR = Path("data")

YAML_SUFFIXES = (".yaml", ".yml")

# Разделы файла существуют только для читаемости: набор ключей плоский, и
# раскладка по разделам ни на что не влияет. Так в конфиге видны смысловые
# группы, а в коде не появляется второй уровень имён.
SECTIONS = (
    "crawler",
    "politeness",
    "retry",
    "timeouts",
    "filters",
    "storage_settings",
    "logging",
    "output",
)

# Привычные синонимы: в задании лимит скорости называется rate_limit, а список
# адресов — urls. Отказывать из-за названия невежливо.
ALIASES = {
    "urls": "start_urls",
    "rate_limit": "requests_per_second",
    "rps": "requests_per_second",
    "sitemap": "sitemaps",
    "include_patterns": "include",
    "exclude_patterns": "exclude",
    "same_domain_only": "same_domain",
    "user_agents": "rotate_ua",
    "retry_backoff_factor": "retry_backoff",
}


class ConfigError(ValueError):
    """Конфигурация непригодна: неизвестный ключ, не тот тип, плохое значение."""


@dataclass(slots=True)
class CrawlerConfig:
    """Настройки одного прогона краулера.

    Значения по умолчанию здесь библиотечные, а не те, что у демо-режимов CLI:
    конфигом пользуются и без командной строки. При слиянии с флагами
    применяются только ключи, которые действительно были в файле (`provided`),
    поэтому молчаливой подмены пользовательских значений не происходит.
    """

    # что обходить
    start_urls: list[str] = field(default_factory=list)
    sitemaps: list[str] = field(default_factory=list)
    max_pages: int = 100
    max_depth: int = 2
    # конкурентность
    max_concurrent: int = 10
    max_per_domain: int | None = 3
    # вежливость
    requests_per_second: float | None = None
    global_rate: bool = False
    min_delay: float = 0.0
    jitter: float = 0.0
    max_backoff: float = 60.0
    respect_robots: bool = False
    user_agent: str = DEFAULT_USER_AGENT
    rotate_ua: list[str] = field(default_factory=list)
    # таймауты
    timeout_total: float = 15.0
    timeout_connect: float = 5.0
    timeout_read: float = 10.0
    timeout_growth: float = 1.5
    # повторы и circuit breaker
    max_retries: int = 2
    retry_base_delay: float = 0.5
    retry_backoff: float = 2.0
    retry_max_delay: float = 30.0
    circuit_breaker: bool = False
    circuit_threshold: int = 5
    circuit_recovery: float = 30.0
    # фильтры
    same_domain: bool = False
    include: list[str] = field(default_factory=list)
    exclude: list[str] = field(default_factory=list)
    # сохранение данных
    storage: list[str] = field(default_factory=list)
    storage_path: str | None = None
    batch_size: int | None = None
    save_failed: bool = False
    # логирование
    log_level: str = "INFO"
    log_file: str | None = None
    log_max_bytes: int = 5_000_000
    log_backups: int = 3
    # отчёты
    output: str | None = None
    report_html: str | None = None
    report_json: str | None = None
    progress_interval: float = 0.5
    # откуда конфиг пришёл и какие ключи в нём были заданы
    source: str = ""
    provided: frozenset[str] = frozenset()

    @classmethod
    def field_names(cls) -> tuple[str, ...]:
        """Имена настроек; `source` и `provided` задаются не пользователем."""
        return tuple(
            item.name for item in fields(cls) if item.name not in ("source", "provided")
        )

    @classmethod
    def from_file(cls, path: str | Path) -> CrawlerConfig:
        """Читает YAML или JSON — формат выбирается по расширению файла."""
        target = Path(path)
        if not target.exists():
            raise ConfigError(f"файл конфигурации не найден: {target}")

        try:
            text = target.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as exc:
            # Каталог вместо файла, нет прав, не та кодировка — всё это ошибка
            # конфигурации, а не повод показать пользователю трейсбек.
            raise ConfigError(f"не удалось прочитать {target}: {exc}") from exc

        suffix = target.suffix.lower()
        try:
            if suffix in YAML_SUFFIXES:
                if yaml is None:
                    raise ConfigError(
                        f"для YAML-конфига нужен pyyaml: pip install pyyaml ({target})"
                    )
                data = yaml.safe_load(text)
            elif suffix == ".json":
                data = json.loads(text)
            else:
                raise ConfigError(
                    f"неизвестное расширение конфигурации {suffix or '(нет)'}: "
                    "ожидались .yaml, .yml или .json"
                )
        except ConfigError:
            raise
        except Exception as exc:  # битый YAML или JSON
            raise ConfigError(f"не удалось разобрать {target}: {exc}") from exc

        if data is None:
            # Пустой файл — это не ошибка: так выглядит конфиг, из которого всё
            # закомментировали.
            logger.warning("Конфигурация %s пуста, работают значения по умолчанию", target)
            data = {}

        config = cls.from_dict(data, source=str(target))
        logger.info(
            "Конфигурация загружена: %s (задано ключей %d)", target, len(config.provided)
        )
        return config

    @classmethod
    def from_dict(cls, data: Any, *, source: str = "") -> CrawlerConfig:
        """Собирает конфигурацию из словаря, проверяя ключи и значения.

        Неизвестный ключ — ошибка, а не предупреждение: опечатка в настройке
        иначе стоила бы целого прогона, и узнать о ней было бы неоткуда.
        """
        if not isinstance(data, dict):
            raise ConfigError(
                f"конфигурация должна быть словарём, получено {type(data).__name__}"
            )

        flat = _flatten(data)
        known = set(cls.field_names())
        values: dict[str, Any] = {}
        source_key: dict[str, str] = {}

        for key, value in flat.items():
            name = ALIASES.get(key, key)
            if name not in known:
                hint = _suggest(name, known)
                raise ConfigError(
                    f"неизвестная настройка {key!r}" + (f", похоже на {hint!r}" if hint else "")
                )
            if name in values:
                # `urls` и `start_urls` — одна настройка под двумя именами.
                # Молча взять последнее значение нельзя: файл утверждает две
                # разные вещи, и какую из них имели в виду, неизвестно.
                raise ConfigError(
                    f"настройка {name!r} задана дважды: {source_key[name]!r} и {key!r}"
                )
            values[name] = value
            source_key[name] = key

        config = cls(source=source, provided=frozenset(values))
        for name, value in values.items():
            setattr(config, name, _coerce(name, value))
        config.validate()
        return config

    def validate(self) -> None:
        """Проверяет значения так же, как это делает CLI — до начала обхода."""
        for name, minimum in (
            ("max_pages", 1),
            ("max_concurrent", 1),
            ("max_depth", 0),
            ("max_retries", 0),
            ("circuit_threshold", 1),
        ):
            if getattr(self, name) < minimum:
                raise ConfigError(f"{name} должен быть >= {minimum}")
        if self.max_per_domain is not None and self.max_per_domain < 1:
            raise ConfigError("max_per_domain должен быть >= 1")
        if self.batch_size is not None and self.batch_size < 1:
            raise ConfigError("batch_size должен быть >= 1")
        for name in (
            "min_delay",
            "jitter",
            "max_backoff",
            "retry_base_delay",
            "retry_max_delay",
            "circuit_recovery",
            "timeout_total",
            "timeout_connect",
            "timeout_read",
            "progress_interval",
            "log_max_bytes",
            "log_backups",
        ):
            if getattr(self, name) < 0:
                raise ConfigError(f"{name} не может быть отрицательным")
        if self.requests_per_second is not None and self.requests_per_second < 0:
            raise ConfigError("requests_per_second не может быть отрицательным")
        for name, minimum in (("retry_backoff", 1), ("timeout_growth", 1)):
            if getattr(self, name) < minimum:
                raise ConfigError(f"{name} должен быть >= {minimum}")

        unknown = [name for name in self.storage if name not in STORAGE_SUFFIXES]
        if unknown:
            raise ConfigError(
                f"неизвестные форматы сохранения: {', '.join(unknown)}; "
                f"доступны {', '.join(sorted(STORAGE_SUFFIXES))}"
            )
        if self.log_level.upper() not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
            raise ConfigError(f"неизвестный уровень логирования: {self.log_level}")
        self.log_level = self.log_level.upper()

        for name in ("output", "report_html", "report_json", "log_file"):
            # Проверяем до обхода: узнать, что отчёт некуда записать, после
            # часа работы — обидно вдвойне.
            value = getattr(self, name)
            if value and Path(value).is_dir():
                raise ConfigError(f"{name}: {value} — это каталог, нужен файл")

        if not self.start_urls and not self.sitemaps:
            raise ConfigError("нужны start_urls или sitemaps — обходить нечего")
        if SITEMAP_AUTO in self.sitemaps and not self.start_urls:
            # «auto» означает «поищи карту по robots.txt сайта», а какого сайта
            # — известно только из start_urls.
            raise ConfigError(
                f"для sitemaps: {SITEMAP_AUTO} нужны start_urls — по ним ищется robots.txt"
            )

    def to_dict(self) -> dict[str, Any]:
        """Настройки словарём — для отчёта: видно, с чем шёл прогон."""
        return {name: getattr(self, name) for name in self.field_names()}

    def to_crawler_kwargs(self) -> dict[str, Any]:
        """Аргументы конструктора `AsyncCrawler`.

        Единственное место, где имена настроек переводятся в имена библиотеки:
        `rate_limit` конфига — это `requests_per_second` краулера.
        """
        return {
            "max_concurrent": self.max_concurrent,
            "max_depth": self.max_depth,
            "max_per_domain": self.max_per_domain,
            "requests_per_second": self.requests_per_second or None,
            "per_domain_rate": not self.global_rate,
            "min_delay": self.min_delay,
            "jitter": self.jitter,
            "max_backoff": self.max_backoff,
            "respect_robots": self.respect_robots,
            "user_agent": self.user_agent,
            "user_agents": self.rotate_ua or None,
            "timeout_total": self.timeout_total,
            "timeout_connect": self.timeout_connect,
            "timeout_read": self.timeout_read,
            "timeout_growth": self.timeout_growth,
            "max_retries": self.max_retries,
            "retry_base_delay": self.retry_base_delay,
            "retry_backoff_factor": self.retry_backoff,
            "retry_max_delay": self.retry_max_delay,
            "circuit_breaker": self.circuit_breaker,
            "circuit_threshold": self.circuit_threshold,
            "circuit_recovery": self.circuit_recovery,
            "storage_save_failed": self.save_failed,
        }

    def to_crawl_kwargs(self) -> dict[str, Any]:
        """Аргументы метода `crawl()`: пределы обхода и фильтры."""
        return {
            "max_pages": self.max_pages,
            "max_depth": self.max_depth,
            "same_domain_only": self.same_domain,
            "include_patterns": self.include or None,
            "exclude_patterns": self.exclude or None,
        }

    def build_storage(self) -> DataStorage | None:
        """Собирает хранилище по списку форматов; без форматов — ничего."""
        return build_storage(
            self.storage, prefix=self.storage_path, batch_size=self.batch_size
        )


def build_storage(
    formats: list[str],
    *,
    prefix: str | None = None,
    batch_size: int | None = None,
) -> DataStorage | None:
    """Хранилище под заданные форматы: одно на формат, несколько — `MultiStorage`.

    Общая для CLI и конфигурации сборка: иначе `--storage json csv` и
    `storage: [json, csv]` незаметно разъехались бы по умолчаниям.
    """
    if not formats:
        return None

    base = prefix or str(
        DEFAULT_OUTPUT_DIR / f"pages_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    )
    storages: list[DataStorage] = []
    for name in dict.fromkeys(formats):  # без повторов, порядок сохранён
        path = Path(f"{base}{STORAGE_SUFFIXES[name]}")
        batch = batch_size or DEFAULT_BATCH_SIZES[name]
        if name == "json":
            storages.append(JSONStorage(path, batch_size=batch))
        elif name == "jsonl":
            storages.append(JSONStorage(path, lines=True, batch_size=batch))
        elif name == "csv":
            storages.append(CSVStorage(path, batch_size=batch))
        else:
            storages.append(SQLiteStorage(path, batch_size=batch))

    if len(storages) == 1:
        return storages[0]
    return MultiStorage(storages)


# --- разбор значений --------------------------------------------------------


def _flatten(data: dict[str, Any]) -> dict[str, Any]:
    """Разворачивает разделы конфигурации в один плоский словарь."""
    flat: dict[str, Any] = {}

    def put(key: str, value: Any, where: str) -> None:
        if key in flat:
            raise ConfigError(f"настройка {key!r} задана дважды (второй раз в {where})")
        flat[key] = value

    names = CrawlerConfig.field_names()
    for key, value in data.items():
        # `output` — и раздел, и обычная настройка: разделом он считается
        # только когда это словарь, иначе это путь к файлу результатов.
        if key in SECTIONS and not (key in names and not isinstance(value, dict)):
            if value is None:
                continue  # пустой раздел в YAML — это None, а не словарь
            if not isinstance(value, dict):
                raise ConfigError(f"раздел {key!r} должен быть словарём настроек")
            for inner, inner_value in value.items():
                put(str(inner), inner_value, f"разделе {key!r}")
            continue
        put(str(key), value, "корне файла")

    return flat


def _coerce(name: str, value: Any) -> Any:
    """Приводит значение к типу настройки, ругаясь на явную несовместимость."""
    if name in ("start_urls", "sitemaps", "rotate_ua", "include", "exclude", "storage"):
        return _as_str_list(name, value)
    if name in ("max_pages", "max_depth", "max_concurrent", "max_retries",
                "circuit_threshold", "log_max_bytes", "log_backups"):
        return _as_int(name, value)
    if name in ("max_per_domain", "batch_size"):
        return None if value is None else _as_int(name, value)
    if name == "requests_per_second":
        return None if value in (None, 0, 0.0) else _as_float(name, value)
    if name in (
        "min_delay", "jitter", "max_backoff", "retry_base_delay", "retry_backoff",
        "retry_max_delay", "circuit_recovery", "timeout_total", "timeout_connect",
        "timeout_read", "timeout_growth", "progress_interval",
    ):
        return _as_float(name, value)
    if name in (
        "global_rate", "respect_robots", "circuit_breaker", "same_domain", "save_failed",
    ):
        return _as_bool(name, value)
    if name in ("storage_path", "log_file", "output", "report_html", "report_json"):
        return None if value is None else str(value)
    return str(value)


def _as_str_list(name: str, value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        # Одна строка вместо списка — частая и безобидная вольность.
        return [value]
    if isinstance(value, (list, tuple)):
        return [str(item) for item in value]
    raise ConfigError(f"{name} должен быть списком строк, получено {type(value).__name__}")


def _as_int(name: str, value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise ConfigError(f"{name} должен быть целым числом, получено {value!r}")
    try:
        number = int(value)
    except ValueError as exc:
        raise ConfigError(f"{name} должен быть целым числом, получено {value!r}") from exc
    return number


def _as_float(name: str, value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise ConfigError(f"{name} должен быть числом, получено {value!r}")
    try:
        return float(value)
    except ValueError as exc:
        raise ConfigError(f"{name} должен быть числом, получено {value!r}") from exc


def _as_bool(name: str, value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.lower() in ("true", "yes", "on", "1"):
        return True
    if isinstance(value, str) and value.lower() in ("false", "no", "off", "0"):
        return False
    raise ConfigError(f"{name} должен быть true или false, получено {value!r}")


def _suggest(name: str, known: set[str]) -> str | None:
    """Ближайшее известное имя — чтобы подсказать опечатку."""
    from difflib import get_close_matches

    matches = get_close_matches(name, sorted(known), n=1, cutoff=0.7)
    return matches[0] if matches else None
