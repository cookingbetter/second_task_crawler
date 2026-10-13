"""Тесты конфигурации краулера из файла (День 7)."""

from __future__ import annotations

import json

import pytest

from src.config import ConfigError, CrawlerConfig, build_storage
from src.storage import CSVStorage, JSONStorage, MultiStorage, SQLiteStorage

MINIMAL = {"start_urls": ["https://example.com"]}

FULL_YAML = """
crawler:
  start_urls:
    - https://example.com
  max_pages: 42
  max_depth: 3
  max_concurrent: 7
politeness:
  rate_limit: 2.5
  min_delay: 0.25
  respect_robots: true
filters:
  same_domain: true
  include:
    - /blog
  exclude:
    - /admin
storage_settings:
  storage:
    - jsonl
    - sqlite
  storage_path: data/run
  batch_size: 20
logging:
  log_level: debug
  log_file: logs/run.log
output:
  report_html: data/report.html
  report_json: data/stats.json
"""


def write(tmp_path, name: str, text: str):
    target = tmp_path / name
    target.write_text(text, encoding="utf-8")
    return target


# --- чтение файла -----------------------------------------------------------


def test_yaml_sections_flatten_into_one_namespace(tmp_path):
    config = CrawlerConfig.from_file(write(tmp_path, "c.yaml", FULL_YAML))

    assert config.start_urls == ["https://example.com"]
    assert (config.max_pages, config.max_depth, config.max_concurrent) == (42, 3, 7)
    assert config.requests_per_second == 2.5  # rate_limit — привычный синоним
    assert config.respect_robots is True
    assert config.include == ["/blog"]
    assert config.storage == ["jsonl", "sqlite"]
    assert config.log_level == "DEBUG"  # уровень приводится к верхнему регистру
    assert config.report_html == "data/report.html"
    assert config.source.endswith("c.yaml")


def test_json_config_reads_the_same(tmp_path):
    target = write(
        tmp_path,
        "c.json",
        json.dumps({"crawler": {"urls": ["https://example.com"], "max_pages": 5}}),
    )

    config = CrawlerConfig.from_file(target)

    assert config.start_urls == ["https://example.com"]
    assert config.max_pages == 5


def test_flat_file_without_sections_works(tmp_path):
    target = write(
        tmp_path, "flat.yaml", "start_urls: [https://example.com]\nmax_pages: 3\n"
    )

    assert CrawlerConfig.from_file(target).max_pages == 3


def test_output_is_both_section_and_setting(tmp_path):
    """`output: файл` — путь к результатам, `output:` со словарём — раздел."""
    as_value = CrawlerConfig.from_dict({**MINIMAL, "output": "data/pages.json"})
    as_section = CrawlerConfig.from_dict({**MINIMAL, "output": {"output": "data/p.json"}})

    assert as_value.output == "data/pages.json"
    assert as_section.output == "data/p.json"


def test_empty_file_gives_defaults_but_needs_urls(tmp_path):
    target = write(tmp_path, "empty.yaml", "# всё закомментировано\n")

    with pytest.raises(ConfigError, match="start_urls"):
        CrawlerConfig.from_file(target)


def test_missing_file_reports_path(tmp_path):
    with pytest.raises(ConfigError, match="не найден"):
        CrawlerConfig.from_file(tmp_path / "нет.yaml")


def test_unknown_extension_rejected(tmp_path):
    target = write(tmp_path, "c.ini", "[crawler]\n")

    with pytest.raises(ConfigError, match="расширение"):
        CrawlerConfig.from_file(target)


def test_broken_yaml_reports_file(tmp_path):
    target = write(tmp_path, "c.yaml", "crawler:\n  max_pages: [1,\n")

    with pytest.raises(ConfigError, match="не удалось разобрать"):
        CrawlerConfig.from_file(target)


def test_non_dict_config_rejected():
    with pytest.raises(ConfigError, match="словарём"):
        CrawlerConfig.from_dict(["https://example.com"])


def test_section_with_wrong_type_rejected():
    with pytest.raises(ConfigError, match="раздел 'crawler'"):
        CrawlerConfig.from_dict({"crawler": 5})


def test_empty_section_is_allowed():
    config = CrawlerConfig.from_dict({**MINIMAL, "politeness": None})

    assert config.min_delay == 0.0


def test_duplicate_setting_rejected():
    data = {"start_urls": ["https://example.com"], "crawler": {"start_urls": ["https://b"]}}

    with pytest.raises(ConfigError, match="дважды"):
        CrawlerConfig.from_dict(data)


# --- проверка ключей и значений ---------------------------------------------


def test_unknown_key_is_error_with_hint():
    with pytest.raises(ConfigError, match="max_pages"):
        CrawlerConfig.from_dict({**MINIMAL, "max_page": 5})


def test_unknown_key_without_similar_name():
    with pytest.raises(ConfigError, match="неизвестная настройка"):
        CrawlerConfig.from_dict({**MINIMAL, "турбо_режим": True})


@pytest.mark.parametrize(
    "alias,name,value,expected",
    [
        ("urls", "start_urls", ["https://a.example"], ["https://a.example"]),
        ("rate_limit", "requests_per_second", 1.0, 1.0),
        ("rps", "requests_per_second", 1.0, 1.0),
        ("sitemap", "sitemaps", "https://a.example/s.xml", ["https://a.example/s.xml"]),
        ("include_patterns", "include", ["/a"], ["/a"]),
        ("exclude_patterns", "exclude", ["/b"], ["/b"]),
        ("same_domain_only", "same_domain", True, True),
        ("user_agents", "rotate_ua", ["Bot/1.0"], ["Bot/1.0"]),
        ("retry_backoff_factor", "retry_backoff", 3.0, 3.0),
    ],
)
def test_aliases_write_into_canonical_setting(alias, name, value, expected):
    data = {"start_urls": ["https://example.com"], alias: value}
    if alias == "urls":
        data.pop("start_urls")  # это тот же ключ, и дубль теперь запрещён
    config = CrawlerConfig.from_dict(data)

    assert getattr(config, name) == expected


@pytest.mark.parametrize(
    "data,message",
    [
        ({"start_urls": ["https://a.example"], "urls": ["https://b.example"]}, "start_urls"),
        ({"urls": ["https://a.example"], "rate_limit": 1, "rps": 2}, "requests_per_second"),
    ],
)
def test_same_setting_under_two_names_rejected(data: dict, message: str):
    """Синоним — то же поле: молча взять последнее значение нельзя."""
    with pytest.raises(ConfigError, match=f"{message}. задана дважды"):
        CrawlerConfig.from_dict(data)


@pytest.mark.parametrize("key", ["output", "report_html", "report_json", "log_file"])
def test_directory_instead_of_file_rejected(tmp_path, key: str):
    """Узнать, что отчёт некуда писать, нужно до обхода, а не после."""
    with pytest.raises(ConfigError, match="каталог"):
        CrawlerConfig.from_dict({**MINIMAL, key: str(tmp_path)})


def test_unreadable_config_reports_path(tmp_path):
    """Каталог вместо файла — ошибка конфигурации, а не трейсбек."""
    with pytest.raises(ConfigError, match="не удалось прочитать"):
        CrawlerConfig.from_file(tmp_path)


def test_single_string_instead_of_list():
    config = CrawlerConfig.from_dict({"start_urls": "https://example.com"})

    assert config.start_urls == ["https://example.com"]


@pytest.mark.parametrize("value", ["true", "YES", "on", "1", True])
def test_bool_from_text(value):
    assert CrawlerConfig.from_dict({**MINIMAL, "respect_robots": value}).respect_robots


@pytest.mark.parametrize("value", ["false", "NO", "off", "0", False])
def test_bool_false_from_text(value):
    assert not CrawlerConfig.from_dict({**MINIMAL, "respect_robots": value}).respect_robots


@pytest.mark.parametrize("value", ["может быть", 2, None])
def test_bad_bool_rejected(value):
    with pytest.raises(ConfigError, match="true или false"):
        CrawlerConfig.from_dict({**MINIMAL, "respect_robots": value})


@pytest.mark.parametrize("value", ["много", True, [5]])
def test_bad_int_rejected(value):
    with pytest.raises(ConfigError, match="целым числом"):
        CrawlerConfig.from_dict({**MINIMAL, "max_pages": value})


def test_int_from_string():
    assert CrawlerConfig.from_dict({**MINIMAL, "max_pages": "7"}).max_pages == 7


def test_list_of_wrong_type_rejected():
    with pytest.raises(ConfigError, match="списком строк"):
        CrawlerConfig.from_dict({"start_urls": {"a": 1}})


@pytest.mark.parametrize(
    "key,value,message",
    [
        ("max_pages", 0, ">= 1"),
        ("max_concurrent", 0, ">= 1"),
        ("max_depth", -1, ">= 0"),
        ("max_retries", -1, ">= 0"),
        ("max_per_domain", 0, ">= 1"),
        ("batch_size", 0, ">= 1"),
        ("min_delay", -1, "отрицательным"),
        ("rate_limit", -1, "отрицательным"),
        ("retry_backoff", 0.5, ">= 1"),
        ("timeout_growth", 0.5, ">= 1"),
        ("log_level", "БОЛТЛИВЫЙ", "уровень"),
        ("storage", ["parquet"], "форматы"),
    ],
)
def test_bad_values_rejected(key: str, value, message: str):
    with pytest.raises(ConfigError, match=message):
        CrawlerConfig.from_dict({**MINIMAL, key: value})


def test_nothing_to_crawl_rejected():
    with pytest.raises(ConfigError, match="обходить нечего"):
        CrawlerConfig.from_dict({"max_pages": 5})


def test_sitemaps_alone_are_enough():
    config = CrawlerConfig.from_dict({"sitemaps": ["https://example.com/sitemap.xml"]})

    assert config.start_urls == []


def test_sitemap_auto_needs_start_urls():
    with pytest.raises(ConfigError, match="auto"):
        CrawlerConfig.from_dict({"sitemaps": ["auto"]})


def test_zero_rate_limit_means_no_limit():
    assert CrawlerConfig.from_dict({**MINIMAL, "rate_limit": 0}).requests_per_second is None


# --- provided и слияние с флагами -------------------------------------------


def test_provided_lists_only_keys_from_file():
    config = CrawlerConfig.from_dict({**MINIMAL, "max_pages": 5})

    assert config.provided == frozenset({"start_urls", "max_pages"})
    assert "max_depth" not in config.provided  # осталось значением по умолчанию


def test_provided_uses_canonical_names():
    config = CrawlerConfig.from_dict({"urls": ["https://example.com"], "rate_limit": 1})

    assert config.provided == frozenset({"start_urls", "requests_per_second"})


# --- перевод в аргументы ----------------------------------------------------


def test_to_crawler_kwargs_translates_names():
    config = CrawlerConfig.from_dict(
        {**MINIMAL, "rate_limit": 2, "global_rate": True, "retry_backoff_factor": 3}
    )

    kwargs = config.to_crawler_kwargs()
    assert kwargs["requests_per_second"] == 2.0
    assert kwargs["per_domain_rate"] is False
    assert kwargs["retry_backoff_factor"] == 3.0
    assert "rate_limit" not in kwargs


def test_to_crawler_kwargs_fit_crawler_signature():
    """Самое ценное свойство: эти аргументы действительно принимает краулер."""
    import inspect

    from src.crawler import AsyncCrawler

    accepted = set(inspect.signature(AsyncCrawler.__init__).parameters)
    assert set(CrawlerConfig.from_dict(MINIMAL).to_crawler_kwargs()) <= accepted


def test_to_crawl_kwargs_fit_crawl_signature():
    import inspect

    from src.crawler import AsyncCrawler

    accepted = set(inspect.signature(AsyncCrawler.crawl).parameters)
    assert set(CrawlerConfig.from_dict(MINIMAL).to_crawl_kwargs()) <= accepted


def test_empty_filters_become_none():
    kwargs = CrawlerConfig.from_dict(MINIMAL).to_crawl_kwargs()

    assert kwargs["include_patterns"] is None
    assert kwargs["exclude_patterns"] is None


def test_to_dict_covers_all_settings():
    data = CrawlerConfig.from_dict(MINIMAL).to_dict()

    assert set(data) == set(CrawlerConfig.field_names())
    assert "provided" not in data and "source" not in data


# --- сборка хранилищ --------------------------------------------------------


def test_build_storage_without_formats_is_none():
    assert build_storage([]) is None


@pytest.mark.parametrize(
    "fmt,expected,suffix",
    [
        ("json", JSONStorage, ".json"),
        ("jsonl", JSONStorage, ".jsonl"),
        ("csv", CSVStorage, ".csv"),
        ("sqlite", SQLiteStorage, ".db"),
    ],
)
def test_build_storage_one_format(tmp_path, fmt: str, expected, suffix: str):
    storage = build_storage([fmt], prefix=str(tmp_path / "run"))

    assert isinstance(storage, expected)
    assert str(storage.target).startswith(str(tmp_path / f"run{suffix}"))


def test_build_storage_several_formats_wrapped(tmp_path):
    storage = build_storage(["json", "csv", "json"], prefix=str(tmp_path / "run"))

    assert isinstance(storage, MultiStorage)
    assert len(storage.storages) == 2  # повтор формата отброшен


def test_config_build_storage_uses_its_own_settings(tmp_path):
    config = CrawlerConfig.from_dict(
        {**MINIMAL, "storage": ["jsonl"], "storage_path": str(tmp_path / "run"),
         "batch_size": 7}
    )

    storage = config.build_storage()

    assert isinstance(storage, JSONStorage)
    assert storage.batch_size == 7


def test_example_config_is_valid():
    """Пример из репозитория должен читаться без ошибок."""
    config = CrawlerConfig.from_file("config.example.yaml")

    assert config.start_urls
    assert config.max_pages >= 1
