"""Тесты хранилищ данных: JSON, CSV, SQLite и MultiStorage (День 6)."""

from __future__ import annotations

import asyncio
import csv
import json
import sqlite3
from datetime import datetime
from typing import Any

import pytest

from src.errors import StorageDataError, StorageError, storage_error
from src.models import PageRecord
from src.storage import (
    CSVStorage,
    DataStorage,
    JSONStorage,
    MultiStorage,
    SQLiteStorage,
)
from src.utils import save_crawl_results

# Текст, на котором ломаются самодельные сериализаторы: кавычки, запятая,
# точка с запятой и перевод строки внутри одного поля.
NASTY_TEXT = 'цитата: "жизнь, как она есть";\nвторая строка'


def page(url: str, **extra: Any) -> dict[str, Any]:
    """Словарь страницы в том виде, в каком его отдаёт краулер."""
    data = {
        "url": url,
        "title": "Заголовок",
        "text": "немного текста",
        "links": [f"{url}/next"],
        "metadata": {"description": "описание"},
        "status": 200,
        "content_type": "text/html",
        "final_url": url,
        "ok": True,
        "depth": 0,
        "attempts": 1,
        "elapsed": 0.1,
        "fetched_at": datetime(2026, 10, 11, 12, 30, 45),
    }
    data.update(extra)
    return data


class Flaky(DataStorage):
    """Хранилище, которое падает первые `fails` попыток записи."""

    def __init__(self, fails: int = 1, *, error: Exception | None = None, **kwargs):
        kwargs.setdefault("max_retries", 2)
        super().__init__(**kwargs)
        self.fails = fails
        self.error = error or OSError("на диске нет места")
        self.attempts = 0
        self.written: list[PageRecord] = []

    @property
    def target(self) -> str:
        return "flaky"

    async def _open(self) -> None:
        return None

    async def _write(self, batch: list[PageRecord]) -> None:
        self.attempts += 1
        if self.attempts <= self.fails:
            raise self.error
        self.written.extend(batch)

    async def load(self, limit: int | None = None) -> list[dict[str, Any]]:
        records = [record.as_dict() for record in self.written]
        return records[:limit] if limit is not None else records


class FlakyHandle:
    """Обёртка над дескриптором файла, заваливающая первые `fails` записей."""

    def __init__(self, handle: Any, fails: int = 1) -> None:
        self._handle = handle
        self.fails = fails
        self.writes = 0
        self.closed = False

    async def write(self, data: str) -> None:
        self.writes += 1
        if self.writes <= self.fails:
            raise OSError("на диске нет места")
        await self._handle.write(data)

    async def flush(self) -> None:
        await self._handle.flush()

    async def close(self) -> None:
        self.closed = True
        await self._handle.close()


class FlakyJSON(JSONStorage):
    """JSON-хранилище, у которого первые записи в файл не удаются."""

    def __init__(self, *args: Any, fails: int = 1, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.fails = fails

    async def _open(self) -> None:
        await super()._open()
        self._handle = FlakyHandle(self._handle, fails=self.fails)


# --- стандартная форма записи ----------------------------------------------


def test_record_keeps_the_agreed_shape():
    record = PageRecord.from_page(page("http://a/1"))

    assert record.url == "http://a/1"
    assert record.status_code == 200  # в странице это поле зовётся status
    assert record.crawled_at == datetime(2026, 10, 11, 12, 30, 45)
    assert record.links == ["http://a/1/next"]
    # Первые восемь полей — договор дня 6, и порядок у них фиксирован.
    assert PageRecord.field_names()[:8] == [
        "url",
        "title",
        "text",
        "links",
        "metadata",
        "crawled_at",
        "status_code",
        "content_type",
    ]


def test_record_without_url_is_rejected():
    with pytest.raises(ValueError):
        PageRecord.from_page({"title": "без адреса"})


def test_long_text_is_trimmed_on_request():
    record = PageRecord.from_page(page("http://a/1", text="x" * 100), text_limit=10)

    assert record.text == "x" * 10


def test_crawl_dump_writes_time_like_the_storages(tmp_path):
    """Дамп дня 3 и хранилища дня 6 должны писать время одинаково."""
    target = save_crawl_results([page("http://a/1")], {}, tmp_path / "dump.json")

    dump = json.loads(target.read_text(encoding="utf-8"))
    assert dump["pages"][0]["fetched_at"] == "2026-10-11T12:30:45"


# --- JSON -------------------------------------------------------------------


async def test_json_writes_a_valid_array(tmp_path):
    target = tmp_path / "pages.json"

    async with JSONStorage(target) as storage:
        await storage.save(page("http://a/1"))
        await storage.save(page("http://a/2", title=NASTY_TEXT))

    data = json.loads(target.read_text(encoding="utf-8"))
    assert [item["url"] for item in data] == ["http://a/1", "http://a/2"]
    assert data[1]["title"] == NASTY_TEXT
    # Дата сохраняется в ISO: такую строку читают и Python, и SQLite.
    assert data[0]["crawled_at"] == "2026-10-11T12:30:45"


async def test_empty_json_file_is_still_valid(tmp_path):
    target = tmp_path / "pages.json"

    async with JSONStorage(target):
        pass

    assert json.loads(target.read_text()) == []


async def test_json_can_be_read_before_the_array_is_closed(tmp_path):
    """Обход ещё идёт, скобки нет — но записанное уже должно читаться."""
    storage = JSONStorage(tmp_path / "pages.json")
    await storage.save(page("http://a/1"))

    rows = await storage.load()

    assert [row["url"] for row in rows] == ["http://a/1"]
    await storage.close()


async def test_jsonl_appends_between_runs(tmp_path):
    target = tmp_path / "pages.jsonl"

    async with JSONStorage(target, lines=True) as first:
        await first.save(page("http://a/1"))
    async with JSONStorage(target, lines=True) as second:
        await second.save(page("http://a/2"))
        rows = await second.load()

    assert [row["url"] for row in rows] == ["http://a/1", "http://a/2"]
    # Каждая запись — ровно одна строка: иначе файл не поток документов.
    assert len(target.read_text(encoding="utf-8").strip().splitlines()) == 2


async def test_broken_jsonl_line_does_not_hide_the_rest(tmp_path):
    """Процесс мог упасть на середине строки — остальные записи всё равно нужны."""
    target = tmp_path / "pages.jsonl"
    async with JSONStorage(target, lines=True) as storage:
        await storage.save(page("http://a/1"))
    with target.open("a", encoding="utf-8") as handle:
        handle.write('{"url": "http://a/2", "tit')

    rows = await JSONStorage(target, lines=True).load()

    assert [row["url"] for row in rows] == ["http://a/1"]


async def test_json_rewrites_the_file_on_open(tmp_path):
    """Массив дописать нельзя: закрывающая скобка уже стоит на месте."""
    target = tmp_path / "pages.json"
    async with JSONStorage(target) as first:
        await first.save(page("http://a/1"))

    async with JSONStorage(target) as second:
        await second.save(page("http://a/2"))

    assert [item["url"] for item in json.loads(target.read_text())] == ["http://a/2"]


async def test_json_array_stays_valid_after_a_retried_write(tmp_path):
    """Первая запись упала и повторилась — запятая не должна уехать в начало."""
    target = tmp_path / "pages.json"

    async with FlakyJSON(target, fails=1) as storage:
        assert await storage.save(page("http://a/1")) is True
        assert await storage.save(page("http://a/2")) is True

    data = json.loads(target.read_text(encoding="utf-8"))
    assert [item["url"] for item in data] == ["http://a/1", "http://a/2"]


async def test_file_is_released_even_if_the_array_is_not_closed(tmp_path):
    """Закрывающая скобка не записалась — дескриптор всё равно отпускаем."""
    storage = FlakyJSON(tmp_path / "pages.json", fails=0)
    await storage.save(page("http://a/1"))
    handle = storage._handle
    handle.fails = handle.writes + 1  # завалить запись скобки

    await storage.close()

    assert handle.closed
    assert storage._handle is None


# --- CSV --------------------------------------------------------------------


async def test_csv_escapes_special_characters(tmp_path):
    target = tmp_path / "pages.csv"

    async with CSVStorage(target) as storage:
        await storage.save(page("http://a/1", title=NASTY_TEXT))
        rows = await storage.load()

    assert rows[0]["title"] == NASTY_TEXT
    # Проверка независимым читателем: экранирование должно быть стандартным.
    with target.open(newline="", encoding="utf-8") as handle:
        plain = list(csv.DictReader(handle))
    assert plain[0]["title"] == NASTY_TEXT


async def test_csv_restores_types_on_read(tmp_path):
    async with CSVStorage(tmp_path / "pages.csv") as storage:
        await storage.save(page("http://a/1", depth=2, elapsed=0.25))
        await storage.save(page("http://a/2", ok=False, status=None, error="timeout"))
        rows = await storage.load()

    assert rows[0]["links"] == ["http://a/1/next"]
    assert rows[0]["metadata"] == {"description": "описание"}
    assert rows[0]["depth"] == 2
    assert rows[0]["elapsed"] == 0.25
    assert rows[0]["ok"] is True
    assert rows[1]["ok"] is False
    assert rows[1]["status_code"] is None


async def test_csv_header_is_written_once(tmp_path):
    target = tmp_path / "pages.csv"

    async with CSVStorage(target) as first:
        await first.save(page("http://a/1"))
    async with CSVStorage(target) as second:
        await second.save(page("http://a/2"))
        rows = await second.load()

    assert [row["url"] for row in rows] == ["http://a/1", "http://a/2"]
    assert target.read_text(encoding="utf-8").count("url,title,text") == 1


async def test_csv_supports_other_encodings(tmp_path):
    target = tmp_path / "pages.csv"

    async with CSVStorage(
        target, encoding="cp1251", errors="replace", delimiter=";"
    ) as storage:
        await storage.save(page("http://a/1", title="Привет, 日本"))
        rows = await storage.load()

    text = target.read_text(encoding="cp1251")
    assert "Привет" in text
    # Иероглифов в cp1251 нет, и запись не должна из-за них падать.
    assert rows[0]["title"] == "Привет, ??"


async def test_csv_refuses_a_file_with_another_structure(tmp_path):
    """Дописать строки с другим разделителем — значит испортить файл молча."""
    target = tmp_path / "pages.csv"
    async with CSVStorage(target, delimiter=";") as first:
        await first.save(page("http://a/1"))

    second = CSVStorage(target)  # разделитель по умолчанию — запятая
    assert await second.save(page("http://a/2")) is False
    await second.close()

    stats = second.get_stats()
    assert stats["failed"] == 1
    assert stats["retries"] == 0  # повтор здесь не поможет
    assert "другой структурой" in stats["last_error"]
    assert [row["url"] for row in await CSVStorage(target, delimiter=";").load()] == [
        "http://a/1"
    ]


# --- SQLite -----------------------------------------------------------------


async def test_sqlite_creates_table_and_indexes(tmp_path):
    target = tmp_path / "pages.db"
    storage = SQLiteStorage(target)

    await storage.init_db()
    await storage.close()

    with sqlite3.connect(target) as db:
        tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        indexes = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='index'")}
    assert "pages" in tables
    assert {"idx_pages_crawled_at", "idx_pages_status_code"} <= indexes


async def test_sqlite_writes_and_reads_back(tmp_path):
    async with SQLiteStorage(tmp_path / "pages.db", batch_size=1) as storage:
        await storage.save(page("http://a/1", title=NASTY_TEXT))
        rows = await storage.load()

    assert rows[0]["title"] == NASTY_TEXT
    assert rows[0]["links"] == ["http://a/1/next"]
    assert rows[0]["metadata"] == {"description": "описание"}
    # В базе логическое поле лежит числом, наружу должно возвращаться bool.
    assert rows[0]["ok"] is True


async def test_sqlite_waits_for_the_batch(tmp_path):
    storage = SQLiteStorage(tmp_path / "pages.db", batch_size=3)
    await storage.init_db()

    for number in range(2):
        await storage.save(page(f"http://a/{number}"))
    assert await storage.count() == 0  # батч ещё не набрался

    await storage.save(page("http://a/2"))
    assert await storage.count() == 3
    assert storage.get_stats()["batches"] == 1  # одна вставка на три страницы
    await storage.close()


async def test_sqlite_updates_the_same_url(tmp_path):
    """Второй обход должен обновлять страницу, а не плодить копии."""
    async with SQLiteStorage(tmp_path / "pages.db", batch_size=1) as storage:
        await storage.save(page("http://a/1", title="Старый"))
        await storage.save(page("http://a/1", title="Новый"))
        rows = await storage.load()

    assert len(rows) == 1
    assert rows[0]["title"] == "Новый"


async def test_sqlite_search_and_count(tmp_path):
    async with SQLiteStorage(tmp_path / "pages.db", batch_size=1) as storage:
        await storage.save(page("http://a/1", text="цитата про сон"))
        await storage.save(page("http://a/2", text="совсем другое"))

        assert await storage.count() == 2
        assert [row["url"] for row in await storage.search("цитата")] == ["http://a/1"]


async def test_sqlite_search_treats_wildcards_as_text(tmp_path):
    """«%» в запросе — это символ процента, а не «что угодно»."""
    async with SQLiteStorage(tmp_path / "pages.db", batch_size=1) as storage:
        await storage.save(page("http://a/1", title="Скидка 100% на всё"))
        await storage.save(page("http://a/2", title="Обычный заголовок"))

        # Без экранирования «%» был бы шаблоном и нашёл бы обе страницы.
        assert [row["url"] for row in await storage.search("%")] == ["http://a/1"]
        assert [row["url"] for row in await storage.search("100%")] == ["http://a/1"]
        assert await storage.search("_") == []


async def test_sqlite_reads_after_close(tmp_path):
    """База остаётся на диске: проверять записанное после close — обычное дело."""
    storage = SQLiteStorage(tmp_path / "pages.db", batch_size=1)
    await storage.save(page("http://a/1"))
    await storage.close()

    assert await storage.count() == 1
    assert len(await storage.load(limit=1)) == 1


async def test_reading_a_database_that_does_not_exist_yet(tmp_path):
    """Писать ещё не начинали — чтение отвечает пустотой, а не ошибкой."""
    storage = SQLiteStorage(tmp_path / "pages.db")

    assert await storage.count() == 0
    assert await storage.load() == []
    assert await storage.search("что-нибудь") == []
    assert not (tmp_path / "pages.db").exists()


async def test_sqlite_schema_covers_every_field(tmp_path):
    """Поле добавили в запись, а в схему забыли — это должно быть видно сразу."""
    storage = SQLiteStorage(tmp_path / "pages.db")

    assert set(storage.columns) == set(PageRecord.field_names())


def test_sqlite_rejects_strange_table_name(tmp_path):
    with pytest.raises(ValueError):
        SQLiteStorage(tmp_path / "pages.db", table="pages; DROP TABLE pages")


# --- батчи, буфер и конкурентность -----------------------------------------


async def test_batch_keeps_records_until_it_fills(tmp_path):
    target = tmp_path / "pages.jsonl"
    storage = JSONStorage(target, lines=True, batch_size=3)

    await storage.save(page("http://a/1"))
    await storage.save(page("http://a/2"))
    assert storage.get_stats()["buffered"] == 2
    assert not target.exists() or target.stat().st_size == 0

    await storage.save(page("http://a/3"))
    assert storage.get_stats()["buffered"] == 0
    assert len(await storage.load()) == 3
    await storage.close()


async def test_close_writes_the_unfinished_batch(tmp_path):
    target = tmp_path / "pages.jsonl"
    storage = JSONStorage(target, lines=True, batch_size=10)
    await storage.save(page("http://a/1"))

    await storage.close()

    assert len(await storage.load()) == 1
    assert storage.get_stats()["buffered"] == 0


async def test_flush_does_not_wait_for_the_batch(tmp_path):
    storage = JSONStorage(tmp_path / "pages.jsonl", lines=True, batch_size=10)
    await storage.save(page("http://a/1"))

    assert await storage.flush() is True

    assert len(await storage.load()) == 1
    await storage.close()


async def test_concurrent_saves_do_not_mix_records(tmp_path):
    """Страницы сохраняют несколько воркеров сразу, а файл один."""
    target = tmp_path / "pages.jsonl"
    async with JSONStorage(target, lines=True) as storage:
        await asyncio.gather(
            *(storage.save(page(f"http://a/{number}")) for number in range(50))
        )

    rows = await JSONStorage(target, lines=True).load()
    assert len(rows) == 50
    assert len({row["url"] for row in rows}) == 50


async def test_saving_after_close_is_refused(tmp_path):
    storage = JSONStorage(tmp_path / "pages.jsonl", lines=True)
    await storage.close()

    assert await storage.save(page("http://a/1")) is False
    assert storage.get_stats()["failed"] == 1


def test_batch_size_must_be_positive(tmp_path):
    with pytest.raises(ValueError):
        JSONStorage(tmp_path / "pages.json", batch_size=0)


# --- ошибки записи ----------------------------------------------------------


async def test_write_error_is_retried():
    storage = Flaky(fails=1, max_retries=2)

    assert await storage.save(page("http://a/1")) is True

    assert storage.attempts == 2
    assert storage.get_stats()["retries"] == 1
    assert storage.get_stats()["failed"] == 0


async def test_write_error_does_not_raise():
    """Упавшая запись не должна рвать обход: страница уже загружена."""
    storage = Flaky(fails=99)

    assert await storage.save(page("http://a/1")) is False

    stats = storage.get_stats()
    assert stats["failed"] == 1
    assert stats["saved"] == 0
    assert "места" in stats["last_error"]


async def test_broken_record_is_not_retried():
    """Данные, которые не сериализуются, второй раз сериализоваться не начнут."""
    storage = Flaky(fails=99, error=TypeError("объект не переводится в JSON"))

    assert await storage.save(page("http://a/1")) is False

    assert storage.attempts == 1
    assert storage.get_stats()["retries"] == 0


async def test_unsuitable_data_never_reaches_the_buffer():
    storage = Flaky(fails=0)

    assert await storage.save("просто строка") is False

    assert storage.attempts == 0
    assert storage.get_stats()["buffered"] == 0


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        (OSError("диск"), StorageError),
        (TypeError("не сериализуется"), StorageDataError),
        (ValueError("плохое значение"), StorageDataError),
    ],
)
def test_storage_errors_are_classified(exc, expected):
    assert isinstance(storage_error(exc), expected)


# --- несколько форматов сразу ----------------------------------------------


async def test_multi_storage_writes_every_format(tmp_path):
    multi = MultiStorage(
        [
            JSONStorage(tmp_path / "pages.json"),
            CSVStorage(tmp_path / "pages.csv"),
            SQLiteStorage(tmp_path / "pages.db", batch_size=2),
        ]
    )

    async with multi:
        await multi.save(page("http://a/1"))
        await multi.save(page("http://a/2"))

    assert len(json.loads((tmp_path / "pages.json").read_text())) == 2
    assert await SQLiteStorage(tmp_path / "pages.db").count() == 2
    assert len(await CSVStorage(tmp_path / "pages.csv").load()) == 2


async def test_failing_format_does_not_stop_the_others(tmp_path):
    broken = Flaky(fails=99)
    multi = MultiStorage([JSONStorage(tmp_path / "pages.json"), broken])

    async with multi:
        assert await multi.save(page("http://a/1")) is True

    assert len(json.loads((tmp_path / "pages.json").read_text())) == 1
    stats = multi.get_stats()
    assert stats["saved"] == 1  # запись роздана
    assert stats["failed"] == 1  # и в одном формате не прижилась
    assert [child["storage"] for child in stats["storages"]] == [
        "JSONStorage",
        "Flaky",
    ]


async def test_nested_storage_keeps_its_own_text_limit(tmp_path):
    """Запись строится один раз на все форматы, но обрезка у каждого своя."""
    short = JSONStorage(tmp_path / "short.jsonl", lines=True, text_limit=5)
    full = JSONStorage(tmp_path / "full.jsonl", lines=True)

    async with MultiStorage([short, full]) as multi:
        await multi.save(page("http://a/1", text="x" * 50))

    assert (await short.load())[0]["text"] == "x" * 5
    assert (await full.load())[0]["text"] == "x" * 50


async def test_multi_storage_needs_a_format():
    with pytest.raises(ValueError):
        MultiStorage([])


async def test_multi_storage_reads_from_the_first(tmp_path):
    multi = MultiStorage(
        [JSONStorage(tmp_path / "pages.json"), CSVStorage(tmp_path / "pages.csv")]
    )

    async with multi:
        await multi.save(page("http://a/1"))
        rows = await multi.load()

    assert [row["url"] for row in rows] == ["http://a/1"]
