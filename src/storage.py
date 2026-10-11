"""Асинхронное сохранение собранных данных: JSON, CSV и SQLite (день 6).

Все хранилища устроены одинаково: страница превращается в `PageRecord`,
попадает в буфер и уходит на диск батчем. Отличается только `_write` — то, как
именно формат кладёт записи. Поэтому буферизация, блокировка, повторы при сбоях
записи и статистика живут в одном месте, в `DataStorage`.

Главное правило дня: сохранение не имеет права сорвать обход. `save()` не
поднимает исключений — неудача попадает в лог и в счётчик `failed`, а краулер
продолжает собирать страницы.
"""

from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import textwrap
import time
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Iterable
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from types import TracebackType
from typing import Any

import aiofiles
import aiosqlite

from src.errors import CrawlerError, StorageDataError, StorageError, storage_error
from src.models import PageRecord
from src.retry import RetryStrategy
from src.utils import json_default

logger = logging.getLogger(__name__)

# Повторы записи настроены жёстче сетевых: диск либо освобождается сразу, либо
# не освобождается вовсе, и держать страницу в памяти минутами смысла нет.
STORAGE_MAX_RETRIES = 2
STORAGE_BASE_DELAY = 0.2
# Символ экранирования для LIKE: в SQL он не задан по умолчанию, его объявляют
# в самом запросе через ESCAPE.
LIKE_ESCAPE = "\\"

# Схема таблицы SQLite: столбцы повторяют поля `PageRecord`, плюс свой `id`.
# Списки и словари лежат JSON-строками в TEXT — реляционно разложить ссылки по
# отдельной таблице было бы честнее, но день 6 про сохранение, а не про схему.
_SCHEMA: dict[str, str] = {
    "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
    # UNIQUE нужен не только от дубликатов: на него опирается UPSERT по URL.
    "url": "TEXT NOT NULL UNIQUE",
    "title": "TEXT",
    "text": "TEXT",
    "links": "TEXT",
    "metadata": "TEXT",
    "crawled_at": "TEXT",
    "status_code": "INTEGER",
    "content_type": "TEXT",
    "final_url": "TEXT",
    "depth": "INTEGER",
    "ok": "INTEGER",
    "error": "TEXT",
    "error_class": "TEXT",
    "attempts": "INTEGER",
    "elapsed": "REAL",
}
# Индексы под обычные вопросы к собранным данным: что собрали за последний час
# и какие страницы отдали ошибку. По URL индекс уже есть — он UNIQUE.
_INDEXES = ("crawled_at", "status_code")


def as_cell(value: Any) -> Any:
    """Готовит значение к записи в плоскую ячейку CSV или столбец базы.

    Ни CSV, ни SQLite не умеют хранить списки и словари, поэтому ссылки и
    метаданные кладутся JSON-строкой: так они переживают запись и читаются
    обратно одним `json.loads`, а не разбором собственного формата.
    """
    if isinstance(value, datetime):
        return value.isoformat(timespec="seconds")
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False, default=json_default)
    return value


# Какой тип был у поля до записи в плоскую ячейку: ни CSV, ни SQLite типов не
# хранят, а читать ссылки строкой «["a", "b"]» бессмысленно.
_CELL_TYPES = {
    "links": "json",
    "metadata": "json",
    "status_code": "int",
    "depth": "int",
    "attempts": "int",
    "elapsed": "float",
    "ok": "bool",
}


def _escape_like(text: str) -> str:
    """Экранирует подстановочные знаки LIKE в поисковом запросе.

    Без этого поиск «100%» означал бы «100 и что угодно», а поиск по одному
    «%» возвращал бы вообще все страницы.
    """
    for char in (LIKE_ESCAPE, "%", "_"):
        text = text.replace(char, f"{LIKE_ESCAPE}{char}")
    return text


def _restore(name: str, value: Any) -> Any:
    """Возвращает прочитанной ячейке исходный тип поля.

    Из CSV всё приходит строками, из SQLite — числами, поэтому значение может
    быть уже готовым: приводить к типу нужно только то, что его потеряло.
    """
    kind = _CELL_TYPES.get(name)
    if kind is None or value is None:
        return value
    if kind == "bool" and isinstance(value, int):
        # В базе логическое поле лежит числом: SQLite своего типа для него нет.
        return bool(value)
    if not isinstance(value, str):
        return value
    if value == "":
        # Пустая ячейка — это None, а не нуль: статуса у сетевой ошибки нет.
        return [] if name == "links" else None

    try:
        if kind == "json":
            return json.loads(value)
        if kind == "int":
            return int(value)
        if kind == "float":
            return float(value)
        if kind == "bool":
            return value not in ("0", "False", "false")
    except ValueError:
        logger.debug("Ячейка %s не разобралась: %r", name, value)
    return value


class DataStorage(ABC):
    """Базовое хранилище: буфер, повторы записи и статистика.

    Наследник описывает три вещи: `_open` (куда писать), `_write` (как писать
    батч) и `load` (как прочитать записанное обратно).
    """

    def __init__(
        self,
        *,
        batch_size: int = 1,
        text_limit: int | None = None,
        retry_strategy: RetryStrategy | None = None,
        max_retries: int | None = None,
    ) -> None:
        if batch_size < 1:
            raise ValueError("batch_size должен быть >= 1")
        if retry_strategy is not None and max_retries is not None:
            logger.warning(
                "Передана готовая RetryStrategy: настройка max_retries не применяется"
            )

        self.batch_size = batch_size
        # Текст страницы может быть огромным, а в базе и CSV он лежит целиком.
        self.text_limit = text_limit
        self.retry_strategy = retry_strategy or RetryStrategy(
            STORAGE_MAX_RETRIES if max_retries is None else max_retries,
            base_delay=STORAGE_BASE_DELAY,
            # Повторять стоит внешние обстоятельства (диск, блокировка базы), а
            # не сами данные: несериализуемая запись такой и останется.
            retry_on=(StorageError,),
        )

        self._buffer: list[PageRecord] = []
        # Страницы сохраняют несколько воркеров одновременно, а файл и буфер
        # одни. Блокировка держится и на время записи: иначе два батча
        # перемешались бы в одном файле.
        self._lock = asyncio.Lock()
        self._opened = False
        self._closed = False

        self.saved = 0
        self.failed = 0
        self.batches = 0
        self.flushes = 0
        self.write_time = 0.0
        self.last_error: str | None = None

    @property
    def name(self) -> str:
        return type(self).__name__

    @property
    def target(self) -> str:
        """Куда пишем — путь к файлу или базе; идёт в логи и статистику."""
        return ""

    @property
    def closed(self) -> bool:
        return self._closed

    async def save(self, data: dict[str, Any] | PageRecord) -> bool:
        """Сохраняет одну страницу. Возвращает `False`, если не получилось.

        Исключений не поднимает намеренно: страница уже загружена, и терять
        из-за проблем с диском весь обход было бы обидно.

        При `batch_size` больше единицы `True` означает «запись принята»: она
        может ещё лежать в буфере, и о сбое её записи станет известно из
        результата `flush()` или `close()` и из статистики.
        """
        if self._closed:
            logger.warning("%s уже закрыто, запись пропущена", self.name)
            self.failed += 1
            return False

        try:
            record = self._as_record(data)
        except Exception as exc:
            # Негодная запись до буфера не доходит: иначе она ломала бы и
            # запись соседних страниц из того же батча.
            self._fail(1, storage_error(exc, target=self.target))
            return False

        async with self._lock:
            self._buffer.append(record)
            if len(self._buffer) < self.batch_size:
                return True
            return await self._flush_locked()

    async def save_many(self, items: Iterable[dict[str, Any] | PageRecord]) -> int:
        """Сохраняет несколько страниц и возвращает число удачных."""
        saved = 0
        for item in items:
            if await self.save(item):
                saved += 1
        return saved

    async def flush(self) -> bool:
        """Досылает буфер на диск, не дожидаясь, пока он наберётся."""
        async with self._lock:
            self.flushes += 1
            return await self._flush_locked()

    async def open(self) -> None:
        """Готовит файл или базу заранее, не дожидаясь первой записи.

        В отличие от `save`, исключение здесь поднимается: открыть хранилище
        просят явно и до обхода, и о неподходящем файле лучше узнать сразу,
        пока ничего не потеряно.
        """
        async with self._lock:
            await self._ensure_open()

    async def close(self) -> None:
        """Досылает буфер и освобождает ресурсы; повторный вызов безопасен."""
        if self._closed:
            return

        async with self._lock:
            await self._flush_locked()
            self._closed = True
            if not self._opened:
                return
            try:
                await self._shutdown()
            except Exception as exc:
                self.last_error = str(storage_error(exc, target=self.target))
                logger.error("%s: не удалось закрыть %s — %s", self.name, self.target, exc)
            else:
                logger.info(
                    "%s закрыто: %s (записей %d, ошибок %d)",
                    self.name,
                    self.target,
                    self.saved,
                    self.failed,
                )

    def get_stats(self) -> dict[str, Any]:
        """Что и куда записано. Счётчики накопительные, как и сам файл.

        В отличие от очереди или лимитера, хранилище не сбрасывает статистику
        между обходами: файл после второго запуска содержит записи обоих, и
        счётчик, начинающийся с нуля, вводил бы в заблуждение.
        """
        return {
            "storage": self.name,
            "target": self.target,
            "batch_size": self.batch_size,
            "saved": self.saved,
            "failed": self.failed,
            "batches": self.batches,
            "flushes": self.flushes,
            "buffered": len(self._buffer),
            "write_time": self.write_time,
            "retries": self.retry_strategy.retries,
            "retry_success": self.retry_strategy.retried_successes,
            "last_error": self.last_error,
            "closed": self._closed,
        }

    async def __aenter__(self) -> DataStorage:
        await self.open()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()

    # --- внутреннее ---------------------------------------------------------

    def _as_record(self, data: dict[str, Any] | PageRecord) -> PageRecord:
        if isinstance(data, PageRecord):
            # Ограничение текста применяется и к готовой записи: иначе внутри
            # MultiStorage, который строит её один раз на все форматы, свой
            # text_limit вложенного хранилища не работал бы.
            return self._trim(data)
        if not isinstance(data, dict):
            raise TypeError(
                "ожидался словарь страницы или PageRecord, получено "
                f"{type(data).__name__}"
            )
        return PageRecord.from_page(data, text_limit=self.text_limit)

    def _trim(self, record: PageRecord) -> PageRecord:
        if self.text_limit is None or len(record.text) <= self.text_limit:
            return record
        return replace(record, text=record.text[: self.text_limit])

    async def _flush_locked(self) -> bool:
        """Пишет накопленный буфер. Вызывается уже под блокировкой."""
        if not self._buffer:
            return True
        batch = self._buffer
        self._buffer = []
        return await self._write_batch(batch)

    async def _write_batch(self, batch: list[PageRecord]) -> bool:
        started = time.perf_counter()
        try:
            await self.retry_strategy.execute_with_retry(
                self._write_attempt, batch, label=f"{self.name} -> {self.target}"
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # Батч, не записанный и после повторов, теряется: держать его в
            # памяти до конца обхода значило бы расти без предела, а причина
            # сбоя (нет места, нет прав) сама не проходит. Потерянные записи
            # видны в счётчике failed и в логе.
            self._fail(len(batch), storage_error(exc, target=self.target))
            return False
        else:
            self.saved += len(batch)
            self.batches += 1
            logger.debug(
                "%s: записано %d записей в %s", self.name, len(batch), self.target
            )
            return True
        finally:
            self.write_time += time.perf_counter() - started

    async def _write_attempt(self, batch: list[PageRecord]) -> None:
        """Одна попытка записи, где любой сбой получает наш класс ошибки.

        Без этого `RetryStrategy` увидела бы `OSError` неизвестной ошибкой и
        повторять не стала: решение о повторе она принимает по классу.
        Открытие файла тоже здесь — значит, и оно повторяется.
        """
        try:
            await self._ensure_open()
            await self._write(batch)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            error = storage_error(exc, target=self.target)
            if error is exc:
                raise
            raise error from exc

    async def _ensure_open(self) -> None:
        if self._opened:
            return
        await self._open()
        self._opened = True
        logger.info("%s готово к записи: %s", self.name, self.target)

    def _fail(self, count: int, error: CrawlerError) -> None:
        self.failed += count
        self.last_error = str(error)
        logger.error(
            "%s: не удалось сохранить %d записей в %s — [%s] %s",
            self.name,
            count,
            self.target,
            error.error_class,
            error,
        )

    @abstractmethod
    async def _open(self) -> None:
        """Создаёт файл или таблицы — лениво, перед первой записью."""

    @abstractmethod
    async def _write(self, batch: list[PageRecord]) -> None:
        """Пишет батч записей. Любой сбой поднимает исключение — его повторят."""

    async def _shutdown(self) -> None:
        """Закрывает ресурс: файл, соединение с базой. По умолчанию нечего."""

    @abstractmethod
    async def load(self, limit: int | None = None) -> list[dict[str, Any]]:
        """Читает сохранённые записи обратно — для проверок и демонстрации."""


class FileStorage(DataStorage):
    """Общее для файловых хранилищ: путь, каталог и открытый дескриптор."""

    def __init__(
        self,
        path: str | Path,
        *,
        encoding: str = "utf-8",
        errors: str = "strict",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.path = Path(path)
        self.encoding = encoding
        # По умолчанию строго: символ, который не лезет в выбранную кодировку,
        # лучше увидеть сразу. Для cp1251 и подобных помогает "replace".
        self.errors = errors
        self._handle: Any = None

    @property
    def target(self) -> str:
        return str(self.path)

    @property
    def size(self) -> int:
        """Размер файла на диске, 0 — если его ещё нет."""
        return self.path.stat().st_size if self.path.exists() else 0

    async def _open_handle(self, mode: str) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = await aiofiles.open(
            self.path, mode, encoding=self.encoding, errors=self.errors, newline=""
        )

    async def _shutdown(self) -> None:
        if self._handle is not None:
            await self._handle.close()
            self._handle = None

    async def _read_text(self) -> str:
        if not self.path.exists():
            return ""
        async with aiofiles.open(
            self.path, "r", encoding=self.encoding, errors=self.errors
        ) as handle:
            return await handle.read()

    async def _read_first_line(self) -> str:
        """Читает только первую строку — файл целиком для этого не нужен."""
        if not self.path.exists():
            return ""
        async with aiofiles.open(
            self.path, "r", encoding=self.encoding, errors=self.errors, newline=""
        ) as handle:
            return await handle.readline()

    def get_stats(self) -> dict[str, Any]:
        return {**super().get_stats(), "bytes": self.size}


class JSONStorage(FileStorage):
    """Сохранение в JSON: одним массивом или построчно (JSONL).

    Режима два, потому что требования к файлу противоположные. Валидный
    JSON-массив читается целиком одной строкой кода, но дописать в него запись
    нельзя — закрывающая скобка уже стоит на месте, поэтому файл создаётся
    заново. JSONL дописывается в конец и переживает падение процесса, но это не
    один JSON-документ, а поток документов.

    В обоих режимах запись идёт потоково, без сборки всего результата в памяти:
    так сохраняется и обход на сотни тысяч страниц.
    """

    def __init__(
        self,
        path: str | Path,
        *,
        lines: bool = False,
        indent: int | None = 2,
        ensure_ascii: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(path, **kwargs)
        self.lines = lines
        # В JSONL отступы невозможны: запись должна занимать ровно одну строку.
        self.indent = None if lines else indent
        self.ensure_ascii = ensure_ascii
        self._first = True

    async def _open(self) -> None:
        if self.lines:
            # Дописывание — весь смысл JSONL: прошлый обход остаётся в файле.
            await self._open_handle("a")
            return
        await self._open_handle("w")
        await self._handle.write("[")

    async def _write(self, batch: list[PageRecord]) -> None:
        chunks: list[str] = []
        first = self._first
        for record in batch:
            body = self._dumps(record)
            if self.lines:
                chunks.append(f"{body}\n")
                continue
            chunks.append(("\n" if first else ",\n") + body)
            first = False

        await self._handle.write("".join(chunks))
        # Сброс на диск после каждого батча: файл можно читать, не дожидаясь
        # конца обхода.
        await self._handle.flush()
        # Массив помечается непустым только после удачной записи: иначе повтор
        # упавшего батча поставил бы запятую перед первой записью, и файл
        # начинался бы с «[,» — такой JSON не разберётся.
        self._first = first

    def _dumps(self, record: PageRecord) -> str:
        body = json.dumps(
            record.as_dict(),
            ensure_ascii=self.ensure_ascii,
            indent=self.indent,
            # datetime сам в JSON не переводится, а терять время обхода нельзя.
            default=json_default,
        )
        if self.indent and not self.lines:
            # Запись лежит внутри массива, поэтому её сдвигают на один отступ.
            body = textwrap.indent(body, " " * self.indent)
        return body

    async def _shutdown(self) -> None:
        try:
            if self._handle is not None and not self.lines:
                # Массив нужно закрыть, иначе файл не разберётся как JSON.
                await self._handle.write("]" if self._first else "\n]")
        finally:
            # Дескриптор отпускаем, даже если скобка не записалась: иначе
            # открытый файл переживёт хранилище.
            await super()._shutdown()

    async def load(self, limit: int | None = None) -> list[dict[str, Any]]:
        """Читает файл обратно; незакрытый массив тоже разбирается.

        Незакрытый массив — это обход, который ещё идёт (или упал): данные в
        файле уже есть, и отказываться их читать из-за отсутствия скобки
        неправильно.
        """
        text = await self._read_text()
        if not text.strip():
            return []

        records = self._parse_lines(text) if self.lines else self._parse_array(text)
        return records[:limit] if limit is not None else records

    @staticmethod
    def _parse_array(text: str) -> list[dict[str, Any]]:
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            data = json.loads(text.rstrip().rstrip(",") + "]")
        return list(data)

    @staticmethod
    def _parse_lines(text: str) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for number, line in enumerate(text.splitlines(), start=1):
            if not line.strip():
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                # Обычно это последняя строка, которую не успели дописать.
                logger.warning("Строка %d не разобралась как JSON, пропущена", number)
        return records


class CSVStorage(FileStorage):
    """Сохранение в CSV: плоская таблица, которую открывает любой редактор.

    Колонки — поля `PageRecord` в порядке объявления, поэтому файл второго
    обхода совпадает по структуре с файлом первого и строки можно дописывать.
    Сами строки собирает стандартный `csv.writer`: в тексте страниц полно
    кавычек, запятых и переводов строк, и экранировать их руками — верный
    способ однажды получить сдвинутые колонки.
    """

    def __init__(
        self,
        path: str | Path,
        *,
        fields: Iterable[str] | None = None,
        delimiter: str = ",",
        **kwargs: Any,
    ) -> None:
        super().__init__(path, **kwargs)
        self.fields = list(fields) if fields is not None else PageRecord.field_names()
        self.delimiter = delimiter

    async def _open(self) -> None:
        # Файл уже с данными — значит, заголовок в нём есть, и строки просто
        # дописываются в конец.
        existing = self.size > 0
        if existing:
            await self._check_header()
        await self._open_handle("a" if existing else "w")
        if not existing:
            await self._handle.write(self._row(self.fields))

    async def _check_header(self) -> None:
        """Сверяет заголовок файла со своими колонками перед дописыванием.

        Без проверки строки с другим разделителем или другим порядком колонок
        легли бы в тот же файл, и прочитать его потом не смог бы никто —
        причём молча, уже после обхода.
        """
        line = await self._read_first_line()
        reader = csv.reader(io.StringIO(line, newline=""), delimiter=self.delimiter)
        if next(reader, []) != self.fields:
            raise StorageDataError(
                f"{self.path} создан с другой структурой: в файле "
                f"«{line.strip()[:60]}», ожидались колонки {self.fields[:3]} "
                f"через «{self.delimiter}» — укажите другой путь, разделитель "
                "или fields"
            )

    async def _write(self, batch: list[PageRecord]) -> None:
        rows = []
        for record in batch:
            data = record.as_dict()
            rows.append(self._row([as_cell(data.get(name)) for name in self.fields]))

        await self._handle.write("".join(rows))
        await self._handle.flush()

    def _row(self, values: Iterable[Any]) -> str:
        buffer = io.StringIO(newline="")
        writer = csv.writer(
            buffer,
            delimiter=self.delimiter,
            quoting=csv.QUOTE_MINIMAL,
            lineterminator="\r\n",
        )
        writer.writerow(list(values))
        return buffer.getvalue()

    async def load(self, limit: int | None = None) -> list[dict[str, Any]]:
        """Читает таблицу обратно, возвращая строкам исходные типы."""
        text = await self._read_text()
        if not text.strip():
            return []

        reader = csv.DictReader(io.StringIO(text, newline=""), delimiter=self.delimiter)
        records: list[dict[str, Any]] = []
        for row in reader:
            records.append({name: _restore(name, value) for name, value in row.items()})
            if limit is not None and len(records) >= limit:
                break
        return records


class SQLiteStorage(DataStorage):
    """Сохранение в SQLite через `aiosqlite`: одна страница — одна строка.

    Батч по умолчанию большой: каждая вставка в SQLite — это транзакция с
    сбросом на диск, и `executemany` на 50 записей обходится примерно как одна
    отдельная вставка. Для файлов батч не нужен, а для базы он и есть
    оптимизация производительности.

    URL уникален, и повторная запись той же страницы её обновляет: при втором
    обходе в базе должна лежать свежая версия, а не вторая копия.
    """

    def __init__(
        self,
        path: str | Path,
        *,
        table: str = "pages",
        batch_size: int = 50,
        **kwargs: Any,
    ) -> None:
        super().__init__(batch_size=batch_size, **kwargs)
        self.path = Path(path)
        # Имя таблицы подставляется в SQL, параметром его не передать, поэтому
        # допускаются только безопасные символы.
        if not table.replace("_", "").isalnum():
            raise ValueError(f"недопустимое имя таблицы: {table!r}")
        self.table = table
        self.columns = PageRecord.field_names()
        if missing := [name for name in self.columns if name not in _SCHEMA]:
            # Поле добавили в запись, а в схему забыли: лучше узнать об этом
            # при создании хранилища, чем на первой вставке.
            raise ValueError(f"в схеме таблицы нет столбцов: {', '.join(missing)}")
        self._db: Any = None

    @property
    def target(self) -> str:
        return f"{self.path}:{self.table}"

    @property
    def size(self) -> int:
        return self.path.stat().st_size if self.path.exists() else 0

    async def init_db(self) -> None:
        """Создаёт таблицу и индексы. Повторный вызов ничего не портит."""
        await self.open()

    def get_stats(self) -> dict[str, Any]:
        return {**super().get_stats(), "table": self.table, "bytes": self.size}

    async def _open(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._db = await aiosqlite.connect(self.path)
        self._db.row_factory = aiosqlite.Row
        # WAL: читать базу можно, пока обход ещё пишет. Без него demo-скрипт,
        # читающий данные по ходу, упирался бы в блокировку.
        await self._db.execute("PRAGMA journal_mode=WAL")
        columns = ", ".join(f"{name} {kind}" for name, kind in _SCHEMA.items())
        await self._db.execute(
            f"CREATE TABLE IF NOT EXISTS {self.table} ({columns})"
        )
        for column in _INDEXES:
            await self._db.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{self.table}_{column} "
                f"ON {self.table} ({column})"
            )
        await self._db.commit()

    async def _write(self, batch: list[PageRecord]) -> None:
        placeholders = ", ".join("?" * len(self.columns))
        updates = ", ".join(
            f"{column}=excluded.{column}" for column in self.columns if column != "url"
        )
        rows = [
            tuple(as_cell(record.as_dict()[column]) for column in self.columns)
            for record in batch
        ]
        await self._db.executemany(
            f"INSERT INTO {self.table} ({', '.join(self.columns)}) "
            f"VALUES ({placeholders}) "
            f"ON CONFLICT(url) DO UPDATE SET {updates}",
            rows,
        )
        # Один commit на батч: в этом и смысл батча.
        await self._db.commit()

    async def _shutdown(self) -> None:
        if self._db is not None:
            await self._db.close()
            self._db = None

    async def load(self, limit: int | None = None) -> list[dict[str, Any]]:
        """Читает страницы из базы в порядке записи — как файловые хранилища."""
        query = f"SELECT * FROM {self.table} ORDER BY id"
        return await self._select(query, limit=limit)

    async def count(self) -> int:
        """Сколько страниц сейчас в таблице."""
        if self._absent:
            return 0
        async with self._connection() as db:
            async with db.execute(f"SELECT COUNT(*) FROM {self.table}") as cursor:
                row = await cursor.fetchone()
        return int(row[0]) if row else 0

    async def search(self, text: str, limit: int | None = 10) -> list[dict[str, Any]]:
        """Ищет страницы по заголовку и тексту — для демонстрации чтения."""
        query = (
            f"SELECT * FROM {self.table} "
            f"WHERE title LIKE ? ESCAPE '{LIKE_ESCAPE}' "
            f"OR text LIKE ? ESCAPE '{LIKE_ESCAPE}' "
            "ORDER BY id"
        )
        pattern = f"%{_escape_like(text)}%"
        return await self._select(query, (pattern, pattern), limit=limit)

    async def _select(
        self,
        query: str,
        params: tuple[Any, ...] = (),
        *,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        if self._absent:
            return []
        if limit is not None:
            query = f"{query} LIMIT {int(limit)}"
        async with self._connection() as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(query, params) as cursor:
                rows = await cursor.fetchall()
        return [self._as_record_dict(row) for row in rows]

    @property
    def _absent(self) -> bool:
        """Базы ещё нет на диске: таблицы не создавали, читать нечего.

        Чтение отвечает пустым результатом, а не ошибкой: файловые хранилища на
        отсутствующий файл ведут себя так же, да и подключение к SQLite создало
        бы пустую базу — странный побочный эффект для запроса на чтение.
        """
        return self._db is None and not self.path.exists()

    @asynccontextmanager
    async def _connection(self) -> AsyncIterator[Any]:
        """Отдаёт открытое соединение или заводит своё на время чтения.

        Читать можно и из закрытого хранилища: файл базы остаётся на диске, и
        проверить записанное после `close()` — обычное дело.
        """
        if self._db is not None:
            yield self._db
            return
        db = await aiosqlite.connect(self.path)
        try:
            yield db
        finally:
            await db.close()

    def _as_record_dict(self, row: Any) -> dict[str, Any]:
        """Превращает строку таблицы в словарь с исходными типами полей."""
        data = {key: row[key] for key in row.keys()}
        return {name: _restore(name, value) for name, value in data.items()}


class MultiStorage(DataStorage):
    """Пишет каждую страницу сразу в несколько хранилищ.

    Нужна, когда одни и те же данные хотят и в базе (искать), и в CSV (открыть
    в редакторе). Сбои независимы: упавший формат не отменяет остальные, и
    каждое хранилище считает свои ошибки само. Общей транзакции тут нет и быть
    не может — файл и база не умеют коммититься вместе.
    """

    def __init__(self, storages: Iterable[DataStorage], **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.storages = list(storages)
        if not self.storages:
            raise ValueError("MultiStorage нужен хотя бы один формат")

    @property
    def target(self) -> str:
        return ", ".join(storage.target for storage in self.storages)

    async def _open(self) -> None:
        await self._spread("открыть", (storage.open() for storage in self.storages))

    async def _write(self, batch: list[PageRecord]) -> None:
        """Раздаёт готовые записи хранилищам.

        Исключение наружу не уходит: каждое хранилище само решает, повторять ли
        запись, и само считает неудачи. Поэтому повторы на этом уровне не нужны
        — иначе удачно записавшие форматы получили бы дубли.
        """
        await self._spread(
            "записать", (storage.save_many(batch) for storage in self.storages)
        )

    async def flush(self) -> bool:
        own = await super().flush()
        results = await asyncio.gather(
            *(storage.flush() for storage in self.storages), return_exceptions=True
        )
        return own and all(result is True for result in results)

    async def _shutdown(self) -> None:
        await self._spread("закрыть", (storage.close() for storage in self.storages))

    async def _spread(self, what: str, calls: Iterable[Any]) -> None:
        """Выполняет операцию во всех хранилищах, не прерываясь на сбое одного."""
        results = await asyncio.gather(*calls, return_exceptions=True)
        for storage, result in zip(self.storages, results, strict=True):
            if isinstance(result, BaseException):
                self.last_error = f"{storage.name}: {result}"
                logger.error(
                    "%s: не удалось %s %s — %s", self.name, what, storage.target, result
                )

    async def load(self, limit: int | None = None) -> list[dict[str, Any]]:
        """Читает из первого хранилища: данные во всех одни и те же."""
        return await self.storages[0].load(limit)

    def get_stats(self) -> dict[str, Any]:
        """Своя сводка плюс сводки вложенных хранилищ.

        `saved` здесь — сколько записей роздано, а `failed` — сколько неудач
        набралось во всех форматах вместе. Одна страница может попасть и туда,
        и туда: в базу легла, а в файл не записалась.
        """
        children = [storage.get_stats() for storage in self.storages]
        return {
            **super().get_stats(),
            "formats": [storage.name for storage in self.storages],
            "failed": sum(child["failed"] for child in children),
            "retries": sum(child["retries"] for child in children),
            "storages": children,
        }
