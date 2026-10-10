"""Очередь URL с приоритетами и учётом глубины обхода."""

from __future__ import annotations

import asyncio
import heapq
import logging
import time
from typing import Any

logger = logging.getLogger(__name__)


class CrawlerQueue:
    """Хранит URL, которые осталось обойти, и следит за ходом обработки.

    Дедупликация живёт здесь: URL, однажды попавший в очередь, второй раз не
    принимается, поэтому воркерам не нужно сверяться между собой.

    Повторных попыток для неудачных URL нет: `mark_failed` закрывает URL
    окончательно. Политика повторов — тема дня 4.
    """

    def __init__(self) -> None:
        self._heap: list[tuple[int, int, str]] = []
        self._counter = 0
        self._in_progress = 0
        self._closed = False
        # Событие будит воркеров, ждущих, пока в очереди появится работа.
        self._wakeup = asyncio.Event()
        self._started = time.perf_counter()

        self.seen: set[str] = set()
        self.processed: set[str] = set()
        self.failed: dict[str, str] = {}
        self.depths: dict[str, int] = {}
        self.added = 0
        self.skipped_duplicates = 0
        self.skipped = 0
        # Глубина обработанных URL, а не поставленных в очередь: иначе
        # «достигнутая глубина» росла бы от ссылок, которые так и не загрузили.
        self.max_depth_done = 0

    def __len__(self) -> int:
        """Количество URL, ожидающих обработки."""
        return len(self._heap)

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def in_progress(self) -> int:
        return self._in_progress

    def add_url(self, url: str, priority: int = 0, *, depth: int = 0) -> bool:
        """Ставит URL в очередь; возвращает False, если он уже был добавлен.

        Чем больше `priority`, тем раньше URL будет выдан. При равных
        приоритетах порядок — FIFO.
        """
        if self._closed:
            logger.debug("Очередь закрыта, URL отброшен: %s", url)
            return False
        if url in self.seen:
            self.skipped_duplicates += 1
            logger.debug("URL уже был в очереди: %s", url)
            return False

        self.seen.add(url)
        self.depths[url] = depth
        self.added += 1
        self._counter += 1
        # priority со знаком минус: heapq отдаёт минимальный элемент. Счётчик
        # ломает ничьи, попутно избавляя от сравнения самих URL.
        heapq.heappush(self._heap, (-priority, self._counter, url))
        self._wakeup.set()
        return True

    def add_alias(self, url: str) -> bool:
        """Запоминает URL как уже известный, не ставя его в очередь.

        Нужно для целей редиректов: страница запрошена по одному адресу, а
        отдана с другого, и ссылку на второй адрес грузить повторно незачем.
        Обратный случай — редирект на уже обойдённую страницу — так не
        лечится: о нём становится известно только после загрузки.
        """
        if url in self.seen:
            return False
        self.seen.add(url)
        logger.debug("URL запомнен как алиас, в очередь не ставится: %s", url)
        return True

    async def get_next(self) -> str | None:
        """Выдаёт следующий URL или None, когда обход закончен.

        Пустая очередь ещё не означает конец работы: URL в полёте могут принести
        новые ссылки. Поэтому None возвращается только когда очередь пуста и
        никто ничего не обрабатывает — либо после `close()`.
        """
        while True:
            if self._closed:
                return None
            if self._heap:
                return self._take()
            if self._in_progress == 0:
                # Работы нет и взяться ей больше неоткуда: обход завершён.
                self.close()
                return None

            # Между clear и wait нет точки переключения, поэтому состояние не
            # изменится незаметно и пробуждение не потеряется.
            self._wakeup.clear()
            await self._wakeup.wait()

    def _take(self) -> str:
        _priority, _counter, url = heapq.heappop(self._heap)
        self._in_progress += 1
        return url

    def mark_processed(self, url: str) -> None:
        """Помечает URL как успешно обработанный."""
        self.processed.add(url)
        self.max_depth_done = max(self.max_depth_done, self.depth_of(url))
        self._release(url)

    def mark_failed(self, url: str, error: str) -> None:
        """Помечает URL как неудачный, сохраняя описание ошибки."""
        self.failed[url] = error
        self.max_depth_done = max(self.max_depth_done, self.depth_of(url))
        self._release(url)

    def mark_skipped(self, url: str) -> None:
        """Отпускает взятый URL, не записывая его ни в успешные, ни в неудачные.

        Нужно, когда URL уже выдан, но загружать его не стали: исчерпан лимит
        страниц или выяснилось, что он ведёт на уже загруженную страницу.
        """
        self.skipped += 1
        self._release(url)

    def _release(self, url: str) -> None:
        if self._in_progress > 0:
            self._in_progress -= 1
        else:
            logger.warning("mark_* без парного get_next: %s", url)
        # Разбудить ожидающих: возможно, это была последняя задача в полёте.
        self._wakeup.set()

    def close(self) -> None:
        """Останавливает выдачу URL: все `get_next` начинают отдавать None.

        Флаг не снимается, а событие остаётся установленным: иначе ожидающие
        воркеры, которых событие уже не разбудит, зависли бы навсегда.
        """
        if not self._closed:
            self._closed = True
            logger.debug(
                "Очередь закрыта: осталось %d URL, в полёте %d",
                len(self._heap),
                self._in_progress,
            )
        self._wakeup.set()

    def depth_of(self, url: str) -> int:
        """Глубина, на которой URL попал в очередь (стартовые URL — 0)."""
        return self.depths.get(url, 0)

    def get_stats(self) -> dict[str, Any]:
        """Сводка по очереди: сколько сделано, сколько осталось, с какой скоростью."""
        done = len(self.processed) + len(self.failed)
        elapsed = time.perf_counter() - self._started
        return {
            "pending": len(self._heap),
            "in_progress": self._in_progress,
            "processed": len(self.processed),
            "failed": len(self.failed),
            "seen": len(self.seen),
            "added": self.added,
            "skipped_duplicates": self.skipped_duplicates,
            "skipped_unfetched": self.skipped,
            # reached — глубина пройденных страниц, queued — глубина всего, что
            # попало в очередь (включая так и не загруженное).
            "max_depth_reached": self.max_depth_done,
            "max_depth_queued": max(self.depths.values(), default=0),
            "elapsed": elapsed,
            "pages_per_sec": done / elapsed if elapsed > 0 else 0.0,
            "closed": self._closed,
        }
