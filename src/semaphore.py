"""Управление конкурентностью: общий лимит и лимит на домен."""

from __future__ import annotations

import asyncio
import logging
from collections import Counter
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from src.filters import domain_key

logger = logging.getLogger(__name__)


class SemaphoreManager:
    """Пропускает запросы через два лимита: общий и на один домен.

    Вежливость к сайту задаётся лимитом на домен, а защита собственных ресурсов
    (сокеты, память) — общим лимитом.
    """

    def __init__(
        self, max_concurrent: int = 10, max_per_domain: int | None = None
    ) -> None:
        if max_concurrent < 1:
            raise ValueError("max_concurrent должен быть >= 1")
        if max_per_domain is not None and max_per_domain < 1:
            raise ValueError("max_per_domain должен быть >= 1")

        self.max_concurrent = max_concurrent
        # None означает «не ограничивать домен отдельно», то есть лимит домена
        # совпадает с общим и ничего не меняет.
        self.max_per_domain = max_per_domain or max_concurrent

        self._global = asyncio.Semaphore(max_concurrent)
        self._per_host: dict[str, asyncio.Semaphore] = {}
        # Сколько задач держит семафор домена или стоит за ним в очереди.
        self._host_refs: Counter[str] = Counter()

        self.active_total = 0
        self.active_by_host: Counter[str] = Counter()
        self.waiting = 0
        self.peak_total = 0
        self.peak_by_host: Counter[str] = Counter()
        self.acquired_total = 0

    @asynccontextmanager
    async def acquire(self, url: str) -> AsyncIterator[str]:
        """Занимает слот под запрос к `url` и освобождает его на выходе.

        Сначала берётся слот домена, только потом общий. Обратный порядок тоже
        не приводит к взаимоблокировке (порядок захвата везде одинаков), но
        задача, ожидающая свой домен, держала бы общий слот и зря не пускала
        запросы к другим доменам.

        Домен определяется тем же `domain_key`, что и в фильтрах: иначе один
        сайт, доступный как `example.com`, `www.example.com` и
        `example.com:443`, получил бы три семафора и втрое больший лимит.
        """
        host = domain_key(url)
        # Счётчик ссылок растёт до первого await, поэтому семафор не могут
        # удалить из-под работающей задачи.
        host_semaphore = self._host_semaphore(host)
        self._host_refs[host] += 1

        self.waiting += 1
        acquired = False
        try:
            async with host_semaphore, self._global:
                self.waiting -= 1
                acquired = True
                self._enter(host)
                try:
                    yield host
                finally:
                    # finally, а не код после yield: при отмене задачи счётчики
                    # иначе остались бы завышенными навсегда.
                    self._leave(host)
        finally:
            if not acquired:
                # Отмена во время ожидания слота.
                self.waiting -= 1
            self._host_refs[host] -= 1
            if self._host_refs[host] <= 0:
                # Домен никому не нужен: не держать семафор на каждый хост,
                # встреченный за обход, иначе словарь растёт без границ.
                del self._host_refs[host]
                self._per_host.pop(host, None)

    def _host_semaphore(self, host: str) -> asyncio.Semaphore:
        semaphore = self._per_host.get(host)
        if semaphore is None:
            semaphore = asyncio.Semaphore(self.max_per_domain)
            self._per_host[host] = semaphore
            logger.debug(
                "Семафор для домена %s: лимит %d", host or "<пусто>", self.max_per_domain
            )
        return semaphore

    def _enter(self, host: str) -> None:
        self.active_total += 1
        self.active_by_host[host] += 1
        self.acquired_total += 1
        self.peak_total = max(self.peak_total, self.active_total)
        self.peak_by_host[host] = max(
            self.peak_by_host[host], self.active_by_host[host]
        )

    def _leave(self, host: str) -> None:
        self.active_total -= 1
        self.active_by_host[host] -= 1
        if self.active_by_host[host] <= 0:
            del self.active_by_host[host]

    def reset_stats(self) -> None:
        """Обнуляет накопленные пики и счётчик захватов перед новым обходом.

        Активные слоты не трогаются: они отражают реальное состояние, а не
        историю, и их обнуление сломало бы учёт при освобождении.
        """
        self.peak_total = self.active_total
        self.peak_by_host = Counter(self.active_by_host)
        self.acquired_total = 0

    def get_stats(self) -> dict[str, Any]:
        """Текущая и пиковая загрузка — по процессу целиком и по доменам."""
        return {
            "max_concurrent": self.max_concurrent,
            "max_per_domain": self.max_per_domain,
            "active": self.active_total,
            "waiting": self.waiting,
            "peak_active": self.peak_total,
            "acquired_total": self.acquired_total,
            "active_by_host": dict(self.active_by_host),
            "peak_by_host": dict(self.peak_by_host),
            # Встреченные за обход домены, а не живые семафоры: последние
            # удаляются, как только домен освобождается.
            "hosts": len(self.peak_by_host),
            "hosts_active": len(self._per_host),
        }
