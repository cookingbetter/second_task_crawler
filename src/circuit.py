"""Circuit breaker: временное отключение домена, который отдаёт одни ошибки."""

from __future__ import annotations

import logging
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from src.errors import CircuitOpenError, ParseError, PermanentError
from src.filters import domain_key

logger = logging.getLogger(__name__)

STATE_CLOSED = "closed"
STATE_OPEN = "open"
STATE_HALF_OPEN = "half-open"

# Ошибки, которые не говорят ничего о здоровье сервера: битая ссылка и
# неразбираемый HTML — это свойства страницы, а не признак аварии.
HARMLESS_ERRORS = (PermanentError, ParseError, CircuitOpenError)


@dataclass(slots=True)
class _DomainState:
    """Состояние одного домена: ошибки в окне и момент отключения."""

    state: str = STATE_CLOSED
    failures: deque[float] = field(default_factory=deque)
    opened_at: float = 0.0
    # Идёт пробный запрос в состоянии half-open: второй пускать нельзя.
    probing: bool = False
    probe_started: float = 0.0
    trips: int = 0
    refused: int = 0
    recoveries: int = 0


class CircuitBreaker:
    """Отключает домен, если ошибки идут слишком часто, и возвращает его обратно.

    Три состояния на домен:

    - `closed` — запросы идут как обычно;
    - `open` — после `failure_threshold` ошибок за `window` секунд домен
      отключён на `recovery_time`: новые запросы отвергаются без обращения в сеть;
    - `half-open` — время вышло, пропускается один пробный запрос. Успех
      закрывает цепь, неудача снова открывает её.

    Ключ домена — тот же `domain_key`, что у лимитера и семафоров: `www` и порт
    одного сайта делят одно состояние. Повторы отдельного URL здесь не при чём:
    breaker защищает сайт от нас целиком.
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        window: float = 60.0,
        recovery_time: float = 30.0,
        *,
        clock: Callable[[], float] | None = None,
    ) -> None:
        if failure_threshold < 1:
            raise ValueError("failure_threshold должен быть >= 1")
        if window <= 0:
            raise ValueError("window должен быть > 0")
        if recovery_time < 0:
            raise ValueError("recovery_time не может быть отрицательным")

        self.failure_threshold = failure_threshold
        self.window = window
        self.recovery_time = recovery_time
        # Монотонные часы, подменяемые в тестах: ждать полминуты незачем.
        self._clock = clock or time.monotonic
        self._domains: dict[str, _DomainState] = {}

    def allow(self, url: str) -> bool:
        """Можно ли сейчас обратиться к домену этого URL."""
        key = domain_key(url)
        domain = self._domains.get(key)
        if domain is None or domain.state == STATE_CLOSED:
            return True

        now = self._clock()
        if domain.state == STATE_OPEN:
            if now - domain.opened_at < self.recovery_time:
                domain.refused += 1
                return False
            domain.state = STATE_HALF_OPEN
            domain.probing = True
            domain.probe_started = now
            logger.info(
                "Домен %s проверяется пробным запросом после %.0f с отключения",
                key,
                self.recovery_time,
            )
            return True

        # half-open: один пробный запрос уже в пути, остальные ждут его исхода.
        # Если исход так и не пришёл (например, задачу отменили), через
        # `recovery_time` пробуем снова — иначе домен остался бы закрытым навсегда.
        if domain.probing and now - domain.probe_started < self.recovery_time:
            domain.refused += 1
            return False
        domain.probing = True
        domain.probe_started = now
        return True

    def record_success(self, url: str) -> None:
        """Успешный ответ: домен здоров, счётчик ошибок обнуляется."""
        key = domain_key(url)
        domain = self._domains.get(key)
        if domain is None:
            return

        self._close(key, domain, "домен снова отвечает")

    def record_failure(self, url: str, error: BaseException | None = None) -> None:
        """Ошибка загрузки: приближает отключение домена.

        Постоянные ошибки (404, 403) и ошибки разбора игнорируются: обход сайта
        с битыми ссылками не должен выглядеть как авария сервера. Но если такой
        ответ пришёл на пробный запрос, цепь закрывается: сервер ответил — пусть
        и отказом, — а значит, он жив. Иначе 404 на пробе держал бы живой домен
        отключённым ещё `recovery_time`.
        """
        key = domain_key(url)
        if error is not None and isinstance(error, HARMLESS_ERRORS):
            domain = self._domains.get(key)
            if domain is not None and domain.state == STATE_HALF_OPEN:
                self._close(key, domain, "сервер ответил на пробный запрос")
            return

        domain = self._domains.setdefault(key, _DomainState())
        now = self._clock()
        domain.failures.append(now)
        self._prune(domain, now)

        if domain.state == STATE_HALF_OPEN:
            domain.probing = False
            self._open(key, domain, now, "пробный запрос снова не удался")
            return
        if domain.state == STATE_CLOSED and len(domain.failures) >= self.failure_threshold:
            self._open(
                key,
                domain,
                now,
                f"{len(domain.failures)} ошибок за {self.window:.0f} с",
            )

    def state_of(self, url: str) -> str:
        """Состояние домена с учётом истёкшего времени отключения."""
        domain = self._domains.get(domain_key(url))
        if domain is None:
            return STATE_CLOSED
        if (
            domain.state == STATE_OPEN
            and self._clock() - domain.opened_at >= self.recovery_time
        ):
            return STATE_HALF_OPEN
        return domain.state

    def failures_for(self, url: str) -> int:
        """Сколько ошибок домена сейчас в окне."""
        domain = self._domains.get(domain_key(url))
        if domain is None:
            return 0
        self._prune(domain, self._clock())
        return len(domain.failures)

    def reset_stats(self) -> None:
        """Обнуляет счётчики, оставляя состояния: открытая цепь так и открыта."""
        for domain in self._domains.values():
            domain.trips = 0
            domain.refused = 0
            domain.recoveries = 0

    def get_stats(self) -> dict[str, Any]:
        """Настройки breaker'а и то, что он успел сделать."""
        states = {key: self.state_of(key) for key in self._domains}
        return {
            "enabled": True,
            "failure_threshold": self.failure_threshold,
            "window": self.window,
            "recovery_time": self.recovery_time,
            "domains": len(self._domains),
            "states": states,
            "open": sorted(
                key for key, state in states.items() if state == STATE_OPEN
            ),
            # Домен с пробным запросом в пути тоже никого не пускает, поэтому в
            # отчёте он должен быть виден отдельно, а не исчезать из обоих списков.
            "half_open": sorted(
                key for key, state in states.items() if state == STATE_HALF_OPEN
            ),
            "trips": sum(domain.trips for domain in self._domains.values()),
            "refused": sum(domain.refused for domain in self._domains.values()),
            "recoveries": sum(
                domain.recoveries for domain in self._domains.values()
            ),
        }

    def _close(self, key: str, domain: _DomainState, reason: str) -> None:
        """Возвращает домен в обычный режим и забывает прошлые ошибки."""
        domain.failures.clear()
        domain.probing = False
        if domain.state == STATE_CLOSED:
            return
        domain.state = STATE_CLOSED
        domain.recoveries += 1
        logger.info("Цепь домена %s закрыта: %s", key, reason)

    def _open(self, key: str, domain: _DomainState, now: float, reason: str) -> None:
        domain.state = STATE_OPEN
        domain.opened_at = now
        domain.probing = False
        domain.trips += 1
        logger.warning(
            "Домен %s отключён на %.0f с: %s", key, self.recovery_time, reason
        )

    def _prune(self, domain: _DomainState, now: float) -> None:
        """Выбрасывает ошибки, вышедшие из окна: авария не длится вечно."""
        deadline = now - self.window
        while domain.failures and domain.failures[0] < deadline:
            domain.failures.popleft()
