"""Повторы запросов с экспоненциальным backoff и политиками по типам ошибок."""

from __future__ import annotations

import asyncio
import inspect
import logging
import random
from collections import Counter
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any

from src.errors import (
    CrawlerError,
    NetworkError,
    RateLimitError,
    ServerError,
    TransientError,
    from_exception,
)

logger = logging.getLogger(__name__)

# Повторять стоит то, что может пройти со второй попытки: перегруз, таймаут,
# сетевой сбой. Постоянные ошибки и ошибки разбора в этот список не входят.
DEFAULT_RETRY_ON: tuple[type[BaseException], ...] = (TransientError, NetworkError)
# 429 означает «вы частите»: пауза перед повтором начинается заметно выше.
RATE_LIMIT_BASE_DELAY = 5.0


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Сколько раз и с какой паузой повторять ошибку одного типа."""

    max_retries: int
    base_delay: float
    backoff_factor: float

    def delay_for(self, attempt: int) -> float:
        """Пауза после `attempt`-й неудачной попытки (нумерация с единицы)."""
        return self.base_delay * self.backoff_factor ** (attempt - 1)


@dataclass(slots=True)
class RetryOutcome:
    """Что стоило получение результата: сколько попыток и сколько ожидания.

    Создаётся вызывающим кодом на один вызов `execute_with_retry` и заполняется
    по ходу дела. Отдельный объект, а не поля стратегии: повторы идут
    одновременно для многих URL, и общее состояние перемешалось бы.
    """

    attempts: int = 0
    waited: float = 0.0
    errors: list[str] = field(default_factory=list)


class RetryStrategy:
    """Выполняет корутину заново, пока ошибка выглядит временной.

    Пауза между попытками растёт экспоненциально: `base_delay`, умноженная на
    `backoff_factor` в степени номера попытки, но не больше `max_delay`. Разным
    типам ошибок положены разные лимиты и базовые паузы (`policies`): 429 просит
    подождать дольше, а 5xx повторяется меньше раз, потому что чаще означает
    настоящую поломку, а не всплеск нагрузки.

    Пауза между попытками одного URL — дело стратегии; пауза между разными URL
    остаётся делом `RateLimiter`. Они не складываются: лимитер считает шаг от
    времени последнего запроса, поэтому фактическая пауза равна большей из двух.
    """

    def __init__(
        self,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
        retry_on: Iterable[type[BaseException]] | None = None,
        *,
        base_delay: float = 0.5,
        max_delay: float = 30.0,
        jitter: float = 0.0,
        policies: dict[type[BaseException], RetryPolicy] | None = None,
        sleep: Callable[[float], Any] | None = None,
        rng: random.Random | None = None,
    ) -> None:
        if max_retries < 0:
            raise ValueError("max_retries не может быть отрицательным")
        if backoff_factor < 1:
            raise ValueError("backoff_factor должен быть >= 1")
        if base_delay < 0 or max_delay < 0:
            raise ValueError("base_delay и max_delay не могут быть отрицательными")
        if jitter < 0:
            raise ValueError("jitter не может быть отрицательным")

        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.retry_on: tuple[type[BaseException], ...] = (
            tuple(retry_on) if retry_on is not None else DEFAULT_RETRY_ON
        )
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.jitter = jitter
        self.policies: dict[type[BaseException], RetryPolicy] = {
            **self._default_policies(),
            **(policies or {}),
        }
        # Подменяемые сон и случайность: тесты проверяют последовательность
        # задержек, не ожидая её в реальном времени.
        self._sleep = sleep or asyncio.sleep
        self._rng = rng or random.Random()

        self.calls = 0
        self.attempts = 0
        self.retries = 0
        self.successes = 0
        self.retried_successes = 0
        self.exhausted = 0
        self.not_retried = 0
        self.wait_total = 0.0
        self.max_wait = 0.0
        self.by_class: Counter[str] = Counter()
        self.by_error_type: Counter[str] = Counter()

    @property
    def enabled(self) -> bool:
        """Может ли стратегия вообще что-нибудь повторить."""
        return self.max_retries > 0 or any(
            policy.max_retries > 0 for policy in self.policies.values()
        )

    async def execute_with_retry(
        self,
        coro: Callable[..., Any],
        *args: Any,
        label: str | None = None,
        attempt_kwarg: str | None = None,
        outcome: RetryOutcome | None = None,
        **kwargs: Any,
    ) -> Any:
        """Вызывает корутину заново, пока ошибка временная и попытки не кончились.

        `coro` — функция-корутина: её нужно вызывать на каждой попытке заново.
        Готовый объект корутины тоже принимается, но повторить его нельзя
        (awaitable одноразовый), поэтому он просто выполняется с записью в лог.

        `label` попадает в логи (краулер передаёт URL), `attempt_kwarg` — имя
        аргумента, в который передать номер попытки (так краулер растит
        таймауты), `outcome` — куда записать число попыток и общее ожидание.
        """
        target = label if label is not None else _label_of(coro, args)
        self.calls += 1
        report = outcome if outcome is not None else RetryOutcome()

        if not callable(coro):
            if not inspect.isawaitable(coro):
                raise TypeError(
                    "execute_with_retry ожидает функцию-корутину или awaitable"
                )
            logger.warning(
                "Повторы для %s отключены: передан готовый объект корутины, "
                "его можно дождаться только один раз",
                target,
            )
            self.attempts += 1
            report.attempts = 1
            result = await coro
            self.successes += 1
            return result

        attempt = 0
        while True:
            attempt += 1
            if attempt_kwarg is not None:
                kwargs[attempt_kwarg] = attempt
            self.attempts += 1
            report.attempts = attempt

            try:
                result = await coro(*args, **kwargs)
            except Exception as exc:  # CancelledError — BaseException, не сюда
                error = from_exception(exc)
                error.attempts = attempt
                if isinstance(exc, CrawlerError):
                    exc.attempts = attempt
                self._count_error(error)
                report.errors.append(f"{error.error_class}: {error}")

                policy = self.policy_for(error)
                if not self._retryable(exc, error, policy, attempt):
                    self._log_give_up(target, error, attempt, policy)
                    raise

                delay = self.delay_for(error, attempt, policy)
                self.retries += 1
                self.wait_total += delay
                self.max_wait = max(self.max_wait, delay)
                report.waited += delay
                logger.warning(
                    "Попытка %d/%d не удалась: %s [%s] %s — повтор через %.2f с",
                    attempt,
                    policy.max_retries + 1,
                    target,
                    error.error_class,
                    error,
                    delay,
                )
                await self._sleep(delay)
                continue

            self.successes += 1
            if attempt > 1:
                self.retried_successes += 1
                logger.info(
                    "Успех с %d-й попытки: %s (ожидание повторов %.2f с)",
                    attempt,
                    target,
                    report.waited,
                )
            return result

    def policy_for(self, error: BaseException) -> RetryPolicy:
        """Политика для ошибки: самая конкретная из найденных по иерархии.

        Поиск идёт по MRO исключения, поэтому `ServerError` берёт свою политику,
        а неизвестный подвид `TransientError` — политику родителя.
        """
        for klass in type(error).__mro__:
            policy = self.policies.get(klass)
            if policy is not None:
                return policy
        return RetryPolicy(self.max_retries, self.base_delay, self.backoff_factor)

    def delay_for(
        self,
        error: BaseException,
        attempt: int,
        policy: RetryPolicy | None = None,
    ) -> float:
        """Сколько ждать после `attempt`-й неудачи.

        `Retry-After` от сервера не позволяет уйти раньше, чем он просит, но и
        не отменяет наш backoff: берётся большее из двух, и всё это не выше
        `max_delay` — иначе один заголовок с «подождите час» остановил бы обход.
        """
        policy = policy or self.policy_for(error)
        delay = min(policy.delay_for(attempt), self.max_delay)

        retry_after = getattr(error, "retry_after", None)
        if retry_after is not None:
            delay = min(max(delay, float(retry_after)), self.max_delay)

        if self.jitter:
            # Разброс только вверх: раньше срока повтор уходить не должен.
            delay += self._rng.uniform(0.0, self.jitter)
        return delay

    def reset_stats(self) -> None:
        """Обнуляет счётчики перед новым обходом; настройки остаются."""
        self.calls = 0
        self.attempts = 0
        self.retries = 0
        self.successes = 0
        self.retried_successes = 0
        self.exhausted = 0
        self.not_retried = 0
        self.wait_total = 0.0
        self.max_wait = 0.0
        self.by_class.clear()
        self.by_error_type.clear()

    def get_stats(self) -> dict[str, Any]:
        """Настройки повторов и то, как они сказались на запросах."""
        return {
            "enabled": self.enabled,
            "max_retries": self.max_retries,
            "backoff_factor": self.backoff_factor,
            "base_delay": self.base_delay,
            "max_delay": self.max_delay,
            "jitter": self.jitter,
            "retry_on": [klass.__name__ for klass in self.retry_on],
            "calls": self.calls,
            "attempts": self.attempts,
            "retries": self.retries,
            "successes": self.successes,
            # Главная цифра дня: сколько URL спасли повторы.
            "retry_success": self.retried_successes,
            "exhausted": self.exhausted,
            "not_retried": self.not_retried,
            "errors_total": sum(self.by_class.values()),
            "by_class": dict(self.by_class),
            "by_error_type": dict(self.by_error_type),
            "wait_total": self.wait_total,
            "avg_wait": self.wait_total / self.retries if self.retries else 0.0,
            "max_wait": self.max_wait,
            "policies": {
                klass.__name__: {
                    "max_retries": policy.max_retries,
                    "base_delay": policy.base_delay,
                    "backoff_factor": policy.backoff_factor,
                }
                for klass, policy in self.policies.items()
            },
        }

    def _default_policies(self) -> dict[type[BaseException], RetryPolicy]:
        """Разные лимиты и паузы для разных типов временных ошибок.

        При `max_retries=0` политик нет вовсе: выключенные повторы не должны
        оживать из-за того, что у 429 лимит на одну попытку больше.
        """
        if self.max_retries <= 0:
            return {}
        return {
            RateLimitError: RetryPolicy(
                # Единственный случай, когда сервер прямо просит повторить
                # позже, — ему стоит дать лишний шанс.
                self.max_retries + 1,
                max(self.base_delay, RATE_LIMIT_BASE_DELAY),
                self.backoff_factor,
            ),
            ServerError: RetryPolicy(
                # 5xx чаще означает поломку, чем всплеск: долбить её незачем.
                max(1, self.max_retries - 1),
                self.base_delay * 2,
                self.backoff_factor,
            ),
            NetworkError: RetryPolicy(
                self.max_retries,
                self.base_delay * 2,
                self.backoff_factor,
            ),
        }

    def _retryable(
        self,
        exc: BaseException,
        error: CrawlerError,
        policy: RetryPolicy,
        attempt: int,
    ) -> bool:
        """Можно ли повторить: подходит ли тип ошибки и остались ли попытки.

        Проверяются и исходное исключение, и его классификация: если вызывающий
        код перечислил в `retry_on` свои классы, они должны работать наравне с
        нашими.
        """
        if not (isinstance(exc, self.retry_on) or isinstance(error, self.retry_on)):
            self.not_retried += 1
            return False
        if attempt > policy.max_retries:
            self.exhausted += 1
            return False
        return True

    def _count_error(self, error: CrawlerError) -> None:
        self.by_class[error.error_class] += 1
        self.by_error_type[error.error_type] += 1

    def _log_give_up(
        self,
        target: str,
        error: CrawlerError,
        attempt: int,
        policy: RetryPolicy,
    ) -> None:
        if attempt > 1:
            logger.warning(
                "Повторы исчерпаны (%d попыток): %s [%s] %s",
                attempt,
                target,
                error.error_class,
                error,
            )
        else:
            logger.info(
                "Повтор не делается: %s [%s] %s%s",
                target,
                error.error_class,
                error,
                "" if policy.max_retries else " (тип не повторяется)",
            )


def _label_of(coro: Any, args: Sequence[Any]) -> str:
    """Короткое имя вызова для логов: первый аргумент обычно и есть URL."""
    name = getattr(coro, "__name__", type(coro).__name__)
    if args and isinstance(args[0], str):
        return args[0]
    return name
