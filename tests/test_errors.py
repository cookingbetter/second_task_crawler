"""Тесты классификации ошибок (День 5)."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime

import aiohttp
import pytest

from src.errors import (
    CrawlerError,
    CrawlerTimeoutError,
    NetworkError,
    ParseError,
    PermanentError,
    RateLimitError,
    ServerError,
    TransientError,
    from_exception,
    from_http_status,
    parse_retry_after,
)
from src.models import (
    ERROR_HTTP,
    ERROR_NETWORK,
    ERROR_PARSE,
    ERROR_TIMEOUT,
    ERROR_UNKNOWN,
)


def response_error(status: int, headers: dict[str, str] | None = None):
    """Собирает ClientResponseError так, как его поднимает raise_for_status."""
    return aiohttp.ClientResponseError(
        request_info=None,
        history=(),
        status=status,
        message="тестовая ошибка",
        headers=headers,
    )


# --- статусы в классы -------------------------------------------------------


@pytest.mark.parametrize(
    "status, expected",
    [
        (429, RateLimitError),
        (500, ServerError),
        (502, ServerError),
        (503, ServerError),
        (504, ServerError),
        (408, TransientError),
        (400, PermanentError),
        (401, PermanentError),
        (403, PermanentError),
        (404, PermanentError),
        (410, PermanentError),
    ],
)
def test_status_picks_error_class(status, expected):
    error = from_http_status(status, "тестовая ошибка")

    assert type(error) is expected
    assert error.status == status
    assert error.error_type == ERROR_HTTP


def test_transient_classes_stay_under_one_parent():
    """Политики повторов ищутся по иерархии, поэтому она важна сама по себе."""
    assert issubclass(RateLimitError, TransientError)
    assert issubclass(ServerError, TransientError)
    assert issubclass(CrawlerTimeoutError, TransientError)
    assert not issubclass(PermanentError, TransientError)
    assert not issubclass(NetworkError, TransientError)


def test_message_keeps_status_and_text():
    error = from_http_status(404, "Not Found")

    assert str(error) == "HTTP 404: Not Found"
    assert error.error_class == "PermanentError"


def test_retry_after_is_read_from_headers():
    error = from_http_status(429, "slow down", headers={"Retry-After": "7"})

    assert error.retry_after == pytest.approx(7.0)


def test_permanent_status_ignores_retry_after():
    """404 не повторяется, и обещание сервера подождать ничего не меняет."""
    error = from_http_status(404, "нет", headers={"Retry-After": "7"})

    assert error.retry_after is None


# --- исключения в классы ----------------------------------------------------


def test_timeout_becomes_transient():
    error = from_exception(asyncio.TimeoutError())

    assert isinstance(error, CrawlerTimeoutError)
    assert error.error_type == ERROR_TIMEOUT
    assert str(error) == "превышен таймаут"


def test_server_timeout_is_timeout_not_network():
    """aiohttp.ServerTimeoutError — подкласс и ClientError, и TimeoutError."""
    error = from_exception(aiohttp.ServerTimeoutError("медленно"))

    assert isinstance(error, CrawlerTimeoutError)


def test_response_error_becomes_http_class():
    error = from_exception(response_error(503, {"Retry-After": "2"}))

    assert isinstance(error, ServerError)
    assert error.status == 503
    assert error.retry_after == pytest.approx(2.0)


def test_connection_error_becomes_network():
    error = from_exception(aiohttp.ClientConnectionError("соединение отвергнуто"))

    assert isinstance(error, NetworkError)
    assert error.error_type == ERROR_NETWORK
    assert "ClientConnectionError" in str(error)


def test_unknown_exception_stays_unknown():
    error = from_exception(ValueError("что-то своё"))

    assert type(error) is CrawlerError
    assert error.error_type == ERROR_UNKNOWN
    assert "ValueError" in str(error)


def test_our_error_passes_through_unchanged():
    original = ParseError("битый HTML")

    assert from_exception(original) is original
    assert original.error_type == ERROR_PARSE


# --- Retry-After ------------------------------------------------------------


@pytest.mark.parametrize("value, expected", [("0", 0.0), ("5", 5.0), (" 2.5 ", 2.5)])
def test_retry_after_seconds(value, expected):
    assert parse_retry_after(value) == pytest.approx(expected)


def test_retry_after_http_date_counts_from_now():
    moment = datetime.now(timezone.utc) + timedelta(seconds=30)

    delay = parse_retry_after(format_datetime(moment))

    assert delay is not None
    assert 25 <= delay <= 31


def test_retry_after_past_date_means_right_now():
    moment = datetime.now(timezone.utc) - timedelta(minutes=5)

    assert parse_retry_after(format_datetime(moment)) == 0.0


@pytest.mark.parametrize("value", [None, "", "   ", "скоро", "-5"])
def test_retry_after_garbage_is_ignored(value):
    assert parse_retry_after(value) is None
