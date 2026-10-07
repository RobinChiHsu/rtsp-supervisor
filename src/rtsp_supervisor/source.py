from __future__ import annotations

from typing import Protocol, TypeVar
from urllib.parse import urlsplit, urlunsplit

T_co = TypeVar("T_co", covariant=True)


class SourceError(Exception):
    pass


class FrameSource(Protocol[T_co]):
    def open(self) -> None: ...

    def read(self) -> T_co: ...

    def close(self) -> None: ...


def redact_url(url: str) -> str:
    parts = urlsplit(url)
    userinfo, separator, hostport = parts.netloc.rpartition("@")
    if not separator or ":" not in userinfo:
        return url
    username = userinfo.split(":", 1)[0]
    return urlunsplit(parts._replace(netloc=f"{username}:***@{hostport}"))
