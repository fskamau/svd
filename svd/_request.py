import logging
from typing import Optional
import urllib3
from typing import Callable

import re
from typing import NamedTuple
from typing import Self
from typing import Optional
from urllib3 import HTTPHeaderDict


class Range(NamedTuple):
    start: int = 0
    stop: Optional[int] = None

    def __ne__(self, other) -> bool:
        return not self == other

    def __eq__(self, other: "ContentRange") -> bool:
        assert isinstance(other, ContentRange)
        return other.__eq__(self)

    @classmethod
    def default(cls):
        return cls()

    @property
    def valid(self) -> bool:
        return True if self.stop is None else self.start < self.stop

    @property
    def as_filename(self) -> str:
        return f"{self.start}-{self.stop or ''}"

    @property
    def is_default(self) -> bool:
        return self.start == 0 and self.stop is None

    def __len__(self) -> Optional[int]:
        return None if self.stop is None else self.stop - self.start + 1

    def __str__(self) -> str:
        return f"bytes={self.as_filename}"


class ContentRange(NamedTuple):
    start: int
    stop: int
    total: int

    def __eq__(self, other: Range) -> bool:
        assert isinstance(other, Range)
        if other.is_default:
            return self.start == other.start or not self
        return self.start == other.start and self.stop == other.stop

    def __ne__(self, other) -> bool:
        return not self == other

    def __bool__(self) -> bool:
        return self.start is not None and self.stop is not None

    def __str__(self):
        return f"bytes {self.start}-{self.stop}/{self.len}"

    @classmethod
    def default(cls) -> Self:
        return cls(None, None, None)

    @property
    def len(self) -> Optional[int]:
        if not self:
            return None
        return self.stop - self.start + 1

    @classmethod
    def from_headers(cls, headers: HTTPHeaderDict, logger: logging.Logger) -> "ContentRange":
        if not isinstance(headers, HTTPHeaderDict):
            raise TypeError(headers)
        content_range = None
        try:
            content_range = tuple(map(int, re.findall(r"\d+", cr := headers.get("content-range"))))
            if len(content_range) != 3 or content_range[0] >= content_range[1] or content_range[1] >= content_range[2]:
                logger.error(f"malformed or range not satisfiable content range {cr!r}")
                content_range = None
        except (ValueError, TypeError):
            pass
        return cls(*([content_range] if content_range else [None, None, None]))

    @property
    def is_none(self) -> bool:
        return self.start is None and self.stop is None


def check_clcr(
    cl: Optional[int],
    rq_r: Range,
    rc_cl: Optional[int],
    rc_cr: ContentRange,
    logger: logging.Logger,
) -> bool:
    """
    args
    ---
    cl: full content length that you may get from a HEAD request
    rc_r: requested range. Full file request is 'bytes 0-' which is equal to Range.default
    rc_cl: received content length (content-range[1]-content-range[0]+1)
    rc_cr: received content range
    """
    if cl is not None and rc_cr and cl != rc_cr.total:
        logger.critical(f"content length from 'HEAD' is {cl} but content range indicate it is {rc_cr.total}  from {rc_cr} and {rc_cl}")
    if rq_r != rc_cr:
        logger.critical(f"requested range != received content range {rq_r} != {rc_cr}")
        return False
    return True


class ProgressFormatter:
    def __init__(self, total: Optional[int] = None, func: Optional[Callable[["ProgressFormatter", int], str]] = None):
        if total == 0:
            raise ZeroDivisionError(total)
        self.total = total
        self.downloaded = 0
        self.func = func or ProgressFormatter.default_progress_formatter

    def __call__(self, downloaded: Optional[int] = None) -> str:
        """Update progress and return formatted string."""
        return self.func(self, downloaded)

    @staticmethod
    def _do_nothing(*args, **kwargs) -> str:
        """Empty formatter for disabled progress output."""
        return ""

    @classmethod
    def default(cls) -> "ProgressFormatter":
        """Return a no-op ProgressFormatter (does nothing)."""
        pf = cls(total=None, func=cls._do_nothing)
        return pf

    def default_progress_formatter(self, downloaded: Optional[int] = None) -> str:
        """Default formatter showing percentage progress."""
        if downloaded is not None:
            self.downloaded += downloaded
        if self.total is None:
            return ""
        return f"{self.downloaded / self.total * 100.0:.2f}%"
