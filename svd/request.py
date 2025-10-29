"""
anything dealing with requests and responses
"""

import logging
from typing import Optional
import urllib3
from .options import get_options
from . import utils
from typing import Callable
import threading

from .rlogger import get_adapter
import re
from . import exceptions
from typing import NamedTuple

from enum import Enum, unique
from typing import Optional, Tuple
from urllib3 import HTTPHeaderDict
import time

Options = get_options()


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
        logger.critical(f"content length from 'HEAD' is {cl} but content range indicate it is {cr.total}  from {cr}")
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


def make_request(method, url, headers, logger, allow_mime_text: bool = True, preload_content: bool = True, enforce_content_length: bool = True):
    try:
        r = Options.http.request(method=method, url=url, headers=headers, preload_content=preload_content, enforce_content_length=enforce_content_length)
        logger.debug(summarize_headers(r.headers))
        if str(r.status)[0] == "4":
            utils.save_response_to_temp_file(r.data)
            raise exceptions.StatusCodeException(r)
        cr = get_content_range(r.headers, logger)
        cl = get_content_length(r.headers, logger)
        if allow_mime_text == False:
            if (ctype := r.headers.get("content-type")) is not None:
                if "text" in ctype and r.status not in (206,):
                    temp_fname = utils.save_response_to_temp_file(r.data)
                    logger.error([url, r.headers])
                    raise exceptions.DownloadError("mime type contains 'text' which is not allowed check headers")
        return cr, cl, r
    except urllib3.exceptions.MaxRetryError as e:
        if e.reason and isinstance(e.reason, urllib3.exceptions.SSLError):
            raise exceptions.HelpExit(f"ssl error {repr(e.reason)}. you may consider turning off ssl with `svd --no-ssl` flag which is dangerous")
    except urllib3.exceptions.MaxRetryError as e:
        raise exceptions.CannotContinue(f"max retries error; {repr(e)}")


def download(
    url: str,
    headers: urllib3.HTTPHeaderDict,
    file_path: str,
    logger: logging.Logger,
    rq_r: Optional[tuple[int]] = Range.default(),
    cl1: Optional[int] = None,
    progress_formatter: Optional[ProgressFormatter] = ProgressFormatter.default,
    preload_content: Optional[bool] = False,
):
    """
    args
    ----
    url : url 
    headers :  headers
    file_path :  where to save the file
    rq_r :  requested range
    log :  the Rlogger object
    cl1 :  total resource length
    progress_formatter :  will format the download progress
    preload_content : whether to load all content first or start streaming it directly to the file
    """
    if file_path.exists():
        logger.debug(f"skipping {str(file_path)} since it exists {utils.format_file_size(file_path.stat().st_size)} {progress_formatter(file_path.stat().st_size)}")
        return
    if "range" in headers or not rq_r.is_default:
        headers["range"] = str(rq_r)
        logger.debug(f"downloading range {headers['range']}")
    cr, cl, r = make_request("GET", url, headers=headers, logger=logger, allow_mime_text=False, preload_content=preload_content, enforce_content_length=preload_content)
    if not check_clcr(cl1, rq_r, cl, cr, logger):
        raise exceptions.DownloadError(f"cl/cr errors while downloading {file_path} try clearing parts folder with  and setting part size to max using svd --clean -s 1000TB")

    if not preload_content:
        with file_path.open("wb") as wrt:
            logger.debug(f".writing into {file_path}")
            while 1:
                chunk = r.read(Options.chunk_read_size)
                if not chunk:
                    break
                wrt.write(chunk)
                logger.info(progress_formatter(len(chunk)))
    else:
        r.data
        with file_path.open("wb") as wrt:
            logger.debug(f"writing into {file_path}")
            wrt.write(r.data)
            progress_formatter(len(r.data))
    s = file_path.stat().st_size
    if not rq_r.is_default:
        if s != len(rq_r):
            raise exceptions.DownloadError("requested range size {len(rq_r)} != size of file written {s}" f"try removing cleaning up part-folder {file_path.parent()} and" "download whole resource by setting part size to max `svd -p 1T`")
    logger.info(f"wrote file {str(file_path)} {utils.format_file_size(s)} {progress_formatter()}")


def get_content_length(headers: urllib3.HTTPHeaderDict, logger: logging.Logger) -> Optional[int]:
    content_length = None
    try:
        content_length = int(v := headers.get("content-length"))
    except (ValueError, TypeError):
        logger.debug(f"cannot get content-length from {v}", critical=True)
    return content_length


def get_content_range(headers: urllib3.HTTPHeaderDict, logger: logging.Logger) -> Optional[tuple[int, int]]:
    content_range = None
    try:
        content_range = list(map(int, re.findall(r"\d+", cr := headers.get("content-range"))))
        if len(content_range) != 3 or content_range[0] + content_range[1] + 1 != content_range[2]:
            logger.debug(f"malformed content range '{cr!r}'")
            content_range = None
    except (ValueError, TypeError):
        pass
    return ContentRange(*(content_range or [None, None, None]))


def get_content_ranges(how_much, size_of_chunk, present_content_ranges: Optional[tuple[tuple[int]]] = None, start: int = 0) -> tuple[tuple[int]]:
    ranges = []
    if present_content_ranges is not None and len(present_content_ranges) != 0:
        for i, x in enumerate(present_content_ranges):
            if x[0] == start:
                start = x[1] + 1
            else:
                [ranges.append(x) for x in get_content_ranges(x[0], Options.part_size, None, start)]
                start = x[1] + 1
    while start < how_much:
        end = min(start + size_of_chunk - 1, how_much - 1)
        ranges.append((start, end))
        start = end + 1
    return [Range(*c) for c in ranges]


def summarize_headers(headers: urllib3.HTTPHeaderDict) -> str:
    return f"header response {[f'{x}= {headers.get(x)}' for x in tuple(map(lambda x:'content-'+x,['range','length','type']))]}"
