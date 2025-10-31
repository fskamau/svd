"""
anything dealing with requests and responses
"""

import logging
from typing import Optional
import urllib3
from .options import get_options
from . import utils
from typing import Callable

import re
from . import exceptions
from typing import NamedTuple
from typing import Self
from typing import Optional
from urllib3 import HTTPHeaderDict

Options = get_options()
from ._request  import *

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
        raise exceptions.DownloadError(f"cl/cr errors while downloading {file_path} try clearing parts folder with  and setting part size to max using svd --no-keep -s 1000TB")

    if not preload_content:
        with file_path.open("wb") as wrt:
            logger.debug(f".writing into {file_path}")
            while 1:
                chunk = r.read(Options.chunk_read_size)
                if not chunk:
                    break
                wrt.write(chunk)
                logger.debug(progress_formatter(len(chunk)))
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
        logger.debug(f"cannot get content-length from {v}")
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



def summarize_headers(headers: urllib3.HTTPHeaderDict) -> str:
    return f"header response {[f'{x}= {headers.get(x)}' for x in tuple(map(lambda x:'content-'+x,['range','length','type']))]}"
