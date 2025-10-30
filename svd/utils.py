from typing import Optional
import urllib3
import logging
import re
from pathlib import Path
import shutil
from urllib.parse import urlparse

import mimetypes
from urllib3._collections import HTTPHeaderDict
import threading
import tempfile


def format_file_size(bytes_: int) -> str:
    sizes = " KMGT"
    if not isinstance(bytes_, (int, float)):
        raise TypeError(bytes_)
    i = 0
    while bytes_ >= 1024 and i < len(sizes) - 1:
        bytes_ /= 1024.0
        i += 1
    k = f"{int(bytes_):,d}" if int(bytes_) == bytes_ else f"{bytes_:,f}"
    return f"{k}{sizes[i]}B".replace(" ", "")


def join_parts(dest: Path, src_dir: Path) -> None:
    ds = sorted(src_dir.iterdir(), key=lambda x: int(x.parts[-1].split("-")[-2]))
    if dest.exists():
        raise FileExistsError("unreachable condition output video filepath exists")
    with dest.open("wb") as wrt:
        for x in ds:
            with (src_dir / x).open("rb") as sfd:
                shutil.copyfileobj(sfd, wrt)



def format_time(seconds):
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h} hr {m} min {s} sec" if h else (f"{m} min {s} sec" if m else f"{s} sec")


def format_bandwidth(bps):
    units = " KMGT"
    i = 0
    while bps >= 1000 and i < len(units) - 1:
        bps /= 1000.0
        i += 1
    return f"{int(bps):,d}{units[i]}bps"



def rm_part_dir(dir_: str, no_keep: bool) -> None:
    if no_keep:
        shutil.rmtree(dir_)


def get_base_url(url: str) -> str:
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}"


def get_extension_from_headers(headers: HTTPHeaderDict) -> str:
    ctype = headers.get("content-type")
    if not ctype:
        return ""
    ext = mimetypes.guess_extension(ctype.split(";", 1)[0].strip())
    return ext or ""


def get_thread_name() -> str:
    return threading.current_thread().name


def save_response_to_temp_file(b: bytes) -> str:
    temp_fname = tempfile.NamedTemporaryFile(delete=False)
    temp_fname.write(b)
    temp_fname.close()
    return temp_fname


def get_folder_size(path:Path) -> int:
    path = Path(path)
    total = 0
    for p in path.rglob('*'):
        try:
            if p.is_file():
                total += p.stat().st_size
        except (FileNotFoundError, PermissionError):
            continue
    return total
