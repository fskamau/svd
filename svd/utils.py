from typing import Optional
import urllib3
import logging
import re
from pathlib import Path
import shutil
from urllib.parse import urlparse


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


def get_range_from_headers(headers):
    range_header = request_headers.get("Range", None)
    if range_header:
        range_value = range_header.split("=")[1]
        byte_range = range_value.split("-")
        try:
            start_byte = int(byte_range[0])
            end_byte = int(byte_range[1]) if byte_range[1] else None
            return [start_byte, end_byte] if end_byte is not None else [start_byte]
        except ValueError:
            return None
    return None


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


def get_part_name_from_content_range(content_range: tuple[int]) -> str:
    return f"{'-'.join(tuple(map(str,content_range)))}"


def rm_part_dir(dir_: str, keep: bool) -> None:
    if not keep:
        shutil.rmtree(dir_)


def child_read_stdin() -> str:
    assert not Data.MESSAGE_EVENT.is_set()
    Data.MESSAGE_QUEUE.put("/stdin")
    Data.MESSAGE_EVENT.wait()
    x = Data.MESSAGE_QUEUE.get_nowait()
    Data.MESSAGE_EVENT.clear()
    return x


def get_base_url(url: str) -> str:
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}"
