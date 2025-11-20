from typing import Optional
import logging
import re
from pathlib import Path
import shutil
from urllib.parse import urlparse
import mimetypes
from urllib3 import HTTPHeaderDict
import threading
import tempfile
from ._request import Range

from . import exceptions


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


def get_folder_size(path: Path) -> int:
    path = Path(path)
    total = 0
    for p in path.rglob("*"):
        try:
            if p.is_file():
                total += p.stat().st_size
        except (FileNotFoundError, PermissionError):
            continue
    return total


def get_missing_ranges(byte_end: int, part_size, pcrs: Optional[tuple[tuple[int, int]]] = None, byte_start: int = 0) -> tuple["Range"]:
    ranges = []
    if pcrs:
        for i, x in enumerate(pcrs):
            if x[0] == byte_start:
                byte_start = x[1] + 1
            else:  # some backward bytes are missing (byte_start...x[0])
                print(byte_end, part_size, pcrs, byte_start)
                [ranges.append(x) for x in get_missing_ranges(x[0], part_size, None, byte_start)]
                byte_start = x[1] + 1
    while byte_start < byte_end:
        end = min(byte_start + part_size - 1, byte_end - 1)
        ranges.append((byte_start, end))
        byte_start = end + 1
    return [Range(*c) for c in ranges]


def update_part_file(f: Path, logger: logging.Logger) -> Optional[tuple[tuple[int, int], int]]:
    """
    update the name of an existing part file if its name does not reflect its size and
    return the new name
    else delete the file if its empty and return None
    """
    if not (v := re.search(r"(\d+)-(\d+)", f.name)):
        raise FileExistsError(f"other files exists in parts dir {f.parent} e.g {str(f)!r}. not continuing")
    v = list(map(int, [v.group(1), v.group(2)]))
    if v[0] >= v[1]:
        raise exceptions.CorruptedPartsDir(f"malformed part file name {f!r}")
    if (fsize := (f.stat().st_size)) == 0:
        logger.debug(f"removing part file {f} since size is 0")
        f.unlink()
        return None
    if fsize != (fname_size := (v[1] - v[0] + 1)):
        if fsize > fname_size:
            raise exceptions.CorruptedPartsDir(
                f"malformed part file range name for {f}; actual partfile size" f"{fsize} is greather than max indicated {fname_size}"
            )
        v[1] = v[0] + fsize - 1
        new_part_name = f.parent / f"{v[0]}-{v[1]}"
        logger.debug(f"renaming part file {f} to {new_part_name} since its size is {fsize} and not {fname_size}")
        if new_part_name.exists():
            raise exceptions.CorruptedPartsDir(f"cannot rename. content range overlap. same filename {new_part_name} exists")
        f.rename(new_part_name)
    return v, fsize
