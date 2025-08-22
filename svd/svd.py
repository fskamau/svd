import argparse
import hashlib
import itertools
import json
import multiprocessing
import os
import re
import select
import shutil
import signal
import subprocess
import sys
import tempfile
import textwrap
import threading
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from enum import Enum, unique
from typing import NewType, Optional

import urllib3

from rlogger import Rlogger


class Data:
    """
    Data class
    """

    NO_OF_WORKER_THREADS = 2  # number or threads to use.
    SIZE_OF_PART = 1024**2 * 10  # size of one part
    DELETE_SEGMENTS_AFTER_COMPLETE = False  # delete the part/segment folders after complete download
    CHUNK_READ_SIZE = 1024 * 8  # size to read from socket. bigger is better but incase of an error all unwritten data is lost
    LOG_ = Rlogger("𝘚𝘝𝘋", datefmt="%H/%M:%S")
    MESSAGE_QUEUE = multiprocessing.Queue()
    MESSAGE_EVENT = multiprocessing.Event()
    WDIR = os.path.join(os.getenv("HOME"), ".svd")  # base working dir
    # complete downloads are saved in WDIR_COMPLETE
    WDIR_COMPLETE = os.path.join(os.getenv("HOME"), "Downloads")
    WDIR_PARTS: str  # parts/segments stay here
    DEPENDECIES={
            'ffmpeg':'joining media segments',
            'xsel':' pasting from clipboard'
        }
    def LOG(msg: str, **kwargs) -> None:
        with Data.LOG_LOCK:
            return Data.LOG_(msg, **kwargs)

    LOG_LOCK = multiprocessing.Lock()

    def init():
        Utils.mkdir_if_not_exists(Data.WDIR)
        Data.WDIR_PARTS = os.path.join(Data.WDIR, ".parts")
        Utils.mkdir_if_not_exists(Data.WDIR_PARTS)
        Utils.mkdir_if_not_exists(Data.WDIR_COMPLETE)
        Data.http = urllib3.PoolManager(8)
        Data.EXEC = ThreadPoolExecutor(Data.NO_OF_WORKER_THREADS, thread_name_prefix="thread")

    def str_():
        print("Options[" f"worker threads: {Data.NO_OF_WORKER_THREADS}\n"
              f"size of part: {Utils.format_file_size(Data.SIZE_OF_PART)}\n"
              f"delete segments after completion: {Data.DELETE_SEGMENTS_AFTER_COMPLETE}\n"
              f"chunk read size: {Utils.format_file_size(Data.CHUNK_READ_SIZE)}\n"
              f"working dir: {Data.WDIR}\n"
              f"temp dir: {Data.WDIR_PARTS}\n"
              f"complete-save dir: {Data.WDIR_COMPLETE}" "]")

    def stats():
        Data.str_()
        temp_size = subprocess.check_output(["du", "-sh", Data.WDIR_PARTS]).split()[0].decode("utf-8")
        if temp_size[-1] == "G":
            Data.LOG(f"temp dir {Data.WDIR_PARTS} is big {temp_size}. consider removing it.", error=True)

    def check_dependecies():
        e=False
        for x in Data.DEPENDECIES:
            try:
                subprocess.call([x],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
            except FileNotFoundError:
                Data.LOG(f"please install {x}; needed for {Data.DEPENDECIES[x]}",error=True)
                e=True
        if e:
            Data.LOG("cannot continue",exit_=True,error=True)


@unique
class CLCR(Enum):
    RQCR_NQ_RCCR = "rq-cr does not match rc-cr"
    NRQCR_RCCRL_NQ_RCCL = "did not request cr, crl from cr  != received cl"
    NRQCR_GCR = "did not rq cr got cr"
    OK = "ok"
    RQCR_NRCCR = "requested cr but did not receive cr"
    RQCR_NRCCR_RQCL_NRCCL = "requested cr but did not receive cr and received cl!= requested cl"
    ERROR_RETRIES = 10
    SLEEP_SECONDS = 1

    def is_error(clcr: "CLCR") -> bool:
        assert isinstance(clcr, CLCR)
        return clcr in [CLCR.RQCR_NQ_RCCR, CLCR.NRQCR_RCCRL_NQ_RCCL, CLCR.RQCR_NRCCR_RQCL_NRCCL]

    def __str__(self) -> str:
        return self.value.__str__()

    def check(cr1, cr, cl, log):
        if cr1 is None:
            cl1 = None
        else:
            cl1 = cr1[1] - cr1[0] + 1
        if cr is not None:
            crl = cr[1] - cr[0] + 1
        else:
            crl = None
        log = Rlogger.get_named_class_log(log, f"[rq<<cr={cr1},cl={cl1}> rc<cr={cr},cl={cl},crl={crl}>>]")
        if cl is not None and crl is not None and crl != cl:
            log(f"rc-cl does not match cl  from rc-cr {cr}; ignoring", critical=True)
        if cr is not None:
            if cr1 is not None:
                if cr[0] != cr1[0] or cr[1] != cr1[1]:
                    log(CLCR.RQCR_NQ_RCCR, error=True)
                    return CLCR.RQCR_NQ_RCCR
            else:
                if cl is not None:
                    if crl != cl:
                        log(CLCR.NRQCR_RCCRL_NQ_RCCL, error=True)
                        return CLCR.NRQCR_RCCRL_NQ_RCCL
                else:
                    return CLCR.OK
        else:
            if cr1 is not None:
                if crl != cl and cl != cl1:
                    log([CLCR.RQCR_NRCCR_RQCL_NRCCL, crl, cl, cl1], error=True)
                    return CLCR.RQCR_NRCCR_RQCL_NRCCL
                log(CLCR.RQCR_NRCCR, error=True)
                return CLCR.RQCR_NRCCR
            else:
                if cl is not None:
                    if cl == 0:
                        log(f"did not request cr, did not receive cr and cl is {cl}", error=True, exit_=True)
                    return CLCR.OK
                else:
                    log("unreachable. no rq-cr no rc-cr no rc-cl possible bug", error=True, exit_=True)
        return CLCR.OK


class SD:
    """
    segment Downloader
    """

    def concatenate_ts_files(ts_files, output_filename, output_dir):
        with open(output_filename, "wb") as wrt:
            for file in ts_files:
                with open(file, "rb") as sfd:
                    shutil.copyfileobj(sfd, wrt)

    def download(data):
        headers = data["headers"]
        segments = data["segInfo"]["segments"]
        stream_url = data["segInfo"]["streamURL"]
        ##TODO:use urllib to get the domain
        domain = re.search(r"(.+?\.\w+?)/", stream_url).group(1)
        log = Rlogger.get_named_class_log(Data.LOG, "[HLS]")
        assert len(segments) > 0, log("segment length is 0", error=True, exit_=True)

        def format_download_progress(x, y):
            x[0] += 1
            return f"{x[0]/x[1]*100:.2f}%"

        counterl = [0, len(segments), format_download_progress]
        main_name = Utils.get_main_name(data["url"])
        output_video = os.path.join(Utils.WDIR_COMPLETE, f"{main_name}.mp4")
        output_dir = os.path.join(Data.WDIR_PARTS, f"{main_name}")
        log(f"<JOB> (jobs={len(segments)}, url={stream_url}, segsfolder={output_dir}, video={output_video})", critical=True)

        if Utils.check_video_already_exists(output_video, log):
            return
        Utils.mkdir_if_not_exists(output_dir)

        def get_ts_file_path(x):
            return os.path.join(output_dir, f"{str(x)}.ts")

        def get_segment_url(x):
            if (segx := segments[x]).startswith("http"):
                return segx
            if segx[0] == "/":
                return f"{domain}{segx}"
            return f"{stream_url}/{segx}"

        [_ for _ in Data.EXEC.map(lambda x: Utils.download(x + 1, len(segments), log, None, get_ts_file_path(x), get_segment_url(x), headers, counterl, True), range(len(segments)))]
        SD.concatenate_ts_files([get_ts_file_path(x) for x in range(len(segments))], output_video, output_dir)
        log(f"download job(video= {output_video} , size={Utils.get_size_from_filename(output_video)} done.", critical=True)
        Utils.rm_part_dir(output_dir, log)


class Utils:
    """
    common functions
    """

    def get_log_lock():
        sys.stdout.flush()
        sys.stderr.flush()
        Data.LOG_LOCK.acquire()

    def release_log_lock():
        Data.LOG_LOCK.release()

    def mkdir_if_not_exists(dir_: str, log: Rlogger = None) -> bool:
        """returns whether a dir was created"""
        if os.path.exists(dir_):
            return False
        try:
            os.mkdir(dir_)
        except Exception as e:
            Data.LOG(f"cannot continue: {str(e)}", error=True, exit_=True)
        if log is None:
            log = Data.LOG
        log(f"created dir {dir_}", critical=True)
        return True

    def get_main_name(url: str) -> str:
        return hashlib.md5(url.encode("utf-8")).hexdigest()

    def get_size_from_filename(filepath: str) -> str:
        return f"{os.path.getsize(filepath)/(1024**2):,.4f} Mb"

    def check_video_already_exists(output_video: str, log: Rlogger, fail: bool = False) -> None:
        if os.path.exists(output_video):
            log(f" {output_video} alredy exists {Utils.get_size_from_filename(output_video)}", error=True, exit_=fail)
            return True
        return False

    def get_content_length(headers: urllib3.HTTPHeaderDict, log: Rlogger) -> Optional[int]:
        content_length = None
        try:
            content_length = int(v := headers.get("content-length"))
        except (ValueError, TypeError):
            log(f"cannot get content-length from {v}", critical=True)
        return content_length

    def get_content_range(headers: urllib3.HTTPHeaderDict, log: Rlogger) -> Optional[tuple[int]]:
        content_range = None
        try:
            content_range = list(map(int, re.findall(r"\d+", cr := headers.get("content-range"))))
            if len(content_range) != 3:
                log(f"malformed content range '{cr}'", critical=True)
                content_range = None
        except (ValueError, TypeError):
            log(f"cannot get content-range from {cr}")
        return content_range

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

    def get_content_ranges(how_much, size_of_chunk, present_content_ranges: Optional[tuple[tuple[int]]] = None, start: int = 0) -> tuple[tuple[int]]:
        ranges = []
        if present_content_ranges is not None and len(present_content_ranges) != 0:
            for i, x in enumerate(present_content_ranges):
                if x[0] == start:
                    start = x[1] + 1
                else:
                    [ranges.append(x) for x in Utils.get_content_ranges(x[0], Data.SIZE_OF_PART, None, start)]
                    start = x[1] + 1
        while start < how_much:
            end = min(start + size_of_chunk - 1, how_much - 1)
            ranges.append((start, end))
            start = end + 1
        return ranges

    def summarize_headers(headers: urllib3.HTTPHeaderDict) -> str:
        return f"header response {[f'{x}= {headers.get(x)}' for x in tuple(map(lambda x:'content-'+x,['range','length','type']))]}"

    def join_parts(output_video: str, output_dir: str, len_: Optional[int], log) -> None:
        ds = sorted(os.listdir(output_dir), key=lambda x: int(x.split("-")[-2]))
        if len_ is not None:
            total_part_size = 0
            for x in ds:
                total_part_size += os.path.getsize(os.path.join(output_dir, x))
            if total_part_size != len_:
                log(f"total part file sizes {total_part_size} does not match content length {len_}; saving anyway", critical=True)
        assert not os.path.exists(output_video), log("unreachable condition output video filepath exists", exit_=True, error=True)
        with open(output_video, "wb") as wrt:
            for x in ds:
                with open(os.path.join(output_dir, x), "rb") as sfd:
                    shutil.copyfileobj(sfd, wrt)
        Utils.print_over(log(f"joined part-files into {output_video}", emit=False))

    def make_request(method, url, headers, log, allow_mime_text: bool = True, preload_content: bool = True, enforce_content_length: bool = True):
        try:
            r = Data.http.request(method=method, url=url, headers=headers, preload_content=preload_content, enforce_content_length=enforce_content_length)
        except urllib3.exceptions.MaxRetryError as e:
            log(repr(e), exit_=True, error=True)
        log(Utils.summarize_headers(r.headers))
        if str(r.status)[0] == "4":
            raise StatusCodeException(r)
        cr = Utils.get_content_range(r.headers, log)
        cl = Utils.get_content_length(r.headers, log)
        if allow_mime_text == False:
            if (ctype := r.headers.get("content-type")) is not None:
                if "text" in ctype:
                    temp_fname = tempfile.NamedTemporaryFile(delete=False)
                    temp_fname.write(r.data)
                    log([url, r.headers], error=True)
                    temp_fname.close()
                    log(f"content type is {ctype}. status code {r.status} saving contents to {temp_fname.name}", error=True, exit_=True)
        return cr, cl, r

    def download(
        jobid: int,
        total_jobs: int,
        log: Rlogger,
        cr1: Optional[tuple[int]],
        file_path: str,
        url: str,
        headers: urllib3.HTTPHeaderDict,
        counterl: list[float],
        preload_content: bool,
    ):
        """
        args
        ----
        jobid; something to help distinguish logs are from which download
        total_jobs: help you know many downloads are remaining
        log: the Rlogger object
        cr1: download a certain content-range
        file_path: where to save the file
        url:
        headers:
        counterl:list of anything. last item should be a function taking (this list and Optional[size of downloaded file]=None)
                 and returning a counter. eg 42% complete  e.g counterl=[total_len,lambda x,y:print(f"{y/x[0]}% complete")]
        preload_content:whether to load all content first or start streaming it directly to the file
        """
        log = Rlogger.get_named_class_log(log, f"[jobid={jobid}/{total_jobs} {threading.current_thread().name}]")
        if os.path.exists(file_path):
            Utils.print_over(log(f"skipping {file_path} since it exists {counterl[2](counterl,os.path.getsize(file_path))}", critical=True, emit=False))
            return
        headers["range"], cr1_is_none = ("bytes=0-", True) if cr1 is None else (f"bytes={'-'.join(tuple(map(str,cr1)))}", False)
        Utils.print_over(log(f"downloading content range {headers['range']}", emit=False))
        rccc = int(CLCR.ERROR_RETRIES.value)
        while rccc != 0:
            rccc -= 1
            if rccc == 0:
                log(f"cr/cl errors after {CLCR.ERROR_RETRIES} retries.  cannot continue", error=True, exit_=True)
            cr, cl, r = Utils.make_request("GET", url, headers=headers, log=log, allow_mime_text=False, preload_content=preload_content, enforce_content_length=preload_content)
            if CLCR.is_error(CLCR.check(cr1, cr, cl, log)):
                log(f"sleeping for {CLCR.SLEEP_SECONDS.value} before retrying<{rccc} retries left> ", critical=True)
                time.sleep(CLCR.SLEEP_SECONDS.value)
                continue
            if cl is None and cr is not None:
                cl = cr[-1]
            break
        log(Utils.summarize_headers(r.headers))
        if not preload_content:
            with open(file_path, "wb") as wrt:
                Utils.print_over(log(f".writing into {file_path}", emit=False))
                while 1:
                    chunk = r.read(Data.CHUNK_READ_SIZE)
                    if not chunk:
                        break
                    wrt.write(chunk)
        else:
            r.data
            with open(file_path, "wb") as wrt:
                Utils.print_over(log(f"writing into {file_path}", emit=False))
                wrt.write(r.data)
        s = os.path.getsize(file_path)
        if not cr1_is_none and s > cl:
            log(f"file {file_path} has size {s} which is  greater than requested one {cl} ", error=True, exit_=True)
        if not cr1_is_none and s < cl:
            log(f"Incomplete read. file {file_path} has size {s} which is  lesser than requested one {cr1[1]-cr1[0]}", error=True, exit_=True)
        Utils.print_over(log(f"wrote file{file_path} {Utils.format_file_size(s)} {counterl[2](counterl,s)}", emit=False))

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

    def print_what():
        print("\033[0;103m\033[7;30m\033[0;103m\033[3;30m>>>\033[0m ", end="")

    def print_over(msg: str) -> None:
        assert msg is not None
        print(msg)
        # print(f"\x1b[1A\x1b[2K{msg}")

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
        return f"{'-'.join(tuple(map(str,content_range)))}.part"

    def rm_part_dir(dir_: str, log: Optional[Rlogger] = None) -> None:
        if log is None:
            log = Data.LOG
        if Data.DELETE_SEGMENTS_AFTER_COMPLETE:
            log(f"rm part folder {dir_}")
            shutil.rmtree(dir_)
        else:
            log(f"not removing {dir_} since DELETE_SEGMENTS_AFTER_COMPLETE is False")

    def child_read_stdin() -> str:
        assert Data.MESSAGE_QUEUE.qsize() == 0
        assert not Data.MESSAGE_EVENT.is_set()
        Data.MESSAGE_QUEUE.put("/stdin")
        Data.MESSAGE_EVENT.wait()
        x = Data.MESSAGE_QUEUE.get_nowait()
        Data.MESSAGE_EVENT.clear()
        return x

    
    def _download(q: multiprocessing.Queue) -> None:
        Data.init()
        Data.stats()
        Data.check_dependecies()
    
        while 1:
            Utils.print_what()
            sys.stdout.flush()
            i = q.get()
            if i == "q":
                break
            if i == "print_what":
                continue
            try:
                data = json.loads(i)
                if not isinstance(data, dict):
                    Data.LOG("expected a dict", error=True, exit_=True)
                download_perf_counter = time.perf_counter()
                if data["type"] == "segments":
                    download_segments(data)
                elif data["type"] == "xml":
                    if any(tuple(map(lambda x: x in data["tabURL"], ["facebook.com", "instagram.com"]))):
                        FbIg.download(data)
                    else:
                        raise NotImplementedError(f"implement xml for {data['tabURL']}")
                else:
                    Raw.download(data)
                Data.LOG(f"took {Utils.format_time(time.perf_counter()-download_perf_counter)}")
            except json.JSONDecodeError as e:
                Data.LOG(f"cannot parse json {i} {repr(e)}", exit_=True, error=True)
    
class Raw:
    def download(data: dict):
        headers = urllib3.HTTPHeaderDict(data["headers"])
        headers["range"] = "bytes=0-"
        log = Rlogger.get_named_class_log(Data.LOG, "[RAW]")
        url = data["url"]
        main_name = Utils.get_main_name(url)
        output_video = os.path.join(Data.WDIR_COMPLETE, f"{main_name}.mp4")
        output_dir = os.path.join(Data.WDIR_PARTS, f"{main_name}")
        if Utils.check_video_already_exists(output_video, log):
            return
        content_range, content_length, r = Utils.make_request("GET", url, headers, log, allow_mime_text=False, preload_content=False)
        r.release_conn()
        if CLCR.is_error(CLCR.check(None, content_range, content_length, log)):
            log(f"cannot continue; content_length/content_range error {summarize_headers(r.headers)} rq headers {headers}", error=True, exit_=True)
        if content_length is None and content_range is not None:
            content_length = content_range[-1]
        log(f"from HEAD request: {Utils.summarize_headers(r.headers)}")
        if content_length is not None and content_range is not None and content_length != content_range[-1]:
            log(f"content_length {Utils.format_file_size(content_length)} != content_range[-1] {Utils.format_file_size(content_range[-1])}", critical=True)
        size_present = 0
        present_content_ranges = []
        if not Utils.mkdir_if_not_exists(output_dir):
            log(f"checking already donwloaded parts in {output_dir}")
            if content_length is None:
                log(f"parts exists but content_length is None. assuming the server will return correct content ranges", error=True, exit_=True)
            ld = os.listdir(output_dir)
            for file in ld:
                if not (v := re.search(r"(\d+)-(\d+)\.part", file)):
                    log(f"other files exists in parts dir `{output_dir} e.g '{file}'. not continuing", error=True, exit_=True)
                v = list(map(int, [v.group(1), v.group(2)]))
                if v[0] >= v[1]:
                    log(f"malformed part file name {file}", error=True, exit_=True)
                if (part_size := os.path.getsize(part_file := os.path.join(output_dir, file))) == 0:
                    log(f"removing part file {file} since size is 0")
                    os.remove(part_file)
                    continue
                if part_size != (ppart_size := (v[1] - v[0] + 1)):
                    if part_size > ppart_size:
                        log(
                            f"malformed part file range name for {file}; actual partfile size {part_size} is greather than max indicated {ppart_size}",
                            exit_=True,
                            error=True,
                        )
                    v[1] = v[0] + part_size - 1
                    new_part_name = Utils.get_part_name_from_content_range(v)
                    log(f"renaming part file {file} to {new_part_name} since its size is {part_size}  and not {ppart_size}", critical=True)
                    if os.path.exists(new_part_name := os.path.join(output_dir, new_part_name)):
                        log(f"cannot rename. content range overlap. smae filename {new_part_name} exists", exit_=True, error=True)
                    os.rename(part_file, new_part_name)
                present_content_ranges.append(v)
                size_present += part_size
            present_content_ranges = sorted(present_content_ranges, key=lambda x: x[0])
            present_content_ranges_ = []
            if len(present_content_ranges) > 1:
                for i, pcr in enumerate(present_content_ranges):
                    if i > 0:
                        if pcr[0] <= present_content_ranges[i - 1][0] or pcr[0] <= present_content_ranges[i - 1][1]:
                            log(f"content range overlap  {present_content_ranges[i-1]} / {pcr} ", exit_=True, error=True)
                    else:
                        present_content_ranges_.append(pcr)
                        continue
                    if present_content_ranges_[-1][-1] + 1 == pcr[0]:
                        present_content_ranges_[-1][-1] = pcr[1]
                    else:
                        present_content_ranges_.append(pcr)
            else:
                present_content_ranges_ = present_content_ranges
            present_content_ranges = present_content_ranges_
            Utils.print_over(log(f"total size of parts present = {Utils.format_file_size(size_present)}", emit=False))
            if content_range is not None:
                if content_length < size_present:
                    log(f"content_length = {content_length:,d} < size_present {size_present:,d}, what do i do?", error=True, exit_=True)
                log(f"downloading remaining {Utils.format_file_size(content_length-size_present)}")
        content_ranges = Utils.get_content_ranges(content_length, Data.SIZE_OF_PART, present_content_ranges)
        log(f"<JOB> (jobs={len(content_ranges)} jobs {content_ranges})", critical=True)

        def format_download_progress(x, y):
            if x[1] is None:
                return "unknown percentage"
            x[0] += y
            return f"{x[0]/x[1]*100:.2f}%"

        counterl = [size_present, (content_length if content_length is not None else content_range[-1] if content_range is not None else None), format_download_progress]
        [_ for _ in Data.EXEC.map(lambda x: Utils.download(x[0] + 1, len(content_ranges), log, x[1], os.path.join(output_dir, Utils.get_part_name_from_content_range(x[1])), url, headers, counterl, False), enumerate(content_ranges))]
        Utils.join_parts(output_video, output_dir, counterl[1], log)
        Utils.print_over(log(f"Download done {output_video} size {Utils.get_size_from_filename(output_video)}", emit=False))
        Utils.rm_part_dir(output_dir, log)


class StatusCodeException(Exception):
    def __init__(self, r: urllib3.BaseHTTPResponse):
        self.r = r

    def __str__(self) -> str:
        return f"status code {self.r.status} headers {self.r.headers} url {self.r.url} data {self.r.data}"


class MaybeMetaAlteredCodeException(NotImplementedError):
    pass


class FbIg:
    """
    download <facebook & instagram> livestreams
    """

    DEFAULT_NS = {"": "urn:mpeg:dash:schema:mpd:2011"}
    MIMETYPE_REGEX = re.compile(r"(audio|video)/(\w+)")
    DEFAULT_PREDICTED_MEDIA_START = 100
    DOWNLOAD_WHOLE_STREAM = True
    MEDIA_NUMBER_STR = "$Number$"
    INIT_FILENAME = "0"  # for easy sorting cat $(ls -v) > out.mp4
    INFINITY = "∞"
    STATUS_CODE_ERROR_COUNT = 3
    STATUS_CODE_ERROR_SLEEP = 5
    assert STATUS_CODE_ERROR_COUNT > 0

    def join(data, log):
        paths = []
        for f in data["jobs"]:
            p = os.path.join(data["output_dir"], f)
            try:
                files = sorted(os.listdir(p), key=lambda x: int(x))  # need int('v') to fail if str file name exists
                part_file = os.path.join(p, f"{f}")
                with open(part_file, "wb") as wrt:
                    for part in files:
                        shutil.copyfileobj(open(os.path.join(p, part), "rb"), wrt)
                if data["jlen"] == 1:
                    out_file = data["output_video"]
                else:
                    paths.append(out_file := os.path.join(p, f"{f}.{data['output_video'].split('.')[-1]}"))
                Utils.check_video_already_exists(out_file, log, True)
                try:
                    subprocess.check_output(["ffmpeg", "-i", part_file, "-c", "copy", out_file])
                except subprocess.CalledProcessError:
                    log(f"cannot copy {part_file}, encoding")
                    subprocess.check_output(["ffmpeg", "-y", "-i", part_file, out_file])
                os.remove(part_file)
                log(f"joined parts to {out_file}")
            except ValueError:
                log(f"dir {data['output_dir']} contains other files; cannot continue. all filenames are ints", error=True, exit_=True)
        if data["jlen"] == 2:
            log("joining video and audio")
            subprocess.check_output(["ffmpeg", "-i", paths[0], "-i", paths[1], "-c", "copy", data["output_video"]])
        [os.remove(x) for x in paths]
        log(f"wrote {data['output_video']} {Utils.get_size_from_filename(data['output_video'])}")

    def parse_xml(xml_str: str) -> list[str]:
        """
        extract useful info from xml mpd. strictly for facebook and instagram
        """
        try:
            reps = []
            root = ET.fromstring(xml_str)
            # print(ET.tostring(root).decode())
            for seg in root.findall(".//AdaptationSet", FbIg.DEFAULT_NS):
                if (s := seg.get("segmentAlignment")) is not None:
                    if s != "true":
                        raise MaybeMetaAlteredCodeException(f"segment alignment is not 'true' {seg.attrib}. not implemented")
            for rep in root.findall(".//Representation", FbIg.DEFAULT_NS):
                if "wvtt" in rep.get("codecs"):  # skip web video text tracks like subtitles and captions
                    continue
                rd = {"mimetype": rep.attrib["mimeType"]}
                try:
                    rd["basename"], rd["file_extension"] = re.search(FbIg.MIMETYPE_REGEX, rep.attrib["mimeType"]).groups()
                except AttributeError:
                    raise MaybeMetaAlteredCodeException(f"cannot match mimeType {rep.attrib['mimeType']} to extract file extension")
                if (a := rep.get("FBQualityLabel")) is not None:
                    rd["quality"] = a
                if rd["mimetype"].startswith("audio") and (a := rep.get("bandwidth")) is not None:
                    rd["bandwidth"] = int(a)
                (st,) = rep.findall(".//SegmentTemplate", FbIg.DEFAULT_NS)
                rd["init"] = st.attrib["initialization"]
                if not rd["init"].startswith("."):
                    raise MaybeMetaAlteredCodeException(f"init url does not start with . {rd['init']}")
                (st,) = rep.findall(".//SegmentTimeline[@FBPredictedMedia][@FBPredictedMediaEndNumber]", FbIg.DEFAULT_NS)
                rd["media_number"] = int(st.attrib["FBPredictedMediaEndNumber"])
                rd["media"] = st.attrib["FBPredictedMedia"]
                if rd["media"].count(FbIg.MEDIA_NUMBER_STR) != 1:
                    raise MaybeMetaAlteredCodeException(f"MEDIA_NUMBER_STR mismatch in {rd['media']}")
                st = rep.findall(".//SegmentTimeline[@FBPredictedMediaStartNumber]", FbIg.DEFAULT_NS)
                if len(st) == 1:
                    if int(st[0].attrib["FBPredictedMediaStartNumber"]) != FbIg.DEFAULT_PREDICTED_MEDIA_START:
                        raise MaybeMetaAlteredCodeException(f"different DEFAULT_PREDICTED_MEDIA_START. dump {rd} {ET.tostring(rep).decode()}")
                else:
                    Data.LOG(
                        f"attribute FBPredictedMediaStartNumber does not exist in mime {rd['mimetype']}. defaulting to '{FbIg.DEFAULT_PREDICTED_MEDIA_START}'",
                        critical=True,
                    )
                reps.append(rd)
            return reps
        except AttributeError as e:
            Data.LOG(f"cannot parse xml {repr(e)}", exit_=True, error=True)
        except Exception as e:
            Data.LOG(f"{repr(e)}", exit_=True, error=True)

    def download(data: dict):
        formats = FbIg.parse_xml(data["xmlData"])
        data = {"url": data["url"], "headers": data["headers"], "main_name": Utils.get_main_name(data["url"])}
        data["output_dir"] = os.path.join(Data.WDIR_PARTS, f"{data['main_name']}")

        log = Rlogger.get_named_class_log(Data.LOG, "[LIVE]")
        Utils.mkdir_if_not_exists(data["output_dir"], log)
        choice = FbIg.get_desired_format_choice(formats)
        data["jlen"] = len(choice)
        # set final file extension based on choice
        data["output_video"] = os.path.join(
            Data.WDIR_COMPLETE,
            f"{data['main_name']}.{ (choice['audio'] if  data['jlen'] == 1 and list(choice)[0] == 'audio' else choice['video']  )['file_extension']}",
        )
        if Utils.check_video_already_exists(data["output_video"], log):
            return
        log(f"downloading {'video + audio' if len(choice)>1 else list(choice)[0] +' only'}")
        for c in choice:
            for k in ["init", "media"]:
                ipu = len(re.search(r"(^\.+)", choice[c][k]).group(1))
                choice[c][k + "_url"] = "/".join(data["url"].split("/")[:-ipu]) + choice[c][k][ipu:]
                choice[c].pop(k)
        data["jobs"] = choice
        FbIg.init_dirs(data, log)
        if FbIg.DOWNLOAD_WHOLE_STREAM == False:
            log("no sys.arg DHS=1 to download whole stream; will download from now on")
        else:
            log("downloading stream from start to now")
        FbIg._download(data, log)

    def _download(data: dict, log) -> None:
        # download  init files first
        jobs = itertools.cycle(data["jobs"])
        for x in data["jobs"]:
            Utils.download("init", 1, log, None, os.path.join(data["output_dir"], x, FbIg.INIT_FILENAME), data["jobs"][x]["init_url"], data["headers"], [0, 0, lambda _, __: "dowloaded init file"], True)
        # we take item from each job
        current_media_number = [data["jobs"][k]["media_number"] for k in list(data["jobs"])]
        if len(current_media_number) == 2:
            if current_media_number[0] != current_media_number[1]:
                log(f"media_number mismatch {current_media_number}. endfile could be corrupted. defaulting to most minimum {current_media_number:=min(current_media_number)}")
            current_media_number = min(current_media_number)
        else:
            current_media_number = current_media_number[0]
        if FbIg.DOWNLOAD_WHOLE_STREAM:
            start = FbIg.DEFAULT_PREDICTED_MEDIA_START
        else:
            start = current_media_number

        log(f"media_number starting at {start} current_media_number {current_media_number}")
        for x in data["jobs"]:
            data["jobs"][x]["current_task"] = start

        def generate_urls():
            while 1:
                for f in data["jobs"]:
                    current_task = data["jobs"][f]["current_task"]
                    data["jobs"][f]["current_task"] += 1
                    yield f"{f}@{current_task}", current_media_number if current_media_number > current_task else FbIg.INFINITY, log, None, os.path.join(data["output_dir"], f, f"{current_task}"), data["jobs"][f]["media_url"].replace(FbIg.MEDIA_NUMBER_STR, f"{current_task}"), data["headers"], [0, 0, lambda _, __: ""], True

        genv = generate_urls()
        # we use threads to download whole stream till last media. then change to only 1 thread to prevent 404
        if FbIg.DOWNLOAD_WHOLE_STREAM:
            log("downloading from livestream start to now")
            assert current_media_number > FbIg.DEFAULT_PREDICTED_MEDIA_START
            tasks = [next(genv) for x in range((current_media_number - FbIg.DEFAULT_PREDICTED_MEDIA_START) * len(data["jobs"]))]
            [_ for _ in Data.EXEC.map(lambda x: Utils.download(*x), tasks)]
        log("downloading from now till the stream ends.")
        # we now use the mainthread infinetly. receiveing error status code might be the end of stream
        while 1:
            try:
                dl = next(genv)
                for x in range(FbIg.STATUS_CODE_ERROR_COUNT - 1, -1, -1):
                    try:
                        Utils.download(*dl)
                        break
                    except StatusCodeException as e:
                        if x == 0:
                            raise
                        log(f"status {e.r.status}; sleeping for {FbIg.STATUS_CODE_ERROR_SLEEP}sec. remaining retries {x} ", critical=True)
                        time.sleep(FbIg.STATUS_CODE_ERROR_SLEEP)
            except StatusCodeException as e:
                log(f"ecountered StatusCodeException with staatus code {e.r.status}; saving file now", critical=True)
                FbIg.join(data, log)
                break

    def init_dirs(data: dict, log: Rlogger):
        """
        intit dir in the following format
        [outdir/video|outdir/audio]
        """
        ed = os.listdir(data["output_dir"])
        if any([x not in list(data["jobs"]) for x in ed]):
            log(f"other dirs exists in {data['output_dir']} cannot continue {ed}", error=True, exit_=True)
        for x in list(data["jobs"].keys()):
            if os.path.exists(p := os.path.join(data["output_dir"], x)):
                log(f"dir {data['main_name']}/{x} exists. format corruption if a different format has been chosen. Nothing will be removed", critical=True)
            else:
                Utils.mkdir_if_not_exists(p, log)

    def get_desired_format_choice(formats: list[str]):
        choices = []
        for index, f in enumerate(formats):
            c = f"{f['mimetype']}"
            if f.get("quality") is not None:
                c += f"@{f['quality']}"
            if f.get("bandwidth") is not None:
                c += f"@{Utils.format_bandwidth(f['bandwidth'])}"
            choices.append((index, c))
        other = list(itertools.combinations(choices, 2))
        if len(other) > 0:
            for o in other:
                if len(set([formats[x[0]]["basename"] for x in o])) == 1:  # don't add same formats like video720p and video1080p
                    continue
                choices.append([*[x[0] for x in o], " + ".join([x[1] for x in o])])
        len_choices = len(choices)
        s = ""
        colors = itertools.cycle(["\033[1;37;40m", "\033[1;30;47m"])
        for index, c in enumerate(choices):
            s += f"{next(colors)}[{index:3d} => {c[-1]}]\033[0m\n"
        s += f"{'-'*50}\nThe following {len(choices)} formats exists. please choose 1"
        Utils.get_log_lock()
        print(s)
        try:
            v = choices[abs(int(i := Utils.child_read_stdin()))]
            Data.LOG_(f"you choose '{v[-1]}'")
            Utils.release_log_lock()
            return {formats[x].pop("basename"): formats[x] for x in v[:-1]}
        except (ValueError, IndexError) as e:
            Utils.release_log_lock()
            Data.LOG(f"cannot get you choice from '{i}'", error=True, exit_=True)

            Data.LOG(f"cannot parse json {i} {repr(e)}", exit_=True, error=True)


def main():
    q = multiprocessing.Queue()
    p = multiprocessing.Process(target=Utils._download, args=(q,))
    p.start()

    def stop_p():
        if p.is_alive():
            os.kill(p.pid, signal.SIGTERM)
            p.terminate()
        return

    while 1:
        try:
            rlist, _, __ = select.select([sys.stdin], [], [], 0.1)
            if not p.is_alive():
                break
            if not rlist:
                continue            
            i = sys.stdin.readline().strip()
            if (il := Data.MESSAGE_QUEUE.qsize()) > 0:
                assert il == 1 and not Data.MESSAGE_EVENT.is_set()
                if (ii := Data.MESSAGE_QUEUE.get()) == "/stdin":
                    Data.MESSAGE_QUEUE.put(i)
                    Data.MESSAGE_EVENT.set()
                    continue
                Data.LOG(f"unknown message from child {ii}", error=True, exit_=True)
            if i == "":
                Data.LOG(f"use 'q' or '.' to exit ")
                q.put("print_what")
                continue
            if i == "q" or i == ".":
                q.put("q")
                break
            if i == "c" or i == "/":
                i = subprocess.check_output(["xsel", "-bo"]).decode().strip()
                Data.LOG(f"paste from clipboard \n{i}")
                q.put(i)
                continue
            if len(i) > 2047:
                Data.LOG("text too long. please use / or c to paste the text from clipboard. stty -icanon may not work as expected", critical=True)
                break
            q.put(i)
        except KeyboardInterrupt as e:
            stop_p()
            Data.LOG(repr(e), error=True, exit_=True)
    stop_p()
main()


class Options:
    def get_bytes(s: str):
        x, y = s[:-1], s[-1]
        if y not in "BMG":
            Data.LOG(f"cannot parse unit of size {y}  '{s}'", critical=True, exit_=True)
        try:
            x = float(x) * (1024 ** (0 if y == "B" else 2 if y == "M" else 3))
            if x <= 0:
                raise ValueError(x)
            return int(x)
        except Exception as e:
            raise

    def vsop(sop: str):
        x = Options.get_bytes(sop)
        if x < 1024**2:
            Data.LOG(f"size of part too small {Utils.format_file_size(x)}", critical=True, exit_=True)
        Data.SIZE_OF_PART = int(x)

    def vcd(cd: str):
        Data.WDIR_COMPLETE = cd

    def vwd(cd: str):
        Data.WDIR = cd

    def vwt(cd: str):
        Data.NO_OF_WORKER_THREADS = int(cd)

    def vdws(dws: str):
        Data.DOWNLOAD_WHOLE_STREAM = bool(int(dws))

    def vd(d: str):
        Data.DELETE_SEGMENTS_AFTER_COMPLETE = bool(int(d))

    def vcrs(crs: str):
        Data.CHUNK_READ_SIZE = Options.get_bytes(crs)

    
def run():
    parser = argparse.ArgumentParser(
        prog="svd",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent(
            """
                        simple video downloader(svd)
    svd is a simple downloader for common video urls passed as json.
    It can download:
                        [1] Raw videos e.g example.com/video.mp4
                        [2] Segmented videos (hls)
                        [3] Specific live videos (from facebook and Instagram)
                        [4] Specific Dash xml videos (from vkvideo.ru and ok.ru)
    
    simple json should include url,headers & type e.g
        {
            "url":"example.com/video.mp4",
            "headers":
                        {"referer":"example.com"},
            "type":"raw"
        }
    
    Since creating this json could be tedious, a simple svd browser extension
        is provided here 
    The program will continously read stdin for control signals.
    Passing / or c will read clipboard contents and treat them as json.
    passing . or q will quit immediately.
                        
                        """
        ),
    )       
    parser.add_argument("-t", "--nowt", help="number of worker threads", type=Options.vwt)
    parser.add_argument("-s", "--sop", help="size of 1 part. e.g 1M, 10M, 10G. A download will be split into parts <= to this size.", type=Options.vsop)
    parser.add_argument("-w", "--wdir", help="working dir. A '.parts' dir will be created here", type=Options.vwd)
    parser.add_argument("-c", "--complete-dir", help="where to save completeled downloads", type=Options.vcd)
    parser.add_argument(
        "-dws",
        "--download_whole_stream",
        help="whether to download whole stream when working with live video. 0 for False or any int for True",
        type=Options.vdws,
    )
    parser.add_argument("-d", "--delete", help="whether to delete segments in '.parts' folder after completion. 0 for False or any int for True", type=Options.vd)
    parser.add_argument("-r", "--chunk_read_size", help="size to read from socket. bigger is better but incase of an error all unwritten data is lost", type=Options.vcrs)
    parser.parse_args()
    main()
