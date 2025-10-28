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

from .options import get_options
from pathlib import Path
from typing import Callable

import logging

import urllib3
import ssl


from enum import Enum, unique
from typing import Optional, Tuple

@unique
class CLCRStatus(Enum):
    OK = "ok"
    RQCR_NQ_RCCR = "requested cr does not match received cr"
    RCCR_NQ_RCCR_RCCR0="requested cr does not match received cr but received cr starts at byte 0-"
    NRQCR_RCCR_NQ_RCCL = "did not request cr, received cr length != received cl"
    RQCR_NRCCRCL = "requested cr but did not receive neither cr or cl "
    RQCR_NRCCR_RQCL_NRCCL = "requested cr but did not receive cr and received cl != requested cl"
    INVALID_RQ = "requested cl and cr are inconsistent"
    INVALID_RC = "received cl and cr are inconsistent"

class CLCRHandler:
    MAX_RETRIES = 10
    SLEEP_SECONDS = 1

    @staticmethod
    def is_error(status: CLCRStatus) -> bool:
        return status not in [
            CLCRStatus.OK,
            CLCRStatus.RCCR_NQ_RCCR_RCCR0,            
        ]

    @staticmethod
    def check(
        rq_cl: Optional[int],
        rq_cr: Optional[Tuple[int,int]],
        rc_cl: Optional[int],
        rc_cr: Optional[Tuple[int,int]],
        logger:logging.Logger,
    ) -> CLCRStatus:   
        # infer lengths from ranges if missing
        if rq_cr and rq_cl is None:
            rq_cl = rq_cr[1] - rq_cr[0] + 1
        if rc_cr and rc_cl is None:
            rc_cl = rc_cr[1] - rc_cr[0] + 1
        if rq_cl and rq_cr is None:
            rq_cr = (0, rq_cl - 1)
        if rc_cl and rc_cr is None:
            rc_cr = (0, rc_cl - 1)

        logger.debug(f"[rq_cl={rq_cl}, rq_cr={rq_cr}, rc_cl={rc_cl}, rc_cr={rc_cr}] Checking CL/CR consistency")

        #requested consistency
        if rq_cl is not None and rq_cr is not None:
            if (rq_cr[1] - rq_cr[0] + 1) != rq_cl:
                logger.critical(CLCRStatus.INVALID_RQ)
                return CLCRStatus.INVALID_RQ

        #received consistency
        if rc_cl is not None and rc_cr is not None:
            if (rc_cr[1] - rc_cr[0] + 1) != rc_cl:
                logger.critical(CLCRStatus.INVALID_RC)
                return CLCRStatus.INVALID_RC

        #cross checks requested vs received
        #this is recoverable if rc_cr is from 0-end       
        if rq_cr and rc_cr:
            if rq_cr != rc_cr:
                if rc_cr[0]==0:
                    return CLCRStatus.RCCR_NQ_RCCR_RCCR0
                logger.critical(CLCRStatus.RQCR_NQ_RCCR)
                return CLCRStatus.RQCR_NQ_RCCR

        if rq_cr and not (rc_cr or rc_cl):
            logger.critical(CLCRStatus.RQCR_NRCCRCL)
            return CLCRStatus.RQCR_NRCCRCL#not received any
            
        if rq_cl and rc_cl and rq_cl != rc_cl:
            logger.critical(CLCRStatus.RQCR_NRCCR_RQCL_NRCCL)
            return CLCRStatus.RQCR_NRCCR_RQCL_NRCCL

        return CLCRStatus.OK
    
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
        output_video = os.path.join(Data.WDIR_COMPLETE, f"{main_name}.mp4")
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

        [
            _
            for _ in Data.EXEC.map(
                lambda x: Utils.download(x + 1, len(segments), log, None, get_ts_file_path(x), get_segment_url(x), headers, counterl, True),
                range(len(segments)),
            )
        ]
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

    def get_main_name(url: str) -> str:
        return hashlib.md5(url.encode("utf-8")).hexdigest()

    def get_size_from_filename(filepath: str) -> str:
        return f"{os.path.getsize(filepath)/(1024**2):,.4f} Mb"

    # def check_video_already_exists(output_video: str, log: Rlogger, fail: bool = False) -> None:
    #     if os.path.exists(output_video):
    #         log(f" {output_video} alredy exists {Utils.get_size_from_filename(output_video)}", error=True, exit_=fail)
    #         return True
    #     return False

    # def get_content_length(headers: urllib3.HTTPHeaderDict, log: Rlogger) -> Optional[int]:
    #     content_length = None
    #     try:
    #         content_length = int(v := headers.get("content-length"))
    #     except (ValueError, TypeError):
    #         log(f"cannot get content-length from {v}", critical=True)
    #     return content_length

    # def get_content_range(headers: urllib3.HTTPHeaderDict, log: Rlogger) -> Optional[tuple[int]]:
    #     content_range = None
    #     try:
    #         content_range = list(map(int, re.findall(r"\d+", cr := headers.get("content-range"))))
    #         if len(content_range) != 3:
    #             log(f"malformed content range '{cr}'", critical=True)
    #             content_range = None
    #     except (ValueError, TypeError):
    #         pass
    #     return content_range

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
        verbose(log(f"joined part-files into {output_video}", emit=False))

    def make_request(method, url, headers, log, allow_mime_text: bool = True, preload_content: bool = True, enforce_content_length: bool = True):
        try:
            r = Data.http.request(method=method, url=url, headers=headers, preload_content=preload_content, enforce_content_length=enforce_content_length)
        except urllib3.exceptions.MaxRetryError as e:
            log(repr(e), exit_=True, error=True)
        verbose(log(Utils.summarize_headers(r.headers), emit=False))
        if str(r.status)[0] == "4":
            raise StatusCodeException(r)
        cr = Utils.get_content_range(r.headers, log)
        cl = Utils.get_content_length(r.headers, log)
        if allow_mime_text == False:
            if (ctype := r.headers.get("content-type")) is not None:
                if "text" in ctype and r.status not in (206,):
                    temp_fname = tempfile.NamedTemporaryFile(delete=False)
                    temp_fname.write(r.data)
                    log([url, r.headers], error=True)
                    temp_fname.close()
                    log(f"content type is {ctype}. status code {r.status} saving contents to {temp_fname.name}", error=True, exit_=True)
        return cr, cl, r

    def download(
        jobid: int,
        total_jobs: int,
        log: logging.Logger,
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
            verbose(log(f"skipping {file_path} since it exists {counterl[2](counterl,os.path.getsize(file_path))}", critical=True, emit=False))
            return
        headers["range"], cr1_is_none = ("bytes=0-", True) if cr1 is None else (f"bytes={'-'.join(tuple(map(str,cr1)))}", False)
        verbose(log(f"downloading content range {headers['range']}", emit=False))
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
        if not preload_content:
            with open(file_path, "wb") as wrt:
                verbose(log(f".writing into {file_path}", emit=False))
                while 1:
                    chunk = r.read(Data.CHUNK_READ_SIZE)
                    if not chunk:
                        break
                    wrt.write(chunk)
        else:
            r.data
            with open(file_path, "wb") as wrt:
                verbose(log(f"writing into {file_path}", emit=False))
                wrt.write(r.data)
        s = os.path.getsize(file_path)
        if not cr1_is_none and s > cl:
            log(f"file {file_path} has size {s} which is  greater than requested one {cl} ", error=True, exit_=True)
        if not cr1_is_none and s < cl:
            log(f"Incomplete read. file {file_path} has size {s} which is  lesser than requested one {cr1[1]-cr1[0]}", error=True, exit_=True)
        log(f"wrote file{file_path} {Utils.format_file_size(s)} {counterl[2](counterl,s)}")

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

    def rm_part_dir(dir_: str, log: Optional[logging.Logger] = None) -> None:
        if log is None:
            log = Data.LOG
        if Data.DELETE_SEGMENTS_AFTER_COMPLETE:
            log(f"rm part folder {dir_}")
            shutil.rmtree(dir_)
        else:
            log(f"not removing {dir_} since DELETE_SEGMENTS_AFTER_COMPLETE is False")

    def child_read_stdin() -> str:
        assert not Data.MESSAGE_EVENT.is_set()
        Data.MESSAGE_QUEUE.put("/stdin")
        Data.MESSAGE_EVENT.wait()
        x = Data.MESSAGE_QUEUE.get_nowait()
        Data.MESSAGE_EVENT.clear()
        return x

    def _download(q: multiprocessing.Queue) -> None:
        # Data.init()
        # Data.stats()
        # Data.check_dependecies()

        while 1:
            Utils.print_what()
            sys.stdout.flush()
            i = q.get()
            if i == "q":
                break
            if i == "print_what":
                continue
            try:
                try:
                    data = json.loads(i)
                except:
                    if re.search(r"https*\://.+$", i):
                        data = {"url": i, "headers": {}, "type": "raw"}
                    else:
                        raise
                if not isinstance(data, dict):
                    Data.LOG("expected a dict", error=True, exit_=True)
                download_perf_counter = time.perf_counter()
                if data["type"] == "segments":
                    SD.download(data)
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
        verbose(log(f"from HEAD request: {Utils.summarize_headers(r.headers)}", emit=False))
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
            verbose(log(f"total size of parts present = {Utils.format_file_size(size_present)}", emit=False))
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

        counterl = [
            size_present,
            (content_length if content_length is not None else content_range[-1] if content_range is not None else None),
            format_download_progress,
        ]
        [
            _
            for _ in Data.EXEC.map(
                lambda x: Utils.download(
                    x[0] + 1,
                    len(content_ranges),
                    log,
                    x[1],
                    os.path.join(output_dir, Utils.get_part_name_from_content_range(x[1])),
                    url,
                    headers,
                    counterl,
                    False,
                ),
                enumerate(content_ranges),
            )
        ]
        Utils.join_parts(output_video, output_dir, counterl[1], log)
        log(f"Download done {output_video} size {Utils.get_size_from_filename(output_video)}")
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
        FbIg._download(data, log)

    def _download(data: dict, log) -> None:
        # download  init files first
        jobs = itertools.cycle(data["jobs"])
        for x in data["jobs"]:
            Utils.download(
                "init",
                1,
                log,
                None,
                os.path.join(data["output_dir"], x, FbIg.INIT_FILENAME),
                data["jobs"][x]["init_url"],
                data["headers"],
                [0, 0, lambda _, __: "dowloaded init file"],
                True,
            )
        # we take item from each job
        current_media_number = [data["jobs"][k]["media_number"] for k in list(data["jobs"])]
        if len(current_media_number) == 2:
            if current_media_number[0] != current_media_number[1]:
                log(f"media_number mismatch {current_media_number}. endfile could be corrupted. defaulting to most minimum {current_media_number:=min(current_media_number)}")
            current_media_number = min(current_media_number)
        else:
            current_media_number = current_media_number[0]

        log(f"media_number starting at {FbIg.DEFAULT_PREDICTED_MEDIA_START} current_media_number {current_media_number}")
        for x in data["jobs"]:
            data["jobs"][x]["current_task"] = FbIg.DEFAULT_PREDICTED_MEDIA_START

        def generate_urls():
            while 1:
                for f in data["jobs"]:
                    current_task = data["jobs"][f]["current_task"]
                    data["jobs"][f]["current_task"] += 1
                    yield f"{f}@{current_task}", current_media_number if current_media_number > current_task else FbIg.INFINITY, log, None, os.path.join(data["output_dir"], f, f"{current_task}"), data["jobs"][f]["media_url"].replace(
                        FbIg.MEDIA_NUMBER_STR, f"{current_task}"
                    ), data["headers"], [0, 0, lambda _, __: ""], True

        genv = generate_urls()
        # we use threads to download whole stream till last media. then change to only 1 thread to prevent 404
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
                log(f"ecountered StatusCodeException with status code {e.r.status}; saving file now", critical=True)
                FbIg.join(data, log)
                break

    def init_dirs(data: dict, log: logging.Logger):
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
            Data.LOG(f"cannot get your choice from '{i}'", error=True, exit_=True)


from  concurrent.futures import Future
from . import exceptions
from urllib3 import HTTPHeaderDict


class Djob:
    def __init__(self,url:str,headers:dict,others:dict):
        self.url=url
        self.headers=HTTPHeaderDict(headers)
        self.others=others
        self.fo=FileObject(url)
        
class Downloader:
    def __init__(self):
        self.logger=rlogger.get_adapter(Options.logger,threading.current_thread().name)

    def parse_data(self,data:dict)->dict:
        try:
            isinstance(data['url'],str)
        except Exception as e:
            raise exceptions.SVDHelpExit("expects key 'url' in json containing a str")
        headers=data.get('headers',{})
        if 'headers' in data:data.pop('headers')
        if not isinstance(headers,dict):
            raise exceptions.SVDHelpExit("expects optional key 'header' in json containing a dict")
        return Djob(data.pop('url'),headers,data)
        
            
    def download(self,data:dict):
        djob=self.parse_data(data)
        print(djob)

        
class Svdwr:
    """
    for sync reading and  sync to the writing to the terminal
    MainThread is probably running this instance
    """
    
    def __init__(self,downloader:Downloader):
        self.__downloader__=downloader
        self.__lock__=rlogger._log_lock
        self.__future_message__=None
        self.logger=Options.logger
        self.logger.info('𝘚𝘝𝘋')
        self.__write__()

    @property
    def __expects_message__(self)->bool:
        return isinstance(self.__future_message__,Future) and not self.__future_message__.done()

    def __set_message__(self,message:str)->None:
        if not self.__expects_message__:
            raise RuntimeError("No future expecting a message")
        self.__future_message__.set_result(message)

    @staticmethod
    def get_input()->str:
        i=input('> ')
        if len(i)>4096-1-1:
            raise exceptions.SVDHelpExit("text too long. some terminal will cut the text. Save text in a file  'f' and use `svd -r f` instead")
        return i
    
    def __write__(self)->None:
        while 1:
            try:                    
                i=Options.get_input_from_file() or Svdwr.get_input()
                if self.__expects_message__:
                    self.__set_message__(i)
                    continue
                if i=='.' or i=='q':
                    break
                if i == "":
                    print(f"use q or . to exit, 'svd -h' for help or  paste from clipboard")
                    continue
                try:
                    i=json.loads(i)
                    if not isinstance(i,dict):
                        raise  TypeError(f'expected  a dict got {type(i)}')
                    self.__downloader__.download(i)
                except (json.JSONDecodeError,TypeError) as e:
                    self.logger.error(f'cannot parse input json; {repr(e)}')
            except exceptions.SVDHelpExit as e:
                self.logger.critical(e.msg)
                sys.exit(1)
            except KeyboardInterrupt:
                return
        

    def read(self)->str:
        if self.__expects_message__:
            self.__future_message__.result()
            return self.read()
        else:
            self.__future_message__=Future()
            with self.__lock__:
                return self.__future_message__.result()


       

Options=get_options()


class FileObject:
    """
    aliases for file paths for a download
    """

    def __init__(self, url:str):
        self.__hash__ = self.hash_url(url)
        self.parts_dir = Path(Options.parts_dir / self.__hash__) / "parts"
        self.mime_type: str = ""
        self.initialize_dirs()

    @property
    def complete_download(self) -> Path:
        return Path(Options.complete_dir / (self.__hash__ + self.mime_type))

    def initialize_dirs(self) -> None:
        if not self.parts_dir.exists():
            self.parts_dir.mkdir(parents=True, exist_ok=True)
            Options.logger.info(f"created dir {str(self.parts_dir)!r}")

    def get_completed_filename(self, mime_type: Optional[str] = None) -> Path:
        complete_fp = Path()
        if mime_type:
            if mime_type[0] != ".":
                raise ValueError(f"expects a . in mime_type {mime_type!r}")
            return Options.complete_dir / (str(self.__completed_file__) + mime_type)
        return self.complete_download

    @staticmethod
    def hash_url(url: str) -> str:
        return hashlib.md5(url.encode("utf-8")).hexdigest()

    @property
    def fully_downloaded(self) -> bool:
        """
        return: whether a file is completely downloaded
        """
        return self.complete_download.exists()

from . import rlogger

if __name__ == "__main__":
    Svdwr(Downloader())
