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


class HLS:
    """
    segment Downloader
    """

    def concatenate_ts_files(ts_files, output_filename, output_dir):
        with open(output_filename, "wb") as wrt:
            for file in ts_files:
                with open(file, "rb") as sfd:
                    shutil.copyfileobj(sfd, wrt)

    def download(djob):
        segments = djob.others["segInfo"]["segments"]
        seg_len = len(segments)
        stream_url = djob.others["segInfo"]["streamURL"]
        domain = utils.get_base_url(stream_url)
        logger = rlogger.get_adapter(Options.logger, HLS.__name__)
        if len(segments) == 0:
            raise exceptions.HLSError("segments length is 0")
        output_video = djob.fo.get_completed_filename()
        logger.debug(f"<JOB> (jobs={seg_len}, url={stream_url}, segsfolder={djob.fo.parts_dir}, video={output_video})")

        def get_segment_url(x):
            if (segx := segments[x]).startswith("http"):
                return segx
            if segx[0] == "/":
                return f"{domain}{segx}"
            return f"{stream_url}/{segx}"

        def format_progress(self, _=None):
            self.downloaded += 1
            return f"{self.downloaded/self.total*100:.2f}%"

        progress_formatter = request.ProgressFormatter(seg_len, format_progress)
        [
            _
            for _ in Options.exec.map(
                lambda x: request.download(
                    get_segment_url(x),
                    djob.headers,
                    djob.fo.parts_dir / str(x),
                    logger=rlogger.get_adapter(logger, f"job {x+1}/{seg_len}"),
                    progress_formatter=progress_formatter,
                    preload_content=True,
                ),
                range(seg_len),
            )
        ]
        HLS.concatenate_ts_files([get_ts_file_path(x) for x in range(seg_len)], output_video, djob.fo.parts_dir)
        logger.ok(f"download done {str(output_video)} , size={output_video.stat().st_size} done.")
        utils.rm_part_dir(djob.fo.parts_dir, logger)


class Raw:
    def download(djob: "Djob"):
        djob.headers["range"] = "bytes=0-"
        logger = rlogger.get_adapter(Options.logger, Raw.__name__)
        if djob.fo.check_completed_download(logger=logger):
            return
        content_range, content_length, r = request.make_request(
            "GET", djob.url, djob.headers, logger, allow_mime_text=False, preload_content=False
        )  # some servers will return 403 with 'HEAD'
        r.release_conn()
        logger.debug(f"from HEAD request: {request.summarize_headers(r.headers)}")
        if content_range.total != content_length:
            logger.debug(f"content_length {content_length} != content_range total {content_range}")
        size_present = 0
        present_content_ranges = []
        pdir = str(djob.fo.parts_dir)

        if any(djob.fo.parts_dir.iterdir()):
            logger.debug("checking already donwloaded parts")
            if content_length is None:
                logger.critical(f"parts exists but content_length is None. cannot assume the server will return correct content ranges; remove {pdir} manually")
            ld = os.listdir(djob.fo.parts_dir)
            for file in ld:
                if not (v := re.search(r"(\d+)-(\d+)", file)):
                    raise FileExistsError(f"other files exists in parts dir {pdir} e.g {file!r}. not continuing")
                v = list(map(int, [v.group(1), v.group(2)]))
                if v[0] >= v[1]:
                    raise exceptions.CorruptedPartsDir(f"malformed part file name {file!r}")
                if (part_size := (part_file := (djob.fo.parts_dir / file)).stat().st_size) == 0:
                    logger.debug(f"removing part file {file!r} since size is 0")
                    part_file.unlink()
                    continue
                if part_size != (ppart_size := (v[1] - v[0] + 1)):
                    if part_size > ppart_size:
                        raise exceptions.CorruptedPartsDir(
                            f"malformed part file range name for {file}; actual partfile size" f"{part_size} is greather than max indicated {ppart_size}"
                        )
                    v[1] = v[0] + part_size - 1
                    new_part_name = djob.fo.parts_dir / utils.get_part_name_from_content_range(v)
                    logger.critical(f"renaming part file {file} to {new_part_name} since its size is {part_size}  and not {ppart_size}")
                    if (new_part_name).exists():
                        raise exceptions.CorruptedPartsDir(f"cannot rename. content range overlap. same filename {new_part_name} exists")
                    part_file.rename(new_part_name)
                present_content_ranges.append(v)
                size_present += part_size
            present_content_ranges = sorted(present_content_ranges, key=lambda x: x[0])
            present_content_ranges_ = []
            if len(present_content_ranges) > 1:
                for i, pcr in enumerate(present_content_ranges):
                    if i > 0:
                        if pcr[0] <= present_content_ranges[i - 1][0] or pcr[0] <= present_content_ranges[i - 1][1]:
                            raise exceptions.CorruptedPartsDir(f"content range overlap  {present_content_ranges[i-1]} / {pcr} ")
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
            logger.info(f"total size of parts present : {utils.format_file_size(size_present)}")
            if content_range is not None:
                if content_length < size_present:
                    raise exceptions.CorruptedPartsDir(f"content_length = {content_length:,d} < size_present {size_present:,d}")
        content_ranges = request.get_content_ranges(content_length, Options.part_size, present_content_ranges)
        logger.info(f"downloading  {utils.format_file_size(content_length-size_present)}")
        logger.debug(f"<JOB> (jobs={len(content_ranges)} jobs {content_ranges})")

        [
            _
            for _ in Options.exec.map(
                lambda x: request.download(
                    djob.url,
                    djob.headers,
                    djob.fo.parts_dir / x[1].as_filename,
                    rlogger.get_adapter(logger, f"job {x[0]+1}/{len(content_ranges)}"),
                    x[1],
                    content_length,
                    progress_formatter=request.ProgressFormatter(content_length - size_present),
                    preload_content=False,
                ),
                enumerate(content_ranges),
            )
        ]
        utils.join_parts(djob.fo.get_completed_filename(), djob.fo.parts_dir)
        logger.ok(f"Download done {djob.fo.get_completed_filename()} size {utils.format_file_size(djob.fo.get_completed_filename().stat().st_size)}")
        utils.rm_part_dir(djob.fo.parts_dir, Options.keep_parts)


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
                log(
                    f"media_number mismatch {current_media_number}. endfile could be corrupted. defaulting to most minimum {current_media_number:=min(current_media_number)}"
                )
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
                    yield f"{f}@{current_task}", current_media_number if current_media_number > current_task else FbIg.INFINITY, log, None, os.path.join(
                        data["output_dir"], f, f"{current_task}"
                    ), data["jobs"][f]["media_url"].replace(FbIg.MEDIA_NUMBER_STR, f"{current_task}"), data["headers"], [0, 0, lambda _, __: ""], True

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


from concurrent.futures import Future
from . import exceptions
from urllib3 import HTTPHeaderDict
from . import utils
from . import request


class Djob:
    def __init__(self, url: str, headers: dict, others: dict):
        self.url = url
        self.headers = HTTPHeaderDict(headers)
        self.others = others
        self.fo = FileObject(url)


class Downloader:
    def __init__(self):
        self.logger = rlogger.get_adapter(Options.logger, threading.current_thread().name)

    def parse_data(self, data: dict) -> dict:
        try:
            isinstance(data["url"], str)
        except Exception as e:
            raise exceptions.HelpExit("expects key 'url' in json containing a str")
        headers = data.get("headers", {})
        if "headers" in data:
            data.pop("headers")
        if not isinstance(headers, dict):
            raise exceptions.HelpExit("expects optional key 'header' in json containing a dict")
        return Djob(data.pop("url"), headers, data)

    def download(self, data: dict):
        djob = self.parse_data(data)
        t = data.get("type", "raw")
        f = Raw.download if t == "raw" else HLS.download if t == "segments" else FbIg.download if type == "fb/ig" else None
        if not f:
            raise exceptions.HelpExit(f"malformed type: unknown 'type':{t}")
        with (djob.fo.cwd / "info").open("w") as wrt:
            wrt.write(json.dumps(data))
        self.logger.debug(f"cwd {djob.fo.cwd}")
        f(djob)


class FileObject:
    """
    aliases for file paths for a download
    """

    def __init__(self, url: str):
        self.cwd = Options.parts_dir / self.hash_url(url)
        self.parts_dir = self.cwd / "parts"
        self.mime_type: str = ""
        self.initialize_dirs()

    @property
    def complete_download(self) -> Path:
        return Path(Options.complete_dir / (self.cwd.parts[-1] + self.mime_type))

    def initialize_dirs(self) -> None:
        if not self.parts_dir.exists():
            self.parts_dir.mkdir(parents=True, exist_ok=True)
            Options.logger.info(f"created dir {str(self.parts_dir)!r}")

    def check_completed_download(self, mime_type: Optional[str] = None, logger: logging.Logger = None) -> None:
        if (f := self.get_completed_filename(mime_type)).exists():
            if logger:
                logger.ok(f"already downloaded {str(f)} {utils.format_file_size(f.stat().st_size)}")
            return True
        return False

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


class Svdwr:
    """
    for sync reading and  sync to the writing to the terminal
    MainThread is probably running this instance
    """

    def __init__(self, downloader: Downloader):
        self.__downloader__ = downloader
        self.__lock__ = rlogger._log_lock
        self.__future_message__ = None
        self.logger = Options.logger
        self.logger.info("𝘚𝘝𝘋")
        if Options.clean:
            self.logger.critical(f"rmdir {Options.parts_dir} since --clean was set")
            utils.rm_part_dir(Options.parts_dir, not Options.clean)
        self.__write__()

    @property
    def __expects_message__(self) -> bool:
        return isinstance(self.__future_message__, Future) and not self.__future_message__.done()

    def __set_message__(self, message: str) -> None:
        if not self.__expects_message__:
            raise RuntimeError("No future expecting a message")
        self.__future_message__.set_result(message)

    @staticmethod
    def get_input() -> str:
        i = input("> ")
        if len(i) > 4096 - 1 - 1:
            raise exceptions.HelpExit("text too long. some terminal will cut the text. Save text in a file  'f' and use `svd -i f` instead")
        return i

    def __write__(self) -> None:
        while 1:
            try:
                i = Options.get_input_from_file() or Svdwr.get_input()
                if self.__expects_message__:
                    self.__set_message__(i)
                    continue
                if i == "." or i == "q":
                    break
                if i == "":
                    print(f"use q or . to exit, 'svd -h' for help or  paste from clipboard")
                    continue
                try:
                    i = json.loads(i)
                    if not isinstance(i, dict):
                        raise json.JSONDecodeError(f"expected  a dict got {type(i)}")
                    self.__downloader__.download(i)
                except json.JSONDecodeError as e:
                    self.logger.error(f"cannot parse input json; {repr(e)}")
            except exceptions.HelpExit as e:
                self.logger.critical(e.msg)
                sys.exit(1)
            except exceptions.CannotContinue as e:
                self.logger.error(e.msg)
                sys.exit(1)
            except Exception as e:
                if self.logger.level == logging.DEBUG:
                    raise
                self.logger.error(e)
                sys.exit(1)
            except KeyboardInterrupt:
                return

    def read(self) -> str:
        if self.__expects_message__:
            self.__future_message__.result()
            return self.read()
        else:
            self.__future_message__ = Future()
            with self.__lock__:
                return self.__future_message__.result()


Options = get_options()
from . import rlogger

if __name__ == "__main__":
    Svdwr(Downloader())
