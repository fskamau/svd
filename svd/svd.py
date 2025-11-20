import hashlib
import itertools
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import xml.etree.ElementTree as ET
from concurrent.futures import Future
from io import StringIO
from pathlib import Path
from typing import NamedTuple, Optional
import time
from urllib3 import HTTPHeaderDict
from urllib.parse import urljoin
from . import exceptions, request, utils
from .options import get_options

Options = get_options()
from . import rlogger


class Djob:
    def __init__(self, url: str, headers: dict, others: Optional[dict], logger: Optional[logging.Logger] = None):
        self.url = url
        self.headers = HTTPHeaderDict(headers)
        self.others = others
        self.fo = FileObject(url)
        self.logger = logger


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
        output_video = djob.fo.complete_download_filename
        logger.debug(f"<JOB> (jobs={seg_len}, url={stream_url}, segsfolder={djob.fo.parts_dir}, video={output_video})")

        if djob.fo.check_completed_download(logger=logger):
            return

        def get_segment_url(x):
            if (segx := segments[x]).startswith("http"):
                return segx
            if segx[0] == "/":
                return f"{domain}{segx}"
            return f"{stream_url}/{segx}"

        def format_progress(self, downloaded: Optional[int] = None):
            if downloaded is not None:
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
                    logger=rlogger.get_adapter(logger, f"job {x+1}/{seg_len} {utils.get_thread_name()}"),
                    progress_formatter=progress_formatter,
                    preload_content=True,
                ),
                range(seg_len),
            )
        ]
        HLS.concatenate_ts_files([djob.fo.parts_dir / str(x) for x in range(seg_len)], output_video, djob.fo.parts_dir)
        logger.ok(f"download done {str(output_video)} , size={utils.format_file_size(output_video.stat().st_size)} done.")
        utils.rm_part_dir(djob.fo.parts_dir, Options.no_keep)


class Raw:

    def download(djob: "Djob"):
        djob.headers["range"] = "bytes=0-"
        if hasattr(djob, "logger"):
            logger = djob.logger
        else:
            logger = rlogger.get_adapter(Options.logger, Raw.__name__)
        if djob.fo.check_completed_download(logger=logger):
            return
        content_range, content_length, r = request.make_request(
            "GET", djob.url, djob.headers, logger, allow_mime_text=False, preload_content=False
        )  # some servers will return 403 with 'HEAD' since they dont expect you to ...
        r.release_conn()
        logger.debug(f"from HEAD request: {request.summarize_headers(r.headers)}")
        if content_range.total != content_length:
            logger.debug(f"content_length {content_length} != content_range total {content_range}")
        size_present = 0
        pcrs = []
        pdir = str(djob.fo.parts_dir)

        if content_length is None:  # e.g tiktok lives
            if any(djob.fo.parts_dir.iterdir()):
                raise FileExistsError("content length is None but part files exists. cannot correcly identify missing data. remove it with --no-keep")
            logger.critical("no content range, downloading till server closes connection")

            def format_progress(x, y):
                if y is not None:
                    x.downloaded += y
                return f"saved {utils.format_file_size(x.downloaded)}"

            Options.exec.submit(
                lambda: request.download(
                    djob.url,
                    djob.headers,
                    djob.fo.complete_download_filename,
                    logger=rlogger.get_adapter(logger, f"no content length"),
                    progress_formatter=request.ProgressFormatter(None, format_progress),
                    preload_content=False,
                )
            ).result()
        else:
            if any(djob.fo.parts_dir.iterdir()):
                logger.debug("checking already donwloaded parts")
                if content_length is None:
                    logger.critical(
                        f"parts exists but content_length is None. cannot assume the server will return correct content ranges; remove {pdir} manually"
                    )
                for file in djob.fo.parts_dir.iterdir():
                    if v := utils.update_part_file(file, logger):
                        pcrs.append(v[0])
                        size_present += v[1]
                if content_length < size_present:
                    raise exceptions.CorruptedPartsDir(f"content_length = {content_length} < size_present {size_present}")
                pcrs = sorted(pcrs, key=lambda x: x[0])
                pcrs_ = []
                if len(pcrs) > 1:
                    for i, p in enumerate(pcrs):
                        if i > 0:
                            if p[0] <= pcrs[i - 1][0] or p[0] <= pcrs[i - 1][1]:
                                raise exceptions.CorruptedPartsDir(f"content range overlap  {pcrs[i-1]} / {p} ")
                        else:
                            pcrs_.append(p)
                            continue
                        if pcrs_[-1][-1] + 1 == p[0]:
                            pcrs_[-1][-1] = p[1]
                        else:
                            pcrs_.append(p)
                else:
                    pcrs_ = pcrs
                pcrs = pcrs_

            logger.info(f"total size of parts present : {utils.format_file_size(size_present)}")
            content_ranges = utils.get_missing_ranges(byte_end=content_length, part_size=Options.part_size, pcrs=pcrs)
            logger.info(f"downloading  {utils.format_file_size(content_length-size_present)}")
            logger.debug(f"<JOB> (jobs={len(content_ranges)} jobs {content_ranges})")
            [
                _
                for _ in Options.exec.map(
                    lambda x: request.download(
                        djob.url,
                        djob.headers,
                        djob.fo.parts_dir / x[1].as_filename,
                        rlogger.get_adapter(logger, f"job {x[0]+1}/{len(content_ranges)} {utils.get_thread_name()}"),
                        x[1],
                        content_length,
                        progress_formatter=request.ProgressFormatter(content_length - size_present),
                        preload_content=False,
                    ),
                    enumerate(content_ranges),
                )
            ]
            utils.join_parts(djob.fo.complete_download_filename, djob.fo.parts_dir)
        logger.ok(f"Download done {djob.fo.complete_download_filename} size {utils.format_file_size(djob.fo.complete_download_filename.stat().st_size)}")
        utils.rm_part_dir(djob.fo.parts_dir, Options.no_keep)


class MPD:
    """
    common funcs for Media Presentation Description
    """

    def get_namespaces(xml_str: str) -> dict:
        """Return all namespaces in an XML string as {prefix: uri}."""
        namespaces = {}
        for event, elem in ET.iterparse(StringIO(xml_str), events=("start-ns",)):
            prefix, uri = elem
            namespaces[prefix] = uri
        return namespaces

    def get_desired_format_choice(formats: list[str], logger: logging.Logger) -> dict:
        choices = []
        for index, f in enumerate(formats):
            c = f"{f['mimetype']}"
            if f.get("quality") is not None:
                c += f"@{f['quality']}"
            if f.get("bandwidth") is not None:
                c += f"@{utils.format_bandwidth(f['bandwidth'])}"
            choices.append((index, c))
        other = list(itertools.combinations(choices, 2))
        if len(other) > 0:
            for o in other:
                if len(set([formats[x[0]]["basename"] for x in o])) == 1:  # don't add same formats with diff qualities like video720p and video1080p
                    continue
                choices.append([*[x[0] for x in o], " + ".join([x[1] for x in o])])
        len_choices = len(choices)
        s = ""
        colors = itertools.cycle(["\033[1;37;40m", "\033[1;30;47m"])
        for index, c in enumerate(choices):
            s += f"{next(colors)}[{index:3d} => {c[-1]}]\033[0m\n"
        s += f"{'-'*50}\nThe following {len(choices)} formats exists. please choose 1"
        try:
            text = Wr.read(s)
            v = choices[abs(int(i := text))]
            logger.info(f"you choose '{v[-1]}'")
            return {formats[x].pop("basename"): formats[x] for x in v[:-1]}
        except (ValueError, IndexError) as e:
            raise exceptions.CannotContinue(f"cannot get your choice from '{i}'")

    def init_dirs(djob: "Djob", sub_dirs: list[Path], logger: logging.Logger):
        """
        intit dir in the following format
        [outdir/video|outdir/audio]
        """
        for p in djob.fo.parts_dir.iterdir():
            if p.name not in sub_dirs:
                raise FileExistsError(f"other dirs exists in {djob.fo.parts_dir} cannot continue. consider using --no-keep to clean the parts dir first")

        for x in sub_dirs:
            if (p := (djob.fo.parts_dir / x)).exists():
                logger.critical(f"dir {p} exists. format corruption if a different format has been chosen. Nothing will be removed")
            else:
                p.mkdir()


class VK:
    """
    download videos &| audio from VKvideo
    """

    def parse_xml(xml_str: str, logger: logging.Logger) -> list[str]:
        """
        extract useful info from xml mpd. strictly for VK
        """
        try:
            reps = []
            root = ET.fromstring(xml_str)
            namespaces = MPD.get_namespaces(xml_str)
            # logger.debug(ET.tostring(root).decode())
            for seg in root.findall(".//AdaptationSet", namespaces):
                mimeType = seg.get("mimeType")
                if (mimeType and "webvtt" in mimeType.lower()) or not mimeType:  # skip web video text tracks like subtitles and captions
                    continue
                for rep in seg.findall(".//Representation", namespaces):
                    rd = {"mimetype": mimeType}
                    try:
                        rd["basename"], rd["file_extension"] = re.search(FbIg.MIMETYPE_REGEX, mimeType).groups()
                    except AttributeError:
                        logger.error(f"cannot match mimeType {mimeType} to extract file extension")
                        raise NotImplementedError
                    if (a := rep.get("quality")) is not None:
                        rd["quality"] = a
                    if "audio" in rd["basename"].lower() and (a := rep.get("bandwidth")) is not None:
                        rd["bandwidth"] = int(a)
                    base_url = rep.find("./BaseURL", namespaces)
                    if base_url is None:
                        logger.error(msg := f"missing a base url for {rd}")
                        raise NotImplementedError
                    rd["url"] = base_url.text
                    reps.append(rd)
            return reps
        except AttributeError as e:
            raise exceptions.CannotContinue(f"cannot parse xml {repr(e)}")
        except Exception as e:
            raise exceptions.CannotContinue(f"{repr(e)}")

    def download(djob: "Djob"):
        logger = rlogger.get_adapter(Options.logger, "VK")
        logger.debug("VKvideo")
        formats = VK.parse_xml(djob.others["xmlData"], logger)
        choice = MPD.get_desired_format_choice(formats, logger)
        djob.jlen = len(choice)
        # set final file extension based on choice
        djob.fo.set_mime_type(f".{(choice['audio'] if  djob.jlen == 1 and list(choice)[0] == 'audio' else choice['video']  )['file_extension']}")
        if djob.fo.check_completed_download(logger):
            return
        logger.info(f"downloading {'video + audio' if len(choice)>1 else list(choice)[0] +' only'}")
        djob.others = choice
        MPD.init_dirs(djob, choice.keys(), logger)
        VK._download(djob, logger)
        logger.ok(f"downloaded {djob.fo.complete_download_filename} size {utils.format_file_size(djob.fo.complete_download_filename.stat().st_size)}")

    def _download(djob: Djob, logger: logging.Logger) -> None:
        # we take item from each job
        jobs = []
        for x in djob.others:
            j = djob.others[x]
            fo = FileObject("")
            fo.set_cwd(djob.fo.parts_dir / x)
            fo.initialize_dirs(logger)
            mt = j.get("file_extension")
            mt = "" if not mt else f".{mt}"
            fo.set_completed_filepath(fo.cwd / f"{x}{mt}")
            d = Djob(urljoin(utils.get_base_url(djob.url), j["url"]), djob.headers, None)
            d.fo = fo
            d.logger = rlogger.get_adapter(logger, x)
            jobs.append(d)
            Raw.download(d)  # we could submit both (audio&video) but if user has <=2 workers, deadlock will occur
        VK.join(djob, jobs, logger)

    def join(main: Djob, djobs: list[Djob], logger: logging.Logger):
        complete = main.fo.complete_download_filename
        if main.fo.check_completed_download(logger):
            raise FileExistsError(f"file {str(complete)} exists")
        cmd = [
            "ffmpeg",
        ]
        for d in djobs:
            cmd.append("-i")
            cmd.append(d.fo.complete_download_filename)
        [cmd.append(x) for x in ["-c", "copy", complete]]
        subprocess.check_output(cmd)
        logger.info(f"wrote {complete} {utils.format_file_size(complete.stat().st_size)}")


class MaybeMetaAlteredCodeException(NotImplementedError):
    pass


class FbIg:
    """
    download <facebook & instagram> livestreams
    """

    MIMETYPE_REGEX = re.compile(r"(audio|video)/(\w+)")
    DEFAULT_PREDICTED_MEDIA_START = 100
    MEDIA_NUMBER_STR = "$Number$"
    INIT_FILENAME = "0"  # for easy sorting cat $(ls -v) > out.mp4
    INFINITY = "∞"
    STATUS_CODE_ERROR_COUNT = 3
    STATUS_CODE_ERROR_SLEEP = 5
    assert STATUS_CODE_ERROR_COUNT > 0

    def join(djob: "Djob", logger: logging.Logger):
        paths = []
        complete = djob.fo.complete_download_filename
        for f in djob.others:
            p = djob.fo.parts_dir / f
            try:
                files = sorted(p.iterdir(), key=lambda path: int(path.name))  # will raise ValueError if not numeric
                part_file = p / f
                logger.ok(part_file)
                with part_file.open("wb") as wrt:
                    for part in files:
                        shutil.copyfileobj((p / part).open("rb"), wrt)
                if len(djob.others) == 1:
                    out_file = complete
                else:
                    paths.append(out_file := (p / f"{complete.name}"))
                try:
                    subprocess.check_output(["ffmpeg", "-i", part_file, "-c", "copy", out_file])
                    part_file.unlink()
                except subprocess.CalledProcessError:
                    logger.critical(f"cannot copy {part_file}, encoding instead (might take a long time)")
                    try:
                        subprocess.check_output(["ffmpeg", "-y", "-i", part_file, out_file])
                    except Exception as e:
                        raise RuntimeError(f"cannot continue; ffmpeg encode has failed {repr(e)}")
                logger.debug(f"joined parts to {out_file}")
            except ValueError:
                raise RuntimeError(f"dir {complete} is corrupted as it contains other files, try cleaning parts dir with --no-keep ")
        if len(djob.others) == 2:
            logger.debug("joining video and audio")
            if complete.exists():
                raise RuntimeError(f"{complete} has existed while process was running; did you create it?")
            subprocess.check_output(["ffmpeg", "-i", paths[0], "-i", paths[1], "-c", "copy", complete])
        [x.unlink() for x in paths]
        logger.info(f"wrote {complete} {utils.format_file_size(complete.stat().st_size)}")

    def parse_xml(xml_str: str, logger: logging.Logger) -> list[str]:
        """
        extract useful info from xml mpd. strictly for facebook and instagram
        """
        try:
            reps = []
            root = ET.fromstring(xml_str)
            namespaces = MPD.get_namespaces(xml_str)
            # logger.debug(ET.tostring(root).decode())
            for seg in root.findall(".//AdaptationSet", namespaces):
                if (s := seg.get("segmentAlignment")) is not None:
                    if s != "true":
                        raise MaybeMetaAlteredCodeException(f"segment alignment is not 'true' {seg.attrib}. not implemented")
            for rep in root.findall(".//Representation", namespaces):
                if "wvtt" in rep.get("codecs") or []:  # skip web video text tracks like subtitles and captions
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
                (st,) = rep.findall(".//SegmentTemplate", namespaces)
                rd["init"] = st.attrib["initialization"]
                if not rd["init"].startswith("."):
                    raise MaybeMetaAlteredCodeException(f"init url does not start with . {rd['init']}")
                (st,) = rep.findall(".//SegmentTimeline[@FBPredictedMedia][@FBPredictedMediaEndNumber]", namespaces)
                rd["media_number"] = int(st.attrib["FBPredictedMediaEndNumber"])
                rd["media"] = st.attrib["FBPredictedMedia"]
                if rd["media"].count(FbIg.MEDIA_NUMBER_STR) != 1:
                    raise MaybeMetaAlteredCodeException(f"MEDIA_NUMBER_STR mismatch in {rd['media']}")
                st = rep.findall(".//SegmentTimeline[@FBPredictedMediaStartNumber]", namespaces)
                if len(st) == 1:
                    if int(st[0].attrib["FBPredictedMediaStartNumber"]) != FbIg.DEFAULT_PREDICTED_MEDIA_START:
                        raise MaybeMetaAlteredCodeException(f"different DEFAULT_PREDICTED_MEDIA_START. dump {rd} {ET.tostring(rep).decode()}")
                else:
                    logger.critical(
                        f"attribute FBPredictedMediaStartNumber does not exist in mime {rd['mimetype']}. "
                        f"defaulting to '{FbIg.DEFAULT_PREDICTED_MEDIA_START}'"
                    )
                reps.append(rd)
            return reps
        except AttributeError as e:
            raise exceptions.CannotContinue(f"cannot parse xml {repr(e)}")
        except Exception as e:
            raise exceptions.CannotContinue(f"{repr(e)}")

    def download(djob: "Djob"):
        logger = rlogger.get_adapter(Options.logger, "LIVE")
        logger.debug("live facebook/instagram")
        formats = FbIg.parse_xml(djob.others["xmlData"], logger)
        choice = MPD.get_desired_format_choice(formats, logger)

        djob.jlen = len(choice)
        # set final file extension based on choice
        djob.fo.set_mime_type(f".{(choice['audio'] if  djob.jlen == 1 and list(choice)[0] == 'audio' else choice['video']  )['file_extension']}")
        if djob.fo.check_completed_download(logger):
            return
        logger.info(f"downloading {'video + audio' if len(choice)>1 else list(choice)[0] +' only'}")
        for c in choice:
            for k in ["init", "media"]:
                ipu = len(re.search(r"(^\.+)", choice[c][k]).group(1))
                choice[c][k + "_url"] = "/".join(djob.url.split("/")[:-ipu]) + choice[c][k][ipu:]
                choice[c].pop(k)
        MPD.init_dirs(djob, choice.keys(), logger)
        FbIg._download(djob, logger)
        logger.ok(f"downloaded {djob.fo.complete_download_filename} size {utils.format_file_size(djob.fo.complete_download_filename.stat().st_size)}")

    def _download(djob: "Djob", choice: dict[str, dict], logger: logging.Logger) -> None:
        # download  init files first
        for x in djob.jobs:
            request.download(
                djob.jobs[x]["init_url"],
                djob.headers,
                djob.fo.parts_dir / x / FbIg.INIT_FILENAME,
                rlogger.get_adapter(logger, f"init-header for {x}"),
                progress_formatter=request.ProgressFormatter.default(),
                preload_content=True,
            )
        # we take item from each job
        current_media_number = [djob.jobs[k]["media_number"] for k in list(djob.jobs)]
        if len(current_media_number) == 2:
            if current_media_number[0] != current_media_number[1]:
                logger.critical(
                    f"media_number mismatch {current_media_number}. endfile could be corrupted. defaulting to most minimum {current_media_number:=min(current_media_number)}"
                )
            current_media_number = min(current_media_number)
        else:
            current_media_number = current_media_number[0]

        logger.debug(f"media_number starting at {FbIg.DEFAULT_PREDICTED_MEDIA_START} current_media_number {current_media_number}")
        for x in djob.jobs:
            djob.jobs[x]["current_task"] = FbIg.DEFAULT_PREDICTED_MEDIA_START

        def generate_urls():
            while 1:
                for f in djob.jobs:
                    current_task = djob.jobs[f]["current_task"]
                    djob.jobs[f]["current_task"] += 1
                    yield {"url": djob.jobs[f]["media_url"].replace(FbIg.MEDIA_NUMBER_STR, f"{current_task}"), "f": f, "current_task": current_task}

        def download_with_thread_local_vars(t):
            end = current_media_number if t["current_task"] <= current_media_number else FbIg.INFINITY
            request.download(
                t["url"],
                djob.headers,
                djob.fo.parts_dir / t["f"] / str(t["current_task"]),
                rlogger.get_adapter(logger, f"{t['f']}@{t['current_task']}/{end} {utils.get_thread_name()}"),
                progress_formatter=request.ProgressFormatter.default(),
                preload_content=True,
            )

        genv = generate_urls()
        # we use threads to download whole stream till last media. then change to only 1 thread to prevent 404
        logger.info("downloading from livestream start to now")
        if current_media_number < FbIg.DEFAULT_PREDICTED_MEDIA_START:
            raise RuntimeError("current_media_number < FbIg.DEFAULT_PREDICTED_MEDIA_START")
        tasks = [next(genv) for _ in range((current_media_number - FbIg.DEFAULT_PREDICTED_MEDIA_START))]
        [_ for _ in Options.exec.map(download_with_thread_local_vars, tasks)]
        logger.info("downloading from now till the stream ends.")

        # we now use the mainthread infinetly. receiveing error status code might be the end of stream
        def one_thread():
            while 1:
                try:
                    dl = next(genv)
                    for x in range(FbIg.STATUS_CODE_ERROR_COUNT - 1, -1, -1):
                        try:
                            download_with_thread_local_vars(dl)
                            break
                        except exceptions.StatusCodeException as e:
                            if x == 0:
                                raise
                            logger.critical(f"status {e.r.status}; sleeping for {FbIg.STATUS_CODE_ERROR_SLEEP}sec. remaining retries {x} ")
                            time.sleep(FbIg.STATUS_CODE_ERROR_SLEEP)
                except exceptions.StatusCodeException as e:
                    logger.info(f"ecountered StatusCodeException with status code {e.r.status}; saving file now")
                    FbIg.join(djob, logger)
                    break

        Options.exec.submit(one_thread).result()


class Downloader:
    def __init__(self):
        self.logger = rlogger.get_adapter(Options.logger, threading.current_thread().name)

    def parse_data(self, data: dict) -> Djob:
        try:
            isinstance(data["url"], str)
        except Exception as e:
            raise exceptions.HelpExit("expects key 'url' in json containing a str")
        headers = data.get("headers", {})
        if "headers" in data:
            data.pop("headers")
        if not isinstance(headers, dict):
            raise exceptions.HelpExit("expects optional key 'header' in json containing a dict")
        djob = Djob(data.pop("url"), headers, data)
        djob.fo.set_mime_type(utils.get_extension_from_headers(djob.headers))
        return djob

    def download(self, data: dict):
        djob = self.parse_data(data)
        t = data.get("type", "raw")
        f = None
        if t == "raw":
            f = Raw.download
        elif t == "segments":
            f = HLS.download
        elif t == "xml":
            base_url = utils.get_base_url(djob.url)
            if ".okcdn." in base_url:
                f = VK.download
            else:
                f = FbIg.download
        else:
            raise exceptions.CannotContinue(f"malformed type: unknown 'type':{t}")
        self.logger.debug(f"cwd {djob.fo.cwd}")
        djob.fo.initialize_dirs(self.logger)
        if o := Options.get_output_filepath():
            djob.fo.set_completed_filepath(o)
        with (djob.fo.cwd / "info").open("w") as wrt:
            wrt.write(json.dumps(data))
        t = time.perf_counter()
        f(djob)
        self.logger.ok(f"took {utils.format_time(time.perf_counter()-t)}")


class FileObject:
    """
    aliases for file paths for a download
    """

    def __init__(self, url: str):
        self.cwd = Options.parts_dir / self.hash_url(url)
        self.parts_dir = self.cwd / "parts"
        self.mime_type: str = ""
        self._cfilename = None

    @property
    def complete_download_filename(self) -> Path:
        return self._cfilename or Path(Options.complete_dir / (self.cwd.parts[-1] + self.mime_type))

    def initialize_dirs(self, logger: logging.Logger) -> None:
        if Options.no_keep and self.cwd.exists():
            logger.critical(f"clearing {self.cwd} since --no-keep was passed")
            utils.rm_part_dir(self.cwd, Options.no_keep)
        if not self.parts_dir.exists():
            self.parts_dir.mkdir(parents=True, exist_ok=True)
            Options.logger.info(f"created dir {str(self.parts_dir)!r}")

    def check_completed_download(self, logger: logging.Logger = None) -> None:
        if (f := self.complete_download_filename).exists():
            if logger:
                logger.ok(f"already downloaded {str(f)} {utils.format_file_size(f.stat().st_size)}")
            return True
        return False

    def set_mime_type(self, mime_type: str):
        if mime_type and mime_type[0] != ".":
            raise ValueError(f"expects a . in mime_type {mime_type!r}")
        self.mime_type = mime_type

    @staticmethod
    def hash_url(url: str) -> str:
        return hashlib.md5(url.encode("utf-8")).hexdigest()

    def set_cwd(self, cwd: Path) -> None:
        self.cwd = cwd
        self.parts_dir = self.cwd / "parts"

    def set_completed_filepath(self, file_path: str) -> None:
        p = Path(file_path)
        if not p.parent.exists():
            raise FileNotFoundError(f"Parent directory does not exist: {p.parent}")
        self._cfilename = file_path


class _Wr:
    """
    synced write && read
    for sync reading and  sync to the writing to the terminal
    MainThread is probably running this instance
    """

    def __init__(self, downloader: Downloader):
        self.__downloader__ = downloader
        self.__lock__ = rlogger._log_lock
        self.__future_message__ = None
        self.logger = Options.logger
        self.__check_dirs__()

    def __check_dirs__(self) -> None:
        if Options.clean:
            self.logger.critical(f"rmdir {Options.parts_dir} since --clean was set")
            utils.rm_part_dir(Options.parts_dir, Options.clean)
        if (s := utils.get_folder_size(Options.parts_dir)) > 1024 * 1024 * 1024:
            self.logger.critical(
                f"parts folder {Options.parts_dir} > 1GB consider removing to save space  with `svd --clean`, currently  {utils.format_file_size(s)}"
            )

    @property
    def __expects_message__(self) -> bool:
        return isinstance(self.__future_message__, Future) and not self.__future_message__.done()

    def __set_message__(self, message: str) -> None:
        if not self.__expects_message__:
            raise RuntimeError("No future expecting a message")
        self.__future_message__.set_result(message)

    @staticmethod
    def get_input() -> str:
        i = input(">>> ")
        if len(i) > 4096 - 1 - 1:
            raise exceptions.HelpExit(f"text too long {len(i)}. some terminal will cut the text. Save text in a file  'f' and use `svd -i f` instead")
        return i

    def repl(self) -> None:
        while 1:
            try:
                i = Options.get_input_from_file() or _Wr.get_input()
                if self.__expects_message__:
                    self.__set_message__(i)
                    continue
                if i == "." or i == "q":
                    break
                if i == "":
                    print(f"use q or . to exit, 'svd -h' for help or  paste from clipboard")
                    continue
                try:
                    inp, i = i, json.loads(i)
                    if not isinstance(i, dict):
                        raise json.JSONDecodeError(f"expected  a dict got {type(i)}")
                    if self.logger.level == logging.DEBUG:
                        print(f"\n{inp}\ninput len: {len(inp)}")
                    self.__downloader__.download(i)
                    break
                except json.JSONDecodeError as e:
                    self.logger.error(f"cannot parse input json; {repr(e)}")
            except (KeyboardInterrupt, EOFError):
                return
            except exceptions.HelpExit as e:
                self.logger.critical(e.msg)
                sys.exit(1)
            except Exception as e:
                if self.logger.level == logging.DEBUG:
                    raise
                self.logger.error(e)
                sys.exit(1)

    def read(self, text: Optional[str] = None) -> str:
        if threading.current_thread() is threading.main_thread():
            with self.__lock__:
                if text:
                    print(text)
                return Wr.get_input()
        if self.__expects_message__:
            self.__future_message__.result()
            return self.read(text)
        else:
            self.__future_message__ = Future()
            with self.__lock__:
                if text:
                    print(text)
                return self.__future_message__.result()


def main():
    global Wr
    Wr = _Wr(Downloader())
    Wr.repl()


if __name__ == "__main__":
    main()
