import argparse, logging, threading, sys
from pathlib import Path
from datetime import datetime
import shutil
from .rlogger import *
import sys
from typing import Optional
import textwrap
from concurrent.futures import ThreadPoolExecutor
import urllib3
import ssl 
from . import exceptions

from . import utils
_options_instance: Optional["_Options"] = None


class _Options:
    def __init__(
        self,
        workers,
        part_size,
        filename,
        chunk_read_size,
        complete_dir,
        parts_dir,
        verbose,
        ssl_on,
    ):        
        self.workers=workers
        self.part_size=part_size
        self.filename=filename
        self.chunk_read_size=chunk_read_size
        self.complete_dir =complete_dir.resolve()
        self.parts_dir = parts_dir.resolve()
        self.verbose=verbose
        self.ssl_on=ssl_on

        self.logger =get_logger("𝘚𝘝𝘋"+(' with-no-ssl' if not self.ssl_on else ''))
        if not ssl_on:
            ssl_context=ssl.create_default_context()            
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            self.http = urllib3.PoolManager(ssl_context=ssl_context)
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            self.logger.warning("ssl turned off.")
        else:
            self.http=urllib3.PoolManager(8)
        # try:
        #     self.http.request("GET",url = "https://self-signed.badssl.com/")
        # except urllib3.exceptions.MaxRetryError as e:
        #     if e.reason and  isinstance(e.reason,urllib3.exceptions.SSLError):
        #         self.logger.critical(f"ssl error {repr(e.reason)}. you may consider turning off ssl with --no-ssl flag")
            
        
        self.logger.setLevel(logging.DEBUG if verbose else logging.INFO)

        self.exec=ThreadPoolExecutor(self.workers)        
        self.dependencies = {
            "ffmpeg": "joining media parts",
        }
        

    def get_input_from_file(self)->Optional[str]:
        if self.filename:
            try:
                r=self.filename.open('r').read()
                self.filename=None
                return r
            except Exception as e:
                raise exceptions.SVDHelpExit(f"cannot read file  {str(self.filename)!r} supplied  through -r;  {repr(e)} ")
            
    @staticmethod
    def _get_bytes_from_str(s: str):
        x, y = s[:-1], s[-1]
        ss = "BKMGT"
        if y not in ss:
            raise ValueError(f"cannot parse unit of size {y}  {s!r}. valid units are {ss!r}")
        try:
            x = float(x) * (1024 ** ss.index(y))
            if x <= 0:
                raise ValueError(x)
            return int(x)
        except Exception as e:
            raise

    def init(self) -> None:
        self.initialize_dirs()
        self.print_options()
        self.check_dependecies()

    def initialize_dirs(self):
        for d in (self.parts_dir, self.complete_dir):
            if not d.exists():
                d.mkdir(parents=True, exist_ok=True)
                self.logger.info(f"created dir: {d}")

    def print_options(self):
        self.logger.debug(
            {
                "workers":str(self.workers),
                "part_size":str(utils.format_file_size(self.part_size)),
                "filename":str(self.filename),
                "chunk_read_size":str(utils.format_file_size(self.chunk_read_size)),
                "complete_dir":str(self.complete_dir),
                "parts_dir":str(self.parts_dir),
                "verbose":str(self.verbose),
                "ssl on":str(self.ssl_on),
                })

    def check_dependecies(self) -> None:
        for dependency in self.dependencies:
            path = shutil.which(dependency)
            if path:
                self.logger.debug(f"using {dependency!r} from {path}")
            else:
                raise FileNotFoundError(f"cannot locate dependency {dependency!r} for {self.dependencies[dependency]}")


def get_options() -> _Options:
    global _options_instance
    if _options_instance:
        return _options_instance
    parser = argparse.ArgumentParser(
        prog="svd",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent(
            """
                        simple video downloader(svd)
    svd is a simple downloader for common video urls passed as json.
    It can download:
                        [1] Raw videos e.g example.com/video.mp4
                        [2] Segmented videos (hls)
                        [3] Specific live videos (from facebook and Instagram)
    
    simple json should include url,headers & type e.g
        {
            "url":"example.com/video.mp4",
            "headers":
                        {"referer":"example.com"},
            "type":"raw"
        }
    
    [#] Since building this json could be tedious, a simple svd browser extension
        is provided here:  https://github.com/fskamau/svd-extension
    [#] Since some links contain text longer than 2048 bytes, normal clipboards will cut
            the json which will result to json.decoder.JSONDecodeError. save the text to
            a file and pass it to svd. e.g svd -r clip.txt
    [#] The program will continously read stdin for control signals.
        Passing / or c will read clipboard contents and treat them as json.
        passing . or q will quit immediately.

                        
                        """
        ),
    )
    parser.add_argument("-w", dest="workers", type=int, default=1, help="number of worker threads")
    parser.add_argument("-s", dest="part_size", default="1024T", help="size of 1 download part. e.g 1M, 512M, 2G. A download will be split into parts with @ part-size <= to this size.")
    parser.add_argument("-r", dest="filename", type=Path, default=None, help="file to read json to download")

    parser.add_argument("-c", dest="chunk_read_size", default="8K", help="chunk size to read from socket. bigger is better but incase of an error all unwritten data is lost ")
    parser.add_argument("-d", dest="complete_dir", type=Path, default= Path.home() / "Downloads", help="complete files directory")
    parser.add_argument("-p", dest="parts_dir", type=Path, default= Path.home() / ".svd", help="temporary parts directory")
    parser.add_argument("-v", dest="verbose", action="store_true", default=False, help="verbose")

    parser.add_argument("--no-ssl", default=True, action='store_false', help="turn off ssl. unless you know what you are doing, *This is completely dangerous*. It can be used to access content where some servers host files in storage buckets without ssl")

    args = parser.parse_args()
    _options_instance = _Options(
        workers=args.workers,
        part_size=_Options._get_bytes_from_str(args.part_size),
        filename=args.filename,
        chunk_read_size=_Options._get_bytes_from_str(args.chunk_read_size),
        complete_dir=args.complete_dir,
        parts_dir=args.parts_dir,
        verbose=args.verbose,
        ssl_on=args.no_ssl
    )
    _options_instance.init()
    return get_options()



if __name__ == "__main__":
    try:
        opts = get_options()
    except Exception as e:
        sys.exit(repr(e))
