import logging
import sys
from typing import Optional

print_ = sys.stderr.write


class Rlogger:
    """
    simple logger
    -------------
    Sends every log to `stderr`. simple.

    args
    ----
    name: Optional. Name of the logger, If None, then `'Rlogger'` is defaulted to.

    filename: Optional. filepath to save logs. If None, then logs are sent only to `stderr`.

    stderr: Optional. whether to send logs to stderr. If False then logs are written to `filename`.
            if `filename` and `stderr` is `False` an exception is raised to prevent silent and possibly
            unwanted behaviour.

    Usage
    -----
    once initialized, send logs by calling the instance

        e.g::
            >>> log=Rlogger()
            >>> log("info log")
            >>> log("error log",error=True)
            >>> log("critical log",critical=True)
            >>> log("this will fail",error=True,critical=True) #will fail
            >>> log("this will exit after printing message",exit_=True)
            >>> log("this log will be returned as a string unless exit_=True",emit=False)
    """

    class CustomStreamHandler(logging.StreamHandler):
        def __init__(self):
            super().__init__()
            self.emit_this = True
            self.last_non_emitted_message: str

        def emit(self, record: logging.LogRecord) -> None:
            if not self.emit_this:
                self.last_non_emitted_message = self.format(record)
                return
            super().emit(record)

    def __init__(self, name: str = None, filename: str = None, stderr=True, datefmt: str = "%Y:%m:%d-%H:%M:%S"):
        if not any((filename, stderr)):
            raise Exception("no need for logging")
        self._logger = logging.getLogger(name if name is not None else Rlogger.__name__)
        self._logger.setLevel(logging.INFO)
        c_format = logging.Formatter("[%(name)s %(asctime)s %(levelname)s] %(message)s", datefmt=datefmt)
        if filename:
            f_handler = ogging.FileHandler(filename)
            f_handler.setLevel(logging.INFO)
            f_handler.setFormatter(c_format)
            self._logger.addHandler(f_handler)
        if stderr:
            self._custom_stream_handler = s_handler = Rlogger.CustomStreamHandler()
            s_handler.setLevel(logging.INFO)
            s_handler.setFormatter(c_format)
            self._logger.addHandler(s_handler)

    def __call__(self, msg: tuple[str], ok: bool = False, error: bool = False, critical: bool = False, emit: bool = True, exit_: bool = False) -> Optional[str]:
        if error and critical:
            raise AttributeError("Cannot determine the level from multiple choices 'error' and 'critical'")
        if not emit and exit_:
            raise AttributeError("cannot emit and exit")
        log = self._logger.info
        self._custom_stream_handler.emit_this = emit
        color = ""
        if ok:
            color = "\033[92m"
        if error:
            log = self._logger.error
            color = "\033[91m"
        if critical:
            log = self._logger.critical
            color = "\033[95m"
        if emit:
            print_(color)
        log(msg)
        if emit:
            print_("\033[0m")
            sys.stderr.flush()
        if exit_:
            exit()
        if not emit:
            return f"{color}{self._custom_stream_handler.last_non_emitted_message}\033[0m"

    def __del__(self) -> None:
        for x in self._logger.handlers:
            x.close()
            self._logger.removeHandler(x)

    @staticmethod
    def do_nothing(*args, **kwargs) -> None:
        return

    @staticmethod
    def get_named_class_log(logger: "Rlogger", name: str):
        def log(msg: str, **kwargs) -> None:
            return logger(f"{name} {msg}", **kwargs)

        return log
