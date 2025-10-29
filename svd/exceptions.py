import urllib3


class HelpExit(Exception):
    def __init__(self, msg: str):
        self.msg = msg


class CannotContinue(Exception):
    def __init__(self, msg: str):
        self.msg = msg


class StatusCodeException(Exception):
    def __init__(self, r: urllib3.BaseHTTPResponse):
        self.r = r

    def __repr__(self) -> str:
        return self.__str__()

    def __str__(self) -> str:
        return f"status code {self.r.status} headers {self.r.headers} url {self.r.url} data {self.r.data}"


class CorruptedPartsDir(Exception):
    pass


class DownloadError(Exception):
    pass
