
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
