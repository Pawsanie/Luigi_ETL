import logging
from traceback import format_exc
# from imp import reload
# reload(logging)
"""
Logging configuration.
"""


def logging_config(*, log_path: str, log_level: int):
    """
    Get logging configuration.
    As result set logging rules.
    --------------
    log_path - path to logging file.
    log_level:
    CRITICAL - 50
    ERROR - 40
    WARNING - 30
    INFO - 20
    DEBUG - 10
    NOTSET - 0
    """
    logging.basicConfig(
        filename=log_path,
        encoding='utf-8',  # Not mistake: parameter added in python 3.9...
        level=log_level,
        format='%(asctime)s - %(levelname)s:\n%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S %p',
        # force=True
    )


def text_for_logging(*, log_text: str, log_error: Exception) -> str:
    """
    Wrapper for log text.
    :param log_text: Arbitrary text for logging.
    :type log_text: str
    :param log_error: Custom or standard Exception object..
    :type log_error: Exception
    """
    result_text: str = \
        f"{'=' * 30}\n"\
        f"Raise: {repr(log_error)}"\
        f"\n{log_text}"\
        f"\n{'-' * 30}"\
        f"\n{format_exc()}"\
        f"\n{'=' * 30}\n\n"
    return result_text
