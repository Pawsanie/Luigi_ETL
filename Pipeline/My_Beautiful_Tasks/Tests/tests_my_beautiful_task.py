from os import walk, path
from sys import platform
from datetime import date, datetime
import logging
from traceback import format_exc

from pandas import DataFrame  # Do not delete!
"""
Tests are run while the luigi pipeline is running and crashes if there are errors,
displaying information about them to the terminal.
"""


def error_warp(funk):
    """
    Error decorator:
    """
    print(f"{'='*22} error: {'='*22}\n\n'")
    funk()
    print(f"'\n\n'{'='*52}")
    exit(1)


def error_logger(error: Exception):
    """
    Logging error.

    :param error: Exception of test.
    :type error: Exception
    """
    logging.error(
        f"{'=' * 52}\n"
        f"Program launch raise: '{repr(error)}'"
        f"\n{'-' * 52}"
        f"\n{format_exc()}"
        f"\n{'=' * 52}\n\n"
    )


def error_message_logger(message):
    """
    Logging error message.

    :param message: Text for logging.
    :type message: str
    """
    print(message)
    logging.error(
        f"{'='*22} error: {'='*22}\n'"
        f"{str(message)}"
        f"\n{'=' * 52}\n\n"
    )


def path_separator() -> str:
    """
    Return path separator for OS.
    :return: '\' or '/'
    """
    if platform == "win32" or platform == "win64":
        return '\\'
    else:
        return '/'


def test_external_task_path(arg) -> int:
    """
    Test passed in the path parameter:
    Checks for the presence of ./YYYY/MM/DD/
    in the root directory passed in the argument.
    """
    def incorrect_paths_in_dir_error():
        message: str = f"Root path <{arg}'>:'" \
                       f"\nHave no data dirs in format '%Y/%m/%d'." \
                       f"\nExample ./YYYY/MM/DD/."
        error_message_logger(message)

    is_it_date_in_path: list = []
    for dirs, folder, files in walk(arg):
        search_for_an_interesting_path: str = dirs.replace(arg, '')
        is_it_date_in_path.append(search_for_an_interesting_path)
    ok_count: int = 0

    for element in is_it_date_in_path:
        if platform == "win32" or platform == "win64":
            check: list[str] = element.split(path_separator())
        else:
            check: list[str] = element.split(path_separator())
        while '' in check:
            check.remove('')

        if len(check) != 4 or len(check) < 4:
            continue
        is_it_date: str = path.join(*[check[-3], check[-2], check[-1]])
        try:
            datetime.strptime(is_it_date, f"%Y{path_separator()}%m{path_separator()}%d").date()
            ok_count += 1
        except ValueError as error:
            error_logger(error)
            continue
    if ok_count == 0:
        error_warp(incorrect_paths_in_dir_error)
    return ok_count


def test_path_mask_type_for_date(arg):
    """
    Test passed in the path parameter:
    1) Having a non-empty string in the argument.
    2) Checking if the path exists.
    3) Checking the last character in the path.
    """
    def path_das_not_exist_error():
        message: str = f"The path passed as a parameter to Luigi <{arg}> does not exist."
        error_message_logger(message)

    def path_symbol_error():
        message: str = f"The root path argument passed to Luigi must not have a trailing '/'."\
                       f"\nThe task itself will add the desired symbol in the process." \
                       f"\nThe current value of the path argument: <{arg}>."
        error_message_logger(message)

    def path_incorrect_format_error():
        message: str = f"Nothing was passed in the parameter for the Luigi task's root directory."\
                       f"\nOr it's not a string."
        error_message_logger(message)

    if len(arg) != 0 and type(arg) is str:
        if not path.exists(arg):
            error_warp(path_das_not_exist_error)
        if arg[-1] == '/':
            error_warp(path_symbol_error)
    else:
        error_warp(path_incorrect_format_error)


def test_transform_task_time_mask(arg):
    """
    Checks the date_path_part mask against a date in the format '%Y/%m/%d'
    """
    def mask_error():
        message: str = f"Nothing was passed in the time mask parameter for Luigi's TransformTask path."\
                       f"\nOr it does not contain a date in the format '%Y/%m/%d'."
        error_message_logger(message)

    if type(arg) is not date:
        error_warp(mask_error)
    try:
        check: str = arg.strftime(f"%Y{path_separator()}%m{path_separator()}%d")
        datetime.strptime(check, f"%Y{path_separator()}%m{path_separator()}%d").date()
    except ValueError as error:
        error_logger(error)
        error_warp(mask_error)


def test_file_mask_arguments(arg):
    """
    Checks that the file masks in the pipeline arguments are strings that match the required formats.
    """
    def is_not_str_error():
        message: str = f"The argument received in <{arg}> is not a string."\
                       f"\nOr an empty argument is passed."
        error_message_logger(message)

    def type_of_arg_is_not_correct():
        message: str = f"The passed <{arg}> argument is not of type:"\
              f"\n'.json'"\
              f"\n'.csv'"\
              f"\n'.parquet'"\
              f"\nOr is not a file extension, in the format '.type'."
        error_message_logger(message)

    if type(arg) is str and len(arg) != 0:
        if arg != 'json' and arg != 'csv' and arg != 'parquet':
            error_warp(type_of_arg_is_not_correct)
    else:
        error_warp(is_not_str_error)


def test_output_df(arg):
    """
    Checks that the pandas DF being written to is not empty.
    """
    def df_is_empty():
        message: str = f"An empty pandas DF was sent to the entry."
        error_message_logger(message)

    if len(arg.index) == 0:
        error_warp(df_is_empty)


def test_output_file_exist(arg):
    """
    Checks if a file with the result of Luigi task has been created.
    """
    def file_exist_error():
        message: str = f"The result file was not created. Path:\n{arg}"
        error_message_logger(message)

    try:
        path.exists(arg)
    except OSError as error:
        error_logger(error)
        error_warp(file_exist_error)
