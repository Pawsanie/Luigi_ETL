from os import walk, path
from datetime import date, datetime

from pandas import DataFrame  # Do not delete!

"""
Tests are run while the luigi pipeline is running and crashes if there are errors,
displaying information about them to the terminal.
"""


def error_warp(funk):
    """
    Error decorator:
    """
    print('====================== error: ======================\n\n')
    funk()
    print('\n\n====================================================')
    exit(1)


def test_external_task_path(arg) -> int:
    """
    Test passed in the path parameter:
    Checks for the presence of ./YYYY/MM/DD/
    in the root directory passed in the argument.
    """
    def incorrect_paths_in_dir_error():
        print('Root path <' + arg + '>:' +
              "\nHave no data dirs in format '%Y/%m/%d'." +
              '\nExample ./YYYY/MM/DD/.')
    is_it_date_in_path = []
    for dirs, folder, files in walk(arg):
        search_for_an_interesting_path: str = dirs.replace(arg, '')
        is_it_date_in_path.append(search_for_an_interesting_path)
    ok_count = 0
    for element in is_it_date_in_path:
        if '/' in element:
            check: list[str] = element.split('/')
            if len(check) >= 4:
                is_it_date = f"{check[-3]}/{check[-2]}/{check[-1]}"
                try:
                    datetime.strptime(is_it_date, '%Y/%m/%d').date()
                    ok_count += 1
                except ValueError:
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
        print('The path passed as a parameter to Luigi <' + arg + '> does not exist.')

    def path_symbol_error():
        print("The root path argument passed to Luigi must not have a trailing '/'." +
              '\nThe task itself will add the desired symbol in the process.' +
              '\nThe current value of the path argument: <' + arg + '>.')

    def path_incorrect_format_error():
        print("Nothing was passed in the parameter for the Luigi task's root directory." +
              "\nOr it's not a string.")
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
        print("Nothing was passed in the time mask parameter for Luigi's TransformTask path." +
              "\nOr it does not contain a date in the format '%Y/%m/%d'.")
    if type(arg) is not date:
        error_warp(mask_error)
    try:
        check = arg.strftime('%Y/%m/%d')
        datetime.strptime(check, '%Y/%m/%d').date()
    except ValueError:
        error_warp(mask_error)


def test_file_mask_arguments(arg):
    """
    Checks that the file masks in the pipeline arguments are strings that match the required formats.
    """
    def is_not_str_error():
        print('The argument received in <' + arg + '> is not a string.' +
              '\nOr an empty argument is passed.')

    def type_of_arg_is_not_correct():
        print('The passed <' + arg + '> argument is not of type:' +
              "\n'.json'"
              "\n'.csv'"
              "\n'.parquet'"
              "\nOr is not a file extension, in the format '.type'.")
    if type(arg) is str and len(arg) != 0:
        if arg != '.json' and arg != '.csv' and arg != '.parquet':
            error_warp(type_of_arg_is_not_correct)
    else:
        error_warp(is_not_str_error)


def test_output_df(arg):
    """
    Checks that the pandas DF being written to is not empty.
    """
    def df_is_empty():
        print('An empty pandas DF was sent to the entry.')
    if len(arg.index) == 0:
        error_warp(df_is_empty)


def test_output_file_exist(arg):
    """
    Checks if a file with the result of Luigi task has been created.
    """
    def file_exist_error():
        print('The result file was not created. Path:\n' + arg)
    try:
        path.exists(arg)
    except OSError:
        error_warp(file_exist_error)
