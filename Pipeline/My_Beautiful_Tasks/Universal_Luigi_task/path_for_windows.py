from os import path, sep
from sys import platform
"""
This script file is filled with a piece of my "love" for Windows...
Generates paths for Windows OS.
"""


def get_cross_os_path(path_list: list[str]) -> str:
    """
    Returns paths not truncated after the Windows OS disc name...
    """
    if platform != "win32" and platform != "win64":
        return path.join(*path_list)

    # Processing windows disc path part:
    root_path: list[str] = path_list[0].split('\\')
    for element in root_path:
        while '\\' in element:
            element.replace('\\', '')
    windows_disc: str = root_path[0]
    root_path.remove(root_path[0])
    path_list.remove(path_list[0])

    # Processing another path parts:
    tail_path: list = []
    for element in path_list:
        try:
            folder_list: list[str] = element.split('\\')
            for folder in folder_list:
                tail_path.append(fr"{folder}")
        except Exception:
            tail_path.append(fr"{element}")

    windows_path: str = path.join(
        f"{windows_disc}{sep}", *root_path, *tail_path
    )
    return windows_path


def get_root_path(*, path_dir: str, tail: int = 4) -> str:
    """
    Return root path for partition.
    """
    def split_path(separator: str) -> list[str]:
        """
        Split directory path to folders.
        """
        path_split_list: list[str] = path_dir.split(separator)
        counter: int = 0
        while '' in path_split_list:
            path_split_list.remove('')
        while counter != tail:
            path_split_list.remove(path_split_list[-1])
            counter += 1
        return path_split_list

    if platform != "win32" and platform != "win64":
        path_list: list[str] = split_path('/')
        return path.join(*path_list)

    path_list: list[str] = split_path('\\')
    return get_cross_os_path(path_list)


def parsing_date_part_path(path_dir: str) -> str:
    """
    Return date part path.
    """
    if platform != "win32" and platform != "win64":
        separator: str = '/'
    else:
        separator: str = '\\'
    path_list: list[str] = path_dir.split(separator)
    while '' in path_list:
        path_list.remove('')
    return get_cross_os_path([path_list[-3], path_list[-2], path_list[-1]])
