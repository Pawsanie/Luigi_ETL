from os import path, walk
from datetime import date

from luigi import LocalTarget

from .path_for_windows import get_cross_os_path, parsing_date_part_path, get_root_path
"""
Contents code for task paths parsing.
"""


class PathsParser:
    """
    Parsing paths for tasks.
    """
    # File format:
    file_mask: str = ""
    # Root path for output processing:
    partition_path: str = ""
    # Date path part:
    date_parameter: date or None = None

    # Luigi local target file:
    success_flag: str = "_Validate_Success"
    # Local targets from past Luigi Task:
    result_successor: list[LocalTarget] | tuple[LocalTarget] | LocalTarget = ""
    # List with paths for processing:
    input_path_list: list = []

    # Dictionary with paths from which to read partitions:
    interested_partition: dict = {}

    # List with paths for data landing:
    output_paths_list: list = []

    task_namespace: str = ""
    file_name: str = ""

    def result_successor_path_generator(self):
        """
        Inherits paths from the previous task.
        Generate 'path_list'.
        """
        if type(self.result_successor) is list \
                or type(self.result_successor) is tuple \
                or type(self.result_successor) is set:
            for flag in self.result_successor:
                path_to_table: str = str.replace(flag.path, self.success_flag, '')
                self.input_path_list.append(path_to_table)
        else:
            path_to_table: str = str.replace(self.result_successor.path, self.success_flag, '')
            self.input_path_list.append(path_to_table)

    def task_input_path_parser(self, tail: int = 4):
        """
        Path inheritance from result_successor.
        :param tail: Number of folders + file to discard to get root partition folder.
                     4 as default: '/YYYY/MM/DD/file_name.format'.
        :type tail: int
        """
        self.result_successor_path_generator()
        for parsing_dir in self.input_path_list:
            for dirs, folders, files in walk(parsing_dir):
                for file in files:
                    partition_path: str = path.join(dirs, file)

                    if path.isfile(partition_path) is False or self.file_mask not in file:
                        continue

                    input_path = get_root_path(
                        path_dir=partition_path,
                        tail=tail)

                    partition_date: str = partition_path\
                        .replace(input_path, '')\
                        .replace(file, '')
                    interested_partition_path: str = get_cross_os_path(
                        [input_path, partition_date, file]
                    )

                    self.interested_partition.setdefault(partition_date, {}).update(
                        {file: interested_partition_path})

    def get_data_path_part(self, *, input_partition_path: str) -> str:
        """
        Get date path part.
        :param input_partition_path: Path to partition in string format.
        :type input_partition_path: str
        """
        if self.date_parameter is None:
            date_path_part: str = parsing_date_part_path(input_partition_path)
        else:
            date_path_part: str = path.join(*[
                str(self.date_parameter.year),
                str(self.date_parameter.month),
                str(self.date_parameter.day)
            ])
        return date_path_part

    def task_output_path_parser(self):
        """
        Generate paths for Luigi LocalTargets and output data files.
        """
        for input_partition_path in self.interested_partition.keys():
            date_path_part: str = self.get_data_path_part(
                input_partition_path=input_partition_path
            )

            output_partition_path: str = get_cross_os_path(
                [self.partition_path, date_path_part]
            )
            self.output_paths_list.append(output_partition_path)
