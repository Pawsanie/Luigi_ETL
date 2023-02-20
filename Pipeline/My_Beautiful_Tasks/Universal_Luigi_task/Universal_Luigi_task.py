from os import sep

from luigi import Task, LocalTarget

from .Data_Landing import DataLanding
from .Paths_Parser import PathsParser
from .Data_Parser import DataParser
"""
Contents code for Universal Task.
"""


class UniversalLuigiTask(Task, DataLanding, PathsParser, DataParser):
    """
    Super class for pipeline.
    """
    # File format:
    file_mask: str = ""
    #
    day_for_landing: str = ""
    date_path_part: str = ""

    # Luigi local target file:
    success_flag: str = "_Validate_Success"
    # Local targets from past Luigi Task:
    result_successor: list[LocalTarget] | tuple[LocalTarget] | LocalTarget = ""
    # List with paths for processing:
    input_path_list: list = []

    # Dictionary with paths from which to read partitions:
    interested_partition: dict = {}
    # Dictionary with partition data for processing:
    interested_data: dict = {}
    # List of columns to be drop during data processing:
    drop_list: list = []

    # List with paths for data landing:
    output_paths_list: list = []

    def output(self) -> set[LocalTarget]:
        """
        Output Luigi.output method for task.
        """
        self.devnull_legacy_paths()
        result: set = set()
        self.get_targets()
        for dir_path in self.output_paths_list:
            result.add(LocalTarget(f"{dir_path}{sep}{self.success_flag}"))
        return result

    def get_targets(self):
        """
        The method must be overridden in every task.
        Collect paths for the run and output methods.
        """
        ...
        self.task_input_path_parser()
        self.task_output_path_parser()
        ...

    def task_universal_parser_part(self):
        """
        Runs code after inheriting paths from the previous task.
        """
        # Parsing data from files along the paths inherited from the previous task:
        self.task_data_table_parser()

    def task_universal_data_landing_part(self):
        """
        Runs data landing code for each inherited date.
        """
        for key in self.interested_data:
            data_to_landing = self.interested_data.get(key)
            day_for_landing_path_part = key
            self.task_data_landing(
                data_to_landing=data_to_landing,
                day_for_landing_path_part=day_for_landing_path_part
            )

    def devnull_legacy_paths(self):
        """
        Clear data.
        """
        for collection in [
            self.interested_data,
            self.interested_partition,
            self.input_path_list,
            self.output_paths_list,
        ]:
            collection.clear()

    def devnull_legacy_collections(self):
        """
        Clear data.
        """
        for collection in [
            self.interested_data,
            self.interested_partition,
        ]:
            collection.clear()
