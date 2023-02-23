from os import path, sep
from datetime import date

from luigi import Parameter, configuration, DateParameter
from pandas import DataFrame

from .Universal_Luigi_task.Universal_Luigi_task import UniversalLuigiTask
from .Universal_Luigi_task.Get_Luigi_Config import get_config
from .Transform_Task import TransformTask
from .Tests.tests_my_beautiful_task import test_path_mask_type_for_date, test_file_mask_arguments
from .Universal_Luigi_task.path_for_windows import parsing_date_part_path
"""
Contents code for Load task.
"""


class LoadTask(UniversalLuigiTask):
    """
    Landing data.
    """
    # Luigi parameters:
    load_data_path: str = Parameter(significant=True, description='Root path for LoadTask files')
    load_file_mask: str = Parameter(significant=True, description='File type Mask')
    transform_file_mask: str = Parameter(significant=True, description='File type Mask')
    date_parameter: date = DateParameter(default=date.today())

    task_namespace: str = 'LoadTask'
    priority: int = 300

    # Task parameters:
    file_name: str = 'load_data_result'
    dependency: str = 'Transform'
    parsing_data: DataFrame | None = None

    def requires(self):
        """
        Dependency Luigi.requires method for Load task.
        """
        return {self.dependency: TransformTask()}

    def run(self):
        """
        Run Luigi.run method for Load task.
        """
        self.get_targets()
        self.task_universal_parser_part()

        for data in self.interested_data.values():
            self.parsing_data: DataFrame = self.task_data_frame_merge(self.parsing_data, data)

        date_path_part: str = path.join(
            parsing_date_part_path(
                self.date_parameter.strftime(f"%Y{sep}%m{sep}%d")
            ))
        self.task_data_landing(
            data_to_landing=self.parsing_data,
            day_for_landing_path_part=date_path_part
        )

    def get_targets(self):
        """
        Get Luigi LocalTargets paths, for Luigi.output method and task processing.
        """
        # Arguments parsing:
        # Output path:
        self.partition_path: str = self.load_data_path
        test_path_mask_type_for_date(self.partition_path)
        # File format:
        self.input_file_mask: str = self.transform_file_mask
        test_file_mask_arguments(self.input_file_mask)
        self.output_file_mask: str = self.load_file_mask
        test_file_mask_arguments(self.output_file_mask)
        # Input path:
        self.result_successor = self.input()[self.dependency]

        # Paths processing:
        self.task_input_path_parser()
        self.task_output_path_parser()


def load_config() -> dict[str, configuration]:
    """
    Generate configuration for LoadTask.
    """
    config: configuration = get_config()
    config_result: dict[str, configuration] = {
        "load_data_path": config.get('LoadTask', 'load_data_path'),
        "load_file_mask": config.get('LoadTask', 'load_file_mask'),
        "transform_file_mask": config.get('TransformTask', 'transform_file_mask')
    }
    return config_result
