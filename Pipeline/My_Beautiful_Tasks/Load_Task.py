from luigi import Parameter, configuration

from .Universal_Luigi_task.Universal_Luigi_task import UniversalLuigiTask
from .Universal_Luigi_task.Get_Luigi_Config import get_config
from .Transform_Task import TransformTask
from .Tests.tests_my_beautiful_task import test_path_mask_type_for_date, test_file_mask_arguments
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

    task_namespace: str = 'LoadTask'
    priority: int = 300

    # Task parameters:
    file_name: str = 'load_data_result'
    dependency: str = 'Transform'

    def requires(self):
        return {self.dependency: TransformTask()}

    def run(self):
        ...
        # file_mask: str = self.load_file_mask
        # test_file_mask_arguments(file_mask)
        # result_successor = self.input()['TransformTask']  # Path inheritance from TransformTask.
        # interested_data = my_beautiful_task_universal_parser_part(result_successor, file_mask, drop_list=None)
        #
        # parsing_data = None
        # for data in interested_data.values():
        #     parsing_data = my_beautiful_task_data_frame_merge(parsing_data, data)
        #
        # file_mask = 'LoadTask.parquet'
        # partition_path = f"{self.load_data_path}"
        # test_path_mask_type_for_date(partition_path)
        # my_beautiful_task_universal_data_landing_part(self, interested_data, partition_path, file_mask)

    def get_targets(self):
        """
        Get Luigi LocalTargets paths, for Luigi.output method and task processing.
        """
        # Arguments parsing:
        # Output path:
        self.partition_path: str = self.load_data_path
        test_path_mask_type_for_date(self.partition_path)
        # File format:
        self.file_mask: str = self.load_file_mask
        test_file_mask_arguments(self.file_mask)
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
    }
    return config_result
