from configparser import NoOptionError

from luigi import Parameter, ListParameter, configuration

from .Tests.tests_my_beautiful_task import test_path_mask_type_for_date, test_file_mask_arguments
from .External_Data_task import ExternalDataTask
from .Universal_Luigi_task.Universal_Luigi_task import UniversalLuigiTask
from .Universal_Luigi_task.Get_Luigi_Config import get_config
"""
Contents code for Extract task.
"""


class ExtractTask(UniversalLuigiTask):
    """
    Retrieving data from ExternalData sources.
    Combining into one array.
    """
    # Luigi parameters:
    extract_data_path: str = Parameter(significant=True, description='Root path for ExtractTask files')
    extract_file_mask: str = Parameter(significant=True, description='File type Mask')
    external_data_file_mask: str = Parameter(significant=True, description='File type Mask')
    # Extra columns that need to be dropped in tables:
    drop_list: list = ListParameter(significant=False, default=None)

    task_namespace: str = 'ExtractTask'
    priority: int = 100

    # Task parameters:
    file_name: str = 'extract_data_result'
    dependency: str = 'ExternalData'

    def requires(self) -> dict:
        """
        Dependency Luigi.requires method for extract task.
        """
        return {self.dependency: ExternalDataTask()}

    def run(self):
        """
        Run Luigi.run method for extract task.
        """
        self.get_targets()
        # Arguments parsing:
        self.drop_list: list = self.drop_list
        # Data processing:
        self.task_universal_parser_part()
        # Data landing:
        self.task_universal_data_landing_part()

    def get_targets(self):
        """
        Get Luigi LocalTargets paths, for Luigi.output method and task processing.
        """
        # Arguments parsing:
        # Output path:
        self.partition_path: str = self.extract_data_path
        test_path_mask_type_for_date(self.partition_path)
        # File format:
        self.output_file_mask: str = self.extract_file_mask
        test_file_mask_arguments(self.output_file_mask)
        self.input_file_mask: str = self.external_data_file_mask
        test_file_mask_arguments(self.input_file_mask)
        # Input path:
        self.result_successor = self.input()[self.dependency]

        # Paths processing:
        self.task_input_path_parser()
        self.task_output_path_parser()


def extract_config() -> dict[str, configuration]:
    """
    Generate configuration for ExtractTask.
    """
    config: configuration = get_config()
    config_result: dict[str, configuration] = {
        "extract_data_path": config.get('ExtractTask', 'extract_data_path'),
        "extract_file_mask": config.get('ExtractTask', 'extract_file_mask'),
        "external_data_file_mask": config.get('ExtractTask', 'external_data_file_mask'),
    }
    try:
        config_result.update({"drop_list": config.get('ExtractTask', 'drop_list')})
    except NoOptionError:
        config_result.update({"drop_list": None})
    return config_result
