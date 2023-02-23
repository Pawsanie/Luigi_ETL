from os import path, walk

from luigi import LocalTarget, ExternalTask, Parameter, configuration

from .Tests.tests_my_beautiful_task import test_path_mask_type_for_date, test_external_task_path
from .Universal_Luigi_task.path_for_windows import get_cross_os_path
from .Universal_Luigi_task.Get_Luigi_Config import get_config
"""
Contents code for ExternalData task.
"""


class ExternalDataTask(ExternalTask):
    """
    Wrappers for data from external sources.
    Looks for directories in "external_data_path" that match the requirements:
    1) End up on a path "dataset_name/YYYY/MM/DD/".
       Or its equivalent in Windows.
    2) Inside the directory, in addition to the partition, there must be a file
       the name of which is written in variable "validate_flag": "_Validate" as default.
    Creates "success_flag" files for the pipeline: "_Validate_Success" as default.
    """
    # Luigi parameters:
    external_data_path: str = Parameter(significant=True, description='Root path for ExternalData files')

    task_namespace: str = 'ExternalData'
    priority: int = 200

    # Task variables:
    external_data_result: list = []
    validate_flag: str = "_Validate"
    success_flag: str = "_Validate_Success"

    def output(self) -> set[LocalTarget]:
        """
        Output Luigi.output method for wrapper task.
        """
        self.get_targets()
        result: set = set()
        for target in self.external_data_result:
            result.add(LocalTarget(target))
        return result

    def run(self):
        """
        Run Luigi.run method for wrapper task.
        """
        for target in self.external_data_result:
            with open(target, "w"):
                pass

    def get_targets(self):
        """
        Get Luigi LocalTargets paths, for Luigi.output method and ExternalData task processing.
        """
        input_path: str = self.external_data_path
        test_path_mask_type_for_date(input_path)
        test_external_task_path(input_path)

        for dirs in walk(input_path):
            interest_partition_path = path.join(*[dirs[0], self.validate_flag])

            if path.isfile(interest_partition_path) is False:
                continue
            partition_dir: str = str.replace(dirs[0], input_path, '')
            path_of_partition: str = str.replace(input_path, partition_dir, '')

            flag_path: str = get_cross_os_path(
                [path_of_partition, partition_dir, self.success_flag]
            )
            self.external_data_result.append(flag_path)
        self.external_data_result: list = list(set(self.external_data_result))


def external_data_config() -> dict[str, configuration]:
    """
    Generate configuration for ExternalDataTask.
    """
    config: configuration = get_config()
    return {
        "external_data_path": config.get('ExternalData', 'external_data_path')
    }
