from datetime import date
from configparser import NoOptionError
from ast import literal_eval
from os import path, sep
import logging

from luigi import Parameter, configuration, DateParameter, DictParameter
from pandas import DataFrame
from numpy import NaN

from .Universal_Luigi_task.Universal_Luigi_task import UniversalLuigiTask
from .Universal_Luigi_task.Get_Luigi_Config import get_config
from .Extract_Task import ExtractTask
from .Tests.tests_my_beautiful_task import test_path_mask_type_for_date, test_file_mask_arguments, \
    test_transform_task_time_mask
from .Universal_Luigi_task.path_for_windows import parsing_date_part_path
from .Tests.Logging_Config import text_for_logging
"""
Contents code for Transform task.
"""


class TransformTask(UniversalLuigiTask):
    """
    Data cleansing.
    """
    # Luigi parameters:
    file_to_transform_path: str = Parameter(significant=True, description='Root path for ExtractTask files')
    transform_file_mask: str = Parameter(significant=True, description='File type Mask')
    extract_file_mask: str = Parameter(significant=True, description='File type Mask')
    date_parameter: date = DateParameter(default=date.today())
    transform_parsing_rules_drop: dict = DictParameter(
        significant=False, default=None,
        description='Dictionary with parsing rules (must be dropped)')
    transform_parsing_rules_filter: dict = DictParameter(
        significant=False, default=None,
        description='Dictionary with parsing rules (analise and drop)')
    transform_parsing_rules_vip: dict = DictParameter(
        significant=False, default=None,
        description='Dictionary with parsing rules (Interesting data)')

    task_namespace: str = 'TransformTask'
    priority: int = 100

    # Task parameters:
    file_name: str = 'transform_data_result'
    dependency: str = 'Extract'
    parsing_data: DataFrame | None = None
    vip_list: DataFrame = DataFrame()

    def requires(self):
        """
        Dependency Luigi.requires method for transform task.
        """
        return {self.dependency: ExtractTask()}

    def run(self):
        """
        Run Luigi.run method for transform task.
        """
        # Get paths and raw data:
        self.get_targets()
        self.task_universal_parser_part()

        # Drop values in columns by rules from transform_parsing_rules_drop parameter:
        for data in self.interested_data.values():
            self.parsing_data: DataFrame or None = self.task_merge_with_concatenate(self.parsing_data, data)

        self.data_frame_filter_drop()
        self.data_frame_parsing()

        test_transform_task_time_mask(self.date_parameter)
        date_path_part: str = path.join(
            parsing_date_part_path(
                self.date_parameter.strftime(f"%Y{sep}%m{sep}%d")
            ))

        self.task_data_landing(
            data_to_landing=self.parsing_data,
            day_for_landing_path_part=date_path_part
        )

    def data_frame_filter_drop(self):
        """
        All DataFrame rows will be filtered out if all conditions are satisfied:
        1) DataFrame column name is key of transform_parsing_rules_drop parameter dictionary.
        2) Row cell value of this column in list of dictionary key value.

        Example: {'column_name': ['element_from_list_in_column_values'], ...}
        """
        transform_parsing_rules_drop: dict = self.transform_parsing_rules_drop
        if transform_parsing_rules_drop is not None:
            for element in transform_parsing_rules_drop.keys():
                try:
                    rule: list[str] = transform_parsing_rules_drop.get(element)
                    rule: list[str, NaN] = self.nan_pandas_df_converter(rule)
                    rules_drop: DataFrame = self.parsing_data[self.parsing_data[element].isin(rule)]
                    rules_drop: DataFrame = self.parsing_data[~self.parsing_data.index.isin(rules_drop.index)]
                    self.parsing_data: DataFrame = rules_drop
                except KeyError as error:
                    logging.warning(
                        text_for_logging(
                            log_text=
                            f"Column with key name '{element}' dos not exist in DataFrame.\n"
                            f"Problem with 'transform_parsing_rules_drop' Luigi.DictParameter...",
                            log_error=error))

    def data_frame_parsing(self):
        """
        Rows will be discarded if at least one value matches in ALL transform_parsing_rules_byte keys.
        And provided that the string does not contain values from the keys transform_parsing_rules_vip.
        """
        if self.transform_parsing_rules_filter is None:
            return

        for column in self.parsing_data:
            if column in self.transform_parsing_rules_filter:
                for index, row in self.parsing_data.iterrows():

                    self.get_vip_list(row)
                    self.data_frame_filter(row, column, index)

        if self.transform_parsing_rules_vip is not None and len(self.vip_list) > 0:
            self.parsing_data: DataFrame = self.task_data_frame_merge(self.parsing_data, self.vip_list)

    def data_frame_filter(self, row, column, index):
        """
        Rows will be discarded if at least one value matches in ALL keys of transform_parsing_rules_byte.
        """
        parsing_for_byte_count: int = 0
        parsing_for_byte_element_count: int = 0
        parsing_for_byte_is_in: int = 0

        row: str = row[column]
        for element in self.transform_parsing_rules_filter.get(column):
            element = self.str_from_argument_converter(element)
            if row == element:
                parsing_for_byte_count += 1
                parsing_for_byte_is_in += 1
        if parsing_for_byte_is_in > 0:
            parsing_for_byte_element_count += 1
        if parsing_for_byte_count == parsing_for_byte_element_count and parsing_for_byte_count > 0:
            self.parsing_data: DataFrame = \
                self.parsing_data.drop(self.parsing_data.index[[index - 1]])  # index counts from 1.

    def get_vip_list(self, row):
        """
        Collects rows that must remain.
        All strings whose values match any of the 'transform_parsing_rules_vip'
        dictionary keys values will be stored.
        """
        if self.transform_parsing_rules_vip is not None:
            for cell in row:
                for vip in self.transform_parsing_rules_vip:
                    if cell in self.transform_parsing_rules_vip.get(vip):
                        self.vip_list: DataFrame = self.vip_list.append(row, ignore_index=True)

    def get_targets(self):
        """
        Get Luigi LocalTargets paths, for Luigi.output method and task processing.
        """
        # Arguments parsing:
        # Output path:
        self.partition_path: str = self.file_to_transform_path
        test_path_mask_type_for_date(self.partition_path)
        # File format:
        self.output_file_mask: str = self.transform_file_mask
        test_file_mask_arguments(self.output_file_mask)
        self.input_file_mask: str = self.extract_file_mask
        test_file_mask_arguments(self.input_file_mask)
        self.parsing_data: None = None
        # Input path:
        self.result_successor = self.input()[self.dependency]

        # Paths processing:
        self.task_input_path_parser()
        self.task_output_path_parser()


def transform_config() -> dict[str, configuration]:
    """
    Generate configuration for TransformTask.
    """
    config: configuration = get_config()
    config_result: dict[str, configuration] = {
        "file_to_transform_path": config.get('TransformTask', 'file_to_transform_path'),
        "transform_file_mask": config.get('TransformTask', 'transform_file_mask'),
        "extract_file_mask": config.get('ExtractTask', 'extract_file_mask')
    }

    try:
        config_result.update({"date_parameter": config.get('TransformTask', 'date_parameter')})
    except NoOptionError:
        config_result.update({"date_parameter": date.today()})

    try:
        config_result.update(
            {"transform_parsing_rules_drop":
                literal_eval(
                    config.get('TransformTask', 'transform_parsing_rules_drop')[1:-1:]
                )})
    except NoOptionError:
        config_result.update({"transform_parsing_rules_drop": None})

    try:
        config_result.update(
            {"transform_parsing_rules_filter":
                literal_eval(
                    config.get('TransformTask', 'transform_parsing_rules_filter')[1:-1:]
                )})
    except NoOptionError:
        config_result.update({"transform_parsing_rules_filter": None})

    try:
        config_result.update(
            {"transform_parsing_rules_vip":
                literal_eval(
                    config.get('TransformTask', 'transform_parsing_rules_vip')[1:-1:]
                )})
    except NoOptionError:
        config_result.update({"transform_parsing_rules_vip": None})

    return config_result
