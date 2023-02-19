from os import path, makedirs, sep
import json

from pandas import DataFrame
from pyarrow import parquet, Table

from ..Tests.tests_my_beautiful_task import test_output_df, test_output_file_exist
from .path_for_windows import get_cross_os_path
"""
Contents code for data landing.
"""


class DataLanding:
    """
    Landing task parsed data to DWH or data lake.
    """
    # Dataset directory:
    partition_path: str = ''
    # Directory with date for data landing"
    output_dir: str = ""
    # Path with date and file name:
    output_path: str = ''
    # File name part:
    file_name: str = ''
    # File format:
    file_mask: str = ''
    # Name of success flag file:
    success_flag: str = ""

    def json_landing(self, data_from_files: DataFrame):
        """
        Landing json dict.
        :param data_from_files: Parsed data fro landing.
        :type data_from_files: DataFrame
        """
        data_from_files: str = data_from_files.to_json(orient='records')
        data_from_files: json = json.loads(data_from_files)
        json_data: json = json.dumps(data_from_files, indent=4, ensure_ascii=False)
        with open(self.output_path, 'w', encoding='utf-8') as json_file:
            json_file.write(json_data)

    def parquet_landing(self, data_from_files: DataFrame):
        """
        Landing parquet table.
        :param data_from_files: Parsed data fro landing.
        :type data_from_files: DataFrame
        """
        parquet_table: Table = Table.from_pandas(data_from_files)
        parquet.write_table(
            parquet_table,
            self.output_path,
            use_dictionary=False,
            compression=None)

    def csv_landing(self, data_from_files: DataFrame):
        """
        Landing csv table.
        :param data_from_files: Parsed data fro landing.
        :type data_from_files: DataFrame
        """
        data_to_csv: str = data_from_files.to_csv(index=False)
        with open(self.output_path, 'w') as csv_file:
            csv_file.write(data_to_csv)

    def make_success_flag(self):
        """
        Make success flag for Luigi.
        """
        success_flag_path: str = f"{self.output_dir}{sep}{self.success_flag}"
        with open(success_flag_path, 'w'):
            pass
        test_output_file_exist(success_flag_path)

    def task_data_landing(self, *, data_to_landing: dict or DataFrame, day_for_landing_path_part: str):
        """
        Landing parsed data as json, csv or parquet.
        :param data_to_landing:
        :type data_to_landing: data_to_landing: dict | DataFrame
        :param day_for_landing_path_part: Part of path to partition with date.
        :type day_for_landing_path_part: str
        """
        if type(data_to_landing) is not DataFrame:
            data_from_files: DataFrame = DataFrame(data_to_landing)
        else:
            data_from_files: DataFrame = data_to_landing
        test_output_df(data_from_files)

        self.output_dir: str = get_cross_os_path(
            [self.partition_path, day_for_landing_path_part]
        )
        if not path.exists(self.output_dir):
            makedirs(self.output_dir)

        self.output_path: str = f"{self.output_dir}{self.file_name}.{self.file_mask}"
        if self.file_mask == 'json':
            self.json_landing(data_from_files)
        elif self.file_mask == 'parquet':
            self.parquet_landing(data_from_files)
        elif self.file_mask == 'csv':
            self.csv_landing(data_from_files)
        test_output_file_exist(self.output_path)

        self.make_success_flag()
