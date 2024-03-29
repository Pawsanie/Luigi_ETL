import logging
import typing

from pandas import DataFrame, read_csv, read_json, concat
from numpy import NaN

from ..Tests.Logging_Config import text_for_logging
"""
Contents code for DataParser.
"""


class DataParser:
    # File format:
    input_file_mask: str = ""
    # Dictionary with paths from which to read partitions:
    interested_partition: dict = {}
    # Dictionary with partition data for processing:
    interested_data: dict = {}
    # List of columns to be drop during data processing:
    drop_list: list | None = None

    def task_data_table_parser(self):
        """
        Universal reading of data from tables.
        """
        self.interested_data.clear()

        def how_to_extract(*args):  # Defining a data read method for pandas.
            how_to_extract_format = None
            if self.input_file_mask == 'csv':
                how_to_extract_format = read_csv(*args).astype(str)
            if self.input_file_mask == 'json':
                how_to_extract_format = read_json(*args, dtype='int64', convert_dates=False)
                # json requires manual output type for long numbers.
            return how_to_extract_format

        for key in self.interested_partition:
            data_from_files: None = None
            files: dict.get = self.interested_partition.get(key)
            files = files.values()
            for file in files:  # Parsing tables into a raw dataframe
                if self.drop_list is not None:
                    columns_for_drop: list = []
                    extract_data: DataFrame = how_to_extract(file)
                    data_frame_columns: list[str] = list(extract_data.columns)

                    for drop_name in list(self.drop_list):
                        if drop_name in data_frame_columns:
                            columns_for_drop.append(drop_name)
                        else:
                            text_for_logging(
                                log_text=
                                f"Column with name '{drop_name}' dos not exist in DataFrame.\n"
                                f"Problem with 'drop_list' Luigi.ListParameter...",
                                log_error=KeyError(drop_name))

                    extract_data: DataFrame = extract_data.drop(columns_for_drop, axis=1)
                else:
                    extract_data: DataFrame = how_to_extract(file)
                # Merging dataframes:
                data_from_files: DataFrame = self.task_data_frame_merge(data_from_files, extract_data)
            self.interested_data[key] = data_from_files

    def task_data_frame_merge(self, data_from_files: DataFrame or None, extract_data: DataFrame) -> DataFrame:
        """
        Merges the given dataframes into one, filling empty cells with NaNs.

        :param data_from_files: Already extracted data to the data pool.
        :type data_from_files: None | DataFrame
        :param extract_data: New extract data for data pull.
        :type extract_data: DataFrame
        """
        if data_from_files is None:
            data_from_files: DataFrame = extract_data
        else:
            new_point_for_merge = extract_data.columns.difference(data_from_files.columns)
            for column in new_point_for_merge:
                data_from_files.astype(object)[column] = NaN
            data_from_files = data_from_files.merge(extract_data, how='outer')
        return data_from_files

    def nan_pandas_df_converter(self, can_have_nan: list or tuple or str) -> list[NaN]:
        """
        Takes a list, tuple, or string.
        Converts the strings 'None' and 'NaN' from the given variables to the NunPy NaN data type.
        Returns a list with an element.

        :param can_have_nan: Row or cell value from Pandas Data Frame for convert.
        :type can_have_nan: list | tuple | str
        """
        if isinstance(can_have_nan, typing.List) or isinstance(can_have_nan, typing.Tuple):
            if "NaN" in can_have_nan:
                can_have_nan = list(can_have_nan)
                can_have_nan[can_have_nan.index("NaN")] = NaN
        if isinstance(can_have_nan, str):
            if can_have_nan == "NaN":
                can_have_nan: list[NaN] = [NaN]
        return can_have_nan

    def str_from_argument_converter(self, element: str) -> str or bool or NaN:
        """
        Converts string values from Luigi task arguments to standard python values.
        :param element: Table cell value for convert.
        :type element: str
        """
        if element == "NaN":
            element = self.nan_pandas_df_converter(element)
            element = element[0]
        if element == 'None':
            element = None
        if element == 'False':
            element = False
        if element == 'True':
            element = True
        return element

    def task_data_frame_concatenate(self, data_from_files: DataFrame or None, extract_data: DataFrame) -> DataFrame:
        """
        Concatenate the given dataframes into one, filling empty cells with NaNs.

        :param data_from_files: Already extracted data to the data pool.
        :type data_from_files: None | DataFrame
        :param extract_data: New extract data for data pull.
        :type extract_data: DataFrame
        """
        if data_from_files is None:
            data_from_files: DataFrame = extract_data
        else:
            new_point_for_merge = extract_data.columns.difference(data_from_files.columns)
            for column in new_point_for_merge:
                data_from_files.astype(object)[column] = NaN
            data_from_files = concat([data_from_files, extract_data], join='outer')
        return data_from_files

    def task_merge_with_concatenate(self, data_from_files: DataFrame or None, extract_data: DataFrame) -> DataFrame:
        """
        Merges the given dataframes into one, if it possible.
        If is not - concatenate them.

        :param data_from_files: Already extracted data to the data pool.
        :type data_from_files: None | DataFrame
        :param extract_data: New extract data for data pull.
        :type extract_data: DataFrame
        """
        try:
            return self.task_data_frame_merge(data_from_files, extract_data)
        except ValueError as error:
            logging.warning(
                text_for_logging(
                    log_text=
                    f"Impossible to merge two DataFrames: try to concatenate...",
                    log_error=error
                ))
            return self.task_data_frame_concatenate(data_from_files, extract_data)
