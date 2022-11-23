from os import walk, path, makedirs
import json
import typing

from pandas import DataFrame, read_csv, read_json
from numpy import NaN
from pyarrow import Table, parquet

from tests_my_beautiful_task import test_output_df, test_output_file_exist


def my_beautiful_task_path_parser(result_successor: list or tuple or str, dir_list: list,
                                  interested_partition: dict[str], file_mask: str):
    """
    Path inheritance from result_successor.
    """
    if result_successor is list or result_successor is tuple:
        for flag in result_successor:
            path_to_table: str = str.replace(flag.path, '_Validate_Success', '')
            dir_list.append(path_to_table)
    else:
        path_to_table: str = str.replace(result_successor.path, '_Validate_Success', '')
        dir_list.append(path_to_table)
    for parsing_dir in dir_list:  # Path parsing.
        for dirs, folders, files in walk(parsing_dir):
            for file in files:
                partition_path = f'{dirs}{file}'
                if path.isfile(partition_path) and file_mask in file:
                    partition_path_split: list[str] = partition_path.split('/')
                    partition_file: str = partition_path_split[-1]
                    partition_date = f'{partition_path_split[-4]}/{partition_path_split[-3]}' \
                                     f'/{partition_path_split[-2]}/'
                    partition_path: str = str.replace(partition_path, partition_date + partition_file, '')
                    interested_partition_path = f'{partition_path}{partition_date}{partition_file}'
                    interested_partition.setdefault(partition_date, {}).update(
                        {partition_file: interested_partition_path})


def my_beautiful_task_data_landing(data_to_landing: dict or DataFrame, day_for_landing: str,
                                   output_path_list: list, partition_path: str, file_mask: str) -> list[str]:
    """
    Landing parsed data as json, csv or parquet.
    """
    data_type_need: list[str] = file_mask.split('.')
    data_type_need: str = data_type_need[1]
    output_path = f'{partition_path}/{day_for_landing}'
    data_from_files: DataFrame = DataFrame(data_to_landing)
    test_output_df(data_from_files)
    if not path.exists(output_path):
        makedirs(output_path)
    flag_path = f'{output_path}{"_Validate_Success"}'
    output_path = f'{output_path}{file_mask}'
    if data_type_need == 'json':
        data_from_files: str = data_from_files.to_json(orient='records')
        data_from_files: json = json.loads(data_from_files)
        json_data: json = json.dumps(data_from_files, indent=4, ensure_ascii=False)
        with open(output_path, 'w', encoding='utf-8') as json_file:
            json_file.write(json_data)
    if data_type_need == 'parquet':
        parquet_table: Table = Table.from_pandas(data_to_landing)
        parquet.write_table(parquet_table, output_path, use_dictionary=False, compression=None)
    if data_type_need == 'csv':
        data_to_csv: str = data_to_landing.to_csv(index=False)
        with open(output_path, 'w') as csv_file:
            csv_file.write(data_to_csv)
    test_output_file_exist(output_path)
    with open(flag_path, 'w'):
        pass
    test_output_file_exist(flag_path)
    output_path_list.append(flag_path)
    return output_path_list


def my_beautiful_task_data_frame_merge(data_from_files: DataFrame or None, extract_data: DataFrame) -> DataFrame:
    """
    Merges the given dataframes into one, filling empty cells with NaNs.
    """
    if data_from_files is None:
        data_from_files: DataFrame = extract_data
    else:
        new_point_for_merge = extract_data.columns.difference(data_from_files.columns)
        for column in new_point_for_merge:
            data_from_files.astype(object)[column] = NaN
        data_from_files = data_from_files.merge(extract_data, how='outer')
    return data_from_files


def my_beautiful_task_data_table_parser(interested_partition: dict[DataFrame], drop_list: list or None,
                                        interested_data: dict[DataFrame], file_mask: str):
    """
    Universal reading of data from tables.
    """
    def how_to_extract(*args):  # Defining a data read method for pandas.
        how_to_extract_format = None
        if file_mask == '.csv':
            how_to_extract_format = read_csv(*args).astype(str)
        if file_mask == '.json':
            how_to_extract_format = read_json(*args, dtype='int64')
            # json requires manual output type for long numbers.
        return how_to_extract_format

    for key in interested_partition:
        data_from_files: None = None
        files: dict.get = interested_partition.get(key)
        files = files.values()
        for file in files:  # Parsing tables into a raw dataframe
            if drop_list is not None:
                extract_data = how_to_extract(file).drop([drop_list], axis=1)
            else:
                extract_data = how_to_extract(file)
            # Merging dataframes:
            data_from_files: DataFrame = my_beautiful_task_data_frame_merge(data_from_files, extract_data)
        interested_data[key] = data_from_files


def nan_pandas_df_converter(can_have_nan: list or tuple or str) -> list[NaN]:
    """
    Takes a list, tuple, or string.
    Converts the strings 'None' and 'NaN' from the given variables to the NunPy NaN data type.
    Returns a list with an element.
    """
    if isinstance(can_have_nan, typing.List) or isinstance(can_have_nan, typing.Tuple):
        if "NaN" in can_have_nan:
            can_have_nan = list(can_have_nan)
            can_have_nan[can_have_nan.index("NaN")] = NaN
    if isinstance(can_have_nan, str):
        if can_have_nan == "NaN":
            can_have_nan: list[NaN] = [NaN]
    return can_have_nan


def str_from_argument_converter(element: str) -> str or bool or NaN:
    """
    Converts string values from Luigi task arguments to standard python values.
    """
    if element == "NaN":
        element = nan_pandas_df_converter(element)
        element = element[0]
    if element == 'None':
        element = None
    if element == 'False':
        element = False
    if element == 'True':
        element = True
    return element


def my_beautiful_task_universal_parser_part(result_successor: list or tuple or str,
                                            file_mask: str, drop_list: list or None) -> dict[DataFrame]:
    """
    Runs code after inheriting paths from the previous task.
    """
    interested_partition = {}
    dir_list = []
    my_beautiful_task_path_parser(result_successor, dir_list, interested_partition, file_mask)
    # Parsing data from files along the paths inherited from the previous task:
    interested_data = {}
    my_beautiful_task_data_table_parser(interested_partition, drop_list, interested_data, file_mask)
    return interested_data


def my_beautiful_task_universal_data_landing_part(self, interested_data: dict[str: DataFrame],
                                                  partition_path: str, file_mask: str):
    """
    Runs data landing code for each inherited date.
    """
    for key in interested_data:
        data_to_landing = interested_data.get(key)
        day_for_landing = key
        my_beautiful_task_data_landing(data_to_landing, day_for_landing,
                                       self.output_path_list, partition_path, file_mask)
