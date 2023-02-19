from My_Beautiful_Tasks.Pipeline_launcher import pipeline_launcher
from My_Beautiful_Tasks.External_Data_task import ExternalDataTask, external_data_config
from My_Beautiful_Tasks.Extract_Task import ExtractTask, extract_config
"""
Contents ETL pipeline code.
"""


class ExternalData(ExternalDataTask):
    """
    Wrappers for data from external sources.
    """
    # Task settings:
    task_namespace = 'ExternalData'
    priority = 200


class Extract(ExtractTask):
    """
    Retrieving data from ExternalData sources.
    Combining into one array.
    """
    task_namespace = 'ExtractTask'
    priority = 100

    def requires(self):
        return {'ExternalData': ExternalData(
            external_data_path=external_data_config()['external_data_path']
        )}


# class TransformTask(Task):
#     """
#     Data cleansing.
#     """
#     task_namespace = 'TransformTask'
#     priority = 100
#     file_to_transform_path: str = Parameter(significant=True, description='Root path for ExtractTask files')
#     transform_file_mask: str = Parameter(significant=True, description='File type Mask')
#     date_path_part: date = DateParameter(default=date.today())
#     transform_parsing_rules_drop: dict = DictParameter(
#         significant=False, default=None,
#         description='Json obj. with parsing rules (must be dropped)')
#     transform_parsing_rules_byte: dict = DictParameter(
#         significant=False, default=None,
#         description='Json obj. with parsing rules (analise and drope)')
#     transform_parsing_rules_vip: dict = DictParameter(
#         significant=False, default=None,
#         description='Json obj. with parsing rules (Interesting data)')
#     output_path_list = []
#
#     def requires(self):
#         return {'ExtractTask': ExtractTask()}
#
#     def output(self):
#         for path_of_flag in self.output_path_list:
#             yield LocalTarget(path.join(path_of_flag))
#
#     def run(self):
#         file_mask: str = self.transform_file_mask
#         test_file_mask_arguments(file_mask)
#         result_successor = self.input()['ExtractTask']  # Path inheritance from ExtractTask.
#         interested_data: dict[DataFrame] = my_beautiful_task_universal_parser_part(
#             result_successor,
#             file_mask,
#             drop_list=None)
#
#         parsing_data = None
#         for data in interested_data.values():
#             parsing_data: DataFrame or None = my_beautiful_task_data_frame_merge(parsing_data, data)
#         """
#         All elements in transform_parsing_rules_drop will be filtered out.
#         """
#         transform_parsing_rules_drop: dict = self.transform_parsing_rules_drop
#         if transform_parsing_rules_drop is not None:
#             for element in transform_parsing_rules_drop.keys():
#                 rule = transform_parsing_rules_drop.get(element)
#                 rule = nan_pandas_df_converter(rule)
#                 rules_drop = parsing_data[parsing_data[element].isin(rule)]
#                 rules_drop = parsing_data[~parsing_data.index.isin(rules_drop.index)]
#                 parsing_data = rules_drop
#         """
#         Rows will be discarded if at least one value matches in ALL transform_parsing_rules_byte keys.
#         And provided that the string does not contain values from the keys transform_parsing_rules_vip.
#         """
#         transform_parsing_rules_byte = self.transform_parsing_rules_byte
#         transform_parsing_rules_vip = self.transform_parsing_rules_vip
#         vip_list = DataFrame()
#         parsing_for_byte_count = 0
#         if transform_parsing_rules_byte is not None:
#             for column in parsing_data:
#                 if column in transform_parsing_rules_byte:
#                     for index, row in parsing_data.iterrows():
#                         if transform_parsing_rules_vip is not None:
#                             for cell in row:
#                                 for vip in transform_parsing_rules_vip:
#                                     if cell in transform_parsing_rules_vip.get(vip):
#                                         vip_list = vip_list.append(row, ignore_index=True)
#                         row = row[column]
#                         parsing_for_byte_element_count = 0
#                         parsing_for_byte_is_in = 0
#                         for element in transform_parsing_rules_byte.get(column):
#                             element = str_from_argument_converter(element)
#                             if row == element:
#                                 parsing_for_byte_count = parsing_for_byte_count+1
#                                 parsing_for_byte_is_in = parsing_for_byte_is_in+1
#                         if parsing_for_byte_is_in > 0:
#                             parsing_for_byte_element_count = parsing_for_byte_element_count+1
#                         if parsing_for_byte_count == parsing_for_byte_element_count and parsing_for_byte_count > 0:
#                             parsing_data = parsing_data.drop(parsing_data.index[[index-1]])  # index counts from 1.
#             if transform_parsing_rules_vip is not None:
#                 parsing_data = my_beautiful_task_data_frame_merge(parsing_data, vip_list)
#         partition_path = f"{self.file_to_transform_path}"
#         test_path_mask_type_for_date(partition_path)
#         test_transform_task_time_mask(self.date_path_part)
#         day_for_landing = f"/{self.date_path_part:%Y/%m/%d}/"
#         file_mask = 'TransformTask.json'
#         my_beautiful_task_data_landing(parsing_data, day_for_landing, self.output_path_list, partition_path, file_mask)


# class LoadTask(Task):
#     """
#     Landing data.
#     """
#     task_namespace = 'LoadTask'
#     load_data_path: str = Parameter(significant=True, description='Root path for LoadTask files')
#     load_file_mask: str = Parameter(significant=True, description='File type Mask')
#     priority = 300
#     output_path_list = []
#
#     def requires(self):
#         return {'TransformTask': TransformTask()}
#
#     def output(self):
#         for path_of_flag in self.output_path_list:
#             yield LocalTarget(path.join(path_of_flag))
#
#     def run(self):
#         file_mask: str = self.load_file_mask
#         test_file_mask_arguments(file_mask)
#         result_successor = self.input()['TransformTask']  # Path inheritance from TransformTask.
#         interested_data = my_beautiful_task_universal_parser_part(result_successor, file_mask, drop_list=None)
#
#         parsing_data = None
#         for data in interested_data.values():
#             parsing_data = my_beautiful_task_data_frame_merge(parsing_data, data)
#
#         file_mask = 'LoadTask.parquet'
#         partition_path = f"{self.load_data_path}"
#         test_path_mask_type_for_date(partition_path)
#         my_beautiful_task_universal_data_landing_part(self, interested_data, partition_path, file_mask)


if __name__ == "__main__":
    """
    Generate pipeline from Luigi Tasks.
    """
    tasks_list: list = [
        ExternalData(
            external_data_path=external_data_config()['external_data_path']
        ),
        Extract(
            extract_data_path=extract_config()['extract_data_path'],
            extract_file_mask=extract_config()['extract_file_mask'],
            drop_list=extract_config()['drop_list']
                )
    ]

    pipeline_launcher(tasks_list)
