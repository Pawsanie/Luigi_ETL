from luigi import run, Task, LocalTarget, ExternalTask, ListParameter, DateParameter, Parameter, DictParameter
from os import walk, path
from pandas import DataFrame
from datetime import date
from my_beautiful_task import my_beautiful_task_data_landing, my_beautiful_task_data_frame_merge, \
    nan_pandas_df_converter, str_from_argument_converter, my_beautiful_task_universal_parser_part, \
    my_beautiful_task_universal_data_landing_part
from tests_my_beautiful_task import test_path_mask_type_for_date, test_external_task_path, \
    test_transform_task_time_mask, test_file_mask_arguments


class ExternalData(ExternalTask):
    """Обёртки для данных из внешних источников"""
    task_namespace = 'ExternalData'
    external_data_path = Parameter(significant=True, description='Root path for ExternalData files')
    priority = 200
    external_data_result = []

    def output(self):
        for path_of_flag in self.external_data_result:
            yield LocalTarget(path.join(path_of_flag))

    def run(self):
        partition_path = f"{self.external_data_path}"
        test_path_mask_type_for_date(partition_path)
        test_external_task_path(partition_path)
        for dirs, folder, files in walk(partition_path):
            interest_partition_path = f'{dirs}/{"_Validate"}'
            if path.isfile(interest_partition_path):
                partition_dir = str.replace(dirs, partition_path, '')
                path_of_partition = str.replace(partition_path, partition_dir, '')
                flag_path = f'{path_of_partition}{partition_dir}/{"_Validate_Success"}'
                self.external_data_result.append(flag_path)
                flag_file = open(flag_path, "w")
                flag_file.close()
        return self.external_data_result


"""
Очередь собирает из источников, в виде (csv таблиц/json словарей) данные, чтобы в итоге:
1) Удалить все строки сответсвующие параметру transform_parsing_rules_drop.
2) Удалить часть строк соответсвующую transform_parsing_rules_byte и transform_parsing_rules_vip.
3) Приземлить данные в parquet.
"""


class ExtractTask(Task):
    """
    Извлечение данных из источников в виде csv таблиц.
    Объединение в один массив.
    """
    task_namespace = 'ExtractTask'
    extract_data_path = Parameter(significant=True, description='Root path for ExtractTask files')
    priority = 100
    extract_file_mask = Parameter(significant=True, description='File type Mask')
    drop_list = ListParameter(significant=False,
                              default=None)  # Лишние колонки, которые нужно будет дропнуть, в таблицах.
    output_path_list = []

    def requires(self):
        return {'ExternalData': ExternalData()}

    def output(self):
        for path_of_flag in self.output_path_list:
            yield LocalTarget(path.join(path_of_flag))

    def run(self):
        partition_path = f"{self.extract_data_path}"
        test_path_mask_type_for_date(partition_path)
        file_mask = self.extract_file_mask
        test_file_mask_arguments(file_mask)
        result_successor = self.input()['ExternalData']  # Наследование путей от ExternalTask.
        drop_list = self.drop_list
        interested_data = my_beautiful_task_universal_parser_part(result_successor, file_mask, drop_list)

        file_mask = "ExtractTask_result.json"
        my_beautiful_task_universal_data_landing_part(self, interested_data, partition_path, file_mask)


class TransformTask(Task):
    """Очистка данных"""
    task_namespace = 'TransformTask'
    file_to_transform_path = Parameter(significant=True, description='Root path for ExtractTask files')
    transform_file_mask = Parameter(significant=True, description='File type Mask')
    priority = 100
    output_path_list = []
    date_path_part = DateParameter(default=date.today())
    transform_parsing_rules_drop = DictParameter(significant=False, default=None,
                                                 description='Json obj. with parsing rules (must be droped)')
    transform_parsing_rules_byte = DictParameter(significant=False, default=None,
                                                 description='Json obj. with parsing rules (analise and drope)')
    transform_parsing_rules_vip = DictParameter(significant=False, default=None,
                                                description='Json obj. with parsing rules (Interesting data)')

    def requires(self):
        return {'ExtractTask': ExtractTask()}

    def output(self):
        for path_of_flag in self.output_path_list:
            yield LocalTarget(path.join(path_of_flag))

    def run(self):
        file_mask = self.transform_file_mask
        test_file_mask_arguments(file_mask)
        result_successor = self.input()['ExtractTask']  # Наследование путей от ExtractTask.
        interested_data = my_beautiful_task_universal_parser_part(result_successor, file_mask, drop_list=None)

        parsing_data = None
        for data in interested_data.values():
            parsing_data = my_beautiful_task_data_frame_merge(parsing_data, data)
        """Все элементы в transform_parsing_rules_drop будут отсеяны:"""
        transform_parsing_rules_drop = self.transform_parsing_rules_drop
        if transform_parsing_rules_drop is not None:
            for element in transform_parsing_rules_drop.keys():
                rule = transform_parsing_rules_drop.get(element)
                rule = nan_pandas_df_converter(rule)
                rules_drop = parsing_data[parsing_data[element].isin(rule)]
                rules_drop = parsing_data[~parsing_data.index.isin(rules_drop.index)]
                parsing_data = rules_drop
        """
        Будут отсеяны строки, при совпадении хотя бы одного значения во ВСЕХ ключах transform_parsing_rules_byte.
        И при условии что срока не сдержит значений из ключей transform_parsing_rules_vip.
        """
        transform_parsing_rules_byte = self.transform_parsing_rules_byte
        transform_parsing_rules_vip = self.transform_parsing_rules_vip
        vip_list = DataFrame()
        parsing_for_byte_count = 0
        if transform_parsing_rules_byte is not None:
            for column in parsing_data:
                if column in transform_parsing_rules_byte:
                    for index, row in parsing_data.iterrows():
                        if transform_parsing_rules_vip is not None:
                            for cell in row:
                                for vip in transform_parsing_rules_vip:
                                    if cell in transform_parsing_rules_vip.get(vip):
                                        vip_list = vip_list.append(row, ignore_index=True)
                        row = row[column]
                        parsing_for_byte_element_count = 0
                        parsing_for_byte_is_in = 0
                        for element in transform_parsing_rules_byte.get(column):
                            element = str_from_argument_converter(element)
                            if row == element:
                                parsing_for_byte_count = parsing_for_byte_count+1
                                parsing_for_byte_is_in = parsing_for_byte_is_in+1
                        if parsing_for_byte_is_in > 0:
                            parsing_for_byte_element_count = parsing_for_byte_element_count+1
                        if parsing_for_byte_count == parsing_for_byte_element_count and parsing_for_byte_count > 0:
                            parsing_data = parsing_data.drop(parsing_data.index[[index-1]])  # index отсчитывает с 1
            if transform_parsing_rules_vip is not None:
                parsing_data = my_beautiful_task_data_frame_merge(parsing_data, vip_list)
        partition_path = f"{self.file_to_transform_path}"
        test_path_mask_type_for_date(partition_path)
        test_transform_task_time_mask(self.date_path_part)
        day_for_landing = f"/{self.date_path_part:%Y/%m/%d}/"
        file_mask = 'TransformTask.json'
        my_beautiful_task_data_landing(parsing_data, day_for_landing, self.output_path_list, partition_path, file_mask)


class LoadTask(Task):
    """Загрузка данных"""
    task_namespace = 'LoadTask'
    load_data_path = Parameter(significant=True, description='Root path for LoadTask files')
    load_file_mask = Parameter(significant=True, description='File type Mask')
    priority = 300
    output_path_list = []

    def requires(self):
        return {'TransformTask': TransformTask()}

    def output(self):
        for path_of_flag in self.output_path_list:
            yield LocalTarget(path.join(path_of_flag))

    def run(self):
        file_mask = self.load_file_mask
        test_file_mask_arguments(file_mask)
        result_successor = self.input()['TransformTask']  # Наследование путей от TransformTask.
        interested_data = my_beautiful_task_universal_parser_part(result_successor, file_mask, drop_list=None)

        parsing_data = None
        for data in interested_data.values():
            parsing_data = my_beautiful_task_data_frame_merge(parsing_data, data)

        file_mask = 'LoadTask.parquet'
        partition_path = f"{self.load_data_path}"
        test_path_mask_type_for_date(partition_path)
        my_beautiful_task_universal_data_landing_part(self, interested_data, partition_path, file_mask)


if __name__ == "__main__":
    run()
