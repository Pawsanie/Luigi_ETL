from My_Beautiful_Tasks.Pipeline_launcher import pipeline_launcher
from My_Beautiful_Tasks.External_Data_task import ExternalDataTask, external_data_config
from My_Beautiful_Tasks.Extract_Task import ExtractTask, extract_config
from My_Beautiful_Tasks.Transform_Task import TransformTask, transform_config
from My_Beautiful_Tasks.Load_Task import LoadTask, load_config
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
    # Task settings:
    task_namespace = 'ExtractTask'
    priority = 100

    def requires(self):
        return {'ExternalData': ExternalData(
            external_data_path=external_data_config()['external_data_path']
        )}


class Transform(TransformTask):
    """
    Data cleansing.
    """
    # Task settings:
    task_namespace = 'TransformTask'
    priority = 100

    def requires(self):
        return {'Extract': Extract(
            extract_data_path=extract_config()['extract_data_path'],
            extract_file_mask=extract_config()['extract_file_mask'],
            external_data_file_mask=extract_config()['external_data_file_mask'],
            drop_list=extract_config()['drop_list']
                )}


class Load(LoadTask):
    """
    Landing data.
    """
    # Task settings:
    task_namespace = 'LoadTask'
    priority = 300

    def requires(self):
        return {'Transform': Transform(
            file_to_transform_path=transform_config()['file_to_transform_path'],
            transform_file_mask=transform_config()['transform_file_mask'],
            extract_file_mask=transform_config()['extract_file_mask'],
            transform_parsing_rules_drop=transform_config()['transform_parsing_rules_drop'],
            transform_parsing_rules_byte=transform_config()['transform_parsing_rules_byte'],
            transform_parsing_rules_vip=transform_config()['transform_parsing_rules_vip'],
        )}


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
            external_data_file_mask=extract_config()['external_data_file_mask'],
            drop_list=extract_config()['drop_list']
                ),

        Transform(
            file_to_transform_path=transform_config()['file_to_transform_path'],
            transform_file_mask=transform_config()['transform_file_mask'],
            extract_file_mask=transform_config()['extract_file_mask'],
            transform_parsing_rules_drop=transform_config()['transform_parsing_rules_drop'],
            transform_parsing_rules_byte=transform_config()['transform_parsing_rules_byte'],
            transform_parsing_rules_vip=transform_config()['transform_parsing_rules_vip'],
        ),

        # Load(
        #     load_data_path=load_config()['load_data_path'],
        #     load_file_mask=load_config()['load_file_mask']
        # )

    ]

    pipeline_launcher(tasks_list)
