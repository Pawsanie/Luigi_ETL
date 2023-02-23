#!/bin/bash
#Root Path Settings for Luigi ETL Pipeline:
#Partitions inside the "external_data_path" root must be stored along the path "./YYYY/MM/DD/partition".
external_data_path="~Luigi/luigi_tasks/ExternalData"
external_data_file_mask="csv"
extract_data_path="~/Luigi/luigi_tasks/ExtractTask"
extract_data_drop_list="['drop_name', 'Delete']"
file_to_transform_path="~Luigi/luigi_tasks/TransformTask"
load_data_path="~Luigi/luigi_tasks/LoadTask"
#Setting masks with file extensions at the input of tasks:
extract_file_mask=".csv"
transform_file_mask=".json"
Load_file_mask=".json"
#Setting the date for the TransformTask path (default is the start day):
date_path_part=$(date +%F)
#date_path_part=$(date +%F --date "YYYY-MM-DD")  # If you need a specific date.

#Launching the Luigi Central Scheduler:
#python3 -B -m luigi_pipeline LoadTask.LoadTask --scheduler-host localhost \
#Running a local Luigi queue with the arguments given above:
python3 -B -m luigi_pipeline LoadTask.LoadTask --local-scheduler \
--ExternalData.ExternalData-external-data-path $external_data_path \
\
--Extract.Extract-extract-data-path $extract_data_path \
--Extract.Extract-extract-file-mask $extract_file_mask \
--Extract.Extract-external-data-file-mask $external_data_file_mask \
--Extract.Extract-drop-list $extract_data_drop_list \
\
--Transform.Transform-file-to-transform-path $file_to_transform_path \
--Transform.Transform-transform-file-mask $transform_file_mask \
--Transform.Transform-transform-parsing-rules-drop "{'column_to_drop': [False, 'NaN', 0]}" \
--Transform.Transform-transform-parsing-rules-filter "{'column_to_filter': ['drop_if_not_in_vip', 'drop_too'], 'filter_too': ['0']}" \
--Transform.Transform-transform-parsing-rules-vip "{'data_to_save_like_vip': ['vip_value_1, vip_value_2'], 'save_too': ['vip_value_3']}" \
--Transform.Transform-date-path-part $date_path_part \
\
--Load.Load-load-data-path $load_data_path \
--Load.Load-load-file-mask $load_file_mask
#Dictionaries from the arguments above, unfortunately, cannot be passed to arguments in any way, except directly.
