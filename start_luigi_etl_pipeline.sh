#!/bin/bash
#Root Path Settings for Luigi ETL Pipeline:
#Partitions inside the "external_data_path" root must be stored along the path "./YYYY/MM/DD/partition".
external_data_path="~Luigi/luigi_tasks/ExternalData"
extract_data_path="~/Luigi/luigi_tasks/ExtractTask"
file_to_transform_path="~Luigi/luigi_tasks/TransformTask"
load_data_path="~Luigi/luigi_tasks/LoadTask"
#Setting masks with file extensions at the input of tasks:
extract_file_mask=".csv"
transform_file_mask=".json"
Load_file_mask=".json"
#Setting the date for the TransformTask path (default is the start day):
date_path_part=$(date +%F)
#date_path_part=$(date +%F --date "YYYY-MM-DD)  # If you need a specific date.

#Launching the Luigi Central Scheduler:
#python3 -B -m luigi_task LoadTask.LoadTask --scheduler-host localhost \
#Running a local Luigi queue with the arguments given above:
python3 -B -m luigi_task LoadTask.LoadTask --local-scheduler \
--ExternalData.ExternalData-external-data-path $external_data_path \
--ExtractTask.ExtractTask-extract-data-path $extract_data_path \
--ExtractTask.ExtractTask-extract-file-mask $extract_file_mask \
--TransformTask.TransformTask-file-to-transform-path $file_to_transform_path \
--TransformTask.TransformTask-transform-file-mask $transform_file_mask \
--TransformTask.TransformTask-transform-parsing-rules-drop '{"key":["value", "value"]}' \
--TransformTask.TransformTask-transform-parsing-rules-byte '{"key":["value", "value"], "key":["value", "value"]}' \
--TransformTask.TransformTask-transform-parsing-rules-vip '{"key":["value", "value"]}' \
--TransformTask.TransformTask-date-path-part $date_path_part \
--LoadTask.LoadTask-load-data-path $load_data_path \
--LoadTask.LoadTask-load-file-mask $load_file_mask
#Dictionaries from the arguments above, unfortunately, cannot be passed to arguments in any way, except directly.
