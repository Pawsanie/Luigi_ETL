#!/bin/bash
#Настройки корневых путей для ETL пайплайна Luigi:
#Партиции внутри корня "external_data_path" должны хранится по пути "./YYYY/MM/DD/partition".
external_data_path="~Luigi/luigi_tasks/ExternalData"
extract_data_path="~/Luigi/luigi_tasks/ExtractTask"
file_to_transform_path="~Luigi/luigi_tasks/TransformTask"
load_data_path="~Luigi/luigi_tasks/LoadTask"
#Настройка масок с разрешением файлов, на входе тасок:
extract_file_mask=".csv"
transform_file_mask=".json"
Load_file_mask=".json"
#Настройка даты, для пути TransformTask(по умолчанию - день запуска):
date_path_part=$(date +%F)
#date_path_part='YYYY-MM-DD'.  # Если нужна конкретная дата.

#Запуск центрального планировщика Luigi:
#python3 -B -m luigi_task ExtractTask.ExtractTask --scheduler-host localhost \
#Запуск локальной очереди Luigi с аргументами заданными выше:
python3 -B -m luigi_task TransformTask.TransformTask --local-scheduler \
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
