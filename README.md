# Luigi ETL pipeline

## Disclaimer:
**Using** some or all of the elements of this code, **You** assume **responsibility for any consequences!**

Saved the repository as a reminder.<br/>
Requires serious improvement in code extensibility and readability.

## Description of the pipeline:
The pipeline collects data from sources, in the form of (csv tables / json dictionaries) data, so that in the end:
* Collects data from external sources to Luigi targets.
* Data cleansing.
* Land data in parquet.

## Installing the Required Packages:
```bash
pip install luigi
pip install pandas
pip install numpy
pip install pyarrow
```
## Launch:
If Your OS has a bash shell the ETL pipeline can be started using the bash script:
```bash
./start_luigi_etl_pipeline.sh
```
The script contains an example of all the necessary arguments to run.<br/>
To launch the pipeline through this script, do not forget to make it executable.
```bash
chmod +x ./start_luigi_etl_pipeline.sh
```
The script can also be run directly with python.
```bash
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
```
The example above shows the launch of all tasks.
## Description of tasks:
ExternalData:
* Wrappers for data from external sources.<br/>
**IMPORTANT!**<br/>
All paths to partitions inside the root directory of the passed ExternalData must be in the format ./YYYY/MM/DD/<br/>

ExtractTask:
* Reads data from ExternalData.
* Merges them into one array.

TransformTask:
* Remove all lines matching the transform_parsing_rules_drop parameter.
* Rows will be discarded if at least one value matches in ALL keys of transform_parsing_rules_byte.
* And provided that the string does not contain values from the transform_parsing_rules_vip keys.

LoadTask:
* Landing data.

## Tests:
Tests are embedded inside the pipeline.

Thank you for showing interest in my work.
