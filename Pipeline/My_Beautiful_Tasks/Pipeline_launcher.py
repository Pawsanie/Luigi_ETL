import logging
from traceback import format_exc

from luigi import build

from .Tests.Logging_Config import logging_config
"""
Contents ETL launch code.
"""


def pipeline_launcher(tasks_list: list):
    """
    Launch ETL pipeline.

    :param tasks_list: List with Luigi tasks classes and their parameters.
    :type tasks_list: list[Task(luigi_parameter=value)]
    """
    logging_config(
        log_path="logg_file.txt",
        log_level=40
    )
    try:
        build(
            tasks=tasks_list,
            local_scheduler=True,
            detailed_summary=True,
            log_level="WARNING",
            workers=1
        )
    except Exception as error:
        logging.critical(
            f"{'=' * 30}\n"
            f"Program launch raise: '{repr(error)}'"
            f"\n{'-' * 30}"
            f"\n{format_exc()}"
            f"\n{'=' * 30}\n\n"
        )
