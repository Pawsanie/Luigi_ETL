from os import path, sep, environ

from luigi import configuration
"""
Contents configuration code for Luigi pipeline.
"""


def get_config() -> configuration.get_config:
    """
    Get configuration for all pipelines.
    """
    luigi_config_path: str = \
        f"{path.abspath(__file__).replace(path.join(*['Universal_Luigi_task', 'Get_Luigi_Config.py']), '')}" \
        f"{sep}Configuration"\
        f"{sep}luigi_config.cfg"

    environ['LUIGI_CONFIG_PATH'] = luigi_config_path
    configuration.add_config_path(luigi_config_path)

    return configuration.get_config(parser='cfg')
