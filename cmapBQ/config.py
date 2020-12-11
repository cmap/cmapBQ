import os
import sys
import yaml
from dataclasses import dataclass
import dacite
from google.cloud import bigquery
from google.auth.exceptions import DefaultCredentialsError


@dataclass
class TableDirectory:
    compoundinfo: str
    genetic_pertinfo: str
    geneinfo: str
    cellinfo: str
    instinfo: str
    siginfo: str
    level3: str
    level4: str
    level5: str

    def __str__(self):
        tables = ["{}: {}".format(attr, getattr(self, attr)) for attr in dir(self) if not attr.startswith('__')]
        return "\n".join(tables)


@dataclass
class Configuration:
    """
    Data class for configuration of cmapBQ. Object for config.txt
    """
    credentials: str
    tables: TableDirectory

def _write_default_config(path):
    default_config = {
        "credentials": "PATH TO CREDENTIALS",
        "tables": {
            "level3": "cmap-big-table.cmap_lincs_public_views.L1000_Level3",
            "level4": "cmap-big-table.cmap_lincs_public_views.L1000_Level4",
            "level5": "cmap-big-table.cmap_lincs_public_views.L1000_Level5",
            "siginfo": "cmap-big-table.cmap_lincs_public_views.siginfo",
            "instinfo": "cmap-big-table.cmap_lincs_public_views.instinfo",
            "compoundinfo": "cmap-big-table.cmap_lincs_public_views.compoundinfo",
            "geneinfo": "cmap-big-table.cmap_lincs_public_views.geneinfo",
            "genetic_pertinfo": "cmap-big-table.cmap_lincs_public_views.genetic_pertinfo",
            "cellinfo": "cmap-big-table.cmap_lincs_public_views.cellinfo",
        },
    }

    with open(path, "w") as fh:
        yaml.dump(default_config, fh)
    return path


def setup_credentials(path_to_credentials):
    """
    Setup script for pointing config.txt to a GOOGLE_APPLICATION_CREDENTIALS JSON key.
    Writes default tables if ~/.cmapBQ/config.txt does not exist.

    :param path_to_credentials:
    :return: None (side effect)
    """
    config_path = _get_config_path()
    if not os.path.exists(config_path):
        _write_default_config(config_path)

    with open(config_path, "r") as ymlfile:
        cfg = yaml.safe_load(ymlfile)

    cfg["credentials"] = path_to_credentials

    with open(config_path, "w") as fh:
        yaml.dump(cfg, fh)
    return


def _config_dir():
    PATH = os.path.expanduser("~/.cmapBQ")
    if os.path.exists(PATH):
        pass
    else:
        os.mkdir(PATH)
    return PATH


def _get_config_path():
    config_path = os.path.join(_config_dir(), "config.txt")
    if os.path.exists(config_path):
        return config_path
    else:
        return config_path


def get_default_config():
    """
    Get configuration object from reading ~/.cmapBQ/config.txt

    :return: cmapBQ.config.Configuration class.
    """
    config_path = _get_config_path()

    try:
        return _load_config(config_path)
    except FileNotFoundError as f:
        print(
            "Credentials file not found in: ~/.cmapBQ/config.txt. Check that file exists. Alternatively, run " +
            "setup_credentials(path_to_google_credentials)"
        )

        sys_exit = sys.exit(1)


def _load_config(config_path):
    """
    Read in config file
    :param config_path: path to YAML config file
    :return: Configuration Dataclass
    """
    with open(config_path, "r") as ymlfile:
        cfg = yaml.safe_load(ymlfile)

    config = dacite.from_dict(data_class=Configuration, data=cfg)
    return config


def set_default_config(input_config_path):
    """
    Change configuration in ~/.cmapBQ to input config path. Overwrites ~/.cmapBQ/config.txt.,

    :param input_config_path: valid YAML formatted config file
    :return: location in ~/.cmapBQ
    """
    config_location = _get_config_path()

    with open(input_config_path, "r") as ymlfile:
        cfg = yaml.safe_load(ymlfile)

    with open(config_location, "w") as fh:
        yaml.dump(cfg, fh)

    return config_location


def get_bq_client(config=None):
    """
    Return authenticated BigQuery client object.

    :param config: optional path to config if not default
    :return: BigQuery Client
    """
    if config is None:
        config = get_default_config()
    else:
        config = _load_config(config)

    try:
        # Will automatically try to get credentials from environment
        return bigquery.Client()
    except DefaultCredentialsError:
        try:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config.credentials
            return bigquery.Client()
        except DefaultCredentialsError:
            print(
                "GOOGLE_APPLICATION_CREDENTIALS not valid, check credentials parameter in ~/.cmapBQ/config.txt"
            )
            sys.exit(1)
    except:
        print(
            "GOOGLE_APPLICATION_CREDENTIALS not valid, check credentials parameter in ~/.cmapBQ/config.txt"
        )
        sys.exit(1)
