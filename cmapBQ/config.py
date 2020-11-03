import os
import sys
import yaml
from dataclasses import dataclass
import dacite

from google.cloud import bigquery

@dataclass
class TableDirectory:
    compoundinfo: str
    instinfo: str
    siginfo: str
    level3: str
    level4: str
    level5: str

@dataclass
class Configuration:
    credentials: str
    tables: TableDirectory

def _config_dir():
    PATH = os.path.expanduser('~/.cmapBQ')
    if os.path.exists(PATH):
        pass
    else:
        os.mkdir(PATH)
    return PATH

def _get_config_path():
    config_path = os.path.join(_config_dir(), 'config.txt')
    if os.path.exists(config_path):
        return config_path

def get_config():
    config_path = _get_config_path()

    with open(config_path, "r") as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

    config = dacite.from_dict(data_class=Configuration, data=cfg)

    return config

def get_bq_client():
    config = get_config()
    try:
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config.credentials
        return bigquery.Client()
    except:
        print( "GOOGLE_APPLICATION_CREDENTIALS not valid, check credentials parameter in ~/.cmapBQ/config.txt" )
        sys.exit(1)
