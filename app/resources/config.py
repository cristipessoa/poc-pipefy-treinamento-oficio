import json
import configparser
from pathlib import Path

def load_config_ini():
    script_location = Path(__file__).absolute().parent
    file_location = script_location / 'config.ini'
    config = configparser.ConfigParser()                                     
    config.read(file_location)    
    return config