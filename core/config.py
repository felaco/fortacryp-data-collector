import json
import os
import logging

from core.configCore import _config, RootConfig
from Buda.BudaIntegrationConfig import BudaMarketConfig
from cryptoCompare.CryptoCompareIntegrationConfig import CryptoCompareConfig

logger = logging.getLogger('BudaLogger')


def _is_valid_dict(config_dict: dict) -> bool:
    return 'crypto_compare' in config_dict and 'buda' in config_dict


def root_config_from_dict(config_dict: dict) -> RootConfig:
    if _is_valid_dict(config_dict):
        root_instance = RootConfig.get_instanciator()

        buda = BudaMarketConfig.from_dict(config_dict['buda'])
        buda.root_config = root_instance

        crypto_compare = CryptoCompareConfig.from_dict(config_dict['crypto_compare'])
        crypto_compare.root_config = root_instance

        root_instance.buda = buda
        root_instance.crypto_compare = crypto_compare

        return root_instance
    else:
        raise TypeError('Cannot reconstruct Root config from config file')


def load_config(persist_on_create: bool = False) -> RootConfig:
    """
    Crates a new root config based on a json config, if it exists or create a default one if not
    :param persist_on_create:
    :return:
    """
    dir_path = os.path.dirname(os.path.realpath(__file__))  # current project path.
    dir_path = os.path.dirname(dir_path)  # parent folder
    # useful for executing this script using cron and storing the config in the project folder,
    # not the user folder
    abs_path = os.path.join(dir_path, 'config.json')
    if os.path.isfile(abs_path):
        try:
            with open(abs_path) as file:
                r = file.read()
                json_dict = json.loads(r)
                return root_config_from_dict(json_dict)

        except (TypeError, json.JSONDecodeError):
            logger.warning('An error ocurred while deserializing the config file.'
                           ' Using the default one instead')
            root_config = root_config_from_dict(_config)
            root_config.persist()
            return root_config

    else:
        root_config = root_config_from_dict(_config)
        if persist_on_create:
            root_config.persist()
        return root_config


config = load_config()  # config variable stores the stored config or the default one.
# remember this line is executed as soon as the first import ocurs, and happens once i think ... i hope so
