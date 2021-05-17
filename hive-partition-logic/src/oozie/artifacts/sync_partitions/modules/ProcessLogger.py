import os
import json
import logging.config
from modules.envvalues import tdy
def setup_logging(
        default_path= "./resources/logging.json",
        default_level=logging.INFO,
        env_key='LOG_CFG'
):
    """Setup logging configuration
    """
    #print (CWD)
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = json.load(f)
        log_location = "./Hive_Sync_Partition_Error_{}.log".format(tdy)
        config["handlers"]["debug_file_handler"]["filename"] = log_location
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)
        logging.info("basic logging")

    return logging