import configparser

CONFIG_FILE = './config.cfg'


def read_config(section):

    new_config = {}
    cf = configparser.ConfigParser()
    cf.read(CONFIG_FILE)
    configs = cf.items(section)

    for item in configs:
        key, value = item
        new_config[key] = value

    return new_config


