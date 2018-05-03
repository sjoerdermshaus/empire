import requests
import logging
import json
import time
import random
import pandas as pd
import sys

from logging.config import dictConfig
import yaml


def get_proxies(file='proxies.csv'):
    df = pd.read_csv(file, sep=';')
    proxies = [{'http': f'http://{ip}'} for ip in df['ip']]
    return proxies


def setup_logging(file, filename=None):
    with open(file, 'r') as f:
        config = yaml.load(f.read())
        if filename is not None:
            config['handlers']['file']['filename'] = filename
            config['handlers']['file']['mode'] = 'w'
        dictConfig(config)
    logger = logging.getLogger(__name__) if filename is None else logging.getLogger(filename)
    return logger


def requests_get(logger, url, max_number_of_attempts=3, timeout=5, proxies=None):
    number_of_attempts = 0
    # time.sleep(random.randint(1, 5))
    while number_of_attempts < max_number_of_attempts:
        number_of_attempts += 1
        # noinspection PyBroadException
        try:
            if proxies is None:
                result = requests.get(url, timeout=timeout)
            else:
                random_proxy = random.choice(proxies)
                result = requests.get(url, timeout=timeout, proxies=random_proxy)
            if result.status_code == 200:
                if number_of_attempts > 1:
                    logger.info(f'SuccessfulAttempt|#{number_of_attempts}|{url}')
                return result.content
            elif result.status_code == 404:
                logger.error(f'404|#{number_of_attempts}|{url}')
                return -1
            else:
                logger.info(f'StatusCode:{result.status_code}|#{number_of_attempts}|{url}')
        except Exception as e:
            logger.info(f'{str(e)}|#{number_of_attempts}|{url}')
            time.sleep(1)
        logger.error(f'UnSuccessfulAttempt|#{number_of_attempts}|{url}')
    return -1


def print_movies(movies):
    movies2 = movies.copy()
    for title in movies2:
        for key in ['InfoThumbnail', 'Picture']:
            if key in movies2[title]:
                if movies2[title][key] is not None:
                    if 'File' in movies2[title][key]:
                        if movies2[title][key]['File'] is not None:
                            movies2[title][key]['File'] = str(movies2[title][key]['File'])
    print(json.dumps(movies2, sort_keys=True, indent=4))


def simple_logger(name=None, level=logging.INFO, propagate=True, mode='w', use_fh=True, use_ch=True):
    if name is None:
        name = 'root'
        logger = logging.getLogger()
    else:
        logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = propagate

    if use_fh is False and use_ch is False:
        return logger

    fmt = '%(asctime)-20s|%(filename)-20s|%(funcName)-40s|%(lineno)-4s|%(levelname)-7s|%(message)s'
    datefmt = '%Y-%m-%d %H:%M:%S'
    fmt = logging.Formatter(fmt=fmt, datefmt=datefmt)
    fh = logging.FileHandler(name + '.log', mode=mode)
    ch = logging.StreamHandler(sys.stdout)

    handlers =[]
    if use_fh:
        handlers.append(fh)
    if use_ch:
        handlers.append(ch)

    if len(handlers) > 0:
        for handler in handlers:
            handler.setFormatter(fmt)
            handler.setLevel(level)
            logger.addHandler(handler)

    return logger


if __name__ == '__main__':
    print(random.choice(get_proxies()))
