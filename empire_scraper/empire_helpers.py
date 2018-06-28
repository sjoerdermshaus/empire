import requests
import logging
import json
import time
import random
import pandas as pd

from logging.config import dictConfig
from logging.handlers import QueueListener
import yaml


def get_proxies(file='proxies.csv'):
    df = pd.read_csv(file, sep=';')
    proxies = [{'http': f'http://{ip}'} for ip in df['ip']]
    return proxies


def requests_get(logger, url, max_number_of_attempts=3, timeout=5, proxies=None):
    number_of_attempts = 0
    # time.sleep(random.randint(1, 5))
    while number_of_attempts < max_number_of_attempts:
        number_of_attempts += 1
        # noinspection PyBroadException
        try:
            # Get result
            if proxies is None:
                result = requests.get(url, timeout=timeout)
            else:
                random_proxy = random.choice(proxies)
                result = requests.get(url, timeout=timeout, proxies=random_proxy)

            # Inspect result
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
            time.sleep(5)
        logger.info(f'UnSuccessfulAttempt|#{number_of_attempts}|{url}')
        time.sleep(5)
    if number_of_attempts == max_number_of_attempts:
        logger.error(f'UnSuccessfulAttempt|#{number_of_attempts}|{url}')
    return -1


def print_movies(movies):
    if movies is None:
        return dict()
    movies2 = movies.copy()
    for title in movies2:
        for key in ['InfoThumbnail', 'Picture']:
            if key in movies2[title]:
                if movies2[title][key] is not None:
                    if 'File' in movies2[title][key]:
                        if movies2[title][key]['File'] is not None:
                            movies2[title][key]['File'] = str(movies2[title][key]['File'])
    print(json.dumps(movies2, sort_keys=True, indent=4))


class MyHandler(object):

    @staticmethod
    def handle(record):
        logger = logging.getLogger(record.name)
        logger.handle(record)


def listener_process(queue, stop_event):
    # config_listener = {
    #     'version': 1,
    #     'disable_existing_loggers': True,
    #     'formatters': {
    #         'detailed': {
    #             'class': 'logging.Formatter',
    #             'format': '%(asctime)-20s|%(filename)-20s|%(funcName)-40s|%(lineno)-4s|%(levelname)-7s|%(message)s',
    #             'datefmt': '%Y-%m-%d %H:%M:%S',
    #         },
    #     },
    #     'handlers': {
    #         'console': {
    #             'class': 'logging.StreamHandler',
    #             'level': 'INFO',
    #             'formatter': 'detailed',
    #             'stream': 'ext://sys.stdout',
    #         },
    #         'file': {
    #             'class': 'logging.FileHandler',
    #             'filename': 'empire_movies.log',
    #             'mode': 'w',
    #             'formatter': 'detailed',
    #         },
    #     },
    #     'loggers': {
    #         'foo': {
    #             'handlers': ['file']
    #         }
    #     },
    #     'root': {
    #         'level': 'INFO',
    #         'handlers': ['console', 'file']
    #     },
    with open('listener.yaml', 'r') as f:
        config_listener = yaml.load(f.read())
    dictConfig(config_listener)
    listener = QueueListener(queue, MyHandler())
    listener.start()
    stop_event.wait()
    listener.stop()


if __name__ == '__main__':
    print(random.choice(get_proxies()))
