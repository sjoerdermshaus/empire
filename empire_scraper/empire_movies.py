from bs4 import BeautifulSoup
import pandas as pd
from PIL import Image
import requests
from io import BytesIO
import pickle
from empire_scraper.empire_movie import EmpireMovie
from empire_scraper.empire_helpers import get_proxies, print_movies
from multiprocessing import Pool, Event
from empire_scraper.empire_helpers import requests_get, listener_process
from datetime import datetime as dt
import os
from datetime import datetime

import logging
import multiprocessing

from logging.config import dictConfig
import yaml


class EmpireMovies(object):
    def __init__(self, process_images=True, number_of_processors=1, use_proxies=True):
        self.process_images = process_images
        self.movies = dict()
        self.parser = "lxml"
        self.df = None
        self.number_of_processors = number_of_processors
        self.proxies = None
        if use_proxies:
            self.proxies = get_proxies(file='proxies.csv')
        self.pages = None
        self.log_file = 'empire_movies.log'
        self.pickle_file = None
        self.result_file = None
        self.error_file = None
        self.now = datetime.strftime(datetime.now(), "%Y%m%d-%H%M%S")
        if not os.path.exists('thumbnails'):
            os.makedirs('thumbnails')
        if not os.path.exists('pictures'):
            os.makedirs('pictures')
        if not os.path.exists(os.path.join('results', self.now)):
            os.makedirs(os.path.join('results', self.now))

    @staticmethod
    def __get_title_from_article(article):
        """
        Find the title of the movie and derive whether it is an essay.
        :param article: article about the movie in BeautifulSoup format
        :return: title (string) and a boolean, which indicates whether the movie is an essay
        """
        title, is_essay = None, None
        result = article.find('p', class_='hdr no-marg gamma txt--black pad__top--half')
        if result is not None:
            title = result.text.strip()
            is_essay = True if title.startswith('EMPIRE ESSAY') else False
        return title, is_essay

    @staticmethod
    def __get_review_url_from_article(article):
        """
        Find the URL of the review page of the movie.
        :param article: article about the movie in BeautifulSoup format
        :return: URL of the review page (string)
        """
        review_url = None
        result = article.find('a')
        if result is not None:
            review_url = f"https://www.empireonline.com{result['href'].strip()}"
        return review_url

    @staticmethod
    def __get_rating_from_article(article):
        rating = None
        result = article.find("span", class_="stars--on")
        if result is not None:
            rating = len(result.text.strip())
        return rating

    @staticmethod
    def __get_thumbnail_from_article(article):
        thumbnail = None
        result = article.find('img')
        if result is not None:
            thumbnail = dict()
            thumbnail['Source'] = None
            thumbnail['File'] = None
            src = result['src']
            if src is not None:
                thumbnail['Source'] = src
                if src.find('no-photo') == -1:
                    response = requests.get(src)
                    if response.status_code == 200:
                        thumbnail['File'] = Image.open(BytesIO(response.content))
                        out_file = os.path.join('thumbnails', src.split('/')[-1])
                        with open(out_file, 'wb') as f:
                            f.write(response.content)
        return thumbnail

    def __get_info_from_article(self, article):
        info = dict()
        info['InfoMovie'], info['IsEssay'] = self.__get_title_from_article(article)
        info['InfoReviewUrl'] = self.__get_review_url_from_article(article)
        info['InfoRating'] = self.__get_rating_from_article(article)
        info['InfoThumbnail'] = self.__get_thumbnail_from_article(article)
        return info

    def get_movies_for_page(self, queue, page, article_number=None):

        config_worker = {
            'version': 1,
            'disable_existing_loggers': True,
            'handlers': {
                'queue': {
                    'class': 'logging.handlers.QueueHandler',
                    'queue': queue,
                },
            },
            'root': {
                'level': 'INFO',
                'handlers': ['queue']
            },
        }
        dictConfig(config_worker)

        local_logger = logging.getLogger(f'sub_logger{page}')

        info_url = f"https://www.empireonline.com/movies/reviews/{page}/"
        local_logger.info(f'GetReviewPage|{page}|{info_url}')

        html = requests_get(local_logger, info_url, max_number_of_attempts=3, timeout=5, proxies=self.proxies)
        if html == -1:
            local_logger.error(f'RequestFailed|{page}|{info_url}')
            return -1
        else:
            soup = BeautifulSoup(html, self.parser)

        # Each movie is represented by an article
        articles = soup.find_all("article")
        if len(articles) == 0:
            local_logger.info(f'NonexistentPage|{page}|{info_url}')
            return -1

        # Loop over all articles
        movies = dict()
        for i, article in enumerate(articles, 1):
            if article_number is None or i == article_number:
                info_id = f'{page:03d}-{i:02d}'
                info = dict()
                info[info_id] = dict()
                # Process meta data
                info[info_id]['InfoPage'] = page
                info[info_id]['InfoArticle'] = i
                info[info_id]['InfoUrl'] = info_url
                info[info_id].update(self.__get_info_from_article(article))

                E = EmpireMovie(local_logger, info, self.process_images)
                new_movie = E.get_movie()
                movies.update(new_movie)

        return movies

    def get_movies_for_pages(self, pages=None, article_number=None):

        logger = logging.getLogger('root')
        logger.info(f'Start scraping||')

        # Start the listener process for multi-processing logging
        manager = multiprocessing.Manager()
        queue = manager.Queue()
        stop_event = Event()
        listener = multiprocessing.Process(target=listener_process,
                                           name='listener',
                                           args=(queue, stop_event))
        listener.start()

        # Organize the pages and article numbers as input list for pool.starmap
        if isinstance(pages, int):
            pages = [pages]
        elif not isinstance(pages, list):
            pages = list(pages)
        else:
            pass
        self.pages = pages
        x = [(queue, page, article_number) for page in pages]

        processes = 1 if article_number is not None else self.number_of_processors

        # Start multi-processing all pages
        start = dt.now()
        with Pool(processes=processes) as pool:
            results = pool.starmap(self.get_movies_for_page, iterable=iter(x), chunksize=1)
        end = dt.now()

        movies = dict()
        [movies.update(result) for result in results if result != -1]

        # Stop the listener
        stop_event.set()
        listener.join()

        scraping_time = str(end - start).split('.')[0]
        logger.info(f'Scraping time for {len(x)} pages: {scraping_time}||')
        return movies

    def __save_to_pickle(self):
        logger = logging.getLogger('root')
        logger.info('Pickling movies||')
        self.pickle_file = os.path.join('results', self.now, f'{self.now}_empire_movies.pickle')
        with open(self.pickle_file, 'wb') as f:
            pickle.dump(self, f, protocol=pickle.HIGHEST_PROTOCOL)

    def __save_to_excel(self):
        logger = logging.getLogger('root')
        logger.info('Saving movies in Excel||')
        self.result_file = os.path.join('results', self.now, f'{self.now}_empire_movies.xlsx')
        labels = list({'InfoThumbnail', 'Picture', 'Introduction', 'Review'} & set(self.df.columns))
        self.df.drop(labels=labels, axis=1, inplace=True)
        with open(self.result_file, 'wb') as f:
            self.df.to_excel(f, index=True)

    @staticmethod
    def load_from_pickle(file):
        with open(file, 'rb') as f:
            return pickle.load(f)

    def get_df(self):
        return self.df

    @staticmethod
    def line_splitter(line):
        return [i.strip() for i in line.split('|')]

    @staticmethod
    def analyze_error_message(message):
        solvable_error = True
        if message[2].split('/')[4].startswith('55'):
            solvable_error = False
        elif message[2].count('/') > 6:
            solvable_error = False
        elif message[0] == '404':
            solvable_error = False
        else:
            pass
        return solvable_error

    def get_solvable_movies_from_log_file(self):
        logger = logging.getLogger('root')
        logger.info(f'Get solvable movies from log file||')
        columns = ['asctime',
                   'filename',
                   'funcName',
                   'lineno',
                   'levelname',
                   'message0',
                   'message1',
                   'message2']

        with open(self.log_file, 'r') as f:
            lines = f.readlines()
        lines = [self.line_splitter(line) for line in lines]
        df_log_file = pd.DataFrame(data=lines, columns=columns)

        df_error = df_log_file.query('levelname == "ERROR"')
        if len(df_error) == 0:
            logger.info(f'No errors found||')
            return None

        solvable_error = [EmpireMovies.analyze_error_message(message)
                          for message in df_error[['message0', 'message1', 'message2']].values]
        df_error = df_error.assign(SolvableError=solvable_error)

        logger.info(f'Saving errors to Excel||')
        self.error_file = os.path.join('results', self.now, f'{self.now}_empire_movies_errors.xlsx')
        df_error.to_excel(self.error_file, index=True)

        list_of_solvable_movies = list(df_error.query('SolvableError == True')['message2'].unique())
        if len(list_of_solvable_movies) == 0:
            logger.info(f'No movies to be solved||')
            return None

        solvable_movies = dict()
        [solvable_movies.update({key: value}) for key, value in self.movies.items() if key in list_of_solvable_movies]
        return solvable_movies

    def solve_movies(self):
        logger = logging.getLogger('root')

        solvable_movies = self.get_solvable_movies_from_log_file()
        if solvable_movies is None:
            logger.info(f'No movies to be solved||')
            return None

        solved_movies = dict()
        for key, value in solvable_movies.items():
            logger.info(f'GetReviewSolvableMovie|{key}|{value["InfoReviewUrl"]}')
            solved_movie = self.get_movies_for_pages(value["InfoPage"], value["InfoArticle"])
            if solved_movie is not None:
                if solved_movie[key]["InfoMovie"] != value["InfoMovie"]:
                    logger.error(f'IDMismatch|{solved_movie[key]["InfoMovie"]}|{value["InfoMovie"]}')
                    return None
                else:
                    solved_movies.update(solved_movie)

        return solved_movies

    def get_movies(self, pages=None, article_number=None):
        movies = self.get_movies_for_pages(pages, article_number)
        self.movies = movies
        solved_movies = self.solve_movies()
        if solved_movies is not None:
            self.movies.update(solved_movies)
        self.df = pd.DataFrame.from_dict(self.movies, orient='index')
        self.df.index.name = 'ID'
        self.__save_to_pickle()
        self.__save_to_excel()
        return solved_movies


def test_pages(pages, number_of_processors=2):
    E = EmpireMovies(process_images=True, number_of_processors=number_of_processors)
    print_movies(E.get_movies(pages))


if __name__ == '__main__':
    # config_initial = {
    #     'version': 1,
    #     'disable_existing_loggers': False,
    #     'formatters': {
    #         'detailed': {
    #             'class': 'logging.Formatter',
    #             'format': '%(asctime)-20s|%(filename)-20s|%(funcName)-40s|%(lineno)-4s|%(levelname)-7s|%(message)s',
    #             'datefmt': '%Y-%m-%d %H:%M:%S',
    #         }
    #     },
    #     'handlers': {
    #         'console': {
    #             'class': 'logging.StreamHandler',
    #             'level': 'INFO',
    #             'formatter': 'detailed',
    #             'stream': 'ext://sys.stdout'
    #         },
    #         'file': {
    #             'class': 'logging.FileHandler',
    #             'filename': 'root.log',
    #             'mode': 'w',
    #             'formatter': 'detailed'
    #         }
    #     },
    #     'root': {
    #         'level': 'INFO',
    #         'handlers': ['console', 'file']
    #     },
    # }
    with open('root.yaml', 'r') as fh:
        config_root = yaml.load(fh.read())
    dictConfig(config_root)
    root_logger = logging.getLogger('root')
    test_pages(range(1, 501), 8)
