from bs4 import BeautifulSoup
import pandas as pd
from PIL import Image
import requests
from io import BytesIO
import pickle
from empire_scraper.src.empire_movie import EmpireMovie
from empire_scraper.src.empire_helpers import get_proxies
from multiprocessing import Pool, Event
from empire_scraper.src.empire_helpers import requests_get, listener_process
from datetime import datetime as dt
import os
from datetime import datetime

import logging
import multiprocessing

from logging.config import dictConfig
import shutil

from itertools import zip_longest


logging.getLogger('root')


class EmpireMovies(object):
    def __init__(self, process_images=True, number_of_processors=1, use_proxies=True, max_number_of_attempts=5):
        self.process_images = process_images
        self.movies = dict()
        self.parser = "lxml"
        self.df = None
        self.number_of_processors = number_of_processors
        self.proxies = None
        self.use_proxies = use_proxies
        self.proxies = get_proxies(file='proxies.csv') if self.use_proxies else None
        self.max_number_of_attempts = max_number_of_attempts
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

        self.number_of_pages = 0
        self.number_of_articles = 0

        self.queue = None

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
        """
        Find the rating of the movie.
        :param article: article about the movie in BeautifulSoup format
        :return: rating of the movie (integer)
        """
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

    def get_movies_for_page(self, page, article_number=None, queue=None):

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

        html = requests_get(local_logger,
                            info_url,
                            max_number_of_attempts=self.max_number_of_attempts,
                            timeout=5,
                            proxies=self.proxies)
        if html == -1:
            local_logger.error(f'RequestFailed|{page}|{info_url}')
            return None
        else:
            soup = BeautifulSoup(html, self.parser)

        # Each movie is represented by an article
        articles = soup.find_all("article")
        if len(articles) == 0:
            local_logger.info(f'NonexistentPage|{page}|{info_url}')
            return None

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

                new_movie = EmpireMovie(logger=local_logger,
                                        info=info,
                                        process_images=self.process_images,
                                        use_proxies=self.use_proxies,
                                        max_number_of_attempts=self.max_number_of_attempts).get_movie()
                movies.update(new_movie)

        return movies

    def get_movies_for_pages(self, pages=None, article_numbers=None):

        logger = logging.getLogger('root')
        logger.info(f'Start scraping||')

        # Organize the pages and article numbers as input list for pool.starmap
        self.pages = pages
        if article_numbers is None:
            article_numbers = [None]

        # Start (multi-)processing all pages
        start = dt.now()

        iterable = ((page, article_number, self.queue) for page, article_number in zip_longest(pages, article_numbers))
        processes = self.number_of_processors
        with Pool(processes=processes) as pool:
            results = pool.starmap(self.get_movies_for_page, iterable=iterable, chunksize=1)

        end = dt.now()

        movies = dict()
        [movies.update(result) for result in results if result is not None]

        scraping_time = str(end - start).split('.')[0]
        logger.info(f'Scraping time for {len(pages)} pages: {scraping_time}||')

        return movies

    def save_to_pickle(self):
        logger = logging.getLogger('root')
        logger.info('Pickling movies||')
        self.pickle_file = os.path.join('results', self.now, f'{self.now}_empire_movies.pickle')
        with open(self.pickle_file, 'wb') as f:
            pickle.dump(self, f, protocol=pickle.HIGHEST_PROTOCOL)

    def save_to_excel(self):
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
    def __line_splitter(line):
        return [i.strip() for i in line.split('|')]

    @staticmethod
    def __analyze_error_message(message):
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

    def analyze_log_file(self):
        logger = logging.getLogger('root')
        logger.info(f'Analyzing log file||')
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
        lines = [self.__line_splitter(line) for line in lines]
        df_log_file = pd.DataFrame(data=lines, columns=columns)

        df_error = df_log_file.query('levelname == "ERROR"')
        if len(df_error) == 0:
            logger.info(f'No errors found in log file||')
            return {}, {}

        logger.info(f'Errors found in log file||')

        solvable_error = [EmpireMovies.__analyze_error_message(message)
                          for message in df_error[['message0', 'message1', 'message2']].values]
        df_error = df_error.assign(SolvableError=solvable_error)

        logger.info(f'Saving errors to Excel||')
        self.error_file = os.path.join('results', self.now, f'{self.now}_empire_movies_errors.xlsx')
        df_error.to_excel(self.error_file, index=True)

        query = 'SolvableError == True and message0 == "RequestsGetFailed"'
        list_of_solvable_movies = list(df_error.query(query)['message1'].unique())
        if len(list_of_solvable_movies) == 0:
            logger.info(f'Errors found in log file but no movies to be solved||')
        else:
            logger.info(f'Errors found in log file and movies to be solved||')

        query = 'message0 == "RequestsGetFailed"'
        list_of_movies = list(df_error.query(query)['message1'].unique())
        list_of_non_solvable_movies = list(set(list_of_movies) - set(list_of_solvable_movies))

        solvable_movies = dict()
        non_solvable_movies = dict()
        [solvable_movies.update({key: value}) for key, value in self.movies.items() if key in list_of_solvable_movies]
        [non_solvable_movies.update({key: value}) for key, value in self.movies.items() if
         key in list_of_non_solvable_movies]
        return solvable_movies, non_solvable_movies

    def solve_movies(self):
        logger = logging.getLogger('root')

        solvable_movies, non_solvable_movies = self.analyze_log_file()
        if solvable_movies == {}:
            logger.info(f'No solvable movies found||')
            return {}, non_solvable_movies

        logger.info(f'Scraping solvable movies||')

        solved_movies = dict()
        for key, value in solvable_movies.items():
            logger.info(f'GetReviewSolvableMovie|{key}|{value["InfoReviewUrl"]}')
            solved_movie = self.get_movies_for_pages([value["InfoPage"]], [value["InfoArticle"]])
            if solved_movie is not None:
                if solved_movie[key]["InfoMovie"] != value["InfoMovie"]:
                    logger.error(f'IDMismatch|{solved_movie[key]["InfoMovie"]}|{value["InfoMovie"]}')
                    return None, non_solvable_movies
                else:
                    solved_movies.update(solved_movie)
        return solved_movies, non_solvable_movies

    def get_movies(self, pages, article_numbers=None):
        """
        This function returns a dictionary of movies scraped from a list of Empire pages. A page is a url of the form:
        https://www.empireonline.com/movies/reviews/{page}/.
        A page typically contains 24 articles, i.e., movies with links to their review page.

        Usually, it suffices to provide a list page numbers. However, if the error analysis indicates that a certain
        movie cannot be retrieved, then article_numbers provides a way to retrieve specific movies. These specific
        movies can be retrieved from multiple pages. E.g.: pages = [3, 4], article_numbers = [12, 23]

        :param pages: a list of pages.
        :param article_numbers: a list of article numbers, i.e., movies.
        :return: a dictionary with scraped movies.
        """
        logger = logging.getLogger('root')

        # Get movies for the requested pages
        logger.info('Get movies||')

        manager = multiprocessing.Manager()
        self.queue = manager.Queue()
        stop_event = Event()
        listener = multiprocessing.Process(target=listener_process,
                                                name='listener',
                                                args=(self.queue, stop_event))
        listener.start()

        self.movies = self.get_movies_for_pages(pages, article_numbers)

        # Analyze the log file and try to scrape movies which haven't been scraped yet
        logger.info('Solving movies (if any)||')
        solved_movies, non_solvable_movies = self.solve_movies()
        if solved_movies != {}:
            logger.info('Adding solved movies||')
            self.movies.update(solved_movies)

        # Remove non solvable movies from the final list
        if non_solvable_movies != {}:
            logger.info('Removing non solvable movies||')
            [self.movies.pop(key) for key in non_solvable_movies.keys()]

        # Create a DataFrame from the movies and export it to Excel
        logger.info('Create DataFrame||')
        self.df = pd.DataFrame.from_dict(self.movies, orient='index')
        self.df.index.name = 'ID'
        self.save_to_pickle()
        self.save_to_excel()

        # Stop the listener
        stop_event.set()
        listener.join()

        # Finally, copy log files to the results folder
        logger.info('Copy log files||')
        shutil.copyfile('root.log', f'results/{self.now}/{self.now}_root.log')
        shutil.copyfile('empire_movies.log', f'results/{self.now}/{self.now}_empire_movies.log')

        return self.movies
