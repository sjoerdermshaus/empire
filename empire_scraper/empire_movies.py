from bs4 import BeautifulSoup
import pandas as pd
from PIL import Image
import requests
from io import BytesIO
import pickle
from empire_scraper.empire_movie import EmpireMovie
from empire_scraper.empire_helpers import setup_logging, get_proxies
from multiprocessing import Pool
from empire_scraper.empire_helpers import requests_get
from datetime import datetime as dt
import os
import logging
from datetime import datetime


logger = logging.getLogger(__name__)


class EmpireMovies(object):
    def __init__(self, process_images=True, number_of_processors=1, use_proxies=True):
        self.process_images = process_images
        self.movies = dict()
        self.parser = "lxml"
        self.df = None
        self.number_of_processors = number_of_processors
        self.proxies = None
        if use_proxies:
            self.proxies = get_proxies(file='empire_scraper/proxies.csv')
        self.now = None

    @staticmethod
    def get_title_from_article(article):
        title = None
        result = article.find('p', class_='hdr no-marg gamma txt--black pad__top--half')
        if result is not None:
            title = result.text.strip()
        return title

    @staticmethod
    def get_review_url_from_article(article):
        review_url = None
        result = article.find('a')
        if result is not None:
            review_url = f"https://www.empireonline.com{result['href'].strip()}"
        return review_url

    @staticmethod
    def get_rating_from_article(article):
        rating = None
        result = article.find("span", class_="stars--on")
        if result is not None:
            rating = len(result.text.strip())
        return rating

    def get_thumbnail_from_article(self, article):
        thumbnail = None
        if self.process_images:
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
        return thumbnail

    def get_info_from_article(self, article):
        info = dict()
        info['InfoMovie'] = self.get_title_from_article(article)
        info['InfoReviewUrl'] = self.get_review_url_from_article(article)
        info['InfoRating'] = self.get_rating_from_article(article)
        info['InfoThumbnail'] = self.get_thumbnail_from_article(article)
        return info

    def get_movies_for_page(self, page, article_number=None):
        file = 'empire.yaml'
        local_logger = setup_logging(file, f'empire_movies.{page}.log')
        info_url = f"https://www.empireonline.com/movies/reviews/{page}/"
        local_logger.info(f'GetReviewPage|{page}|{info_url}')
        html = requests_get(info_url, max_number_of_attempts=3, timeout=5, proxies=self.proxies)
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
                id = f'{page:03d}-{i:02d}'
                info = dict()
                info[id] = dict()
                # Process meta data
                info[id]['InfoPage'] = page
                info[id]['InfoArticle'] = i
                info[id]['InfoUrl'] = info_url
                info[id].update(self.get_info_from_article(article))

                E = EmpireMovie(info, self.process_images)
                new_movie = E.get_movie()
                movies.update(new_movie)

        return movies

    def save_to_pickle(self):
        file = f'{self.now}_empire_movies.pickle'
        with open(file, 'wb') as f:
            pickle.dump(self, f, protocol=pickle.HIGHEST_PROTOCOL)

    @staticmethod
    def save_to_excel(df, file=None, now=None):
        if file is None: # from object
            file = f'{now}_empire_movies.xlsx'
        labels = list({'InfoThumbnail', 'Picture', 'Introduction', 'Review'} & set(df.columns))
        df.drop(labels=labels, axis=1, inplace=True)
        with open(file, 'wb') as f:
            df.to_excel(f, index=True)

    @staticmethod
    def load_from_pickle(file):
        with open(file, 'rb') as f:
            return pickle.load(f)

    def post_process_movies(self):
        self.df = pd.DataFrame.from_dict(self.movies, orient='index')
        self.df.index.name = 'ID'
        self.save_to_pickle()
        self.save_to_excel(self.df, file=None, now=self.now)

        # df['Essay'] = np.full((len(df), 1), False)
        #         # for tp in df.itertuples():
        #         #     pattern = 'EMPIRE ESSAY: '
        #         #     if tp.Movie.startswith(pattern):
        #         #         df.loc[tp.Index, 'Essay'] = True
        #         #         df.loc[tp.Index, 'Movie'] = tp.Movie.split(pattern)[1]
        #         # df.to_excel('Empire.xlsx', index=False)

    def teardown_log_files(self, pages):
        self.now = datetime.strftime(datetime.now(), "%Y%m%d-%H%M%S")
        with open(f'{self.now}_empire_movies.log', 'w') as outfile:
            log_files = [f'empire_movies.{page}.log' for page in pages]
            for log_file in log_files:
                with open(log_file, 'r') as infile:
                    outfile.write(infile.read())
                os.remove(log_file)


    def get_movies_for_pages(self, pages=None, article_number=None):

        if isinstance(pages, int): pages = [pages]
        elif not isinstance(pages, list): pages = list(pages)
        else: pass
        x = [(page, article_number) for page in pages]

        start = dt.now()
        with Pool(processes=self.number_of_processors) as pool:
            movies = pool.starmap(self.get_movies_for_page, iterable=iter(x), chunksize=1)
        [self.movies.update(res) for res in movies if res != -1]
        end = dt.now()

        scraping_time = str(end - start).split('.')[0]
        logger.info(f'Scraping time for {len(x)} pages: {scraping_time}')
        self.teardown_log_files(pages)

    def get_df(self):
        return self.df

    def get_movies(self, pages=None, article_number=None):
        self.get_movies_for_pages(pages, article_number)
        self.post_process_movies()
        return self.movies
