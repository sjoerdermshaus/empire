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


logger = logging.getLogger(__name__)


class EmpireMovies(object):
    def __init__(self, lb=1, ub=1, process_images=True, number_of_processors=1, use_proxies=True):
        self.process_images = process_images
        self.movies = dict()
        self.parser = "lxml"
        self.df = None
        self.number_of_processors = number_of_processors
        self.lb = lb
        self.ub = ub
        self.proxies = None
        if use_proxies:
            self.proxies = get_proxies(file='empire_scraper/proxies.csv')

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
        info_from_article = dict()
        info_from_article['InfoMovie'] = self.get_title_from_article(article)
        info_from_article['InfoReviewUrl'] = self.get_review_url_from_article(article)
        info_from_article['InfoRating'] = self.get_rating_from_article(article)
        info_from_article['InfoThumbnail'] = self.get_thumbnail_from_article(article)
        return info_from_article

    def get_movies_for_page(self, page_number):
        file = 'empire.yaml'
        local_logger = setup_logging(file, f'empire_movies.{page_number}.log')
        info_url = f"https://www.empireonline.com/movies/reviews/{page_number}/"
        local_logger.info(f'GetReviewPage|{page_number}|{info_url}')
        html = requests_get(info_url, max_number_of_attempts=3, timeout=5, proxies=self.proxies)
        if html == -1:
            local_logger.error(f'RequestFailed|{page_number}|{info_url}')
            return -1
        else:
            soup = BeautifulSoup(html, self.parser)

        # Each movie is represented by an article
        articles = soup.find_all("article")
        if len(articles) == 0:
            local_logger.info(f'NonexistentPage|{page_number}|{info_url}')
            return -1

        # Loop over all articles
        movies = dict()
        for i, article in enumerate(articles, 1):
            id = f'{page_number:03d}-{i:02d}'
            info = dict()
            info[id] = dict()
            # Process meta data
            info[id]['InfoPage'] = page_number
            info[id]['InfoLocationOnPage'] = i
            info[id]['InfoUrl'] = info_url
            info[id].update(self.get_info_from_article(article))

            E = EmpireMovie(info, self.process_images)
            new_movie = E.get_movie()
            movies.update(new_movie)
        return movies

    def save_to_pickle(self):
        with open('empire_movies.pickle', 'wb') as f:
            pickle.dump(self, f, protocol=pickle.HIGHEST_PROTOCOL)

    @staticmethod
    def save_to_excel(df):
        labels = ['InfoThumbnail', 'Picture', 'Introduction', 'Review']
        df.drop(labels=labels, axis=1, inplace=True)
        with open('empire_movies.xlsx', 'wb') as f:
            df.to_excel(f, index=False)

    @staticmethod
    def load_from_pickle():
        with open('empire_movies.pickle', 'rb') as f:
            return pickle.load(f)

    def post_process_movies(self):
        self.df = pd.DataFrame.from_dict(self.movies, orient='index')
        self.df.index.name = 'ID'
        self.save_to_pickle()
        self.save_to_excel(self.df)

        # df['Essay'] = np.full((len(df), 1), False)
        #         # for tp in df.itertuples():
        #         #     pattern = 'EMPIRE ESSAY: '
        #         #     if tp.Movie.startswith(pattern):
        #         #         df.loc[tp.Index, 'Essay'] = True
        #         #         df.loc[tp.Index, 'Movie'] = tp.Movie.split(pattern)[1]
        #         # df.to_excel('Empire.xlsx', index=False)

    def concatenate_log_files(self):
        with open('empire_movies.log', 'w') as outfile:
            log_files = [f'empire_movies.{i}.log' for i in range(self.lb, self.ub + 1)]
            for log_file in log_files:
                with open(log_file) as infile:
                    outfile.write(infile.read())
                os.remove(log_file)

    def get_movies_for_pages(self):
        start = dt.now()
        with Pool(processes=self.number_of_processors) as pool:
            movies = pool.map(self.get_movies_for_page, range(self.lb, self.ub + 1), chunksize=1)
        [self.movies.update(res) for res in movies if res != -1]
        end = dt.now()
        scraping_time = str(end - start).split('.')[0]
        logger.info(f'Scraping time for {self.ub -  self.lb + 1} pages: {scraping_time}')
        self.concatenate_log_files()

    def run(self):
        self.get_movies_for_pages()
        self.post_process_movies()

    def get_df(self):
        return self.df

    def get_movies(self):
        return self.movies
