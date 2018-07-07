from bs4 import BeautifulSoup
import numpy as np
# from PIL import Image
import requests
# from io import BytesIO
from empire_scraper.empire_helpers import requests_get, get_proxies
from datetime import datetime
import os


class EmpireMovie(object):
    def __init__(self, logger, info=None, process_images=True, use_proxies=True):
        self.logger = logger
        self.info = info
        self.info_id = None
        self.info_movie = None
        self.info_rating = None
        self.review_url = None
        self.movie = dict()
        self.process_relevant_info()
        self.process_images = process_images
        self.title = None
        self.parser = "lxml"
        self.soup = None
        if use_proxies:
            self.proxies = get_proxies(file='proxies.csv')

    def process_relevant_info(self):
        if self.info is not None:
            if len(list(self.info.keys())) > 0:
                self.info_id = list(self.info.keys())[0]
                self.info_movie = self.info[self.info_id]['InfoMovie']
                self.info_rating = self.info[self.info_id]['InfoRating']
                self.review_url = self.info[self.info_id]['InfoReviewUrl']
            self.movie[self.info_id] = dict()
            self.movie[self.info_id].update(self.info[self.info_id])

    def get_soup(self):
        html = requests_get(self.logger, self.review_url, max_number_of_attempts=5, timeout=5, proxies=self.proxies)
        if html == -1:
            self.logger.error(f'RequestsGetFailed|{self.info_id}|{self.review_url}')
            self.soup = None
        else:
            self.soup = BeautifulSoup(html, self.parser)

    def get_review_author(self):
        movie = self.movie[self.info_id]
        movie['Author'] = None
        result = self.soup.find("div", class_="author")
        if result is not None:
            movie['Author'] = self.soup.find("div", class_="author").text.strip()

    def get_review_date_published(self):
        movie = self.movie[self.info_id]
        movie['DatePublished'] = None
        result = self.soup.find("time", class_="datePublished")
        if result is not None:
            movie['DatePublished'] = result['datetime'].strip()[:10]

    def get_review_last_update(self):
        movie = self.movie[self.info_id]
        movie['LastUpdate'] = None
        result = self.soup.find_all("time")
        if len(result) > 0:
            for res in result:
                temp_res = res.find('strong')
                if temp_res is not None:
                    movie['LastUpdate'] = res['datetime'].strip()[:10]

    def get_review_title_and_other_info(self):

        # There are up to 4 entries (some might be missing):
        # - release date
        # - certificate
        # - running time of the movie
        # - title of the movie -> important for dict!

        result = self.soup.find("ul", class_="list__keyline delta txt--mid-grey")
        if result is None:
            self.logger.info(f'NoInfoLeft|{self.info_id}|{self.review_url}')
            return None

        result = result.get_text('|').split('|')
        dim = int(len(result) / 2)
        result = np.reshape(result, (dim, 2))

        movie = self.movie[self.info_id]
        for res in result:
            key, value = res[0].strip(), res[1].strip()
            if key == 'Release date':
                key = 'ReleaseDate'
                movie[key] = datetime.strptime(value, '%d %b %Y').strftime('%Y-%m-%d')
            elif key == 'Running time':
                key = 'RunningTime'
                res = ''.join([s for s in value if s.isdigit()])
                movie[key] = None
                if len(res) > 0:
                    movie[key] = int(res)
            else:
                movie[key] = value

        return 1

    def get_review_rating(self):
        movie = self.movie[self.info_id]
        movie['Rating'] = None
        result = self.soup.find("span", class_="stars--on")
        if result is not None:
            movie['Rating'] = len(result.text.strip())

    def get_review_introduction_text(self):
        movie = self.movie[self.info_id]
        movie['Introduction'] = None
        result = self.soup.find('h2', class_='gamma gamma--tall txt--black')
        if result is not None:
            movie['Introduction'] = result.text.strip()

    def get_review_text(self):
        movie = self.movie[self.info_id]
        movie['Review'] = None
        result = self.soup.find('div', class_='article__text')
        if result is not None:
            paragraphs = [p.text.strip() for p in result.find_all('p')]
            if len(paragraphs) > 0:
                movie['Review'] = '\n'.join(paragraphs)

    def get_review_picture(self):
        movie = self.movie[self.info_id]
        movie['Picture'] = dict()
        movie['Picture']['Source'] = None
        movie['Picture']['File'] = None
        if self.process_images:
            result = self.soup.find('div', class_='imageWrapper imageWrapper--kenburns')
            if result is not None:
                result = result.find('img')
                if result is not None:
                    src = result['src']
                    movie['Picture']['Source'] = src
                    if src.find('no-photo') == -1:
                        response = requests.get(src)
                        if response.status_code == 200:
                            # movie['Picture']['File'] = Image.open(BytesIO(response.content))
                            out_file = os.path.join('pictures', src.split('/')[-1])
                            with open(out_file, 'wb') as f:
                                f.write(response.content)

    def get_review(self):
        self.logger.info(f'GetReview|{self.info_id}|{self.review_url}')
        self.get_soup()
        if self.soup is None:
            return
        if self.get_review_title_and_other_info() is None:
            return
        self.get_review_rating()
        self.get_review_author()
        self.get_review_date_published()
        self.get_review_last_update()
        self.get_review_introduction_text()
        self.get_review_text()
        self.get_review_picture()

    def get_movie(self):
        self.get_review()
        return self.movie
