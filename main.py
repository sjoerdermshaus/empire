from empire_scraper.src.empire_movies import EmpireMovies
import yaml
from logging.config import dictConfig
from empire_scraper.src.empire_helpers import print_movies
import logging


def main():
    import os
    print(os.getcwd())
    with open('root.yaml', 'r') as fh:
        config_root = yaml.load(fh.read(), Loader=yaml.FullLoader)
    dictConfig(config_root)
    logger = logging.getLogger('root')
    logger.info('Starting main script')
    # my_pages = [446, 447, 68]
    # my_article_numbers = [5, 1, 20]
    my_pages = list(range(1, 551))
    my_article_numbers = None
    my_movies = EmpireMovies(process_images=False,
                             number_of_processors=10,
                             use_proxies=True,
                             max_number_of_attempts=1).get_movies(my_pages, my_article_numbers)
    # print_movies(my_movies)


if __name__ == '__main__':
    main()
