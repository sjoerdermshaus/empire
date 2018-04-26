from empire_scraper import EmpireMovie, EmpireMovies
from empire_scraper import print_movies, setup_logging

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

file = 'empire.yaml'
logger = setup_logging(file)


def test_movie(review_url):
    info = {}
    info['InfoID'] = {}
    info['InfoID']['InfoPage'] = None
    info['InfoID']['InfoLocationOnPage'] = None
    info['InfoID']['InfoMovie'] = 'The Best Ever'
    info['InfoID']['InfoRating'] = 6
    info['InfoID']['InfoReviewUrl'] = review_url
    info['InfoID']['InfoUrl'] = None
    info['InfoID']['InfoThumbnail'] = None
    E = EmpireMovie(info)
    print_movies(E.get_movie())


def test_specific_movie(page_number, article_number):
    E = EmpireMovies(process_images=True)
    print_movies(E.get_movies_for_page(page_number, article_number))


def test_page(page_number):
    E = EmpireMovies(process_images=True)
    print_movies(E.get_movies_for_page(page_number=page_number))


def test_movies(lb=1, ub=1000, number_of_processors=2):
    EmpireMovies(lb=lb, ub=ub, process_images=False, number_of_processors=number_of_processors).run()
    # print_movies(E.get_movies())


def clean_df(df):

    df.loc[df['Movie'] == 'Nymphomaniac Volumes I And II', 'RunningTime'] = 122 + 123
    #df.loc[df['Author'].isnull(), 'Rating'] = df.loc[df['Author'].isnull(), 'InfoRating']
    EmpireMovies.save_to_excel(df=df)
    return df


def test_results():
    df = EmpireMovies.load_from_pickle().get_df()
    df = clean_df(df)

    rating = pd.pivot_table(data=df, index=['Rating'], values=['Movie'], aggfunc=len)
    rating.plot(kind='bar')
    plt.show()

    df_author = pd.pivot_table(data=df, index=['Author'], values=['Rating'], aggfunc=[np.mean, len], margins=True)
    df_author = df_author[df_author[('len', 'Rating')] >= 10]
    df_author.sort_values(by=('mean', 'Rating'), ascending=False, inplace=True)
    print(df_author)
    # pivot.query("Rating != 'All'").plot(kind='bar')
    positive_author = df_author.index[0]

    df_positive_author = df.query('Author == @positive_author')[['Movie', 'Rating']]\
        .sort_values(by='Rating', ascending=False)
    print(f'\n{positive_author}\n')
    print(df_positive_author)

    df_top_movies = df.query('Rating == 5')[['Movie', 'Author', 'Rating']]
    print('\ntop_movies\n')
    print(df_top_movies)

    df_worst_movies = df.query('Rating == 1')[['Movie', 'Author', 'Rating']]
    print('\nworst_movies\n')
    print(df_worst_movies)


    df_running_time = df[['Movie', 'Rating', 'RunningTime']].sort_values(by='RunningTime', ascending=False)
    print('\nRunningTime\n')
    print(df_running_time.head(10))


if __name__ == '__main__':
    # test_movie(review_url='https://www.empireonline.com/movies/gods-pocket/review/')
    # test_movie(review_url='https://www.empireonline.com/movies/mrs-browns-boys-dmovie/review/')
    # test_movie(review_url='https://www.empireonline.com/movies/us/review/')
    # test_movie(review_url='https://www.empireonline.com/movies/avengers-infinity-war/review/')
    # test_page(52)
    # test_movies(1, 500, 5)
    # test_results()
    test_specific_movie(93, 1)
