import pandas as pd
from empire_scraper import EmpireMovies, print_movies



def main(file):
    df = EmpireMovies.load_from_pickle(file).get_df()
    EmpireMovies.save_to_excel(df, file=file.replace('pickle','xlsx'))
    print(len(df))
    log_file = file.replace('pickle', 'log')
    L = LogAnalyzer(log_file)


if __name__ == '__main__':
    main(file=r'C:\Users\Sjoerd\PycharmProjects\Empire\20180427-103724_empire_movies.pickle')
