import pandas as pd
from empire_scraper import EmpireMovies, print_movies


class LogAnalyzer(object):
    def __init__(self, file):
        self.file = file
        self.now = file[:15]
        self.columns = ['asctime',
                        'filename',
                        'funcName',
                        'lineno',
                        'levelname',
                        'message0',
                        'message1',
                        'message2']
        self.df = pd.DataFrame()
        self.df_error = pd.DataFrame()

    @staticmethod
    def line_splitter(line):
        return [i.strip() for i in line.split('|')]

    def analyze(self):
        with open(self.file, 'r') as f:
            lines = f.readlines()
        lines = [self.line_splitter(line) for line in lines]
        self.df = pd.DataFrame(data=lines, columns=self.columns)

        print(self.df.pivot_table(index=['levelname'], values=['message1'], aggfunc=[len], margins=True))
        self.df_error = self.df.query('levelname == "ERROR"')
        solvable_error = [self.analyze_message(message)
                          for message in self.df_error[['message0', 'message1', 'message2']].values]
        self.df_error = self.df_error.assign(SolvableError=solvable_error)
        self.df_error.to_excel('{self.now}_log_errors.xlsx', index=True)

    @staticmethod
    def analyze_message(message):
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

    def get_solvable_movies(self, df):
        self.analyze()
        solvable_movies = list(self.df_error.query('SolvableError == True')['message2'].unique())
        print(solvable_movies)
        return df[df['InfoReviewUrl'].isin(solvable_movies)]


def main(file):
    df = EmpireMovies.load_from_pickle(file).get_df()
    print(len(df))
    log_file = file.replace('pickle', 'log')
    L = LogAnalyzer(log_file)
    df_solvable_movies = L.get_solvable_movies(df)
    for row in df_solvable_movies.itertuples():
        E = EmpireMovies(process_images=False)
        print_movies(E.get_movies(row.InfoPage, row.InfoArticle, export=False))


if __name__ == '__main__':
    main(file=r'C:\Users\Sjoerd\PycharmProjects\Empire\20180427-000119_empire_movies.pickle')
