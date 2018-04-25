import pandas as pd
from empire_scraper import EmpireMovies

class LogAnalyzer(object):
    def __init__(self, file):
        self.file=file
        self.columns = ['asctime',
                        'filename',
                        'funcName',
                        'lineno',
                        'levelname',
                        'message']
        self.df = pd.DataFrame()
        self.df_error = pd.DataFrame()

    @staticmethod
    def line_splitter(line):
        line_list = line.split()
        # re-join date and time into asctime
        asctime = [' '.join(line_list[:2])]
        # these remain unchanged
        body = line_list[2:6]
        # re-join message
        message = [' '.join(line_list[6:])]
        return asctime + body + message

    def analyze(self):
        with open(self.file, 'r') as f:
            lines = f.readlines()
        lines = [self.line_splitter(line) for line in lines]
        self.df = pd.DataFrame(data=lines, columns=self.columns)
        print(self.df.pivot_table(index=['levelname'], values=['message'], aggfunc=[len], margins=True))
        self.df_error = self.df.query('levelname == "ERROR"')
        results = [self.analyze_message(message) for message in self.df_error['message']]
        self.df_error['SolvableError'] = [res[0] for res in results]
        #self.df_error.loc[:, 'Movie'] = [res[1] for res in results].copy()
        print(self.df_error.pivot_table(index=['message'], values=['asctime'], aggfunc=[len], margins=True))
        self.df_error.to_excel('log_errors.xlsx', index=True)

    @staticmethod
    def analyze_message(message):
        solvable_error = True
        movie = None
        if message.startswith('html = -1 for '):
            review_url = message.replace('html = -1 for ', '')
            if review_url.split('/')[4].startswith('55'):
                solvable_error = False
            elif review_url.count('/') > 6:
                solvable_error = False
            else:
                movie = review_url
        elif message == 'bad website: 404':
            solvable_error = False
        elif message.startswith('getting result for '):
            review_url = message.replace('getting result for ', '')
            review_url = review_url.replace(' not successful:-(', '')
            movie = review_url
        return [solvable_error, movie]

    def get_solvable_movies(self, df):
        self.analyze()
        solvable_movies = list(self.df_error.query('SolvableError == True')['Movie'].unique())
        df_query = df[df['InfoReviewUrl'].isin(solvable_movies)]
        print(df_query['Movie'])
        print(df[df['InfoReviewUrl'] == 'https://www.empireonline.com/movies/passenger-57/review/'])


def main():
    df = EmpireMovies.load_from_pickle().get_df()
    print(len(df))
    file = 'empire_movies.log'
    L = LogAnalyzer(file)
    L.get_solvable_movies(df)



if __name__ == '__main__':
    main()
