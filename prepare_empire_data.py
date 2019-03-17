from empire_scraper.src.empire_movies import EmpireMovies
from nltk.corpus import stopwords
from stop_words import get_stop_words
from collections import Counter
import re
import string
import logging
from logging.config import dictConfig
import yaml
import numpy as np
import os
import pandas as pd
import pickle
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score
from multiprocessing import Pool


class CleanData(object):
    def __init__(self, pickle_file=None, min_letters=4, use_sample=False):
        self.pickle_file = pickle_file
        self.min_letters = min_letters
        self.use_sample = use_sample
        self.stop_words = self.define_stop_words()
        self.df_clean = pd.DataFrame()
        self.bag_of_words = None
        self.sum_of_bag_of_words = None
        self.features = None
        self.df_words = pd.DataFrame()

    @staticmethod
    def define_stop_words():
        logger = logging.getLogger('root')

        stop_words = list(get_stop_words('en'))
        logger.info(f'Number of stop_words: {len(stop_words)}')

        nltk_words = list(stopwords.words('english'))
        logger.info(f'Number of nltk_words: {len(nltk_words)}')
        stop_words.extend(nltk_words)
        stop_words = list(set(stop_words))
        logger.info(f'Number of stop_words: {len(stop_words)}')

        other_words = []
        for word in stop_words:
            other_words.append(''.join(re.findall(r'([\w]+)', word)))
        other_words = list(set(other_words) - set(stop_words))
        logger.info(f'Number of other_words: {len(other_words)}')
        stop_words.extend(other_words)
        stop_words = list(set(stop_words))
        logger.info(f'Number of stop_words: {len(stop_words)}')

        return stop_words

    def clean_introduction_and_review(self):
        logger = logging.getLogger('root')
        logger.info('Loading data')
        df = EmpireMovies.load_from_pickle(self.pickle_file).get_df()
        if self.use_sample:
            df = df.head(1000)
        logger.info('Dropping columns without review')
        self.df_clean = df.dropna(axis=0, subset=['Review']).copy()
        #df_clean = self.df.iloc[0:10, :].copy()
        #review = list(df_clean.head(100)['Review'])
        #review = [(rev,) for rev in review]
        #processes = self.number_of_processors
        #with Pool(processes=5) as pool:
        #    results = pool.starmap(self.clean_text, iterable=iter(review), chunksize=1)
        #print(results)
        for row in self.df_clean.itertuples():
            logger.info(f'Cleaning {row.Index}')
            cleaned_text, before, after, ratio = self.clean_text(row.Review)
            self.df_clean.loc[row.Index, 'CleanReview'] = cleaned_text
            self.df_clean.loc[row.Index, 'NumberOfWordsBeforeCleaning'] = before
            self.df_clean.loc[row.Index, 'NumberOfWordsAfterCleaning'] = after
            self.df_clean.loc[row.Index, 'CleaningRatio'] = ratio
        self.save_to_pickle()

    def save_to_pickle(self):
        logger = logging.getLogger('root')
        logger.info('Pickling cleaned data||')
        if not self.use_sample:
            self.pickle_file = os.path.join(f'clean_data.pickle')
        else:
            self.pickle_file = os.path.join(f'clean_data_sample.pickle')
        with open(self.pickle_file, 'wb') as f:
            pickle.dump(self, f, protocol=pickle.HIGHEST_PROTOCOL)

    def get_df_clean(self):
        return self.df_clean

    @staticmethod
    def load_from_pickle(file):
        with open(file, 'rb') as f:
            return pickle.load(f)

    @staticmethod
    def lowercase(text: str):
        return text.lower()

    @staticmethod
    def remove_punctuation(text: str):
        own_punctuation = '’‘“”'
        punctuation = ''.join([string.punctuation, own_punctuation])
        table = text.maketrans('', '', punctuation)
        return text.translate(table)

    @staticmethod
    def replace_text(text: str):
        table = [("’s", "")]
        for entry in table:
            text = text.replace(entry[0], entry[1])
        return text

    @staticmethod
    def remove_stop_words(text_list: list, stop_words: list):
        return [word for word in text_list if word not in stop_words]

    @staticmethod
    def remove_non_alphabet(text_list: list):
        return [word for word in text_list if word.isalpha()]

    @staticmethod
    def remove_short_words(text_list: list, min_letters):
        return [word for word in text_list if len(word) >= min_letters]

    def clean_text(self, text: str):
        if not isinstance(text, str):
            return 4 * [np.nan]

        number_of_words_before = len(text.split())
        text = self.lowercase(text)
        text = self.remove_punctuation(text)

        text_list = text.split()
        text_list = self.remove_non_alphabet(text_list)
        text_list = self.remove_stop_words(text_list, self.stop_words)
        text_list = self.remove_short_words(text_list, self.min_letters)
        text = ' '.join(text_list)
        number_of_words_after = len(text_list)
        cleaning_ratio = number_of_words_after / number_of_words_before

        return [text, number_of_words_before, number_of_words_after, cleaning_ratio]

    def tokenize_text(self):
        root_logger.info('Bags of words started')
        self.bag_of_words = [Counter(re.findall(r'[\w]+', row.CleanReview)) for row in self.df_clean.itertuples()]
        self.sum_of_bags_of_words = sum(self.bag_of_words, Counter())
        root_logger.info('Bags of words finished')
        self.save_to_pickle()

    def make_features(self):
        root_logger.info('Making features started')
        self.features = np.zeros((len(self.bag_of_words), len(self.sum_of_bags_of_words)))
        #with Pool(processes=5) as pool:
        #    results = pool.starmap(self.clean_text, iterable=iter(review), chunksize=1)
        for i, counter in enumerate(self.bag_of_words):
            for j, word in enumerate(self.sum_of_bags_of_words):
                if word in counter:
                    self.features[i, j] = counter[word]
        root_logger.info('Making features finished')
        self.save_to_pickle()

    def analyze_features(self):
        root_logger.info('Analyzing features started')
        self.df_words = pd.DataFrame(index=list(self.sum_of_bags_of_words.keys()),
                                     data=list(self.sum_of_bags_of_words.values()),
                                     columns=['Count'])
        self.df_words['Std'] = np.nan
        self.df_words['Average'] = np.nan
        rating = self.df_clean['InfoRating'].values
        for i, row in enumerate(self.df_words.itertuples()):
            ind = self.features[:, i] >= 1
            self.df_words.loc[row.Index, 'Average'] = np.mean(rating[ind])
            if sum(ind) > 1:
                self.df_words.loc[row.Index, 'Std'] = np.std(rating[ind])
        df_words_sorted = self.df_words.sort_values(by=['Std'], ascending=False)
        print(df_words_sorted)
        root_logger.info('Analyzing features finished')

    @staticmethod
    def sentiment(rating):
        if rating <=2:
            return 0
        else:
            return 1

    def count_words(self):
        self.df_clean.dropna(axis=0, subset=['InfoRating'], inplace=True)
        data = self.df_clean['Review']
        self.df_clean['InfoRating'] = [int(rating) for rating in self.df_clean['InfoRating']]
        self.df_clean['Sentiment'] = [self.sentiment(rating) for rating in self.df_clean['InfoRating']]

        target = self.df_clean['Sentiment'].values
        target = self.df_clean['InfoRating'].values
        vectorizer = CountVectorizer(ngram_range=(2, 3))
        X = vectorizer.fit_transform(data).toarray()
        X = np.column_stack((self.df_clean['NumberOfWordsAfterCleaning'].values, X))
        X_train, X_test, y_train, y_test = train_test_split(X,
                                                            target,
                                                            test_size = 0.3,
                                                            random_state = 0)
        logistic_model = LogisticRegression(multi_class='ovr')
        logistic_model = logistic_model.fit(X_train, y_train)
        y_pred = logistic_model.predict(X_test)
        print(accuracy_score(y_test, y_pred))


def main():
    #file = r"C:\Users\Sjoerd\PycharmProjects\Empire\empire_scraper\results\20180504-105917\20180504-105917_empire_movies.pickle"
    #C = CleanData(file, use_sample=True)
    #C.clean_introduction_and_review()
    #C.tokenize_text()
    #C.make_features()
    file = 'clean_data_sample.pickle'
    C = CleanData.load_from_pickle(file)
    cleaning_ratio = sum(C.df_clean['NumberOfWordsAfterCleaning']) / sum(C.df_clean['NumberOfWordsBeforeCleaning'])
    print(f'Total CleaningRatio: {100 * cleaning_ratio:.1f}%')
    #print(C.tokenize_text())
    C.count_words()


def test():
    text = "halloø, ' p and tg h saæ % abc abc in # KLOß witch’s aren't ‘magic “youre 4thgraders”"
    # clean_text = CleanData.clean_text(text)
    tokenenized_text = CleanData.tokenize_text()
    import json
    print(json.dumps(tokenenized_text, indent=4))


if __name__ == '__main__':
    with open('root.yaml', 'r') as fh:
        config_root = yaml.load(fh.read())
    dictConfig(config_root)
    root_logger = logging.getLogger('root')
    main()
    # test()
