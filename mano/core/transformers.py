import unidecode
import pandas as pd
from sklearn.base import TransformerMixin, BaseEstimator


class ColumnsTypeConverter(TransformerMixin, BaseEstimator):
    """ Convert all columns from of type "from_type" to type "to_type" """

    def __init__(self, from_type, to_type):
        self.from_type = from_type
        self.to_type = to_type

    def fit(self, X, y):
        return self

    def transform(self, X):
        X = X.copy()
        for c in X.columns:
            if (isinstance(self.from_type, str) and str(X[c].dtype) == self.from_type) or \
                    (isinstance(self.from_type, type) and X[c].dtype == self.from_type):
                X[c] = X[c].astype(self.to_type)
        return X


class ColumnsRenamer(TransformerMixin, BaseEstimator):
    """ Remove special characters and spaces from columns names """

    def fit(self, X, y):
        return self

    def transform(self, X):
        X = X.copy()
        X.columns = [unidecode.unidecode(c).replace(' ', '_').strip() for c in X.columns]
        X.columns = [''.join([c if c.isalnum() else '_' for c in column]) for column in X.columns]
        return X


class CategoryCounter(TransformerMixin, BaseEstimator):
    """ Count the number of observations associated to each modality of a given column """

    def __init__(self, column):
        self.column = column
        self.count_column = column + '_count'

    def fit(self, X, y):
        self.count = X.groupby(self.column).size().reset_index()
        self.count.columns = [self.column, self.count_column]
        return self

    def transform(self, X):
        X = pd.merge(X, self.count, on=self.column, how='left')
        X[self.count_column] = X[self.count_column].fillna(0)
        return X


class CategoryAverager(TransformerMixin, BaseEstimator):
    """ Compute the average value of average_column for each modality of a given column """

    def __init__(self, column, average_column):
        self.column = column
        self.average_column = average_column
        self.ouput_column = column + '_' + average_column + '_average'

    def fit(self, X, y):
        self.count = X.groupby(self.column)[self.average_column].mean().reset_index()
        self.count.columns = [self.column, self.ouput_column]
        return self

    def transform(self, X):
        X = pd.merge(X, self.count, on=self.column, how='left')
        X[self.ouput_column] = X[self.ouput_column].fillna(0)
        return X