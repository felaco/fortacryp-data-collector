import pandas as pd
from utils import get_diff_datetime

WEEK = 60 * 60 * 24 * 7
WEEK2 = WEEK * 2
MONTH = 60 * 60 * 24 * 30


class PreprocessList:
    def __init__(self, preprocess_list):
        self._preprocess_list = []

        for p in preprocess_list:
            assert isinstance(p, Preprocess)
            self._preprocess_list.append(p)

    def append(self, preprocess_instance):
        assert isinstance(preprocess_instance, Preprocess)

        self._preprocess_list.append(preprocess_instance)

    def execute(self, x_dataset, y_dataset=None):
        x_dataset = x_dataset.copy(deep=True)
        if y_dataset is not None:
            y_dataset = y_dataset.copy(deep=True)

        for p in self._preprocess_list:
            x_dataset, y_dataset = p.execute(x_dataset, y_dataset)

        return x_dataset, y_dataset


class Preprocess(object):
    """
    Clase base para encadenar metodos para preprocesar
    """

    def execute(self, x_dataset, y_dataset=None):
        pass


class PreprocessLagMatrix(Preprocess):
    """
    Aplica un retraso al dataset x. Se retorna el dataset original como x, mientras que la version desplazada como y
    El parametro y_dataset debe ser nulo o arrojara una excepcion
    """

    def __init__(self, lag=4):
        self.lag = lag

    def execute(self, x_dataset, y_dataset=None):
        if y_dataset is not None:
            raise ValueError("Y_dataset must be None")

        return x_dataset.shift(self.lag), x_dataset


class PreprocessRemoveFirstElements(Preprocess):
    def __init__(self, x_lag=4, y_lag=4):
        self.x_lag = x_lag
        self.y_lag = y_lag

    def execute(self, x_dataset, y_dataset=None):
        return x_dataset.drop(x_dataset.iloc[:self.x_lag].index), \
               y_dataset.drop(y_dataset.iloc[:self.y_lag].index)


def test_train_split_timestamp(x_dataset, y_dataset, timestamp_diff=WEEK):
    ts = get_diff_datetime(timestamp_diff)

    xshape = x_dataset.shape
    yshape = y_dataset.shape

    assert xshape == yshape, 'La forma de x_dataset no es la misma que y_dataset'

    x_test = x_dataset[x_dataset.index > ts]
    df_len = x_test.shape[0]
    y_test = y_dataset.tail(df_len)

    x_train = x_dataset[x_dataset.index <= ts]
    df_len = x_train.shape[0]
    y_train = y_dataset.head(df_len)

    return x_train, y_train, x_test, y_test


class PreprocessRemoveNan(Preprocess):

    def execute(self, x_dataset, y_dataset=None):
        if y_dataset is not None:
            y_dataset = y_dataset.fillna(method='ffill')

        return x_dataset.fillna(method='ffill'), y_dataset


def normalize_sliding_windows(df, label, window=10):
    # df =df.dropna()
    min = df[label].rolling(window=window).min()
    max = df[label].rolling(window=window).max()
    new_df = (df[label] - min) / (max - min)
    return new_df


class PreprocessScale(Preprocess):
    def __init__(self, scale=1e-07):
        self.scale = scale

    def execute(self, x_dataset, y_dataset=None):
        if y_dataset is not None:
            y_dataset = y_dataset * self.scale

        return x_dataset * self.scale, y_dataset


class MinMaxNormalizeSlidingWindow:
    def __init__(self):
        self._fitted = False
        self.max = None
        self.min = None

    def fit(self, series, window=10):
        self.min = series.rolling(window=window).min()
        self.max = series.rolling(window=window).max()
        self._fitted = True

    def transform(self, series):
        if self._fitted == False:
            raise ValueError('This instance is not fitted')

        return (series - self.min) / (self.max - self.min)

    def fit_transform(self, series, window=10):
        self.fit(series, window)
        return self.transform(series)

    def inverse_transformation(self, series):
        return series * (self.max - self.min) + self.min
