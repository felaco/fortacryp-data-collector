import pandas as pd
import numpy as np
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
    Aplica un retraso al dataset x. Se retorna el dataset original como y, mientras que la version desplazada como x
    El parametro y_dataset debe ser nulo o arrojara una excepcion
    """

    def __init__(self, lag=4):
        self.lag = lag

    def execute(self, x_dataset, y_dataset=None):
        if y_dataset is not None:
            raise ValueError("Y_dataset must be None")

        df = pd.DataFrame()
        for i in range(1, self.lag + 1):
            temp = x_dataset.shift(i)
            self._change_col_names(temp, i)
            self.append(df, temp)

        return df, x_dataset

    def _change_col_names(self, df, lag):
        if isinstance(df, pd.Series):
            df.name = df.name + '_t-{}'.format(lag)
            return

        colnames = list(df.columns)
        colnames = [name + '_t-{}'.format(lag) for name in colnames]
        df.columns = colnames

    def append(self, df, app):
        if isinstance(app, pd.Series):
            df[app.name] = app
        else:
            for col in app:
                series = app[col]
                df[series.name] = series

class PreprocessRemoveFirstElements(Preprocess):
    def __init__(self, lag):
        self.lag = lag

    def execute(self, x_dataset, y_dataset=None):
        return x_dataset.drop(x_dataset.iloc[:self.lag].index), \
               y_dataset.drop(y_dataset.iloc[:self.lag].index)


def test_train_split_timestamp(x_dataset, y_dataset, timestamp_diff=WEEK):
    ts = get_diff_datetime(timestamp_diff)

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
            y_dataset = y_dataset.dropna()

        return x_dataset.dropna(), y_dataset


class PreprocessFillNan(Preprocess):
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


class PreprocessDifferenciate(Preprocess):
    def __init__(self, periods=1):
        self.periods = periods
        self._fitted = False

    def execute(self, x_dataset, y_dataset=None):
        self.x_original = x_dataset.copy(deep=True)
        self._fitted = True

        return x_dataset.diff(self.periods), None

    def inverse_transformation(self, data):
        if self._fitted == False:
            raise AttributeError('the execute method has not been run')

        lista = []
        for i in range(len(self.x_original)):
            val = data[i] + self.x_original[i - self.periods]
            lista.append(val)

        # return np.array(lista)
        return pd.DataFrame(lista)

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


if __name__ == '__main__':
    import pandas as pd
    from sklearn.preprocessing import scale

    lag=4
    df = pd.read_excel('Consumo_energetico.xlsx')
    df['consumo'] = scale(df['consumo'])
    x, y = PreprocessLagMatrix(lag).execute(df.consumo)
    x, y = PreprocessRemoveFirstElements(lag).execute(x, y)
    print(x.head(10))