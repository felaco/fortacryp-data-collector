import pandas as pd


def _to_iso_format(datetime):
    if hasattr(datetime, 'isoformat'):
        return datetime.isoformat()
    else:
        raise TypeError('Parámetro no posee método isoformat')


class Dataframe_wrapper:
    def __init__(self, df: pd.DataFrame):
        self.dataframe = df  # dataframe que se modificará en las funciones
        self._resampled_flag = False
        self._dataframe_original = df.copy(deep=True)  # dataframe que nunca será alterado
        self.resample = None

    def ohlc(self, resample):
        if self._resampled_flag:
            raise RuntimeError('El dataframe ya ha sido remuestreado,'
                               ' no puede volver a ejecutarse dicha operación')

        self._resampled_flag = True
        self.resample = resample

        self.dataframe = self.dataframe['Price'].resample(resample). \
            ohlc().fillna(method='ffill')

        return self

    def append_volume(self):
        if not self._resampled_flag:
            raise RuntimeError('El dataframe debe haber sido remuestreado con anterioridad'
                               'para poder agregar el volumen')

        self.dataframe['Volume'] = self._dataframe_original['Amount']. \
            resample(self.resample).sum()

        return self

    def to_json(self, include_index=False, timestamp_index=False, orient='records'):
        df = self.dataframe
        if timestamp_index:
            import numpy as np

            df.set_index(df.index.astype(np.int64) // 10 ** 6,
                         inplace=True)  # unix timestamp en milisegundos

        if include_index:
            df = self.dataframe.reset_index()

        return df.to_json(orient=orient)


def get_pandas_dataframe(ts_since, file='./dataset/bitcoin.csv', index_name='Date'):
    df = pd.read_csv(file, index_col=0)
    df = df[df.index > ts_since]
    df.set_index(pd.to_datetime(df.index, unit='s').
                 tz_localize('UTC').
                 tz_convert('Etc/GMT-4'),
                 inplace=True)

    df.index.name = index_name
    return Dataframe_wrapper(df)


if __name__ == '__main__':
    dfw = get_pandas_dataframe(1522271459).ohlc(resample='1D').to_json(include_index=True)
    print(dfw)
