import os
from typing import Optional, Dict, List, Union

import pandas as pd
import numpy as np


class KrakenPersistor:
    def __init__(self, market: str = 'btc', base_path: str = './'):
        self.market: str = market
        self.base_path: str = base_path
        self.buffer_df: Optional[pd.DataFrame] = None
        self.name_convention: str = 'kraken_{}_.csv'
        self.timestamp_key: str = 'timestamp'
        self.default_first_timestamp = 1356998400

    def persist(self, new_data: List[Dict[str, Union[float, int]]]):
        self._read_buffer()
        df = pd.DataFrame(new_data, columns=self._get_columns_names())

        last_timestamp = self.get_most_recent_timestamp()
        df = df[df[self.timestamp_key] > last_timestamp]  # stores only the entries newer than last_timestamp

        self.buffer_df = self.buffer_df.append(df)
        self.buffer_df.to_csv(self._get_csv_path())

    def get_most_recent_timestamp(self) -> int:
        self._read_buffer()
        if self.buffer_df.size <= 0:
            return self.default_first_timestamp

        return int(self.buffer_df[self.timestamp_key].values[-1])

    def clear_buffer(self) -> None:
        del self.buffer_df
        self.buffer_df = None

    def _read_buffer(self) -> pd.DataFrame:
        path = self._get_csv_path()
        if self.buffer_df is None and os.path.isfile(path):
            self.buffer_df = pd.read_csv(path)
        elif self.buffer_df is not None:
            return self.buffer_df
        else:
            # if file doesnt exists, creates an empty dataframe
            arr = np.array([[], [], [], [], [], []]).transpose()  # 6 columns, no rows
            self.buffer_df = pd.DataFrame(arr, columns=self._get_columns_names())

        return self.buffer_df

    def _get_columns_names(self) -> tuple:
        return self.timestamp_key, 'open', 'high', 'low', 'close', 'volume'

    def _get_csv_path(self) -> str:
        return os.path.join(self.base_path, self.name_convention.format(self.market))
