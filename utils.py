import json
import box
import os
import time
import logging
from dateutil.tz import tzlocal
from datetime import datetime
from pandas.compat import lmap
import numpy as np


def split_thousands(num, unused):
    if isinstance(num, float):
        return '{:,.1f}'.format(num).replace(",", "X").replace(".", ",").replace("X", ".")
    elif isinstance(num, int):
        return '{:,d}'.format(num).replace(",", ".")
    else:
        return str(num)


def read_config_file():
    file_name = './config.json'
    if not os.path.isfile(file_name):
        raise FileNotFoundError('El archivo de configuración no existe')

    with open(file_name) as file:
        json_str = file.read()

    config_dict = json.loads(json_str)
    return box.Box(config_dict)


def create_base_config_file():
    config_dict = dict()

    config_dict['last_timestamp_bitcoin'] = None
    config_dict['last_timestamp_ether'] = None
    config_dict['last_timestamp_bch'] = None
    config_dict['last_timestamp_litecoin'] = None

    config_dict['last_correct_request'] = None

    save_config_file(config_dict)


def save_config_file(config):
    if isinstance(config, box.Box):
        config = config.to_dict()

    json_str = json.dumps(config, indent=4)
    with open('./config.json', 'w') as file:
        file.write(json_str)


class ProyectLogger(logging.Logger):
    HIGH = 1
    LOW = 0

    def __init__(self, name, level=logging.INFO):
        self.info_call_amount = 0
        super().__init__(name, level)

    def info(self, msg, *args, **kwargs):
        self.info_call_amount += 1
        if 'priority' in kwargs and kwargs['priority'] == self.HIGH:
            kwargs.pop('priority', None)
            super().info(msg, *args, **kwargs)
        else:
            if self.info_call_amount % 4 == 0:
                kwargs.pop('priority', None)
                super().info(msg, *args, **kwargs)


def get_last_week_datetime():
    last_week_ts = time.time() - (60 * 60 * 24 * 7)
    tz = tzlocal()
    return datetime.fromtimestamp(last_week_ts, tz)


def get_diff_datetime(ts_diff):
    diff_ts = time.time() - ts_diff
    tz = tzlocal()
    return datetime.fromtimestamp(diff_ts, tz)


def autocorrelation_plot(series, n_samples=None, ax=None, **kwds):
    """Autocorrelation plot for time series.

    Parameters:
    -----------
    series: Time series
    ax: Matplotlib axis object, optional
    kwds : keywords
        Options to pass to matplotlib plotting method

    Returns:
    -----------
    ax: Matplotlib axis object
    """
    import matplotlib.pyplot as plt
    n = len(series)
    data = np.asarray(series)
    if ax is None:
        ax = plt.gca(xlim=(1, n_samples), ylim=(-1.0, 1.0))
    mean = np.mean(data)
    c0 = np.sum((data - mean) ** 2) / float(n)

    def r(h):
        return ((data[:n - h] - mean) *
                (data[h:] - mean)).sum() / float(n) / c0

    x = (np.arange(n) + 1).astype(int)
    y = lmap(r, x)
    z95 = 1.959963984540054
    z99 = 2.5758293035489004
    ax.axhline(y=z99 / np.sqrt(n), linestyle='--', color='grey')
    ax.axhline(y=z95 / np.sqrt(n), color='grey')
    ax.axhline(y=0.0, color='black')
    ax.axhline(y=-z95 / np.sqrt(n), color='grey')
    ax.axhline(y=-z99 / np.sqrt(n), linestyle='--', color='grey')
    ax.set_xlabel("Lag")
    ax.set_ylabel("Autocorrelation")
    if n_samples:
        ax.plot(x[:n_samples], y[:n_samples], **kwds)
    else:
        ax.plot(x, y, **kwds)
    if 'label' in kwds:
        ax.legend()
    ax.grid()
    return ax


if __name__ == '__main__':
    # create_base_config_file()
    print(split_thousands(1234567))
    print('{:,d}'.format(1234567))

