from datetime import datetime, timedelta
from typing import List

from core.model.CoreModels import OhlcFrame, TradesEntry
from core.model.models import OHLC


def is_valid_market_json(market_dict: dict) -> bool:
    """
    I want to remove this function. But there is still some codebase that uses it
    :param market_dict:
    :return:
    """
    return 'btc' in market_dict and \
           'eth' in market_dict and \
           'ltc' in market_dict and \
           'bch' in market_dict


def get_close_datetime(date: datetime) -> datetime:
    """
    Return a datetime that should be used as a close limit of the current ohlc frame
    :param date: The datetime of the current transaction
    :return: The datetime where the frame should close
    """
    if date.minute != 0 or date.second != 0 or date.microsecond != 0:
        return date.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    else:
        return date


def get_minmax(curr_min: float, curr_max: float, new: float) -> (float, float):
    return min(curr_min, new), max(curr_max, new)


def reset_ohlc(curr_price) -> (float, float, float, float):
    """
    Sets the open, high, low and close with the same price. Should be used when creating a new ohlc frame as all
    columns has the same value when the opening price comes.
    :param curr_price:
    :return:
    """
    return curr_price, curr_price, curr_price, curr_price


def trade_entries_to_ohlc_frames(trade_list: List[TradesEntry]) -> List[OhlcFrame]:
    """
    Maps a list of trades into an hourly ohlc list
    :param trade_list: list of trades
    :return: A list of ohlc frames that can be used to insert into a database via a Persistor instance
    """

    # i could use pandas dataframe.resample('1H').ohlc() but i want to remove that dependency
    if trade_list is None or len(trade_list) == 0:
        return []

    ohlc_list: List[OhlcFrame] = []
    close_datetime = get_close_datetime(trade_list[0].date)
    open_price, high_price, low_price, close_price = reset_ohlc(trade_list[0].price)
    volume = 0

    for trade in trade_list:
        if trade.date > close_datetime:
            ohlc_frame = OhlcFrame(open=open_price, high=high_price, low=low_price,
                                   close=close_price, date=close_datetime, volume=volume)

            ohlc_list.append(ohlc_frame)
            close_datetime = get_close_datetime(trade.date)
            open_price, high_price, low_price, close_price = reset_ohlc(trade.price)
            volume = trade.volume

        else:
            close_price = trade.price
            low_price, high_price = get_minmax(low_price, high_price, trade.price)
            volume += trade.volume

    ohlc_frame = OhlcFrame(open=open_price, high=high_price, low=low_price,
                           close=close_price, date=close_datetime, volume=volume)

    ohlc_list.append(ohlc_frame)

    return ohlc_list


def map_frame_to_ohlc(ohlc_frame: OhlcFrame) -> OHLC:
    return OHLC(open_price=ohlc_frame.open,
                high=ohlc_frame.high,
                low=ohlc_frame.low,
                close=ohlc_frame.close,
                volume=ohlc_frame.volume,
                date=ohlc_frame.date)


def map_ohlc_to_frame(ohlc: OHLC) -> OhlcFrame:
    return OhlcFrame(
        open=ohlc.open,
        high=ohlc.high,
        low=ohlc.low,
        close=ohlc.close,
        date=ohlc.date,
        volume=ohlc.volume
    )
