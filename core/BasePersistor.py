from abc import ABC, abstractmethod
from typing import List, Optional

from core.Enums import Mnemonic
from core.model.CoreModels import OhlcFrame, TradesEntry
from core.model.models import OHLC
from core.utils import trade_entries_to_ohlc_frames, map_ohlc_to_frame


class BasePersistor(ABC):
    @abstractmethod
    def persist_ohlc(self, tick_list: List[OhlcFrame], nemo: Mnemonic) -> None:
        raise NotImplementedError()

    @abstractmethod
    def persist_entry(self, entry_list: List[TradesEntry], nemo: Mnemonic) -> None:
        raise NotImplementedError()

    def get_oldest_ohlc(self, nemo: Mnemonic) -> OhlcFrame:
        ohlc = self._get_oldest_ohlc_dto(nemo)
        return map_ohlc_to_frame(ohlc) if ohlc is not None else None

    def get_newest_ohlc(self, nemo: Mnemonic) -> OhlcFrame:
        ohlc = self._get_newest_ohlc_dto(nemo)
        return map_ohlc_to_frame(ohlc) if ohlc is not None else None

    @abstractmethod
    def get_newest_trade(self, nemo: Mnemonic) -> TradesEntry:
        raise NotImplementedError()

    @abstractmethod
    def _get_newest_ohlc_dto(self, nemo: Mnemonic) -> OHLC:
        raise NotImplementedError()

    def _get_oldest_ohlc_dto(self, nemo: Mnemonic) -> OHLC:
        raise NotImplementedError()

    @staticmethod
    def trades_to_ohlc(trade_list: List[TradesEntry]) -> List[OhlcFrame]:
        return trade_entries_to_ohlc_frames(trade_list)

    def merge_ohlc_with_last(self, tick_list: List[OhlcFrame], nemo: Mnemonic) -> (
            Optional[OHLC], List[OhlcFrame]):
        last = self._get_newest_ohlc_dto(nemo)

        if last is not None:
            def filter_func(ohlc_frame: OhlcFrame):
                return ohlc_frame.date >= last.date

            tick_list = list(filter(filter_func, tick_list))
            first_frame = tick_list[0]

            if first_frame.date == last.date:
                last.close = first_frame.close
                last.low = min(first_frame.low, last.low)
                last.high = max(first_frame.high, last.high)
                last.volume += first_frame.volume
                tick_list.pop(0)

        return last, tick_list
