from typing import List, Dict, Optional

from sqlalchemy.orm import Session

from config import BaseConfig
from core.BasePersistor import BasePersistor
from core.Enums import Mnemonic
from core.model.CoreModels import TradesEntry, OhlcFrame
from core.model.models import Trades, OHLC, CryptoCurrency, Exchange
from core.orm.orm import session as session_maker
from core.utils import map_frame_to_ohlc, map_ohlc_to_frame


class KrakenPersistor(BasePersistor):
    def __init__(self):
        self.recover_from = BaseConfig.Exchanges.Kraken.recover_from
        self.session_maker = session_maker
        self.market_config = None
        self.nemo_index: Dict[str, int] = {}
        self._load_mnemonic()
        self.kraken_id = self._load_kraken_id()

    def persist_ohlc(self, tick_list: List[OhlcFrame], nemo: Mnemonic) -> None:
        to_update, new = self.merge_ohlc_with_last(tick_list, nemo)
        session: Session = self.session_maker()

        try:
            if to_update is not None:
                session.add(to_update)

            for entry in new:
                ohlc = map_frame_to_ohlc(entry)
                ohlc.currency_id = self.nemo_index[nemo.value]
                ohlc.exchange_id = self.kraken_id

                session.add(ohlc)
                session.commit()

        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def persist_entry(self, entry_list: List[TradesEntry], nemo: Mnemonic) -> None:
        pass

    def _get_newest_ohlc_dto(self, nemo: Mnemonic) -> OHLC:
        return self._do_recovery(nemo, OHLC.date.desc())

    def _get_oldest_ohlc_dto(self, nemo: Mnemonic) -> OHLC:
        return self._do_recovery(nemo, OHLC.date.asc())

    def get_newest_trade(self, nemo: Mnemonic) -> TradesEntry:
        id_currency = self.nemo_index[nemo.value]

        session: Session = self.session_maker()
        res: Trades = session.query(Trades) \
            .filter(Trades.currency_id == id_currency) \
            .order_by(Trades.date.desc()).first()

        session.close()

        if res is not None:
            return TradesEntry(price=res.price, volume=res.volume, direction=res.direction, date=res.date)

    def _do_recovery(self, nemo: Mnemonic, order) -> Optional[OHLC]:
        id_currency = self.nemo_index[nemo.value]

        session: Session = self.session_maker()
        res: Optional[OHLC] = session.query(OHLC)\
            .filter(OHLC.currency_id == id_currency and OHLC.exchange_id == self.kraken_id)\
            .order_by(order).first()

        session.close()
        return res

    def _load_mnemonic(self):
        session: Session = self.session_maker()
        res: List[CryptoCurrency] = session.query(CryptoCurrency).all()
        session.close()

        for currency in res:
            self.nemo_index[currency.mnemonic] = currency.id_currency

    def _load_kraken_id(self) -> int:
        session: Session = self.session_maker()
        res = session.query(Exchange.id_exchange).filter(Exchange.name == 'Kraken').first()
        session.close()

        return res.id_exchange

