from sqlalchemy.orm import Session

from config import BaseConfig
from core.model.models import Trades
from core.orm.orm import Session as Session_maker


class KrakenPersistor:
    def __init__(self):
        self.recover_from = BaseConfig.Exchanges.Kraken.recover_from
        self.session_maker = Session_maker
        self.market_config = None

    def load_currencies_from_markets(self):

    def persist(self):
        pass

    def get_since(self):
        session: Session = self.session_maker()
        session.query(Trades).filter(Trades.)
