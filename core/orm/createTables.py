from core.model.models import CryptoCurrency
from .orm import session as session_maker, Base, engine
from ..model.models import Exchange
from config import BaseConfig
import logging

_logger = logging.getLogger('FortacrypLogger')


def create_tables(log=True):
    Base.metadata.create_all(engine)
    session = session_maker()

    exchanges = BaseConfig.Exchanges
    kraken = Exchange('Kraken', exchanges.Kraken.url, exchanges.Kraken.ms_ts)
    buda = Exchange('Buda', exchanges.Buda.url, exchanges.Buda.ms_ts)

    btc = CryptoCurrency('Bitcoin', 'btc')
    eth = CryptoCurrency('Ethereum', 'eth')
    ltc = CryptoCurrency('Litecoin', 'ltc')
    bch = CryptoCurrency('Bitcoin Cash', 'bch')

    try:
        session.add(kraken)
        session.add(buda)

        session.add(btc)
        session.add(eth)
        session.add(ltc)
        session.add(bch)

        session.commit()
        if log:
            _logger.info('Tables correctly created. Initial data inserted')
    except Exception as e:
        if log:
            _logger.error(e)
        session.rollback()

    finally:
        session.close()


if __name__ == '__main__':
    create_tables()
