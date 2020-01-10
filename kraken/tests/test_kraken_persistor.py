from datetime import datetime
from typing import List

import pytest
from sqlalchemy.orm import Session
from sqlalchemy_utils.functions import drop_database

from core.Enums import Mnemonic
from core.model.CoreModels import OhlcFrame, TradesEntry
from core.model.models import OHLC, Trades
from core.orm.createTables import create_tables
from core.orm.orm import Base, engine, connection, session as session_maker
from kraken.KrakenPersistors import KrakenPersistor


def setup_module():
    create_tables(log=False)
    i = 0


def teardown_module():
    Base.metadata.drop_all(engine)
    connection.close()

    # if some test fails and doesnt close its session, the db may not de destroyed
    # so i force close all sessions
    session = session_maker()
    session.close_all()

    engine.dispose()
    drop_database(engine.url)
    pass


# both list must have the same members in the same order. Is not intented to be optimal, just a helper
def are_list_equals_helper(l1: list, l2: list) -> bool:
    for el1, el2 in zip(l1, l2):
        if el1 != el2:
            return False

    return len(l1) == len(l2)


@pytest.fixture
def instantiate_persistor():
    yield KrakenPersistor()
    session: Session = session_maker()
    session.query(OHLC).delete()
    session.query(Trades).delete()
    session.commit()
    session.close()


@pytest.fixture
def ohlc_data():
    l: List[OhlcFrame] = []
    d = datetime(2019, 1, 2, 12)

    l.append(OhlcFrame(100, 120, 80, 101, d, 10))

    d = d.replace(hour=13)
    l.append(OhlcFrame(103, 105, 99, 100, d, 11))

    d = d.replace(hour=14)
    l.append(OhlcFrame(101, 108, 91, 103, d, 12))

    return l


@pytest.fixture
def trades_data():
    l: List[TradesEntry] = []

    # first ohlc
    d = datetime(2019, 1, 2, 11, minute=40)
    l.append(TradesEntry(100, 3, 'buy', d))

    d = d.replace(minute=45)
    l.append(TradesEntry(120, 3, 'buy', d))

    d = d.replace(minute=50)
    l.append(TradesEntry(80, 2, 'buy', d))

    d = d.replace(minute=51)
    l.append(TradesEntry(101, 2, 'buy', d))
    # end first ohlc
    # second ohlc

    d = d.replace(hour=12, minute=10)
    l.append(TradesEntry(103, 3, 'buy', d))

    d = d.replace(minute=15)
    l.append(TradesEntry(105, 3, 'buy', d))

    d = d.replace(minute=30)
    l.append(TradesEntry(99, 3, 'buy', d))

    d = d.replace(minute=31)
    l.append(TradesEntry(100, 2, 'buy', d))
    # end first ohlc
    # third ohlc

    d = d.replace(hour=13, minute=10)
    l.append(TradesEntry(101, 3, 'buy', d))

    d = d.replace(minute=11)
    l.append(TradesEntry(108, 3, 'buy', d))

    d = d.replace(minute=12)
    l.append(TradesEntry(91, 3, 'buy', d))

    d = d.replace(minute=13)
    l.append(TradesEntry(103, 3, 'buy', d))

    return l


def clear_table(table):
    session: Session = session_maker()
    session.query(table).delete()
    session.commit()
    session.close()


def test_empty_last_ohlc(instantiate_persistor: KrakenPersistor):
    ohlc = instantiate_persistor.get_newest_ohlc(Mnemonic.BTC)
    assert ohlc is None


def test_insert_ohlc_frames(instantiate_persistor: KrakenPersistor, ohlc_data: List[OhlcFrame]):
    instantiate_persistor.persist_ohlc(ohlc_data, Mnemonic.BTC)
    session: Session = session_maker()
    ohlc = session.query(OHLC).all()
    session.close()
    assert len(ohlc) == 3


def test_generate_ohlc(instantiate_persistor: KrakenPersistor, trades_data: List[TradesEntry],
                       ohlc_data: List[OhlcFrame]):
    ohlc_from_trades = instantiate_persistor.trades_to_ohlc(trades_data)
    assert are_list_equals_helper(ohlc_from_trades, ohlc_data)


def test_merge_ohlc_frames(instantiate_persistor: KrakenPersistor, ohlc_data: List[OhlcFrame]):
    # when the table is empty, should insert the new data as it is
    to_update, new = instantiate_persistor.merge_ohlc_with_last(ohlc_data, Mnemonic.BTC)
    assert len(new) == 3
    assert are_list_equals_helper(new, ohlc_data)
    assert to_update is None

    kraken_id = instantiate_persistor.kraken_id

    ohlc = OHLC(50, 51, 49, 50, volume=3, date=ohlc_data[0].date, exchange_id=kraken_id, currency_id=1)
    session: Session = session_maker()
    session.add(ohlc)
    session.commit()
    session.close()

    # when the last frame collides with some part of the new data, should update the last one and
    # remove the collisions from ohlc_data. The ohlc values should update based on the merge of the last frame
    to_update, new = instantiate_persistor.merge_ohlc_with_last(ohlc_data, Mnemonic.BTC)
    assert to_update is not None
    assert to_update.open == 50 and to_update.high == 120 and to_update.low == 49 and to_update.close == 101
    assert to_update.volume == 13
    assert len(new) == 2

    # Data inserted from other currencies should not affect the new data
    to_update, new = instantiate_persistor.merge_ohlc_with_last(ohlc_data, Mnemonic.ETH)
    assert to_update is None
    assert len(new) == 3
    assert are_list_equals_helper(ohlc_data, new)


def test_insert_ohlc_with_no_empty_db(instantiate_persistor: KrakenPersistor, ohlc_data: List[OhlcFrame]):
    ohlc = OHLC(50, 55, 49, 52, 10, ohlc_data[0].date,
                instantiate_persistor.kraken_id, instantiate_persistor.nemo_index[Mnemonic.BTC.value])

    first = ohlc_data[0]
    session = session_maker()
    session.add(ohlc)
    session.commit()

    instantiate_persistor.persist_ohlc(ohlc_data, Mnemonic.BTC)
    oldest_ohlc: OHLC = session.query(OHLC).order_by(OHLC.date.asc()).first()
    all_ohlc: List[OHLC] = session.query(OHLC).all()

    assert oldest_ohlc is not None
    assert oldest_ohlc.open == 50 and oldest_ohlc.low == 49 and oldest_ohlc.high == first.high
    assert oldest_ohlc.close == first.close
    assert oldest_ohlc.volume == first.volume + 10
    assert len(all_ohlc) == 3

    # persisting with another nemo, should not be affected by the above results
    instantiate_persistor.persist_ohlc(ohlc_data, Mnemonic.ETH)
    eth_id = instantiate_persistor.nemo_index[Mnemonic.ETH.value]

    # dont need to filter by exchange since im not testing that
    all_ohlc = session.query(OHLC).filter(OHLC.currency_id == eth_id).all()
    oldest_ohlc = all_ohlc[0]

    assert len(all_ohlc) == 3
    assert oldest_ohlc.open == first.open and oldest_ohlc.high == first.high and oldest_ohlc.low == first.low
    assert oldest_ohlc.close == first.close and oldest_ohlc.volume == first.volume

    session.close()
