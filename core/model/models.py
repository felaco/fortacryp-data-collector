from typing import Optional

from sqlalchemy import Column, Integer, String, BigInteger, ForeignKey, Boolean, Float
from sqlalchemy.orm import relationship
from sqlalchemy.orm.dynamic import AppenderQuery

from config import BaseConfig
from core.orm.orm import Base


class Exchange(Base):
    __tablename__ = BaseConfig.DBNames.exchange

    id_exchange: int = Column(Integer, primary_key=True)
    name: str = Column(String(20))
    url: str = Column(String(50))
    milliseconds_timestamp: bool = Column(Boolean)
    ohlc_data: AppenderQuery = relationship('OHLC', back_populates='exchange', lazy='dynamic')
    trades: AppenderQuery = relationship('Trades', back_populates='exchange', lazy='dynamic')

    def __init__(self, name: str, url: str, ms_ts: bool):
        self.name = name
        self.url = url
        self.milliseconds_timestamp = ms_ts


class CryptoCurrency(Base):
    __tablename__ = BaseConfig.DBNames.currency

    id_currency: int = Column(Integer, primary_key=True)
    name: str = Column(String(20))
    mnemonic: str = Column(String(5))
    ohlc_data: AppenderQuery = relationship('OHLC', back_populates='currency', lazy='dynamic')
    trades: AppenderQuery = relationship('Trades', back_populates='exchange', lazy='dynamic')

    def __init__(self, name, mnemonic):
        self.name = name
        self.mnemonic = mnemonic


class OHLC(Base):
    __tablename__ = BaseConfig.DBNames.ohlc

    id_frame: int = Column(Integer, primary_key=True)
    open: float = Column(Float)
    high: float = Column(Float)
    low: float = Column(Float)
    close: float = Column(Float)
    volume: float = Column(Float)
    timestamp: int = Column(Integer)
    exchange_id: int = Column(Integer, ForeignKey(BaseConfig.DBNames.exchange + '.id_exchange'))
    currency_id: int = Column(Integer, ForeignKey(BaseConfig.DBNames.currency + '.id_currency'))
    currency: CryptoCurrency = relationship('CryptoCurrency', uselist=False, back_populates='ohlc_data')
    exchange: Exchange = relationship('Exchange', uselist=False, back_populates='ohlc_data')

    def __init__(self, open_price: float,
                 high: float,
                 low: float,
                 close: float,
                 volume: float,
                 timestamp: int,
                 exchange_id: Optional[int]):
        self.open = open_price
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume
        self.timestamp = timestamp
        self.exchange_id = exchange_id


class Trades(Base):
    __tablename__ = BaseConfig.DBNames.trades

    id_trade: int = Column(Integer, primary_key=True)
    price: float = Column(Float)
    volume: float = Column(Float)
    direction: str = Column(String(10))
    timestamp: int = Column(BigInteger)
    exchange_id: int = Column(Integer, ForeignKey(BaseConfig.DBNames.exchange + '.id_exchange'))
    currency_id: int = Column(Integer, ForeignKey(BaseConfig.DBNames.currency + '.id_currency'))
    currency: CryptoCurrency = relationship('CryptoCurrency', uselist=False, back_populates='ohlc_data')
    exchange: Exchange = relationship('Exchange', uselist=False, back_populates='ohlc_data')

    def __init__(self, price: float, volume: float, timestamp: int, direction: str):
        self.price = price
        self.volume = volume
        self.timestamp = timestamp
        self.direction = direction
