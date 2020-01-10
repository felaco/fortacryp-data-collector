from datetime import datetime
from typing import Optional

from sqlalchemy import Column, Integer, String, ForeignKey, Boolean, Float, DateTime
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
    trades: AppenderQuery = relationship('Trades', back_populates='currency', lazy='dynamic')

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
    date: datetime = Column(DateTime)
    exchange_id: int = Column(Integer, ForeignKey(BaseConfig.DBNames.exchange + '.id_exchange'))
    currency_id: int = Column(Integer, ForeignKey(BaseConfig.DBNames.currency + '.id_currency'))
    currency: CryptoCurrency = relationship('CryptoCurrency', uselist=False, back_populates='ohlc_data')
    exchange: Exchange = relationship('Exchange', uselist=False, back_populates='ohlc_data')

    def __init__(self, open_price: float,
                 high: float,
                 low: float,
                 close: float,
                 volume: float,
                 date: datetime,
                 exchange_id: Optional[int] = None,
                 currency_id: Optional[int] = None):
        self.open = open_price
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume
        self.date = date
        self.exchange_id = exchange_id
        self.currency_id = currency_id


class Trades(Base):
    __tablename__ = BaseConfig.DBNames.trades

    id_trade: int = Column(Integer, primary_key=True)
    price: float = Column(Float)
    volume: float = Column(Float)
    direction: str = Column(String(10))
    date: datetime = Column(DateTime)
    exchange_id: int = Column(Integer, ForeignKey(BaseConfig.DBNames.exchange + '.id_exchange'))
    currency_id: int = Column(Integer, ForeignKey(BaseConfig.DBNames.currency + '.id_currency'))
    currency: CryptoCurrency = relationship('CryptoCurrency', uselist=False, back_populates='trades')
    exchange: Exchange = relationship('Exchange', uselist=False, back_populates='trades')

    def __init__(self, price: float, volume: float, date: datetime, direction: str):
        self.price = price
        self.volume = volume
        self.date = date
        self.direction = direction
