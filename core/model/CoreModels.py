from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class OhlcFrame:
    open: float
    high: float
    low: float
    close: float
    date: datetime
    volume: Optional[float]

    def to_dict(self):
        return {
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'date': self.date,
            'volume': self.volume
        }


@dataclass
class TradesEntry:
    price: float
    volume: float
    direction: str
    date: datetime

    def to_dict(self):
        return {
            'price': self.price,
            'volume': self.volume,
            'direction': self.direction,
            'date': self.date
        }
