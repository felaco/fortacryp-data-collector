from dataclasses import dataclass
from typing import Optional


@dataclass
class OhlcFrame:
    open: float
    high: float
    low: float
    close: float
    timestamp: int
    volume: Optional[float]

    def to_dict(self):
        return {
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'timestamp': self.timestamp,
            'volume': self.volume
        }
