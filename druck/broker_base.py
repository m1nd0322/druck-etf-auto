from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Dict

class Broker(ABC):
    @abstractmethod
    def connect(self) -> None: ...

    @abstractmethod
    def get_positions(self) -> Dict[str, int]: ...

    @abstractmethod
    def get_cash(self) -> float: ...

    @abstractmethod
    def get_last_price(self, ticker: str) -> float: ...

    @abstractmethod
    def place_order(self, ticker: str, qty: int, side: str, order_type: str = "MKT") -> None: ...
