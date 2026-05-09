"""
Flink ValueState serialisation for LastLocationState.

Uses struct.pack binary encoding (10 bytes) instead of JSON or pickle to
minimise per-event serialisation overhead at 25K tps on the RocksDB backend.

Layout: [latitude: double (8 bytes)] [longitude: double (8 bytes)] [event_time_ms: long long (8 bytes)]
Total: 24 bytes per state entry.
"""

import struct
from dataclasses import dataclass
from typing import Optional

_FMT = ">ddq"   # big-endian: double, double, signed long long
_SIZE = struct.calcsize(_FMT)   # 24 bytes


@dataclass
class LastLocationState:
    latitude: float
    longitude: float
    event_time_ms: int

    def to_bytes(self) -> bytes:
        return struct.pack(_FMT, self.latitude, self.longitude, self.event_time_ms)

    @staticmethod
    def from_bytes(data: bytes) -> "LastLocationState":
        if len(data) != _SIZE:
            raise ValueError(f"Expected {_SIZE} bytes, got {len(data)}")
        lat, lon, ts = struct.unpack(_FMT, data)
        return LastLocationState(latitude=lat, longitude=lon, event_time_ms=ts)

    @staticmethod
    def size() -> int:
        return _SIZE
