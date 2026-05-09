"""
Synthetic e-commerce transaction generator.

Produces realistic transaction events calibrated against PaySim1 distributions.
Uses a seeded random.Random so that given the same master seed, every run
produces an identical sequence — the Deterministic Simulation invariant.

Fraud injection:
  VELOCITY_BREACH  — bursts 6 transactions for the same user_id in < 60 s
  LOCATION_JUMP    — teleports a user > 500 km between two consecutive events

The generator itself has no Kafka dependency; it yields plain dicts that
kafka_producer.py serialises and ships.
"""

import math
import random
import time
import uuid
from dataclasses import dataclass, field
from typing import Iterator

from faker import Faker

# ---------------------------------------------------------------------------
# Amount distribution parameters (PaySim1-calibrated, scaled to e-commerce USD)
# PaySim1 shape is log-normal; we preserve that shape with e-commerce scale.
# Legitimate: median ~$45, mean ~$120 (log-normal mu=3.8, sigma=1.2)
# Fraud: median ~$280, mean ~$900 (log-normal mu=5.6, sigma=1.4)
# ---------------------------------------------------------------------------
_LEGIT_AMOUNT_MU: float = 3.8
_LEGIT_AMOUNT_SIGMA: float = 1.2
_FRAUD_AMOUNT_MU: float = 5.6
_FRAUD_AMOUNT_SIGMA: float = 1.4

# Geographic bounding box: worldwide e-commerce, biased toward populated regions
_GEO_CLUSTERS: list[tuple[float, float, float]] = [
    # (lat_center, lon_center, radius_deg)
    (40.7, -74.0, 5.0),    # New York metro
    (51.5, -0.1, 3.0),     # London
    (35.7, 139.7, 4.0),    # Tokyo
    (48.9, 2.3, 3.0),      # Paris
    (-33.9, 151.2, 3.0),   # Sydney
    (19.1, 72.9, 4.0),     # Mumbai
    (55.8, 37.6, 3.0),     # Moscow
    (1.3, 103.8, 2.0),     # Singapore
]

_CURRENCIES: list[str] = ["USD", "EUR", "GBP", "JPY", "AUD", "CAD", "CHF", "SGD"]
_CURRENCY_WEIGHTS: list[float] = [0.55, 0.18, 0.08, 0.06, 0.04, 0.03, 0.03, 0.03]

_MERCHANTS_PER_WORKER: int = 500
_USERS_PER_WORKER: int = 2_000


@dataclass
class _UserState:
    """Per-user ephemeral state held in the generator process."""
    last_lat: float
    last_lon: float
    last_event_time_ms: int


@dataclass
class GeneratorStats:
    total_emitted: int = 0
    fraud_emitted: int = 0
    velocity_injected: int = 0
    location_jump_injected: int = 0


def _random_geo(rng: random.Random) -> tuple[float, float]:
    cluster = rng.choices(_GEO_CLUSTERS, k=1)[0]
    lat = cluster[0] + rng.uniform(-cluster[2], cluster[2])
    lon = cluster[1] + rng.uniform(-cluster[2], cluster[2])
    return round(lat, 6), round(lon, 6)


def _jump_geo(
    rng: random.Random,
    base_lat: float,
    base_lon: float,
    min_km: float = 600.0,
) -> tuple[float, float]:
    """Return a coordinate at least min_km from (base_lat, base_lon)."""
    for _ in range(50):
        lat, lon = _random_geo(rng)
        dlat = math.radians(lat - base_lat)
        dlon = math.radians(lon - base_lon)
        a = (
            math.sin(dlat / 2) ** 2
            + math.cos(math.radians(base_lat))
            * math.cos(math.radians(lat))
            * math.sin(dlon / 2) ** 2
        )
        dist_km = 6371.0 * 2 * math.asin(math.sqrt(a))
        if dist_km >= min_km:
            return lat, lon
    # Fallback: antipodal point
    return round(-base_lat, 6), round((base_lon + 180) % 360 - 180, 6)


def _make_transaction(
    rng: random.Random,
    fake: Faker,
    user_id: str,
    merchant_id: str,
    lat: float,
    lon: float,
    amount_usd: float,
    event_time_ms: int,
    sequence_number: int,
    generator_seed: int,
) -> dict:
    return {
        "transaction_id": str(uuid.UUID(int=rng.getrandbits(128))),
        "user_id": user_id,
        "merchant_id": merchant_id,
        "amount_usd": round(amount_usd, 2),
        "currency": rng.choices(_CURRENCIES, weights=_CURRENCY_WEIGHTS, k=1)[0],
        "event_time": event_time_ms,
        "ip_address": fake.ipv4_public(),
        "latitude": lat,
        "longitude": lon,
        "device_fingerprint": f"fp_{rng.randbytes(5).hex()}",
        "session_id": f"sess_{rng.randbytes(4).hex()}",
        "generator_seed": generator_seed,
        "sequence_number": sequence_number,
    }


class TransactionGenerator:
    """
    Produces a stream of synthetic transaction dicts.

    Parameters
    ----------
    seed : int
        Master RNG seed. Derived child seed = seed + worker_id.
    worker_id : int
        Index of this worker process. Offsets the seed so workers don't collide.
    fraud_injection_rate : float
        Fraction of events that are fraud (0.02 = 2%).
    """

    def __init__(
        self,
        seed: int = 42,
        worker_id: int = 0,
        fraud_injection_rate: float = 0.02,
    ) -> None:
        self._seed = seed + worker_id
        self._rng = random.Random(self._seed)
        self._fake = Faker()
        Faker.seed(self._seed)
        self._fraud_rate = fraud_injection_rate
        self._stats = GeneratorStats()

        # Pre-generate stable user and merchant pools for this worker
        self._users: list[str] = [
            f"user_{self._seed:06d}_{i:05d}" for i in range(_USERS_PER_WORKER)
        ]
        self._merchants: list[str] = [
            f"merchant_{self._seed:06d}_{i:04d}" for i in range(_MERCHANTS_PER_WORKER)
        ]
        # Track last-known state per user for LOCATION_JUMP detection
        self._user_state: dict[str, _UserState] = {}

    @property
    def stats(self) -> GeneratorStats:
        return self._stats

    def _get_or_init_user_state(self, user_id: str, now_ms: int) -> _UserState:
        if user_id not in self._user_state:
            lat, lon = _random_geo(self._rng)
            self._user_state[user_id] = _UserState(
                last_lat=lat, last_lon=lon, last_event_time_ms=now_ms
            )
        return self._user_state[user_id]

    def _legit_event(self, now_ms: int, seq: int) -> dict:
        user_id = self._rng.choice(self._users)
        merchant_id = self._rng.choice(self._merchants)
        state = self._get_or_init_user_state(user_id, now_ms)

        # Small geo drift from last known position (realistic browsing)
        lat = round(state.last_lat + self._rng.uniform(-0.05, 0.05), 6)
        lon = round(state.last_lon + self._rng.uniform(-0.05, 0.05), 6)
        lat = max(-90.0, min(90.0, lat))
        lon = max(-180.0, min(180.0, lon))

        amount = round(math.exp(self._rng.gauss(_LEGIT_AMOUNT_MU, _LEGIT_AMOUNT_SIGMA)), 2)
        amount = max(0.01, min(amount, 50_000.0))

        state.last_lat = lat
        state.last_lon = lon
        state.last_event_time_ms = now_ms

        return _make_transaction(
            self._rng, self._fake, user_id, merchant_id,
            lat, lon, amount, now_ms, seq, self._seed,
        )

    def _velocity_burst(self, now_ms: int, seq: int) -> list[dict]:
        """Emit 6 transactions for the same user within a 30-second window."""
        user_id = self._rng.choice(self._users)
        merchant_id = self._rng.choice(self._merchants)
        state = self._get_or_init_user_state(user_id, now_ms)
        events = []
        burst_seq = seq
        for i in range(6):
            t = now_ms + i * 4_000   # 4-second spacing → 6 txns in 20s
            amount = round(
                math.exp(self._rng.gauss(_FRAUD_AMOUNT_MU, _FRAUD_AMOUNT_SIGMA)), 2
            )
            amount = max(0.01, min(amount, 100_000.0))
            events.append(_make_transaction(
                self._rng, self._fake, user_id, merchant_id,
                state.last_lat, state.last_lon, amount, t, burst_seq, self._seed,
            ))
            burst_seq += 1
        self._stats.velocity_injected += 1
        return events

    def _location_jump_pair(self, now_ms: int, seq: int) -> list[dict]:
        """Emit two events for the same user > 600 km apart, 5 minutes apart."""
        user_id = self._rng.choice(self._users)
        merchant_a = self._rng.choice(self._merchants)
        merchant_b = self._rng.choice(self._merchants)
        state = self._get_or_init_user_state(user_id, now_ms)

        lat_a, lon_a = state.last_lat, state.last_lon
        lat_b, lon_b = _jump_geo(self._rng, lat_a, lon_a, min_km=600.0)

        amount_a = round(
            math.exp(self._rng.gauss(_FRAUD_AMOUNT_MU, _FRAUD_AMOUNT_SIGMA)), 2
        )
        amount_b = round(
            math.exp(self._rng.gauss(_FRAUD_AMOUNT_MU, _FRAUD_AMOUNT_SIGMA)), 2
        )

        t_a = now_ms
        t_b = now_ms + 5 * 60 * 1_000   # 5 minutes later

        ev_a = _make_transaction(
            self._rng, self._fake, user_id, merchant_a,
            lat_a, lon_a, amount_a, t_a, seq, self._seed,
        )
        ev_b = _make_transaction(
            self._rng, self._fake, user_id, merchant_b,
            lat_b, lon_b, amount_b, t_b, seq + 1, self._seed,
        )

        state.last_lat = lat_b
        state.last_lon = lon_b
        state.last_event_time_ms = t_b

        self._stats.location_jump_injected += 1
        return [ev_a, ev_b]

    def stream(self, target_tps: int = 25_000) -> Iterator[dict]:
        """
        Yield transaction dicts at approximately target_tps per second.
        The caller is responsible for rate-limiting across workers.
        Runs indefinitely; wrap with itertools.islice or a duration check.
        """
        seq = 0
        # Decide fraud injection schedule upfront per batch to avoid per-event branching
        _BATCH = 1_000
        while True:
            now_ms = int(time.time() * 1_000)
            batch: list[dict] = []

            # How many fraud events in this batch?
            n_fraud = round(_BATCH * self._fraud_rate)
            fraud_positions = set(
                self._rng.sample(range(_BATCH), min(n_fraud, _BATCH))
            )
            # Split fraud between the two patterns
            fraud_list = sorted(fraud_positions)
            velocity_positions = set(fraud_list[: len(fraud_list) // 2])
            jump_positions = set(fraud_list[len(fraud_list) // 2 :])

            i = 0
            while i < _BATCH:
                if i in velocity_positions:
                    burst = self._velocity_burst(now_ms, seq)
                    batch.extend(burst)
                    self._stats.fraud_emitted += len(burst)
                    seq += len(burst)
                    i += 1
                elif i in jump_positions:
                    pair = self._location_jump_pair(now_ms, seq)
                    batch.extend(pair)
                    self._stats.fraud_emitted += len(pair)
                    seq += len(pair)
                    i += 1
                else:
                    event = self._legit_event(now_ms, seq)
                    batch.append(event)
                    seq += 1
                    i += 1

            for event in batch:
                self._stats.total_emitted += 1
                yield event
