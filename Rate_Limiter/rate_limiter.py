import time
import threading
from typing import Dict, Tuple, TypedDict
from collections import deque

Key = Tuple[str, str]
Decision = Tuple[bool, int, int]


class StatsDict(TypedDict):
    strategy: str
    total_allowed: int
    total_rejected: int
    active_keys: int


class Bucket:
    def __init__(self, tokens: float, last: float):
        self.tokens = tokens
        self.last = last


class RateLimiter:
    def __init__(self, strategy: str = "policy_a"):
        if strategy not in ("policy_a", "policy_b"):
            raise ValueError(f"Unknown strategy: {strategy}")

        self.strategy = strategy
        self._lock = threading.Lock()

        # Generic per-key state store. You may choose any internal representation
        # that preserves the required behavior and return contract.
        self._state: Dict[Key, object] = {}

        self._total_allowed = 0
        self._total_rejected = 0

    def check(
        self,
        client_id: str,
        endpoint: str,
        limit: int,
        window_ms: int,
        capacity: int,
        refill_rate: float,
    ) -> Decision:
        """
        Run the configured policy and return (allowed, remaining, reset_after_ms).

        Policy A uses: limit, window_ms
        Policy B uses: capacity, refill_rate

        remaining:
            Remaining capacity after the current decision.

        reset_after_ms:
            Milliseconds until capacity becomes available again.
        """
        with self._lock:
            if self.strategy == "policy_a":
                result = self.policy_a_limit(client_id, endpoint, limit, window_ms)
            else:
                result = self.policy_b_limit(client_id, endpoint, capacity, refill_rate)

            if result[0]:
                self._total_allowed += 1
            else:
                self._total_rejected += 1

            return result

    def policy_a_limit(
        self,
        client_id: str,
        endpoint: str,
        limit: int,
        window_ms: int,
    ) -> Decision:
        """
        Policy A:
        For each (client_id, endpoint), allow at most `limit` requests during
        the most recent `window_ms` milliseconds.
        output format: (allowed, remaining, reset_after_ms)

        """
        now = time.monotonic() * 1000
        key = (client_id, endpoint)
        
        if key not in self._state:
            self._state[key] = deque()
        q = self._state[key]
        while q and (now - q[0]) >= window_ms:
            q.popleft()
        if len(q) >= limit:
            if limit <= 0:
                return (False, 0, 0) #for the case when limit = 0    
            return (False, 0, int(q[0] + window_ms - now))
        else:
            q.append(now)
            return (True, limit - len(q), int(q[0] + window_ms - now))

    def policy_b_limit(
        self,
        client_id: str,
        endpoint: str,
        capacity: int,
        refill_rate: float,
    ) -> Decision:
        """
        Policy B:
        For each (client_id, endpoint), maintain a request budget with maximum
        size `capacity`. The budget increases over time at `refill_rate` units
        per second, up to `capacity`. Each accepted request consumes 1 unit.
        """
        now = time.monotonic() * 1000
        key = (client_id, endpoint)

        if key not in self._state:
            self._state[key] = Bucket(tokens = capacity, last = now)
 

        b = self._state[key]
        b.tokens += (now - b.last) * refill_rate / 1000
        b.tokens = min(b.tokens, capacity)
        b.last = now

        if b.tokens >= 1:
            b.tokens -= 1
            return (True, int(b.tokens), 0)
        else:
            if refill_rate == 0 or capacity < 1:
                return (False, 0, 0)
            return (False, 0, int((1 - b.tokens)*1000 / refill_rate) )

 

    def stats(self) -> StatsDict:
        """
        Return aggregate limiter statistics.

        active_keys counts tracked (client_id, endpoint) entries for the active
        policy state.
        """
        with self._lock:
            return {
                "strategy": self.strategy,
                "total_allowed": self._total_allowed,
                "total_rejected": self._total_rejected,
                "active_keys": len(self._state),
            }
