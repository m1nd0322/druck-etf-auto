import time
from collections import deque

class RateLimiter:
    def __init__(self, max_per_sec: int = 5):
        self.max_per_sec = int(max_per_sec)
        self.calls = deque()

    def wait(self):
        now = time.time()
        while self.calls and now - self.calls[0] > 1:
            self.calls.popleft()
        if len(self.calls) >= self.max_per_sec:
            sleep_time = 1 - (now - self.calls[0])
            if sleep_time > 0:
                time.sleep(sleep_time)
        self.calls.append(time.time())
