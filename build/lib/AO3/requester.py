import threading
import time

import requests
from ratelimit import limits, sleep_and_retry, RateLimitException
import backoff
from random import expovariate
import datetime

RATE = .2
PERIOD = 150


class RateLimitedError(Exception):
    def __init__(self, message, errors=[]):
        super().__init__(message)
        self.errors = errors

class Requester:
    """Requester object"""
    
    def __init__(self, rqtw=-1, timew=60):
        """Limits the request rate to prevent HTTP 429 (rate limiting) responses.
        12 request per minute seems to be the limit.

        Args:
            rqm (int, optional): Maximum requests per time window (-1 -> no limit). Defaults to -1.
            timew (int, optional): Time window (seconds). Defaults to 60.
        """
        
        self._requests = []
        self._rqtw = rqtw
        self._timew = timew
        self._lock = threading.Lock()
        self.total = 0

        self.time_start = time.time()
        
        
        self.waiting = False
        self.wait_condition = threading.Condition()
        
        self._explambda = 0
    
    def setExpLambda(self, value):
        self._explambda = value    
    
    def setRQTW(self, value):
        self._rqtw = value
        
    def setTimeW(self, value):
        self._timew = value

    def backoff_funciton(r):
        out = int(r.headers.get("Retry-After"))
        print(f"Rate limited. Waiting {out} seconds. You may want to adjust the rate limiter.")
        return out

    def request(self, *args, **kwargs):
        """Requests a web page once enough time has passed since the last request
        
        Args:
            session(requests.Session, optional): Session object to request with

        Returns:
            requests.Response: Response object
        """
        if self._explambda>0:
            time.sleep(expovariate(self._explambda))
        
        self.total+=1
        req = self.request_helper(*args, **kwargs)
                
            
        return req
        
    @sleep_and_retry
    @limits(calls=int(PERIOD*RATE), period=PERIOD)
    def check_limit(self):
        ''' Empty function just to check for calls to API '''
        return
    
    
    @backoff.on_predicate(
        backoff.runtime,
        predicate=lambda r: r.status_code == 429,
        value=backoff_funciton,
        jitter=None,
    )
    @sleep_and_retry
    @limits(calls=int(PERIOD*RATE), period=PERIOD)
    def request_helper(self, *args, **kwargs):
        """Requests a web page once enough time has passed since the last request
        
        Args:
            session(requests.Session, optional): Session object to request with

        Returns:
            requests.Response: Response object
        """
        if "session" in kwargs:
            sess = kwargs["session"]
            del kwargs["session"]
            req = sess.request(*args, **kwargs)
        else:
            req = requests.request(*args, **kwargs)
                
            
        return req

requester = Requester()