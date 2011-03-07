__doc__ = """
Description: High level abstract datatypes
Website: http://code.google.com/p/webscraping/
License: LGPL
"""


from datetime import datetime, timedelta
from collections import defaultdict, deque
try:
    import hashlib
except ImportError:
    import md5 as hashlib



class Bag(dict):
    """Dictionary object with attribute like access
    """
    def __init__(self, *args, **kwargs):
        dict.__init__(self, *args, **kwargs)

    def __getattr__(self, name):
        return self.get(name)

    def __setattr__(self, name, value):
        self[name] = value


class HashDict:
    """For storing keys with large amounts of data where don't need need original value
    """
    def __init__(self, default_factory=str):
        self.d = defaultdict(default_factory)

    def __len__(self):
        return self.d.__len__()

    def __contains__(self, name):
        return self.d.__contains__(self.get_hash(name))

    def __getitem__(self, name):
        return self.d.__getitem__(self.get_hash(name))

    def __setitem__(self, name, value):
        return self.d.__setitem__(self.get_hash(name), value)

    def add(self, *names):
        for name in names:
            self[name]

    def get(self, name, default=None):
        return self.d.get(self.get_hash(name), default)

    def get_hash(self, value):
        return hashlib.md5(str(value)).hexdigest()



class ExpireCounter:
    """Tracks how many events were added in the preceding time period
    """
    def __init__(self, timeout=timedelta(seconds=1)):
        self.timeout = timeout
        self.events = deque()

    def add(self):
        """Add event time
        """
        self.events.append(datetime.now())

    def __len__(self):
        """Return number of active events
        """
        self.expire()
        return len(self.events)

    def expire(self):
        """Remove any expired events
        """
        now = datetime.now()
        try:
            while self.events[0] + self.timeout < now:
                self.events.popleft()
        except IndexError:
            pass

