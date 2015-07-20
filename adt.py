__doc__ = 'High level abstract datatypes'

from datetime import datetime, timedelta
from collections import defaultdict, deque
try:
    import pybloom
except ImportError:
    pass


class Bag(dict):
    """Dictionary object with attribute like access

    >>> b = Bag()
    >>> b.name = 'company'
    >>> b.name
    'company'
    >>> b.address
    """
    def __init__(self, *args, **kwargs):
        dict.__init__(self, *args, **kwargs)

    def __getattr__(self, name):
        return self.get(name)

    def __setattr__(self, name, value):
        self[name] = value


class HashDict:
    """For storing large quantities of keys where don't need the original value of the key
    Instead each key is hashed and hashes are compared for equality

    >>> hd = HashDict()
    >>> url = 'http://webscraping.com'
    >>> hd[url] = True
    >>> url in hd
    True
    >>> 'other url' in hd
    False
    >>> len(hd)
    1
    """
    def __init__(self, default_factory=str):
        self.d = defaultdict(default_factory)

    def __len__(self):
        """How many keys are stored in the HashDict
        """
        return self.d.__len__()

    def __contains__(self, name):
        return self.d.__contains__(self.get_hash(name))

    def __getitem__(self, name):
        return self.d.__getitem__(self.get_hash(name))

    def __setitem__(self, name, value):
        return self.d.__setitem__(self.get_hash(name), value)

    def add(self, name):
        self[name] = True

    def get(self, name, default=None):
        """Get the value at this key

        Returns default if key does not exist
        """
        return self.d.get(self.get_hash(name), default)

    def get_hash(self, value):
        """get the hash value of this value
        """
        return hash(value)


class Bloom:
    """A bloom filter is a space efficient way to tell if an element is in a set.
    False positive are possible - set by err rate - but false negatives are not.
    """
    def __init__(self, start_items=10000, err_rate=0.0001):
        self.bloom = pybloom.ScalableBloomFilter(10000, err, 4)

    def __contains__(self, key):
        return key in self.bloom

    def add(self, key):
        return self.bloom.add(key)
