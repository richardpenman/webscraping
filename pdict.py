__doc__ = """
pdict has a dictionary like interface and a sqlite backend
It uses pickle to store Python objects and strings, which are then compressed
Multithreading is supported
"""

import os
import sys
import datetime
import time
import sqlite3
import zlib
import itertools
import threading
import md5
import shutil
import glob
try:
    import cPickle as pickle
except ImportError:
    import pickle

DEFAULT_LIMIT = 1000
DEFAULT_TIMEOUT = 10000



class _PersistentDictPool:
    def __init__(self, filename, max_size=2):
        """Splits cache over multiple sqlite instances to avoid each exceeding the limit

        `filename' of the cache
        `max_size' in GB of the cache before splitting
        """
        #caches = glob.glob(filename + '*')
        #print len(caches)
        #os.path.getsize(f) for f in caches



class PersistentDict:
    """Stores and retrieves persistent data through a dict-like interface
    Data is stored compressed on disk using sqlite3 

    filename: 
        where to store sqlite database. Uses in memory by default.
    compress_level: 
        between 1-9 (in my test levels 1-3 produced a 1300kb file in ~7 seconds while 4-9 a 288kb file in ~9 seconds)
    expires: 
        a timedelta object of how old data can be before expires. By default is set to None to disable.
    timeout: 
        how long should a thread wait for sqlite to be ready (in ms)
    isolation_level: 
        None for autocommit or else 'DEFERRED' / 'IMMEDIATE' / 'EXCLUSIVE'

    >>> filename = 'cache.db'
    >>> cache = PersistentDict(filename)
    >>> url = 'http://google.com/abc'
    >>> html = '<html>abc</html>'
    >>>
    >>> url in cache
    False
    >>> cache[url] = html
    >>> url in cache
    True
    >>> cache[url] == html
    True
    >>> cache.get(url)['value'] == html
    True
    >>> now = datetime.datetime.now()
    >>> cache.meta(url)
    {}
    >>> cache.meta(url, 'meta')
    >>> cache.meta(url)
    'meta'
    >>> del cache[url]
    >>> url in cache
    False
    >>> os.remove(filename)
    """
    def __init__(self, filename='cache.db', compress_level=6, expires=None, timeout=DEFAULT_TIMEOUT, isolation_level=None, disk=False):
        """initialize a new PersistentDict with the specified database file.
        """
        self.filename = filename
        self.compress_level = compress_level
        self.expires = expires
        self.timeout = timeout
        self._conn = sqlite3.connect(filename, timeout=timeout, isolation_level=isolation_level, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
        self._conn.text_factory = lambda x: unicode(x, 'utf-8', 'replace')
        sql = """
        CREATE TABLE IF NOT EXISTS config (
            key TEXT NOT NULL PRIMARY KEY UNIQUE,
            value BLOB,
            meta BLOB,
            status INTEGER,
            updated timestamp DEFAULT (datetime('now', 'localtime'))
        );
        """
        self._conn.execute(sql)
        self._conn.execute("CREATE INDEX IF NOT EXISTS keys ON config (key);")
        if disk:
            self.fscache = FSCache(os.path.dirname(filename))
        else:
            self.fscache = None


    def __copy__(self):
        """make a copy of current cache settings
        """
        return PersistentDict(filename=self.filename, compress_level=self.compress_level, expires=self.expires, timeout=self.timeout)


    def __contains__(self, key):
        """check the database to see if a key exists
        """
        row = self._conn.execute("SELECT updated FROM config WHERE key=?;", (key,)).fetchone()
        return row and self.is_fresh(row[0])
   

    def __iter__(self):
        """iterate each key in the database
        """
        c = self._conn.cursor()
        c.execute("SELECT key FROM config;")
        for row in c:
            yield row[0]


    def __getitem__(self, key):
        """return the value of the specified key or raise KeyError if not found
        """
        row = self._conn.execute("SELECT value, updated FROM config WHERE key=?;", (key,)).fetchone()
        if row:
            if self.is_fresh(row[1]):
                try:
                    if self.fscache:
                        value = self.fscache[key]
                    else:
                        # XXX remove this when migrated
                        raise KeyError()
                except KeyError:
                    value = row[0]
                return self.deserialize(value)
            else:
                raise KeyError("Key `%s' is stale" % key)
        else:
            raise KeyError("Key `%s' does not exist" % key)


    def __delitem__(self, key):
        """remove the specifed value from the database
        """
        self._conn.execute("DELETE FROM config WHERE key=?;", (key,))
        if self.fscache:
            del self.fscache[key]


    def __setitem__(self, key, value):
        """set the value of the specified key
        """
        updated = datetime.datetime.now()
        if self.fscache:
            self._conn.execute("INSERT OR REPLACE INTO config (key, meta, updated) VALUES(?, ?, ?, ?);", (
                key, self.serialize({}), updated)
            )
            self.fscache[key] = self.serialize(value)
        else:
            self._conn.execute("INSERT OR REPLACE INTO config (key, value, meta, updated) VALUES(?, ?, ?, ?);", (
                key, self.serialize(value), self.serialize({}), updated)
            )


    def serialize(self, value):
        """convert object to a compressed pickled string to save in the db
        """
        return sqlite3.Binary(zlib.compress(pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL), self.compress_level))
    
    def deserialize(self, value):
        """convert compressed pickled string from database back into an object
        """
        if value:
            return pickle.loads(zlib.decompress(value))


    def is_fresh(self, t):
        """returns whether this datetime has expired
        """
        return self.expires is None or datetime.datetime.now() - t < self.expires


    def get(self, key, default=None):
        """Get data at key and return default if not defined
        """
        data = default
        if key:
            row = self._conn.execute("SELECT value, meta, updated FROM config WHERE key=?;", (key,)).fetchone()
            if row:
                try:
                    if self.fscache:
                        value = self.fscache[key]
                    else:
                        # XXX remove after migrated
                        raise KeyError()
                except KeyError:
                    value = row[0] 
                data = dict(
                    value=self.deserialize(value),
                    meta=self.deserialize(row[1]),
                    updated=row[2]
                )
        return data


    def meta(self, key, value=None):
        """Get / set meta for this value

        if value is passed then set the meta attribute for this key
        if not then get the existing meta data for this key
        """
        if value is None:
            # want to get meta
            row = self._conn.execute("SELECT meta FROM config WHERE key=?;", (key,)).fetchone()
            if row:
                return self.deserialize(row[0])
            else:
                raise KeyError("Key `%s' does not exist" % key)
        else:
            # want to set meta
            self._conn.execute("UPDATE config SET meta=?, updated=? WHERE key=?;", (self.serialize(value), datetime.datetime.now(), key))


    def clear(self):
        """Clear all cached data
        """
        self._conn.execute("DELETE FROM config;")
        if self.fscache:
            self.fscache.clear()


    def merge(self, db, override=False):
        """Merge this databases content
        override determines whether to override existing keys
        """
        for key in db.keys():
            if override or key not in self:
                self[key] = db[key]



class Queue:
    """Stores queue of outstanding URL's on disk

    >>> filename = 'queue.db'
    >>> queue = Queue(filename)
    >>> keys = [('a', 1), ('b', 2), ('c', 1)]
    >>> queue.push(keys) # add new keys
    >>> len(queue)
    3
    >>> queue.push(keys) # trying adding duplicate keys
    >>> len(queue)
    3
    >>> queue.clear(keys=['a'])
    1
    >>> queue.pull(limit=1)
    [u'b']
    >>> queue.clear() # remove all queue
    1
    >>> os.remove(filename)
    """
    size = None # track the size of the queue
    counter = itertools.count().next # counter gives a unique status for each pull()

    def __init__(self, filename, timeout=DEFAULT_TIMEOUT, isolation_level=None):
        self._conn = sqlite3.connect(filename, timeout=timeout, isolation_level=isolation_level, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
        self._conn.text_factory = lambda x: unicode(x, 'utf-8', 'replace')
        sql = """
        CREATE TABLE IF NOT EXISTS queue (
            key TEXT NOT NULL PRIMARY KEY UNIQUE,
            status INTEGER,
            priority INTEGER
        );
        """
        self._conn.execute(sql)
        self._conn.execute("CREATE INDEX IF NOT EXISTS priorities ON queue (priority);")
        if Queue.size is None:
            self._update_size()


    def __len__(self):
        """Get number of records queued
        """
        return Queue.size
            
    def _update_size(self):
        """Calculate the number of records queued
        """
        row = self._conn.execute("SELECT count(*) FROM queue WHERE status=?;", (False,)).fetchone()
        Queue.size = row[0]


    def push(self, key_map):
        """Add these keys to the queue
        Will not insert if key already exists.

        key_map:
            a list of (key, priority) tuples
        """
        if key_map:
            c = self._conn.cursor()
            c.execute("BEGIN TRANSACTION")
            c.executemany("INSERT OR IGNORE INTO queue (key, priority, status) VALUES(?, ?, ?);", [(key, priority, False) for key, priority in key_map])
            c.execute("END TRANSACTION")
            self._update_size()


    def pull(self, limit=DEFAULT_LIMIT):
        """Get queued keys up to limit
        """
        status = Queue.counter()
        self._conn.execute('UPDATE queue SET status=? WHERE key in (SELECT key FROM queue WHERE status=? ORDER BY priority DESC LIMIT ?);', (status, False, limit))
        rows = self._conn.execute('SELECT key FROM queue WHERE status=?', (status,))
        keys = [row[0] for row in rows]
        Queue.size -= len(keys)
        return keys


    def clear(self, keys=None):
        """Remove keys from queue.
        If keys is None remove all.

        Returns the number of keys removed
        """
        prev_size = len(self)
        c = self._conn.cursor()
        if keys:
            c.execute("BEGIN TRANSACTION")
            c.executemany("DELETE FROM queue WHERE key=?;", [(key,) for key in keys])
            c.execute("END TRANSACTION")
            self._update_size()
        else:
            c.execute("DELETE FROM queue;")
            Queue.size = 0
        return prev_size - len(self)



class FSCache:
    """
    Dictionary interface that stores cached 
    values in the file system rather than in memory.
    The file path is formed from an md5 hash of the key.

    folder:
        the root level folder for the cache

    >>> fscache = FSCache('.')
    >>> url = 'http://google.com/abc'
    >>> html = '<html>abc</html>'
    >>> url in fscache
    False
    >>> fscache[url] = html
    >>> url in fscache
    True
    >>> fscache.get(url) == html
    True
    >>> fscache.get(html) == ''
    True
    >>> fscache.clear()
    """
    PARENT_DIR = 'fscache'
    FILE_NAME = 'index.html'

    def __init__(self, folder):
        self.folder = os.path.join(folder, FSCache.PARENT_DIR)

    
    def __contains__(self, key):
        """Does data for this key exist
        """
        return os.path.exists(self._key_path(key))


    def __getitem__(self, key):
        path = self._key_path(key)
        try:
            fp = open(path, 'rb')
        except IOError:
            # key does not exist
            raise KeyError('%s does not exist' % key)
        else:
            # get value in key
            return fp.read()


    def __setitem__(self, key, value):
        """Save value at this key to this value
        """
        path = self._key_path(key)
        folder = os.path.dirname(path)
        if not os.path.exists(folder):
            os.makedirs(folder)
        open(path, 'wb').write(value)


    def __delitem__(self, key):
        """Remove the value at this key and any empty parent sub-directories
        """
        path = self._key_path(key)
        try:
            os.remove(path)
            os.removedirs(os.path.dirname(path))
        except OSError:
            pass

    def _key_path(self, key):
        """The fils system path for this key
        """
        # create unique hash for this key
        try:
            key = key.encode('utf-8')
        except UnicodeDecodeError:
            pass
        h = md5.md5(key).hexdigest()
        # create file system path
        path = os.path.join(self.folder, os.path.sep.join(h), FSCache.FILE_NAME)
        return path


    def get(self, key, default=''):
        """Get data at this key and return default if does not exist
        """
        try:
            value = self[key]
        except KeyError:
            value = default
        return value


    def clear(self):
        """Remove all the cached values
        """
        if os.path.exists(self.folder):
            shutil.rmtree(self.folder)



if __name__ == '__main__':
    import tempfile
    import webbrowser
    from optparse import OptionParser
    parser = OptionParser(usage='usage: %prog [options] <cache file>')
    parser.add_option('-k', '--key', dest='key', help='The key to use')
    parser.add_option('-v', '--value', dest='value', help='The value to store')
    parser.add_option('-b', '--browser', action='store_true', dest='browser', default=False, help='View content of this key in a web browser')
    parser.add_option('-c', '--clear', action='store_true', dest='clear', default=False, help='Clear all data for this cache')
    options, args = parser.parse_args()
    if not args:
        parser.error('Must specify the cache file')
    cache = PersistentDict(args[0])

    if options.value:
        # store thie value 
        if options.key:
            cache[options.key] = options.value
        else:
            parser.error('Must specify the key')
    elif options.browser:
        if options.key:
            value = cache[options.key]
            filename = tempfile.NamedTemporaryFile().name
            fp = open(filename, 'w')
            fp.write(value)
            fp.flush()
            webbrowser.open(filename)
        else:
            parser.error('Must specify the key')
    elif options.key:
        print cache[options.key]
    elif options.clear:
        if raw_input('Really? Clear the cache? (y/n) ') == 'y':
            cache.clear()
            print 'cleared'
    else:
        parser.error('No options selected')
