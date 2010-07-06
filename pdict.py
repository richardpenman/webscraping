#
# Description:
# pdict has a dictionary like interface and a sqlite backend
# It uses pickle to store Python objects and strings, which are then compressed
# Multithreading is supported
#
# Author: Richard Penman (richard@sitescraper.net)
#


from datetime import datetime
import sqlite3
import zlib
import threading
try:
    import cPickle as pickle
except ImportError:
    import pickle



lock = threading.Lock() # need to lock writes between threads
def synchronous(f):
    def call(*args, **kwargs):
        lock.acquire()
        try:
            return f(*args, **kwargs)
        finally:
            lock.release()
    return call


class PersistentDict(object):
    """stores and retrieves persistent data through a dict-like interface
    data is stored compressed on disk using sqlite3 
    """
    
    @synchronous
    def __init__(self, filename=':memory:', compress_level=6, timeout=None):
        """initialize a new PersistentDict with the specified database file.

        filename: where to store sqlite database. Uses in memory by default.
        compress_level: between 1-9 (in my test levels 1-3 produced a 1300kb file in ~7 seconds while 4-9 a 288kb file in ~9 seconds)
        timeout: a timedelta object of how old data can be. By default is set to None to disable.
        """
        self._conn = sqlite3.connect(filename, isolation_level=None, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
        self._conn.text_factory = lambda x: unicode(x, 'utf-8', 'replace')
        sql = """
        CREATE TABLE IF NOT EXISTS config (
            key TEXT NOT NULL PRIMARY KEY UNIQUE,
            value BLOB,
            created timestamp DEFAULT (datetime('now', 'localtime')),
            updated timestamp DEFAULT (datetime('now', 'localtime'))
        );"""
        self._conn.execute(sql)
        self.compress_level = compress_level
        self.timeout = timeout

    
    def __contains__(self, key):
        """check the database to see if a key exists
        """
        row = self._conn.execute("SELECT updated FROM config WHERE key=?;", (key,)).fetchone()
        return row and self.is_fresh(row[0])
            
    def __getitem__(self, key):
        """return the value of the specified key
        """
        row = self._conn.execute("SELECT value, updated FROM config WHERE key=?;", (key,)).fetchone()
        if row:
            if self.is_fresh(row[1]):
                return self.deserialize(row[0])
            else:
                raise KeyError("Key `%s' is stale" % key)
        else:
            raise KeyError("Key `%s' does not exist" % key)
    
    @synchronous
    def __setitem__(self, key, value):
        """set the value of the specified key
        """
        if key in self:
            self._conn.execute("UPDATE config SET value=?, updated=? WHERE key=?;", (self.serialize(value), datetime.now(), key))
        else:
            self._conn.execute("INSERT INTO config (key, value) VALUES(?, ?);", (key, self.serialize(value)))

    @synchronous
    def __delitem__(self, key):
        """remove the specifed value from the database
        """
        self._conn.execute("DELETE FROM config WHERE key=?;", (key,))
        
    def serialize(self, value):
        """convert object to a compressed blog string to save in the db
        """
        return sqlite3.Binary(zlib.compress(pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL), self.compress_level))
    
    def deserialize(self, value):
        """convert compressed string from database back into an object
        """
        return pickle.loads(zlib.decompress(value))

    def keys(self):
        """returns a list containing each key in the database
        """
        return [row[0] for row in self._conn.execute("SELECT key FROM config;").fetchall()]

    def is_fresh(self, t):
        """returns whether this datetime has expired
        """
        return self.timeout is None or datetime.now() - t < self.timeout


if __name__ == '__main__':
    # test performance of compression and verify stored data is correct
    import os
    import time
    key = 'key'
    input = 'abc' * 100000
    for compress_level in range(1, 10):
        print 'Compression:', compress_level
        start = time.time()
        file = 'persistent%d.db' % compress_level
        try:
            os.remove(file)
        except OSError:
            pass
        p = PersistentDict(file, compress_level)
        p[key] = input
        print 'Time: %.2f seconds' % (time.time() - start)
        print 'Size: %d bytes' % os.path.getsize(file)
        print
        assert key in p
        assert input == p[key]
        assert p.keys() == [key]
        del p[key]
        assert p.keys() == []

