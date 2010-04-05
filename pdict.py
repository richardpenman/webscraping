#
#
#

import sqlite3 as db
import zlib
import threading
try:
    import cPickle as pickle
except ImportError:
    import pickle



class PersistentDict(dict):
    """stores and retrieves persistent data through a dict-like interface
    data is stored compressed on disk using sqlite3 
    """
    lock = threading.Lock() # need to lock writes between threads

    def __init__(self, filename, compress_level=6):
        """initialize a new PersistentDict with the specified db file.

        filename: where to store sqlite database
        compress_level: between 1-9 - in my test levels 1-3 produced a 1300kb file in ~7 seconds while 4-9 a 288kb file in ~9 seconds
        """
        self._conn = db.connect(filename)
        sql = """
        CREATE TABLE IF NOT EXISTS config (
            key TEXT NOT NULL PRIMARY KEY UNIQUE,
            value BLOB
        );"""
        self._cursor = self._conn.cursor()
        self._cursor.execute(sql)
        self._conn.commit()
        self.compress_level = compress_level

    
    def __contains__(self, key):
        """check the database to see if a key exists
        """
        self._cursor.execute("SELECT COUNT(key) FROM config WHERE key=?;", (key,))
        return int(self._cursor.fetchone()[0]) > 0
            
    def __getitem__(self, key):
        """return the value of the specified key
        """
        self._cursor.execute("SELECT value FROM config WHERE key=?;", (key,))
        row = self._cursor.fetchone()
        if row and len(row) > 0:
            return self._deserialize(row[0])
        else:
            raise KeyError("Key `%s' does not exist" % key)
    
    def __setitem__(self, key, value):
        """set the value of the specified key
        """
        lock = PersistentDict.lock
        lock.acquire()
        try:
            if key in self:
                self._cursor.execute("UPDATE config SET value=? WHERE key=?;", (self._serialize(value), key))
            else:
                self._cursor.execute("INSERT INTO config (key, value) VALUES(?, ?);", (key, self._serialize(value)))
            self._conn.commit()
        finally:
            lock.release()

    def __delitem__(self, key):
        """remove the specifed value from the database
        """
        self._cursor.execute("DELETE FROM config WHERE key=?;", (key,))
        if self._cursor.rowcount == 0:
            raise KeyError("Key `%s' does not exist" % key)
        self._conn.commit()
    
    def _serialize(self, value):
        """convert object to a compressed blog string to save in the db
        """
        return db.Binary(zlib.compress(pickle.dumps(value), self.compress_level))
    
    def _deserialize(self, value):
        """convert compressed string from database back into an object
        """
        return pickle.loads(zlib.decompress(value))

    def keys(self):
        """returns a list containing each key in the database
        """
        self._cursor.execute("SELECT key FROM config;")
        return [row[0] for row in self._cursor]


if __name__ == '__main__':
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

