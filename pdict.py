#
# Description:
# pdict has a dictionary like interface and a sqlite backend
# It uses pickle to store Python objects and strings, which are then compressed
# Multithreading is supported
#
# Author: Richard Penman (richard@sitescraper.net)
# License: LGPL
#


from datetime import datetime
import sqlite3
import zlib
import threading
try:
    import cPickle as pickle
except ImportError:
    import pickle


class PersistentDict(object):
    """stores and retrieves persistent data through a dict-like interface
    data is stored compressed on disk using sqlite3 
    """
    
    def __init__(self, filename=':memory:', compress_level=6, timeout=None):
        """initialize a new PersistentDict with the specified database file.

        filename: where to store sqlite database. Uses in memory by default.
        compress_level: between 1-9 (in my test levels 1-3 produced a 1300kb file in ~7 seconds while 4-9 a 288kb file in ~9 seconds)
        timeout: a timedelta object of how old data can be. By default is set to None to disable.
        """
        self._conn = sqlite3.connect(filename, timeout=1000, isolation_level=None, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
        self._conn.text_factory = lambda x: unicode(x, 'utf-8', 'replace')
        sql = """
        CREATE TABLE IF NOT EXISTS config (
            key TEXT NOT NULL PRIMARY KEY UNIQUE,
            value BLOB,
            meta BLOB,
            created timestamp DEFAULT (datetime('now', 'localtime')),
            updated timestamp DEFAULT (datetime('now', 'localtime'))
        );
        """
        self._conn.execute(sql)
        self._conn.execute("CREATE INDEX IF NOT EXISTS keys ON config (key);")
        #try:
        #    self._conn.execute("ALTER TABLE config ADD COLUMN url TEXT;")
        #except sqlite3.OperationalError:
        #    pass # already have column
        self.compress_level = compress_level
        self.timeout = timeout

    
    def __contains__(self, key):
        """check the database to see if a key exists
        """
        row = self._conn.execute("SELECT updated FROM config WHERE key=?;", (key,)).fetchone()
        return row and self.is_fresh(row[0])
            
    def __getitem__(self, key):
        """return the value of the specified key or raise KeyError if not found
        """
        row = self._conn.execute("SELECT value, updated FROM config WHERE key=?;", (key,)).fetchone()
        if row:
            if self.is_fresh(row[1]):
                return self.deserialize(row[0])
            else:
                raise KeyError("Key `%s' is stale" % key)
        else:
            raise KeyError("Key `%s' does not exist" % key)
    
    
    def __setitem__(self, key, value):
        """set the value of the specified key
        """
        try:
            self._conn.execute("INSERT INTO config (key, value, meta) VALUES(?, ?, ?);", (key, self.serialize(value), self.serialize({})))
        except sqlite3.IntegrityError:
            # already exists, so update
            self._conn.execute("UPDATE config SET value=?, updated=? WHERE key=?;", (self.serialize(value), datetime.now(), key))

    def __delitem__(self, key):
        """remove the specifed value from the database
        """
        self._conn.execute("DELETE FROM config WHERE key=?;", (key,))
        
    def serialize(self, value):
        """convert object to a compressed pickled string to save in the db
        """
        return sqlite3.Binary(zlib.compress(pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL), self.compress_level))
    
    def deserialize(self, value):
        """convert compressed pickled string from database back into an object
        """
        return pickle.loads(zlib.decompress(value)) if value else value

    def keys(self):
        """returns a generator of each key in the database
        """
        c = self._conn.cursor()
        c.execute("SELECT key FROM config;")
        for row in c:
            yield row[0]
        #return [row[0] for row in self._conn.execute("SELECT key FROM config;").fetchall()]

    def is_fresh(self, t):
        """returns whether this datetime has expired
        """
        return self.timeout is None or datetime.now() - t < self.timeout

    def get(self, key, default=None):
        """Get data at key and return default if not defined
        """
        data = default
        if key:
            row = self._conn.execute("SELECT value, meta, created, updated FROM config WHERE key=?;", (key,)).fetchone()
            if row:
                data = dict(
                    value=self.deserialize(row[0]),
                    meta=self.deserialize(row[1]),
                    created=row[2],
                    updated=row[3]
                )
        return data

    def set(self, key, new_data):
        """set the data for the specified key

        data is a dict {'value': ..., 'meta': ..., 'created': ..., 'updated': ...}
        """
        current_data = self.get(key)
        current_data.update(new_data)
        value = self.serialize(current_data.get('value'))
        meta = self.serialize(current_data.get('meta'))
        created = current_data.get('created')
        updated = current_data.get('updated')
        #keys = new_data.keys() + ['key']
        #values = [new_data[key] for key in keys] + [key]
        #self._conn.execute("INSERT INTO config (%s) VALUES(%s);" % (', '.join(keys), ', '.join(['?'] * len(keys))), values)
        # already exists, so update
        self._conn.execute("UPDATE config SET value=?, meta=?, created=?, updated=? WHERE key=?;", (value, meta, created, updated, key))

    def meta(self, key, value=None):
        """Set of get the meta attribute
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
            self._conn.execute("UPDATE config SET meta=?, updated=? WHERE key=?;", (self.serialize(value), datetime.now(), key))


    def __delitem__(self, key):
        """remove the specifed value from the database
        """
        self._conn.execute("DELETE FROM config WHERE key=?;", (key,))

    def merge(self, db, override=False):
        """Merge this databases content
        override determines whether to override existing keys
        """
        for key in db.keys():
            if override or key not in self:
                self[key] = db[key]



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
