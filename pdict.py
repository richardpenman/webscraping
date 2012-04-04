__doc__ = """
pdict has a dictionary like interface and a sqlite backend
It uses pickle to store Python objects and strings, which are then compressed
Multithreading is supported
"""

import sys
import datetime
import sqlite3
import zlib
import threading
try:
    import cPickle as pickle
except ImportError:
    import pickle



class BufferList:
    """Track memory used by buffer
    """
    def __init__(self, *args, **kwargs):
        self.data = []
        self.num_bytes = 0

    def __len__(self):
        return len(self.data)

    def add(self, sql, args):
        """add sql and args to list
        """
        self.data.append((sql, args))
        self.num_bytes += len(sql) 
        for arg in args:
            try:
                self.num_bytes += len(arg)
            except TypeError:
                pass

    def is_full(self, max_buffer_size):
        """returns whether buffer data size is greater than max_buffer_size in MB
        """
        BYTES_PER_MB = 1024 * 1024
        #print 'buffer size', self.num_bytes, len(self.d), 'records'
        return self.num_bytes > max_buffer_size * BYTES_PER_MB

    def pop_all(self):
        self.num_bytes = 0
        data, self.data = self.data, []
        return data
        

"""
change above class to storing sql string list
collect sql strings from many operations
use decorator to test flush
"""
class PersistentDict:
    """stores and retrieves persistent data through a dict-like interface
    data is stored compressed on disk using sqlite3 
    """
    # buffer data so can insert multiple records in a single transaction
    buffered_sql = BufferList()

    def __init__(self, filename=':memory:', compress_level=6, expires=None, timeout=1000, max_buffer_size=0):
        """initialize a new PersistentDict with the specified database file.

        filename: where to store sqlite database. Uses in memory by default.
        compress_level: between 1-9 (in my test levels 1-3 produced a 1300kb file in ~7 seconds while 4-9 a 288kb file in ~9 seconds)
        expires: a timedelta object of how old data can be before expires. By default is set to None to disable.
        timeout: how long should a thread wait for sqlite to be ready
        max_buffer_size: maximum size in MB of buffered data before write to sqlite
        """
        self._conn = sqlite3.connect(filename, timeout=timeout, isolation_level=None, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
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
        self.filename = filename
        self.compress_level = compress_level
        self.expires = expires
        self.timeout = timeout
        self.max_buffer_size = max_buffer_size

    
    def __del__(self):
        self.flush() 
  

    def __copy__(self):
        """make copy with current cache settings
        """
        return PersistentDict(filename=self.filename, compress_level=self.compress_level, expires=self.expires, timeout=self.timeout, max_buffer_size=self.max_buffer_size)


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
                return self.deserialize(row[0])
            else:
                raise KeyError("Key `%s' is stale" % key)
        else:
            raise KeyError("Key `%s' does not exist" % key)


    def __delitem__(self, key):
        """remove the specifed value from the database
        """
        self.buffer_execute("DELETE FROM config WHERE key=?;", (key,))


    def __setitem__(self, key, value):
        """set the value of the specified key
        """
        self.buffer_execute("INSERT OR REPLACE INTO config (key, value, meta, updated) VALUES(?, ?, ?, ?);", (key, self.serialize(value), self.serialize({}), datetime.datetime.now()))


    def buffer_execute(self, sql, args):
        #if re.match('(INSERT|UPDATE|DELETE)', sql):
        PersistentDict.buffered_sql.add(sql, args)
        if PersistentDict.buffered_sql.is_full(self.max_buffer_size):
            self.flush()

    # XXX need to lock?
    def flush(self):
        """write any buffered records to sqlite
        """
        if PersistentDict.buffered_sql:
            sql_args = PersistentDict.buffered_sql.pop_all()
            need_transaction = len(sql_args) > 1
            if need_transaction:
                self._conn.execute('BEGIN TRANSACTION;')
            for sql, args in sql_args:
                self._conn.execute(sql, args)
            if need_transaction:
                self._conn.execute('COMMIT;')


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
        current_data = self.get(key, {})
        current_data.update(new_data)
        value = self.serialize(current_data.get('value'))
        meta = self.serialize(current_data.get('meta'))
        created = current_data.get('created')
        updated = current_data.get('updated')
        self.buffer_execute("INSERT OR REPLACE INTO config (key, value, meta, created, updated) VALUES(?, ?, ?, ?, ?);", (key, value, meta, created, updated))


    def meta(self, key, value=None):
        """
        if value is passed then set the meta attribute for this key
        XXX return true/false if successful

        otherwise get the existing meta attribute for this key
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
            self.buffer_execute("UPDATE config SET meta=?, updated=? WHERE key=?;", (self.serialize(value), datetime.datetime.now(), key))


    def clear(self):
        """Clear all cached data
        """
        self._conn.execute("DELETE FROM config;")


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
        del p[key]
