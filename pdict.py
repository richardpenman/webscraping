__doc__ = """
pdict has a dictionary like interface and a sqlite backend
It uses pickle to store Python objects and strings, which are then compressed
Multithreading is supported
"""

import os
import sys
import datetime
import sqlite3
import zlib
import threading
import md5
import shutil
try:
    import cPickle as pickle
except ImportError:
    import pickle


class PersistentDict:
    """Stores and retrieves persistent data through a dict-like interface
    Data is stored compressed on disk using sqlite3 

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
    >>> cache.set(url, dict(created=now))
    >>> cache.meta(url)
    {}
    >>> cache.meta(url, 'meta')
    >>> cache.meta(url)
    'meta'
    >>> del cache[url]
    >>> url in cache
    False
    >>>
    >>> keys = ['a', 'b', 'c']
    >>> cache.add_status(status=False, keys=keys)
    3
    >>> cache.get_status_count(status=False)
    3
    >>> cache.get_status(status=False, limit=len(keys)) == keys 
    True
    >>> key = keys.pop()
    >>> cache.set_status(keys=[key], status=True) # set status to True for this key
    1
    >>> cache.get_status_count(status=False) # get number of records with status False
    2
    >>> cache.set_status(keys=None, status=True) # set all status to True
    3
    >>> os.remove(filename)
    """
    DEFAULT_LIMIT = 1000

    def __init__(self, filename='cache.db', compress_level=6, expires=None, timeout=5000, isolation_level=None):
        """initialize a new PersistentDict with the specified database file.

        filename: where to store sqlite database. Uses in memory by default.
        compress_level: between 1-9 (in my test levels 1-3 produced a 1300kb file in ~7 seconds while 4-9 a 288kb file in ~9 seconds)
        expires: a timedelta object of how old data can be before expires. By default is set to None to disable.
        timeout: how long should a thread wait for sqlite to be ready (in ms)
        isolation_level: None for autocommit or else 'DEFERRED' / 'IMMEDIATE' / 'EXCLUSIVE'
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
            created timestamp DEFAULT (datetime('now', 'localtime')),
            updated timestamp DEFAULT (datetime('now', 'localtime'))
        );
        """
        self._conn.execute(sql)
        self._conn.execute("CREATE INDEX IF NOT EXISTS keys ON config (key);")
        # XXX no performance increase
        #self._conn.execute("CREATE INDEX IF NOT EXISTS timestamp ON config (created);")
        try:
            self._conn.execute("ALTER TABLE config ADD COLUMN status INTEGER;")
        except sqlite3.OperationalError:
            pass # column already exists
        #self._conn.execute("CREATE INDEX IF NOT EXISTS crawled ON config (status);")
        self.fscache = FSCache(os.path.dirname(filename))


    def __copy__(self):
        """make copy with current cache settings
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
        row = self._conn.execute("SELECT value, status, updated FROM config WHERE key=?;", (key,)).fetchone()
        if row:
            if self.is_fresh(row[2]):
                try:
                    value = self.fscache[key]
                except KeyError:
                    # XXX remove this when migrated
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
        del self.fscache[key]


    def __setitem__(self, key, value):
        """set the value of the specified key
        """
        updated = datetime.datetime.now()
        self._conn.execute("INSERT OR REPLACE INTO config (key, meta, status, updated) VALUES(?, ?, ?, ?);", (
            key, self.serialize({}), True, updated)
        )
        self.fscache[key] = self.serialize(value)


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
                try:
                    value = self.fscache[key]
                except KeyError:
                    value = row[0] # XXX remove after migrated
                data = dict(
                    value=self.deserialize(value),
                    meta=self.deserialize(row[1]),
                    created=row[2],
                    updated=row[3]
                )
        return data



    def get_status(self, status, limit=DEFAULT_LIMIT, ascending=True):
        """Get keys with this status

        Limit to given number
        Orders by ascending by default
        """
        order = ascending and 'ASC' or 'DESC'
        rows = self._conn.execute("SELECT key FROM config WHERE status=? ORDER BY created %s LIMIT ?;" % order, (status, limit)).fetchall()
        return [row[0] for row in rows]


    def get_status_count(self, status):
        """Get number of rows with this status
        """
        row = self._conn.execute("SELECT count(*) FROM config WHERE status=?;", (status,)).fetchone()
        return row[0]


    def set_status(self, keys, status):
        """Set status of given key

        If key is None then set all keys
        Returns number of rows effected
        """
        c = self._conn.cursor()
        if keys is None:
            c.execute("UPDATE config SET status=?;", (status,))
        else:
            c.executemany("UPDATE config SET status=? WHERE key=? AND status!=?;", [(status, key, status) for key in keys])
        return c.rowcount
        

    def add_status(self, keys, status):
        """Add records for these keys without setting the content

        Will not insert if key already exists.
        Returns the number of inserted rows.
        """
        c = self._conn.cursor()
        timestamp = datetime.datetime.now()
        records = [(key, None, None, status, timestamp, timestamp) for key in keys]
        # ignore if key already exists
        c.executemany("INSERT OR IGNORE INTO config (key, value, meta, status, created, updated) VALUES(?, ?, ?, ?, ?, ?);", records)
        return c.rowcount


    def set(self, key, data):
        """Set data for the specified key

        data is a dict {'value': ..., 'meta': ..., 'created': ..., 'updated': ...}
        """
        if 'value' in data:
            value = self.serialize(data['value'])
            self.fscache[key] = value
        row = self._conn.execute("SELECT meta, created, updated FROM config WHERE key=?;", (key,)).fetchone()
        if row:
            current_data = dict(meta=self.deserialize(row[0]), created=row[1], updated=row[2])
            current_data.update(data)
            data = current_data
        meta = self.serialize(data.get('meta'))
        created = data.get('created')
        updated = data.get('updated')
        self._conn.execute("INSERT OR REPLACE INTO config (key, meta, created, updated) VALUES(?, ?, ?, ?);", (key, meta, created, updated))


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
        self.fscache.clear()


    def merge(self, db, override=False):
        """Merge this databases content
        override determines whether to override existing keys
        """
        for key in db.keys():
            if override or key not in self:
                self[key] = db[key]


    def shrink(self):
        """Shrink the cache by writing values to disk
        """
        limit = 998 # SQLITE_MAX_VARIABLE_NUMBER = 999
        num_updates = 0
        
        #num_outstanding = c.execute("SELECT count(*) from config WHERE length(value) > 0;").fetchone()[0]
        while True:
            keys = []
            for record in self._conn.execute("SELECT key, value from config WHERE length(value) > 0 LIMIT ?;", (limit, )):
                num_updates += 1
                key, value = record
                self.fscache[key] = value
                keys.append(key)
            print num_updates
            if keys:
                self._conn.execute("UPDATE config SET value=? WHERE key IN (%s)" % ','.join('?' * len(keys)), [None] + keys)
                #c.executemany("UPDATE config SET value=? WHERE key=?", [(None, key) for key in keys])
            else:
                break
        if num_updates > 0:
            # reduce size of database after values removed
            self._conn.execute('VACUUM')
        return num_updates


class FSCache:
    """Cache files in the file system

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
    parser.add_option('-s', '--shrink', action='store_true', dest='shrink', default=False, help='Shrink the cache by saving to disk')
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
    elif options.shrink:
        cache.shrink()
        print 'shrunk'
    elif options.clear:
        if raw_input('Really? Clear the cache? (y/n) ') == 'y':
            cache.clear()
            print 'cleared'
    else:
        parser.error('No options selected')
