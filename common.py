# -*- coding: utf-8 -*-
__doc__ = 'Common web scraping related functions'

import os
import re
import sys
import csv
csv.field_size_limit(sys.maxint)
import time
import glob
import string
import urllib
import string
import urllib2
import urlparse
import cookielib
import itertools
import htmlentitydefs
import logging
import logging.handlers
import threading
import collections
from datetime import datetime, timedelta
try:
    # should use pysqlite2 to read the cookies.sqlite on Windows
    # otherwise will raise the "sqlite3.DatabaseError: file is encrypted or is not a database" exception
    from pysqlite2 import dbapi2 as sqlite3
except ImportError:
    import sqlite3 
import adt
import settings

try:
    import json
except ImportError:
    import simplejson as json


class WebScrapingError(Exception):
    pass


# known media file extensions
MEDIA_EXTENSIONS = ['ai', 'aif', 'aifc', 'aiff', 'asc', 'avi', 'bcpio', 'bin', 'c', 'cc', 'ccad', 'cdf', 'class', 'cpio', 'cpt', 'csh', 'css', 'csv', 'dcr', 'dir', 'dms', 'doc', 'drw', 'dvi', 'dwg', 'dxf', 'dxr', 'eps', 'etx', 'exe', 'ez', 'f', 'f90', 'fli', 'flv', 'gif', 'gtar', 'gz', 'h', 'hdf', 'hh', 'hqx', 'ice', 'ico', 'ief', 'iges', 'igs', 'ips', 'ipx', 'jpe', 'jpeg', 'jpg', 'js', 'kar', 'latex', 'lha', 'lsp', 'lzh', 'm', 'man', 'me', 'mesh', 'mid', 'midi', 'mif', 'mime', 'mov', 'movie', 'mp2', 'mp3', 'mpe', 'mpeg', 'mpg', 'mpga', 'ms', 'msh', 'nc', 'oda', 'pbm', 'pdb', 'pdf', 'pgm', 'pgn', 'png', 'pnm', 'pot', 'ppm', 'pps', 'ppt', 'ppz', 'pre', 'prt', 'ps', 'qt', 'ra', 'ram', 'ras', 'rgb', 'rm', 'roff', 'rpm', 'rtf', 'rtx', 'scm', 'set', 'sgm', 'sgml', 'sh', 'shar', 'silo', 'sit', 'skd', 'skm', 'skp', 'skt', 'smi', 'smil', 'snd', 'sol', 'spl', 'src', 'step', 'stl', 'stp', 'sv4cpio', 'sv4crc', 'swf', 't', 'tar', 'tcl', 'tex', 'texi', 'tif', 'tiff', 'tr', 'tsi', 'tsp', 'tsv', 'txt', 'unv', 'ustar', 'vcd', 'vda', 'viv', 'vivo', 'vrml', 'w2p', 'wav', 'wmv', 'wrl', 'xbm', 'xlc', 'xll', 'xlm', 'xls', 'xlw', 'xml', 'xpm', 'xsl', 'xwd', 'xyz', 'zip']

# tags that do not contain content
EMPTY_TAGS = 'br', 'hr', 'meta', 'link', 'base', 'img', 'embed', 'param', 'area', 'col', 'input'


def to_ascii(html):
    """Return ascii part of html
    """
    return ''.join(c for c in (html or '') if ord(c) < 128)

def to_int(s, default=0):
    """Return integer from this string

    >>> to_int('90')
    90
    >>> to_int('-90.2432')
    -90
    >>> to_int('a90a')
    90
    >>> to_int('a')
    0
    >>> to_int('a', 90)
    90
    """
    return int(to_float(s, default))

def to_float(s, default=0.0):
    """Return float from this string

    >>> to_float('90.45')
    90.45
    >>> to_float('')
    0.0
    >>> to_float('90')
    90.0
    >>> to_float('..9')
    0.0
    >>> to_float('.9')
    0.9
    >>> to_float(None)
    0.0
    >>> to_float(1)
    1.0
    """
    result = default
    if s:
        valid = string.digits + '.-'
        try:
            result = float(''.join(c for c in str(s) if c in valid))
        except ValueError:
            pass # input does not contain a number
    return result

    
def to_unicode(obj, encoding=settings.default_encoding):
    """Convert obj to unicode
    """
    if isinstance(obj, basestring):
        if not isinstance(obj, unicode):
            obj = obj.decode(encoding, 'ignore')
    return obj


def html_to_unicode(html, charset=settings.default_encoding):
    """Convert html to unicode, decoding by specified charset when available
    """
    m = re.compile(r'<meta[^<>]*charset=\s*([a-z\d\-]+)', re.IGNORECASE).search(html)
    if m:
        charset = m.groups()[0].strip().lower()
        
    return to_unicode(html, charset)
    
    
def is_html(html):
    """Returns whether content is likely HTML based on search for common tags
    """
    try:
        result = re.search('html|head|body', html) is not None
    except TypeError:
        result = False
    return result


def is_url(text):
    """Returns whether passed text is a URL

    >>> is_url('abc')
    False
    >>> is_url('webscraping.com')
    False
    >>> is_url('http://webscraping.com/blog')
    True
    """
    return re.match('https?://', text) is not None


def unique(l):
    """Remove duplicates from list, while maintaining order

    >>> unique([3,6,4,4,6])
    [3, 6, 4]
    >>> unique([])
    []
    >>> unique([3,6,4])
    [3, 6, 4]
    """
    checked = []
    for e in l:
        if e not in checked:
            checked.append(e)
    return checked


def flatten(l):
    """Flatten a list of lists into a single list

    >>> flatten([[1,2,3], [4,5,6]])
    [1, 2, 3, 4, 5, 6]
    """
    return [item for sublist in l for item in sublist]


def nth(l, i, default=''):
    """Return nth item from list or default value if out of range
    """
    try:
        return l[i] 
    except IndexError:
        return default

def first(l, default=''):
    """Return first element from list or default value if out of range

    >>> first([1,2,3])
    1
    >>> first([], None)
    
    """
    return nth(l, i=0, default=default)

def last(l, default=''):
    """Return last element from list or default value if out of range
    """
    return nth(l, i=-1, default=default)


def pad(l, size, default=None, end=True):
    """Return list of given size
    Insert elements of default value if too small
    Remove elements if too large
    Manipulate end of list if end is True, else start

    >>> pad(range(5), 5)
    [0, 1, 2, 3, 4]
    >>> pad(range(5), 3)
    [0, 1, 2]
    >>> pad(range(5), 7, -1)
    [0, 1, 2, 3, 4, -1, -1]
    >>> pad(range(5), 7, end=False)
    [None, None, 0, 1, 2, 3, 4]
    """
    while len(l) < size:
        if end:
            l.append(default)
        else:
            l.insert(0, default)
    while len(l) > size:
        if end:
            l.pop()
        else:
            l.pop(0)
    return l


def remove_tags(html, keep_children=True):
    """Remove HTML tags leaving just text
    If keep children is True then keep text within child tags

    >>> remove_tags('hello <b>world</b>!')
    'hello world!'
    >>> remove_tags('hello <b>world</b>!', False)
    'hello !'
    >>> remove_tags('hello <br>world<br />!', False)
    'hello world!'
    >>> remove_tags('<span><b></b></span>test</span>', False)
    'test'
    """
    html = re.sub('<(%s)[^>]*>' % '|'.join(EMPTY_TAGS), '', html)
    if not keep_children:
        for tag in unique(re.findall('<(\w+?)\W', html)):
            if tag not in EMPTY_TAGS:
                html = re.compile('<\s*%s.*?>.*?</\s*%s\s*>' % (tag, tag), re.DOTALL).sub('', html)
    return re.compile('<[^<]*?>').sub('', html)
    
    
def unescape(text, encoding=settings.default_encoding, keep_unicode=False):
    """Interpret escape characters

    >>> unescape('&lt;hello&nbsp;&amp;%20world&gt;')
    '<hello & world>'
    """
    if not text:
        return ''
    try:
        text = to_unicode(text, encoding)
    except UnicodeError:
        pass

    def fixup(m):
        text = m.group(0)
        if text[:2] == '&#':
            # character reference
            try:
                if text[:3] == '&#x':
                    return unichr(int(text[3:-1], 16))
                else:
                    return unichr(int(text[2:-1]))
            except ValueError:
                pass
        else:
            # named entity
            try:
                text = unichr(htmlentitydefs.name2codepoint[text[1:-1].lower()])
            except KeyError:
                pass
        return text # leave as is
    text = re.sub('&#?\w+;', fixup, text)
    text = urllib.unquote(text)
    if keep_unicode:
        return text
    try:
        text = text.encode(encoding, 'ignore')
    except UnicodeError:
        pass
    
    if encoding != 'utf-8':
        return text

    # remove annoying characters
    chars = {
        '\xc2\x82' : ',',        # High code comma
        '\xc2\x84' : ',,',       # High code double comma
        '\xc2\x85' : '...',      # Tripple dot
        '\xc2\x88' : '^',        # High carat
        '\xc2\x91' : '\x27',     # Forward single quote
        '\xc2\x92' : '\x27',     # Reverse single quote
        '\xc2\x93' : '\x22',     # Forward double quote
        '\xc2\x94' : '\x22',     # Reverse double quote
        '\xc2\x95' : ' ',  
        '\xc2\x96' : '-',        # High hyphen
        '\xc2\x97' : '--',       # Double hyphen
        '\xc2\x99' : ' ',
        '\xc2\xa0' : ' ',
        '\xc2\xa6' : '|',        # Split vertical bar
        '\xc2\xab' : '<<',       # Double less than
        '\xc2\xae' : '®',
        '\xc2\xbb' : '>>',       # Double greater than
        '\xc2\xbc' : '1/4',      # one quarter
        '\xc2\xbd' : '1/2',      # one half
        '\xc2\xbe' : '3/4',      # three quarters
        '\xca\xbf' : '\x27',     # c-single quote
        '\xcc\xa8' : '',         # modifier - under curve
        '\xcc\xb1' : ''          # modifier - under line
    }
    def replace_chars(match):
        char = match.group(0)
        return chars[char]

    return re.sub('(' + '|'.join(chars.keys()) + ')', replace_chars, text)

   
def normalize(s, encoding=settings.default_encoding):
    """Normalize the string by removing tags, unescaping, and removing surrounding whitespace
    
    >>> normalize('<span>Tel.:   029&nbsp;-&nbsp;12345678   </span>')
    'Tel.: 029 - 12345678'
    """
    if isinstance(s, basestring):
        return re.sub('[\n\r]+', '\n', re.sub('[ \t]+', ' ', unescape(remove_tags(s), encoding=encoding, keep_unicode=isinstance(s, unicode)))).strip()
    else:
        return s


def regex_get(html, pattern, index=None, normalized=True, flag=re.DOTALL|re.IGNORECASE, default=''):
    """Helper method to extract content from regular expression
    
    >>> regex_get('<div><span>Phone: 029&nbsp;01054609</span><span></span></div>', r'<span>Phone:([^<>]+)')
    '029 01054609'
    >>> regex_get('<div><span>Phone: 029&nbsp;01054609</span><span></span></div>', r'<span>Phone:\s*(\d+)&nbsp;(\d+)')
    ['029', '01054609']
    """
    m = re.compile(pattern, flag).search(html)
    if m:
        if len(m.groups()) == 1:
            return normalize(m.groups()[0]) if normalized else m.groups()[0]
        elif index != None:
            return normalize(m.groups()[index]) if normalized else m.groups()[index]
        else:
            return [normalize(item) if normalized else item for item in m.groups()]
    return default


def safe(s):
    """Return characters in string that are safe for URLs
    
    >>> safe('U@#$_#^&*-2')
    'U_-2'
    """
    safe_chars = string.letters + string.digits + '-_ '
    return ''.join(c for c in s if c in safe_chars).replace(' ', '-')


def pretty(s):
    """Return pretty version of string for display
    
    >>> pretty('hello_world')
    'Hello World'
    """
    return re.sub('[-_]', ' ', s.title())


def pretty_paragraph(s):
    """Return pretty version of text in paragraph for display
    """
    s = re.sub('<(br|hr|/li)[^>]*>', '\n', s, re.IGNORECASE)
    s = unescape(remove_tags(s))
    def fixup(m):
        text = m.group(0)
        if '\r' in text or '\n' in text: return '\n'
        return ' '
    return re.sub('\s+', fixup, s).strip()
    

def get_extension(url):
    """Return extension from given URL

    >>> get_extension('hello_world.JPG')
    'jpg'
    >>> get_extension('http://www.google-analytics.com/__utm.gif?utmwv=1.3&utmn=420639071')
    'gif'
    """
    return os.path.splitext(urlparse.urlsplit(url).path)[-1].lower().replace('.', '')


def get_domain(url):
    """Extract the domain from the given URL

    >>> get_domain('http://www.google.com.au/tos.html')
    'google.com.au'
    >>> get_domain('www.google.com')
    'google.com'
    """
    m = re.compile(r"^.*://(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})").search(url)
    if m:
        # an IP address
        return m.groups()[0]
    
    suffixes = 'ac', 'ad', 'ae', 'aero', 'af', 'ag', 'ai', 'al', 'am', 'an', 'ao', 'aq', 'ar', 'arpa', 'as', 'asia', 'at', 'au', 'aw', 'ax', 'az', 'ba', 'bb', 'bd', 'be', 'bf', 'bg', 'bh', 'bi', 'biz', 'bj', 'bm', 'bn', 'bo', 'br', 'bs', 'bt', 'bv', 'bw', 'by', 'bz', 'ca', 'cat', 'cc', 'cd', 'cf', 'cg', 'ch', 'ci', 'ck', 'cl', 'cm', 'cn', 'co', 'com', 'coop', 'cr', 'cu', 'cv', 'cx', 'cy', 'cz', 'de', 'dj', 'dk', 'dm', 'do', 'dz', 'ec', 'edu', 'ee', 'eg', 'er', 'es', 'et', 'eu', 'fi', 'fj', 'fk', 'fm', 'fo', 'fr', 'ga', 'gb', 'gd', 'ge', 'gf', 'gg', 'gh', 'gi', 'gl', 'gm', 'gn', 'gov', 'gp', 'gq', 'gr', 'gs', 'gt', 'gu', 'gw', 'gy', 'hk', 'hm', 'hn', 'hr', 'ht', 'hu', 'id', 'ie', 'il', 'im', 'in', 'info', 'int', 'io', 'iq', 'ir', 'is', 'it', 'je', 'jm', 'jo', 'jobs', 'jp', 'ke', 'kg', 'kh', 'ki', 'km', 'kn', 'kp', 'kr', 'kw', 'ky', 'kz', 'la', 'lb', 'lc', 'li', 'lk', 'lr', 'ls', 'lt', 'lu', 'lv', 'ly', 'ma', 'mc', 'md', 'me', 'mg', 'mh', 'mil', 'mk', 'ml', 'mm', 'mn', 'mo', 'mobi', 'mp', 'mq', 'mr', 'ms', 'mt', 'mu', 'mv', 'mw', 'mx', 'my', 'mz', 'na', 'name', 'nc', 'ne', 'net', 'nf', 'ng', 'ni', 'nl', 'no', 'np', 'nr', 'nu', 'nz', 'om', 'org', 'pa', 'pe', 'pf', 'pg', 'ph', 'pk', 'pl', 'pm', 'pn', 'pr', 'pro', 'ps', 'pt', 'pw', 'py', 'qa', 're', 'ro', 'rs', 'ru', 'rw', 'sa', 'sb', 'sc', 'sd', 'se', 'sg', 'sh', 'si', 'sj', 'sk', 'sl', 'sm', 'sn', 'so', 'sr', 'st', 'su', 'sv', 'sy', 'sz', 'tc', 'td', 'tel', 'tf', 'tg', 'th', 'tj', 'tk', 'tl', 'tm', 'tn', 'to', 'tp', 'tr', 'tt', 'tv', 'tw', 'tz', 'ua', 'ug', 'uk', 'us', 'uy', 'uz', 'va', 'vc', 've', 'vg', 'vi', 'vn', 'vu', 'wf', 'ws', 'xn', 'ye', 'yt', 'za', 'zm', 'zw'
    url = re.sub('^.*://', '', url).partition('/')[0].lower()
    domain = []
    for section in url.split('.'):
        if section in suffixes:
            domain.append(section)
        else:
            domain = [section]
    return '.'.join(domain)


def same_domain(url1, url2):
    """Return whether URLs belong to same domain
    
    >>> same_domain('http://www.google.com.au', 'code.google.com')
    True
    >>> same_domain('http://www.facebook.com', 'http://www.myspace.com')
    False
    """
    server1 = get_domain(url1)
    server2 = get_domain(url2)
    return server1 and server2 and (server1 in server2 or server2 in server1)


def pretty_duration(dt):
    """Return english description of this time difference
    
    >>> from datetime import timedelta
    >>> pretty_duration(timedelta(seconds=1))
    '1 second'
    >>> pretty_duration(timedelta(hours=1))
    '1 hour'
    >>> pretty_duration(timedelta(days=2))
    '2 days'
    """
    if isinstance(dt, datetime):
        # convert datetime to timedelta
        dt = datetime.now() - dt
    if not isinstance(dt, timedelta):
        return ''
    if dt.days >= 2*365: 
        return '%d years' % int(dt.days / 365) 
    elif dt.days >= 365: 
        return '1 year' 
    elif dt.days >= 60: 
        return '%d months' % int(dt.days / 30) 
    elif dt.days > 21: 
        return '1 month' 
    elif dt.days >= 14: 
        return '%d weeks' % int(dt.days / 7) 
    elif dt.days >= 7: 
        return '1 week' 
    elif dt.days > 1: 
        return '%d days' % dt.days 
    elif dt.days == 1: 
        return '1 day' 
    elif dt.seconds >= 2*60*60: 
        return '%d hours' % int(dt.seconds / 3600) 
    elif dt.seconds >= 60*60: 
        return '1 hour' 
    elif dt.seconds >= 2*60: 
        return '%d minutes' % int(dt.seconds / 60) 
    elif dt.seconds >= 60: 
        return '1 minute' 
    elif dt.seconds > 1: 
        return '%d seconds' % dt.seconds 
    elif dt.seconds == 1: 
        return '1 second' 
    else: 
        return ''


def parse_proxy(proxy):
    """Parse a proxy into its fragments
    Returns a dict with username, password, host, and port

    >>> f = parse_proxy('login:pw@66.197.208.200:8080')
    >>> f.username
    'login'
    >>> f.password
    'pw'
    >>> f.host
    '66.197.208.200'
    >>> f.port
    '8080'
    >>> f = parse_proxy('66.197.208.200')
    >>> f.username == f.password == f.port == ''
    True
    >>> f.host
    '66.197.208.200'
    """
    fragments = adt.Bag()
    if isinstance(proxy, basestring):
        match = re.match('((?P<username>\w+):(?P<password>\w+)@)?(?P<host>\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3})(:(?P<port>\d+))?', proxy)
        if match:
            groups = match.groupdict()
            fragments.username = groups.get('username') or ''
            fragments.password = groups.get('password') or ''
            fragments.host = groups.get('host')
            fragments.port = groups.get('port') or ''
    return fragments


def read_list(file):
    """Return file as list if exists
    """
    l = []
    if os.path.exists(file):
        l.extend(open(file).read().splitlines())
    else:
        logger.debug('%s not found' % file)
    return l


class UnicodeWriter:
    """A CSV writer that produces Excel-compatible CSV files from unicode data.
    
    file: 
        can either be a filename or a file object
    encoding:
        the encoding to use for output
    mode:
        the mode for writing to file
    unique:
        if True then will only write unique rows to output
    unique_by:
        make the rows unique by these columns(the value is a list of indexs), default by all columns
    quoting:
        csv module quoting style to use
    utf8_bom:
        whether need to add the BOM
    auto_repair:
        whether need to remove the invalid rows automatically
    
    >>> from StringIO import StringIO
    >>> fp = StringIO()
    >>> writer = UnicodeWriter(fp, quoting=csv.QUOTE_MINIMAL)
    >>> writer.writerow(['a', '1'])
    >>> writer.flush()
    >>> fp.seek(0)
    >>> fp.read().strip()
    'a,1'
    """
    def __init__(self, file, encoding=settings.default_encoding, mode='wb', unique=False, unique_by=None, quoting=csv.QUOTE_ALL, utf8_bom=False, auto_repair=False, **argv):
        self.encoding = encoding
        self.unique = unique
        self.unique_by = unique_by
        if hasattr(file, 'write'):
            self.fp = file
        else:
            if auto_repair:
                self._remove_invalid_rows(file=file, quoting=quoting, **argv)
            if utf8_bom:
                self.fp = open(file, 'wb')
                self.fp.write('\xef\xbb\xbf')
                self.fp.close()
                self.fp = open(file, mode=mode.replace('w', 'a'))
            else:
                self.fp = open(file, mode)
        if self.unique:
            self.rows = adt.HashDict() # cache the rows that have already been written
            for row in csv.reader(open(self.fp.name)):
                self.rows[self._unique_key(row)] = True
        self.writer = csv.writer(self.fp, quoting=quoting, **argv)
        
    def _unique_key(self, row):
        """Generate the unique key
        """
        return '_'.join([str(row[i]) for i in self.unique_by]) if self.unique_by else str(row)

    def _remove_invalid_rows(self, file, **argv):
        """Remove invalid csv rows e.g. newline inside string
        """
        if os.path.exists(file):
            file_obj = open(file)
            tmp_file = file + '.tmp'
            tmp_file_obj = open(tmp_file, 'wb')
            writer = csv.writer(tmp_file_obj, **argv)
            try:
                for row in csv.reader(file_obj):
                    writer.writerow(row)
            except Exception, e:
                pass
            file_obj.close()
            tmp_file_obj.close()
            os.remove(file)
            os.rename(tmp_file, file)

    def _cell(self, s):
        """Normalize the content for this cell
        """
        if isinstance(s, basestring):
            if isinstance(s, unicode):
                s = s.encode(self.encoding, 'ignore')
        elif s is None:
            s = ''
        else:
            s = str(s)
        return s

    def writerow(self, row):
        """Write row to output
        """
        row = [self._cell(col) for col in row]
        if self.unique:
            if self._unique_key(row) not in self.rows:
                self.writer.writerow(row)
                self.rows[self._unique_key(row)] = True
        else:
            self.writer.writerow(row)
            
    def writerows(self, rows):
        """Write multiple rows to output
        """
        for row in rows:
            self.writerow(row)

    def flush(self):
        """Flush output to disk
        """
        self.fp.flush()
        if hasattr(self.fp, 'fileno'):
            # this is a real file
            os.fsync(self.fp.fileno())
        
    def close(self):
        """Close the output file pointer
        """
        self.fp.close()



# decrypt chrome cookies
class Chrome:
    def __init__(self):
        import keyring
        from Crypto.Protocol.KDF import PBKDF2
        salt = b'saltysalt'
        length = 16
        # If running Chrome on OSX
        if sys.platform == 'darwin':
            my_pass = keyring.get_password('Chrome Safe Storage', 'Chrome')
            my_pass = my_pass.encode('utf8')
            iterations = 1003
            self.cookie_file = os.path.expanduser('~/Library/Application Support/Google/Chrome/Default/Cookies')

        # If running Chromium on Linux
        elif 'linux' in sys.platform:
            my_pass = 'peanuts'.encode('utf8')
            iterations = 1
            self.cookie_file = os.path.expanduser('~/.config/chromium/Default/Cookies')
        else: 
            raise Exception("This script only works on OSX or Linux.")
        self.key = PBKDF2(my_pass, salt, length, iterations)
    
    def decrypt(self, value, encrypted_value):
        if value or (encrypted_value[:3] != b'v10'):
            return value
    
        from Crypto.Cipher import AES
        
        # Encrypted cookies should be prefixed with 'v10' according to the 
        # Chromium code. Strip it off.
        encrypted_value = encrypted_value[3:]
 
        # Strip padding by taking off number indicated by padding
        # eg if last is '\x0e' then ord('\x0e') == 14, so take off 14.
        # You'll need to change this function to use ord() for python2.
        def clean(x):
            return x[:-ord(x[-1])].decode('utf8')

        iv = b' ' * 16
        cipher = AES.new(self.key, AES.MODE_CBC, IV=iv)
        decrypted = cipher.decrypt(encrypted_value)
        return clean(decrypted)


# XXX merge common parts with firefox
def chrome_cookie(filename=None, tmp_sqlite_file='cookies.sqlite', tmp_cookie_file='cookies.txt'):
    if filename is None:
        filename = os.path.expanduser("~/.config/google-chrome/Default/Cookies")
    if not os.path.exists(filename):
        raise WebScrapingError('Can not find chrome cookie file')

    open(tmp_sqlite_file, 'wb').write(open(filename, 'rb').read())
    con = sqlite3.connect(tmp_sqlite_file)
    cur = con.cursor()
    cur.execute('SELECT host_key, path, secure, expires_utc, name, value, encrypted_value FROM cookies;')
    # create standard cookies file that can be interpreted by cookie jar 
    # XXX change to create directly without temp file
    fp = open(tmp_cookie_file, 'w')
    fp.write('# Netscape HTTP Cookie File\n')
    fp.write('# http://www.netscape.com/newsref/std/cookie_spec.html\n')
    fp.write('# This is a generated file!  Do not edit.\n')
    ftstr = ['FALSE', 'TRUE']
    chrome = Chrome()
    for item in cur.fetchall():
        value = chrome.decrypt(item[5], item[6])
        row = u'%s\t%s\t%s\t%s\t%s\t%s\t%s\n' % (item[0], ftstr[item[0].startswith('.')], item[1], ftstr[item[2]], item[3], item[4], value)
        fp.write(row)

    fp.close()
    # close the connection before delete the sqlite file
    con.close()
    os.remove(tmp_sqlite_file)
    
    cookie_jar = cookielib.MozillaCookieJar()
    cookie_jar.load(tmp_cookie_file)
    os.remove(tmp_cookie_file)

    return cookie_jar



def firefox_cookie(file=None, tmp_sqlite_file='cookies.sqlite', tmp_cookie_file='cookies.txt'):
    """Create a cookie jar from this FireFox 3 sqlite cookie database

    >>> cj = firefox_cookie()
    >>> opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))
    >>> url = 'http://code.google.com/p/webscraping'
    >>> html = opener.open(url).read()
    """
    if file is None:
        try:
            # add Windows version support
            file = (glob.glob(os.path.join(os.environ.get('PROGRAMFILES', ''), 'Mozilla Firefox/profile/cookies.sqlite')) or \
                    glob.glob(os.path.join(os.environ.get('PROGRAMFILES(X86)', ''), 'Mozilla Firefox/profile/cookies.sqlite')) or \
                    glob.glob(os.path.expanduser('~/.mozilla/firefox/*.default/cookies.sqlite')) or \
                    glob.glob(os.path.expanduser(r'~\AppData\Roaming\Mozilla\Firefox\Profiles\*.default\cookies.sqlite')))[0]
        except IndexError:
            raise WebScrapingError('Can not find filefox cookie file')

    # copy firefox cookie file locally to avoid locking problems
    open(tmp_sqlite_file, 'wb').write(open(file, 'rb').read())
    con = sqlite3.connect(tmp_sqlite_file)
    cur = con.cursor()
    cur.execute('select host, path, isSecure, expiry, name, value from moz_cookies')

    # create standard cookies file that can be interpreted by cookie jar 
    fp = open(tmp_cookie_file, 'w')
    fp.write('# Netscape HTTP Cookie File\n')
    fp.write('# http://www.netscape.com/newsref/std/cookie_spec.html\n')
    fp.write('# This is a generated file!  Do not edit.\n')
    ftstr = ['FALSE', 'TRUE']
    for item in cur.fetchall():
        row = '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' % (item[0], ftstr[item[0].startswith('.')], item[1], ftstr[item[2]], item[3], item[4], item[5])
        fp.write(row)

    # session cookies are saved into sessionstore.js
    session_cookie_path = os.path.join(os.path.dirname(file), 'sessionstore.js')  
    if os.path.exists(session_cookie_path):  
        try:  
            json_data = json.loads(open(session_cookie_path, 'rb').read().strip('()'))  
        except Exception, e:  
            print str(e)
        else:
            ftstr = ['FALSE', 'TRUE']
            if 'windows' in json_data:  
                for window in json_data['windows']:
                    if 'cookies' in window:
                        for cookie in window['cookies']:
                            row = "%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (cookie.get('host', ''), ftstr[cookie.get('host', '').startswith('.')], \
                                                                    cookie.get('path', ''), False, str(int(time.time()) + 3600 * 24 * 7), \
                                                                    cookie.get('name', ''), cookie.get('value', ''))
                            fp.write(row)

    fp.close()
    # close the connection before delete the sqlite file
    con.close()
    
    cookie_jar = cookielib.MozillaCookieJar()
    cookie_jar.load(tmp_cookie_file)

    # remove temporary files
    os.remove(tmp_sqlite_file)
    os.remove(tmp_cookie_file)
    return cookie_jar


def build_opener(cj=None):
    if cj is None:
        cj = cookielib.CookieJar()
    return urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))


def start_threads(fn, num_threads=20, args=(), wait=True):
    """Shortcut to start these threads with given args and wait for all to finish
    """
    threads = [threading.Thread(target=fn, args=args) for i in range(num_threads)]
    # Start threads one by one         
    for thread in threads: 
        thread.start()
    # Wait for all threads to finish
    if wait:
        for thread in threads: 
            thread.join()


class ConsoleHandler(logging.StreamHandler):
    """Log to stderr for errors else stdout
    """
    def __init__(self):
        logging.StreamHandler.__init__(self)
        self.stream = None

    def emit(self, record):
        if record.levelno >= logging.ERROR:
            self.stream = sys.stderr
        else:
            self.stream = sys.stdout
        logging.StreamHandler.emit(self, record)


def get_logger(output_file, level=settings.log_level, maxbytes=0):
    """Create a logger instance

    output_file:
        file where to save the log
    level:
        the minimum logging level to save
    maxbytes:
        the maxbytes allowed for the log file size. 0 means no limit.
    """
    logger = logging.getLogger(output_file)
    # avoid duplicate handlers
    if not logger.handlers:
        logger.setLevel(logging.DEBUG)
        try:
            if not maxbytes:
                file_handler = logging.FileHandler(output_file)
            else:
                file_handler = logging.handlers.RotatingFileHandler(output_file, maxBytes=maxbytes)
        except IOError:
            pass # can not write file
        else:
            file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
            logger.addHandler(file_handler)

        console_handler = ConsoleHandler()
        console_handler.setLevel(level)
        logger.addHandler(console_handler)
    return logger
logger = get_logger(settings.log_file, maxbytes=2*1024*1024*1024)
