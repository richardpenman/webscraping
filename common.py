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
import threading
from datetime import datetime, timedelta
import adt
import settings


class WebScrapingError(Exception):
    pass


# known media file extensions
MEDIA_EXTENSIONS = ['ai', 'aif', 'aifc', 'aiff', 'asc', 'au', 'avi', 'bcpio', 'bin', 'c', 'cc', 'ccad', 'cdf', 'class', 'cpio', 'cpt', 'csh', 'css', 'csv', 'dcr', 'dir', 'dms', 'doc', 'drw', 'dvi', 'dwg', 'dxf', 'dxr', 'eps', 'etx', 'exe', 'ez', 'f', 'f90', 'fli', 'flv', 'gif', 'gtar', 'gz', 'h', 'hdf', 'hh', 'hqx', 'ice', 'ico', 'ief', 'iges', 'igs', 'ips', 'ipx', 'jpe', 'jpeg', 'jpg', 'js', 'kar', 'latex', 'lha', 'lsp', 'lzh', 'm', 'man', 'me', 'mesh', 'mid', 'midi', 'mif', 'mime', 'mov', 'movie', 'mp2', 'mp3', 'mpe', 'mpeg', 'mpg', 'mpga', 'ms', 'msh', 'nc', 'oda', 'pbm', 'pdb', 'pdf', 'pgm', 'pgn', 'png', 'pnm', 'pot', 'ppm', 'pps', 'ppt', 'ppz', 'pre', 'prt', 'ps', 'qt', 'ra', 'ram', 'ras', 'rgb', 'rm', 'roff', 'rpm', 'rtf', 'rtx', 'scm', 'set', 'sgm', 'sgml', 'sh', 'shar', 'silo', 'sit', 'skd', 'skm', 'skp', 'skt', 'smi', 'smil', 'snd', 'sol', 'spl', 'src', 'step', 'stl', 'stp', 'sv4cpio', 'sv4crc', 'swf', 't', 'tar', 'tcl', 'tex', 'texi', 'tif', 'tiff', 'tr', 'tsi', 'tsp', 'tsv', 'txt', 'unv', 'ustar', 'vcd', 'vda', 'viv', 'vivo', 'vrml', 'w2p', 'wav', 'wmv', 'wrl', 'xbm', 'xlc', 'xll', 'xlm', 'xls', 'xlw', 'xml', 'xpm', 'xsl', 'xwd', 'xyz', 'zip']

# tags that do not contain content
EMPTY_TAGS = 'br', 'hr', 'meta', 'link', 'base', 'img', 'embed', 'param', 'area', 'col', 'input'


def to_ascii(html):
    """Return ascii part of html
    """
    return ''.join(c for c in html if ord(c) < 128)

def to_int(s):
    """Return integer from this string

    >>> to_int('90')
    90
    >>> to_int('-90.2432')
    -90
    >>> to_int('a90a')
    90
    >>> to_int('a')
    0
    """
    return int(to_float(s))

def to_float(s):
    """Return float from this string
    """
    valid = string.digits + '.-'
    return float(''.join(c for c in s if c in valid) or 0)

    
def to_unicode(obj, encoding=settings.default_encoding):
    """Convert obj to unicode
    """
    if isinstance(obj, basestring):
        if not isinstance(obj, unicode):
            obj = obj.decode(encoding, 'ignore')
    return obj


def html_to_unicode(html, charset=settings.default_encoding):
    """Convert html to unicode, decoding by charset
    """
    m = re.compile(r'''<meta\s+http-equiv=["']Content-Type["']\s+content=["'][^"']*?charset=([a-zA-z\d\-]+)["']''', re.IGNORECASE).search(html)
    if m:
        charset = m.groups()[0].strip().lower()
        
    return to_unicode(html, charset)
    
    
def is_html(html):
    """Returns whether content is HTML
    """
    try:
        result = re.search('html|head|body', html) is not None
    except TypeError:
        result = False
    return result


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
    """
    html = re.sub('<(%s)[^>]*>' % '|'.join(EMPTY_TAGS), '', html)
    if not keep_children:
        # XXX does not work for multiple nested tags
        html = re.compile('<.*?>(.*?)</.*?>', re.DOTALL).sub('', html)
    return re.compile('<[^<]*?>').sub('', html)
    
    
def unescape(text, encoding=settings.default_encoding, keep_unicode=False):
    """Interpret escape characters

    >>> unescape('&lt;hello&nbsp;&amp;%20world&gt;\xc2\x85')
    '<hello & world>...'
    """
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
                text = unichr(htmlentitydefs.name2codepoint[text[1:-1]])
            except KeyError:
                pass
        return text # leave as is
    try:
        text = to_unicode(text, encoding)
    except UnicodeError:
        pass
    #text = text.replace('&nbsp;', ' ').replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')
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
    """Return normalized string
    
    >>> normalize('''<span>Tel.:   029&nbsp;-&nbsp;12345678   </span>''')
    'Tel.: 029 - 12345678'
    """
    return re.sub('\s+', ' ', unescape(remove_tags(s), encoding=encoding, keep_unicode=isinstance(s, unicode))).strip()

def regex_get(html, pattern, index=None, normalized=True, flag=re.DOTALL|re.IGNORECASE):
    """Extract data with regular expression
    
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

def safe(s):
    """Return safe version of string for URLs
    
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
    """Return pretty version of paragraph for display
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


def read_list(file):
    """Return file as list if exists
    """
    l = []
    if os.path.exists(file):
        l.extend(open(file).read().splitlines())
    else:
        logger.debug('%s not found' % file)
    return l


class UnicodeWriter(object):
    """A CSV writer that produces Excel-compatibly CSV files from unicode data.
    
    file can either be a filename or a file object

    >>> from StringIO import StringIO
    >>> fp = StringIO()
    >>> writer = UnicodeWriter(fp, quoting=csv.QUOTE_MINIMAL)
    >>> writer.writerow(['a', '1'])
    >>> writer.flush()
    >>> fp.seek(0)
    >>> fp.read().strip()
    'a,1'
    """
    def __init__(self, file, encoding=settings.default_encoding, mode='wb', unique=False, quoting=csv.QUOTE_ALL, utf8_bom=False, unescape=True, **argv):
        self.encoding = encoding
        self.unique = unique
        self.unescape = unescape
        if hasattr(file, 'write'):
            self.fp = file
        else:
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
                self.rows[str(row)] = True
        self.writer = csv.writer(self.fp, quoting=quoting, **argv)

    def cell(self, s):
        if isinstance(s, basestring):
            if isinstance(s, unicode):
                s = s.encode(self.encoding, 'ignore')
            if self.unescape:
                s = unescape(s, self.encoding)
        elif s is None:
            s = ''
        else:
            s = str(s)
        return s

    def writerow(self, row):
        row = [self.cell(col) for col in row]
        if self.unique:
            if str(row) not in self.rows:
                self.writer.writerow(row)
                self.rows[str(row)] = True
        else:
            self.writer.writerow(row)
            
    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

    def flush(self):
        self.fp.flush()
        
    def close(self):
        self.fp.close()


def firefox_cookie(file=None, tmp_sqlite_file='cookies.sqlite', tmp_cookie_file='cookies.txt'):
    """Create a cookie jar from this FireFox 3 sqlite cookie database

    >>> cj = firefox_cookie()
    >>> opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))
    >>> url = 'http://code.google.com/p/webscraping'
    >>> html = opener.open(url).read()
    """
    # XXX remove temporary files
    if file is None:
        try:
            file = glob.glob(os.path.expanduser('~/.mozilla/firefox/*.default/cookies.sqlite'))[0]
        except IndexError:
            raise WebScrapingError('Can not find filefox cookie file')

    import sqlite3 
    # copy firefox cookie file locally to avoid locking problems
    open(tmp_sqlite_file, 'w').write(open(file).read())
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
        #print row
    fp.close()

    cookie_jar = cookielib.MozillaCookieJar()
    cookie_jar.load(tmp_cookie_file)
    return cookie_jar


def start_threads(fun, num_threads=20, args=(), wait=True):
    """Start up threads
    """
    threads = [threading.Thread(target=fun, args=args) for i in range(num_threads)]
    # Start threads one by one         
    for thread in threads: 
        thread.start()
    # Wait for all threads to finish
    if wait:
        for thread in threads: 
            thread.join()


def get_logger(output_file=settings.log_file, stdout=True, level=settings.log_level):
    """Create a logger instance
    """
    logger = logging.getLogger(output_file)
    # void duplicate handlers
    if not logger.handlers:
        try:
            file_handler = logging.FileHandler(output_file)
        except IOError:
            pass # can not write file
        else:
            file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
            logger.addHandler(file_handler)
        if stdout:
            logger.addHandler(logging.StreamHandler())
        logger.setLevel(level)
    return logger
logger = get_logger()
