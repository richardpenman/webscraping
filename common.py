#
# Common web scraping related functions
#
#

import os
import gzip
import re
import time
import urllib
import urllib2
from datetime import datetime, timedelta
from urlparse import urlparse
import string
from StringIO import StringIO
import htmlentitydefs
import socket
import tempfile
from threading import Thread
import Queue
import cookielib
import shutil




def download(url, delay=3, headers=None, output_dir='.', use_cache=True, use_remote=True, retry=False, proxy=None, flatten_path=False, ascii=True, opener=None):
    """Download this URL and return the HTML. Files are cached so only have to download once.

    url is what to download
    delay is the amount of time to delay after downloading
    output_dir is where to store cached files
    use_cache determines whether to load from cache if exists
    use_remote determines whether to download from remote website if not in cache
    retry sets whether to try downloading webpage again if got error last time
    proxy is a proxy to download content through
    flatten_path will store file beneath directory rather than creating full nested structure
    ascii sets whether to only return ascii characters
    opener sets an optional opener to use
    """
    socket.setdefaulttimeout(20)
    output_file = url_dir(url, output_dir=output_dir, flatten_path=flatten_path)
    if use_cache and os.path.exists(output_file):
        html = open(output_file).read()
        if html or not retry:
            return html
        else:
            print 'Redownloading'
    if not use_remote:
        return ''

    # need to download file
    print url
    # crawl slowly to reduce risk of being blocked
    if delay > 0: time.sleep(delay) 
    # set the user agent and compression for url requests
    headers = headers or {'User-agent': 'Mozilla/5.0', 'Accept-encoding': 'gzip'}
    opener = opener or urllib2.build_opener()
    if proxy:
        opener.add_handler(urllib2.ProxyHandler({'http' : proxy}))
    try:
        response = opener.open(urllib2.Request(url, None, headers))
    except urllib2.URLError, e:
        # create empty file, so don't repeat downloading again
        print e
        html = ''
        open(output_file, 'w').write(html)
    else:
        # download completed successfully
        try:
            html = response.read()
        except socket.timeout:
            html = ''
        else:
            if response.headers.get('content-encoding') == 'gzip':
                # data came back gzip-compressed so decompress it          
                html = gzip.GzipFile(fileobj=StringIO(html)).read()
            # achieve atomic write by moving temporary file
            temp = tempfile.NamedTemporaryFile(delete=False)
            temp.file.write(html)
            temp.file.close()
            shutil.move(temp.name, output_file) 
            #open(output_file, 'w').write(html)
    return to_ascii(html) if html else html



def url_dir(url, output_dir, flatten_path=False):
    """Return filepath where to store URL and create folders if necessary
    """
    scheme, netloc, path, params, query, fragment = urlparse(url)
    if not path:
        path = '/'
    if path.endswith('/'):
        path += 'index.html'
    output_file = netloc + ('/' + path[1:].replace('/', '_') if flatten_path else path) + ('?' + query if query else '')
    output_file = os.path.join(output_dir, output_file)
    try:
        os.makedirs(os.path.dirname(output_file))
    except OSError, e:
        if not os.path.exists(os.path.dirname(output_file)):
            raise e
    return output_file


def threaded_download(urls, proxies=[None], use_memory=True, **kwargs):
    """Download these urls in parallel using the given list of proxies 
    To use the same proxy multiple times in parallel provide it multiple times
    None means use no proxy

    if use_memory is True then returns list of htmls in same order as urls
    """
    class Downloader(Thread):
        def __init__(self, id, urls, proxy):
            Thread.__init__(self)
            self.id, self.urls, self.proxy, self.results = id, urls, proxy, {}

        def run(self):
            try:
                while 1:
                    url = self.urls.get(block=False)
                    html = download(url, proxy=self.proxy, **kwargs)
                    if use_memory:
                        self.results[url] = html
            except Queue.Empty:
                pass # finished

    # put urls into thread safe queue
    queue = Queue.Queue()
    for url in urls:
        queue.put(url)

    downloaders = []
    for id, proxy in enumerate(proxies):
        downloader = Downloader(id, queue, proxy)
        downloaders.append(downloader)
        downloader.start()

    results = {}
    for downloader in downloaders:
        downloader.join()
        results = dict(results, **downloader.results)
    if use_memory:
        return [results[url] for url in urls]


def to_ascii(html):
    #html = html.decode('utf-8')
    return ''.join(c for c in html if ord(c) < 128)

def to_int(s):
    """Return integer from this string
    """
    return int('0' + ''.join(c for c in s if c.isdigit()))



def unique(l):
    """Remove duplicates from list, while maintaining order
    """
    checked = []
    for e in l:
        if e not in checked:
            checked.append(e)
    return checked


def flatten(ls):
    """Flatten sub lists into single list
    """
    return [e for l in ls for e in l]


def remove_tags(html, keep_children=True):
    """Remove HTML tags leaving just text
    If keep children is True then keep text within child tags
    """
    if not keep_children:
        html = re.compile('<.*?>.*?</.*?>', re.DOTALL).sub('', html)
    return re.compile('<[^<]*?>').sub('', html)


def select_options(html, attributes=''):
    """Extract options from HTML select box with given attributes
    >>> html = "Go: <select id='abc'><option value='1'>a</option><option value='2'>b</option></select>"
    >>> select_options(html, "id='abc'")
    [('1', 'a'), ('2', 'b')]
    """
    select_re = re.compile('<select[^>]*?%s[^>]*?>.*?</select>' % attributes, re.DOTALL)
    option_re = re.compile('<option[^>]*?value=[\'"](.*?)[\'"][^>]*?>(.*?)</option>', re.DOTALL)
    try:
        select_html = select_re.findall(html)[0]
    except IndexError:
        return []
    else:
        return option_re.findall(select_html)
    

def unescape(text, encoding='utf-8'):
    def fixup(m):
        text = m.group(0)
        if text[:2] == "&#":
            # character reference
            try:
                if text[:3] == "&#x":
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
    text = text.replace('&nbsp;', ' ').replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')
    return re.sub("&#?\w+;", fixup, urllib.unquote(text.decode(encoding))).encode(encoding)


def extract_domain(url):
    """Extract the domain from the given URL

    >>> extract_domain('http://www.google.com.au/tos.html')
    'google.com.au'
    """
    suffixes = 'ac', 'ad', 'ae', 'aero', 'af', 'ag', 'ai', 'al', 'am', 'an', 'ao', 'aq', 'ar', 'arpa', 'as', 'asia', 'at', 'au', 'aw', 'ax', 'az', 'ba', 'bb', 'bd', 'be', 'bf', 'bg', 'bh', 'bi', 'biz', 'bj', 'bm', 'bn', 'bo', 'br', 'bs', 'bt', 'bv', 'bw', 'by', 'bz', 'ca', 'cat', 'cc', 'cd', 'cf', 'cg', 'ch', 'ci', 'ck', 'cl', 'cm', 'cn', 'co', 'com', 'coop', 'cr', 'cu', 'cv', 'cx', 'cy', 'cz', 'de', 'dj', 'dk', 'dm', 'do', 'dz', 'ec', 'edu', 'ee', 'eg', 'er', 'es', 'et', 'eu', 'fi', 'fj', 'fk', 'fm', 'fo', 'fr', 'ga', 'gb', 'gd', 'ge', 'gf', 'gg', 'gh', 'gi', 'gl', 'gm', 'gn', 'gov', 'gp', 'gq', 'gr', 'gs', 'gt', 'gu', 'gw', 'gy', 'hk', 'hm', 'hn', 'hr', 'ht', 'hu', 'id', 'ie', 'il', 'im', 'in', 'info', 'int', 'io', 'iq', 'ir', 'is', 'it', 'je', 'jm', 'jo', 'jobs', 'jp', 'ke', 'kg', 'kh', 'ki', 'km', 'kn', 'kp', 'kr', 'kw', 'ky', 'kz', 'la', 'lb', 'lc', 'li', 'lk', 'lr', 'ls', 'lt', 'lu', 'lv', 'ly', 'ma', 'mc', 'md', 'me', 'mg', 'mh', 'mil', 'mk', 'ml', 'mm', 'mn', 'mo', 'mobi', 'mp', 'mq', 'mr', 'ms', 'mt', 'mu', 'mv', 'mw', 'mx', 'my', 'mz', 'na', 'name', 'nc', 'ne', 'net', 'nf', 'ng', 'ni', 'nl', 'no', 'np', 'nr', 'nu', 'nz', 'om', 'org', 'pa', 'pe', 'pf', 'pg', 'ph', 'pk', 'pl', 'pm', 'pn', 'pr', 'pro', 'ps', 'pt', 'pw', 'py', 'qa', 're', 'ro', 'rs', 'ru', 'rw', 'sa', 'sb', 'sc', 'sd', 'se', 'sg', 'sh', 'si', 'sj', 'sk', 'sl', 'sm', 'sn', 'so', 'sr', 'st', 'su', 'sv', 'sy', 'sz', 'tc', 'td', 'tel', 'tf', 'tg', 'th', 'tj', 'tk', 'tl', 'tm', 'tn', 'to', 'tp', 'tr', 'tt', 'tv', 'tw', 'tz', 'ua', 'ug', 'uk', 'us', 'uy', 'uz', 'va', 'vc', 've', 'vg', 'vi', 'vn', 'vu', 'wf', 'ws', 'xn', 'ye', 'yt', 'za', 'zm', 'zw'
    url = re.sub('^.*://', '', url).partition('/')[0].lower()
    domain = []
    for section in url.split('.'):
        if section in suffixes:
            domain.append(section)
        else:
            domain = [section]
    return '.'.join(domain)


def pretty_duration(dt):
    """Return english description of this time difference
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


def firefox_cookie(file):
    """Create a cookie jar from this FireFox 3 sqlite cookie database
    """
    import sqlite3 
    # copy firefox cookie file locally to avoid locking problems
    sqlite_file = 'cookies.sqlite'
    open(sqlite_file, 'w').write(open(file).read())
    con = sqlite3.connect(sqlite_file)
    cur = con.cursor()
    cur.execute('select host, path, isSecure, expiry, name, value from moz_cookies')

    # create standard cookies file that can be interpreted by cookie jar 
    cookie_file = 'cookies.txt'
    fp = open(cookie_file, 'w')
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
    cookie_jar.load(cookie_file)
    return cookie_jar
