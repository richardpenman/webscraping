#
# Description:
# Common web scraping related functions
#
# Author: Richard Penman (richard@sitescraper.net)
#

import os
import re
import time
import urllib
import urlparse
import string
import htmlentitydefs
import cookielib
from datetime import datetime, timedelta



# known media file extensions
MEDIA_EXTENSIONS = ['.ai', '.aif', '.aifc', '.aiff', '.asc', '.au', '.avi', '.bcpio', '.bin', '.c', '.cc', '.ccad', '.cdf', '.class', '.cpio', '.cpt', '.csh', '.css', '.csv', '.dcr', '.dir', '.dms', '.doc', '.drw', '.dvi', '.dwg', '.dxf', '.dxr', '.eps', '.etx', '.exe', '.ez', '.f', '.f90', '.fli', '.flv', '.gif', '.gtar', '.gz', '.h', '.hdf', '.hh', '.hqx', 'ice', '.ico', '.ief', '.iges', '.igs', '.ips', '.ipx', '.jpe', '.jpeg', '.jpg', '.js', '.kar', '.latex', '.lha', '.lsp', '.lzh', '.m', '.man', '.me', '.mesh', '.mid', '.midi', '.mif', '.mime', '.mov', '.movie', '.mp2', '.mp3', '.mpe', '.mpeg', '.mpg', '.mpga', '.ms', '.msh', '.nc', '.oda', '.pbm', '.pdb', '.pdf', '.pgm', '.pgn', '.png', '.pnm', '.pot', '.ppm', '.pps', '.ppt', '.ppz', '.pre', '.prt', '.ps', '.qt', '.ra', '.ram', '.ras', '.rgb', '.rm', '.roff', '.rpm', '.rtf', '.rtx', '.scm', '.set', '.sgm', '.sgml', '.sh', '.shar', '.silo', '.sit', '.skd', '.skm', '.skp', '.skt', '.smi', '.smil', '.snd', '.sol', '.spl', '.src', '.step', '.stl', '.stp', '.sv4cpio', '.sv4crc', '.swf', '.t', '.tar', '.tcl', '.tex', '.texi', '.tif', '.tiff', '.tr', '.tsi', '.tsp', '.tsv', '.txt', '.unv', '.ustar', '.vcd', '.vda', '.viv', '.vivo', '.vrml', '.w2p', '.wav', '.wrl', '.xbm', '.xlc', '.xll', '.xlm', '.xls', '.xlw', '.xml', '.xpm', '.xsl', '.xwd', '.xyz', '.zip']


def to_ascii(html):
    """Return ascii part of html
    """
    return ''.join(c for c in html if ord(c) < 128)

def to_int(s):
    """Return integer from this string

    >>> to_int('90')
    90
    >>> to_int('a90a')
    90
    >>> to_int('a')
    0
    """
    return int(to_float(s))

def to_float(s):
    """Return float from this string
    """
    valid = string.digits + '.'
    return float('0' + ''.join(c for c in s if c in valid))


def get_extension(url):
    """Return extension from given URL

    >>> get_extension('hello_world.JPG')
    '.jpg'
    >>> get_extension('http://www.google-analytics.com/__utm.gif?utmwv=1.3&utmn=420639071')
    '.gif'
    """
    return os.path.splitext(urlparse.urlsplit(url).path)[-1].lower()


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


def flatten(ls):
    """Flatten sub lists into single list

    >>> [[1,2,3], [4,5,6], [7,8,9]]
    [1, 2, 3, 4, 5, 6, 7, 8, 9]
    """
    return [e for l in ls for e in l]


def first(l, default=''):
    """Return first element from list or default value if empty

    >>> first([1,2,3])
    1
    >>> first([], None)
    None
    """
    return l[0] if l else default
def last(l, default=''):
    """Return last element from list or default value if empty
    """
    return l[-1] if l else default


def remove_tags(html, keep_children=True):
    """Remove HTML tags leaving just text
    If keep children is True then keep text within child tags

    >>> remove_tags('hello <b>world</b>!')
    'hello world!'
    >>> remove_tags('hello <b>world</b>!', False)
    'hello !'
    """
    # XXX does not work for multiple nested tags
    if not keep_children:
        html = re.compile('<.*?>(.*?)</.*?>', re.DOTALL).sub('', html)
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
    """Interpret escape characters

    >>> unescape('&lt;hello&nbsp;&amp;&nbsp;world&gt;')
    '<hello & world>'
    """
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



def extract_emails(html):
    """Extract emails and look for common obfuscations

    >>> extract_emails('')
    []
    >>> extract_emails('hello richard@sitescraper.net world')
    ['richard@sitescraper.net']
    >>> extract_emails('hello richard@<!-- trick comment -->sitescraper.net world')
    ['richard@sitescraper.net']
    >>> extract_emails('hello richard AT sitescraper DOT net world')
    ['richard@sitescraper.net']
    """
    email_re = re.compile('[\w\.\+-]{1,64}@\w[\w\.\+-]{1,255}\.\w+')
    # remove comments, which can obfuscate emails
    html = re.compile('<!--.*?-->', re.DOTALL).sub('', html)
    emails = []
    for email in email_re.findall(html):
        if email not in emails:
            emails.append(email)
    # look for obfuscated email
    for user, domain, ext in re.compile('([\w\.\+-]{1,64}) .?AT.? ([\w\.\+-]{1,255}) .?DOT.? (\w+)', re.IGNORECASE).findall(html):
        email = '%s@%s.%s' % (user, domain, ext)
        if email not in emails:
            emails.append(email)
    return emails



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

    >>> file = os.path.expanduser('~/.mozilla/firefox/<random chars>.default/cookies.sqlite')
    >>> cj = firefox_cookie(file=file)
    >>> opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))
    >>> html = opener.open(url).read()
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
