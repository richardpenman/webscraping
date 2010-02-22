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
from urlparse import urlparse
import string
from StringIO import StringIO
import htmlentitydefs
import socket
socket.setdefaulttimeout(20)



def download(url, delay=3, output_dir='.', use_cache=True):
    """Download this URL and return the HTML. Files are cached so only have to download once.
    sleep_secs is the amount of time to delay after downloading.
    """
    scheme, netloc, path, params, query, fragment = urlparse(url)
    if path.endswith('/'):
        path = path[:-1] # remove end slash
        if not path:
            path = '/index.html' # default file
    output_file = netloc + path + ('?' + query if query else '')
    if use_cache and os.path.exists(output_file):
        html = open(output_file).read()
        if not html:
            print 'Empty file'
    else:
        print url
        if not os.path.exists(os.path.dirname(output_file)):
            os.makedirs(os.path.dirname(output_file))
        # crawl slowly to reduce risk of being blocked
        time.sleep(delay) 
        # set the user agent and compression for url requests
        headers = {'User-agent': 'Mozilla/5.0', 'Accept-encoding': 'gzip'}
        try:
            response = urllib2.urlopen(urllib2.Request(url, None, headers))
        except urllib2.URLError, e:
            # create empty file, so don't repeat downloading again
            print e
            open(output_file, 'w').write('')
            html = ''
        else:
            # download completed successfully
            html = response.read()
            if response.headers.get('content-encoding') == 'gzip':
                # data came back gzip-compressed so decompress it          
                html = gzip.GzipFile(fileobj=StringIO(html)).read()
            open(output_file, 'w').write(html)
    return to_ascii(html)


def to_ascii(html):
    #html = html.decode('utf-8')
    return ''.join(c for c in html if ord(c) < 128)


def unique(l):
    """Remove duplicates from list, while maintaining order
    """
    checked = []
    for e in l:
        if e not in checked:
            checked.append(e)
    return checked


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
    

def unescape(text):
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
    return re.sub("&#?\w+;", fixup, urllib.unquote(text))
