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
import string
from StringIO import StringIO
import htmlentitydefs
import socket
socket.setdefaulttimeout(20)



def download(url, delay=3, output_dir='.', use_cache=True):
    """Download this URL and return the HTML. Files are cached so only have to download once.
    sleep_secs is the amount of time to delay after downloading.
    """
    output_file = url.replace('http:/', output_dir)
    if use_cache and os.path.exists(output_file):
        html = open(output_file).read()
        if not html:
            raise urllib2.HTTPError('Empty file')
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
        except urllib2.HTTPError, e:
            # create empty file, so don't repeat downloading again
            open(output_file, 'w').write('')
            raise e
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
