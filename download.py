#
# Common web scraping related functions
#
#

import os
import gzip
import re
import time
import urllib2
from datetime import datetime
from urlparse import urlparse
from StringIO import StringIO
import socket
from threading import Thread
import Queue
from robotparser import RobotFileParser

from common import to_ascii, extract_domain
from pdict import PersistentDict


DEFAULT_USER_AGENT = 'Mozilla/5.0'
DEFAULT_CACHE_FILE = 'cache.db'
DEFAULT_DOMAIN_FILE = '/tmp/domains.db'



def get(url, 
        delay=5, headers=None, data=None, proxy=None, user_agent=DEFAULT_USER_AGENT,
        cache_file=DEFAULT_CACHE_FILE, domain_file=DEFAULT_DOMAIN_FILE, use_cache=True, use_remote=True, 
        retry=False, opener=None, force_text=False, force_ascii=True, max_size=None):
    """Download this URL and return the HTML. Data is cached so only have to download once.

    url is what to download
    delay is the minimum amount of time (in seconds) to wait after downloading content from this domain
    headers are the headers to include in the request
    data is what to post at the URL
    proxy is a proxy to download content through
    retry sets whether to try downloading webpage again if got error last time
    force_text sets whether to download non-text data
    force_ascii sets whether to only return ascii characters
    opener sets an optional opener to use instead of using urllib2 directly
    user_agent sets the user_agent to download content with
    cache_file sets where to store cached data
    domain_file sets where to store domain statistics data
    use_cache determines whether to load from cache if exists
    use_remote determines whether to download from remote website if not in cache
    max_size determines maximum number of bytes that will be downloaded
    """
    socket.setdefaulttimeout(20)
    cache = PersistentDict(cache_file)
    if use_cache and url in cache:
        html = cache[url]
        if retry and not html:
            print 'Redownloading'
        else:
            return html
    if not use_remote:
        return '' # do not try downloading but return empty

    print url # need to download url
    domain_delay(url, delay, domain_file) # crawl slowly for each domain to reduce risk of being blocked
    opener = opener or urllib2.build_opener()
    if proxy:
        opener.add_handler(urllib2.ProxyHandler({'http' : proxy}))
    headers = headers or {'User-agent': user_agent, 'Accept-encoding': 'gzip'}
    try:
        response = opener.open(urllib2.Request(url, data, headers))
    except urllib2.URLError, e:
        print e
        html = ''
    else:
        if force_text and 'text' not in response.info().dict.get('content-type', 'unknown'):
            html = '' # only use content if type is text/html or text/plain
        else:
            try:
                html = response.read()
            except socket.timeout:
                html = ''
            else:
                if response.headers.get('content-encoding') == 'gzip':
                    # data came back gzip-compressed so decompress it          
                    html = gzip.GzipFile(fileobj=StringIO(html)).read()
                if max_size is not None and len(html) > max_size:
                    html = '' # too big to store
                elif force_ascii:
                    html = to_ascii(html) # remove non-ascii characters
    cache[url] = html
    return html



def domain_delay(url, delay, domain_file):
    """Make sure to delay a minimum time for each domain by storing last access times in a pdict

    url is what intend to download
    delay is the minimum amount of time (in seconds) to wait after downloading content from this domain
    domain_file sets where to store domain statistics data
    """
    cache = PersistentDict(domain_file)
    domain = extract_domain(url)
    if domain in cache:
        dt = datetime.now() - cache[domain]
        wait_secs = delay - dt.days * 24 * 60 * 60 - dt.seconds
        if wait_secs > 0:
            time.sleep(wait_secs)
    cache[domain] = datetime.now() # update database to now



def crawl(seed_url, max_urls=30, max_depth=1, obey_robots=False, max_size=1000000, force_text=True, user_agent=DEFAULT_USER_AGENT, **kwargs):
    """Crawl website html and return list of URLs crawled

    seed_url: url to start crawling from
    max_urls: maximum number of URLs to crawl (use negative number for no limit)
    max_depth: maximum depth to follow links into website (use negative number for no limit)
    obey_robots: whether to obey robots.txt
    max_size is passed to get() and is limited to 1MB by default
    force_text is passed to get() and is set to True by default so only crawl HTML content
    user_agent is used for checking robots.txt and is passed to get()
    **kwargs is passed to get()
    """
    # known non-html extensions to avoid crawling
    ignored_extensions = '.ai', '.aif', '.aifc', '.aiff', '.asc', '.au', '.avi', '.bcpio', '.bin', '.c', '.cc', '.ccad', '.cdf', '.class', '.cpio', '.cpt', '.csh', '.css', '.csv', '.dcr', '.dir', '.dms', '.doc', '.drw', '.dvi', '.dwg', '.dxf', '.dxr', '.eps', '.etx', '.exe', '.ez', '.f', '.f90', '.fli', '.flv', '.gif', '.gtar', '.gz', '.h', '.hdf', '.hh', '.hqx', 'ice', '.ico', '.ief', '.iges', '.igs', '.ips', '.ipx', '.jpe', '.jpeg', '.jpg', '.js', '.kar', '.latex', '.lha', '.lsp', '.lzh', '.m', '.man', '.me', '.mesh', '.mid', '.midi', '.mif', '.mime', '.mov', '.movie', '.mp2', '.mp3', '.mpe', '.mpeg', '.mpg', '.mpga', '.ms', '.msh', '.nc', '.oda', '.pbm', '.pdb', '.pdf', '.pgm', '.pgn', '.png', '.pnm', '.pot', '.ppm', '.pps', '.ppt', '.ppz', '.pre', '.prt', '.ps', '.qt', '.ra', '.ram', '.ras', '.rgb', '.rm', '.roff', '.rpm', '.rtf', '.rtx', '.scm', '.set', '.sgm', '.sgml', '.sh', '.shar', '.silo', '.sit', '.skd', '.skm', '.skp', '.skt', '.smi', '.smil', '.snd', '.sol', '.spl', '.src', '.step', '.stl', '.stp', '.sv4cpio', '.sv4crc', '.swf', '.t', '.tar', '.tcl', '.tex', '.texi', '.tif', '.tiff', '.tr', '.tsi', '.tsp', '.tsv', '.txt', '.unv', '.ustar', '.vcd', '.vda', '.viv', '.vivo', '.vrml', '.w2p', '.wav', '.wrl', '.xbm', '.xlc', '.xll', '.xlm', '.xls', '.xlw', '.xml', '.xpm', '.xsl', '.xwd', '.xyz', '.zip'
    server = 'http://' + extract_domain(seed_url)
    robots = RobotFileParser()
    if obey_robots:
        robots.parse(get(server + '/robots.txt').splitlines()) # load robots.txt
    outstanding = [(seed_url, 0), (server, 0)] # which URLs need to crawl
    crawled = [] # urls that have crawled

    while outstanding: 
        # more URLs to crawl
        if len(crawled) == max_urls:
            break
        url, cur_depth = outstanding.pop(0)
        if url not in crawled:
            crawled.append(url)
            html = get(url, max_size=max_size, force_text=force_text, **kwargs)
            if cur_depth < max_depth or max_depth < 0:
                # continue crawling
                for scraped_url in re.findall(re.compile('<a[^>]+href=["\'](.*?)["\']', re.IGNORECASE), html):
                    if '#' in scraped_url:
                        scraped_url = scraped_url[:scraped_url.index('#')] # remove internal links to prevent duplicates
                    if os.path.splitext(scraped_url)[-1].lower() not in ignored_extensions and robots.can_fetch(user_agent, scraped_url):
                        scraped_url = urljoin(server, scraped_url) # support relative links
                        # check if same domain or sub-domain
                        this_server = extract_domain(scraped_url)
                        if this_server and (this_server in server or server in this_server):
                            outstanding.append((scraped_url, cur_depth+1))
    return crawled



def threaded_get(urls, proxies=[None], return_html=True, **kwargs):
    """Download these urls in parallel using the given list of proxies 
    To use the same proxy multiple times in parallel provide it multiple times
    None means use no proxy but connect directly

    if return_html is True then returns list of htmls in same order as urls
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
                    if return_html:
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
    if return_html:
        return [results[url] for url in urls]



def threaded_crawl(seed_urls, num_threads=10, max_urls=30, max_depth=1, **kwargs):
    """Crawl websites in parallel
    Returns a dict of crawled urls for each site

    seed_urls is a list of the urls to crawl from
    num_threads is the number of crawlers to run in parallel
    max_urls: maximum number of URLs to crawl
    max_depth: maximum depth to follow links into website
    **kwargs is passed to crawl()
    """
    class Crawler(Thread):
        def __init__(self, id, urls):
            Thread.__init__(self)
            self.id = id
            self.urls = urls
            self.results = {}

        def run(self):
            try:
                while 1:
                    url = self.urls.get(block=False)
                    print self.id
                    self.results[url] = crawl(url, max_urls=max_urls, max_depth=max_depth, **kwargs)
            except Queue.Empty:
                pass # finished

    # put urls into thread safe queue
    queue = Queue.Queue()
    for url in seed_urls:
        queue.put(url)
    crawlers = []
    for i in range(num_threads):
        crawler = Crawler(i, queue)
        crawlers.append(crawler)
        crawler.start()

    results = {}
    for crawler in crawlers:
        crawler.join()
        results = dict(results, **crawler.results)
    return results

