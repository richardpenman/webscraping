#
# Description: Helper methods to download and crawl web content using threads
# Author: Richard Penman (richard@sitescraper.net)
#

import os
import gzip
import re
import time
import random
import urllib2
from urlparse import urljoin
from datetime import datetime
from StringIO import StringIO
import socket
socket.setdefaulttimeout(30)
from threading import Thread
import Queue
from robotparser import RobotFileParser

from common import to_ascii, extract_domain
from pdict import PersistentDict

DEBUG = True


class Download(object):
    DEFAULT_USER_AGENT = 'Mozilla/5.0'
    DEFAULT_CACHE_FILE = 'cache.db'
    # known non-html extensions to avoid when crawling
    MEDIA_EXTENSIONS = '.ai', '.aif', '.aifc', '.aiff', '.asc', '.au', '.avi', '.bcpio', '.bin', '.c', '.cc', '.ccad', '.cdf', '.class', '.cpio', '.cpt', '.csh', '.css', '.csv', '.dcr', '.dir', '.dms', '.doc', '.drw', '.dvi', '.dwg', '.dxf', '.dxr', '.eps', '.etx', '.exe', '.ez', '.f', '.f90', '.fli', '.flv', '.gif', '.gtar', '.gz', '.h', '.hdf', '.hh', '.hqx', 'ice', '.ico', '.ief', '.iges', '.igs', '.ips', '.ipx', '.jpe', '.jpeg', '.jpg', '.js', '.kar', '.latex', '.lha', '.lsp', '.lzh', '.m', '.man', '.me', '.mesh', '.mid', '.midi', '.mif', '.mime', '.mov', '.movie', '.mp2', '.mp3', '.mpe', '.mpeg', '.mpg', '.mpga', '.ms', '.msh', '.nc', '.oda', '.pbm', '.pdb', '.pdf', '.pgm', '.pgn', '.png', '.pnm', '.pot', '.ppm', '.pps', '.ppt', '.ppz', '.pre', '.prt', '.ps', '.qt', '.ra', '.ram', '.ras', '.rgb', '.rm', '.roff', '.rpm', '.rtf', '.rtx', '.scm', '.set', '.sgm', '.sgml', '.sh', '.shar', '.silo', '.sit', '.skd', '.skm', '.skp', '.skt', '.smi', '.smil', '.snd', '.sol', '.spl', '.src', '.step', '.stl', '.stp', '.sv4cpio', '.sv4crc', '.swf', '.t', '.tar', '.tcl', '.tex', '.texi', '.tif', '.tiff', '.tr', '.tsi', '.tsp', '.tsv', '.txt', '.unv', '.ustar', '.vcd', '.vda', '.viv', '.vivo', '.vrml', '.w2p', '.wav', '.wrl', '.xbm', '.xlc', '.xll', '.xlm', '.xls', '.xlw', '.xml', '.xpm', '.xsl', '.xwd', '.xyz', '.zip'


    def __init__(self, cache_file=DEFAULT_CACHE_FILE, user_agent=DEFAULT_USER_AGENT, delay=5, proxy=None, opener=None, 
            headers=None, data=None, use_cache=True, use_remote=True, retry=False, force_html=False, force_ascii=False, max_size=None):
        """
        cache_file sets where to store cached data
        user_agent sets the user_agent to download content with
        delay is the minimum amount of time (in seconds) to wait after downloading content from this domain
        proxy is a proxy to download content through. If a list is passed then will cycle through list.
        opener sets an optional opener to use instead of using urllib2 directly
        headers are the headers to include in the request
        data is what to post at the URL
        retry sets whether to try downloading webpage again if got error last time
        force_html sets whether to download non-text data
        force_ascii sets whether to only return ascii characters
        use_cache determines whether to load from cache if exists
        use_remote determines whether to download from remote website if not in cache
        max_size determines maximum number of bytes that will be downloaded
        """
        self.cache = PersistentDict(cache_file)
        self.delay = delay
        self.proxy = proxy
        self.user_agent = user_agent
        self.opener = opener
        self.headers = headers
        self.data = data
        self.use_cache = use_cache
        self.use_remote = use_remote
        self.retry = retry
        self.force_html = force_html
        self.force_ascii = force_ascii
        self.max_size = max_size


    def get(self, url, **kwargs):
        """Download this URL and return the HTML. Data is cached so only have to download once.

        url is what to download
        kwargs can override any of the arguments passed to constructor
        """
        delay = kwargs.get('delay', self.delay)
        proxy = self.get_proxy(kwargs.get('proxy', self.proxy))
        user_agent = kwargs.get('user_agent', self.user_agent)
        opener = kwargs.get('opener', self.opener)
        headers = kwargs.get('headers', self.headers)
        data = kwargs.get('data', self.data)
        use_cache = kwargs.get('use_cache', self.use_cache)
        use_remote = kwargs.get('use_remote', self.use_remote)
        retry = kwargs.get('retry', self.retry)
        force_html = kwargs.get('force_html', self.force_html)
        force_ascii = kwargs.get('force_ascii', self.force_ascii)
        max_size = kwargs.get('max_size', self.max_size)

        key = url + ' ' + data if data else url
        if use_cache and key in self.cache:
            html = self.cache[key]
            if retry and not html:
                if DEBUG: print 'Redownloading'
            else:
                return html
        if not use_remote:
            return '' # do not try downloading but return empty

        self.domain_delay(url, delay=delay, proxy=proxy) # crawl slowly for each domain to reduce risk of being blocked
        html = self.fetch(url, headers=headers, data=data, proxy=proxy, user_agent=user_agent, opener=opener)
        if max_size is not None and len(html) > max_size:
            html = '' # too big to store
        elif force_html and not re.search('html|head|body', html):
            html = '' # non-html content
        elif force_ascii:
            html = to_ascii(html) # remove non-ascii characters
        self.cache[key] = html
        return html


    def fetch(self, url, headers=None, data=None, proxy=None, user_agent='', opener=None):
        """Simply download the url and return the content
        """
        if DEBUG: print url
        opener = opener or urllib2.build_opener()
        if proxy:
            opener.add_handler(urllib2.ProxyHandler({'http' : proxy}))
        headers = headers or {'User-agent': user_agent or Download.DEFAULT_USER_AGENT, 'Accept-encoding': 'gzip', 'Referrer': url}
        try:
            response = opener.open(urllib2.Request(url, data, headers))
            content = response.read()
            if response.headers.get('content-encoding') == 'gzip':
                # data came back gzip-compressed so decompress it          
                content = gzip.GzipFile(fileobj=StringIO(content)).read()
            #url = response.url
        except Exception, e:
            # so many kinds of errors are possible here so just catch them all
            if DEBUG: print e
            content = ''
        return content


    def domain_delay(self, url, delay, proxy=None, variance=0.5):
        """Delay a minimum time for each domain per proxy by storing last access times in a pdict

        url is what intend to download
        delay is the minimum amount of time (in seconds) to wait after downloading content from this domain
        variance is the amount of randomness in delay, 0-1
        """
        key = str(proxy) + ':' + extract_domain(url)
        if key in self.cache:
            dt = datetime.now() - self.cache[key]
            wait_secs = delay - dt.days * 24 * 60 * 60 - dt.seconds
            if wait_secs > 0:
                # randomize the time so less suspicious
                wait_secs = wait_secs - variance * delay + (2 * variance * delay * random.random())
                time.sleep(max(0, wait_secs)) # make sure isn't negative time
        self.cache[key] = datetime.now() # update database timestamp to now


    def get_proxy(self, proxies):
        if proxies and isinstance(proxies, list):
            proxy = proxies.pop(0)
            proxies.append(proxy)
        else:
            proxy = proxies
        return proxy


    def crawl(self, seed_url, max_urls=30, max_depth=1, obey_robots=False, max_size=1000000, force_html=True, return_html=False, **kwargs):
        """Crawl website html and return list of URLs crawled

        seed_url: url to start crawling from
        max_urls: maximum number of URLs to crawl (use None for no limit)
        max_depth: maximum depth to follow links into website (use None for no limit)
        obey_robots: whether to obey robots.txt
        max_size is passed to get() and is limited to 1MB by default
        force_text is passed to get() and is set to True by default so only crawl HTML content
        **kwargs is passed to get()
        """
        user_agent = kwargs.get('user_agent', self.user_agent)
        server = 'http://' + extract_domain(seed_url)
        robots = RobotFileParser()
        if obey_robots:
            robots.parse(self.get(server + '/robots.txt').splitlines()) # load robots.txt
        outstanding = [(seed_url, 0), (server, 0)] # which URLs need to crawl
        crawled = {} if return_html else [] # urls that have crawled

        while outstanding: 
            # more URLs to crawl
            if len(crawled) == max_urls:
                break
            url, cur_depth = outstanding.pop(0)
            if url not in crawled:
                html = self.get(url, max_size=max_size, force_html=force_html, **kwargs)
                if return_html:
                    crawled[url] = html
                else:
                    crawled.append(url)
                if max_depth is None or cur_depth < max_depth:
                    # continue crawling
                    for scraped_url in re.findall(re.compile('<a[^>]+href=["\'](.*?)["\']', re.IGNORECASE), html):
                        if '#' in scraped_url:
                            scraped_url = scraped_url[:scraped_url.index('#')] # remove internal links to prevent duplicates
                        if os.path.splitext(scraped_url)[-1].lower() not in Download.MEDIA_EXTENSIONS and robots.can_fetch(user_agent, scraped_url):
                            scraped_url = urljoin(url, scraped_url) # support relative links
                            # check if same domain or sub-domain
                            this_server = extract_domain(scraped_url)
                            if this_server and (this_server in server or server in this_server):
                                outstanding.append((scraped_url, cur_depth+1))
        return crawled



def threaded_get(urls, proxies=[None], return_html=False, **kwargs):
    """Download these urls in parallel

    urls are the webpages to download
    proxies is a list of servers to download content via
        To use the same proxy in parallel provide it multiple times in the proxy list
        None means use no proxy but connect directly
    if return_html is True then returns list of htmls in same order as urls
        be careful of the memory this will take up when urls is large
    """
    class Helper(Thread):
        def __init__(self, urls, proxy):
            Thread.__init__(self)
            self.urls, self.proxy, self.results = urls, proxy, {}

        def run(self):
            # XXX separate thread for puting in database
            # XXX combine with threaded_crawl helper
            # XXX change to process?
            d = Download(proxy=self.proxy, **kwargs)
            try:
                while 1:
                    url = self.urls.get(block=False)
                    html = d.get(url, **kwargs)
                    if return_html:
                        self.results[url] = html
            except Queue.Empty:
                pass # finished

    # put urls into thread safe queue
    queue = Queue.Queue()
    for url in urls:
        queue.put(url)

    downloaders = []
    for proxy in proxies:
        downloader = Helper(queue, proxy)
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
    class Helper(Thread):
        count = 0

        def __init__(self, urls):
            Thread.__init__(self)
            self.urls, self.results = urls, {}
            self.id = Helper.count
            Helper.count += 1

        def run(self):
            d = Download(**kwargs)
            try:
                while 1:
                    url = self.urls.get(block=False)
                    if DEBUG: print self.id, 'crawl', url
                    urls = d.crawl(url, max_urls=max_urls, max_depth=max_depth, **kwargs)
                    self.results[url] = urls
            except Queue.Empty:
                pass # finished

    # randomize url order to balance requests across domains
    random.shuffle(seed_urls)
    # put urls into thread safe queue
    queue = Queue.Queue()
    for url in seed_urls:
        queue.put(url)
    crawlers = []
    for i in range(num_threads):
        crawler = Helper(queue)
        crawlers.append(crawler)
        crawler.start()

    results = {}
    for crawler in crawlers:
        crawler.join()
        results = dict(results, **crawler.results)
    return results
