#
# Description: Helper methods to download and crawl web content using threads
# Author: Richard Penman (richard@sitescraper.net)
# License: LGPL
#

import os
import gzip
import re
import time
import random
import urllib
import urllib2
from urlparse import urljoin
from datetime import datetime
from StringIO import StringIO
import socket
from threading import Thread
import Queue
from robotparser import RobotFileParser
from webscraping import common, pdict, settings

DEBUG = True


class Download(object):

    def __init__(self, cache_file=None, user_agent=None, timeout=30, delay=5, proxy=None, opener=None, 
            headers=None, data=None, use_cache=True, use_remote=True, retry=False, num_retries=0, force_html=False, force_ascii=False, max_size=None):
        """
        cache_file sets where to store cached data
        user_agent sets the user_agent to download content with
        timeout is the maximum amount of time to wait for http response
        delay is the minimum amount of time (in seconds) to wait after downloading content from this domain
        proxy is a proxy to download content through. If a list is passed then will cycle through list.
        opener sets an optional opener to use instead of using urllib2 directly
        headers are the headers to include in the request
        data is what to post at the URL
        retry sets whether to try downloading webpage again if got error last time
        num_retries sets how many times to try downloading a URL after getting an error
        force_html sets whether to download non-text data
        force_ascii sets whether to only return ascii characters
        use_cache determines whether to load from cache if exists
        use_remote determines whether to download from remote website if not in cache
        max_size determines maximum number of bytes that will be downloaded
        """
        socket.setdefaulttimeout(timeout)
        self.cache = pdict.PersistentDict(cache_file or settings.cache_file)
        self.delay = delay
        self.proxy = proxy
        self.user_agent = user_agent or settings.user_agent
        self.opener = opener
        self.headers = headers
        self.data = data
        self.use_cache = use_cache
        self.use_remote = use_remote
        self.retry = retry
        self.num_retries = num_retries
        self.force_html = force_html
        self.force_ascii = force_ascii
        self.max_size = max_size
        self.final_url = None


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
        num_retries = kwargs.get('num_retries', self.num_retries)
        force_html = kwargs.get('force_html', self.force_html)
        force_ascii = kwargs.get('force_ascii', self.force_ascii)
        max_size = kwargs.get('max_size', self.max_size)

        key = url + ' ' + str(data) if data else url
        if use_cache:
            try:
                html = self.cache[key]
                if retry and not html:
                    if DEBUG: print 'Redownloading'
                else:
                    return html
            except KeyError:
                pass # have not downloaded yet
        if not use_remote:
            return '' # do not try downloading but return empty

        self.domain_delay(url, delay=delay, proxy=proxy) # crawl slowly for each domain to reduce risk of being blocked
        html = self.fetch(url, headers=headers, data=data, proxy=proxy, user_agent=user_agent, opener=opener, num_retries=num_retries)
        if max_size is not None and len(html) > max_size:
            if DEBUG: print 'Too big:', len(html)
            html = '' # too big to store
        elif force_html and not common.is_html(html):
            if DEBUG: print 'Not html'
            html = '' # non-html content
        elif force_ascii:
            html = common.to_ascii(html) # remove non-ascii characters
        self.cache[key] = html
        return html


    def fetch(self, url, headers=None, data=None, proxy=None, user_agent='', opener=None, num_retries=1):
        """Simply download the url and return the content
        """
        if DEBUG: print url
        opener = opener or urllib2.build_opener()
        if proxy:
            opener.add_handler(urllib2.ProxyHandler({'http' : proxy}))
        headers = headers or {'User-agent': user_agent or Download.DEFAULT_USER_AGENT, 'Accept-encoding': 'gzip', 'Referrer': url}
        data = urllib.urlencode(data) if isinstance(data, dict) else data
        try:
            response = opener.open(urllib2.Request(url, data, headers))
            content = response.read()
            if response.headers.get('content-encoding') == 'gzip':
                # data came back gzip-compressed so decompress it          
                content = gzip.GzipFile(fileobj=StringIO(content)).read()
            self.final_url = response.url # store where redirected to
        except Exception, e:
            # so many kinds of errors are possible here so just catch them all
            if DEBUG: print e
            if num_retries > 0:
                if DEBUG: print 'Retrying'
                content = self.fetch(url, headers, data, proxy, user_agent, opener, num_retries - 1)
            else:
                content, self.final_url = '', url
        return content


    def domain_delay(self, url, delay, proxy=None, variance=0.5):
        """Delay a minimum time for each domain per proxy by storing last access times in a pdict

        url is what intend to download
        delay is the minimum amount of time (in seconds) to wait after downloading content from this domain
        variance is the amount of randomness in delay, 0-1
        """
        key = str(proxy) + ':' + common.get_domain(url)
        if key in self.cache:
            # time since cache last accessed for this domain+proxy combination
            dt = datetime.now() - self.cache[key]
            wait_secs = delay - dt.days * 24 * 60 * 60 - dt.seconds
            # randomize the time so less suspicious
            wait_secs += (variance * delay * (random.random() - 0.5))
            time.sleep(max(0, wait_secs)) # make sure isn't negative time
        self.cache[key] = datetime.now() # update database timestamp to now


    def get_proxy(self, proxies):
        if proxies and isinstance(proxies, list):
            proxy = proxies.pop(0)
            proxies.append(proxy)
        else:
            proxy = proxies
        return proxy


    def crawl(self, seed_url, max_urls=30, max_depth=1, allowed_urls='', banned_urls='^$', obey_robots=False, max_size=1000000, force_html=True, return_html=False, recrawl=True, **kwargs):
        """Crawl website html and return list of URLs crawled

        seed_url: url to start crawling from
        max_urls: maximum number of URLs to crawl (use None for no limit)
        max_depth: maximum depth to follow links into website (use None for no limit)
        allowed_urls: regex for allowed urls
        banned_urls: regex for banned urls
        obey_robots: whether to obey robots.txt
        max_size is passed to get() and is limited to 1MB by default
        force_text is passed to get() and is set to True by default so only crawl HTML content
        recrawl sets whether to return content already downloaded previously
        **kwargs is passed to get()
        """
        user_agent = kwargs.get('user_agent', self.user_agent)
        robots = RobotFileParser()
        if obey_robots:
            robots_url = 'http://' + common.get_domain(seed_url) + '/robots.txt'
            robots.parse(self.get(robots_url).splitlines()) # load robots.txt
        allowed_urls = re.compile(allowed_urls or seed_url)
        banned_urls = re.compile(banned_urls)
        outstanding = [(seed_url, 0)]#, (server, 0)] # which URLs need to crawl
        found = set() # urls that have already found
        crawled = {} if return_html else [] # urls that have successfully crawled
        # XXX recrawl ftonr page

        while outstanding and len(crawled) != max_urls: 
            # crawl next url in queue
            cur_url, cur_depth = outstanding.pop(0)
            html = self.get(cur_url, max_size=max_size, force_html=force_html, **kwargs)
            if return_html:
                crawled[cur_url] = html
            else:
                crawled.append(cur_url)

            if cur_depth != max_depth:
                # extract links to continue crawling
                for url in re.findall(re.compile('<a[^>]+href=["\'](.*?)["\']', re.IGNORECASE), html):
                    url = url[:url.index('#')] if '#' in url else url  # remove internal links to avoid duplicates
                    url = urljoin(cur_url, url) # support relative links
                    #print allowed_urls.match(url), banned_urls.match(url), url
                    #url not in crawled and not in outstanding # check if have already crawled
                    if url not in found:
                        # check if a media file
                        if common.get_extension(url) not in common.MEDIA_EXTENSIONS:
                            # not blocked by robots.txt
                            if robots.can_fetch(user_agent, url):
                                # passes regex
                                if allowed_urls.match(url) and not banned_urls.match(url):
                                    # only crawl within website
                                    if common.same_domain(seed_url, url):
                                        # allowed to recrawl
                                        if recrawl or url not in self.cache: 
                                            outstanding.append((url, cur_depth+1))
                    found.add(url)
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



def threaded_crawl(seed_urls, num_threads=10, max_urls=30, max_depth=1, allowed_urls='', banned_urls='^$', **kwargs):
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
                    urls = d.crawl(url, max_urls=max_urls, max_depth=max_depth, allowed_urls=allowed_urls, banned_urls=banned_urls, **kwargs)
                    self.results[url] = urls
            except Queue.Empty:
                pass # finished

    # randomize url order to balance requests across domains
    #random.shuffle(seed_urls)
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
