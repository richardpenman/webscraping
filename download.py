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
try:
    import json
except ImportError:
    import simplejson as json
from urlparse import urljoin
from StringIO import StringIO
from datetime import datetime, timedelta
from collections import defaultdict, deque
import socket
from threading import Thread
from webscraping import adt, common, data, pdict, settings

DEBUG = True
SLEEP_TIME = 0.1 # how long to sleep when waiting for network activity



class Download(object):
    DL_TYPES = ALL, LOCAL, REMOTE, NEW = range(4)

    def __init__(self, cache_file=None, user_agent=None, timeout=30, delay=5, cap=10, proxy=None, proxies=None, opener=None, 
            headers=None, data=None, dl=ALL, retry=False, num_retries=0, num_redirects=1, allow_redirect=True,
            force_html=False, force_ascii=False, max_size=None):
        """
        cache_file sets where to store cached data
        user_agent sets the user_agent to download content with
        timeout is the maximum amount of time to wait for http response
        delay is the minimum amount of time (in seconds) to wait after downloading content from a domain per proxy
        cap is the maximum number of requests that can be made per second
        proxy is a proxy to download content through. If a list is passed then will cycle through list.
        opener sets an optional opener to use instead of using urllib2 directly
        headers are the headers to include in the request
        data is what to post at the URL
        retry sets whether to try downloading webpage again if got error last time
        num_retries sets how many times to try downloading a URL after getting an error
        num_redirects sets how many times the URL is allowed to be redirected
        force_html sets whether to download non-text data
        force_ascii sets whether to only return ascii characters
        max_size determines maximum number of bytes that will be downloaded
        dl: sets how to download content
            LOCAL means only load content already in cache
            REMOTE means ignore cache and download all content
            NEW means download content when not in cache or return empty
        """
        socket.setdefaulttimeout(timeout)
        self.cache = pdict.PersistentDict(cache_file or settings.cache_file)
        self.delay = delay
        self.cap = cap
        self.proxies = proxies or []
        if proxy: self.proxies.append(proxy)
        self.user_agent = user_agent or settings.user_agent
        self.opener = opener
        self.headers = headers
        self.data = data
        self.dl = dl
        self.retry = retry
        self.num_retries = num_retries
        self.num_redirects = num_redirects
        self.allow_redirect = allow_redirect
        self.force_html = force_html
        self.force_ascii = force_ascii
        self.max_size = max_size


    def get(self, url, **kwargs):
        """Download this URL and return the HTML. Data is cached so only have to download once.

        url is what to download
        kwargs can override any of the arguments passed to constructor
        """
        delay = kwargs.get('delay', self.delay)
        cap = kwargs.get('cap', self.cap)
        proxy = random.choice(kwargs.get('proxies', self.proxies) or [kwargs.get('proxy')])
        user_agent = kwargs.get('user_agent', self.user_agent)
        opener = kwargs.get('opener', self.opener)
        headers = kwargs.get('headers', self.headers)
        data = kwargs.get('data', self.data)
        dl = kwargs.get('dl', self.dl)
        retry = kwargs.get('retry', self.retry)
        num_retries = kwargs.get('num_retries', self.num_retries)
        num_redirects = kwargs.get('num_redirects', self.num_redirects)
        allow_redirect = kwargs.get('allow_redirect', self.allow_redirect)
        force_html = kwargs.get('force_html', self.force_html)
        force_ascii = kwargs.get('force_ascii', self.force_ascii)
        max_size = kwargs.get('max_size', self.max_size)
        self.final_url = None

        # check cache for whether this content is already downloaded
        key = url
        if data:
            key += ' ' + str(data)
        if dl != Download.REMOTE:
            try:
                html = self.cache[key]
            except KeyError:
                pass # have not downloaded yet
            else:
                if retry and not html:
                    # try downloading again
                    if DEBUG: print 'Redownloading'
                else:
                    if dl == Download.NEW:
                        return '' # only want newly downloaded content
                    else:
                        return html # return previously downloaded content
        if dl == Download.LOCAL:
            return '' # only want previously cached content

        self.throttle(url, delay=delay, cap=cap, proxy=proxy) # crawl slowly for each domain to reduce risk of being blocked
        html = self.fetch(url, headers=headers, data=data, proxy=proxy, user_agent=user_agent, opener=opener, num_retries=num_retries)
        if allow_redirect:
            redirect_url = self.check_redirect(url=url, html=html)
            if redirect_url:
                # found a redirection
                if num_redirects > 0:
                    print 'redirecting to', redirect_url
                    kwargs['num_redirects'] = num_redirects - 1
                    html = self.get(redirect_url, **kwargs)
                    # make relative links absolute so will still work after redirect
                    relative_re = re.compile('(<\s*a[^>]+href\s*=\s*["\']?)(?!http)([^"\'>]+)', re.IGNORECASE)
                    html = relative_re.sub(lambda m: m.group(1) + urljoin(url, m.group(2)), html)
                else:
                    print '%s wanted to redirect to %s' % (url, redirect_url)
        html = self.clean_content(html=html, max_size=max_size, force_html=force_html, force_ascii=force_ascii)
        self.cache[key] = html
        if url != self.final_url:
            self.cache.meta(key, dict(url=self.final_url))
        return html


    def clean_content(self, html, max_size, force_html, force_ascii):
        """Clean up downloaded content
        """
        if max_size is not None and len(html) > max_size:
            if DEBUG: print 'Too big:', len(html)
            html = '' # too big to store
        elif force_html and not common.is_html(html):
            if DEBUG: print 'Not html'
            html = '' # non-html content
        elif force_ascii:
            html = common.to_ascii(html) # remove non-ascii characters
        return html


    redirect_re = re.compile('<meta[^>]*?url=(.*?)["\']', re.IGNORECASE)
    def check_redirect(self, url, html):
        """Check for meta redirects and return redirect URL if found
        """
        match = Download.redirect_re.search(html)
        if match:
            return urljoin(url, match.groups()[0].strip()) 


    def fetch(self, url, headers=None, data=None, proxy=None, user_agent='', opener=None, num_retries=1):
        """Simply download the url and return the content
        """
        if DEBUG: print 'Downloading', url
        opener = opener or urllib2.build_opener()
        if proxy:
            opener.add_handler(urllib2.ProxyHandler({'http' : proxy}))
        headers = headers or {'User-agent': user_agent or Download.DEFAULT_USER_AGENT, 'Accept-encoding': 'gzip', 'Referrer': url}
        if isinstance(data, dict):
            data = urllib.urlencode(data) 
        try:
            response = opener.open(urllib2.Request(url, data, headers))
            content = response.read()
            if response.headers.get('content-encoding') == 'gzip':
                # data came back gzip-compressed so decompress it          
                content = gzip.GzipFile(fileobj=StringIO(content)).read()
            self.final_url = response.url # store where redirected to
        except Exception, e:
            # so many kinds of errors are possible here so just catch them all
            if DEBUG: print 'Fetch', e
            if num_retries > 0:
                if DEBUG: print 'Retrying'
                content = self.fetch(url, headers, data, proxy, user_agent, opener, num_retries - 1)
            else:
                content, self.final_url = '', url
        return content


    counter = adt.ExpireCounter() # track how often requests are bring made
    domains = {}
    def throttle(self, url, delay, cap, proxy=None, variance=0.5):
        """Delay a minimum time for each domain per proxy by storing last access times in a pdict

        url is what intend to download
        delay is the minimum amount of time (in seconds) to wait after downloading content from this domain
        variance is the amount of randomness in delay, 0-1
        """
        key = str(proxy) + ':' + common.get_domain(url)
        start = datetime.now()
        while len(Download.counter) > cap or datetime.now() < Download.domains.get(key, start):
            time.sleep(SLEEP_TIME)
        Download.counter.add()
        # update domain timestamp to when can query next
        Download.domains[key] = datetime.now() + timedelta(seconds=delay * (1 + variance * (random.random() - 0.5)))


    def geocode(self, address):
        """Geocode address using Google's API and return dictionary of useful fields
        """
        url = 'http://maps.google.com/maps/api/geocode/json?address=%s&sensor=false' % address.replace(' ', '%20')
        html = self.get(url)
        results = defaultdict(str)
        if html:
            geo_data = json.loads(html)
            for result in geo_data.get('results', []):
                for e in result['address_components']:
                    types, value = e['types'], e['long_name']
                    if 'street_number' in types:
                        results['number'] = value
                    elif 'route' in types:
                        results['street'] = value
                    elif 'postal_code' in types:
                        results['postcode'] = value
                    elif 'locality' in types:
                        results['suburb'] = value
                    elif 'administrative_area_level_1' in types:
                        results['state'] = value
                    elif 'country' in types:
                        results['country'] = value
                results['full_address'] = result['formatted_address']
            results['address'] = (results['number'] + ' ' + results['street']).strip()
        if not results:
            # error geocoding - try again later
            del self.cache[url]
        return results


    def get_emails(self, website):
        """Crawl this website and return all emails found
        """
        c = CrawlerCallback()
        outstanding = deque([website])
        emails = set()
        while outstanding:
            url = outstanding.popleft()
            html = self.get(url, retry=False)
            emails.update(data.extract_emails(html))
            outstanding.extend(c.crawl(self, url, html))
        return list(emails)



def threaded_get(url=None, urls=None, num_threads=10, cb=None, depth=False, **kwargs):
    """Download these urls in parallel

    url[s] are the webpages to download
    cb is called after each download with the HTML of the download   
        the arguments are the url and downloaded html
        whatever URLs are returned are added to the crawl queue
    depth sets to traverse depth first rather than the default breadth first
    """
    class DownloadThread(Thread):
        """Download data
        """
        processing = deque()

        def __init__(self):
            Thread.__init__(self)

        def run(self):
            D = Download(**kwargs)
            while urls or DownloadThread.processing:
                DownloadThread.processing.append(1) # keep track that are processing url
                try:
                    if depth:
                        url = urls.popleft()
                    else:
                        url = urls.pop()
                except IndexError:
                    # currently no urls to process
                    DownloadThread.processing.popleft()
                    time.sleep(SLEEP_TIME)
                else:
                    # download this url
                    try:
                        html = D.get(url, **kwargs)
                        if cb:
                            # scrape download
                            urls.extend(cb(D, url, html) or [])
                    finally:
                        # have finished processing
                        DownloadThread.processing.popleft()

    # put urls into thread safe queue
    urls = urls or []
    if url: urls.append(url)
    urls = deque(urls)
    threads = [DownloadThread() for i in range(num_threads)]
    for thread in threads:
        thread.start()
    # wait for threads to finish
    for thread in threads:
        thread.join()


class CrawlerCallback:
    """Example callback to crawl the website
    """
    def __init__(self, output_file=None, max_urls=30, max_depth=1, allowed_urls='', banned_urls='^$', robots=None, crawl_existing=True):
        """
        max_urls: maximum number of URLs to crawl (use None for no limit)
        max_depth: maximum depth to follow links into website (use None for no limit)
        allowed_urls: regex for allowed urls
        banned_urls: regex for banned urls
        robots: RobotFileParser object
        max_size is passed to get() and is limited to 1MB by default
        force_html is set to True by default so only crawl HTML content
        crawl_existing sets whether to crawl content already downloaded previously
        """
        if output_file:
            self.writer = data.UnicodeWriter(output_file) 
        else:
            self.writer = None
        self.max_urls = max_urls
        self.max_depth = max_depth
        self.allowed_urls = re.compile(allowed_urls)
        self.banned_urls = re.compile(banned_urls)
        self.robots = robots
        self.crawl_existing = crawl_existing
        self.found = adt.HashDict(int) # track depth of found URLs
        self.crawled = adt.HashDict() # track which URLs have been crawled (not all found URLs will be crawled)


    def __call__(self, D, url, html):
        """Scrape HTML
        """
        self.scrape(D, url, html)
        return self.crawl(D, url, html)

    def scrape(self, D, url, html):
        """Reimplement this in subclass to scrape data
        """
        pass


    link_re = re.compile('<a[^>]+href=["\'](.*?)["\']', re.IGNORECASE)
    def crawl(self, D, url, html): 
        """Crawl website html and return list of URLs crawled
        """
        # XXX add robots back
        self.crawled.add(url)
        depth = self.found[url]
        outstanding = []
        if len(self.crawled) != self.max_urls and depth != self.max_depth: 
            # extract links to continue crawling
            for link in CrawlerCallback.link_re.findall(html):
                if '#' in link:
                    # remove internal links to avoid duplicates
                    link = link[:link.index('#')] 
                link = urljoin(url, link) # support relative links
                #print allowed_urls.match(url), banned_urls.match(url), url
                if link not in self.found:
                    self.found[link] = depth + 1
                    # check if a media file
                    if common.get_extension(link) not in common.MEDIA_EXTENSIONS:
                        # not blocked by robots.txt
                        if not self.robots or self.robots.can_fetch(settings.user_agent, link):
                            # passes regex
                            if self.allowed_urls.match(link) and not self.banned_urls.match(link):
                                # only crawl within website
                                if common.same_domain(url, link):
                                    # allowed to recrawl
                                    #if self.crawl_existing or url not in self.cache: XXX
                                    outstanding.append(link)
        return outstanding
