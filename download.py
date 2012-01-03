__doc__ = """
Description: Helper methods to download and crawl web content using threads
Website: http://code.google.com/p/webscraping/
License: LGPL
"""

import os
import gzip
import re
import time
import random
import urllib
import urllib2
from urlparse import urljoin
from StringIO import StringIO
import subprocess
from datetime import datetime, timedelta
from collections import defaultdict, deque
import socket
from threading import Thread, Event
try:
    import hashlib
except ImportError:
    import md5 as hashlib
import adt
import alg
import common
import settings
try:
    import pdict
except ImportError:
    # sqlite not installed
    pdict = None

SLEEP_TIME = 0.1 # how long to sleep when waiting for network activity


class Download(object):

    def __init__(self, cache=None, cache_file=None, read_cache=True, write_cache=True, use_network=True, 
            user_agent=None, timeout=30, delay=5, proxies=None, proxy_file=None, opener=None, 
            headers=None, data=None, num_retries=0, num_redirects=1,
            force_html=False, force_ascii=False, max_size=None, default='', pattern=None):
        """
        `cache' is a pdict object to use for the cache
        `cache_file' sets filename to store cached data
        `read_cache' sets whether to read from the cache
        `write_cache' sets whether to write to the cache
        `use_network' sets whether to download content not in the cache
        `user_agent' sets the User Agent to download content with
        `timeout' is the maximum amount of time to wait for http response
        `delay' is the minimum amount of time (in seconds) to wait after downloading content from a domain per proxy
        `proxies' is a list of proxies to cycle through when downloading content
        `opener' sets an optional opener to use instead of using urllib2 directly
        `headers' are the headers to include in the request
        `data' is what to post at the URL
        `num_retries' sets how many times to try downloading a URL when get an error
        `num_redirects' sets how many times the URL is allowed to be redirected, to avoid infinite loop
        `force_html' sets whether to download non-text data
        `force_ascii' sets whether to only return ascii characters
        `max_size' determines maximum number of bytes that will be downloaded, or None to disable
        `default' is what to return when no content can be downloaded
        `pattern' is a regular expression that the downloaded HTML has to match to be considered a valid download
        """
        socket.setdefaulttimeout(timeout)
        cache_file = cache_file or settings.cache_file
        if pdict:
            self.cache = cache or pdict.PersistentDict(cache_file)
        else:
            self.cache = None
            if read_cache or write_cache:
                common.logger.info('Cache disabled because could not import pdict')

        self.settings = adt.Bag(
            read_cache = read_cache,
            write_cache = write_cache,
            use_network = use_network,
            delay = delay,
            proxies = (common.read_list(proxy_file) if proxy_file else []) or proxies or [],
            proxy_file = proxy_file,
            user_agent = user_agent or settings.user_agent,
            opener = opener,
            headers = headers,
            data = data,
            num_retries = num_retries,
            num_redirects = num_redirects,
            force_html = force_html,
            force_ascii = force_ascii,
            max_size = max_size,
            default = default,
            pattern = pattern
        )
        self.last_load_time = self.last_mtime = time.time()


    def get(self, url, **kwargs):
        """Download this URL and return the HTML. Data is cached so only have to download once.

        `url' is what to download
        `kwargs' can override any of the arguments passed to constructor
        """
        self.reload_proxies()
        # for tracking the request
        self.final_url = self.response_headers = None 
                
        # update settings with any local overrides
        settings = adt.Bag(self.settings)
        settings.update(kwargs)
        
        # check cache for whether this content is already downloaded
        key = self.get_key(url, settings.data)
        if self.cache and settings.read_cache:
            try:
                html = self.cache[key]
                if html and settings.pattern and not re.compile(settings.pattern, re.DOTALL | re.IGNORECASE).search(html):
                    # invalid result from download
                    html = None
            except KeyError:
                pass # have not downloaded yet
            else:
                if not html and settings.num_retries > 0:
                    # try downloading again
                    common.logger.debug('Redownloading')
                else:
                    # return previously downloaded content
                    return html or settings.default 
        if not settings.use_network:
            # only want previously cached content
            return settings.default 

        html = None
        # attempt downloading content at URL
        while html is None:
            # crawl slowly for each domain to reduce risk of being blocked
            settings.proxy = random.choice(settings.proxies) if settings.proxies else None
            self.throttle(url, delay=settings.delay, proxy=settings.proxy) 
            html = self.fetch(url, headers=settings.headers, data=settings.data, proxy=settings.proxy, user_agent=settings.user_agent, opener=settings.opener, pattern=settings.pattern)
            if settings.num_retries == 0:
                break # don't try downloading again
            else:
                settings.num_retries -= 1

        if html:
            if settings.num_redirects > 0:
                # allowed to redirect
                redirect_url = self.get_redirect(url=url, html=html)
                if redirect_url:
                    # found a redirection
                    common.logger.info('%s redirecting to %s' % (url, redirect_url))
                    settings.num_redirects -= 1
                    html = self.get(redirect_url, **settings) or ''
                    # make relative links absolute so will still work after redirect
                    relative_re = re.compile('(<\s*a[^>]+href\s*=\s*["\']?)(?!http)([^"\'>]+)', re.IGNORECASE)
                    html = relative_re.sub(lambda m: m.group(1) + urljoin(url, m.group(2)), html)
            html = self.clean_content(html=html, max_size=settings.max_size, force_html=settings.force_html, force_ascii=settings.force_ascii)

        if self.cache and settings.write_cache:
            # cache results
            self.cache[key] = html
            meta = dict(headers=self.response_headers)
            if url != self.final_url:
                meta[url] = self.final_url
            self.cache.meta(key, meta)
        
        # return default if no content
        return html or settings.default 


    def get_key(self, url, data=None):
        """Create key for storing in database
        """
        key = url
        if data:
            key += ' ' + str(data)
        return key


    def clean_content(self, html, max_size, force_html, force_ascii):
        """Clean up downloaded content
        """
        if max_size is not None and len(html) > max_size:
            common.logger.info('Too big: %s' % len(html))
            html = '' # too big to store
        elif force_html and not common.is_html(html):
            common.logger.info('Not html')
            html = '' # non-html content
        elif force_ascii:
            html = common.to_ascii(html) # remove non-ascii characters
        return html


    def get_redirect(self, url, html):
        """Check for meta redirects and return redirect URL if found
        """
        match = re.compile('<meta[^>]*?url=(.*?)["\']', re.IGNORECASE).search(html)
        if match:
            return urljoin(url, common.unescape(match.groups()[0].strip())) 


    def fetch(self, url, headers=None, data=None, proxy=None, user_agent=None, opener=None, pattern=None):
        """Simply download the url and return the content
        """
        common.logger.info('Downloading %s' % url)
        # create opener with headers
        opener = opener or urllib2.build_opener()
        if proxy:
            if url.lower().startswith('https://'):
                opener.add_handler(urllib2.ProxyHandler({'https' : proxy}))
            else:
                opener.add_handler(urllib2.ProxyHandler({'http' : proxy}))
        default_headers =  {'User-agent': user_agent or settings.user_agent, 'Accept-encoding': 'gzip', 'Referer': url, 'Accept-Language': 'en-us,en;q=0.5'}
        headers = headers and default_headers.update(headers) or default_headers
        
        if isinstance(data, dict):
            data = urllib.urlencode(data) 
        try:
            response = opener.open(urllib2.Request(url, data, headers))
            content = response.read()
            if response.headers.get('content-encoding') == 'gzip':
                # data came back gzip-compressed so decompress it          
                content = gzip.GzipFile(fileobj=StringIO(content)).read()
            # keep track of server interaction so can save
            self.final_url = response.url 
            self.response_headers = dict(response.headers)
            if pattern and not re.compile(pattern, re.DOTALL | re.IGNORECASE).search(content):
                # invalid result from download
                content = None
                common.logger.info('Content did not match expected pattern - %s' % url)
        except Exception, e:
            # so many kinds of errors are possible here so just catch them all
            common.logger.info('Error: %s %s' % (url, e))
            content, self.final_url = None, url
        return content


    domains = adt.HashDict()
    def throttle(self, url, delay, proxy=None, variance=0.5):
        """Delay a minimum time for each domain per proxy by storing last access time

        `url' is what intend to download
        `delay' is the minimum amount of time (in seconds) to wait after downloading content from this domain
        `proxy' is the proxy to download through
        `variance' is the amount of randomness in delay, 0-1
        """
        key = str(proxy) + ':' + common.get_domain(url)
        start = datetime.now()
        while datetime.now() < Download.domains.get(key, start):
            time.sleep(SLEEP_TIME)
        # update domain timestamp to when can query next
        Download.domains[key] = datetime.now() + timedelta(seconds=delay * (1 + variance * (random.random() - 0.5)))


    def reload_proxies(self):
        """Check every 10 minutes for updated proxy file
        """
        if self.settings.proxy_file and time.time() - self.last_load_time > 10 * 60:
            self.last_load_time = time.time()
            if os.path.exists(self.settings.proxy_file):
                if os.stat(self.settings.proxy_file).st_mtime != self.last_mtime:
                    self.last_mtime = os.stat(self.settings.proxy_file).st_mtime
                    self.settings.proxies = common.read_list(self.settings.proxy_file)
                    common.logger.debug('Reloaded proxies.')


    def geocode(self, address, delay=5):
        """Geocode address using Google's API and return dictionary of useful fields
        """
        try:
            import simplejson as json
        except ImportError:
            import json
        url = 'http://maps.google.com/maps/api/geocode/json?address=%s&sensor=false' % urllib.quote_plus(address)
        html = self.get(url, delay=delay)
        results = defaultdict(str)
        if html:
            try:
                geo_data = json.loads(html)
            except Exception, e:
                common.logger.debug(str(e))
                return {}
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
                    elif 'administrative_area_level_2' in types:
                        results['county'] = value
                    elif 'administrative_area_level_3' in types:
                        results['district'] = value
                    elif 'country' in types:
                        results['country'] = value
                results['full_address'] = result['formatted_address']
                results['lat'] = result['geometry']['location']['lat']
                results['lng'] = result['geometry']['location']['lng']
            if 'street' in results:
                results['address'] = (results['number'] + ' ' + results['street']).strip()
        if not results:
            # error geocoding - try again later
            common.logger.debug('delete invalid geocode')
            if self.cache:
                del self.cache[url]
        return results


    def get_emails(self, website, max_depth=1, max_urls=None, max_emails=None):
        """Crawl this website and return all emails found
        """
        scraped = adt.HashDict()
        c = CrawlerCallback(max_depth=max_depth)
        outstanding = deque([website])
        emails = []
        while outstanding and (max_urls is None or len(scraped) < max_urls) \
                          and (max_emails is None or len(emails) < max_emails):
            url = outstanding.popleft()
            scraped[url] = True
            html = self.get(url, delay=1)
            if html:
                for email in alg.extract_emails(html):
                    if email not in emails:
                        emails.append(email)
                        if len(emails) == max_emails:
                            break
                outstanding.extend(c.crawl(self, url, html))
        return list(emails)


    def gcache_get(self, url, **kwargs):
        """Get page from google cache
        """
        return self.get('http://www.google.com/search?&q=cache%3A' + urllib.quote(url), **kwargs)


    def gtrans_get(self, url, **kwargs):
        """Get page via Google Translation
        """
        url = 'http://translate.google.com/translate?sl=nl&anno=2&u=%s' % urllib.quote(url)
        html = self.get(url, **kwargs)
        if html:
            m = re.compile(r'<frame src="([^"]+)" name=c>', re.DOTALL|re.IGNORECASE).search(html)
            if m:
                frame_src = urljoin(url, common.unescape(m.groups()[0].strip()))
                # force to check redirect here
                if kwargs.has_key('num_redirects'): kwargs['num_redirects'] = 1
                html = self.get(frame_src, **kwargs)
                if html:
                    # remove google translations content
                    return re.compile(r'<span class="google-src-text".+?</span>', re.DOTALL|re.IGNORECASE).sub('', html)
    

    def whois(self, url, timeout=10):
        """Query whois info
        """
        domain = common.get_domain(url)
        if domain:
            text = ''
            key = 'whois_%s' % domain
            try:
                if self.cache:
                    text = self.cache[key]
                else:
                    raise KeyError()
            except KeyError:
                # try online whois app
                query_url = 'http://whois.chinaz.com/%s' % domain
                html = self.get(query_url)
                match = re.compile("<script src='(request.aspx\?domain=.*?)'></script>").search(html)
                if match:
                    script_url = urljoin(query_url, match.groups()[0])
                    text = self.get(script_url, read_cache=False)

                if '@' not in text:
                    if self.cache:
                        del self.cache[query_url]
                    # failed, so try local whois command
                    r = subprocess.Popen(['whois', domain], stdout=subprocess.PIPE)
                    start = time.time()
                    while r.poll() is None:
                        time.sleep(0.5)
                        if time.time() - start > timeout:
                            try:
                                r.kill()
                            except Exception, e:
                                pass
                            break
                    if r.poll() != 1:
                        text = r.communicate()[0]
                
                if '@' in text:
                    if self.cache:
                        self.cache[key] = text
            return text

        
    def save_as(self, url, filename=None, save_dir='images'):
        """Download url and save into disk.
        """
        if url:
            _bytes = self.get(url, num_redirects=0)
            if _bytes:
                if not os.path.exists(save_dir):
                    os.makedirs(save_dir)
                save_path = os.path.join(save_dir, filename or '%s.%s' % (hashlib.md5(url).hexdigest(), common.get_extension(url)))
                open(save_path, 'wb').write(_bytes)
                return save_path



def threaded_get(url=None, urls=None, num_threads=10, cb=None, post=False, depth=False, **kwargs):
    """Download these urls in parallel

    `url[s]' are the webpages to download
    `num_threads' determines the number of threads to download urls with
    `cb' is called after each download with the HTML of the download   
        the arguments are the url and downloaded html
        whatever URLs are returned are added to the crawl queue
    `post' is whether to use POST instead of default GET
    `depth' sets to traverse depth first rather than the default breadth first
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
                # keep track that are processing url
                DownloadThread.processing.append(1) 
                try:
                    if depth:
                        url = urls.popleft()
                    else:
                        url = urls.pop()
                except IndexError:
                    # currently no urls to process
                    DownloadThread.processing.popleft()
                    # so check again later
                    time.sleep(SLEEP_TIME)
                else:
                    # download this url
                    try:
                        html = (D.post if post else D.get)(url, **kwargs)
                        if cb:
                            # use callback to process downloaded HTML
                            urls.extend(cb(D, url, html) or [])
                    finally:
                        # have finished processing
                        # make sure this is called even on exception
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
    found = adt.HashDict(int) # track depth of found URLs

    def __init__(self, output_file=None, max_links=100, max_depth=1, allowed_urls='', banned_urls='^$', robots=None, crawl_existing=True):
        """
        `output_file' is where to save scraped data
        `max_links' is the maximum number of links to follow per page
        `max_depth' is the maximum depth to follow links into website (use None for no limit)
        `allowed_urls' is a regex for allowed urls, defaults to all urls
        `banned_urls' is a regex for banned urls, defaults to no urls
        `robots': RobotFileParser object to determine which urls allowed to crawl
        `crawl_existing' sets whether to crawl content already downloaded previously in the cache
        """
        if output_file:
            self.writer = common.UnicodeWriter(output_file) 
        else:
            self.writer = None
        self.max_links = max_links
        self.max_depth = max_depth
        self.allowed_urls = re.compile(allowed_urls)
        self.banned_urls = re.compile(banned_urls)
        self.robots = robots
        self.crawl_existing = crawl_existing


    def __call__(self, D, url, html):
        """Scrape HTML
        """
        self.scrape(D, url, html)
        return self.crawl(D, url, html)

    def scrape(self, D, url, html):
        """Reimplement this in subclass to scrape data
        """
        pass


    def crawl(self, D, url, html): 
        """Crawl website html and return list of URLs crawled
        """
        def normalize(link):
            """Normalize the link to avoid duplicates
            """
            if '#' in link:
                # remove internal links to avoid duplicates
                link = link[:link.index('#')] 
            link = common.unescape(link) # remove &amp; from link
            return urljoin(url, link) # support relative links

        def valid(link):
            """Check if should crawl this link
            """
            # check if a media file
            if common.get_extension(link) not in common.MEDIA_EXTENSIONS:
                # check if a proper HTTP link
                if link.lower().startswith('http'):
                    # only crawl within website
                    if common.same_domain(domain, link):
                        # passes regex
                        if self.allowed_urls.match(link) and not self.banned_urls.match(link):
                            # not blocked by robots.txt
                            if not self.robots or self.robots.can_fetch(settings.user_agent, link):
                                # allowed to recrawl
                                if self.crawl_existing or (D.cache and link not in D.cache):
                                    return True
            return False


        domain = common.get_domain(url)
        depth = CrawlerCallback.found[url]
        outstanding = []
        if depth != self.max_depth: 
            # extract links to continue crawling
            links_re = re.compile('<a[^>]+href=["\'](.*?)["\']', re.IGNORECASE)
            for link in links_re.findall(html):
                link = normalize(link)
                if link not in CrawlerCallback.found:
                    CrawlerCallback.found[link] = depth + 1
                    if valid(link):
                        # is a new link
                        outstanding.append(link)
                        if len(outstanding) == self.max_links:
                            break
        return outstanding
