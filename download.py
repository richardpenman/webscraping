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
import adt, alg, common, pdict, settings
try:
    import hashlib
except ImportError:
    import md5 as hashlib

SLEEP_TIME = 0.1 # how long to sleep when waiting for network activity

class Download(object):
    DL_TYPES = ALL, LOCAL, REMOTE, NEW = range(4)

    def __init__(self, cache=None, cache_file=None, cache_timeout=None, user_agent=None, timeout=30, delay=5, proxy=None, proxies=None, proxy_file=None, opener=None, 
            headers=None, data=None, dl=ALL, retry=False, num_retries=2, num_redirects=1, allow_redirect=True,
            force_html=False, force_ascii=False, max_size=None):
        """
        `cache' is a pdict object to use for the cache
        `cache_file' sets where to store cached data
        'cache_timeout' is the maximum time of cache timeout
        `user_agent' sets the User Agent to download content with
        `timeout' is the maximum amount of time to wait for http response
        `delay' is the minimum amount of time (in seconds) to wait after downloading content from a domain per proxy
        `proxy' is a proxy to download content through. If a list is passed then will cycle through list.
        `opener' sets an optional opener to use instead of using urllib2 directly
        `headers' are the headers to include in the request
        `data' is what to post at the URL
        `retry' sets whether to try downloading webpage again if got error last time
        `num_retries' sets how many times to try downloading a URL after getting an error
        `num_redirects' sets how many times the URL is allowed to be redirected, to avoid infinite loop
        `force_html' sets whether to download non-text data
        `force_ascii' sets whether to only return ascii characters
        `max_size' determines maximum number of bytes that will be downloaded
        `dl' sets how to download content
            LOCAL means only load content already in cache
            REMOTE means ignore cache and download all content
            NEW means download content when not in cache or return empty
        """
        socket.setdefaulttimeout(timeout)
        self.cache = cache or pdict.PersistentDict(cache_file or settings.cache_file, cache_timeout=cache_timeout)
        self.delay = delay
        self.proxies = (common.read_list(proxy_file) if proxy_file else []) or proxies or [proxy]
        self.proxy_file = proxy_file
        self.last_load_time = self.last_mtime =time.time()
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

        `url' is what to download
        `kwargs' can override any of the arguments passed to constructor
        """
        delay = kwargs.get('delay', self.delay)
        self.reload_proxies()
        proxies = kwargs.get('proxies') or []
        if not any(proxies): proxies = self.proxies
        if kwargs.has_key('proxy'): proxies = [kwargs.get('proxy')]
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
        key = self.get_key(url, data)
        if dl != Download.REMOTE:
            try:
                html = self.cache[key]
            except KeyError:
                pass # have not downloaded yet
            else:
                if retry and not html:
                    # try downloading again
                    common.logger.debug('Redownloading')
                else:
                    if dl == Download.NEW:
                        return '' # only want newly downloaded content
                    else:
                        return html # return previously downloaded content
        if dl == Download.LOCAL:
            return '' # only want previously cached content

        html = None
        # attempt downloading URL
        while html is None and num_retries >= 0:
            # crawl slowly for each domain to reduce risk of being blocked
            proxy = random.choice(proxies)
            self.throttle(url, delay=delay, proxy=proxy) 
            html = self.fetch(url, headers=headers, data=data, proxy=proxy, user_agent=user_agent, opener=opener)
            num_retries -= 1

        if html:
            if allow_redirect:
                redirect_url = self.check_redirect(url=url, html=html)
                if redirect_url:
                    # found a redirection
                    if num_redirects > 0:
                        common.logger.info('redirecting to %s' % redirect_url)
                        kwargs['num_redirects'] = num_redirects - 1
                        html = self.get(redirect_url, **kwargs) or ''
                        # make relative links absolute so will still work after redirect
                        relative_re = re.compile('(<\s*a[^>]+href\s*=\s*["\']?)(?!http)([^"\'>]+)', re.IGNORECASE)
                        html = relative_re.sub(lambda m: m.group(1) + urljoin(url, m.group(2)), html)
                    else:
                        common.logger.info('%s wanted to redirect to %s' % (url, redirect_url))
            html = self.clean_content(html=html, max_size=max_size, force_html=force_html, force_ascii=force_ascii)

        # cache results
        self.cache[key] = html
        if url != self.final_url:
            self.cache.meta(key, dict(url=self.final_url))
        return html or '' # make sure return string


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


    redirect_re = re.compile('<meta[^>]*?url=(.*?)["\']', re.IGNORECASE)
    def check_redirect(self, url, html):
        """Check for meta redirects and return redirect URL if found
        """
        match = Download.redirect_re.search(html)
        if match:
            return urljoin(url, common.unescape(match.groups()[0].strip())) 


    def fetch(self, url, headers=None, data=None, proxy=None, user_agent=None, opener=None):
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
        default_headers =  {'User-agent': user_agent or settings.user_agent, 'Accept-encoding': 'gzip', 'Referer': url}
        headers = headers and default_headers.update(headers) or default_headers
        
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
            common.logger.debug('Error: %s %s' % (url, e))
            content, self.final_url = None, url
        return content

    domains = {}
    def throttle(self, url, delay, proxy=None, variance=0.5):
        """Delay a minimum time for each domain per proxy by storing last access times in a pdict

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
        """Reload proxies
        Check every 10 minutes, if file changed, reloading it
        """
        if self.proxy_file and time.time() - self.last_load_time > 10 * 60:
            self.last_load_time = time.time()
            if os.path.exists(self.proxy_file):
                if os.stat(self.proxy_file).st_mtime != self.last_mtime:
                    self.last_mtime = os.stat(self.proxy_file).st_mtime
                    self.proxies = common.read_list(self.proxy_file)
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
            del self.cache[url]
        return results


    def get_emails(self, website, max_depth=1, max_urls=None, max_emails=None):
        """Crawl this website and return all emails found
        """
        scraped = adt.HashDict()
        c = CrawlerCallback(max_depth=max_depth)
        outstanding = deque([website])
        emails = set()
        while outstanding and (not max_urls or len(scraped) <= max_urls):
            url = outstanding.popleft()
            if not max_urls or len(scraped) <= max_urls:
                scraped.add(url)
                html = self.get(url, retry=False, delay=1)
                if html:
                    emails.update(alg.extract_emails(html))
                    if max_emails and len(emails) >= max_emails: break
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
        if not html: return
        
        m = re.compile(r'<frame src="([^"]+)" name=c>', re.DOTALL|re.IGNORECASE).search(html)
        if not m: return
        frame_src = urljoin(url, common.unescape(m.groups()[0].strip()))
        
        # force to check redirect here
        if kwargs.has_key('allow_redirect'): kwargs['allow_redirect'] = True
        html = self.get(frame_src, **kwargs)
        if not html: return
        
        # remove google translations content
        return re.compile(r'<span class="google-src-text".+?</span>', re.DOTALL|re.IGNORECASE).sub('', html)
    
    def whois(self, url, timeout=10):
        """Query whois info
        Compatible with windows
        Note:
        On unix please install whois first.
        On windows please download whois.exe from http://technet.microsoft.com/en-us/sysinternals/bb897435.aspx, then place it in Python directory e.g. C:\Python27
        """
        domain = common.get_domain(url)
        if domain:
            key = 'whos_%s' % domain
            try:
                text = self.cache[key]
                if text:
                    return text
            except KeyError:
                pass
            
            # try http://whois.chinaz.com/ first
            query_url = 'http://whois.chinaz.com/%s' % domain
            html = self.get(query_url)
            if html:
                m = re.compile("<script src='(request.aspx\?domain=.*?)'></script>").search(html)
                if m:
                    script_url = urljoin(query_url, m.groups()[0])
                    text = self.get(script_url)
                    if not text or not '@' in text:
                        del self.cache[query_url]
                        del self.cache[script_url]
                        
                        # try whois command
                        r = subprocess.Popen(['whois', domain], stdout=subprocess.PIPE)
                        start = time.time()
                        while r.poll() == None:
                            time.sleep(0.5)
                            if time.time() - start > timeout:
                                try:
                                    r.kill()
                                except Exception, e:
                                    pass
                                break
                        if r.poll() !=1:
                            text = r.communicate()[0]
       
                    if text and '@' in text:
                        self.cache[key] = text
                        return text

        
    def save_as(self, url, filename=None, save_dir='images'):
        """Download url and save into disk.
        """
        if url:
            _bytes = self.get(url, allow_redirect=False)
            if _bytes:
                if not os.path.exists(save_dir):
                    os.makedirs(save_dir)
                save_path = os.path.join(save_dir, filename or '%s.%s' % (hashlib.md5(url).hexdigest(), common.get_extension(url)))
                open(save_path, 'wb').write(_bytes)
                return save_path
        
def update_proxy_file(proxy_file='proxies.txt', interval=20, mrt=1):
    """Update proxies periodically
    proxy_file - Local proxies file
    interval - Unit: minute
    mrt -  Max response time
    """
    event = Event()
    event.set()
    def update_proxies():
        D = Download(dl=Download.REMOTE)
        last_time = time.time()
        while event.isSet():
            time.sleep(1)
            if time.time() - last_time >= interval * 60:
                last_time = time.time()
                html = D.get('http://django.redicecn.com/proxies/', data='max_rt=%d' % mrt)
                if html:
                    open(proxy_file, 'w').write(html)
    thread = Thread(target=update_proxies)
    thread.start()
    return event


def threaded_get(url=None, urls=None, num_threads=10, cb=None, df='get', depth=False, **kwargs):
    """Download these urls in parallel

    `url[s]' are the webpages to download
    `num_threads' determines the number of threads to download urls with
    `cb' is called after each download with the HTML of the download   
        the arguments are the url and downloaded html
        whatever URLs are returned are added to the crawl queue
    `df' is download method, default is get
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
                        html = eval('D.%s' % df)(url, **kwargs)
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
        `max_urls' is the maximum number of URLs to crawl (use None for no limit)
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
                link = common.unescape(link) # remove &amp; from link
                #print allowed_urls.match(url), banned_urls.match(url), url
                if not link.lower().startswith('mailto:')  and link not in self.found:
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
                                    if self.crawl_existing or url not in D.cache:
                                        outstanding.append(link)
        return outstanding
