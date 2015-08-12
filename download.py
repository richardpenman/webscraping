__doc__ = 'Helper methods to download and crawl web content using threads'

import os
import re
import sys
import copy
import collections 
import random
import urllib
import urllib2
import urlparse
import StringIO
import time
import datetime
import subprocess
import socket
import gzip
import zlib
import thread
import threading
import contextlib
import tempfile
import traceback
try:
    import hashlib
except ImportError:
    import md5 as hashlib
try:
    import cPickle as pickle
except:
    import pickle
try:
    import json
except ImportError:
    import simplejson as json

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
DEFAULT_PRIORITY = 1 # default queue priority



class ProxyPerformance:
    """Track performance of proxies
    If 10 errors in a row that other proxies could handle then need to remove
    """
    def __init__(self):
        self.proxy_errors = collections.defaultdict(int)

    def success(self, proxy):
        """Successful download - so clear error count
        """
        self.proxy_errors[proxy] = 0

    def error(self, proxy):
        """Add to error count and returns number of consecutive errors for this proxy
        """
        if proxy:
            self.proxy_errors[proxy] += 1
        return self.proxy_errors[proxy]



class Download:
    """
    cache:
        a pdict object to use for the cache
    cache_file:
        filename to store cached data
    read_cache:
        whether to read from the cache
    write_cache:
        whether to write to the cache
    use_network:
        whether to download content not in the cache
    user_agent
        the User Agent to download content with
    timeout:
        the maximum amount of time to wait for http response
    delay:
        the minimum amount of time (in seconds) to wait after downloading content from a domain per proxy
    proxy_file:
        a filename to read proxies from
    max_proxy_errors:
        the maximum number of consecutive errors allowed per proxy before discarding
        an error is only counted if another proxy is able to successfully download the URL
        set to None to disable
    proxies:
        a list of proxies to cycle through when downloading content
    opener:
        an optional opener to use instead of using urllib2 directly
    headers:
        the headers to include in the request
    data:
        what to post at the URL
        if None (default) then a GET request will be made
    num_retries:
        how many times to try downloading a URL when get an error
    num_redirects:
        how many times the URL is allowed to be redirected, to avoid infinite loop
    force_html:
        whether to download non-text data
    force_ascii:
        whether to only return ascii characters
    max_size:
        maximum number of bytes that will be downloaded, or None to disable
    default:
        what to return when no content can be downloaded
    pattern:
        a regular expression that the downloaded HTML has to match to be considered a valid download
    acceptable_errors:
        a list contains all acceptable HTTP codes, don't try downloading for them e.g. no need to retry for 404 error
    throttle_additional_key:
        Sometimes the website limits the request only by session(rather than IP), we can use this parameter to keep each thread delaying independently
    keep_ip_ua:
        If it's True, one proxy IP will keep using the same User-agent, otherwise will use a random User-agent for each request.
    """

    def __init__(self, cache=None, cache_file=None, read_cache=True, write_cache=True, cache_expires=None, use_network=True, 
            user_agent=None, timeout=30, delay=5, proxies=None, proxy_file=None, max_proxy_errors=5,
            opener=None, headers=None, data=None, num_retries=0, num_redirects=0, 
            force_html=False, force_ascii=False, max_size=None, default='', pattern=None, acceptable_errors=None, 
            throttle_additional_key=None, keep_ip_ua=True, **kwargs):
        socket.setdefaulttimeout(timeout)
        need_cache = read_cache or write_cache
        if pdict and need_cache:
            cache_file = cache_file or settings.cache_file
            self.cache = cache or pdict.PersistentDict(cache_file, expires=cache_expires)
        else:
            self.cache = None
            if need_cache:
                common.logger.warning('Cache disabled because could not import pdict')

        self.settings = adt.Bag(
            read_cache = read_cache,
            write_cache = write_cache,
            use_network = use_network,
            delay = delay,
            proxies = (common.read_list(proxy_file) if proxy_file else []) or proxies or [],
            proxy_file = proxy_file,
            max_proxy_errors = max_proxy_errors,
            user_agent = user_agent,
            opener = opener,
            headers = headers,
            data = data,
            num_retries = num_retries,
            num_redirects = num_redirects,
            force_html = force_html,
            force_ascii = force_ascii,
            max_size = max_size,
            default = default,
            pattern = pattern,
            keep_ip_ua = keep_ip_ua,
            acceptable_errors = acceptable_errors
        )
        self.last_load_time = self.last_mtime = time.time()
        self.num_downloads = self.num_errors = 0
        self.throttle_additional_key = throttle_additional_key


    proxy_performance = ProxyPerformance()
    def get(self, url, **kwargs):
        """Download this URL and return the HTML. 
        By default HTML is cached so only have to download once.

        url:
            what to download
        kwargs:
            override any of the arguments passed to constructor
        """
        self.reload_proxies()
        self.proxy = None # the current proxy
        self.final_url = None # for tracking redirects
        self.response_code = '' # keep response code
        self.response_headers = {} # keep response headers
        self.downloading_error = None # keep downloading error
        self.num_downloads = self.num_errors = 0 # track the number of downloads made
                
        # update settings with any local overrides
        settings = adt.Bag(self.settings)
        settings.update(kwargs)
        # check cache for whether this content is already downloaded
        key = self.get_key(url, settings.data)
        if self.cache and settings.read_cache:
            try:
                html = self.cache[key]
                if self.invalid_response(html, settings.pattern):
                    # invalid result from download
                    html = None
            except KeyError:
                pass # have not downloaded yet
            else:
                if not html and settings.num_retries > 0:
                    # try downloading again
                    common.logger.debug('Redownloading')
                    settings.num_retries -= 1
                else:
                    # return previously downloaded content
                    return html or settings.default 
        if not settings.use_network:
            # only want previously cached content
            return settings.default 

        html = None
        failed_proxies = set() # record which proxies failed to download for this URL
        # attempt downloading content at URL
        while settings.num_retries >= 0 and html is None:
            settings.num_retries -= 1
            if settings.proxy:
                self.proxy = settings.proxy
            else:
                self.proxy = self.get_proxy(settings.proxies)
            # crawl slowly for each domain to reduce risk of being blocked
            self.throttle(url, delay=settings.delay, proxy=self.proxy) 
            html = self.fetch(url, headers=settings.headers, data=settings.data, proxy=self.proxy, user_agent=settings.user_agent, opener=settings.opener, pattern=settings.pattern, max_size=settings.max_size)

            if html:
                # successfully downloaded
                self.num_downloads += 1
                if settings.max_proxy_errors is not None:
                    Download.proxy_performance.success(self.proxy)
                    # record which proxies failed for this download
                    for proxy in failed_proxies:
                        if Download.proxy_performance.error(self.proxy) > settings.max_proxy_errors:
                            # this proxy has had too many errors so remove
                            common.logger.warning('Removing unstable proxy from list after %d consecutive errors: %s' % (settings.max_proxy_errors, self.proxy))
                            settings.proxies.remove(self.proxy)
            else:
                # download failed - try again
                self.num_errors += 1
                failed_proxies.add(self.proxy)


        if html:
            if settings.num_redirects > 0:
                # allowed to redirect
                redirect_url = get_redirect(url=url, html=html)
                if redirect_url:
                    # found a redirection
                    common.logger.debug('%s redirecting to %s' % (url, redirect_url))
                    settings.num_redirects -= 1
                    html = self.get(redirect_url, **settings) or ''
                    # make relative links absolute so will still work after redirect
                    relative_re = re.compile('(<\s*a[^>]+href\s*=\s*["\']?)(?!http)([^"\'>]+)', re.IGNORECASE)
                    try:
                        html = relative_re.sub(lambda m: m.group(1) + urlparse.urljoin(url, m.group(2)), html)
                    except UnicodeDecodeError:
                        pass
            html = self._clean_content(html=html, max_size=settings.max_size, force_html=settings.force_html, force_ascii=settings.force_ascii)

        if self.cache and settings.write_cache:
            # cache results
            self.cache[key] = html
            if url != self.final_url:
                # cache what URL was redirected to
                self.cache.meta(key, dict(url=self.final_url))
        
        # return default if no content
        return html or settings.default 


    def exists(self, url):
        """Do a HEAD request to check whether webpage exists
        """
        success = False
        key = self.get_key(url, 'head')
        try:
            if self.cache and self.settings.read_cache:
                success = self.cache[key]
            else:
                raise KeyError('No cache')
        except KeyError:
            # have not downloaded yet
            request = urllib2.Request(url)
            request.get_method = lambda : 'HEAD'
            try:
                response = urllib2.urlopen(request)
            except Exception, e:
                common.logger.warning('HEAD check miss: %s %s' % (url, e))
            else:
                success = True
                common.logger.info('HEAD check hit: %s' % url)
            if self.cache:
                self.cache[key] = success
        return success


    def get_key(self, url, data=None):
        """Create key for caching this request
        """
        key = url
        if data:
            key += ' ' + str(data)
        return key


    def _clean_content(self, html, max_size, force_html, force_ascii):
        """Clean up downloaded content

        html:
            the input to clean
        max_size:
            the maximum size of data allowed
        force_html:
            content must be HTML
        force_ascii:
            content must be ASCII
        """
        if max_size is not None and len(html) > max_size:
            common.logger.info('Webpage is too big: %s' % len(html))
            html = '' # too big to store
        elif force_html and not common.is_html(html):
            common.logger.info('Webpage is not html')
            html = '' # non-html content
        elif force_ascii:
            html = common.to_ascii(html) # remove non-ascii characters
        return html


    def get_proxy(self, proxies=None):
        """Return random proxy if available
        """
        if proxies:
            proxy = random.choice(proxies)
        elif self.settings.proxies:
            # select next available proxy
            proxy = random.choice(self.settings.proxies)
        else:
            proxy = None
        return proxy


    # cache the user agent used for each proxy
    proxy_agents = {}
    def get_user_agent(self, proxy):
        """Get user agent for this proxy
        """
        if self.settings.keep_ip_ua and proxy in Download.proxy_agents:
            # have used this proxy before so return same user agent
            user_agent = Download.proxy_agents[proxy]
        else:
            # assign random user agent to this proxy
            user_agent = alg.rand_agent()
            Download.proxy_agents[proxy] = user_agent
        return user_agent


    def invalid_response(self, html, pattern):
        """Return whether the response contains a regex error pattern 
        """
        return html is None or (pattern and not re.compile(pattern, re.DOTALL | re.IGNORECASE).search(html))


    def fetch(self, url, headers=None, data=None, proxy=None, user_agent=None, opener=None, pattern=None, max_size=None):
        """Simply download the url and return the content
        """
        self.error_content = None
        # create opener with headers
        if not opener:
            opener = common.build_opener()
        if proxy:
            opener.add_handler(urllib2.ProxyHandler({urlparse.urlparse(url).scheme : proxy}))
        
        headers = headers or {}
        default_headers = settings.default_headers.copy()
        default_headers['User-Agent'] = user_agent or self.get_user_agent(proxy)
        if not max_size:
            default_headers['Accept-Encoding'] = 'gzip, deflate'
        lowercase_headers = [name.lower() for name in headers.keys()]
        for name, value in default_headers.items():
            if name.lower() not in lowercase_headers:
                if name == 'Referer':
                    value = url
                headers[name] = value

        if isinstance(data, dict):
            # encode data for POST
            data = urllib.urlencode(sorted(data.items()))
        common.logger.info('Downloading %s %s' % (url, data or ''))
        try:
            request = urllib2.Request(url, data, headers)
            with contextlib.closing(opener.open(request)) as response:
                if max_size is not None:
                    content = response.read(max_size)
                else:
                    content = response.read()
                if response.headers.get('Content-Encoding') == 'gzip':
                    # data came back gzip-compressed so decompress it          
                    content = gzip.GzipFile(fileobj=StringIO.StringIO(content)).read()
                elif response.headers.get('Content-Encoding') == 'deflate':
                    content = zlib.decompress(content)
                self.final_url = response.url # store where redirected to
                if self.invalid_response(content, pattern):
                    # invalid result from download
                    content = None
                    common.logger.warning('Content did not match expected pattern: %s' % url)
                self.response_code = str(response.code)
                self.response_headers = dict(response.headers)
        except Exception, e:
            self.downloading_error = str(e)
            if hasattr(e, 'code'):
                self.response_code = str(e.code)
            if hasattr(e, 'read'):
                try:
                    self.error_content = e.read()
                except Exception, e:
                    self.error_content = ''
            # so many kinds of errors are possible here so just catch them all
            common.logger.warning('Download error: %s %s' % (url, e))
            if self.settings.acceptable_errors and self.response_code in self.settings.acceptable_errors:
                content, self.final_url = self.settings.default, url
            else:
                content, self.final_url = None, url
        return content


    _domains = adt.HashDict()
    def throttle(self, url, delay, proxy=None, variance=0.5):
        """Delay a minimum time for each domain per proxy by storing last access time

        url
            what intend to download
        delay
            the minimum amount of time (in seconds) to wait after downloading content from this domain
        proxy
            the proxy to download through
        variance
            the amount of randomness in delay, 0-1
        """
        if delay > 0:
            key = ':'.join([str(proxy), self.throttle_additional_key or '', common.get_domain(url)])
            if key in Download._domains:
                while datetime.datetime.now() < Download._domains.get(key):
                    time.sleep(SLEEP_TIME)
            # update domain timestamp to when can query next
            Download._domains[key] = datetime.datetime.now() + datetime.timedelta(seconds=delay * (1 + variance * (random.random() - 0.5)))


    def reload_proxies(self, timeout=600):
        """Check periodically for updated proxy file

        timeout:
            the number of seconds before check for updated proxies
        """
        if self.settings.proxy_file and time.time() - self.last_load_time > timeout:
            self.last_load_time = time.time()
            if os.path.exists(self.settings.proxy_file):
                if os.stat(self.settings.proxy_file).st_mtime != self.last_mtime:
                    self.last_mtime = os.stat(self.settings.proxy_file).st_mtime
                    self.settings.proxies = common.read_list(self.settings.proxy_file)
                    common.logger.debug('Reloaded proxies from updated file.')


    def geocode(self, address, delay=5, read_cache=True, num_retries=1, language=None):
        gm = GoogleMaps(self)
        return gm.geocode(address, delay, read_cache, num_retries, language)

    def places(self, api_key, keyword, latitude, longitude, radius=10000, delay=5, num_retries=1, language='en'):
        gm = GoogleMaps(self)
        return gm.places(api_key, keyword, latitude, longitude, radius, delay, num_retries, language)


    def get_emails(self, website, max_depth=1, max_urls=10, max_emails=1):
        return DataCrawler(self, alg.extract_emails).find(website, max_depth, max_urls, max_emails)

    def get_phones(self, website, max_depth=1, max_urls=10, max_phones=1):
        return DataCrawler(self, alg.extract_phones).find(website, max_depth, max_urls, max_phones)


    def gcache_get(self, url, **kwargs):
        """Download webpage via google cache
        """
        return self.get('http://www.google.com/search?&q=cache%3A' + urllib.quote(url), **kwargs)


    def gtrans_get(self, url, **kwargs):
        """Download webpage via Google Translation
        """
        url = 'http://translate.google.com/translate?sl=nl&anno=2&u=%s' % urllib.quote(url)
        html = self.get(url, **kwargs)
        if html:
            m = re.compile(r'<iframe[^<>]*src="([^"]+)"[^<>]*name=c', re.DOTALL|re.IGNORECASE).search(html)
            if m:
                frame_src = urlparse.urljoin(url, common.unescape(m.groups()[0].strip()))
                # force to check redirect here
                html = self.get(frame_src, **kwargs)
                if html:
                    # remove google translations content
                    return re.compile(r'<span class="google-src-text".+?</span>', re.DOTALL|re.IGNORECASE).sub('', html)
        return self.settings.default

    
    def archive_get(self, url, timestamp=None, **kwargs):
        """Download webpage via the archive.org cache

        url:
            The webpage to download
        timestamp:
            When passed a datetime object will download the cached webpage closest to this date,
            If passed a string will use this as timestamp
            Else when None (default) will download the most recent archived page.
        """
        if hasattr(timestamp, 'strftime'):
            formatted_ts = timestamp.strftime('%Y%m%d%H%M%S')
        elif isinstance(timestamp, basestring):
            formatted_ts = timestamp
        else:
            formatted_ts = '2' # will return most recent archive
        html = self.get('https://web.archive.org/web/%s/%s' % (formatted_ts, url), **kwargs)
        if not html and timestamp is None:
            # not cached, so get live version
            html = self.get('http://liveweb.archive.org/' + url)
        match = re.search('<p class="impatient"><a href="(/web/\d+.*?)"', html)
        if match:
            redirect = match.groups()[0]
            html = self.get('https://web.archive.org' + redirect, **kwargs)

        if html:
            # remove wayback toolbar
            html = re.compile('<!-- BEGIN WAYBACK TOOLBAR INSERT -->.*?<!-- END WAYBACK TOOLBAR INSERT -->', re.DOTALL).sub('', html)
            html = re.compile('<!--\s+FILE ARCHIVED ON.*?-->', re.DOTALL).sub('', html)
            html = re.sub('/web/\d+/', '', html)
        return html


    def whois(self, url, timeout=10):
        """Return text of this whois query
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
                # try local whois command
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

        
    def save_as(self, url, filename=None, save_dir='images', override=False):
        """Download url and save to disk if does not already exist

        url:
            the webpage to download
        filename:
            output file to save to if not set then will save to file based on URL
        override:
            whether to download if output file already exists
        """
        save_path = os.path.join(save_dir, filename or '%s.%s' % (hashlib.md5(url).hexdigest(), common.get_extension(url)))
        if not os.path.exists(save_path) or override:
            # need to download
            _bytes = self.get(url, num_redirects=0, write_cache=False)
            if _bytes:
                if not os.path.exists(save_dir):
                    os.makedirs(save_dir)
                open(save_path, 'wb').write(_bytes)
            else:
                return None
        return save_path


def get_redirect(url, html):
    """Check for meta redirects and return redirect URL if found
    """
    match = re.compile('<meta[^>]*?url=(.*?)["\']', re.IGNORECASE).search(html)
    if match:
        return urlparse.urljoin(url, common.unescape(match.groups()[0].strip())) 


class GoogleMaps:
    def __init__(self, D):
        self.D = D


    def geocode(self, address, delay=5, read_cache=True, num_retries=1, language=None):
        """Geocode address using Google's API and return dictionary of useful fields

        address:
            what to pass to geocode API
        delay:
            how long to delay between API requests
        read_cache:
            whether to load content from cache when exists
        num_retries:
            the number of times to try downloading
        language:
            the language to set
        """
        try:
            address = address.encode('utf-8')
        except UnicodeDecodeError:
            common.logger.debug('Geocode failed to parse address and needed to cast to ascii: ' + address)
            address = common.to_ascii(address)
        geocode_url = 'http://maps.google.com/maps/api/geocode/json?address=%s&sensor=false%s' % (urllib.quote_plus(address), '&language=' + language if language else '')
        geocode_html = self.D.get(geocode_url, delay=delay, read_cache=read_cache, num_retries=num_retries)
        geocode_data = self.load_result(geocode_url, geocode_html)
        for result in geocode_data.get('results', []):
            return self.parse_location(result)
        return collections.defaultdict(str)


    def places(self, api_key, keyword, latitude, longitude, radius=10000, delay=5, num_retries=1, language='en'):
        """Search the Google Place API for this keyword and location

        api_key is the Google Places API key: https://developers.google.com/places/documentation/#Authentication
        radius around the location can be a maximum 50000

        Returns a list of up to 200 matching places
        """
        search_url_template = 'https://maps.googleapis.com/maps/api/place/radarsearch/json?key={0}&location={1},{2}&radius={3}&keyword={4}&sensor=false'
        place_url_template = 'https://maps.googleapis.com/maps/api/place/details/json?key={0}&reference={1}&language={2}&sensor=false'

        search_url = search_url_template.format(api_key, latitude, longitude, radius, keyword).replace(' ', '+')
        search_html = self.D.get(search_url, delay=delay, num_retries=num_retries)
        search_results = self.load_result(search_url, search_html)

        place_results = []
        # iterate search results
        for search_result in search_results.get('results', []):
            reference = search_result['reference']
            # found a new place
            place_url = place_url_template.format(api_key, reference, language)
            place_html = self.D.get(place_url, delay=delay, num_retries=num_retries)

            place = self.load_result(place_url, place_html)
            if place:
                place = place['result']
                result = self.parse_location(place)
                result['name'] = place['name']
                result['categories'] = place['types']
                result['phone'] = place.get('formatted_phone_number', '')
                result['website'] = place.get('website', '')
                place_results.append(result)
        return place_results


    def load_result(self, url, html):
        """Parse the result from API

        If JSON is well formed and status is OK then will return result
        Else will return an empty dict
        """
        if html:
            try:
                search_data = json.loads(html)
            except ValueError as e:
                common.logger.debug(str(e))
            else:
                status = search_data['status']
                if status == 'OK':
                    return search_data
                elif status == 'ZERO_RESULTS':
                    pass
                elif status == 'OVER_QUERY_LIMIT':
                    # error geocoding - try again later
                    self.D.cache[url] = ''
                elif status in ('REQUEST_DENIED', 'INVALID_REQUEST'):
                    common.logger.info('{0}: {1}'.format(status, url))
        return {}


    def parse_location(self, result):
        """Parse address data from Google's geocoding response into a more usable flat structure
        
        Example: https://developers.google.com/maps/documentation/geocoding/#JSON
        """
        results = collections.defaultdict(str)
        for e in result['address_components']:
            # parse address compenents into flat layer
            types, value, abbrev = e['types'], e['long_name'], e['short_name']
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
                results['state_code'] = abbrev
            elif 'administrative_area_level_2' in types:
                results['county'] = value
            elif 'administrative_area_level_3' in types:
                results['district'] = value
            elif 'country' in types:
                results['country'] = value
                results['country_code'] = abbrev
       
        # extract addresses 
        results['full_address'] = result['formatted_address']
        if 'street' in results:
            results['address'] = (results['number'] + ' ' + results['street']).strip()

        results['lat'] = result['geometry']['location']['lat']
        results['lng'] = result['geometry']['location']['lng']
        return results



class StopCrawl(Exception):
    """Raise this exception to interrupt crawl
    """
    pass


def threaded_get(url=None, urls=None, url_iter=None, num_threads=10, dl=None, cb=None, depth=True, **kwargs):
    """Download these urls in parallel

    url:
        the webpage to download
    urls:
        the webpages to download
    num_threads:
        the number of threads to download urls with
    cb:
        Called after each download with the HTML of the download. 
        The arguments are the url and downloaded html.
        Whatever URLs are returned are added to the crawl queue.
    dl:
        A callback for customizing the download.
        Takes the download object and url and should return the HTML.
    depth:
        True for depth first search
    """
    running = True
    lock = threading.Lock()
    def add_iter_urls():
        if lock.acquire(False):
            for url in url_iter or []:
                download_queue.append(url)
                break
            lock.release()


    def process_queue():
        """Thread for downloading webpages
        """
        D = Download(**kwargs)

        while True:
            try:
                url = download_queue.pop() if depth else download_queue.popleft()

            except IndexError:
                add_iter_urls()
                break

            else:
                # download this url
                html = dl(D, url, **kwargs) if dl else D.get(url, **kwargs)
                if cb:
                    try:
                        # use callback to process downloaded HTML
                        result = cb(D, url, html)

                    except StopCrawl:
                        common.logger.info('Stopping crawl signal')
                        self.running = False

                    except Exception:
                        # catch any callback error to avoid losing thread
                        common.logger.exception('\nIn callback for: ' + str(url))

                    else:
                        # add these URL's to crawl queue
                        for link in result or []:
                            download_queue.append(link)
                                        
                # update the crawler state
                # no download or error so must have read from cache
                num_caches = 0 if D.num_downloads or D.num_errors else 1
                state.update(num_downloads=D.num_downloads, num_errors=D.num_errors, num_caches=num_caches, queue_size=len(download_queue))

    download_queue = collections.deque()
    if urls:
        download_queue.extend(urls)
    if url:
        download_queue.append(url)
    common.logger.debug('Start new crawl')

    # initiate the state file with the number of URL's already in the queue
    state = State()
    state.update(queue_size=len(download_queue))

    # wait for all download threads to finish
    threads = []
    while running and (threads or download_queue):
        for thread in threads:
            if not thread.is_alive():
                threads.remove(thread)
        while len(threads) < num_threads and download_queue:
            # cat start more threads
            thread = threading.Thread(target=process_queue)
            thread.setDaemon(True) # set daemon so main thread can exit when receives ctrl-c
            thread.start()
            threads.append(thread)
        time.sleep(SLEEP_TIME)
    # save the final state after threads finish
    state.save()



class State:
    """Save state of crawl to disk

    output_file:
        where to save the state
    timeout:
        how many seconds to wait between saving the state
    """
    def __init__(self, output_file=None, timeout=10):
        # where to save state to
        self.output_file = output_file or settings.status_file
        # how long to wait between saving state
        self.timeout = timeout
        # track the number of downloads and errors
        self.num_downloads = self.num_errors = self.num_caches = self.queue_size = 0
        # data to save to disk
        self.data = {}
        # whether data needs to be saved to dosk
        self.flush = False
        # track time duration of crawl
        self.start_time = time.time()
        self.last_time = 0
        # a lock to prevent multiple threads writing at once
        self.lock = threading.Lock()

    def update(self, num_downloads=0, num_errors=0, num_caches=0, queue_size=0):
        """Update the state with these values

        num_downloads:
            the number of downloads completed successfully
        num_errors:
            the number of errors encountered while downloading
        num_caches:
            the number of webpages read from cache instead of downloading
        queue_size:
            the number of URL's in the queue
        """
        self.num_downloads += num_downloads
        self.num_errors += num_errors
        self.num_caches += num_caches
        self.queue_size = queue_size
        self.data['num_downloads'] = self.num_downloads
        self.data['num_errors'] = self.num_errors
        self.data['num_caches'] = self.num_caches
        self.data['queue_size'] = self.queue_size

        if time.time() - self.last_time > self.timeout:
            self.lock.acquire()
            self.save()
            self.lock.release()

    def save(self):
        """Save state to disk
        """
        self.last_time = time.time()
        self.data['duration_secs'] = int(self.last_time - self.start_time)
        self.flush = False
        text = json.dumps(self.data)
        tmp_file = '%s.%d' % (self.output_file, os.getpid())
        fp = open(tmp_file, 'wb')
        fp.write(text)
        # ensure all content is written to disk
        fp.flush()
        fp.close()
        try:
            if os.name == 'nt': 
                # on windows can not rename if file exists
                if os.path.exists(self.output_file):
                    os.remove(self.output_file)
            # atomic copy to new location so state file is never partially written
            os.rename(tmp_file, self.output_file)
        except OSError:
            pass



class CrawlerCallback:
    """Example callback to crawl a website
    """
    def __init__(self, output_file=None, max_links=100, max_depth=1, allowed_urls='', banned_urls='^$', robots=None, crawl_existing=True):
        """
        output_file:
            where to save scraped data
        max_links:
            the maximum number of links to follow per page
        max_depth:
            the maximum depth to follow links into website (use None for no limit)
        allowed_urls:
            a regex for allowed urls, defaults to all urls
        banned_urls:
            a regex for banned urls, defaults to no urls
        robots:
            RobotFileParser object to determine which urls allowed to crawl
        crawl_existing:
            sets whether to crawl content already downloaded previously in the cache
        """
        self.found = adt.HashDict(int) # track depth of found URLs
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
        # override this method to add scraping code ...
        return self.crawl(D, url, html)                                                                                                          


    def normalize(self, url, link):
        """Normalize the link to avoid duplicates

        >>> cb = CrawlerCallback()
        >>> cb.normalize('http://example.com', '../abc.html')
        'http://example.com/abc.html'
        >>> cb.normalize('http://example.com', 'abc.html#link')
        'http://example.com/abc.html'
        >>> cb.normalize('http://example.com', 'abc.html?a=1&amp;b=2')
        'http://example.com/abc.html?a=1&b=2'
        """
        link, _ = urlparse.urldefrag(link) # remove hash to avoid duplicates
        link = common.unescape(link) # parse escaped characters such as &amp;
        link = urlparse.urljoin(url, link) # support relative links
        while urlparse.urlsplit(link).path.startswith('/..'):
            # remove invalid parent directory
            link = link.replace('/..', '', 1)
        return link


    def crawl(self, D, url, html): 
        """Crawl website html and return list of URLs crawled
        """
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
        depth = self.found[url]
        outstanding = []
        if depth != self.max_depth: 
            # extract links to continue crawling
            links_re = re.compile('<a[^>]+href=["\'](.*?)["\']', re.IGNORECASE)
            for link in links_re.findall(html):
                try:
                    link = self.normalize(url, link)
                except UnicodeDecodeError as e:
                    # unicode error when joining url
                    common.logger.info(e)
                else:
                    if link not in self.found:
                        self.found[link] = depth + 1
                        if valid(link):
                            # is a new link
                            outstanding.append(link)
                            if len(outstanding) == self.max_links:
                                break
        return outstanding


class DataCrawler:
    """Crawl a website and return all matches extracted using a given function
    """
    def __init__(self, D, extract_fn):
        """
        extract_fn:
            a function to parse given HTML and return a list of matches
        """
        self.D = D
        self.extract_fn = extract_fn

    def link_score(self, link):
        """Return how valuable this link is for ordering crawling
        The lower the better"""
        link = link.lower()
        total = 0
        if 'contact' in link:
            pass # this page is top priority
        elif 'about' in link:
            total += 10
        elif 'help' in link:
            total += 20
        else:
            # generic page
            total += 100
        # bias towards shorter links
        total += len(link)
        return total

    def find(self, website, max_depth, max_urls, max_results):
        """
        website:
            the URL of website to crawl
        max_depth:
            how many links deep to follow before stop crawl
        max_urls:
            how many URL's to download before stop crawl
        max_results:
            The maximum number of results to extract before stop crawl.
            If None then extract all results found in crawl.
        """
        # check for redirect URL
        self.D.get(website)
        redirect_url = self.D.cache.meta(website).get('url') if self.D.cache else self.final_url
        website = redirect_url or website
        
        domain = urlparse.urlparse(website).netloc
        scraped = adt.HashDict()
        c = CrawlerCallback(max_depth=max_depth)
        outstanding = [(0, website)] # list of URLs and their score
        results = []
        while outstanding and (max_urls is None or len(scraped) < max_urls) \
                          and (max_results is None or len(results) < max_results):
            _, url = outstanding.pop(0)
            scraped[url] = True
            html = self.D.get(url, num_retries=0)

            if html:
                for result in self.extract_fn(html):
                    if result not in results:
                        results.append(result)
                        if len(results) == max_results:
                            break
                # crawl the linked URLs
                for link in c.crawl(self, url, html):
                    if urlparse.urlparse(link).netloc == domain:
                        if link not in scraped:
                            # insert sort this new record so crawl most promising first
                            score = self.link_score(link)
                            for i, (other_score, other_link) in enumerate(outstanding):
                                if score < other_score:
                                    outstanding.insert(i, ((score, link)))
                                    break
                            else:
                                outstanding.append((score, link))
        return results
