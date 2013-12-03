__doc__ = 'Helper methods to download and crawl web content using threads'

import sys
import time
import cookielib
import base64
import signal
import urlparse

from twisted.internet import reactor, defer, protocol, endpoints
from twisted.web import client, error, http, http_headers
from twisted.python import failure, log

import adt, common, download, settings


"""
TODO
- support for POST
- clean killing of outstanding requests
"""


def threaded_get(**kwargs):
    """Download using asynchronous single threaded twisted callbacks
    """
    tc = TwistedCrawler(**kwargs)
    tc.start()


class TwistedCrawler:
    def __init__(self, url=None, urls=None, num_threads=20, cb=None, depth=True, **kwargs):
        self.settings = adt.Bag(
            read_cache = True,
            write_cache = True,
            num_redirects = 5,
            num_retries = 3,
            timeout = 30,
            headers = {},
            num_threads = num_threads,
            cb = cb,
            depth = depth
        )
        self.settings.update(**kwargs)
        self.D = download.Download(**kwargs)
        self.kwargs = kwargs
        # queue of html to be written to cache
        self.cache_queue = []
        # URL's that are waiting to download
        self.download_queue = (urls or [url])[:] # XXX create compressed dict data type for large in memory?
        # URL's currently downloading 
        self.processing = set()
        # URL's that have been found before
        self.found = adt.HashDict()
        for url in self.download_queue:
            self.found[url] = True
        self.state = download.State()


    def start(self):
        """Start the twisted event loop
        """
        # catch ctrl-c keyboard event and stop twisted
        signal.signal(signal.SIGINT, self.kill)
        self.running = True
        reactor.callWhenRunning(self.crawl)
        reactor.run()


    def stop(self):
        """Stop the twisted event loop
        """
        if self.running:
            common.logger.info('Twisted eventloop shutting down')
            self.running = False
            self.state.save()
            reactor.stop()


    def kill(self, *ignore):
        """Exit the script
        """
        self.stop()
        sys.exit()


    def crawl(self):
        """Crawl more URLs if available
        """
        if self.download_queue or self.processing or self.cache_queue:
            #print len(self.download_queue), len(self.cache_queue), self.processing
            while self.running and self.download_queue and len(self.processing) < self.settings.num_threads:
                url = self.download_queue.pop() if self.settings.depth else self.download_queue.pop(0)
                self.processing.add(url)
                downloaded = False
                if self.D.cache and self.settings.read_cache:
                    key = self.D.get_key(url, self.settings.data)
                    try:
                        html = self.D.cache[key]
                    except KeyError:
                        pass 
                    else:
                        # html is available so scrape this directly
                        if html or self.settings.num_retries == 0:
                            reactor.callLater(0, self.scrape, url, html)
                            downloaded = True

                if downloaded:
                    # record cache load
                    self.state.update(num_caches=1)
                else:
                    # need to download this new URL
                    self.download_start(url)
                self.state.update(queue_size=len(self.download_queue))
      
            if self.running:
                reactor.callLater(0, self.cache_downloads)
                reactor.callLater(0, self.crawl)
        else:
            # save the final state and exit
            self.stop()
        

    def download_start(self, url, num_retries=0, redirects=None):
        """Start url download
        """
        redirects = redirects or []
        proxy = self.D.get_proxy()
        headers = dict(self.settings.headers)
        headers['User-Agent'] = [self.D.get_user_agent(proxy)]
        for name, value in settings.default_headers.items():
            if name not in headers:
                if name == 'Referer':
                    value = url
                headers[name] = [value]
        agent = self.build_agent(proxy, headers)
        data = None
        d = agent.request('GET', url, http_headers.Headers(headers), data) 
        d.addCallback(self.download_headers, url, num_retries, redirects)
        d.addErrback(self.download_error, url)
        d.addErrback(log.err)

        # timeout to stop download if hangs
        timeout_call = reactor.callLater(self.settings.timeout, self.download_timeout, d, url)
        def completed(ignore):
            # remove timeout callback on completion
            if timeout_call.active():
                timeout_call.cancel()
        d.addBoth(completed)
       

    def download_headers(self, response, url, num_retries, redirects):
        """Headers have been returned from download
        """
        common.logger.info('Downloading ' + url)
        finished = defer.Deferred()
        # XXX how to ignore processing body for errors?
        response.deliverBody(DownloadPrinter(finished))
        if response.code in (301, 302, 303, 307) and self.handle_redirect(url, response, num_retries, redirects):
            # redirect handled
            pass
        elif 400 <= response.code < 500:
            raise TwistedError(response.phrase)
        elif 500 <= response.code < 600:
            # server error so try again
            self.handle_retry(url, response, num_retries, redirects)
        else:
            # handle download
            #finished.addCallback(download_complete, url)
            #finished.addErrback(download_error, url)
            finished.addCallbacks(self.download_complete, self.download_error, 
                callbackArgs=[redirects + [url]], errbackArgs=[url]
            )
            finished.addErrback(log.err)


    def download_complete(self, html, redirects):
        """Body has completed downloading
        """
        self.state.update(num_downloads=1)
        if self.D.cache and self.settings.write_cache:
            self.cache_queue.append((redirects, html))
        reactor.callLater(0, self.scrape, redirects[0], html)


    def download_timeout(self, d, url):
        """Catch timeout error and cancel request
        """
        common.logger.warning('Download timeout: ' + url)
        d.cancel()


    def download_error(self, reason, url):
        """Error received during download
        """
        common.logger.warning('Download error: %s: %s' % (reason.getErrorMessage(), url))
        self.state.update(num_errors=1)
        if self.D.cache and self.settings.write_cache:
            self.cache_queue.append((url, ''))
        self.processing.remove(url)


    def handle_retry(self, url, response, num_retries, redirects):
        """Handle retrying a download error
        """
        if num_retries < self.settings.num_retries:
            # retry the download
            common.logger.debug('Download retry: %d: %s' % (num_retries, url))
            reactor.callLater(0, self.download_start, url, num_retries+1, num_redirects)
        else:
            # out of retries
            raise TwistedError('Retry failure: ' + response.phrase)


    def handle_redirect(self, url, response, num_retries, redirects):
        """Handle redirects - the builtin RedirectAgent does not handle relative redirects
        """
        locations = response.headers.getRawHeaders('location', [])
        if locations:
            if len(redirects) < self.settings.num_redirects:
                # a new redirect url
                redirect_url = urlparse.urljoin(url, locations[0])
                redirects.append(url)
                reactor.callLater(0, self.download_start, redirect_url, num_retries, redirects)
                return True
            else: 
                # too many redirects
                return False
        else:
            # no location header given to redirect to
            err = error.RedirectWithNoLocation(response.code, 'No location header field', url)
            raise TwistedError([failure.Failure(err)], response)


    def scrape(self, url, html):
        """Pass completed body to callback for scraping
        """
        self.processing.remove(url)
        if html and self.settings.cb:
            try:
                # get links crawled from webpage
                links = self.settings.cb(self.D, url, html) or []
            except Exception as e:
                common.logger.exception('\nIn callback for: ' + str(url))
            else:
                # add new links to queue
                for link in links:
                    cb_url = urlparse.urljoin(url, link)
                    if cb_url not in self.found:
                        self.found[cb_url] = True
                        self.download_queue.append(cb_url)


    def build_pool(self):
        """Create connection pool
        """
        # XXX connections take too much memory?
        pool = client.HTTPConnectionPool(reactor, persistent=True)
        # 1 connection for each proxy or thread
        pool.maxPersistentPerHost = len(self.D.settings.proxies) or self.settings.num_threads
        pool.cachedConnectionTimeout = 240
        return pool


    def build_agent(self, proxy, headers):
        """Build an agent for this request
        """
        fragments = common.parse_proxy(proxy)
        pool = self.build_pool()
        if fragments.host:
            # add proxy authentication header
            auth = base64.b64encode("%s:%s" % (fragments.username, fragments.password))
            headers['Proxy-Authorization'] = ["Basic " + auth.strip()]
            # generate the agent
            endpoint = endpoints.TCP4ClientEndpoint(reactor, fragments.host, int(fragments.port))
            agent = client.ProxyAgent(endpoint, reactor=reactor, pool=pool)
        else:
            agent = client.Agent(reactor, connectTimeout=self.settings.timeout, pool=pool)

        agent = client.ContentDecoderAgent(agent, [('gzip', client.GzipDecoder)])
        #agent = client.RedirectAgent(agent, self.settings.num_redirects)
        #cookieJar = cookielib.CookieJar()
        #agent = CookieAgent(agent, cookieJar)
        return agent


    def cache_downloads(self):
        """Cache the downloaded HTML
        """
        if self.cache_queue:
            url_htmls = []
            redirect_map = {}
            while self.cache_queue:
                redirects, html = self.cache_queue.pop()
                common.logger.debug('Cached: %d' % len(self.cache_queue))
                url = redirects[0]
                url_htmls.append((url, html))
                redirect_map[url] = redirects[-1]
 
            self.D.cache.update(url_htmls)
            # store the redirect map
            for start_url, final_url in redirect_map.items():
                if start_url != final_url:
                    self.D.cache.meta(start_url, dict(url=final_url))



class TwistedError(Exception):
    pass


class DownloadPrinter(protocol.Protocol):
    """Collect together body requests
    """
    def __init__(self, finished):
        self.finished = finished
        self.data = []

    def dataReceived(self, page):
        self.data.append(page)

    def connectionLost(self, reason):
        if str(reason.value) not in ('', 'Response body fully received'):
            common.logger.info('Download body error: ' + str(reason.value))
        html = ''.join(self.data)
        self.finished.callback(html)
