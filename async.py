__doc__ = 'Helper methods to download and crawl web content using threads'

import sys
import time
import cookielib
import base64
import signal
import urlparse
import collections

from twisted.internet import reactor, defer, protocol, endpoints
from twisted.web import client, error, http, http_headers
from twisted.python import failure, log

import adt, common, download, settings


"""
TODO
- support for POST
- efficient get request callback
"""


def threaded_get(**kwargs):
    """Download using asynchronous single threaded twisted callbacks
    """
    tc = TwistedCrawler(**kwargs)
    tc.start()


class TwistedCrawler:
    def __init__(self, url=None, urls=None, url_iter=None, num_threads=20, cb=None, depth=True, max_errors=None, pattern=None, **kwargs):
        self.settings = adt.Bag(
            read_cache = True,
            write_cache = True,
            num_redirects = 5,
            num_retries = 2,
            timeout = 20,
            headers = {},
            num_threads = num_threads,
            cb = cb,
            url_iter = url_iter,
            depth = depth,
            pattern = pattern
        )
        self.settings.update(**kwargs)
        self.D = download.Download(**kwargs)
        self.kwargs = kwargs
        # queue of html to be written to cache
        self.cache_queue = []
        # URL's that are waiting to download
        self.download_queue = collections.deque()
        if urls:
            self.download_queue.extend(urls)
        if url:
            self.download_queue.append(url) # XXX create compressed dict data type for large in memory?
        # URL's currently downloading 
        self.processing = {}
        # defereds that are downloading
        self.downloading = []
        # URL's that have been found before
        self.found = adt.HashDict()
        for url in self.download_queue:
            self.found[url] = True
        self.state = download.State()
        self.max_errors = max_errors
        self.num_errors = 0 # counter for the number of subsequent errors


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
        for d in self.downloading:
            d.cancel()
        self.stop()
        sys.exit()


    def is_finished(self):
        """Call finish callback in case more processing to do
        """
        for url in self.settings.url_iter or []:
            self.download_queue.append(url)
            return False
        return True
            

    def crawl(self):
        """Crawl more URLs if available
        """
        if self.download_queue or self.processing or self.cache_queue or not self.is_finished():
            #print 'Running: %d, queue: %d, cache: %d, processing: %d, threads: %d' % (self.running, len(self.download_queue), len(self.cache_queue), len(self.processing), self.settings.num_threads)
            while self.running and self.download_queue and len(self.processing) < self.settings.num_threads:
                url = str(self.download_queue.pop() if self.settings.depth else self.download_queue.popleft())
                self.processing[url] = ''
                downloaded = False
                if self.D.cache and self.settings.read_cache:
                    key = self.D.get_key(url, self.settings.data)
                    try:
                        html = self.D.cache[key]
                    except KeyError:
                        pass 
                    else:
                        # html is available so scrape this directly
                        if self.D.invalid_response(html, self.settings.pattern):
                            # invalid result from download
                            html = ''
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

                # XXX test inactive
                try:
                    self.inactive_call.cancel()
                except AttributeError:
                    pass # not defined yet
                self.inactive_call = reactor.callLater(5*60, self.inactive)
                # XXX

            if self.running:
                reactor.callLater(0, self.cache_downloads)
                reactor.callLater(0, self.crawl)
        else:
            # save the final state and exit
            self.stop()


    def inactive(self):
        common.logger.error('crawler inactive')
        common.logger.error('queue (%d): %s' % (len(self.download_queue), ', '.join(self.download_queue)))
        common.logger.error('processing (%d): %s' % (len(self.processing), ', '.join(self.processing)))
        self.stop()


    def download_start(self, url, num_retries=0, redirects=None, proxy=None):
        """Start URL download
        """
        redirects = redirects or []
        redirects.append(url)
        if not proxy:
            proxy = self.D.get_proxy()
            self.processing[redirects[0]] = proxy

        headers = {}
        headers['User-Agent'] = [self.settings.get('user_agent', self.D.get_user_agent(proxy))]
        for name, value in self.settings.headers.items() + settings.default_headers.items():
            if name not in headers:
                if not value:
                    if name == 'Referer':
                        value = url
                headers[name] = [value]
        agent = self.build_agent(proxy, headers)
        data = None
        d = agent.request('GET', url, http_headers.Headers(headers), data) 
        d.addCallback(self.download_headers, url, num_retries, redirects)
        d.addErrback(self.download_error, redirects[0])
        d.addErrback(log.err)

        # timeout to stop download if hangs
        timeout_call = reactor.callLater(self.settings.timeout, self.download_timeout, d, url)
        self.downloading.append(d)

        def completed(ignore):
            # remove timeout callback on completion
            if timeout_call.active():
                timeout_call.cancel()
                self.downloading.remove(d)
        d.addBoth(completed)


    def download_headers(self, response, url, num_retries, redirects):
        """Headers have been returned from download
        """
        common.logger.info('Downloading ' + url)
        finished = defer.Deferred()
        # XXX how to ignore processing body for errors?
        response.deliverBody(DownloadPrinter(finished))
        if self.handle_redirect(url, response, num_retries, redirects):
            # redirect handled
            pass
        elif 400 <= response.code < 500:
            raise TwistedError(response.phrase)
        elif 500 <= response.code < 600:
            # server error so try again
            message = '%s (%d)' % (response.phrase, response.code)
            self.handle_retry(url, message, num_retries, redirects)
        elif self.running:
            # handle download
            finished.addCallbacks(self.download_complete, self.download_error, 
                callbackArgs=[num_retries, redirects], errbackArgs=[redirects[0]]
            )
            finished.addErrback(self.download_error, redirects[0])


    def download_complete(self, html, num_retries, redirects):
        """Body has completed downloading
        """
        redirect_url = download.get_redirect(redirects[0], html)
        if redirect_url:
            # meta redirect
            proxy = self.processing[redirects[0]]
            reactor.callLater(0, self.download_start, redirect_url, 0, redirects, proxy)
        elif self.D.invalid_response(html, self.settings.pattern):
            # invalid result from download
            message = 'Content did not match expected pattern'
            self.handle_retry(redirects[0], message, num_retries, redirects)

        else:
            # successful download
            self.num_errors = 0
            self.state.update(num_downloads=1)
            if self.D.cache and self.settings.write_cache:
                self.cache_queue.append((redirects, html))
            reactor.callLater(0, self.scrape, redirects[0], html)


    def download_timeout(self, d, url):
        """Catch timeout error and cancel request
        """
        self.downloading.remove(d)
        d.cancel()


    def download_error(self, reason, url):
        """Error received during download
        """
        # XXX how to properly pass error from download timeout cancel
        error = reason.getErrorMessage() or 'Download timeout' 
        common.logger.warning('Download error: %s: %s' % (error, url))
        self.state.update(num_errors=1)
        if self.D.cache and self.settings.write_cache:
            self.cache_queue.append((url, ''))
        del self.processing[url]
        # check whether to give up the crawl
        self.num_errors += 1
        if self.max_errors is not None:
            common.logger.debug('Errors: %d / %d' % (self.num_errors, self.max_errors))
            if self.num_errors > self.max_errors:
                common.logger.error('Too many download errors, shutting down')
                self.stop()


    def handle_retry(self, url, message, num_retries, redirects):
        """Handle retrying a download error
        """
        if num_retries < self.settings.num_retries:
            # retry the download
            common.logger.info('Download retry: %d: %s' % (num_retries, url))
            reactor.callLater(0, self.download_start, url, num_retries+1, redirects)
        else:
            # out of retries
            raise TwistedError('Retry failure: %s' % message)


    def handle_redirect(self, url, response, num_retries, redirects):
        """Handle redirects - the builtin RedirectAgent does not handle relative redirects
        """
        if response.code in (301, 302, 303, 307):
            # redirect HTTP code
            locations = response.headers.getRawHeaders('location', [])
            if locations:
                # a new redirect url
                if len(redirects) < self.settings.num_redirects:
                    # can still redirect
                    redirect_url = urlparse.urljoin(url, locations[0])
                    if redirect_url != url:
                        # new redirect URL
                        redirects.append(url)
                        reactor.callLater(0, self.download_start, redirect_url, num_retries, redirects)
                        return True
        return False


    def scrape(self, url, html):
        """Pass completed body to callback for scraping
        """
        del self.processing[url]
        if self.settings.cb and self.running:
            try:
                # get links crawled from webpage
                links = self.settings.cb(self.D, url, html) or []
            except download.StopCrawl:
                common.logger.info('Stopping crawl signal')
                self.stop()
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
        # XXX create limited number of instances
        pool = client.HTTPConnectionPool(reactor, persistent=True)
        # 1 connection for each proxy or thread
        # XXX will this take too much memory?
        pool.maxPersistentPerHost = len(self.D.settings.proxies) or self.settings.num_threads
        pool.cachedConnectionTimeout = 240
        return pool


    #agents = {}
    cookiejars = {}
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
            endpoint = endpoints.TCP4ClientEndpoint(reactor, fragments.host, int(fragments.port), timeout=self.settings.timeout)
            agent = client.ProxyAgent(endpoint, reactor=reactor, pool=pool)
        else:
            agent = client.Agent(reactor, connectTimeout=self.settings.timeout, pool=pool)

        agent = client.ContentDecoderAgent(agent, [('gzip', client.GzipDecoder)])
        # XXX if use same cookie for all then works...
        # cookies usually empty
        if proxy in self.cookiejars:
            cj = self.cookiejars[proxy]
        else:
            cj = cookielib.CookieJar()
            self.cookiejars[proxy] = cj
        agent = client.CookieAgent(agent, cj)
        return agent


    def cache_downloads(self):
        """Cache the downloaded HTML
        """
        if self.cache_queue:
            while self.cache_queue:
                redirects, html = self.cache_queue.pop()
                common.logger.debug('Cached: %d' % len(self.cache_queue))
                url = redirects[0]
                self.D[url] = html
                final_url = redirects[-1]
                if url != final_url:
                    # store the redirect map
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
