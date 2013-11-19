__doc__ = 'Helper methods to download and crawl web content using threads'

import sys
import time
import cookielib
import base64
import signal
import urlparse

from twisted.internet import reactor, defer
from twisted.internet.protocol import Protocol
from twisted.internet.endpoints import TCP4ClientEndpoint
from twisted.web.client import Agent, RedirectAgent, HTTPConnectionPool, CookieAgent, ContentDecoderAgent, GzipDecoder, ProxyAgent
from twisted.web.http_headers import Headers
from twisted.python import log

import adt, common, download, settings



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
        # URL's currently downloading with the number of retry attempts
        self.retries = {}
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
            print 'Shutting down'
            self.running = False
            self.state.save()
            reactor.stop()


    def kill(self, *ignore):
        """Exit the script
        """
        self.stop()
        sys.exit()


    def cache_html(self):
        """Cache the downloaded HTML
        """
        if self.cache_queue:
            cache_queue, self.cache_queue = self.cache_queue, []
            if self.D.cache and self.settings.write_cache:
                common.logger.debug('Cached: %d' % len(cache_queue))
                self.D.cache.update(cache_queue)


    def crawl(self):
        """Crawl more URLs if available
        """
        if self.download_queue or self.retries or self.cache_queue:
            #print len(self.download_queue), len(self.cache_queue), self.retries
            while self.running and self.download_queue and len(self.retries) < self.settings.num_threads:
                url = self.download_queue.pop() if self.settings.depth else self.download_queue.pop(0)
                self.retries[url] = 0
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
                reactor.callLater(0, self.cache_html)
                reactor.callLater(0, self.crawl)
        else:
            # save the final state and exit
            self.stop()
        

    def download_start(self, url):
        """Start url download
        """
        proxy = self.D.get_proxy()
        headers = dict(self.settings.headers)
        headers['User-Agent'] = [self.D.get_user_agent(proxy)]
        agent = self.build_agent(proxy, headers)
        data = None
        d = agent.request('GET', url, Headers(headers), data) 
        d.addCallback(self.download_headers, url)
        d.addErrback(self.download_error, url)
        d.addErrback(log.err)

        # timeout to stop download if hangs
        timeout_call = reactor.callLater(self.settings.timeout, self.download_timeout, d, url)
        def completed(ignore):
            # remove timeout callback on completion
            if timeout_call.active():
                timeout_call.cancel()
        d.addBoth(completed)
       

    def download_headers(self, response, url):
        """Headers have been returned from download
        """
        common.logger.info('Downloading ' + url)
        finished = defer.Deferred()
        # XXX how to ignore processing body for errors?
        response.deliverBody(DownloadPrinter(finished))
        if 400 <= response.code < 500:
            # XXX pass that don't want to retry
            raise Exception(response.phrase)
        elif 500 <= response.code < 600:
            # server error so try again
            raise Exception(response.phrase)
        else:
            # handle download
            #finished.addCallback(download_complete, url)
            #finished.addErrback(download_error, url)
            finished.addCallbacks(self.download_complete, self.download_error, callbackArgs=[url], errbackArgs=[url])
            finished.addErrback(log.err)


    def download_complete(self, html, url):
        """Body has completed downloading
        """
        self.cache_queue.append((url, html))
        self.state.update(num_downloads=1)
        reactor.callLater(0, self.scrape, url, html)


    def download_timeout(self, d, url):
        """Catch timeout error and cancel request
        """
        common.logger.warning('Download timeout: ' + url)
        d.cancel()


    def download_error(self, reason, url):
        """Error received during download
        """
        self.state.update(num_errors=1)

        num_retries = self.retries[url]
        if self.retries[url] < self.settings.num_retries:
            # retry the download
            common.logger.debug('Download retry: %d: %s' % (num_retries, url))
            self.retries[url] += 1
            reactor.callLater(0, self.download_start, url)
        else:
            # out of retries
            common.logger.warning('Download error: %s: %s' % (reason.getErrorMessage(), url))
            if num_retries > 0:
                common.logger.debug('Retry failure')
            del self.retries[url]
            self.cache_queue.append((url, ''))



    def scrape(self, url, html):
        """Pass completed body to callback for scraping
        """
        del self.retries[url]
        if html and self.settings.cb:
            try:
                links = self.settings.cb(self.D, url, html) or []
            except Exception as e:
                common.logger.exception(e)
            else:
                for link in links:
                    cb_url = urlparse.urljoin(url, link)
                    if cb_url not in self.found:
                        self.found[cb_url] = True
                        self.download_queue.append(cb_url)


    def build_pool(self):
        """Create connection pool
        """
        # XXX connections take too much memory?
        pool = HTTPConnectionPool(reactor, persistent=True)
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
            endpoint = TCP4ClientEndpoint(reactor, fragments.host, int(fragments.port))
            agent = ProxyAgent(endpoint, reactor=reactor, pool=pool)
        else:
            agent = Agent(reactor, connectTimeout=self.settings.timeout, pool=pool)

        agent = ContentDecoderAgent(agent, [('gzip', GzipDecoder)])
        agent = RedirectAgent(agent, self.settings.num_redirects)
        #cookieJar = cookielib.CookieJar()
        #agent = CookieAgent(agent, cookieJar)
        return agent



class DownloadPrinter(Protocol):
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
