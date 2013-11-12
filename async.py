__doc__ = 'Helper methods to download and crawl web content using threads'

from cookielib import CookieJar
import base64
from twisted.internet import reactor, defer
from twisted.internet.protocol import Protocol
from twisted.internet.endpoints import TCP4ClientEndpoint
from twisted.web.client import Agent, RedirectAgent, HTTPConnectionPool, CookieAgent, ContentDecoderAgent, GzipDecoder, ProxyAgent
from twisted.web.http_headers import Headers

import adt, common, download, settings



def threaded_get(url=None, urls=None, num_threads=10, cb=None, depth=True, **kwargs):
    """Download using asynchronous single threaded twisted callbacks
    """
    state = download.State()

    settings = adt.Bag(
        read_cache = True,
        write_cache = True,
        num_redirects = 5,
        num_retries = 0,
        timeout = 30,
        headers = {}
    )
    settings.update(**kwargs)
    D = download.Download(**kwargs)
    # XXX connections take too much memory?
    pool = HTTPConnectionPool(reactor, persistent=True)
    # 1 connection for each proxy or thread
    pool.maxPersistentPerHost = len(D.settings.proxies) or num_threads
    pool.retryAutomatically = settings.num_retries > 0
    pool.cachedConnectionTimeout = 240

    agent = Agent(reactor, connectTimeout=settings.timeout, pool=pool)
    agent = ContentDecoderAgent(agent, [('gzip', GzipDecoder)])
    agent = RedirectAgent(agent, settings.num_redirects)
    #cookieJar = CookieJar()
    #agent = CookieAgent(agent, cookieJar)
    # XXX 
    # efficient write caching in separate thread
    # list of urls to crawl
    # XXX compressed dict data type for large in memory
    outstanding = urls or []
    if url:
        outstanding.append(url)
    # list of URL's currently processing
    processing = set() 
    found = adt.HashDict()

    def build_agent(proxy, headers):
        """Build an agent for this request
        """
        fragments = common.parse_proxy(proxy)
        if fragments.host:
            # add proxy authentication header
            auth = base64.b64encode("%s:%s" % (fragments.username, fragments.password))
            headers['Proxy-Authorization'] = ["Basic " + auth.strip()]
            # generate the agent
            endpoint = TCP4ClientEndpoint(reactor, fragments.host, int(fragments.port))
            agent = ProxyAgent(endpoint, reactor=reactor, pool=pool)
        else:
            agent = Agent(reactor, connectTimeout=settings.timeout, pool=pool)

        agent = ContentDecoderAgent(agent, [('gzip', GzipDecoder)])
        agent = RedirectAgent(agent, settings.num_redirects)
        return agent


    def crawl():
        """Crawl another URL if available
        """
        while outstanding and len(processing) < num_threads:
            url = outstanding.pop() if depth else outstanding.pop(0)
            processing.add(url)
            downloaded = False
            if D.cache and settings.read_cache:
                key = D.get_key(url, settings.data)
                try:
                    # XXX could load in thread here for speed
                    html = D.cache[key]
                except KeyError:
                    pass 
                else:
                    # html is available so scrape this directly
                    if html or settings.num_retries == 0:
                        reactor.callLater(0, scrape, html, url)
                        downloaded = True

            if downloaded:
                state.update(num_caches=1)
            else:
                download_start(url)
            state.update(queue_size=len(outstanding))
        
        if outstanding or processing:
            # try crawling again a little later
            pass
        else:
            # save the final state and exit
            state.save()
            reactor.stop()
        

    def download_start(url):
        """Start url download
        """
        proxy = D.get_proxy()
        headers = dict(settings.headers)
        headers['User-Agent'] = [D.get_user_agent(proxy)]
        agent = build_agent(proxy, headers)
        data = None
        d = agent.request('GET', url, Headers(headers), data) 
        #d.addCallback(download_headers, url)
        #print 'download start', url, len(processing), len(outstanding)
        #d.addCallbacks(download_headers, download_error, callbackArgs=[url], errbackArgs=[url])
        d.addCallback(download_headers, url)
        d.addErrback(download_error, url) #XXX

        timeout_call = reactor.callLater(settings.timeout, download_timeout, d, url)
        def completed(ignore):
            if timeout_call.active():
                timeout_call.cancel()
        d.addBoth(completed)
        #d.setTimeout(settings.timeout, download_timeout, url)
        
    def download_headers(response, url):
        """Headers have been returned from download
        """
        print 'Downloading:', url
        finished = defer.Deferred()
        response.deliverBody(DownloadPrinter(finished))
        if response.code >= 400:
            # XXX how to ignore processing body?
            raise Exception('Download error: %s %s' % (url, response.phrase))
        else:
            finished.addCallback(download_complete, url)
            finished.addErrback(download_error, url)
            #finished.addCallbacks(download_complete, download_error, callbackArgs=[url], errbackArgs=[url])

    def download_complete(html, url):
        """Body has completed downloading
        """
        # XXX put caching in thread?
        #print 'Download complete:', url, len(processing), len(outstanding)
        if D.cache and settings.write_cache:
            D.cache[url] = html
        state.update(num_downloads=1)
        reactor.callLater(0, scrape, html, url)

    def download_timeout(d, url):
        print 'Time out:', url
        d.cancel()

    def download_error(reason, url):
        """Error received during download
        """
        processing.remove(url)
        print 'Error:', url, reason.getErrorMessage()
        state.update(num_errors=1)
        if D.cache and settings.write_cache:
            D.cache[url] = ''
        crawl()

    def scrape(html, url):
        """Pass completed body to callback for scraping
        """
        if html and cb:
            cb_urls = cb(D, url, html)
            if cb_urls:
                for cb_url in cb_urls:
                    if cb_url not in found:
                        found[cb_url] = True
                        outstanding.append(cb_url)
        processing.remove(url)
        crawl()

    reactor.callWhenRunning(crawl)
    reactor.run()


class DownloadPrinter(Protocol):
    """Collect together body requests
    """
    def __init__(self, finished):
        self.finished = finished
        self.data = []

    def dataReceived(self, page):
        self.data.append(page)

    def connectionLost(self, reason):
        if str(reason.value) != 'Response body fully received':
            print reason.value
        self.finished.callback(''.join(self.data))

