#
# Description: Framework for crawling and scraping webpages with JQuery
# Author: Richard Penman (richard@sitescraper.net)
#

import sys
import os
import urllib2
from PyQt4.QtGui import QApplication, QDesktopServices
from PyQt4.QtCore import QString, QUrl, QTimer
from PyQt4.QtWebKit import QWebView, QWebPage
from PyQt4.QtNetwork import QNetworkAccessManager, QNetworkProxy, QNetworkRequest, QNetworkReply, QNetworkDiskCache
from webscraping import common, settings
 


"""
TODO
right click find xpath:
    http://doc.qt.nokia.com/4.6/webkit-domtraversal.html
    http://doc.qt.nokia.com/4.6/webkit-simpleselector.html
textbox for jquery input
    http://www.rkblog.rk.edu.pl/w/p/webkit-pyqt-rendering-web-pages/
threaded multiple URLs
timeout
interface with cache to expand and not use pdict

make scrape function sequential after dentist data
"""
class NetworkAccessManager(QNetworkAccessManager):
    def __init__(self, proxy, allowed_extensions, cache_size=100):
        """Subclass QNetworkAccessManager to finer control network operations
        proxy is a QNetworkProxy
        allowed_extensions is a list of extensions to allow
        cache_size is the maximum size of the cache (MB)
        """
        QNetworkAccessManager.__init__(self)
        # initialize the manager cache
        cache = QNetworkDiskCache()#this)
        #QDesktopServices.storageLocation(QDesktopServices.CacheLocation)
        cache.setCacheDirectory('webkit_cache')
        cache.setMaximumCacheSize(cache_size * 1024 * 1024) # need to convert cache value to bytes
        self.setCache(cache)
        # allowed content extensions
        self.banned_extensions = common.MEDIA_EXTENSIONS
        for ext in allowed_extensions:
            if ext in self.banned_extensions:
                self.banned_extensions.remove(ext)
        # and proxy
        if proxy:
            self.setProxy(proxy)
    
    def createRequest(self, operation, request, data):
        #print request.url().toString()
        # XXX cache all requests here
        if operation == self.GetOperation:
            if self.is_forbidden(request):
                # deny GET request for banned media type by setting dummy URL
                #print 'denied'
                request.setUrl(QUrl(QString('forbidden://localhost/')))
            else:
                print request.url().toString()
        request.setAttribute(QNetworkRequest.CacheLoadControlAttribute, QNetworkRequest.PreferCache)
        #connect(reply, SIGNAL(error(QNetworkReply::NetworkError)), this, SLOT(requestError(QNetworkReply::NetworkError)
        reply = QNetworkAccessManager.createRequest(self, operation, request, data)
        reply.error.connect(self.catch_error)
        return reply

    def is_forbidden(self, request):
        """Returns whether this request is permitted by checking URL extension
        XXX head request for mime?
        """
        return common.get_extension(str(request.url().toString())) in self.banned_extensions

    def catch_error(self, eid):
        if eid not in (301, ):
            print 'Error:', eid, self.sender().url().toString()


class WebPage(QWebPage):
    def __init__(self, user_agent):
        QWebPage.__init__(self)
        # set user agent
        self.user_agent = user_agent

    def userAgentForUrl(self, url):
        return self.user_agent

    def javaScriptAlert(self, frame, message):
        """Override default javascript alert popup
        """
        print 'Alert:', message

    def javaScriptConsoleMessage(message, line_number, source_id):
        print 'Console:', message, line_number, source_id


class JQueryBrowser(QWebView):
    """Render webpages using webkit
    """

    def __init__(self, url, gui=False, user_agent=None, proxy=None, allowed_extensions=['.html', '.css', '.js'], timeout=20):
        """
        url is the seed URL where to start crawling
        gui is whether to show webkit window or run headless
        user_agent is used to set the user-agent when downloading content
        proxy is the proxy to download through
        allowed_extensions are the media types to allow
        timeout is the maximum amount of seconds to wait for a request
        """
        self.app = QApplication(sys.argv) # must instantiate first
        QWebView.__init__(self)
        webpage = WebPage(user_agent or settings.user_agent)
        manager = NetworkAccessManager(proxy, allowed_extensions)
        webpage.setNetworkAccessManager(manager)
        self.setPage(webpage)
        self.loadFinished.connect(self._loadFinished)
        self.history = [] # track history of urls crawled
        # initiate the timer
        timer = QTimer()
        timer.setInterval(1000 * timeout) # convert timeout to ms
        timer.timeout.connect(self.error)
        self.timer = timer
        self.seed_url = url
        self.start()
        if gui: self.show() 
        self.app.exec_()


    def start(self):
        self.go(self.seed_url)

    def error(self):
        print 'timed out'
        self.start()
         
    def go(self, url):
        """Load given url in webkit
        """
        self.history.append(url)
        self.load(QUrl(url))
 
    def crawl(self, url, html):
        """This slot is called when the given URL has been crawled and returned this HTML
        """
        return False # return False to stop crawling

    def js(self, script):
        """Shortcut to execute javascript
        """
        self.frame.evaluateJavaScript(script)

    def inject_jquery(self):
        """Inject jquery library into this webpage for easier manipulation
        """
        url = 'http://ajax.googleapis.com/ajax/libs/jquery/1/jquery.min.js'
        if url in self.cache:
            jquery_lib = self.cache[url]
        else:
            jquery_lib = urllib2.urlopen(url).read()
            self.cache[url] = jquery_lib
        self.js(jquery_lib)

    def _loadFinished(self, success):
        """slot for webpage finished loading
        """
        current_url = str(self.url().toString())
        if not success:
            raise Exception('Failed to load URL: ' + current_url)
        self.frame = self.page().mainFrame()
        html = unicode(self.frame.toHtml())
        self.cache[current_url] = html
        self.inject_jquery()

        if self.crawl(current_url, html):
            self.timer.start() # reset timer
        else:
            self.app.quit() # call this to stop crawling # XXX automatic way?


class TestBrowser(JQueryBrowser):
    def crawl(self, url, html):
        return True


if __name__ == '__main__':
    proxy = QNetworkProxy(QNetworkProxy.HttpProxy, '127.0.0.1', 8118)
    TestBrowser(url='http://whatsmyuseragent.com/', gui=True, proxy=proxy)
    #TestBrowser(url='http://www.ioerror.us/ip/', gui=True)
