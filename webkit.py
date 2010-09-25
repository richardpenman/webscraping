#
# Description: Framework for crawling and scraping webpages with JQuery
# Author: Richard Penman (richard@sitescraper.net)
#

import sys
import os
import urllib2
from PyQt4.QtGui import QApplication, QDesktopServices
from PyQt4.QtCore import QString, QUrl, QTimer, QEventLoop
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
    http://www.pyside.org/docs/pyside/PySide/QtCore/QEventLoop.html?highlight=qeventloop

add progress bar for loading page
implement watir API
"""
class NetworkAccessManager(QNetworkAccessManager):
    """Subclass QNetworkAccessManager for finer control network operations
    """

    def __init__(self, proxy, allowed_extensions, cache_size=100):
        """
        proxy is a QNetworkProxy
        allowed_extensions is a list of extensions to allow
        cache_size is the maximum size of the cache (MB)
        """
        QNetworkAccessManager.__init__(self)
        # initialize the manager cache
        cache = QNetworkDiskCache()#this)
        #QDesktopServices.storageLocation(QDesktopServices.CacheLocation)
        cache.setCacheDirectory('.webkit_cache')
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
    """Override QWebPage to set user agent and javascript messages
    """

    def __init__(self, user_agent):
        QWebPage.__init__(self)
        self.user_agent = user_agent


    def userAgentForUrl(self, url):
        return self.user_agent


    def javaScriptAlert(self, frame, message):
        """Override default javascript alert popup
        """
        print 'Alert:', message


    def javaScriptConsoleMessage(self, message, line_number, source_id):
        print 'Console:', message, line_number, source_id



class JQueryBrowser(QWebView):
    """Render webpages using webkit
    """

    def __init__(self, gui=False, user_agent=None, proxy=None, allowed_extensions=['.html', '.css', '.js'], timeout=20):
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
        self.setHtml('<html><head></head><body>No content loaded</body></html>', QUrl('http://localhost'))
        self.timeout = timeout
        self.jquery_lib = None
        QTimer.singleShot(0, self.crawl) # start crawling when all events processed
        if gui: self.show() 
        self.app.exec_() # start GUI thread


    def error(self):
        print 'timed out'
        self.start()
        

    def get(self, url=None):
        """Load given url in webkit and return html when loaded
        """
        if url:
            self.load(QUrl(url))
        loop = QEventLoop()
        timer = QTimer()
        timer.setSingleShot(True)
        timer.timeout.connect(loop.quit)
        self.loadFinished.connect(loop.quit)
    
        timer.start(self.timeout * 1000)
        loop.exec_() # delay here until download finished or timeout
    
        if timer.isActive():
            # downloaded successfully
            timer.stop()
            html = unicode(self.page().mainFrame().toHtml())
            #self.cache[current_url] = html
            self.inject_jquery()
        else:
            # didn't download in time
            print 'Download timeout'
            html = ''
        return html


    def currentURL(self):
        """Return current URL
        """
        return str(self.url().toString())


    def js(self, script):
        """Shortcut to execute javascript on current document
        """
        self.page().mainFrame().evaluateJavaScript(script)


    def inject_jquery(self):
        """Inject jquery library into this webpage for easier manipulation
        """
        # XXX embed header in document, use cache
        if self.jquery_lib is None:
            url = 'http://ajax.googleapis.com/ajax/libs/jquery/1/jquery.min.js'
            self.jquery_lib = urllib2.urlopen(url).read()
        self.js(self.jquery_lib)


    def crawl(self):
        """Override this method in subclass
        """
        self.get('http://code.google.com/p/webscraping/')
        self.get('http://code.google.com/p/sitescraper/')
        QTimer.singleShot(5000, self.app.quit)




if __name__ == '__main__':
    proxy = QNetworkProxy(QNetworkProxy.HttpProxy, '127.0.0.1', 8118)
    JQueryBrowser(gui=True, proxy=proxy)
