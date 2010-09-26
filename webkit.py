#
# Description: Framework for crawling and scraping webpages with JQuery
# Author: Richard Penman (richard@sitescraper.net)
#

import sys
import os
import urllib2
from PyQt4.QtGui import QApplication, QDesktopServices
from PyQt4.QtCore import QString, QUrl, QTimer, QEventLoop, QIODevice
from PyQt4.QtWebKit import QWebView, QWebPage
from PyQt4.QtNetwork import QNetworkAccessManager, QNetworkProxy, QNetworkRequest, QNetworkReply, QNetworkDiskCache
from webscraping import common, pdict, settings
 


TOR_PROXY = QNetworkProxy(QNetworkProxy.HttpProxy, '127.0.0.1', 8118)
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

    def __init__(self, proxy, allowed_extensions, cache_size=100, cache_dir='.webkit_cache'):
        """
        proxy is a QNetworkProxy
        allowed_extensions is a list of extensions to allow
        cache_size is the maximum size of the cache (MB)
        """
        QNetworkAccessManager.__init__(self)
        # initialize the manager cache
        cache = QNetworkDiskCache()
        #QDesktopServices.storageLocation(QDesktopServices.CacheLocation)
        cache.setCacheDirectory(cache_dir)
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
        if operation == self.GetOperation:
            if self.is_forbidden(request):
                # deny GET request for banned media type by setting dummy URL
                request.setUrl(QUrl(QString('forbidden://localhost/')))
            else:
                print request.url().toString()
        else:
            pass
            #print 'POST'
            #print request.url().toString()
            #data.open(QIODevice.ReadOnly)
            #print data.readAll()
            #print data.peek(100000000000)
            #data.seek(0)
            #data.close()
        request.setAttribute(QNetworkRequest.CacheLoadControlAttribute, QNetworkRequest.PreferCache)
        reply = QNetworkAccessManager.createRequest(self, operation, request, data)
        #reply.finished.connect(self.catch_finished)
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
    """Override QWebPage to set User-Agent and JavaScript messages
    """

    def __init__(self, user_agent):
        QWebPage.__init__(self)
        self.user_agent = user_agent


    def userAgentForUrl(self, url):
        return self.user_agent


    def javaScriptAlert(self, frame, message):
        """Override default JavaScript alert popup and print results
        """
        print 'Alert:', message


    def javaScriptConsoleMessage(self, message, line_number, source_id):
        """Print JavaScript console messages
        """
        print 'Console:', message, line_number, source_id



class JQueryBrowser(QWebView):
    """Render webpages using webkit
    """

    def __init__(self, base_url=None, gui=False, user_agent=None, proxy=None, allowed_extensions=['.html', '.css', '.js'], timeout=20, cache_file=None, debug=False):
        """
        base_url is the domain that will be crawled
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
        manager.finished.connect(self.finished)
        webpage.setNetworkAccessManager(manager)
        self.setPage(webpage)
        self.setHtml('<html><head></head><body>No content loaded</body></html>', QUrl('http://localhost'))
        self.timeout = timeout
        self.cache = pdict.PersistentDict(cache_file or settings.cache_file) # cache to store webpages
        self.base_url = base_url
        self.debug = debug
        self.jquery_lib = None
        QTimer.singleShot(0, self.crawl) # start crawling when all events processed
        if gui: self.show() 
        self.app.exec_() # start GUI thread


    def current_url(self):
        """Return current URL
        """
        return str(self.url().toString())

    def current_html(self):
        """Return current rendered HTML
        """
        return unicode(self.page().mainFrame().toHtml())


    def error(self):
        print 'timed out'
        self.start()
        

    def get(self, url=None, script=None, key=None):
        """Load given url in webkit and return html when loaded
        """
        self.base_url = self.base_url or url # set base URL if not set
        html = self.cache.get(key)
        if html:
            if self.debug: print 'load cache', key 
            self.setHtml(html, QUrl(self.base_url))
        elif url:
            self.load(QUrl(url))
        elif script:
            self.js(script)

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
            html = self.current_html()
            if key:
                self.cache[key] = html
            self.inject_jquery()
        else:
            # didn't download in time
            print 'Download timeout'
            html = ''
        return html

    def jsget(self, script, key=None):
        return self.get(script=script, key=key)


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


    def finished(self, reply):
        """Override this in subclasses to process downloaded urls
        """
        pass





if __name__ == '__main__':
    JQueryBrowser(gui=True)
