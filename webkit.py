#
# Description: Framework for crawling and scraping webpages with JQuery
# Author: Richard Penman (richard@sitescraper.net)
#

import sys
import os
import urllib2
from PyQt4 import QtGui, QtWebKit
from PyQt4.QtCore import QString, QUrl
from PyQt4.QtNetwork import QNetworkAccessManager, QNetworkProxy, QNetworkRequest, QNetworkReply, QNetworkDiskCache
from webscraping import common, download, pdict, xpath
 


"""
TODO
right click find xpath:
    http://doc.qt.nokia.com/4.6/webkit-domtraversal.html
    http://doc.qt.nokia.com/4.6/webkit-simpleselector.html
threaded multiple URLs
timeout
interface with cache to expand and not use pdict
"""
class NetworkAccessManager(QNetworkAccessManager):
    def __init__(self, proxy, allowed_extensions, cache_size=100):
        """proxy is a QNetworkProxy
        allowed_extensions is a list of extensions to allow
        cache_size is the maximum size of the cache (MB)
        """
        QNetworkAccessManager.__init__(self)

        # initialize the manager cache
        cache = QNetworkDiskCache()#this)
        cache.setCacheDirectory('webkit_cache')
        cache.setMaximumCacheSize(cache_size * 1024 * 1024) # need to convert cache value to bytes
        self.setCache(cache)
        # allowed content extensions
        self.banned_extensions = download.Download.MEDIA_EXTENSIONS
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
        reply = QNetworkAccessManager.createRequest(self, operation, request, data)
        print reply.attribute(QNetworkRequest.SourceIsFromCacheAttribute).toBool()
        return reply

    def is_forbidden(self, request):
        """Returns whether this request is permitted by checking URL extension
        XXX head request for mime?
        """
        ext = os.path.splitext(str(request.url().toString()))[-1]
        return ext in self.banned_extensions


class WebPage(QtWebKit.QWebPage):
    def __init__(self, user_agent):
        QtWebKit.QWebPage.__init__(self)
        # set user agent
        self.user_agent = user_agent

    def userAgentForUrl(self, url):
        return self.user_agent

    def javaScriptAlert(self, frame, message):
        """Override default javascript alert popup
        """
        print 'Alert:', message


class JQueryBrowser(QtWebKit.QWebView):
    """Render webpages using webkit
    """

    def __init__(self, url, gui=False, cache_file='cache.db', user_agent=None, proxy=None, allowed_extensions=['.html', '.css', '.js']):
        """
        url is the seed URL where to start crawling
        gui is whether to show webkit window or run headless
        cache_file is the filename of the cache file to use
        user_agent is used to set the user-agent when downloading content
        proxy is the proxy to download through
        allowed_extensions are the media types to allow
        """
        self.app = QtGui.QApplication(sys.argv) # must instantiate first
        QtWebKit.QWebView.__init__(self)
        webpage = WebPage(user_agent or QString('Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.2.9) Gecko/20100824 Firefox/3.6.9'))
        manager = NetworkAccessManager(proxy, allowed_extensions)
        webpage.setNetworkAccessManager(manager)
        self.setPage(webpage)
        self.loadStarted.connect(self._loadStarted)
        self.loadFinished.connect(self._loadFinished)
        self.cache = pdict.PersistentDict(cache_file) # cache to store webpages
        self.history = []
        self.go(url)
        if gui: self.show() 
        self.app.exec_()

    def go(self, url, use_cache=True):
        """Load given url in webkit
        """
        if use_cache and url in self.cache:
            html = self.cache[url]
            self.setHtml(QString(html), QUrl(url)) # creates crash if immediately load here XXX
        else:
            self.history.append(url)
            self.load(QUrl(url))
 
    def crawl(self, url, html):
        """This slot is called when the given URL has been crawled and returned this HTML
        """
        return False # return False to stop crawling

    def jquery(self, js):
        """Execute jquery function on current loaded webpage
        """
        self.frame.evaluateJavaScript('$' + js) 

    def inject_jquery(self):
        """Inject jquery library into this webpage for easier manipulation
        """
        url = 'http://ajax.googleapis.com/ajax/libs/jquery/1/jquery.min.js'
        if url in self.cache:
            jquery_lib = self.cache[url]
        else:
            jquery_lib = urllib2.urlopen(url).read()
            self.cache[url] = jquery_lib
        self.frame.evaluateJavaScript(jquery_lib)

    def _loadStarted(self):
        pass

    def _loadFinished(self, success):
        """slot for webpage finished loading
        """
        current_url = str(self.url().toString())
        self.frame = self.page().mainFrame()
        html = unicode(self.frame.toHtml())
        if success:
            self.cache[current_url] = html
        else:
            raise Exception('Failed to load URL: ' + current_url)

        self.inject_jquery()
        if not self.crawl(current_url, html):
            self.app.quit() # call this to stop crawling # XXX automatic way?


class TestBrowser(JQueryBrowser):
    def crawl(self, url, html):
        return True


if __name__ == '__main__':
    proxy = QNetworkProxy(QNetworkProxy.HttpProxy, '127.0.0.1', 8118)
    TestBrowser(url='http://whatsmyuseragent.com/', gui=True, proxy=proxy)
    #TestBrowser(url='http://www.ioerror.us/ip/', gui=True)
