#
# Description: Framework for crawling and scraping webpages with JQuery
# Author: Richard Penman (richard@sitescraper.net)
# License: LGPL
#

import sys
import os
import re
import urllib2
import time
import random
from datetime import datetime
from PyQt4.QtGui import QApplication, QDesktopServices
from PyQt4.QtCore import QString, QUrl, QTimer, QEventLoop, QIODevice, QObject
from PyQt4.QtWebKit import QWebView, QWebPage
from PyQt4.QtNetwork import QNetworkAccessManager, QNetworkProxy, QNetworkRequest, QNetworkReply, QNetworkDiskCache
from webscraping import common, pdict, settings
 


TOR_PROXY = QNetworkProxy(QNetworkProxy.HttpProxy, '127.0.0.1', 8118)
DEBUG = False
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

    def __init__(self, proxy, allowed_media, allowed_regex, cache_size=100, cache_dir='.webkit_cache'):
        """
        See JQueryBrowser for details of arguments
        cache_size is the maximum size of the webkit cache (MB)
        """
        QNetworkAccessManager.__init__(self)
        # initialize the manager cache
        #QDesktopServices.storageLocation(QDesktopServices.CacheLocation)
        cache = QNetworkDiskCache()
        cache.setCacheDirectory(cache_dir)
        cache.setMaximumCacheSize(cache_size * 1024 * 1024) # need to convert cache value to bytes
        self.setCache(cache)
        self.allowed_regex = allowed_regex
        # allowed content extensions
        self.banned_extensions = common.MEDIA_EXTENSIONS
        for ext in allowed_media:
            if ext in self.banned_extensions:
                self.banned_extensions.remove(ext)
        # and proxy
        self.setProxy(proxy)


    def setProxy(self, proxy):
        """Allow setting string as proxy
        """
        if isinstance(proxy, basestring):
            match = re.match('(http://)?(.*?):(\d+)', proxy)
            if match:
                scheme, ip, port = match.groups()
                proxy = QNetworkProxy(QNetworkProxy.HttpProxy, ip, int(port))
            else:
                print 'Invalid proxy:', proxy
                proxy = None
        if proxy:
            QNetworkAccessManager.setProxy(self, proxy)


    def createRequest(self, operation, request, data):
        if operation == self.GetOperation:
            if self.is_forbidden(request):
                # deny GET request for banned media type by setting dummy URL
                # XXX abort properly
                request.setUrl(QUrl(QString('forbidden://localhost/')))
            elif DEBUG:
                print request.url().toString()
        request.setAttribute(QNetworkRequest.CacheLoadControlAttribute, QNetworkRequest.PreferCache)
        reply = QNetworkAccessManager.createRequest(self, operation, request, data)
        reply.error.connect(self.catch_error)
        reply.data = ''
        #if common.get_extension(str(request.url().toString())) not in ('js', 'css'):
        if 'Search' in str(request.url().toString()):
            reply = NetworkReply(reply)
        return reply


    def is_forbidden(self, request):
        """Returns whether this request is permitted by checking URL extension and regex
        XXX head request for mime?
        """
        forbidden = False
        url = str(request.url().toString())
        if common.get_extension(url) in self.banned_extensions:
            forbidden = True
        elif re.match(self.allowed_regex, url) is None:
            forbidden = True
        return forbidden


    def catch_error(self, eid):
        if DEBUG and eid not in (301, ):
            # XXX show string type of error
            print 'Error:', eid, self.sender().url().toString()


# XXX not working properly for js, css, cache - try pasting all methods back again to see what gets called differently
class NetworkReply(QNetworkReply):
    def __init__(self, reply):
        QNetworkReply.__init__(self)
        self.reply = reply # reply to proxy
        self.data = '' # contains downloaded data
        self.buffer = '' # contains buffer of data to read
        self.setOpenMode(QNetworkReply.ReadOnly)
        
        # connect signal from proxy reply
        reply.metaDataChanged.connect(self.applyMetaData)
        reply.readyRead.connect(self.readInternal)
        #reply.error.connect(self.errorInternal)
        reply.finished.connect(self.finished)
        reply.uploadProgress.connect(self.uploadProgress)
        reply.downloadProgress.connect(self.downloadProgress)

    
    def __getattribute__(self, attr):
        """Send undefined methods straight through to proxied reply
        """
        # send these attributes through to proxy reply 
        if attr in ('operation', 'request', 'url', 'abort', 'close', 'isSequential'):
            #print attr
            return self.reply.__getattribute__(attr)
        else:
            return QNetworkReply.__getattribute__(self, attr)
    
    def abort(self):
        pass # qt requires that this be defined

    def bytesToWrite(self):
        print 'btytes to write'
        return -1

    def canReadLine(self):
        print 'can read'
        return False
    def waitForReadyRead(self, t):
        print 'wait ready'
        return False
    def waitForBytesWritten(self, t):
        print 'wait written'
        return False

    def readAll(self):
        print 'read all'
        return self.data

    def read(self, size):
        print 'read'

    def readLine(self):
        print 'line'

    def isReadable(self):
        print 'is read'

    def seek(self, s):
        print 'seek'

    def isFinished(self):
        print 'is finished'

    def isRunning(self):
        print 'is running'


    def attribute (self,code):
        print 'attribute'
    def errorCode (self):
        print 'errorcode'
    def hasRawHeader (self,headerName):
        print 'has raw header'
    def header (self,header):
        print 'header'
    def ignoreSslErrors (self, errors=None):
        print 'ignore ssl'
    def manager (self):
        print 'manager'
    def rawHeader (self,headerName):
        print 'rawheader'
    def rawHeaderList (self):
        print 'raw headerlist'
    def readBufferSize (self):
        print 'read buffer'
    def setError (self,errorCode, errorString):
        print 'set error'
    def setReadBufferSize (self,size):
        print 'setreadbuffersize'
    def setSslConfiguration (self,configuration):
        print 'setssl'
    def sslConfiguration (self):
        print 'sskcibf;'

    def sslErrors (self,errors):
        print 'sslerrors'
    def aboutToClose (self):
        print 'about'
    def atEnd (self):
        print 'at end'
    def bytesToWrite (self):
        print 'bytes to write'
    def bytesWritten (self, bytes):
        print 'bytes written'
    def canReadLine (self):
        print 'canread'
    def close (self):
        print 'close'
    def errorString (self):
        print 'errors tring'
    def getChar (self):
        print 'getchar'
    def isOpen (self):
        print 'isopen'
    def isReadable (self):
        print 'is readable'
    def isTextModeEnabled (self):
        print 'istext'
    def isWritable (self):
        print 'iswrite'
    def open (self, mode):
        print 'open'
    def openMode (self):
        print 'openmode'
        return self.reply.openMode()

    def peek (self, maxlen):
        print 'peek'
    def pos (self):
        print 'pos'
    def putChar (self,c):
        print 'putcha'
    def readChannelFinished (self):
        print 'read channel'
    def readData (self, data, maxlen):
        print 'read daa'
    def reset (self):
        print 'reset'
    def seek (self, pos):
        print 'seek;'
    def setErrorString (self,errorString):
        print 'set error'
    def setTextModeEnabled (self, enabled):
        print 'setttextmodeeb'
    def size (self):
        print 'size'
    def ungetChar (self,c):
        print 'ungetchar'
    def waitForBytesWritten (self,msecs):
        print 'wait for btes'
    def waitForReadyRead (self,msecs):
        print 'wait for ready read'
    def write (self,data):
        print 'write'
    def writeData (self,data, len):
        print 'writedata'

    def applyMetaData(self):
        for header in self.reply.rawHeaderList():
            self.setRawHeader(header, self.reply.rawHeader(header))

        self.setHeader(QNetworkRequest.ContentTypeHeader, self.reply.header(QNetworkRequest.ContentTypeHeader))
        self.setHeader(QNetworkRequest.ContentLengthHeader, self.reply.header(QNetworkRequest.ContentLengthHeader))
        self.setHeader(QNetworkRequest.LocationHeader, self.reply.header(QNetworkRequest.LocationHeader))
        self.setHeader(QNetworkRequest.LastModifiedHeader, self.reply.header(QNetworkRequest.LastModifiedHeader))
        self.setHeader(QNetworkRequest.SetCookieHeader, self.reply.header(QNetworkRequest.SetCookieHeader))

        self.setAttribute(QNetworkRequest.HttpStatusCodeAttribute, self.reply.attribute(QNetworkRequest.HttpStatusCodeAttribute))
        self.setAttribute(QNetworkRequest.HttpReasonPhraseAttribute, self.reply.attribute(QNetworkRequest.HttpReasonPhraseAttribute))
        self.setAttribute(QNetworkRequest.RedirectionTargetAttribute, self.reply.attribute(QNetworkRequest.RedirectionTargetAttribute))
        self.setAttribute(QNetworkRequest.ConnectionEncryptedAttribute, self.reply.attribute(QNetworkRequest.ConnectionEncryptedAttribute))
        self.setAttribute(QNetworkRequest.CacheLoadControlAttribute, self.reply.attribute(QNetworkRequest.CacheLoadControlAttribute))
        self.setAttribute(QNetworkRequest.CacheSaveControlAttribute, self.reply.attribute(QNetworkRequest.CacheSaveControlAttribute))
        self.setAttribute(QNetworkRequest.SourceIsFromCacheAttribute, self.reply.attribute(QNetworkRequest.SourceIsFromCacheAttribute))
        # attribute is undefined
        #self.setAttribute(QNetworkRequest.DoNotBufferUploadDataAttribute, self.reply.attribute(QNetworkRequest.DoNotBufferUploadDataAttribute))
        self.metaDataChanged.emit()

    #def errorInternal(self, e):
    #    self.error.emit(e)
    #    self.setError(e, str(e))

    def bytesAvailable(self):
        """How many bytes in the buffer are available to be read
        """
        return len(self.buffer) + self.reply.bytesAvailable()

    def readInternal(self):
        """New data available to read
        """
        s = str(self.reply.readAll())
        self.data += s
        self.buffer += s
        self.readyRead.emit()

    def readData(self, size):
        """Return up to size bytes from buffer
        """
        size = min(size, len(self.buffer))
        data, self.buffer = self.buffer[:size], self.buffer[size:]
        return data


class WebPage(QWebPage):
    """Override QWebPage to set User-Agent and JavaScript messages
    """

    def __init__(self, user_agent, confirm=True):
        QWebPage.__init__(self)
        self.user_agent = user_agent
        self.confirm = confirm


    def userAgentForUrl(self, url):
        return self.user_agent


    def javaScriptAlert(self, frame, message):
        """Override default JavaScript alert popup and print results
        """
        if DEBUG: print 'Alert:', message

    def javaScriptConfirm(self, frame, message):
        """Override default JavaScript confirm popup and print results
        """
        if DEBUG: print 'Confirm:', message
        return self.confirm

    def javaScriptPrompt(self, frame, message, default):
        """Override default JavaScript prompt popup and print results
        """
        if DEBUG: print 'Prompt:', message, default

    def javaScriptConsoleMessage(self, message, line_number, source_id):
        """Print JavaScript console messages
        """
        if DEBUG: print 'Console:', message, line_number, source_id



class JQueryBrowser(QWebView):
    """Render webpages using webkit
    """

    def __init__(self, base_url=None, gui=False, user_agent=None, proxy=None, allowed_media=['css', 'js'], allowed_regex='.*?', timeout=20, delay=5, cache_file=None):
        """
        base_url is the domain that will be crawled
        gui is whether to show webkit window or run headless
        user_agent is used to set the user-agent when downloading content
        proxy is a QNetworkProxy to download through
        allowed_media are the media extensions to allow
        allowed_regex is a regular expressions of URLS to allow
        timeout is the maximum amount of seconds to wait for a request
        delay is the minimum amount of seconds to wait between requests
        """
        self.app = QApplication(sys.argv) # must instantiate first
        QWebView.__init__(self)
        webpage = WebPage(user_agent or settings.user_agent)
        manager = NetworkAccessManager(proxy, allowed_media, allowed_regex)
        manager.finished.connect(self.finished)
        webpage.setNetworkAccessManager(manager)
        self.setPage(webpage)
        self.setHtml('<html><head></head><body>No content loaded</body></html>', QUrl('http://localhost'))
        self.timeout = timeout
        self.delay = delay
        self.cache = pdict.PersistentDict(cache_file or settings.cache_file) # cache to store webpages
        self.base_url = base_url
        self.jquery_lib = None
        QTimer.singleShot(0, self.crawl) # start crawling when all events processed
        if gui: self.show() 
        self.app.exec_() # start GUI thread


    def debug(self, message):
        # proper logging XXX
        if DEBUG:
            print message

    def current_url(self):
        """Return current URL
        """
        return str(self.url().toString())

    def current_html(self):
        """Return current rendered HTML
        """
        return unicode(self.page().mainFrame().toHtml())


    def get(self, url=None, script=None, key=None, retries=1):
        """Load given url in webkit and return html when loaded
        """
        t1 = datetime.now()
        self.base_url = self.base_url or url # set base URL if not set
        html = self.cache.get(key, {}).get('value')
        if html:
            self.debug('Load cache ' + key)
            self.setHtml(html, QUrl(self.base_url))
        else:
            loop = QEventLoop()
            timer = QTimer()
            timer.setSingleShot(True)
            timer.timeout.connect(loop.quit)
            self.loadFinished.connect(loop.quit)
            if url:
                self.load(QUrl(url))
            elif script:
                self.js(script)
            timer.start(self.timeout * 1000)
            loop.exec_() # delay here until download finished or timeout
        
            if timer.isActive():
                # downloaded successfully
                timer.stop()
                html = self.current_html()
                if key:
                    self.cache[key] = html
                self.wait(t1)
            else:
                # didn't download in time
                if retries > 0:
                    print 'timeout - retrying'
                    self.debug('Timeout - retrying')
                    html = self.get(url, script, key, retries-1)
                else:
                    print 'timed out'
                    self.debug('Timed out')
                    html = ''
        if html:
            self.inject_jquery()
        return html


    def wait(self, t1):
        """Wait for delay time
        """
        keep_waiting = True
        while keep_waiting:
            self.app.processEvents()
            dt = datetime.now() - t1
            wait_secs = self.delay - dt.days * 24 * 60 * 60 - dt.seconds
            #print 'wait', wait_secs
            keep_waiting = wait_secs > 0
        # randomize the delay so less suspicious
        #wait_secs += 0.5 * self.delay * (random.random() - 0.5)
        #time.sleep(max(0, wait_secs))


    def jsget(self, script, key=None, retries=1):
        """Execute JavaScript that will cause page submission, and wait for page to load
        """
        return self.get(script=script, key=key, retries=retries)


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
        """Override this method in subclass to crawl website
        """
        #self.get('http://code.google.com/p/webscraping/')
        #self.get('http://code.google.com/p/sitescraper/')
        self.get('http://nmlsconsumeraccess.org')
        #self.load(QUrl('http://nmlsconsumeraccess.org/Home.aspx/SubSearch?searchText=California&entityType=&state=&page=1'))
        #html = self.get('http://sitescraper.net')
        #self.load(QUrl('http://www.google.com.au'))
        QTimer.singleShot(10000, self.app.quit)


    def finished(self, reply):
        """Override this method in subclasses to process downloaded urls
        """
        pass #print reply.url().toString(), ':', len(reply.data)
        


if __name__ == '__main__':
    DEBUG = True
    JQueryBrowser(gui=True, proxy=TOR_PROXY)#, allowed_media=[])
