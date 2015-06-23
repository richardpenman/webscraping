__doc__ = 'Interface to qt webkit for parsing JavaScript dependent webpages'

import sys
import os
import re
import urllib2
import random
from time import time, sleep
from datetime import datetime

# for using native Python strings
import sip
sip.setapi('QString', 2)
from PyQt4.QtGui import QApplication, QDesktopServices, QImage, QPainter
from PyQt4.QtCore import QByteArray, QUrl, QTimer, QEventLoop, QIODevice, QObject
from PyQt4.QtWebKit import QWebFrame, QWebView, QWebElement, QWebPage, QWebSettings
from PyQt4.QtNetwork import QNetworkAccessManager, QNetworkProxy, QNetworkRequest, QNetworkReply, QNetworkDiskCache
"""
# XXX some seg faults with subclassing QNetworkAccessManager
from PySide.QtGui import QApplication, QDesktopServices, QImage, QPainter
from PySide.QtCore import QByteArray, QUrl, QTimer, QEventLoop, QIODevice, QObject
from PySide.QtWebKit import QWebFrame, QWebView, QWebPage, QWebSettings
from PySide.QtNetwork import QNetworkAccessManager, QNetworkProxy, QNetworkRequest, QNetworkReply, QNetworkDiskCache
"""
import alg
import common
import settings

"""
TODO
textbox for jquery input
    http://www.rkblog.rk.edu.pl/w/p/webkit-pyqt-rendering-web-pages/
threaded multiple URLs

exit on close window signal

add progress bar for loading page
implement watir API?
"""


class NetworkAccessManager(QNetworkAccessManager):
    """Subclass QNetworkAccessManager for finer control network operations
    """

    def __init__(self, proxy, forbidden_extensions, allowed_regex, cache_size=100, cache_dir='.webkit_cache'):
        """
        See WebkitBrowser for details of arguments
    
        cache_size:
            the maximum size of the webkit cache (MB)
        """
        QNetworkAccessManager.__init__(self)
        # and proxy
        self.setProxy(proxy)
        # initialize the manager cache
        QDesktopServices.storageLocation(QDesktopServices.CacheLocation)
        cache = QNetworkDiskCache()
        cache.setCacheDirectory(cache_dir)
        cache.setMaximumCacheSize(cache_size * 1024 * 1024) # need to convert cache value to bytes
        self.setCache(cache)
        self.allowed_regex = allowed_regex
        self.forbidden_extensions = forbidden_extensions
        #self.sslErrors.connect(sslErrorHandler)


    def setProxy(self, proxy):
        """Allow setting string as proxy
        """
        if proxy:
            fragments = common.parse_proxy(proxy)
            if fragments.host:
                QNetworkAccessManager.setProxy(self, 
                    QNetworkProxy(QNetworkProxy.HttpProxy, 
                      fragments.host, int(fragments.port), 
                      fragments.username, fragments.password
                    )
                )
            else:
                common.logger.info('Invalid proxy: ' + str(proxy))
                proxy = None


    def createRequest(self, operation, request, data):
        if operation == self.GetOperation:
            if self.is_forbidden(request):
                # deny GET request for banned media type by setting dummy URL
                # XXX abort properly
                request.setUrl(QUrl('forbidden://localhost/'))
            else:
                common.logger.debug(common.to_unicode(request.url().toString()).encode('utf-8'))
        
        #print request.url().toString(), operation
        request.setAttribute(QNetworkRequest.CacheLoadControlAttribute, QNetworkRequest.PreferCache)
        reply = QNetworkAccessManager.createRequest(self, operation, request, data)
        reply.error.connect(self.catch_error)
        
        # add Base-Url header, then we can get it from QWebView
        if isinstance(request.originatingObject(), QWebFrame):
            try:
                reply.setRawHeader(QByteArray('Base-Url'), QByteArray('').append(request.originatingObject().page().mainFrame().baseUrl().toString()))
            except Exception, e:
                common.logger.debug(e)
        return reply

    def is_forbidden(self, request):
        """Returns whether this request is permitted by checking URL extension and regex
        XXX head request for mime?
        """
        forbidden = False
        url = common.to_unicode(request.url().toString()).encode('utf-8')
        #url = common.to_unicode(request.url().toString()).encode('utf-8')
        if self.forbidden_extensions and common.get_extension(url) in self.forbidden_extensions:
            forbidden = True
        elif re.match(self.allowed_regex, url) is None:
            forbidden = True
        return forbidden

    def catch_error(self, eid):
        """Interpret the HTTP error ID received
        """
        if eid not in (5, 301):
            errors = {
                0 : 'no error condition. Note: When the HTTP protocol returns a redirect no error will be reported. You can check if there is a redirect with the QNetworkRequest::RedirectionTargetAttribute attribute.',
                1 : 'the remote server refused the connection (the server is not accepting requests)',
                2 : 'the remote server closed the connection prematurely, before the entire reply was received and processed',
                3 : 'the remote host name was not found (invalid hostname)',
                4 : 'the connection to the remote server timed out',
                5 : 'the operation was canceled via calls to abort() or close() before it was finished.',
                6 : 'the SSL/TLS handshake failed and the encrypted channel could not be established. The sslErrors() signal should have been emitted.',
                7 : 'the connection was broken due to disconnection from the network, however the system has initiated roaming to another access point. The request should be resubmitted and will be processed as soon as the connection is re-established.',
                101 : 'the connection to the proxy server was refused (the proxy server is not accepting requests)',
                102 : 'the proxy server closed the connection prematurely, before the entire reply was received and processed',
                103 : 'the proxy host name was not found (invalid proxy hostname)',
                104 : 'the connection to the proxy timed out or the proxy did not reply in time to the request sent',
                105 : 'the proxy requires authentication in order to honour the request but did not accept any credentials offered (if any)',
                201 : 'the access to the remote content was denied (similar to HTTP error 401)',
                202 : 'the operation requested on the remote content is not permitted',
                203 : 'the remote content was not found at the server (similar to HTTP error 404)',
                204 : 'the remote server requires authentication to serve the content but the credentials provided were not accepted (if any)',
                205 : 'the request needed to be sent again, but this failed for example because the upload data could not be read a second time.',
                301 : 'the Network Access API cannot honor the request because the protocol is not known',
                302 : 'the requested operation is invalid for this protocol',
                99 : 'an unknown network-related error was detected',
                199 : 'an unknown proxy-related error was detected',
                299 : 'an unknown error related to the remote content was detected',
                399 : 'a breakdown in protocol was detected (parsing error, invalid or unexpected responses, etc.)',
            }
            common.logger.debug('Error %d: %s (%s)' % (eid, errors.get(eid, 'unknown error'), self.sender().url().toString()))

def sslErrorHandler(reply, errors): 
    print errors
    reply.ignoreSslErrors() 


class NetworkReply(QNetworkReply):
    """Override QNetworkReply so can save the original data
    """
    def __init__(self, parent, reply):
        QNetworkReply.__init__(self, parent)
        self.reply = reply # reply to proxy
        self.data = '' # contains downloaded data
        self.buffer = '' # contains buffer of data to read
        self.setOpenMode(QNetworkReply.ReadOnly | QNetworkReply.Unbuffered)
        #print dir(reply)
        
        # connect signal from proxy reply
        reply.metaDataChanged.connect(self.applyMetaData)
        reply.readyRead.connect(self.readInternal)
        reply.finished.connect(self.finished)
        reply.uploadProgress.connect(self.uploadProgress)
        reply.downloadProgress.connect(self.downloadProgress)
        self.ignoreSslErrors()

    
    def __getattribute__(self, attr):
        """Send undefined methods straight through to proxied reply
        """
        # send these attributes through to proxy reply 
        if attr in ('operation', 'request', 'url', 'abort', 'close'):#, 'isSequential'):
            value = self.reply.__getattribute__(attr)
        else:
            value = QNetworkReply.__getattribute__(self, attr)
        #print attr, value
        return value
    
    def abort(self):
        pass # qt requires that this be defined
    
    def isSequential(self):
        return True

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

    def bytesAvailable(self):
        """How many bytes in the buffer are available to be read
        """
        return len(self.buffer) + QNetworkReply.bytesAvailable(self)

    def readInternal(self):
        """New data available to read
        """
        s = self.reply.readAll()
        self.data += s
        self.buffer += s
        self.readyRead.emit()

    def readData(self, size):
        """Return up to size bytes from buffer
        """
        size = min(size, len(self.buffer))
        data, self.buffer = self.buffer[:size], self.buffer[size:]
        return str(data)


class WebPage(QWebPage):
    """Override QWebPage to set User-Agent and JavaScript messages

    user_agent: 
        the User Agent to submit
    confirm:
        default response to confirm dialog boxes
    """

    def __init__(self, user_agent, confirm=True):
        #super(WebPage, self).__init__()
        QWebPage.__init__(self)
        self.user_agent = user_agent
        self.confirm = confirm

    def userAgentForUrl(self, url):
        return self.user_agent

    def javaScriptAlert(self, frame, message):
        """Override default JavaScript alert popup and print results
        """
        common.logger.debug('Alert:' + message)

    def javaScriptConfirm(self, frame, message):
        """Override default JavaScript confirm popup and print results
        """
        common.logger.debug('Confirm:' + message)
        return self.confirm

    def javaScriptPrompt(self, frame, message, default):
        """Override default JavaScript prompt popup and print results
        """
        common.logger.debug('Prompt:%s%s' % (message, default))

    def javaScriptConsoleMessage(self, message, line_number, source_id):
        """Print JavaScript console messages
        """
        common.logger.debug('Console:%s%s%s' % (message, line_number, source_id))

    def shouldInterruptJavaScript(self):
        """Disable javascript interruption dialog box
        """
        return True



class WebkitBrowser(QWebView):
    """Render webpages using webkit

    base_url: 
        the domain that will be crawled
    gui:
        whether to show webkit window or run headless
    user_agent:
        the user-agent when downloading content
    proxy:
        a QNetworkProxy to download through
    load_images:
        whether to download images
    forbidden_extensions
        a list of extensions to prevent downloading
    allowed_regex:
        a regular expressions of URLs to allow
    timeout:
        the maximum amount of seconds to wait for a request
    delay:
        the minimum amount of seconds to wait between requests
    """

    def __init__(self, base_url=None, gui=False, user_agent=None, proxy=None, load_images=False, forbidden_extensions=None, allowed_regex='.*?', timeout=20, delay=5, enable_plugins=False):
        self.app = QApplication(sys.argv) # must instantiate first
        QWebView.__init__(self)
        manager = NetworkAccessManager(proxy, forbidden_extensions, allowed_regex)
        manager.finished.connect(self.finished)
        webpage = WebPage(user_agent or alg.rand_agent())
        webpage.setNetworkAccessManager(manager)
        self.setPage(webpage)
        self.setHtml('<html><head></head><body>No content loaded</body></html>', QUrl('http://localhost'))
        self.timeout = timeout
        self.delay = delay
        self.base_url = base_url
        self.jquery_lib = None
        # enable flash plugin etc.
        self.settings().setAttribute(QWebSettings.PluginsEnabled, enable_plugins)
        self.settings().setAttribute(QWebSettings.JavaEnabled, enable_plugins)
        self.settings().setAttribute(QWebSettings.AutoLoadImages, load_images)
        self.settings().setAttribute(QWebSettings.DeveloperExtrasEnabled, True)
        if gui: self.show() 
   
    
    def __del__(self):
        # not sure why, but to avoid seg fault need to release the QWebPage manually
        self.setPage(None)

    def set_proxy(self, proxy):
        self.page().networkAccessManager().setProxy(proxy)

    def current_url(self):
        """Return current URL
        """
        return str(self.url().toString())

    def current_html(self):
        """Return current rendered HTML
        """
        return unicode(self.page().mainFrame().toHtml())


    def get(self, url=None, html=None, script=None, num_retries=1, jquery=False):
        """Load given url in webkit and return html when loaded

        url:
            the URL to load
        html: 
            optional HTML to set instead of downloading
        script:
            some javasript to exexute that will change the loaded page (eg form submission)
        num_retries:
            how many times to try downloading this URL or executing this script
        jquery:
            whether to inject JQuery library into the document
        """
        t1 = time()
        self.base_url = self.base_url or url # set base URL if not set
        loop = QEventLoop()
        timer = QTimer()
        timer.setSingleShot(True)
        timer.timeout.connect(loop.quit)
        self.loadFinished.connect(loop.quit)
        if url:
            if html:
                self.setHtml(html, QUrl(url))
            else: 
                self.load(QUrl(url))
        elif script:
            self.js(script)
        timer.start(self.timeout * 1000)
        loop.exec_() # delay here until download finished or timeout
    
        if timer.isActive():
            # downloaded successfully
            timer.stop()
            parsed_html = self.current_html()
            self.wait(self.delay - (time() - t1))
        else:
            # did not download in time
            if num_retries > 0:
                common.logger.debug('Timeout - retrying')
                parsed_html = self.get(url, script=script, num_retries=num_retries-1, jquery=jquery)
            else:
                common.logger.debug('Timed out')
                parsed_html = ''
        return parsed_html


    def wait(self, timeout=1, pattern=None):
        """Wait for delay time
        """
        deadline = time() + timeout
        while time() < deadline:
            sleep(0)
            self.app.processEvents()


    def wait_load(self, pattern, timeout=60):
        """Wait for this content to be loaded up to maximum timeout
        """
        deadline = time() + timeout
        while time() < deadline:
            sleep(0)
            self.app.processEvents()
            if self.find(pattern):
                return
        common.logger.info('Wait load pattern timed out')


    def jsget(self, script, num_retries=1, jquery=True):
        """Execute JavaScript that will cause page submission, and wait for page to load
        """
        return self.get(script=script, num_retries=num_retries, jquery=jquery)

    def js(self, script):
        """Shortcut to execute javascript on current document and return result
        """
        self.app.processEvents()
        return self.page().mainFrame().evaluateJavaScript(script).toString()

    def inject_jquery(self):
        """Inject jquery library into this webpage for easier manipulation
        """
        if self.jquery_lib is None:
            url = 'http://ajax.googleapis.com/ajax/libs/jquery/1/jquery.min.js'
            self.jquery_lib = urllib2.urlopen(url).read()
        self.js(self.jquery_lib)


    def click(self, pattern='input'):
        """Click all elements that match the pattern.

        Uses standard CSS pattern matching: http://www.w3.org/TR/CSS2/selector.html
        """
        for e in self.find(pattern):
            e.evaluateJavaScript("var evObj = document.createEvent('MouseEvents'); evObj.initEvent('click', true, true); this.dispatchEvent(evObj);")

    def attr(self, pattern, name, value=None):
        """Set attribute if value is defined, else get
        """
        if value is None:
            # want to get attribute
            return str(self.page().mainFrame().findFirstElement(pattern).attribute(name))
        else:
            for e in self.find(pattern):
                e.setAttribute(name, value)
           
    def fill(self, pattern, value):
        """Set text of these elements to value
        """
        for e in self.find(pattern):
            tag = str(e.tagName()).lower()
            if tag == 'input':
                e.setAttribute('value', value)
                #e.evaluateJavaScript('this.value = "%s"' % value)
            else:
                e.setPlainText(value)
        
    def find(self, pattern):
        """Returns whether element matching css pattern exists
        Note this uses CSS syntax, not Xpath
        """
        # format xpath to webkit style
        #pattern = re.sub('["\']\]', ']', re.sub('=["\']', '=', pattern.replace('[@', '[')))
        if isinstance(pattern, basestring):
            matches = self.page().mainFrame().findAllElements(pattern).toList()
        elif isinstance(pattern, list):
            matches = pattern
        elif isinstance(pattern, QWebElement):
            matches = [pattern]
        else:
            common.logger.warning('Unknown pattern: ' + str(pattern))
            matches = []
        return matches


    def data(self, url):
        """Get data for this downloaded resource, if exists
        """
        record = self.page().networkAccessManager().cache().data(QUrl(url))
        if record:
            data = record.readAll()
            record.reset()
        else:
            data = None
        return data
    
    
    def run(self):
        """Run the Qt event loop so can interact with the browser
        """
        self.app.exec_() # start GUI thread


    def finished(self, reply):
        """Override this method in subclasses to process downloaded urls
        """
        pass 
        #print reply.url().toString(), ':', len(reply.data)
        

    def screenshot(self, output_file):
        """Take screenshot of current webpage and save results
        """
        frame = self.page().mainFrame()
        self.page().setViewportSize(frame.contentsSize())
        image = QImage(self.page().viewportSize(), QImage.Format_ARGB32)
        painter = QPainter(image)
        frame.render(painter)
        painter.end()
        common.logger.debug('saving', output_file)
        image.save(output_file)


    def closeEvent(self, event):
        """Catch the close window event and stop the script
        """
        sys.exit(self.app.quit())


if __name__ == '__main__':
    # initiate webkit and show gui
    # once script is working you can disable the gui
    w = WebkitBrowser(gui=True) 
    # load webpage
    w.get('http://duckduckgo.com')
    # fill search textbox 
    w.fill('input[id=search_form_input_homepage]', 'web scraping')
    # take screenshot of webpage
    w.screenshot('duckduckgo.jpg')
    # click search button 
    w.click('input[id=search_button_homepage]')
    # show webpage for 10 seconds
    w.wait(10)
