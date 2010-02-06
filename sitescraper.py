import re
from html5lib import HTMLParser, treebuilders
#from elementtree import ElementTree
try:
    from xml.etree import cElementTree as ET
except ImportError:
    from xml.etree import ElementTree as ET
from pdis.xpath import compile



class sitescraper(object):
    """A pure python class with the same interface as SiteScraper, but does not support training
    """

    def __init__(self, model):
        self._model = model


    def scrape(self, input, html=False, drop_tags=None):
        """Scrape data from this input using model
        The html flag determines whether to extract the raw HTML instead of parsed text
        """
        tree = self.parse(input, drop_tags)
        #open('tree.html', 'w').write(self.html_content(tree))
        results = []
        for xpath in self._model:
            if isinstance(xpath, list):
                results.append([self.extract_content(e, html) for e in compile(xpath[0]).evaluate(tree)])
            else:
                es = compile(xpath).evaluate(tree)
                results.append(self.extract_content(es[0], html) if es else None)
        return results


    def parse(self, input, drop_tags):
        """Parse HTML from potentially bad formed document, and remove passed tags
        """
        html = input.read() if hasattr(input, 'read') else input
        if drop_tags:
            for tag in drop_tags:
                html = re.compile('<' + tag + '[^>]*?/>', re.DOTALL).sub('', html)
                html = re.compile('<' + tag + '[^>]*?>.*?</' + tag + '>', re.DOTALL).sub('', html)
        return HTMLParser(tree=treebuilders.getTreeBuilder("etree", ET)).parse(html)


    def extract_content(self, e, html):
        if isinstance(e, basestring):
            result = e
        elif e is None:
            result = ''
        elif html:
            result = self.html_content(e)
        else:
            result = self.text_content(e)
        return result.strip()

    def text_content(self, e):
        """Recursively find text content
        """
        get_text = lambda t: t if t else ''
        return get_text(e.text) + ''.join(self.text_content(c) for c in e) + get_text(e.tail)

    def html_content(self, e):
        """Extract HTML under this element
        """
        return (e.text if e.text else '') + \
               ''.join([ET.tostring(c) for c in e.getchildren()]) + \
               (e.tail if e.tail else '')
