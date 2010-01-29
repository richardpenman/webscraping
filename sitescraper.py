import re
import libxml2dom


class sitescraper(object):
    """A pure python class with the same interface as SiteScraper, but does not support training
    """

    def __init__(self, model):
        self._model = model


    def scrape(self, input, html=False, drop_tags=None):
        """Scrape data from this input using model
        The html flag determines whether to extract the raw HTML instead of parsed text"""

        tree = self.parse(input, drop_tags)
        open('tree.html', 'w').write(libxml2dom.toString(tree))
        results = []
        for xpath in self._model:
            if isinstance(xpath, list):
                results.append([self.extract_content(e, html) for e in tree.xpath(xpath[0])])
            else:
                es = tree.xpath(xpath)
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
        return libxml2dom.parseString(html, html=1)


    def extract_content(self, e, html):
        if isinstance(e, basestring):
            result = e
        elif e is None:
            result = ''
        elif html:
            result = e.toString()
        else:
            result = e.textContent
        return result.strip()
