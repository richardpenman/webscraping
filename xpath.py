__doc__ = """
This module implements a subset of the XPath standard:
- tags
- indices
- attributes
- descendants

This was created because I needed a pure Python XPath parser.

Generally XPath solutions will normalize the HTML into XHTML before selecting nodes.
However this module tries to navigate the HTML structure directly without normalizing by searching for the next closing tag.
"""

#TODO:
# - parent
# - search by text: text() == '...'
# - return xpath for most similar to text
# - multiple filters for a tag

import re
import sys
import urllib
import urllib2
import urlparse
from optparse import OptionParser
import adt
import common
import settings


class Doc:
    """Wrapper around a parsed webpage

    html:
        The content of webpage to parse
    remove:
        A list of tags to remove

    >>> doc = Doc('<div>abc<a class="link">LINK 1</a><div><a>LINK 2</a>def</div>abc</div>ghi<div><a>LINK 3</a>jkl</div>')
    >>> doc.search('/div/a')
    ['LINK 1', 'LINK 3']
    >>> doc.search('/div/a[@class="link"]')
    ['LINK 1']
    >>> doc.search('/div[1]//a')
    ['LINK 1', 'LINK 2']
    >>> doc.search('/div/a/@class')
    ['link', '']
    >>> doc.search('/div[-1]/a')
    ['LINK 3']

    >>> # test searching unicode
    >>> doc = Doc(u'<a href="http://www.google.com" class="flink">google</a>')
    >>> doc.get('//a[@class="flink"]')
    u'google'

    >>> # test finding just the first instance for a large amount of content
    >>> doc = Doc('<div><span>content</span></div>' * 10000)
    >>> doc.get('//span')
    'content'

    >>> # test extracting attribute of self closing tag
    >>> Doc('<div><img src="img.png"></div>').get('/div/img/@src')
    'img.png'

    >>> # test extracting attribute after self closing tag
    >>> Doc('<div><br><p>content</p></br></div>').get('/div/p')
    'content'
    """

    # regex to find a tag
    _tag_regex = re.compile('<([\w\:]+)')
    # regex to find an attribute
    _attributes_regex = re.compile('([\w\:-]+)\s*=\s*(".*?"|\'.*?\'|\S+)', re.DOTALL)
    # regex to find content of a tag
    _content_regex = re.compile('<.*?>(.*)</.*?>$', re.DOTALL)


    def __init__(self, html, remove=None):
        #self.html = self._clean(html, remove)
        self.html = html
        self.num_searches = 0

    def get(self, xpath):
        """Return the first result from this XPath selection
        """
        results = self._xpath(self.parse(xpath), self.html, limit=1)
        return common.first(results)

    def search(self, xpath):
        """Return all results from this XPath selection
        """
        return self._xpath(self.parse(xpath), self.html, limit=sys.maxint)


    def _xpath(self, path, html, limit):
        """Recursively search HTML for content at XPath
        """
        counter, separator, tag, index, attributes = path.pop(0)
        if counter == 0:
            self.num_searches += 1

        results = []
        if tag == '..':
            # parent
            raise common.WebScrapingError('.. not yet supported')
            results.append(self.get_parent(html))
        elif tag == 'text()':
            # extract child text
            text = self._get_content(self._get_html(html))
            results.append(common.remove_tags(text, keep_children=False))
            # check if next tag is selecting attribute
        elif tag.startswith('@'):
            attr = tag[1:].lower()
            #parent = self.get_parent(context)
            value = self._get_attributes(html).get(attr, '')
            results.append(value)
        else:
            # have tag
            if counter > 0:
                # get child html when not at root
                html = self._get_content(html)

            # search direct children if / and all descendants if //
            search_fn = self._find_children if separator == '' else self._find_descendants
            matches = search_fn(html, tag)

            # support negative indices
            if index is not None and index < 0:
                matches = list(matches)
                index += len(matches) + 1

            for child_i, child in enumerate(matches):
                # check if matches index
                if index is None or index == child_i + 1:
                    # check if matches attributes
                    if not attributes or self._match_attributes(attributes, self._get_attributes(child)):
                        if path:
                            results.extend(self._xpath(path[:], child, limit))
                        else:
                            # final node
                            results.append(self._get_content(child))
                        if len(results) > limit:
                            break

            #if not children:
            #    attributes_s = attributes and ''.join('[@%s="%s"]' % a for a in attributes) or ''
            #    common.logger.debug('No matches for <%s%s%s> (tag %d)' % (tag, index and '[%d]' % index or '', attributes_s, tag_i + 1))
        return results



    def _clean(self, html, remove):
        """Remove specified unhelpful tags and comments
        """
        self.remove = remove
        html = re.compile('<!--.*?-->', re.DOTALL).sub('', html) # remove comments
        if remove:
            # XXX combine tag list into single regex, if can match same at start and end
            for tag in remove:
                html = re.compile('<' + tag + '[^>]*?/>', re.DOTALL | re.IGNORECASE).sub('', html)
                html = re.compile('<' + tag + '[^>]*?>.*?</' + tag + '>', re.DOTALL | re.IGNORECASE).sub('', html)
                html = re.compile('<' + tag + '[^>]*?>', re.DOTALL | re.IGNORECASE).sub('', html)
        return html


    def parse(self, xpath):
        """Parse the xpath into: counter, separator, tag, index, and attributes

        >>> doc = Doc('')
        >>> doc.parse('/div[1]//span[@class="text"]')
        [(0, '', 'div', 1, []), (1, '/', 'span', None, [('class', 'text')])]
        >>> doc.parse('//li[-2]')
        [(0, '/', 'li', -2, [])]
        >>> doc.parse('//option[@selected]')
        [(0, '/', 'option', None, [('selected', None)])]
        >>> doc.parse('/div[@id="content"]//span[1][@class="text"][@title=""]/a')
        [(0, '', 'div', None, [('id', 'content')]), (1, '/', 'span', 1, [('class', 'text'), ('title', '')]), (2, '', 'a', None, [])]
        """
        tokens = []
        counter = 0
        for separator, token in re.compile('(|/|\.\.)/([^/]+)').findall(xpath):
            index, attributes = None, []
            if '[' in token:
                tag = token[:token.find('[')]
                for attribute in re.compile('\[(.*?)\]').findall(token):
                    try:
                        index = int(attribute)
                    except ValueError:
                        match = re.compile('@(.*?)=["\']?(.*?)["\']?$').search(attribute)
                        if match:
                            key, value = match.groups()
                            attributes.append((key.lower(), value.lower()))
                        else:
                            match = re.compile('@(.*?)$').search(attribute)
                            if match:
                                attributes.append((match.groups()[0].lower(), None))
                            else:
                                raise common.WebScrapingError('Unknown format: ' + attribute)
            else:
                tag = token
            tokens.append((counter, separator, tag, index, attributes))
            counter += 1
        return tokens


    def _get_attributes(self, html):
        """Extract the attributes of the passed HTML tag

        >>> doc = Doc('')
        >>> doc._get_attributes('<div id="ID" name="MY NAME" max-width="20" class=abc>content <span class="inner name">SPAN</span></div>')
        {'max-width': '20', 'class': 'abc', 'id': 'ID', 'name': 'MY NAME'}
        >>> doc._get_attributes('<td width=200 valign=top class=textelien>')
        {'width': '200', 'class': 'textelien', 'valign': 'top'}
        >>> doc._get_attributes('<option value="1" selected>')
        {'selected': None, 'value': '1'}
        """

        for i, c in enumerate(html):
            if c == '>':
                html = html[:i]
                break
        attributes = dict((name.lower().strip(), value.strip('\'" ')) for (name, value) in Doc._attributes_regex.findall(html))
        #for attribute in ('checked', 'selected', 'required', 'multiple', 'disabled'):
        for attribute in re.findall('\s+(checked|selected|required|multiple|disabled)', html):
            attributes[attribute] = None
        return attributes


    def _match_attributes(self, desired_attributes, available_attributes):
        """Returns True if all of desired attributes are in available attributes
        Supports regex, which is not part of the XPath standard but is so useful!

        >>> doc = Doc('')
        >>> doc._match_attributes([], {})
        True
        >>> doc._match_attributes([('class', 'test')], {})
        False
        >>> doc._match_attributes([], {'id':'test', 'class':'test2'})
        True
        >>> doc._match_attributes([('class', 'test')], {'id':'test', 'class':'test2'})
        False
        >>> doc._match_attributes([('class', 'test')], {'id':'test2', 'class':'test'})
        True
        >>> doc._match_attributes([('class', 'test'), ('id', 'content')], {'id':'test', 'class':'content'})
        False
        >>> doc._match_attributes([('class', 'test'), ('id', 'content')], {'id':'content', 'class':'test'})
        True
        >>> doc._match_attributes([('class', 'test\d')], {'id':'test', 'class':'test2'})
        True
        >>> doc._match_attributes([('class', 'test\d')], {'id':'test2', 'class':'test'})
        False
        >>> doc._match_attributes([('selected', None)], {'selected':None, 'class':'test'})
        True
        >>> doc._match_attributes([('selected', None)], {'class':'test'})
        False
        >>> doc._match_attributes([('class', 'test')], {'selected':None, 'class':'test'})
        True
        """
        for name, value in desired_attributes:
            if name in available_attributes:
                available_value = available_attributes[name]
                if value != available_value:
                    if value is None or not re.match(re.compile(value + '$', re.IGNORECASE), available_attributes[name]):
                        return False
            else:
                return False
        return True


    def _get_html(self, context):
        """Return HTML at this context
        """
        return context
        if context:
            i, j = context
        else:
            i, j = 0, len(self.html)
        return self.html[i:j]


    def _get_content(self, context, default=''):
        """Extract the child HTML of a the passed HTML tag

        >>> doc = Doc('')
        >>> doc._get_content('<div id="ID" name="NAME">content <span>SPAN</span></div>')
        'content <span>SPAN</span>'
        """
        match = Doc._content_regex.match(self._get_html(context))
        if match:
            content = match.groups()[0]
        else:
            content = default
        return content


    def _find_children(self, html, tag):
        """Find children with this tag type

        >>> doc = Doc('')
        >>> list(doc._find_children('<span>1</span><div>abc<div>def</div>abc</div>ghi<div>jkl</div>', 'div'))
        ['<div>abc<div>def</div>abc</div>', '<div>jkl</div>']
        >>> list(doc._find_children('<tbody><tr><td></td></tr></tbody>', 'tbody'))
        ['<tbody><tr><td></td></tr></tbody>']
        >>> list(doc._find_children('<tr><td></td></tr>', 'tbody'))
        ['<tr><td></td></tr>']
        """
        found = True
        num_found = 0
        orig_html = html
        while found:
            html = self._jump_next_tag(html)
            if html:
                tag_html, html = self._split_tag(html)
                if tag_html:
                    if tag.lower() in ('*', self._get_tag(tag_html).lower()):
                        num_found += 1
                        yield tag_html
                else:
                    found = False
            else:
                found = False

        if tag == 'tbody' and num_found == 0:
            # skip tbody, which firefox includes in xpath when does not exist
            yield orig_html


    def _find_descendants(self, html, tag):
        """Find descendants with this tag type

        >>> doc = Doc('')
        >>> list(doc._find_descendants('<span>1</span><div>abc<div>def</div>abc</div>ghi<div>jkl</div>', 'div'))
        ['<div>abc<div>def</div>abc</div>', '<div>def</div>', '<div>jkl</div>']
        """
        # XXX search with attribute here
        if tag == '*':
            raise common.WebScrapingError("`*' not currently supported for //")
        for match in re.compile('<%s' % tag, re.DOTALL | re.IGNORECASE).finditer(html):
            tag_html = html[match.start():]
            tag_html, _ = self._split_tag(tag_html)
            yield tag_html


    def _jump_next_tag(self, html):
        """Return html at start of next tag

        >>> doc = Doc('')
        >>> doc._jump_next_tag('<div>abc</div>')
        '<div>abc</div>'
        >>> doc._jump_next_tag(' <div>abc</div>')
        '<div>abc</div>'
        >>> doc._jump_next_tag('</span> <div>abc</div>')
        '<div>abc</div>'
        >>> doc._jump_next_tag(' <br> <div>abc</div>')
        '<br> <div>abc</div>'
        """
        while 1:
            match = Doc._tag_regex.search(html)
            if match:
                return html[match.start():]
            else:
                return None


    def _get_tag(self, html):
        """Find tag type at this location

        >>> doc = Doc('')
        >>> doc._get_tag('<div>abc</div>')
        'div'
        >>> doc._get_tag(' <div>')
        >>> doc._get_tag('div')
        """
        match = Doc._tag_regex.match(html)
        if match:
            return match.groups()[0]
        else:
            return None


    def _split_tag(self, html):
        """Extract starting tag and contents from HTML

        >>> doc = Doc('')
        >>> doc._split_tag('<div>abc<div>def</div>abc</div>ghi<div>jkl</div>')
        ('<div>abc<div>def</div>abc</div>', 'ghi<div>jkl</div>')
        >>> doc._split_tag('<br /><div>abc</div>')
        ('<br />', '<div>abc</div>')
        >>> doc._split_tag('<div>abc<div>def</div>abc</span>')
        ('<div>abc<div>def</div>abc</span></div>', '')
        >>> # test efficiency of splits
        >>> a = [doc._split_tag('<div>abc<div>def</div>abc</span>') for i in range(10000)]
        """
        i = None
        tag = self._get_tag(html)
        depth = 0 # how far nested
        for match in re.compile('</?%s.*?>' % tag, re.DOTALL | re.IGNORECASE).finditer(html):
            if html[match.start() + 1] == '/':
                depth -= 1 # found closing tag
            elif tag in common.EMPTY_TAGS:
                pass # this tag type does not close
            elif html[match.end() - 2] == '/':
                pass # tag starts and ends (eg <br />)
            else:
                depth += 1 # found opening tag
            if depth == 0:
                # found top level match
                i = match.end()
                break
        if i is None:
            # all html is within this tag
            return html + '</%s>' % tag, ''
        else:
            return html[:i], html[i:]


    def _parent_tag(self, html):
        """Find parent tag of this current tag

        >> doc = Doc('<p><div><span id="abc">empty</span></div></p>')
        >> doc._parent_tag('<span id="abc">empty</span>')
        '<div><span id="abc">empty</span></div>'
        >> doc = Doc('<div><p></p><span id="abc">empty</span></div>')
        >> doc._parent_tag('<span id="abc">empty</span>')
        '<div><p></p><span id="abc">empty</span></div>'
        """
        raise Exception('Not implemented')
        #index = self.html.find(html)
        #while index >= 0:
        #    index = self.html.rfind('<', start=0, end=index)


try:
    import lxml.html
except ImportError:
    class Tree:
        def __init__(*args, **kwargs):
            raise ImportError('lxml not installed')
else:
    # if lxml is supported create wrapper
    class Tree:
        def __init__(self, html, **kwargs):
            if isinstance(html, lxml.html.HtmlElement):
                # input is already a passed lxml tree
                self.doc = html
            else:
                try:
                    self.doc = lxml.html.fromstring(html)
                except lxml.etree.XMLSyntaxError:
                    self.doc = None

        def __eq__(self, html):
            return self.orig_html is html


        def xpath(self, path):
            return [] if self.doc is None else self.doc.xpath(path)

        def get(self, path):
            es = self.xpath(path)
            if es:
                return self.tostring(es[0])
            return ''

        def search(self, path):
            return [self.tostring(e) for e in self.xpath(path)]

        def tostring(self, node):
            try:
                return ''.join(filter(None, 
                    [node.text] + [lxml.html.tostring(e) for e in node]
                ))
            except AttributeError:
                return node


def get(html, xpath, remove=None):
    """Return first element from XPath search of HTML
    """
    return Doc(html, remove=remove).get(xpath)

def search(html, xpath, remove=None):
    """Return all elements from XPath search of HTML
    """
    return Doc(html, remove=remove).search(xpath)

def find_children(html, tag, remove=None):
    """Find children with this tag type
    """
    return Doc(html, remove=remove)._find_children(html, tag)



class Form:
    """Helper class for filling and submitting forms
    """
    def __init__(self, form):
        self.data = {}
        for input_name, input_value in zip(search(form, '//input/@name'), search(form, '//input/@value')):
            self.data[input_name] = input_value
        for text_name, text_value in zip(search(form, '//textarea/@name'), search(form, '//textarea')):
            self.data[text_name] = text_value
        for select_name, select_contents in zip(search(form, '//select/@name'), search(form, '//select')):
            self.data[select_name] = get(select_contents, '/option[@selected]/@value')
        if '' in self.data:
            del self.data['']


    def __getitem__(self, key):
        return self.data[key]

    def __setitem__(self, key, value):
        self.data[key] = value

    def __str__(self):
        return urllib.urlencode(self.data)

    def submit(self, D, action, **argv):
        return D.get(url=action, data=self.data, **argv)



js_re = re.compile('location.href ?= ?[\'"](.*?)[\'"]')
def get_links(html, url=None, local=True, external=True):
    """Return all links from html and convert relative to absolute if source url is provided

    html:
        HTML to parse
    url:
        optional URL for determining path of relative links
    local:
        whether to include links from same domain
    external:
        whether to include linkes from other domains
    """
    def normalize_link(link):
        if urlparse.urlsplit(link).scheme in ('http', 'https', ''):
            if '#' in link:
                link = link[:link.index('#')]
            if url:
                link = urlparse.urljoin(url, link)
                if not local and common.same_domain(url, link):
                    # local links not included
                    link = None
                if not external and not common.same_domain(url, link):
                    # external links not included
                    link = None
        else:
            link = None # ignore mailto, etc
        return link
    a_links = search(html, '//a/@href')
    i_links = search(html, '//iframe/@src')
    js_links = js_re.findall(html)
    links = []
    for link in a_links + i_links + js_links:
        try:
            link = normalize_link(link)
        except UnicodeError:
            pass
        else:
            if link and link not in links:
                links.append(link)
    return links
