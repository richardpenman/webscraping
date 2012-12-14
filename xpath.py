__doc__ = """
This module implements a subset of the XPath standard:
 - tags
 - indices
 - attributes
 - descendants
Plus a few extensions useful to my work:
 - attributes can contain regular expressions
 - indices can be negative

Generally XPath solutions will normalize the HTML into XHTML before selecting nodes.
However this module tries to navigate the HTML structure directly without normalizing.
In some cases I have found this faster/more accurate than using lxml.html and in other cases less so.
"""

#TODO:
# - parent
# - search by text: text() == '...'
# - return xpath for most similar to text

import re
import sys
import urllib2
from urlparse import urljoin, urlsplit
from optparse import OptionParser
try:
    from lxml import html as lxmlhtml
except ImportError:
    lxmlhtml = None
import adt
import common
import settings



class Doc:
    """
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

    # test searching unicode
    >>> doc = Doc(u'<a href="http://www.google.com" class="flink">google</a>')
    >>> doc.get('//a[@class="flink"]')
    u'google'

    # test finding just the first instance for a large amount of content
    >>> doc = Doc('<div><span>content</span></div>' * 10000)
    >>> doc.get('//span')
    'content'

    # test extracting attribute of self closing tag
    >>> Doc('<div><img src="img.png"></div>').get('/div/img/@src')
    'img.png'

    # test extracting attribute after self closing tag
    >>> Doc('<div><br><p>content</p></div>').get('/div/p')
    'content'
    """

    # regex to find a tag
    tag_regex = re.compile('<([\w\:]+)')
    # regex to find an attribute
    attributes_regex = re.compile('([\w\:-]+)\s*=\s*(".*?"|\'.*?\'|\S+)', re.DOTALL)
    # regex to find content of a tag
    content_regex = re.compile('<.*?>(.*)</.*?>$', re.DOTALL)


    def __init__(self, html, remove=None):
        self.orig_html = html
        self.html = self.clean(remove)
        self.splits = adt.HashDict()
        self.num_searches = 0

    def __eq__(self, html):
        return self.orig_html is html


    def get(self, xpath):
        results = self.xpath(self.parse(xpath), self.html, limit=1)
        return common.first(results)

    def search(self, xpath):
        return self.xpath(self.parse(xpath), self.html, limit=sys.maxint)


    def xpath(self, path, html, limit):
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
            text = self.get_content(self.get_html(html))
            results.append(common.remove_tags(text, keep_children=False))
            # check if next tag is selecting attribute
        elif tag.startswith('@'):
            attr = tag[1:].lower()
            #parent = self.get_parent(context)
            value = self.get_attributes(html).get(attr, '')
            results.append(value)
        else:
            # have tag
            if counter > 0:
                # get child html when not at root
                html = self.get_content(html)

            # search direct children if / and all descendants if //
            search_fn = self.find_children if separator == '' else self.find_descendants
            matches = search_fn(html, tag)

            # support negative indices
            if index is not None and index < 0:
                matches = list(matches)
                index += len(matches) + 1

            for child_i, child in enumerate(matches):
                # check if matches index
                if index is None or index == child_i + 1:
                    # check if matches attributes
                    if not attributes or self.match_attributes(attributes, self.get_attributes(child)):
                        if path:
                            results.extend(self.xpath(path[:], child, limit))
                        else:
                            # final node
                            results.append(self.get_content(child))
                        if len(results) > limit:
                            break

            #if not children:
            #    attributes_s = attributes and ''.join('[@%s="%s"]' % a for a in attributes) or ''
            #    common.logger.debug('No matches for <%s%s%s> (tag %d)' % (tag, index and '[%d]' % index or '', attributes_s, tag_i + 1))
        return results



    def clean(self, remove):
        """Remove specified unhelpful tags and comments
        """
        self.remove = remove
        html = re.compile('<!--.*?-->', re.DOTALL).sub('', self.orig_html) # remove comments
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
                            raise common.WebScrapingError('Unknown format: ' + attribute)
            else:
                tag = token
            tokens.append((counter, separator, tag, index, attributes))
            counter += 1
        return tokens


    def get_attributes(self, html):
        """Extract the attributes of the passed HTML tag

        >>> doc = Doc('')
        >>> doc.get_attributes('<div id="ID" name="MY NAME" max-width="20" class=abc>content <span class="inner name">SPAN</span></div>')
        {'max-width': '20', 'class': 'abc', 'id': 'ID', 'name': 'MY NAME'}
        """

        for i, c in enumerate(html):
            if c == '>':
                html = html[:i]
                break
        return dict((name.lower().strip(), value.strip('\'" ')) for (name, value) in Doc.attributes_regex.findall(html))


    def match_attributes(self, desired_attributes, available_attributes):
        """Returns True if all of desired attributes are in available attributes
        Supports regex, which is not part of the XPath standard but is so useful!

        >>> doc = Doc('')
        >>> doc.match_attributes([], {})
        True
        >>> doc.match_attributes([('class', 'test')], {})
        False
        >>> doc.match_attributes([], {'id':'test', 'class':'test2'})
        True
        >>> doc.match_attributes([('class', 'test')], {'id':'test', 'class':'test2'})
        False
        >>> doc.match_attributes([('class', 'test')], {'id':'test2', 'class':'test'})
        True
        >>> doc.match_attributes([('class', 'test'), ('id', 'content')], {'id':'test', 'class':'content'})
        False
        >>> doc.match_attributes([('class', 'test'), ('id', 'content')], {'id':'content', 'class':'test'})
        True
        >>> doc.match_attributes([('class', 'test\d')], {'id':'test', 'class':'test2'})
        True
        >>> doc.match_attributes([('class', 'test\d')], {'id':'test2', 'class':'test'})
        False
        """
        for name, value in desired_attributes:
            if name not in available_attributes or not re.match(re.compile(value + '$', re.IGNORECASE), available_attributes[name]):
                return False
        return True


    def get_html(self, context):
        """Return HTML at this context
        """
        return context
        if context:
            i, j = context
        else:
            i, j = 0, len(self.html)
        return self.html[i:j]


    def get_content(self, context, default=''):
        """Extract the child HTML of a the passed HTML tag

        >>> doc = Doc('')
        >>> doc.get_content('<div id="ID" name="NAME">content <span>SPAN</span></div>')
        'content <span>SPAN</span>'
        """
        match = Doc.content_regex.match(self.get_html(context))
        if match:
            content = match.groups()[0]
        else:
            content = default
        return content


    def find_children(self, html, tag):
        """Find children with this tag type

        >>> doc = Doc('')
        >>> list(doc.find_children('<span>1</span><div>abc<div>def</div>abc</div>ghi<div>jkl</div>', 'div'))
        ['<div>abc<div>def</div>abc</div>', '<div>jkl</div>']
        >>> list(doc.find_children('<tbody><tr><td></td></tr></tbody>', 'tbody'))
        ['<tbody><tr><td></td></tr></tbody>']
        >>> list(doc.find_children('<tr><td></td></tr>', 'tbody'))
        ['<tr><td></td></tr>']
        """
        found = True
        num_found = 0
        orig_html = html
        while found:
            html = self.jump_next_tag(html)
            if html:
                tag_html, html = self.split_tag(html)
                if tag_html:
                    if tag.lower() in ('*', self.get_tag(tag_html).lower()):
                        num_found += 1
                        yield tag_html
                else:
                    found = False
            else:
                found = False

        if tag == 'tbody' and num_found == 0:
            # skip tbody, which firefox includes in xpath when does not exist
            yield orig_html


    def find_descendants(self, html, tag):
        """Find descendants with this tag type

        >>> doc = Doc('')
        >>> list(doc.find_descendants('<span>1</span><div>abc<div>def</div>abc</div>ghi<div>jkl</div>', 'div'))
        ['<div>abc<div>def</div>abc</div>', '<div>def</div>', '<div>jkl</div>']
        """
        # XXX search with attribute here
        if tag == '*':
            raise common.WebScrapingError("`*' not currently supported for //")
        for match in re.compile('<%s' % tag, re.DOTALL | re.IGNORECASE).finditer(html):
            tag_html = html[match.start():]
            tag_html, _ = self.split_tag(tag_html)
            yield tag_html


    def jump_next_tag(self, html):
        """Return html at start of next tag

        >>> doc = Doc('')
        >>> doc.jump_next_tag('<div>abc</div>')
        '<div>abc</div>'
        >>> doc.jump_next_tag(' <div>abc</div>')
        '<div>abc</div>'
        >>> doc.jump_next_tag('</span> <div>abc</div>')
        '<div>abc</div>'
        >>> doc.jump_next_tag(' <br> <div>abc</div>')
        '<br> <div>abc</div>'
        """
        while 1:
            match = Doc.tag_regex.search(html)
            if match:
                return html[match.start():]
            else:
                return None


    def get_tag(self, html):
        """Find tag type at this location

        >>> doc = Doc('')
        >>> doc.get_tag('<div>abc</div>')
        'div'
        >>> doc.get_tag(' <div>')
        >>> doc.get_tag('div')
        """
        match = Doc.tag_regex.match(html)
        if match:
            return match.groups()[0]
        else:
            return None


    def split_tag(self, html):
        """Extract starting tag and contents from HTML

        >>> doc = Doc('')
        >>> doc.split_tag('<div>abc<div>def</div>abc</div>ghi<div>jkl</div>')
        ('<div>abc<div>def</div>abc</div>', 'ghi<div>jkl</div>')
        >>> doc.split_tag('<br /><div>abc</div>')
        ('<br />', '<div>abc</div>')
        >>> doc.split_tag('<div>abc<div>def</div>abc</span>')
        ('<div>abc<div>def</div>abc</span></div>', '')
        >>> # test efficiency of splits
        >>> a = [doc.split_tag('<div>abc<div>def</div>abc</span>') for i in range(10000)]
        """
        if html in self.splits:
            i, tag = self.splits[html]
        else:
            i = None
            tag = self.get_tag(html)
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
            self.splits[html] = i, tag
        if i is None:
            # all html is within this tag
            return html + '</%s>' % tag, ''
        else:
            return html[:i], html[i:]


    def parent_tag(self, html):
        """Find parent tag of this current tag

        >> doc = Doc('<p><div><span id="abc">empty</span></div></p>')
        >> doc.parent_tag('<span id="abc">empty</span>')
        '<div><span id="abc">empty</span></div>'
        >> doc = Doc('<div><p></p><span id="abc">empty</span></div>')
        >> doc.parent_tag('<span id="abc">empty</span>')
        '<div><p></p><span id="abc">empty</span></div>'
        """
        raise Exception('Not implemented')
        #index = self.html.find(html)
        #while index >= 0:
        #    index = self.html.rfind('<', start=0, end=index)


prev_doc = None
def get_doc(html, remove):
    global prev_doc
    if prev_doc == html and prev_doc.remove == remove:
        pass # can reuse current doc
    else:
        prev_doc = Doc(html, remove)
    return prev_doc
    
def get(html, xpath, remove=('br', 'hr')):
    """Return first element from search

    >>> html = '<div>1</div><div>2</div>'
    >>> get(html, '/div', None)
    '1'
    >>> search(html, '//div', None)
    ['1', '2']
    >>> get_doc(html, None).num_searches
    2
    """
    return get_doc(html, remove).get(xpath)

def search(html, xpath, remove=('br', 'hr')):
    """Return all elements from search
    """
    return get_doc(html, remove).search(xpath)



js_re = re.compile('location.href ?= ?[\'"](.*?)[\'"]')
def get_links(html, url=None, local=True, external=True):
    """Return all links from html and convert relative to absolute if source url is provided

    local determines whether to include links from same domain
    external determines whether to include linkes from other domains
    """
    def normalize_link(link):
        if urlsplit(link).scheme in ('http', 'https', ''):
            if '#' in link:
                link = link[:link.index('#')]
            if url:
                link = urljoin(url, link)
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
    js_links = js_re.findall(html)
    links = []
    for link in a_links + js_links:
        try:
            link = normalize_link(link)
        except UnicodeError:
            pass
        else:
            if link and link not in links:
                links.append(link)
    return links
