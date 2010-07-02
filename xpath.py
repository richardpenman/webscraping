import re
import urllib2
from optparse import OptionParser

# tags that do not contain content and so can be safely skipped
EMPTY_TAGS = 'br', 'hr', 'meta'



class XPathException(Exception):
    pass


def parse(html, xpath, debug=False, remove=[]):
    """Query HTML document using XPath
    Supports indices, attributes, descendants
    Can handle rough HTML but may miss content if key tags are not closed

    >>> parse('<span>1</span><div>abc<a>LINK 1</a><div><a>LINK 2</a>def</div>abc</div>ghi<div><a>LINK 3</a>jkl</div>', '/div/a')
    ['LINK 1', 'LINK 3']
    >>> parse('<div>abc<a class="link">LINK 1</a><div><a>LINK 2</a>def</div>abc</div>ghi<div><a class="link">LINK 3</a>jkl</div>', '/div[1]/a[@class="link"]')
    ['LINK 1']
    >>> parse('<div>abc<a class="link">LINK 1</a><div><a>LINK 2</a>def</div>abc</div>ghi<div><a class="link">LINK 3</a>jkl</div>', '/div[1]//a')
    ['LINK 1', 'LINK 2']
    >>> parse('<div>abc<a class="link">LINK 1</a></div>', '/div/a/@class')
    ['link']
    """
    orig_html = html
    html = clean_html(html, remove)
    #open('test.html', 'w').write(html)
    contexts = [html] # initial context is entire webpage
    parent_attributes = []
    for tag_i, (separator, tag, index, attribute) in enumerate(xpath_iter(xpath)):
        children = []
        if tag == '..':
            # parent
            raise Exception('.. not yet supported')
        elif tag.startswith('@'):
            # selecting attribute
            for attributes in parent_attributes:
                children.append(attributes.get(tag[1:], ''))
        else:
            # have tag
            parent_attributes = []
            for context in contexts:
                search = separator == '' and find_children or find_descendants
                matches = search(context, tag)
                for child_i, child in enumerate(matches):
                    if index is None or index == child_i + 1 or index == -1 and len(matches) == child_i + 1:
                        # matches index if defined
                        attributes = get_attributes(child)
                        if match_attributes(attribute, attributes):
                            # child matches tag and any defined indices or attributes
                            children.append(get_content(child))
                            parent_attributes.append(attributes)
        if not children and tag == 'tbody':
            pass # skip tbody, which firefox includes in xpath when does not exist
        else:
            contexts = children
        if not contexts:
            if debug:
                print 'No matches for <%s%s%s> (tag %d)' % (tag, '[%d]' % index if index else '', '[@%s="%s"]' % attribute if attribute else '', tag_i + 1)
            break
    return contexts


def clean_html(html, tags):
    """Remove specified unhelpful tags and comments
    """
    html = re.compile('<!--.*?-->', re.DOTALL).sub('', html) # remove comments
    if tags:
        for tag in tags:
            html = re.compile('<' + tag + '[^>]*?/>', re.DOTALL | re.IGNORECASE).sub('', html)
            html = re.compile('<' + tag + '[^>]*?>.*?</' + tag + '>', re.DOTALL | re.IGNORECASE).sub('', html)
            html = re.compile('<' + tag + '[^>]*?>', re.DOTALL | re.IGNORECASE).sub('', html)
    return html


def xpath_iter(xpath):
    """Return an iterator of the xpath parsed into the separator, tag, index, and attribute

    >>> list(xpath_iter('/div[1]//span[@class="text"]'))
    [('', 'div', 1, None), ('/', 'span', None, ('class', 'text'))]
    """
    for separator, token in re.compile('(|/|\.\.)/([^/]+)').findall(xpath):
        index = attribute = None
        if '[' in token:
            tag, selector = token[:-1].split('[')
            try:
                index = int(selector)
            except ValueError:
                match = re.compile('@(.*?)=["\']?(.*?)["\']?$').search(selector)    
                if match:
                    attribute = match.groups()
                else:
                    raise Exception('Unknown format: ' + selector)
        else:
            tag = token
        yield separator, tag, index, attribute


def get_attributes(html):
    """Extract the attributes of the passed HTML tag

    >>> get_attributes('<div id="ID" name="MY NAME" max-width="20" class=abc>content <span>SPAN</span></div>')
    {'max-width': '20', 'class': 'abc', 'id': 'ID', 'name': 'MY NAME'}
    """
    attributes = re.compile('<(.*?)>', re.DOTALL).match(html).groups()[0]
    return dict(
        re.compile('([\w-]+)="(.*?)"', re.DOTALL).findall(attributes) + 
        re.compile("([\w-]+)='(.*?)'", re.DOTALL).findall(attributes) + 
        re.compile("([\w-]+)=(\w+)", re.DOTALL).findall(attributes) # get (illegal) attributes without quotes
    )


def match_attributes(attribute, attributes):
    """Returns True if desired attribute matches one in the set
    Supports regex, which is not part of the XPath standard but is so useful!

    >>> match_attributes(None, {})
    True
    >>> match_attributes(('class', 'test'), {})
    False
    >>> match_attributes(None, {'id':'test', 'class':'test2'})
    True
    >>> match_attributes(('class', 'test'), {'id':'test', 'class':'test2'})
    False
    >>> match_attributes(('class', 'test'), {'id':'test2', 'class':'test'})
    True
    >>> match_attributes(('class', 'test\d'), {'id':'test', 'class':'test2'})
    True
    >>> match_attributes(('class', 'test\d'), {'id':'test2', 'class':'test'})
    False
    """
    if not attribute: return True
    name, value = attribute
    return re.match(value + '$', attributes.get(name, '')) is not None


def get_content(html):
    """Extract the child HTML of a the passed HTML tag

    >>> get_content('<div id="ID" name="NAME">content <span>SPAN</span></div>')
    'content <span>SPAN</span>'
    """
    match = re.compile('<.*?>(.*)</.*?>$', re.DOTALL).match(html)
    return match.groups()[0] if match else ''



def find_children(html, tag):
    """Find children with this tag type

    >>> find_children('<span>1</span><div>abc<div>def</div>abc</div>ghi<div>jkl</div>', 'div')
    ['<div>abc<div>def</div>abc</div>', '<div>jkl</div>']
    """
    results = []
    found = True
    while found:
        html = jump_next_tag(html)
        if html:
            #print 'html:', html[:100] if html else None
            tag_html, html = split_tag(html)
            if tag_html:
                #print 'tag:', get_tag(tag_html)
                if tag.lower() in ('*', get_tag(tag_html).lower()):
                    results.append(tag_html)
            else:
                found = False
        else:
            found = False
    return results


def find_descendants(html, tag):
    """Find descendants with this tag type

    >>> find_descendants('<span>1</span><div>abc<div>def</div>abc</div>ghi<div>jkl</div>', 'div')
    ['<div>abc<div>def</div>abc</div>', '<div>def</div>', '<div>jkl</div>']
    """
    if tag == '*':
        raise XPathException("`*' not currently supported for // because too inefficient")
    results = []
    for match in re.compile('<%s' % tag, re.DOTALL | re.IGNORECASE).finditer(html):
        tag_html, _ = split_tag(html[match.start():])
        results.append(tag_html)
    return results


def jump_next_tag(html):
    """Return html at start of next tag

    >>> jump_next_tag('<div>abc</div>')
    '<div>abc</div>'
    >>> jump_next_tag(' <div>abc</div>')
    '<div>abc</div>'
    >>> jump_next_tag('</span> <div>abc</div>')
    '<div>abc</div>'
    """
    while 1:
        match = re.search('<(\w+)', html)
        if match:
            if match.groups()[0].lower() in EMPTY_TAGS:
                html = html[1:]
            else:
                return html[match.start():]
        else:
            return None


def get_tag(html):
    """Find tag type at this location

    >>> get_tag('<div>abc</div>')
    'div'
    >>> get_tag(' <div>')
    >>> get_tag('div')
    """
    match = re.match('<(\w+)', html)
    if match:
        return match.groups()[0]
    else:
        return None


def split_tag(html):
    """Extract starting tag from HTML

    >>> split_tag('<div>abc<div>def</div>abc</div>ghi<div>jkl</div>')
    ('<div>abc<div>def</div>abc</div>', 'ghi<div>jkl</div>')
    >>> split_tag('<br /><div>abc</div>')
    ('<br />', '<div>abc</div>')
    >>> split_tag('<div>abc<div>def</div>abc</span>')
    ('<div>abc<div>def</div>abc</span></div>', '')
    """
    tag = get_tag(html)
    depth = 0 # how far nested
    for match in re.compile('</?%s.*?>' % tag, re.DOTALL | re.IGNORECASE).finditer(html):
        if html[match.start() + 1] == '/':
            depth -= 1
        elif html[match.end() - 2] == '/':
            pass # tag starts and ends (eg <br />)
        else:
            depth += 1
        if depth == 0:
            # found top level match
            i = match.end()
            return html[:i], html[i:]
    return html + '</%s>' % tag, ''



def main():
    usage = 'usage: %prog [options] xpath1 [xpath2 ...]'
    parser = OptionParser(usage)
    parser.add_option("-f", "--file", dest="filename", help="read html from FILENAME")
    parser.add_option("-s", "--string", dest="string", help="read html from STRING")
    parser.add_option("-u", "--url", dest="url", help="read html from URL")
    parser.add_option("-d", "--doctest", action="store_true", dest="doctest")
    (options, xpaths) = parser.parse_args()

    if options.doctest:
        import doctest
        return doctest.testmod()
    else:
        if len(xpaths) == 0:
            parser.error('Need atleast 1 xpath')

        if options.filename:
            html = open(options.filename).read()
        elif options.string:
            html = options.string
        elif options.url:
            html = urllib2.urlopen(options.url).read()
        
        results = [parse(html, xpath) for xpath in xpaths]
        return results

        
if __name__ == '__main__':
    print main()
