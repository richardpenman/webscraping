# 
# element tree loader based on BeautifulSoup
#
# http://www.crummy.com/software/BeautifulSoup/
#
import re
from htmlentitydefs import name2codepoint

import BeautifulSoup as BS
import elementtree.ElementTree as ET


# soup classes that are left out of the tree
ignorable_soup = BS.Comment, BS.Declaration, BS.ProcessingInstruction


pattern = re.compile("&(\w+);")
def unescape(string):
    # work around oddities in BeautifulSoup's entity handling
    def unescape_entity(m):
        try:
            return unichr(name2codepoint[m.group(1)])
        except KeyError:
            return m.group(0) # use as is
    return pattern.sub(unescape_entity, string)

##
# Loads an XHTML or HTML file into an Element structure, using Leonard
# Richardson's tolerant BeautifulSoup parser.
#
# @param file Source file (either a file object or a file name).
# @param builder Optional tree builder.  If omitted, defaults to the
#     "best" available <b>TreeBuilder</b> implementation.
# @return An Element instance representing the HTML root element.

def parse(input, builder=None, encoding=None):
    html = input.read() if hasattr(input, 'read') else input
    bob = builder
    def emit(soup):
        if isinstance(soup, BS.NavigableString):
            if isinstance(soup, ignorable_soup):
                return
            bob.data(unescape(soup))
        else:
            attrib = dict([(k, unescape(v)) for k, v in soup.attrs if v])
            bob.start(soup.name, attrib)
            for s in soup:
                emit(s)
            bob.end(soup.name)
    
    if not encoding:
        try:
            encoding = "utf-8"
            unicode(html, encoding)
        except UnicodeError:
            encoding = "iso-8859-1"
    soup = BS.BeautifulSoup(html, convertEntities="html", fromEncoding=encoding)
    # build the tree
    if not bob:
        bob = ET.TreeBuilder()
    emit(soup)
    root = bob.close()
    # wrap the document in a html root element, if necessary
    if len(root) == 1 and root[0].tag == "html":
        return root[0]
    root.tag = "html"
    return root

if __name__ == "__main__":
    import sys
    source = sys.argv[1]
    if source.startswith("http:"):
        import urllib
        source = urllib.urlopen(source)
    print ET.tostring(parse(source))
