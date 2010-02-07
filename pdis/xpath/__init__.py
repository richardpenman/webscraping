#
# pdis.xpath (__init__.py)
#
# Copyright 2004 Helsinki Institute for Information Technology (HIIT)
# and the authors.  All rights reserved.
#
# Authors: Ken Rimey <rimey@hiit.fi>
#

# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation files
# (the "Software"), to deal in the Software without restriction,
# including without limitation the rights to use, copy, modify, merge,
# publish, distribute, sublicense, and/or sell copies of the Software,
# and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
# CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
# TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

"""
Package-level definitions for the xpath package

Most applications need only import this file with "import pdis.xpath"
or "from pdis.xpath import ...".
"""

from pdis.xpath.ET import XML
from pdis.xpath.parser import parse_xpath
from pdis.xpath.context import Context
from pdis.xpath.data_model import is_node_set
from pdis.xpath.xpath_exceptions import *

def evaluate(xpath, document):
    element = XML(document)
    return compile(xpath).evaluate(element)

_cache = {}

def compile(xpath, namespace_mapping = None):
    # We only cache when there is no namespace mapping.  Hopefully
    # that is the common case.
    if namespace_mapping:
        return XPath(xpath, namespace_mapping)

    p = _cache.get(xpath)
    if p is None:
        p = XPath(xpath)
        if len(_cache) >= 100:
            _cache.popitem()
        _cache[xpath] = p
    return p

class XPath:
    """
    Preparsed xpath expression
    """
    def __init__(self, xpath, namespace_mapping = None):
        """
        Initialize an XPath instance.

        As a special case, if the xpath expression is the empty string,
        the evaluate() method will always return a true value.
        """
        self.xpath = xpath
        self.namespace_mapping = namespace_mapping
        if not xpath:
            self.parsed_xpath = parse_xpath("1")
        else:
            self.parsed_xpath = parse_xpath(xpath)

    def evaluate(self, element):
        context = Context(element, self.namespace_mapping)
        result = self.parsed_xpath.evaluate(context)
        if is_node_set(result) and not isinstance(result, list):
            # Node sets can be all sorts of things internally, but
            # let's normalize them to lists at this point.
            result = list(result)
        return result
