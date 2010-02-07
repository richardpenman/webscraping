#
# pdis.xpath.matcher
#
# Copyright 2006 Helsinki Institute for Information Technology (HIIT)
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
XPath Template Matching

This module enables matching of a pattern XPath containing variables
against a target XPath to determine a set of variable bindings.

For example:

    >>> target = '/washington/seattle/person[@name="bill" and @born=1955]'
    >>> pattern = '$prefix/person[@name=$name and @born=$year]'
    >>> assert match_xpath(pattern, target) == {
    ...     'prefix': '/washington/seattle',
    ...     'name': '"bill"',
    ...     'year': '1955'}
"""

# XXX Haven't thought much about unicode issues.

from pdis.xpath.atoms import *
from pdis.xpath.syntax import *
from pdis.xpath.parser import parse_xpath

def match_xpath(pattern, target):
    """
    Match the pattern against the target.

    This returns a dictionary of variable bindings if the match
    succeeds, and None otherwise.
    """
    def match(p, t):
        if isinstance(p, VariableReference):
            k = str(p)[1:]      # Skip leading '$'.
            v = str(t)
            if k in d:
                return v == d[k]
            else:
                d[k] = v
                return True

        if p.__class__ is not t.__class__:
            return False

        if p is None:           # Turns up in LocationStep prefix.
            return True
        elif isinstance(p, NodeType):
            return p.name == t.name
        elif isinstance(p, (NameTest, FunctionName)):
            return p.prefix == t.prefix and p.local_part == t.local_part
        elif isinstance(p, (Literal, Number)):
            return p.value == t.value
        elif isinstance(p, UnaryOp):
            return p.op == t.op and match(p.right, t.right)
        elif isinstance(p, BinaryOp):
            return p.op == t.op \
                and match(p.left, t.left) \
                and match(p.right, t.right)
        elif isinstance(p, FunctionCall):
            return match(p.function, t.function) \
                and match(p.argument_list, t.argument_list)
        elif isinstance(p, Root):
            return True
        elif isinstance(p, LocationStep):
            return p.axis == t.axis \
                and match(p.prefix, t.prefix) \
                and match(p.node_test, t.node_test) \
                and match(p.predicate_list, t.predicate_list)
        elif isinstance(p, list):
            if len(p) != len(t):
                return False
            for pp, tt in zip(p, t):
                if not match(pp, tt):
                    return False
            return True
        else:
            raise ValueError('Unexpected node type in XPath syntax tree.')

    d = {}
    if match(parse_xpath(pattern), parse_xpath(target)):
        return d
    else:
        return None

__test__ = {'more tests': """
    >>> path = '/foo[@a=1 and @b=2 and @c=2]'
    >>> assert match_xpath('/foo[@a=$x and @b=$x and @c=$y]', path) is None
    >>> assert match_xpath('/foo[@a=$x and @b=$y and @c=$y]', path) == {
    ...     'x': '1',
    ...     'y': '2'}

    >>> match_xpath('$whole', path)
    {'whole': '/foo[(((@a = 1) and (@b = 2)) and (@c = 2))]'}

    >>> assert match_xpath('42', '42') == {}
    >>> assert match_xpath('"foo"', "'foo'") == {}
    >>> assert match_xpath('42', '"foo"') is None

    >>> assert match_xpath('foo', 'foo') == {}
    >>> assert match_xpath('/foo', '/foo') == {}
    >>> assert match_xpath('foo', '/foo') is None
    >>> assert match_xpath('/foo', '/bar') is None

    >>> assert match_xpath('.', '.') == {}
    >>> assert match_xpath('.', '..') is None
    >>> assert match_xpath('.', 'self::node()') == {}
    >>> assert match_xpath('node()', 'node') is None
    >>> assert match_xpath('node()', 'text()') is None

    >>> assert match_xpath('-42', '-42') == {}
    >>> assert match_xpath('-$answer', '-42') == {'answer': '42'}
    >>> assert match_xpath('2 + 3', '2+3') == {}
    >>> assert match_xpath('2 + 2', '2 + 3') is None
    >>> assert match_xpath('2 + 2', '3 + 2') is None

    >>> assert match_xpath('f()', 'f()') == {}
    >>> assert match_xpath('f(1)', 'f(1)') == {}
    >>> assert match_xpath('f(1, 2, 3)', 'f(1, 2, 3)') == {}
    >>> assert match_xpath('f(1, 2, 3)', 'g(1, 2, 3)') is None
    >>> assert match_xpath('f(1, 2, 3)', 'f(1, 2, 3, 4)') is None
    >>> assert match_xpath('f(1, 2, 3, 4)', 'f(1, 2, 3)') is None
    >>> assert match_xpath('f(1, 2, 3, 4)', 'f(1, 2, 3, 5)') is None
    >>> assert match_xpath("/*/*[contains(., 'xyz')]",
    ...                    "/*/*[contains(., 'xyz')]") == {}
    >>> assert match_xpath("/*/*[contains(., 'xyz')]",
    ...                    "/*/*[contains(., 'XYZ')]") is None
    >>> assert match_xpath("/*/*[contains(., 'xyz')]",
    ...                    "/*/*[starts-with(., 'xyz')]") is None

    >>> assert match_xpath('/*/*', '/*/*') == {}
    >>> assert match_xpath('/*/*[5]', '/*/*[5]') == {}
    >>> assert match_xpath('/*/*', '/*/*[5]') is None
    >>> assert match_xpath('/*/*[5][@color="blue"]',
    ...                    '/*/*[5][@color="blue"]') == {}
    >>> assert match_xpath('/*/*[5]',
    ...                    '/*/*[5][@color="blue"]') is None
    >>> assert match_xpath('/*/*[5][@color="red"]',
    ...                    '/*/*[5][@color="blue"]') is None

    >>> assert match_xpath('processing-instruction("foo")',
    ...                    'processing-instruction("foo")') == {}
    >>> assert match_xpath('processing-instruction("foo")',
    ...                    'processing-instruction("bar")') is None
    """}

def _test():
    import doctest, sys
    doctest.testmod(sys.modules[__name__])

if __name__ == "__main__":
    _test()
