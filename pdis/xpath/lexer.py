#
# pdis.xpath.lexer
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
Slow but portable XPath tokenizer

This intentionally does not use the "re" module.  That was originally
to speed start-up in PyS60, where loading "re" takes a long time.
"""

from pdis.xpath.xpath_exceptions import XPathParseError
from pdis.xpath.atoms import *

punctuation = ['(', ')', '[', ']', '.', '..', '@', ',', '::']

relationals = ['=', '!=', '<', '<=', '>', '>=']
arithmetic = ['*', '+', '-']
operator_syntax =  relationals + arithmetic + ['/', '//', '|']

operator_names = ['and', 'or', 'mod', 'div']
operators = operator_syntax + operator_names

node_types = ['comment', 'text', 'processing-instruction', 'node']
axis_names = ['ancestor', 'ancestor-or-self', 'attribute', 'child', 'descendant',
              'descendant-or-self', 'following', 'following-sibling', 'namespace',
              'parent', 'preceding', 'preceding-sibling', 'self']

class TokenStream:
    """
    Iterable stream of tokens extracted from an XPath expression

    The returned tokens can be any of the following:
     * A string in punctuation + operators.
     * An instance of one of the following:
        - Number
        - Literal
        - NameTest
        - VariableReference
        - FunctionName
        - NodeType
        - AxisName
    """
    def __init__(self, input_string):
        self.tokens = split_tokens(input_string)
        self.position = 0

    def __iter__(self):
        return self

    def next(self):
        """
        Either return the next token or raise StopIteration.
        """
        if self.position >= len(self.tokens):
            raise StopIteration

        token = self.tokens[self.position]
        self.position += 1
        return token

    def peek(self):
        """
        Return either the token that next() would return next or None.
        """
        if self.position >= len(self.tokens):
            return None

        return self.tokens[self.position]

    def push_back(self, token):
        """
        Push a token returned by next() back onto the stream.
        """
        self.position -= 1
        assert self.tokens[self.position] == token

def split_tokens(s):
    """
    Split an XPath expression into a list of tokens.
    """
    i = 0
    t = None
    result = []
    while True:
        t, i = scan_token(s, i, t)
        if t is None:
            return result
        result.append(t)

_tokens_that_never_precede_an_operator = operators + ['@', '::', '(', '[', ',', None]

def scan_token(s, i, preceding_token = None):
    """
    Get the next token starting at position i in the string s.

    The return value is (t, j) where t is the token and j is the
    position at which to start scanning for the next token.  The
    returned token can be any of the following:
     * None, signifying that the end of the input string has been reached.
     * A string in punctuation + operators.
     * An instance of one of the following:
        - Number
        - Literal
        - NameTest
        - VariableReference
        - FunctionName
        - NodeType
        - AxisName
    """
    # This function implements the disambiguation rules defined
    # in Section 3.7 of the XPath specification.  scan_raw_token()
    # does the work of actually splitting the input into tokens.
    t, j = scan_raw_token(s, i)

    if t == '*':
        if preceding_token in _tokens_that_never_precede_an_operator:
            t = NameTest('*')

    if t.__class__ is QName:
        peek = j
        while s[peek:peek+1].isspace():
            peek += 1

        if preceding_token not in _tokens_that_never_precede_an_operator:
            # "...an NCName must be recognized as an OperatorName"
            if t.prefix is None and t.local_part in operator_names:
                t = t.local_part
            else:
                raise XPathParseError('Expected an operator name, not "%s".' % t)
        elif s[peek:peek+1] == '(':
            # "...the token must be recognized as a NodeType or a FunctionName"
            if t.prefix is None and t.local_part in node_types:
                t = NodeType(t.local_part)
            else:
                t = FunctionName(t.prefix, t.local_part)
        elif s[peek:peek+2] == '::':
            # "...the token must be recognized as an AxisName"
            if t.prefix is None and t.local_part in axis_names:
                t = AxisName(t.local_part)
            else:
                raise XPathParseError('Expected an axis name, not "%s".' % t)
        else:
            # "Otherwise, the token must not be recognized as...an
            # OperatorName, a NodeType, a FunctionName, or an AxisName."
            t = NameTest(t.prefix, t.local_part)

    return (t, j)

def _build_punctuation_map():
    tokens = punctuation + operator_syntax

    # Put a after b if a is a prefix of b:
    tokens.sort()
    tokens.reverse()

    map = {}

    for token in tokens:
        key = token[0]
        if key not in map:
            map[key] = []
        map[key].append(token)

    return map

_punctuation_map = _build_punctuation_map()

def scan_raw_token(s, i):
    """
    Get the next token starting at position i in the string s.

    The return value is (t, j) where t is the token and j is the
    position at which to start scanning for the next token.  The
    returned token can be any of the following:
     * None, signifying that the end of the input string has been reached.
     * A string in punctuation + operator_syntax.
     * An instance of one of the following:
        - Number
        - Literal
        - NameTest
        - VariableReference
        - QName
    """
    while s[i:i+1].isspace():
        i += 1
    if i >= len(s):
        return (None, i)
    c = s[i]

    # Punctuation or operator
    for t in _punctuation_map.get(c, ()):
        if s.startswith(t, i):
            j = i + len(t)
            if t == '.' and s[j:j+1].isdigit():
                pass                    # Number -- handled below.
            else:
                return (t, j)

    # Literal
    for quote_char in ['"', "'"]:
        if c == quote_char:
            i += 1
            j = s.find(quote_char, i)
            if j < 0:
                raise XPathParseError('Unmatched quote character.')
            return (Literal(s[i:j]), j + 1)

    # Number
    if c.isdigit() or c == '.':
        j = i
        while s[j:j+1].isdigit():
            j += 1
        if s[j:j+1] == '.':
            j += 1
            while s[j:j+1].isdigit():
                j += 1
        return (Number(s[i:j]), j)

    # Variable Reference
    if c == '$' and ncname_begins(s, i + 1):
        prefix, name, j = scan_qname(s, i + 1)
        return (VariableReference(prefix, name), j)

    # Name Test or QName
    if ncname_begins(s, i):
        prefix, name, j = scan_qname(s, i)
        if prefix is None and s[j:j+2] == ':*':
            return (NameTest(s[i:j], '*'), j + 2)
        else:
            return (QName(prefix, name), j)

    raise XPathParseError('No valid token here: "%s"' % s[i:])

def scan_qname(s, i):
    t1, j = scan_ncname(s, i)
    if s[j:j+1] == ':' and ncname_begins(s, j + 1):
        t2, j = scan_ncname(s, j + 1)
    else:
        t1, t2 = None, t1
    return (t1, t2, j)

def scan_ncname(s, i):
    assert ncname_begins(s, i)
    j = i + 1
    while ncname_continues(s, j):
        j += 1
    return (s[i:j], j)

def ncname_begins(s, i):
    # This is only correct to the extent that the behavior of isalpha()
    # happens to correspond to the XML specification's definition of Letter.
    c = s[i:i+1]
    return c.isalpha() or c == '_'

def ncname_continues(s, i):
    # This is only correct to the extent that the behavior of isalnum()
    # happens to correspond to the XML specification's definition of
    # Letter | Digit | CombiningChar | Extender.
    c = s[i:i+1]
    return c.isalnum() or c in ['.', '-', '_']
