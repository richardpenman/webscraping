#
# pdis.xpath.parser
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
Slow but portable XPath parser
"""

from pdis.xpath.xpath_exceptions import XPathParseError
from pdis.xpath.lexer import TokenStream
from pdis.xpath.syntax import *

def parse_xpath(input_string):
    return Parser(input_string).parse()

class Parser(TokenStream):
    def parse(self):
        x = self.parse_expression()
        t = self.peek()
        if t is not None:
            raise XPathParseError, 'Unexpected token after expression: %s' % t
        return x

    def next(self):
        try:
            return TokenStream.next(self)
        except StopIteration:
            raise XPathParseError, 'Unexpected end of token stream.'

    precedence_table = {
        'or' : 1,
        'and' : 2,
        '=' : 3, '!=' : 3,
        '<' : 4, '>' : 4, '<=' : 4, '>=' : 4,
        '+' : 5, '-' : 5,
        '*' : 6, 'div' : 6, 'mod' : 6,
        '|' : 8 }

    unary_minus_precedence = 7

    def parse_expression(self, left_precedence = 0):
        if self.peek() != '-':
            x = self.parse_path_expression()
        else:
            if left_precedence > self.unary_minus_precedence:
                raise XPathParseError, 'Unary minus not allowed here.'
            else:
                op = self.next()
                x = self.parse_expression(self.unary_minus_precedence)
                x = UnaryOp(op, x)

        while True:
            op = self.peek()
            right_precedence = self.precedence_table.get(op, 0)
            if left_precedence >= right_precedence:
                return x
            else:
                self.next()
                y = self.parse_expression(right_precedence)
                x = BinaryOp(op, x, y)

    def parse_path_expression(self):
        t = self.peek()
        if t == '(' or isinstance(t, (Literal, Number, FunctionName, VariableReference)):
            x = self.parse_filter_expression()
            return self.parse_rest_of_path(x)
        else:
            return self.parse_location_path()

    def parse_filter_expression(self):
        x = self.parse_primary_expression()
        predicate_list = self.parse_predicate_list()
        if predicate_list:
            # XXX This is not as per the XPath specification, because
            # it results in the nodes returned by the expression being
            # filtered as singleton node sets, instead of as a single
            # node set.
            x = LocationStep(x, "self", NodeType("node"), predicate_list)
        return x

    def parse_primary_expression(self):
        t = self.next()
        if t == '(':
            x = self.parse_expression()
            if self.next() != ')':
                raise XPathParseError, 'Expected closing parenthesis.'
            return x
        elif isinstance(t, (Literal, Number, VariableReference)):
            return t
        elif isinstance(t, FunctionName):
            self.push_back(t)
            return self.parse_function_call()
        else:
            raise XPathParseError, 'Invalid primary expression.'

    def parse_function_call(self):
        f = self.next()
        if not isinstance(f, FunctionName):
            raise XPathParseError, 'Expected function name.'
        if self.next() != '(':
            raise XPathParseError, 'Expected opening parenthesis after function name.'
        args = []
        if self.peek() == ')':
            self.next()
        else:
            while True:
                args.append(self.parse_expression())
                t = self.next()
                if t == ',':
                    continue
                elif t == ')':
                    break
                else:
                    raise XPathParseError, \
                          'Expected comma or closing paren after function argument.'
        return FunctionCall(f, args)

    def parse_location_path(self):
        t = self.next()
        if t == '/':
            x = Root()
            t2 = self.peek()
            if not (isinstance(t2, (NameTest, NodeType, AxisName))
                    or t2 in ['@', '.', '..']):
                return x
        elif t == '//':
            x = LocationStep(Root(), "descendant-or-self", NodeType("node"), [])
        else:
            self.push_back(t)
            x = None

        return self.parse_relative_location_path(x)

    def parse_relative_location_path(self, x):
        x = self.parse_location_step(x)
        return self.parse_rest_of_path(x)

    def parse_rest_of_path(self, x):
        t = self.peek()
        if t == '/':
            self.next()
            return self.parse_relative_location_path(x)
        elif t == '//':
            self.next()
            x = LocationStep(x, "descendant-or-self", NodeType("node"), [])
            return self.parse_relative_location_path(x)
        else:
            return x

    def parse_location_step(self, prefix):
        t = self.next()
        if isinstance(t, AxisName):
            if self.next() != '::':
                raise XPathParseError, 'Expected "::" after axis name.'
            axis = t.name
            node_test = self.parse_node_test()
            predicate_list = self.parse_predicate_list()
        elif t == '@':
            axis = "attribute"
            node_test = self.parse_node_test()
            predicate_list = self.parse_predicate_list()
        elif t == '.':
            axis = "self"
            node_test = NodeType("node")
            predicate_list = []
        elif t == '..':
            axis = "parent"
            node_test = NodeType("node")
            predicate_list = []
        else:
            self.push_back(t)
            axis = "child"
            node_test = self.parse_node_test()
            predicate_list = self.parse_predicate_list()

        return LocationStep(prefix, axis, node_test, predicate_list)

    def parse_node_test(self):
        f = self.next()
        if isinstance(f, NameTest):
            return f
        elif isinstance(f, NodeType):
            if self.next() != '(':
                raise XPathParseError, \
                      'Expected opening parenthesis after node type.'
            t = self.next()
            if t == ')':
                return f
            if isinstance(t, Literal) and self.next() == ')' \
                   and f.name == "processing-instruction":
                return t
            raise XPathParseError, 'Unexpected argument in node type test.'
        else:
            raise XPathParseError, 'Invalid node test.'

    def parse_predicate_list(self):
        result = []
        while self.peek() == '[':
            self.next()
            result.append(self.parse_expression())
            if self.next() != ']':
                raise XPathParseError, 'Expected "]".'
        return result
