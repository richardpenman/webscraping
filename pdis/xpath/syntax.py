#
# pdis.xpath.syntax
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
XPath syntax nodes
"""

from pdis.xpath.atoms import *
from pdis.xpath.evaluate import to_number, to_boolean, compare, do_step, do_function_call
from pdis.xpath.data_model import is_node_set, join_node_sets
from pdis.xpath.xpath_exceptions import XPathNotImplementedError, XPathEvaluationError

#
# Expression nodes
#

class UnaryOp:
    """
    Unary expression node

    op -- operator (a string).
    right -- child node.

    The operator is actually always "-".
    """
    def __init__(self, op, right):
        self.op = op
        self.right = right

    def __str__(self):
        return "(%s %s)" % (self.op, self.right)

    def evaluate(self, context):
        assert self.op == '-'
        return - to_number(self.right.evaluate(context))

class BinaryOp:
    """
    Binary expression node

    op -- operator (a string).
    left -- left-hand child node.
    right -- right-hand child node.
    """
    def __init__(self, op, left, right):
        self.op = op
        self.left = left
        self.right = right

    def __str__(self):
        return "(%s %s %s)" % (self.left, self.op, self.right)

    def evaluate(self, context):
        if self.op == 'or':
            if to_boolean(self.left.evaluate(context)):
                return True
            return to_boolean(self.right.evaluate(context))
        elif self.op == 'and':
            if not to_boolean(self.left.evaluate(context)):
                return False
            return to_boolean(self.right.evaluate(context))
        elif self.op in ['=', '!=', '<', '>', '<=', '>=']:
            return compare(self.op, self.left.evaluate(context),
                           self.right.evaluate(context))
        elif self.op in ['+', '-', '*', 'div', 'mod']:
            x = to_number(self.left.evaluate(context))
            y = to_number(self.right.evaluate(context))
            if self.op == '+':
                return x + y
            elif self.op == '-':
                return x - y
            elif self.op == '*':
                return x * y
            elif self.op == 'div':
                return x / y
            elif self.op == 'mod':
                z = abs(x) % abs(y)
                if x >= 0:
                    return z
                else:
                    return -z
            else:
                assert False
        elif self.op == '|':
            x = self.left.evaluate(context)
            y = self.right.evaluate(context)
            if is_node_set(x) and is_node_set(y):
                # XXX This is incorrect, because it neither preserves
                # document order nor removes duplicates.
                return join_node_sets(x, y)
            else:
                raise XPathEvaluationError, "Operands of '|' must be node sets."
        else:
            assert False

class FunctionCall:
    """
    Function call node

    function -- FunctionName.
    argument_list -- list of zero or more nodes.
    """
    def __init__(self, function, argument_list):
        self.function = function
        self.argument_list = argument_list

    def __str__(self):
        return "%s(%s)" % (self.function, ", ".join(map(str, self.argument_list)))

    def evaluate(self, context):
        if self.function.prefix:
            raise XPathNotImplementedError, \
                  "Namespace prefixes for function names not implemented."
        name = self.function.local_part
        args = [arg.evaluate(context) for arg in self.argument_list]
        return do_function_call(name, args, context)

#
# Location path nodes
#

class Root:
    """
    Node representing the head of an absolute location path
    """
    def __init__(self):
        pass

    def __str__(self):
        return "/"

    def evaluate(self, context):
        return [context.get_root()]

class LocationStep:
    """
    Node representing a step in a location path

    prefix -- preceding LocationStep, Root, None, or some other node.
    axis -- axis name (a string).
    node_test -- NameTest, NodeType, or Literal (a processing instruction).
    predicate_list -- list of zero or more nodes.

    A value of None for the prefix indicates that this is the head
    of a relative location path.  A Literal value for the node
    test represents a parameterized processing-instruction test.
    """
    def __init__(self, prefix, axis, node_test, predicate_list):
        self.prefix = prefix
        self.axis = axis
        self.node_test = node_test
        self.predicate_list = predicate_list

    def __str__(self):
        parts = []

        if self.prefix is None:
            pass
        elif isinstance(self.prefix, Root):
            parts.append("/")
        else:
            parts.append("%s/" % self.prefix)

        axis = self.axis
        test = self.node_test
        predicates = self.predicate_list

        if axis == "self" and not predicates \
                and isinstance(test, NodeType) and test.name == "node":
            parts.append(".")
        elif axis == "parent" and not predicates \
                and isinstance(test, NodeType) and test.name == "node":
            parts.append("..")
        elif axis == "descendant-or-self" and not predicates \
                and isinstance(test, NodeType) and test.name == "node":
            assert self.prefix is not None
        else:
            if axis == "child":
                pass
            elif axis == "attribute":
                parts.append("@")
            else:
                parts.append("%s::" % axis)

            if isinstance(test, NodeType):
                parts.append("%s()" % test)
            elif isinstance(test, Literal):
                parts.append("processing-instruction(%s)" % test)
            else:
                parts.append("%s" % test)

            for predicate in predicates:
                parts.append("[%s]" % predicate)

        return "".join(parts)

    def evaluate(self, context):
        if self.prefix == None:
            node_set = [context.node]
        else:
            node_set = self.prefix.evaluate(context)
            assert is_node_set(node_set)

        return do_step(node_set, self.axis, self.node_test, self.predicate_list, context)
