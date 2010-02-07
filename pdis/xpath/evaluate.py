#
# pdis.xpath.evaluate
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
Subroutines used by evaluate() methods in syntax.py
"""

try:
    from math import floor, ceil
except ImportError:
    from pdis.lib.compat import floor, ceil

from pdis.xpath.xpath_exceptions import \
     XPathNotImplementedError, XPathEvaluationError
from pdis.xpath.data_model import *
from pdis.xpath import atoms

def is_string(x):
    return isinstance(x, (str, unicode))

def is_number(x):
    return isinstance(x, float)

def is_boolean(x):
    # Booleans are int in Python 2.2 and bool in Python 2.3.
    return isinstance(x, type(True))

def to_string(x):
    if is_string(x):
        return x
    elif is_node_set(x):
        if len(x) > 0:
            return get_string_value(x[0])
        else:
            return ""
    elif is_number(x):
        if int(x) == x:
            x = int(x)
        return str(x)
    elif is_boolean(x):
        if x:
            return "true"
        else:
            return "false"
    else:
        assert False

def to_number(x):
    if is_node_set(x):
        x = to_string(x)
    try:
        return float(x)
    except ValueError:
        raise XPathEvaluationError, \
              'Could not convert "%s" to number.' % x

def to_boolean(x):
    if is_boolean(x):
        return x
    elif is_number(x):
        return x != 0
    elif is_node_set(x):
        return len(x) > 0
    elif is_string(x):
        return len(x) > 0
    else:
        assert False

relational_transpose = {
    '=' : '=',
    '!=' : '!=',
    '<' : '>',
    '>' : '<',
    '<=' : '>=',
    '>=' : '<=',
    }

def compare(op, x, y):
    x_is_node_set = is_node_set(x)
    y_is_node_set = is_node_set(y)
    if x_is_node_set and y_is_node_set:
        x = map(get_string_value, x)
        y = map(get_string_value, y)
        for xx in x:
            for yy in y:
                if compare2(op, xx, yy):
                    return True
        return False
    elif x_is_node_set or y_is_node_set:
        if y_is_node_set:
            op = relational_transpose[op]
            x, y = y, x
        if is_number(y):
            x = map(get_string_value, x)
            x = map(to_number, x)
            for xx in x:
                if compare2(op, xx, y):
                    return True
            return False
        elif is_string(y):
            x = map(get_string_value, x)
            for xx in x:
                if compare2(op, xx, y):
                    return True
            return False
        elif is_boolean(y):
            return compare2(op, to_boolean(x), y)
    else:
        return compare2(op, x, y)

def compare2(op, x, y):
    if op in ['=', '!=']:
        if is_boolean(x) or is_boolean(y):
            x = to_boolean(x)
            y = to_boolean(y)
        elif is_number(x) or is_number(y):
            x = to_number(x)
            y = to_number(y)
        else:
            x = to_string(x)
            y = to_string(y)
        if op == '=':
            return x == y
        else:
            return x != y
    elif op in ['<', '>', '<=', '>=']:
        x = to_number(x)
        y = to_number(y)
        if op == '<':
            return x < y
        elif op == '>':
            return x > y
        elif op == '<=':
            return x <= y
        elif op == '>=':
            return x >= y
        else:
            assert False
    else:
        assert False

def do_step(input_node_set, axis, node_test, predicate_list, context):
    if axis == "self":
        if isinstance(node_test, atoms.NameTest):
            uri, name = node_test.expand(context)
            node_set = [x for x in input_node_set if is_element_node(x, uri, name)]
        elif isinstance(node_test, atoms.NodeType):
            if node_test.name == "node":
                node_set = input_node_set
            elif node_test.name == "text":
                # XXX Bug: This also selects attribute nodes.
                node_set = filter(is_text_node, input_node_set)
            else:
                node_set = ()
        else:
            node_set = ()

        for predicate in predicate_list:
            node_set = filter_singleton_nodes(node_set, predicate, context)
        return node_set
    elif axis == "attribute":
        if isinstance(node_test, atoms.NameTest):
            uri, name = node_test.expand(context)
            if name is None:
                raise XPathNotImplementedError, \
                      'Wildcard attribute references not supported.'
            node_set = []
            for node in input_node_set:
                value = get_attribute_value(node, uri, name)
                if value is not None:
                    node_set.append(value)
        else:
            raise XPathNotImplementedError, \
                  'Only name tests supported for attribute references.'

        for predicate in predicate_list:
            node_set = filter_singleton_nodes(node_set, predicate, context)
        return node_set
    elif axis == "child":
        result = []
        for context_node in input_node_set:
            if isinstance(node_test, atoms.NameTest):
                uri, name = node_test.expand(context)
                node_set = get_child_element_nodes(context_node, uri, name)
            elif isinstance(node_test, atoms.NodeType):
                if node_test.name == "node":
                    node_set = get_child_nodes(context_node)
                elif node_test.name == "text":
                    node_set = get_child_text_nodes(context_node)
                else:
                    node_set = ()
            else:
                node_set = ()

            for predicate in predicate_list:
                node_set = filter_node_set(node_set, predicate, context)
            result = join_node_sets(result, node_set)
        return result
    else:
        raise XPathNotImplementedError, '"%s" axis not supported.' % axis

def filter_singleton_nodes(node_set, predicate, context_context):
    result = []
    context = context_context.clone()
    context.size = 1
    context.position = 1
    for node in node_set:
        context.node = node
        if evaluate_predicate(predicate, context):
            result.append(node)
    return result

def filter_node_set(node_set, predicate, context_context):
    result = []
    context = context_context.clone()
    context.size = len(node_set)
    context.position = 0
    for node in node_set:
        context.position += 1
        context.node = node
        if evaluate_predicate(predicate, context):
            result.append(node)
    return result

def evaluate_predicate(predicate, context):
    value = predicate.evaluate(context)
    if is_number(value):
        return value == context.position
    else:
        return to_boolean(value)

def do_function_call(name, args, context):
    if name == "last":
        check(name, not args)
        return float(context.size)
    elif name == "position":
        check(name, not args)
        return float(context.position)
    elif name == "count":
        check(name, len(args) == 1 and is_node_set(args[0]))
        # XXX We should make sure the node set doesn't contain any
        # duplicates.  However, that won't be possible when it
        # contains text or attribute nodes.
        return float(len(args[0]))
    elif name == "id":
        check(name, len(args) == 1)
        raise XPathNotImplementedError, 'id() not supported.'
    elif name in ["local-name", "namespace-uri", "name"]:
        check(name, len(args) <= 1)
        if args:
            node_set = args[0]
            check(name, is_node_set(node_set))
            if len(node_set) == 0:
                return ""
            node = node_set[0]
        else:
            node = context.node

        if is_root_node(node):
            uri, local_part = "", ""
        elif is_element_node(node):
            uri, local_part = get_expanded_name(node)
        else:
            # Text or attribute node.
            raise XPathNotImplementedError, \
                  '%s() not supported for this node type.' % name

        if name == "local-name":
            return local_part
        elif name == "namespace-uri":
            return uri
        else:
            assert name == "name"
            if uri:
                raise XPathNotImplementedError, \
                      'name() not supported for qualified names.'
            else:
                return local_part
    elif name == "string":
        check(name, len(args) <= 1)
        if args:
            return to_string(args[0])
        else:
            return get_string_value(context.node)
    elif name == "concat":
        check(name, len(args) >= 2)
        args = map(to_string, args)
        return "".join(args)
    elif name == "starts-with":
        check(name, len(args) == 2)
        args = map(to_string, args)
        return args[0].startswith(args[1])
    elif name == "contains":
        check(name, len(args) == 2)
        args = map(to_string, args)
        return args[0].find(args[1]) >= 0
    elif name == "substring-before":
        check(name, len(args) == 2)
        args = map(to_string, args)
        k = args[0].find(args[1])
        if k == -1:
            return ""
        else:
            return args[0][:k]
    elif name == "substring-after":
        check(name, len(args) == 2)
        args = map(to_string, args)
        k = args[0].find(args[1])
        if k == -1:
            return ""
        else:
            k += len(args[1])
            return args[0][k:]
    elif name == "substring":
        check(name, len(args) == 2 or len(args) == 3)
        s = to_string(args[0])
        i = int(round(to_number(args[1]))) - 1
        if len(args) == 2:
            i = max(i, 0)
            return s[i:]
        else:
            k = int(round(to_number(args[2])))
            j = i + k
            i = max(i, 0)
            return s[i:j]
    elif name == "string-length":
        check(name, len(args) <= 1)
        if args:
            s = to_string(args[0])
        else:
            s = get_string_value(context.node)
        return float(len(s))
    elif name == "normalize-space":
        check(name, len(args) <= 1)
        if args:
            s = to_string(args[0])
        else:
            s = get_string_value(context.node)
        s = s.strip()
        return " ".join(s.split())
    elif name == "translate":
        check(name, len(args) == 3)
        args = map(to_string, args)
        args = map(unicode, args)
        s, a, b = args
        n = len(b)
        d = a[n:]
        a = a[:n]
        table = {}
        for c in d:
            table[ord(c)] = None
        for i in range(n):
            c = a[i]
            if ord(c) not in table:
                table[ord(c)] = ord(b[i])
        return s.translate(table)
    elif name == "boolean":
        check(name, len(args) == 1)
        return to_boolean(args[0])
    elif name == "not":
        check(name, len(args) == 1)
        return not to_boolean(args[0])
    elif name == "true":
        check(name, not args)
        return True
    elif name == "false":
        check(name, not args)
        return False
    elif name == "lang":
        check(name, len(args) == 1)
        raise XPathNotImplementedError, 'lang() not supported.'
    elif name == "number":
        check(name, len(args) == 1)
        if args:
            s = args[0]
        else:
            s = get_string_value(context.node)
        return to_number(s)
    elif name == "sum":
        check(name, len(args) == 1 and is_node_set(args[0]))
        result = 0.0
        for value in args[0]:
            result += to_number(get_string_value(value))
        return result
    elif name == "floor":
        check(name, len(args) == 1)
        return floor(to_number(args[0]))
    elif name == "ceil":
        check(name, len(args) == 1)
        return ceil(to_number(args[0]))
    elif name == "round":
        check(name, len(args) == 1)
        return round(to_number(args[0]))
    else:
        raise XPathEvaluationError, 'Unknown function %s().' % name

def check(name, test):
    if not test:
        raise XPathEvaluationError, \
              'Illegal argument list for %s().' % name
