#
# pdis.xpath.data_model
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
XML data model

Our model is element tree extended with a root node.  We support four
of the XPath data model node types:

 * root node -- singleton tuple containing an Element.
 * element node -- Element.
 * text or attribute node -- string.

We represent node sets as sequences.  They can be lists, but we also
take advantage of the fact that a root or element node is effectively
a sequence containing its element children.
"""

from pdis.xpath.ET import iselement

def is_node_set(x):
    return isinstance(x, (list, tuple)) or iselement(x)

def join_node_sets(x, y):
    if len(x) == 0:
        return y
    elif len(y) == 0:
        return x
    else:
        if not isinstance(x, list):
            x = list(x)
        if not isinstance(y, list):
            y = list(y)
        return x + y

def is_root_node(node):
    return isinstance(node, tuple)

def is_element_node(node, uri = None, name = None):
    if not iselement(node):
        return False
    if uri is None and name is None:
        return True
    element_uri, element_name = get_expanded_name(node)
    if uri != element_uri:
        return False
    return name is None or name == element_name

def is_text_node(node):
    # Note that this predicate also returns True for attribute nodes.
    return isinstance(node, (str, unicode))

def get_string_value(node):
    return "".join(get_text_nodes(node))

def get_expanded_name(element):
    tag = element.tag
    if tag[:1] != "{":
        return (None, tag)
    else:
        return tag[1:].split("}", 1)

def get_attribute_value(node, uri, name):
    if is_element_node(node):
        if uri:
            name = "{%s}%s" % (uri, name)
        return node.get(name)
    else:
        return None

def get_child_element_nodes(node, uri = None, name = None):
    if is_root_node(node):
        if is_element_node(node[0], uri, name):
            return node                 # Container with one element.
        else:
            return ()
    elif is_text_node(node):
        return ()
    else:
        assert is_element_node(node)
        if name:
            if uri:
                name = "{%s}%s" % (uri, name)
            return [x for x in node if x.tag == name]
        elif uri:
            prefix = "{%s}" % uri
            return [x for x in node if x.tag.startswith(prefix)]
        else:
            return node                 # Container with child elements.

def get_child_text_nodes(node):
    result = []
    if is_element_node(node):
        if node.text:
            result.append(node.text)
        for child in node:
            if child.tail:
                result.append(child.tail)
    return result

def get_child_nodes(node):
    if is_root_node(node):
        return node                     # Container with one element.
    elif is_text_node(node):
        return ()
    else:
        assert is_element_node(node)

        # If there are no text children, we can simply return the element,
        # which acts as a container for its element children.
        if not node.text:
            for child in node:
                if child.tail:
                    break
            else:
                return node             # Container with child elements.

        # Collect the text and element children.
        result = []
        if node.text:
            result.append(node.text)
        for child in node:
            result.append(child)
            if child.tail:
                result.append(child.tail)
        return result

def get_text_nodes(node):
    # This is something like descendant-or-self::text(), except that
    # it also returns the given node if it is an attribute node.
    if is_root_node(node):
        return get_text_nodes(node[0])
    elif is_text_node(node):
        return [node]
    else:
        assert is_element_node(node)
        result = []
        push_text_nodes(result, node)
        return result

def push_text_nodes(buffer, element):
    if element.text:
        buffer.append(element.text)
    for child in element:
        push_text_nodes(buffer, child)
        if child.tail:
            buffer.append(child.tail)
