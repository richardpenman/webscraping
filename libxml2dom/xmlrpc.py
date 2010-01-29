#!/usr/bin/env python

"""
XML-RPC support using libxml2dom.

See: http://www.xmlrpc.com/spec

Copyright (C) 2007 Paul Boddie <paul@boddie.org.uk>

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU Lesser General Public License as published by the Free
Software Foundation; either version 3 of the License, or (at your option) any
later version.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more
details.

You should have received a copy of the GNU Lesser General Public License along
with this program.  If not, see <http://www.gnu.org/licenses/>.

--------

The sending and receiving of XML-RPC messages can be done using traditional HTTP
libraries.

See tests/xmlrpc_test.py for more details.
"""

import libxml2dom
from libxml2dom.macrolib import *
from libxml2dom.macrolib import \
    createDocument as Node_createDocument
import datetime

class XMLRPCImplementation(libxml2dom.Implementation):

    "Contains an XML-RPC-specific implementation."

    # Wrapping of documents.

    def adoptDocument(self, node):
        return XMLRPCDocument(node, self)

    # Factory functions.

    def get_node(self, _node, context_node):

        """
        Get a libxml2dom node for the given low-level '_node' and libxml2dom
        'context_node'.
        """

        if Node_nodeType(_node) == context_node.ELEMENT_NODE:

            # Make special elements.

            if Node_localName(_node) in ("methodCall", "methodResponse"):
                return XMLRPCMethodElement(_node, self, context_node.ownerDocument)
            elif Node_localName(_node) == "methodName":
                return XMLRPCMethodNameElement(_node, self, context_node.ownerDocument)
            elif Node_localName(_node) == "fault":
                return XMLRPCFaultElement(_node, self, context_node.ownerDocument)
            elif Node_localName(_node) == "string":
                return XMLRPCStringElement(_node, self, context_node.ownerDocument)
            elif Node_localName(_node) in ("int", "i4"):
                return XMLRPCIntegerElement(_node, self, context_node.ownerDocument)
            elif Node_localName(_node) == "boolean":
                return XMLRPCBooleanElement(_node, self, context_node.ownerDocument)
            elif Node_localName(_node) == "double":
                return XMLRPCDoubleElement(_node, self, context_node.ownerDocument)
            elif Node_localName(_node) == "dateTime.iso8601":
                return XMLRPCDateTimeElement(_node, self, context_node.ownerDocument)
            elif Node_localName(_node) == "base64":
                return XMLRPCBase64Element(_node, self, context_node.ownerDocument)
            elif Node_localName(_node) == "struct":
                return XMLRPCStructElement(_node, self, context_node.ownerDocument)
            elif Node_localName(_node) == "member":
                return XMLRPCMemberElement(_node, self, context_node.ownerDocument)
            elif Node_localName(_node) == "value":
                return XMLRPCValueElement(_node, self, context_node.ownerDocument)
            elif Node_localName(_node) == "name":
                return XMLRPCNameElement(_node, self, context_node.ownerDocument)
            elif Node_localName(_node) == "array":
                return XMLRPCArrayElement(_node, self, context_node.ownerDocument)
            elif Node_localName(_node) == "data":
                return XMLRPCDataElement(_node, self, context_node.ownerDocument)

            # Otherwise, make generic XML-RPC elements.

            return XMLRPCElement(_node, self, context_node.ownerDocument)

        else:
            return libxml2dom.Implementation.get_node(self, _node, context_node)

    # Convenience functions.

    def createXMLRPCMessage(self, namespaceURI, localName):

        "Create a new XML-RPC message document (fragment)."

        return XMLRPCDocument(Node_createDocument(namespaceURI, localName, None), self).documentElement

    def createMethodCall(self):
        return self.createXMLRPCMessage(None, "methodCall")

    def createMethodResponse(self):
        return self.createXMLRPCMessage(None, "methodResponse")

# Node classes.

class XMLRPCNode(libxml2dom.Node):

    "Convenience modifications to nodes specific to libxml2dom.xmlrpc."

    pass

class XMLRPCElement(XMLRPCNode):

    "An XML-RPC element."

    pass

class XMLRPCDocument(libxml2dom._Document, XMLRPCNode):

    "An XML-RPC document fragment."

    def _method(self):
        return (self.xpath("methodCall|methodResponse") or [None])[0]

    def _fault(self):
        if self.method is not None:
            return self.method.fault
        else:
            return None

    method = property(_method)
    fault = property(_fault)

    # Node construction methods.

    def createMethodCall(self):
        return self.ownerDocument.createElement("methodCall")

    def createMethodResponse(self):
        return self.ownerDocument.createElement("methodResponse")

class XMLRPCMethodElement(XMLRPCNode):

    "An XML-RPC method element."

    def _fault(self):
        return (self.xpath("./fault") or [None])[0]

    def _methodNameElement(self):
        return (self.xpath("./methodName") or [None])[0]

    def _methodName(self):
        name = self.methodNameElement
        if name is not None:
            return name.value
        else:
            return None

    def _setMethodName(self, name):
        if self.methodNameElement is None:
            methodName = self.createMethodName()
            self.appendChild(methodName)
        self.methodNameElement.value = name

    def _parameterValues(self):
        return [value.container.contents for value in self.xpath("./params/param/value")]

    # Node construction methods.

    def createMethodName(self):
        return self.ownerDocument.createElement("methodName")

    def createParameters(self):
        return self.ownerDocument.createElement("params")

    def createFault(self):
        return self.ownerDocument.createElement("fault")

    fault = property(_fault)
    methodNameElement = property(_methodNameElement)
    methodName = property(_methodName, _setMethodName)
    parameterValues = property(_parameterValues)

class XMLRPCArrayElement(XMLRPCNode):

    "An XML-RPC array element."

    def _data(self):
        return (self.xpath("./data") or [None])[0]

    def _contents(self):
        return self

    # Sequence emulation.

    def __len__(self):
        if self.data:
            return len(self.data)
        else:
            return 0

    def __getitem__(self, i):
        if self.data:
            return self.data[i]
        else:
            raise IndexError, i

    def __eq__(self, other):
        for i, j in map(None, self, other):
            if i != j:
                return False
        return True

    # Node construction methods.

    def createData(self):
        return self.ownerDocument.createElement("data")

    data = property(_data)
    contents = property(_contents)

class XMLRPCStructElement(XMLRPCNode):

    "An XML-RPC structure element."

    def _members(self):
        return self.xpath("./member")

    def _contents(self):
        return self

    # Sequence emulation.

    def __len__(self):
        return len(self.members)

    def __getitem__(self, i):
        return self.members[i]

    def __eq__(self, other):
        for i, j in map(None, self, other):
            if i != j:
                return False
        return True

    # Node construction methods.

    def createMember(self):
        return self.ownerDocument.createElement("member")

    members = property(_members)
    contents = property(_contents)

class XMLRPCDataElement(XMLRPCNode):

    "An XML-RPC array data element."

    def _values(self):
        return self.xpath("./value")

    # Sequence emulation.

    def __len__(self):
        return len(self.values)

    def __getitem__(self, i):
        return self.values[i].container.contents

    # Node construction methods.

    def createValue(self):
        return self.ownerDocument.createElement("value")

    values = property(_values)

class XMLRPCMemberElement(XMLRPCNode):

    "An XML-RPC structure member element."

    def _value(self):
        return (self.xpath("./value") or [None])[0]

    def _nameElement(self):
        return (self.xpath("./name") or [None])[0]

    def _memberName(self):
        if self.nameElement is not None:
            return self.nameElement.value
        else:
            return None

    def _setMemberName(self, name):
        if self.nameElement is None:
            nameElement = self.createName()
            self.appendChild(nameElement)
        self.nameElement.value = name

    def _contents(self):
        return self

    # Item (name, value) emulation.

    def __len__(self):
        return 2

    def __getitem__(self, i):
        return (self.memberName, self.value.container.contents)[i]

    def __eq__(self, other):
        return self[0] == other[0] and self[1] == other[1]

    # Node construction methods.

    def createName(self):
        return self.ownerDocument.createElement("name")

    def createValue(self):
        return self.ownerDocument.createElement("value")

    value = property(_value)
    nameElement = property(_nameElement)
    memberName = property(_memberName, _setMemberName)
    contents = property(_contents)

class XMLRPCStringElement(XMLRPCNode):

    "An XML-RPC string element."

    typename = "string"

    def _value(self):
        return self.textContent.strip()

    def _setValue(self, value):
        for node in self.childNodes:
            self.removeChild(node)
        text = self.ownerDocument.createTextNode(value)
        self.appendChild(text)

    def _contents(self):
        return convert(self.typename, self.value)

    def __eq__(self, other):
        if hasattr(other, "contents"):
            return self.contents == other.contents
        else:
            return self.contents == other

    value = property(_value, _setValue)
    contents = property(_contents)

class XMLRPCNameElement(XMLRPCStringElement):

    "An XML-RPC name element."

    pass

class XMLRPCValueElement(XMLRPCStringElement):

    "An XML-RPC value element."

    def _type(self):
        elements = self.xpath("*")
        if elements:
            return elements[0].localName
        else:
            return "string"

    def _container(self):
        return (self.xpath("*") or [self])[0]

    type = property(_type)
    container = property(_container)

class XMLRPCMethodNameElement(XMLRPCStringElement):

    "An XML-RPC method element."

    pass

class XMLRPCIntegerElement(XMLRPCStringElement):

    "An XML-RPC integer element."

    typename = "int"

class XMLRPCBooleanElement(XMLRPCStringElement):

    "An XML-RPC boolean element."

    typename = "boolean"

class XMLRPCDoubleElement(XMLRPCStringElement):

    "An XML-RPC double floating point number element."

    typename = "double"

class XMLRPCDateTimeElement(XMLRPCStringElement):

    "An XML-RPC date/time element."

    typename = "datetime"

class XMLRPCBase64Element(XMLRPCStringElement):

    "An XML-RPC integer element."

    typename = "base64"

class XMLRPCFaultElement(XMLRPCNode):

    "An XML-RPC fault element."

    def _code(self):
        code = self.xpath("./value/struct/member[./name/text() = 'faultCode']/value/int")
        if code:
            return code[0].value
        else:
            return None

    def _reason(self):
        reason = self.xpath("./value/struct/member[./name/text() = 'faultString']/value/string")
        if reason:
            return reason[0].value
        else:
            return None

    code = property(_code)
    reason = property(_reason)

# Conversion functions.

def convert(typename, value):
    return default_converters[typename](value)

def boolean(s):
    if s.lower() == "true":
        return True
    elif s.lower() == "false":
        return False
    else:
        raise ValueError, "String value %s not convertable to boolean." % repr(s)

def iso8601(s):
    year, month, day, hour, minute, second = map(int, (s[:4], s[4:6], s[6:8], s[9:11], s[12:14], s[15:17]))
    return datetime.datetime(year, month, day, hour, minute, second)

default_converters = {
    "string" : unicode,
    "int" : int,
    "i4" : int,
    "double" : float,
    "boolean" : boolean,
    "dateTime.iso8601" : iso8601,
    "base64" : str
    }

# Utility functions.

createDocument = libxml2dom.createDocument
createDocumentType = libxml2dom.createDocumentType

def createXMLRPCMessage(namespaceURI, localName):
    return default_impl.createXMLRPCMessage(None, localName)

def createMethodCall():
    return default_impl.createMethodCall()

def createMethodResponse():
    return default_impl.createMethodResponse()

def parse(stream_or_string, html=0, htmlencoding=None, unfinished=0, impl=None):
    return libxml2dom.parse(stream_or_string, html=html, htmlencoding=htmlencoding, unfinished=unfinished, impl=(impl or default_impl))

def parseFile(filename, html=0, htmlencoding=None, unfinished=0, impl=None):
    return libxml2dom.parseFile(filename, html=html, htmlencoding=htmlencoding, unfinished=unfinished, impl=(impl or default_impl))

def parseString(s, html=0, htmlencoding=None, unfinished=0, impl=None):
    return libxml2dom.parseString(s, html=html, htmlencoding=htmlencoding, unfinished=unfinished, impl=(impl or default_impl))

def parseURI(uri, html=0, htmlencoding=None, unfinished=0, impl=None):
    return libxml2dom.parseURI(uri, html=html, htmlencoding=htmlencoding, unfinished=unfinished, impl=(impl or default_impl))

# Single instance of the implementation.

default_impl = XMLRPCImplementation()

# vim: tabstop=4 expandtab shiftwidth=4
