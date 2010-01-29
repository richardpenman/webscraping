#!/usr/bin/env python

"""
SOAP support using libxml2dom. Support for the archaic SOAP namespaces is also
provided.

See: http://www.w3.org/TR/2007/REC-soap12-part0-20070427/

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

The sending and receiving of SOAP messages can be done using traditional HTTP
libraries.

See tests/soap_test.py for more details.
"""

import libxml2dom
from libxml2dom.macrolib import *
from libxml2dom.macrolib import \
    createDocument as Node_createDocument

# SOAP-related namespaces.

SOAP_ENVELOPE_NAMESPACE = "http://www.w3.org/2003/05/soap-envelope"
SOAP_ENCODING_NAMESPACE = "http://www.w3.org/2003/05/soap-encoding"
SOAP_RPC_NAMESPACE = "http://www.w3.org/2003/05/soap-rpc"
XS_NAMESPACE = "http://www.w3.org/2001/XMLSchema"
XSI_NAMESPACE = "http://www.w3.org/2001/XMLSchema-instance"

# Archaic namespaces.

OLD_SOAP_ENVELOPE_NAMESPACE = "http://schemas.xmlsoap.org/soap/envelope/"
OLD_SOAP_ENCODING_NAMESPACE = "http://schemas.xmlsoap.org/soap/encoding/"

# Default namespace bindings for XPath.

default_ns = {
    "env" : SOAP_ENVELOPE_NAMESPACE,
    "enc" : SOAP_ENCODING_NAMESPACE,
    "rpc" : SOAP_RPC_NAMESPACE,
    "xs" : XS_NAMESPACE,
    "xsi" : XSI_NAMESPACE,
    "SOAP-ENV" : OLD_SOAP_ENVELOPE_NAMESPACE,
    "SOAP-ENC" : OLD_SOAP_ENCODING_NAMESPACE
    }

class SOAPImplementation(libxml2dom.Implementation):

    "Contains a SOAP-specific implementation."

    # Wrapping of documents.

    def adoptDocument(self, node):
        return SOAPDocument(node, self)

    # Factory functions.

    def get_node(self, _node, context_node):

        """
        Get a libxml2dom node for the given low-level '_node' and libxml2dom
        'context_node'.
        """

        if Node_nodeType(_node) == context_node.ELEMENT_NODE:

            # Make special envelope elements.

            if Node_namespaceURI(_node) in (SOAP_ENVELOPE_NAMESPACE, OLD_SOAP_ENVELOPE_NAMESPACE):
                if Node_localName(_node) == "Envelope":
                    return SOAPEnvelopeElement(_node, self, context_node.ownerDocument)
                elif Node_localName(_node) == "Header":
                    return SOAPHeaderElement(_node, self, context_node.ownerDocument)
                elif Node_localName(_node) == "Body":
                    return SOAPBodyElement(_node, self, context_node.ownerDocument)
                elif Node_localName(_node) == "Fault":
                    return SOAPFaultElement(_node, self, context_node.ownerDocument)
                elif Node_localName(_node) == "Code":
                    return SOAPCodeElement(_node, self, context_node.ownerDocument)
                elif Node_localName(_node) == "Subcode":
                    return SOAPSubcodeElement(_node, self, context_node.ownerDocument)
                elif Node_localName(_node) == "Value":
                    return SOAPValueElement(_node, self, context_node.ownerDocument)
                elif Node_localName(_node) == "Text":
                    return SOAPTextElement(_node, self, context_node.ownerDocument)

            # Detect the method element.

            if Node_parentNode(_node) and Node_localName(Node_parentNode(_node)) == "Body" and \
                Node_namespaceURI(Node_parentNode(_node)) in (SOAP_ENVELOPE_NAMESPACE, OLD_SOAP_ENVELOPE_NAMESPACE):

                return SOAPMethodElement(_node, self, context_node.ownerDocument)

            # Otherwise, make generic SOAP elements.

            return SOAPElement(_node, self, context_node.ownerDocument)

        else:
            return libxml2dom.Implementation.get_node(self, _node, context_node)

    # Convenience functions.

    def createSOAPMessage(self, namespaceURI, localName):

        "Create a new SOAP message document (fragment)."

        return SOAPDocument(Node_createDocument(namespaceURI, localName, None), self).documentElement

# Node classes.

class SOAPNode(libxml2dom.Node):

    "Convenience modifications to nodes specific to libxml2dom.soap."

    def xpath(self, expr, variables=None, namespaces=None):

        """
        Evaluate the given 'expr' using the optional 'variables' and
        'namespaces'. If not otherwise specified, the prefixes given in the
        module global 'default_ns' will be bound as in that dictionary.
        """

        ns = {}
        ns.update(default_ns)
        ns.update(namespaces or {})
        return libxml2dom.Node.xpath(self, expr, variables, ns)

    # All nodes support convenience methods.

    def convert(self, node):
        return node.textContent.strip()

    def _contents(self):
        # NOTE: Should check whether this should be a leaf element.
        if not self.xpath("*"):
            return (self.localName, getattr(self.ownerDocument, "convert", self.convert)(self))
        else:
            return (self.localName, SOAPContents(self))

    def __len__(self):
        return 2

    def __getitem__(self, i):
        return self.contents[i]

    def __eq__(self, other):
        if hasattr(other, "contents"):
            return self.contents == other.contents
        else:
            return self.contents == other

    # Node construction methods.

    def createSOAPElement(self, localName):

        "Create an element with the appropriate namespace and prefix."

        ref_element = self.ownerDocument.documentElement
        prefix = ref_element.prefix
        if prefix:
            name = prefix + ":" + localName
        else:
            name = localName
        return self.createElementNS(ref_element.namespaceURI, name)

    contents = property(_contents)

class SOAPContents(object):

    "A wrapper around another node in order to provide sequence-like access."

    def __init__(self, node):
        self.node = node

    def __len__(self):
        return len(self.node.xpath("*"))

    def __getitem__(self, i):
        return self.node.xpath("*")[i]

    def __eq__(self, other):
        for i, j in map(None, self, other):
            if i != j:
                return False
        return True

class SOAPDocument(libxml2dom._Document, SOAPNode):

    "A SOAP document fragment."

    def _envelope(self):
        return self.xpath("env:Envelope|SOAP-ENV:Envelope")[0]

    envelope = property(_envelope)

    # Convenience methods and properties.

    def _fault(self):
        return self.envelope.body.fault

    def _method(self):
        return self.envelope.body.method

    fault = property(_fault)
    method = property(_method)

class SOAPElement(SOAPNode):

    "A SOAP element."

    pass

class SOAPEnvelopeElement(SOAPNode):

    "A SOAP envelope element."

    def _body(self):
        return self.xpath("env:Body|SOAP-ENV:Body")[0]

    def _setBody(self, body):
        self.appendChild(body)

    def _delBody(self):
        self.removeChild(self.body)

    def createBody(self):
        return self.createSOAPElement("Body")

    body = property(_body, _setBody, _delBody)

class SOAPHeaderElement(SOAPNode):

    "A SOAP header element."

    pass

class SOAPBodyElement(SOAPNode):

    "A SOAP body element."

    def _fault(self):
        return (self.xpath("env:Fault|SOAP-ENV:Fault") or [None])[0]

    def _method(self):
        if self.namespaceURI == SOAP_ENVELOPE_NAMESPACE:
            return (self.xpath("*[@env:encodingStyle = '%s']" % SOAP_ENCODING_NAMESPACE) or [None])[0]
        else:
            return (self.xpath("*") or [None])[0]

    # Node construction methods.

    def createFault(self):
        return self.createSOAPElement("Fault")

    fault = property(_fault)
    method = property(_method)

class SOAPMethodElement(SOAPNode):

    "A SOAP method element."

    def _methodName(self):
        return self.localName

    def _resultParameter(self):
        return (self.xpath(".//rpc:result") or [None])[0]

    def _resultParameterValue(self):
        if self.resultParameter:
            name = self.resultParameter.textContent.strip()
            result = self.xpath(".//" + name, namespaces={self.prefix : self.namespaceURI})
            if result:
                return result[0].textContent.strip()
            else:
                return None
        else:
            return None

    def _parameterValues(self):
        return [value.contents for value in self.xpath("*")]

    methodName = property(_methodName)
    resultParameter = property(_resultParameter)
    resultParameterValue = property(_resultParameterValue)
    parameterValues = property(_parameterValues)

class SOAPFaultElement(SOAPNode):

    "A SOAP fault element."

    def _code(self):
        code = self.xpath("env:Code|SOAP-ENV:Code")
        if code:
            return code[0].value
        else:
            return None

    def _subcode(self):
        subcode = self.xpath("./env:Code/env:Subcode|./SOAP-ENV:Code/SOAP-ENV:Subcode")
        if subcode:
            return subcode[0].value
        else:
            return None

    def _reason(self):
        return (self.xpath("env:Reason|SOAP-ENV:Reason") or [None])[0]

    def _detail(self):
        return (self.xpath("env:Detail|SOAP-ENV:Detail") or [None])[0]

    def createCode(self):
        return self.createSOAPElement("Code")

    code = property(_code)
    subcode = property(_subcode)
    reason = property(_reason)
    detail = property(_detail)

class SOAPSubcodeElement(SOAPNode):

    "A SOAP subcode element."

    def _value(self):
        value = self.xpath("env:Value|SOAP-ENV:Value")
        if value:
            return value[0].textContent.strip()
        else:
            return None

    def _setValue(self, value):
        nodes = self.xpath("env:Value|SOAP-ENV:Value")
        v = self.createValue()
        if nodes:
            self.replaceChild(v, nodes[0])
        else:
            self.appendChild(v)
        v.value = value

    def createValue(self, value=None):
        code_value = self.createSOAPElement("Value")
        if value is not None:
            code_value.value = code
        return code_value

    value = property(_value, _setValue)

class SOAPCodeElement(SOAPSubcodeElement):

    "A SOAP code element."

    def _subcode(self):
        return (self.xpath("env:Subcode|SOAP-ENV:Subcode") or [None])[0]

    def createSubcode(self):
        return self.createSOAPElement("Subcode")

    subcode = property(_subcode)

class SOAPValueElement(SOAPNode):

    "A SOAP value element."

    def _value(self):
        return self.textContent

    def _setValue(self, value):
        for node in self.childNodes:
            self.removeChild(node)
        text = self.ownerDocument.createTextNode(value)
        self.appendChild(text)

    value = property(_value, _setValue)

class SOAPTextElement(SOAPValueElement):

    "A SOAP text element."

    def _lang(self):
        return self.getAttributeNS(libxml2dom.XML_NAMESPACE, "lang")

    def _setLang(self, value):
        self.setAttributeNS(libxml2dom.XML_NAMESPACE, "xml:lang", value)

    lang = property(_lang, _setLang)

# Utility functions.

createDocument = libxml2dom.createDocument
createDocumentType = libxml2dom.createDocumentType

def createSOAPMessage(namespaceURI, localName):
    return default_impl.createSOAPMessage(namespaceURI, localName)

def parse(stream_or_string, html=0, htmlencoding=None, unfinished=0, impl=None):
    return libxml2dom.parse(stream_or_string, html=html, htmlencoding=htmlencoding, unfinished=unfinished, impl=(impl or default_impl))

def parseFile(filename, html=0, htmlencoding=None, unfinished=0, impl=None):
    return libxml2dom.parseFile(filename, html=html, htmlencoding=htmlencoding, unfinished=unfinished, impl=(impl or default_impl))

def parseString(s, html=0, htmlencoding=None, unfinished=0, impl=None):
    return libxml2dom.parseString(s, html=html, htmlencoding=htmlencoding, unfinished=unfinished, impl=(impl or default_impl))

def parseURI(uri, html=0, htmlencoding=None, unfinished=0, impl=None):
    return libxml2dom.parseURI(uri, html=html, htmlencoding=htmlencoding, unfinished=unfinished, impl=(impl or default_impl))

# Single instance of the implementation.

default_impl = SOAPImplementation()

# vim: tabstop=4 expandtab shiftwidth=4
