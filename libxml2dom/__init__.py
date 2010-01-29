#!/usr/bin/env python

"""
DOM wrapper around libxml2, specifically the libxml2mod Python extension module.

Copyright (C) 2003, 2004, 2005, 2006, 2007, 2008 Paul Boddie <paul@boddie.org.uk>

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
"""

__version__ = "0.4.7"

from libxml2dom.macrolib import *
from libxml2dom.macrolib import \
    createDocument as Node_createDocument, \
    parseString as Node_parseString, parseURI as Node_parseURI, \
    parseFile as Node_parseFile, \
    toString as Node_toString, toStream as Node_toStream, \
    toFile as Node_toFile
import urllib # for parseURI in HTML mode
import libxml2dom.errors

# Standard namespaces.

XML_NAMESPACE = xml.dom.XML_NAMESPACE

# Default namespace bindings for XPath.

default_ns = {
    "xml" : XML_NAMESPACE
    }

class Implementation(object):

    "Contains an abstraction over the DOM implementation."

    def createDocumentType(self, localName, publicId, systemId):
        return DocumentType(localName, publicId, systemId)

    def createDocument(self, namespaceURI, localName, doctype):
        return Document(Node_createDocument(namespaceURI, localName, doctype), self)

    # Wrapping of documents.

    def adoptDocument(self, node):
        return Document(node, self)

    # Factory functions.

    def get_node(self, _node, context_node):

        # Return the existing document.

        if Node_nodeType(_node) == context_node.DOCUMENT_NODE:
            return context_node.ownerDocument

        # Return an attribute using the parent of the attribute as the owner
        # element.

        elif Node_nodeType(_node) == context_node.ATTRIBUTE_NODE:
            return Attribute(_node, self, context_node.ownerDocument,
                self.get_node(Node_parentNode(_node), context_node))

        # Return other nodes.

        else:
            return Node(_node, self, context_node.ownerDocument)

    def get_node_or_none(self, _node, context_node):
        if _node is None:
            return None
        else:
            return self.get_node(_node, context_node)

# Attribute and node list wrappers.

class NamedNodeMap(object):

    """
    A wrapper around Node objects providing DOM and dictionary convenience
    methods.
    """

    def __init__(self, node, impl):
        self.node = node
        self.impl = impl

    def getNamedItem(self, name):
        return self.node.getAttributeNode(name)

    def getNamedItemNS(self, ns, localName):
        return self.node.getAttributeNodeNS(ns, localName)

    def setNamedItem(self, node):
        try:
            old = self.getNamedItem(node.nodeName)
        except KeyError:
            old = None
        self.node.setAttributeNode(node)
        return old

    def setNamedItemNS(self, node):
        try:
            old = self.getNamedItemNS(node.namespaceURI, node.localName)
        except KeyError:
            old = None
        self.node.setAttributeNodeNS(node)
        return old

    def removeNamedItem(self, name):
        try:
            old = self.getNamedItem(name)
        except KeyError:
            old = None
        self.node.removeAttribute(name)
        return old

    def removeNamedItemNS(self, ns, localName):
        try:
            old = self.getNamedItemNS(ns, localName)
        except KeyError:
            old = None
        self.node.removeAttributeNS(ns, localName)
        return old

    # Iterator emulation.

    def __iter__(self):
        return NamedNodeMapIterator(self)

    # Dictionary emulation methods.

    def __getitem__(self, name):
        return self.getNamedItem(name)

    def __setitem__(self, name, node):
        if name == node.nodeName:
            self.setNamedItem(node)
        else:
            raise KeyError, name

    def __delitem__(self, name):
        # NOTE: To be implemented.
        pass

    def values(self):
        return [Attribute(_node, self.impl, self.node.ownerDocument) for _node in Node_attributes(self.node.as_native_node()).values()]

    def keys(self):
        return [(attr.namespaceURI, attr.localName) for attr in self.values()]

    def items(self):
        return [((attr.namespaceURI, attr.localName), attr) for attr in self.values()]

    def __repr__(self):
        return str(self)

    def __str__(self):
        return "{%s}" % ",\n".join(["%s : %s" % (repr(key), repr(value)) for key, value in self.items()])

    def _length(self):
        return len(self.values())

    length = property(_length)

class NamedNodeMapIterator(object):

    "An iterator over a NamedNodeMap."

    def __init__(self, nodemap):
        self.nodemap = nodemap
        self.items = self.nodemap.items()

    def next(self):
        if self.items:
            current = self.items[0][1]
            self.items = self.items[1:]
            return current
        else:
            raise StopIteration

class NodeList(list):

    "A wrapper around node lists."

    def item(self, index):
        return self[index]

    def _length(self):
        return len(self)

    length = property(_length)

# Node classes.

class Node(object):

    """
    A DOM-style wrapper around libxml2mod objects.
    """

    ATTRIBUTE_NODE = xml.dom.Node.ATTRIBUTE_NODE
    COMMENT_NODE = xml.dom.Node.COMMENT_NODE
    DOCUMENT_NODE = xml.dom.Node.DOCUMENT_NODE
    DOCUMENT_TYPE_NODE = xml.dom.Node.DOCUMENT_TYPE_NODE
    ELEMENT_NODE = xml.dom.Node.ELEMENT_NODE
    ENTITY_NODE = xml.dom.Node.ENTITY_NODE
    ENTITY_REFERENCE_NODE = xml.dom.Node.ENTITY_REFERENCE_NODE
    NOTATION_NODE = xml.dom.Node.NOTATION_NODE
    PROCESSING_INSTRUCTION_NODE = xml.dom.Node.PROCESSING_INSTRUCTION_NODE
    TEXT_NODE = xml.dom.Node.TEXT_NODE

    def __init__(self, node, impl=None, ownerDocument=None):
        self._node = node
        self.impl = impl or default_impl
        self.ownerDocument = ownerDocument

    def as_native_node(self):
        return self._node

    def _nodeType(self):
        return Node_nodeType(self._node)

    def _childNodes(self):

        # NOTE: Consider a generator instead.

        return NodeList([self.impl.get_node(_node, self) for _node in Node_childNodes(self._node)])

    def _firstChild(self):
        return (self.childNodes or [None])[0]

    def _lastChild(self):
        return (self.childNodes or [None])[-1]

    def _attributes(self):
        return NamedNodeMap(self, self.impl)

    def _namespaceURI(self):
        return Node_namespaceURI(self._node)

    def _textContent(self):
        return Node_textContent(self._node)

    def _nodeValue(self):
        if self.nodeType in null_value_node_types:
            return None
        return Node_nodeValue(self._node)

    def _setNodeValue(self, value):
        Node_setNodeValue(self._node, value)

    def _prefix(self):
        return Node_prefix(self._node)

    def _nodeName(self):
        return Node_nodeName(self._node)

    def _tagName(self):
        return Node_tagName(self._node)

    def _localName(self):
        return Node_localName(self._node)

    def _parentNode(self):
        return self.impl.get_node_or_none(Node_parentNode(self._node), self)

    def _previousSibling(self):
        return self.impl.get_node_or_none(Node_previousSibling(self._node), self)

    def _nextSibling(self):
        return self.impl.get_node_or_none(Node_nextSibling(self._node), self)

    def _doctype(self):
        _doctype = Node_doctype(self._node)
        if _doctype is not None:
            return self.impl.get_node(_doctype, self)
        else:
            return None

    def _publicId(self):
        # NOTE: To be fixed when the libxml2mod API has been figured out.
        if self.nodeType != self.DOCUMENT_TYPE_NODE:
            return None
        declaration = self.toString()
        return self._findId(declaration, "PUBLIC")

    def _systemId(self):
        # NOTE: To be fixed when the libxml2mod API has been figured out.
        if self.nodeType != self.DOCUMENT_TYPE_NODE:
            return None
        declaration = self.toString()
        if self._findId(declaration, "PUBLIC"):
            return self._findIdValue(declaration, 0)
        return self._findId(declaration, "SYSTEM")

    # NOTE: To be removed when the libxml2mod API has been figured out.

    def _findId(self, declaration, identifier):
        i = declaration.find(identifier)
        if i == -1:
            return None
        return self._findIdValue(declaration, i)

    def _findIdValue(self, declaration, i):
        q = declaration.find('"', i)
        if q == -1:
            return None
        q2 = declaration.find('"', q + 1)
        if q2 == -1:
            return None
        return declaration[q+1:q2]

    def hasAttributeNS(self, ns, localName):
        return Node_hasAttributeNS(self._node, ns, localName)

    def hasAttribute(self, name):
        return Node_hasAttribute(self._node, name)

    def getAttributeNS(self, ns, localName):
        return Node_getAttributeNS(self._node, ns, localName)

    def getAttribute(self, name):
        return Node_getAttribute(self._node, name)

    def getAttributeNodeNS(self, ns, localName):
        return Attribute(Node_getAttributeNodeNS(self._node, ns, localName), self.impl, self.ownerDocument, self)

    def getAttributeNode(self, localName):
        return Attribute(Node_getAttributeNode(self._node, localName), self.impl, self.ownerDocument, self)

    def setAttributeNS(self, ns, name, value):
        Node_setAttributeNS(self._node, ns, name, value)

    def setAttribute(self, name, value):
        Node_setAttribute(self._node, name, value)

    def setAttributeNodeNS(self, node):
        Node_setAttributeNodeNS(self._node, node._node)

    def setAttributeNode(self, node):
        Node_setAttributeNode(self._node, node._node)

    def removeAttributeNS(self, ns, localName):
        Node_removeAttributeNS(self._node, ns, localName)

    def removeAttribute(self, name):
        Node_removeAttribute(self._node, name)

    def createElementNS(self, ns, name):
        return self.impl.get_node(Node_createElementNS(self._node, ns, name), self)

    def createElement(self, name):
        return self.impl.get_node(Node_createElement(self._node, name), self)

    def createAttributeNS(self, ns, name):
        tmp = self.createElement("tmp")
        return Attribute(Node_createAttributeNS(tmp._node, self.impl, ns, name))

    def createAttribute(self, name):
        tmp = self.createElement("tmp")
        return Attribute(Node_createAttribute(tmp._node, name), self.impl)

    def createTextNode(self, value):
        return self.impl.get_node(Node_createTextNode(self._node, value), self)

    def createComment(self, value):
        return self.impl.get_node(Node_createComment(self._node, value), self)

    def createCDATASection(self, value):
        return self.impl.get_node(Node_createCDATASection(self._node, value), self)

    def importNode(self, node, deep):
        if hasattr(node, "as_native_node"):
            return self.impl.get_node(Node_importNode(self._node, node.as_native_node(), deep), self)
        else:
            return self.impl.get_node(Node_importNode_DOM(self._node, node, deep), self)

    def cloneNode(self, deep):
        # This takes advantage of the ubiquity of importNode (in spite of the DOM specification).
        return self.importNode(self, deep)

    def insertBefore(self, tmp, oldNode):
        if tmp.ownerDocument != self.ownerDocument:
            raise xml.dom.WrongDocumentErr()
        if oldNode.parentNode != self:
            raise xml.dom.NotFoundErr()
        if hasattr(tmp, "as_native_node"):
            return self.impl.get_node(Node_insertBefore(self._node, tmp.as_native_node(), oldNode.as_native_node()), self)
        else:
            return self.impl.get_node(Node_insertBefore(self._node, tmp, oldNode.as_native_node()), self)

    def replaceChild(self, tmp, oldNode):
        if tmp.ownerDocument != self.ownerDocument:
            raise xml.dom.WrongDocumentErr()
        if oldNode.parentNode != self:
            raise xml.dom.NotFoundErr()
        if hasattr(tmp, "as_native_node"):
            return self.impl.get_node(Node_replaceChild(self._node, tmp.as_native_node(), oldNode.as_native_node()), self)
        else:
            return self.impl.get_node(Node_replaceChild(self._node, tmp, oldNode.as_native_node()), self)

    def appendChild(self, tmp):
        if tmp.ownerDocument != self.ownerDocument:
            raise xml.dom.WrongDocumentErr()
        if hasattr(tmp, "as_native_node"):
            return self.impl.get_node(Node_appendChild(self._node, tmp.as_native_node()), self)
        else:
            return self.impl.get_node(Node_appendChild(self._node, tmp), self)

    def removeChild(self, tmp):
        if hasattr(tmp, "as_native_node"):
            Node_removeChild(self._node, tmp.as_native_node())
        else:
            Node_removeChild(self._node, tmp)
        return tmp

    def getElementById(self, identifier):
        _node = Node_getElementById(self.ownerDocument.as_native_node(), identifier)
        if _node is None:
            return None
        else:
            return self.impl.get_node(_node, self)

    def getElementsByTagName(self, tagName):
        return self.xpath(".//" + tagName)

    def getElementsByTagNameNS(self, namespaceURI, localName):
        return self.xpath(".//ns:" + localName, namespaces={"ns" : namespaceURI})

    def normalize(self):
        text_nodes = []
        for node in self.childNodes:
            if node.nodeType == node.TEXT_NODE:
                text_nodes.append(node)
            elif len(text_nodes) != 0:
                self._normalize(text_nodes)
                text_nodes = []
        if len(text_nodes) != 0:
            self._normalize(text_nodes)

    def _normalize(self, text_nodes):
        texts = []
        for text_node in text_nodes[:-1]:
            texts.append(text_node.nodeValue)
            self.removeChild(text_node)
        texts.append(text_nodes[-1].nodeValue)
        self.replaceChild(self.ownerDocument.createTextNode("".join(texts)), text_nodes[-1])

    childNodes = property(_childNodes)
    firstChild = property(_firstChild)
    lastChild = property(_lastChild)
    value = data = nodeValue = property(_nodeValue, _setNodeValue)
    textContent = property(_textContent)
    name = nodeName = property(_nodeName)
    tagName = property(_tagName)
    namespaceURI = property(_namespaceURI)
    prefix = property(_prefix)
    localName = property(_localName)
    parentNode = property(_parentNode)
    nodeType = property(_nodeType)
    attributes = property(_attributes)
    previousSibling = property(_previousSibling)
    nextSibling = property(_nextSibling)
    doctype = property(_doctype)
    publicId = property(_publicId)
    systemId = property(_systemId)

    # NOTE: To be fixed - these being doctype-specific values.

    entities = {}
    notations = {}

    def isSameNode(self, other):
        return self == other

    def __hash__(self):
        return hash(self.localName)

    def __eq__(self, other):
        return isinstance(other, Node) and Node_equals(self._node, other._node)

    def __ne__(self, other):
        return not (self == other)

    # 4DOM extensions to the usual PyXML API.
    # NOTE: To be finished.

    def xpath(self, expr, variables=None, namespaces=None):

        """
        Evaluate the given expression 'expr' using the optional 'variables' and
        'namespaces' mappings.
        """

        ns = {}
        ns.update(default_ns)
        ns.update(namespaces or {})
        result = Node_xpath(self._node, expr, variables, ns)
        if isinstance(result, str):
            return to_unicode(result)
        elif hasattr(result, "__len__"):
            return NodeList([self.impl.get_node(_node, self) for _node in result])
        else:
            return result

    # Other extensions to the usual PyXML API.

    def xinclude(self):

        """
        Process XInclude declarations within the document, returning the number
        of substitutions performed (zero or more), raising an XIncludeException
        otherwise.
        """

        return Node_xinclude(self._node)

    # Convenience methods.

    def toString(self, encoding=None, prettyprint=0):
        return toString(self, encoding, prettyprint)

    def toStream(self, stream, encoding=None, prettyprint=0):
        toStream(self, stream, encoding, prettyprint)

    def toFile(self, f, encoding=None, prettyprint=0):
        toFile(self, f, encoding, prettyprint)

# Attribute nodes.

class Attribute(Node):

    "A class providing attribute access."

    def __init__(self, node, impl, ownerDocument=None, ownerElement=None):
        Node.__init__(self, node, impl, ownerDocument)
        self.ownerElement = ownerElement

    def _parentNode(self):
        return self.ownerElement

    parentNode = property(_parentNode)

# Document housekeeping mechanisms.

class _Document:

    """
    An abstract class providing document-level housekeeping and distinct
    functionality. Configuration of the document is also supported.
    See: http://www.w3.org/TR/DOM-Level-3-Core/core.html#DOMConfiguration
    """

    # Constants from 
    # See: http://www.w3.org/TR/DOM-Level-3-Val/validation.html#VAL-Interfaces-NodeEditVAL

    VAL_TRUE = 5
    VAL_FALSE = 6
    VAL_UNKNOWN = 7

    def __init__(self, node, impl):
        self._node = node
        self.implementation = self.impl = impl
        self.error_handler = libxml2dom.errors.DOMErrorHandler()

    # Standard DOM properties and their implementations.

    def _documentElement(self):
        return self.xpath("*")[0]

    def _ownerDocument(self):
        return self

    def __del__(self):
        #print "Freeing document", self._node
        libxml2mod.xmlFreeDoc(self._node)

    documentElement = property(_documentElement)
    ownerDocument = property(_ownerDocument)

    # DOM Level 3 Core DOMConfiguration methods.

    def setParameter(self, name, value):
        if name == "error-handler":
            raise xml.dom.NotSupportedErr()
        raise xml.dom.NotFoundErr()

    def getParameter(self, name):
        if name == "error-handler":
            return self.error_handler
        raise xml.dom.NotFoundErr()

    def canSetParameter(self, name, value):
        return 0

    def _parameterNames(self):
        return []

    # Extensions to the usual PyXML API.

    def validate(self, doc):

        """
        Validate the document against the given schema document, 'doc'.
        """

        validation_ns = doc.documentElement.namespaceURI

        if hasattr(doc, "as_native_node"):
            _schema = Document_schema(doc.as_native_node(), validation_ns)
        else:
            _schema = Document_schemaFromString(doc.toString(), validation_ns)
        try:
            self.error_handler.reset()
            return Document_validate(_schema, self._node, self.error_handler, validation_ns)
        finally:
            Schema_free(_schema, validation_ns)

    # DOM Level 3 Validation methods.

    def validateDocument(self, doc):

        """
        Validate the document against the given schema document, 'doc'.
        See: http://www.w3.org/TR/DOM-Level-3-Val/validation.html#VAL-Interfaces-DocumentEditVAL-validateDocument
        """

        return self.validate(doc) and self.VAL_TRUE or self.VAL_FALSE

class Document(_Document, Node):

    """
    A generic document class. Specialised document classes should inherit from
    the _Document class and their own variation of Node.
    """

    pass

class DocumentType(object):

    "A class providing a container for document type information."

    def __init__(self, localName, publicId, systemId):
        self.name = self.localName = localName
        self.publicId = publicId
        self.systemId = systemId

        # NOTE: Nothing is currently provided to support the following
        # NOTE: attributes.

        self.entities = {}
        self.notations = {}

# Constants.

null_value_node_types = [
    Node.DOCUMENT_NODE, Node.DOCUMENT_TYPE_NODE, Node.ELEMENT_NODE,
    Node.ENTITY_NODE, Node.ENTITY_REFERENCE_NODE, Node.NOTATION_NODE
    ]

# Utility functions.

def createDocumentType(localName, publicId, systemId):
    return default_impl.createDocumentType(localName, publicId, systemId)

def createDocument(namespaceURI, localName, doctype):
    return default_impl.createDocument(namespaceURI, localName, doctype)

def parse(stream_or_string, html=0, htmlencoding=None, unfinished=0, validate=0, remote=0, impl=None):

    """
    Parse the given 'stream_or_string', where the supplied object can either be
    a stream (such as a file or stream object), or a string (containing the
    filename of a document). The optional parameters described below should be
    provided as keyword arguments.

    If the optional 'html' parameter is set to a true value, the content to be
    parsed will be treated as being HTML rather than XML. If the optional
    'htmlencoding' is specified, HTML parsing will be performed with the
    document encoding assumed to that specified.

    If the optional 'unfinished' parameter is set to a true value, unfinished
    documents will be parsed, even though such documents may be missing content
    such as closing tags.

    If the optional 'validate' parameter is set to a true value, an attempt will
    be made to validate the parsed document.

    If the optional 'remote' parameter is set to a true value, references to
    remote documents (such as DTDs) will be followed in order to obtain such
    documents.

    A document object is returned by this function.
    """

    impl = impl or default_impl

    if hasattr(stream_or_string, "read"):
        stream = stream_or_string
        return parseString(stream.read(), html=html, htmlencoding=htmlencoding,
            unfinished=unfinished, validate=validate, remote=remote, impl=impl)
    else:
        return parseFile(stream_or_string, html=html, htmlencoding=htmlencoding,
            unfinished=unfinished, validate=validate, remote=remote, impl=impl)

def parseFile(filename, html=0, htmlencoding=None, unfinished=0, validate=0, remote=0, impl=None):

    """
    Parse the file having the given 'filename'. The optional parameters
    described below should be provided as keyword arguments.

    If the optional 'html' parameter is set to a true value, the content to be
    parsed will be treated as being HTML rather than XML. If the optional
    'htmlencoding' is specified, HTML parsing will be performed with the
    document encoding assumed to that specified.

    If the optional 'unfinished' parameter is set to a true value, unfinished
    documents will be parsed, even though such documents may be missing content
    such as closing tags.

    If the optional 'validate' parameter is set to a true value, an attempt will
    be made to validate the parsed document.

    If the optional 'remote' parameter is set to a true value, references to
    remote documents (such as DTDs) will be followed in order to obtain such
    documents.

    A document object is returned by this function.
    """

    impl = impl or default_impl
    return impl.adoptDocument(Node_parseFile(filename, html=html, htmlencoding=htmlencoding,
        unfinished=unfinished, validate=validate, remote=remote))

def parseString(s, html=0, htmlencoding=None, unfinished=0, validate=0, remote=0, impl=None):

    """
    Parse the content of the given string 's'. The optional parameters described
    below should be provided as keyword arguments.

    If the optional 'html' parameter is set to a true value, the content to be
    parsed will be treated as being HTML rather than XML. If the optional
    'htmlencoding' is specified, HTML parsing will be performed with the
    document encoding assumed to that specified.

    If the optional 'unfinished' parameter is set to a true value, unfinished
    documents will be parsed, even though such documents may be missing content
    such as closing tags.

    If the optional 'validate' parameter is set to a true value, an attempt will
    be made to validate the parsed document.

    If the optional 'remote' parameter is set to a true value, references to
    remote documents (such as DTDs) will be followed in order to obtain such
    documents.

    A document object is returned by this function.
    """

    impl = impl or default_impl
    return impl.adoptDocument(Node_parseString(s, html=html, htmlencoding=htmlencoding,
        unfinished=unfinished, validate=validate, remote=remote))

def parseURI(uri, html=0, htmlencoding=None, unfinished=0, validate=0, remote=0, impl=None):

    """
    Parse the content found at the given 'uri'. The optional parameters
    described below should be provided as keyword arguments.

    If the optional 'html' parameter is set to a true value, the content to be
    parsed will be treated as being HTML rather than XML. If the optional
    'htmlencoding' is specified, HTML parsing will be performed with the
    document encoding assumed to that specified.

    If the optional 'unfinished' parameter is set to a true value, unfinished
    documents will be parsed, even though such documents may be missing content
    such as closing tags.

    If the optional 'validate' parameter is set to a true value, an attempt will
    be made to validate the parsed document.

    If the optional 'remote' parameter is set to a true value, references to
    remote documents (such as DTDs) will be followed in order to obtain such
    documents.

    XML documents are retrieved using libxml2's own network capabilities; HTML
    documents are retrieved using the urllib module provided by Python. To
    retrieve either kind of document using Python's own modules for this purpose
    (such as urllib), open a stream and pass it to the parse function:

    f = urllib.urlopen(uri)
    try:
        doc = libxml2dom.parse(f, html)
    finally:
        f.close()

    A document object is returned by this function.
    """

    if html:
        f = urllib.urlopen(uri)
        try:
            return parse(f, html=html, htmlencoding=htmlencoding, unfinished=unfinished,
                validate=validate, remote=remote, impl=impl)
        finally:
            f.close()
    else:
        impl = impl or default_impl
        return impl.adoptDocument(Node_parseURI(uri, html=html, htmlencoding=htmlencoding,
            unfinished=unfinished, validate=validate, remote=remote))

def toString(node, encoding=None, prettyprint=0):

    """
    Return a string containing the serialised form of the given 'node' and its
    children. The optional 'encoding' can be used to override the default
    character encoding used in the serialisation. The optional 'prettyprint'
    indicates whether the serialised form is prettyprinted or not (the default
    setting).
    """

    return Node_toString(node.as_native_node(), encoding, prettyprint)

def toStream(node, stream, encoding=None, prettyprint=0):

    """
    Write the serialised form of the given 'node' and its children to the given
    'stream'. The optional 'encoding' can be used to override the default
    character encoding used in the serialisation. The optional 'prettyprint'
    indicates whether the serialised form is prettyprinted or not (the default
    setting).
    """

    Node_toStream(node.as_native_node(), stream, encoding, prettyprint)

def toFile(node, filename, encoding=None, prettyprint=0):

    """
    Write the serialised form of the given 'node' and its children to a file
    having the given 'filename'. The optional 'encoding' can be used to override
    the default character encoding used in the serialisation. The optional
    'prettyprint' indicates whether the serialised form is prettyprinted or not
    (the default setting).
    """

    Node_toFile(node.as_native_node(), filename, encoding, prettyprint)

def adoptNodes(nodes, impl=None):

    """
    A special utility method which adopts the given low-level 'nodes' and which
    returns a list of high-level equivalents. This is currently experimental and
    should not be casually used.
    """

    impl = impl or default_impl

    if len(nodes) == 0:
        return []
    doc = impl.adoptDocument(libxml2mod.doc(nodes[0]))
    results = []
    for node in nodes:
        results.append(Node(node, impl, doc))
    return results

def getDOMImplementation():

    "Return the default DOM implementation."

    return default_impl

# Single instance of the implementation.

default_impl = Implementation()

# vim: tabstop=4 expandtab shiftwidth=4
