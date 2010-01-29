#!/usr/bin/env python

"""
DOM macros for virtual libxml2mod node methods and properties.

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

import xml.dom
from libxml2dom.errors import DOMError

# Try the conventional import first.

try:
    import libxml2mod
except ImportError:
    from libxmlmods import libxml2mod

# NOTE: libxml2 seems to use UTF-8 throughout.
# NOTE: Implement: http://www.w3.org/TR/2006/REC-xml-20060816/#AVNormalize

def from_unicode(s):
    if isinstance(s, unicode):
        return s.encode("utf-8")
    else:
        # The string might contain non-ASCII characters, thus upsetting libxml2
        # as it encounters a non-UTF-8 string.
        try:
            unicode(s)
        except UnicodeError:
            raise TypeError, "Please use Unicode for non-ASCII data."
        return s

def to_unicode(s):
    if isinstance(s, str):
        return unicode(s, encoding="utf-8")
    else:
        return s

def get_ns(ns):
    out_ns = to_unicode(libxml2mod.xmlNodeGetContent(ns))
    # Detect "" and produce None as the empty namespace.
    if out_ns:
        return out_ns
    else:
        return None

def _get_prefix_and_localName(name):
    t = name.split(":")
    if len(t) == 1:
        return None, name
    elif len(t) == 2:
        return t
    else:
        # NOTE: Should raise an exception.
        return None, None

def _find_namespace_for_prefix(node, prefix):

    "Find the namespace definition node in the given 'node' for 'prefix'."

    current = libxml2mod.xmlNodeGetNsDefs(node)
    while current is not None:
        if libxml2mod.name(current) == prefix:
            return current
        current = libxml2mod.next(current)
    return None

def _find_namespace(node, ns, prefix):

    """
    Find the namespace definition node in the given 'node' for the given 'ns'
    and 'prefix'.
    """

    # Special treatment for XML namespace.

    if prefix == "xml" and ns == xml.dom.XML_NAMESPACE:
        return libxml2mod.xmlSearchNsByHref(Node_ownerDocument(node), node, xml.dom.XML_NAMESPACE)

    new_ns = None
    current = libxml2mod.xmlNodeGetNsDefs(node)
    while current is not None:
        if _check_namespace(current, ns, prefix):
            new_ns = current
            break
        current = libxml2mod.next(current)
    if new_ns is None:
        node_ns = libxml2mod.xmlNodeGetNs(node)
        if node_ns is not None and _check_namespace(node_ns, ns, prefix):
            new_ns = node_ns
    return new_ns

def _check_namespace(current, ns, prefix):

    "Check the 'current' namespace definition node against 'ns' and 'prefix'."

    current_ns = get_ns(current)
    current_prefix = libxml2mod.name(current)
    if ns == current_ns and (prefix is None or prefix == current_prefix):
        return 1
    else:
        return 0

def _make_namespace(node, ns, prefix, set_default=0):

    """
    Make a new namespace definition node within the given 'node' for 'ns',
    'prefix', setting the default namespace on 'node' when 'prefix' is None and
    'set_default' is set to a true value (unlike the default value for that
    parameter).
    """

    if prefix is not None or set_default:
        new_ns = libxml2mod.xmlNewNs(node, ns, prefix)
    else:
        new_ns = None
    return new_ns

def _get_invented_prefix(node, ns):
    current = libxml2mod.xmlNodeGetNsDefs(node)
    prefixes = []
    while current is not None:
        current_prefix = libxml2mod.name(current)
        prefixes.append(current_prefix)
        current = libxml2mod.next(current)
    i = 0
    while 1:
        prefix = "NS%d" % i
        if prefix not in prefixes:
            return prefix
        i += 1

_nodeTypes = {
    "attribute" : xml.dom.Node.ATTRIBUTE_NODE,
    "cdata" : xml.dom.Node.CDATA_SECTION_NODE,
    "comment" : xml.dom.Node.COMMENT_NODE,
    "document_xml" : xml.dom.Node.DOCUMENT_NODE,
    "document_html" : xml.dom.Node.DOCUMENT_NODE,
    "doctype" : xml.dom.Node.DOCUMENT_TYPE_NODE,
    "dtd" : xml.dom.Node.DOCUMENT_TYPE_NODE, # NOTE: Needs verifying.
    "element" : xml.dom.Node.ELEMENT_NODE,
    "entity" : xml.dom.Node.ENTITY_NODE,
    "entity_ref" : xml.dom.Node.ENTITY_REFERENCE_NODE,
    "notation" : xml.dom.Node.NOTATION_NODE,
    "pi" : xml.dom.Node.PROCESSING_INSTRUCTION_NODE,
    "text" : xml.dom.Node.TEXT_NODE
    }

_reverseNodeTypes = {}
for label, value in _nodeTypes.items():
    _reverseNodeTypes[value] = label

def Node_equals(node, other):
    return libxml2mod.xmlXPathCmpNodes(node, other) == 0

def Node_ownerDocument(node):
    return libxml2mod.doc(node)

def Node_nodeType(node):
    return _nodeTypes[libxml2mod.type(node)]

def Node_childNodes(node):

    # NOTE: Consider a generator instead.

    child_nodes = []
    node = libxml2mod.children(node)
    while node is not None:
        # Remove doctypes.
        if Node_nodeType(node) != xml.dom.Node.DOCUMENT_TYPE_NODE:
            child_nodes.append(node)
        node = libxml2mod.next(node)
    return child_nodes

def Node_attributes(node):
    attributes = {}

    # Include normal attributes.

    current = libxml2mod.properties(node)
    while current is not None:
        ns = libxml2mod.xmlNodeGetNs(current)
        if ns is not None:
            attributes[(get_ns(ns), libxml2mod.name(current))] = current
        else:
            attributes[(None, libxml2mod.name(current))] = current
        current = libxml2mod.next(current)

    # Include xmlns attributes.

    #current = libxml2mod.xmlNodeGetNsDefs(node)
    #while current is not None:
    #    ns = get_ns(current)
    #    prefix = libxml2mod.name(current)
    #    attributes[(xml.dom.XMLNS_NAMESPACE, "xmlns:" + prefix)] = ns # NOTE: Need a real node here.
    #    current = libxml2mod.next(current)

    return attributes

def Node_namespaceURI(node):
    ns = libxml2mod.xmlNodeGetNs(node)
    if ns is not None:
        return get_ns(ns)
    else:
        return None

def Node_nodeValue(node):
    return to_unicode(libxml2mod.xmlNodeGetContent(node))

# NOTE: This is not properly exposed in the libxml2macro interface as the
# NOTE: writable form of nodeValue.

def Node_setNodeValue(node, value):
    # NOTE: Cannot set attribute node values.
    libxml2mod.xmlNodeSetContent(node, from_unicode(value))

# NOTE: Verify this. The data attribute should only really exist for text,
# NOTE: character data, processing instructions and comments.

Node_data = Node_nodeValue

Node_textContent = Node_nodeValue

def Node_prefix(node):
    ns = libxml2mod.xmlNodeGetNs(node)
    if ns is not None:
        return to_unicode(libxml2mod.name(ns))
    else:
        return None

def Node_nodeName(node):
    prefix = Node_prefix(node)
    if prefix is not None:
        return prefix + ":" + Node_localName(node)
    else:
        return Node_localName(node)

def Node_tagName(node):
    if libxml2mod.type(node) == "element":
        return Node_nodeName(node)
    else:
        return None

def Node_localName(node):
    return to_unicode(libxml2mod.name(node))

def Node_parentNode(node):
    if node is None or libxml2mod.type(node) == "document_xml":
        return None
    else:
        return libxml2mod.parent(node)

def Node_previousSibling(node):
    if node is not None and libxml2mod.prev(node) is not None:
        return libxml2mod.prev(node)
    else:
        return None

def Node_nextSibling(node):
    if node is not None and libxml2mod.next(node) is not None:
        return libxml2mod.next(node)
    else:
        return None

def Node_doctype(node):
    return libxml2mod.xmlGetIntSubset(node)

def Node_hasAttributeNS(node, ns, localName):
    return Node_getAttributeNS(node, ns, localName) is not None or \
        _find_namespace(node, ns, localName) is not None

def Node_hasAttribute(node, name):
    return Node_getAttribute(node, name) is not None

def Node_getAttributeNS(node, ns, localName):
    if ns == xml.dom.XMLNS_NAMESPACE:
        ns_def = _find_namespace_for_prefix(node, localName)
        if ns_def is not None:
            return get_ns(ns_def)
        else:
            return None
    else:
        return to_unicode(libxml2mod.xmlGetNsProp(node, localName, ns))

def Node_getAttribute(node, name):
    return to_unicode(libxml2mod.xmlGetProp(node, name))

def Node_getAttributeNodeNS(node, ns, localName):
    # NOTE: Needs verifying.
    return Node_attributes(node)[(ns, localName)]

def Node_getAttributeNode(node, name):
    # NOTE: Needs verifying.
    return Node_attributes(node)[(None, name)]

def Node_setAttributeNS(node, ns, name, value):
    ns, name, value = map(from_unicode, [ns, name, value])
    prefix, localName = _get_prefix_and_localName(name)

    # Detect setting of xmlns:localName=value, looking for cases where
    # x:attr=value have caused the definition of xmlns:x=y (as a declaration
    # with prefix=x, ns=y).
    if prefix == "xmlns" and ns == xml.dom.XMLNS_NAMESPACE:
        if _find_namespace(node, value, localName):
            return
        new_ns = _make_namespace(node, value, localName, set_default=0)
    # For non-xmlns attributes, we find or make a namespace declaration and then
    # set an attribute.
    elif ns is not None:
        # Look for a suitable namespace.
        new_ns = _find_namespace(node, ns, prefix)
        # Create a declaration if no suitable one was found.
        if new_ns is None:
            # Invent a prefix for unprefixed attributes with namespaces.
            if prefix is None:
                prefix = _get_invented_prefix(node, ns)
            new_ns = _make_namespace(node, ns, prefix, set_default=0)
        # Remove any conflicting attribute.
        if Node_hasAttributeNS(node, ns, localName):
            Node_removeAttributeNS(node, ns, localName)
        libxml2mod.xmlSetNsProp(node, new_ns, localName, value)
    else:
        # NOTE: Needs verifying: what should happen to the namespace?
        # NOTE: This also catches the case where None is the element's
        # NOTE: namespace and is also used for the attribute.
        libxml2mod.xmlSetNsProp(node, None, localName, value)

def Node_setAttribute(node, name, value):
    name, value = map(from_unicode, [name, value])

    libxml2mod.xmlSetProp(node, name, value)

def Node_setAttributeNodeNS(node, attr):
    # NOTE: Not actually putting the node on the element.
    Node_setAttributeNS(node, Node_namespaceURI(attr), Node_nodeName(attr), Node_nodeValue(attr))

def Node_setAttributeNode(node, attr):
    # NOTE: Not actually putting the node on the element.
    Node_setAttribute(node, Node_nodeName(attr), Node_nodeValue(attr))

def Node_removeAttributeNS(node, ns, localName):
    attr = Node_getAttributeNodeNS(node, ns, localName)
    libxml2mod.xmlUnsetNsProp(node, libxml2mod.xmlNodeGetNs(attr), libxml2mod.name(attr))

def Node_removeAttribute(node, name):
    name = from_unicode(name)
    libxml2mod.xmlUnsetProp(node, name)

def Node_createElementNS(node, ns, name):
    ns, name = map(from_unicode, [ns, name])

    prefix, localName = _get_prefix_and_localName(name)
    new_node = libxml2mod.xmlNewNode(localName)

    # If the namespace is not empty, set the declaration.
    if ns is not None:
        new_ns = _find_namespace(new_node, ns, prefix)
        if new_ns is None:
            new_ns = _make_namespace(new_node, ns, prefix, set_default=1)
        libxml2mod.xmlSetNs(new_node, new_ns)
    # If the namespace is empty, set a "null" declaration.
    elif prefix is not None:
        new_ns = _find_namespace(new_node, "", prefix)
        if new_ns is None:
            new_ns = _make_namespace(new_node, "", prefix)
        libxml2mod.xmlSetNs(new_node, new_ns)
    else:
        libxml2mod.xmlSetNs(new_node, None)
        Node_setAttribute(new_node, "xmlns", "")
    return new_node

def Node_createElement(node, name):
    name = from_unicode(name)

    new_node = libxml2mod.xmlNewNode(name)
    return new_node

def Node_createAttributeNS(node, ns, name):
    ns, name = map(from_unicode, [ns, name])

    prefix, localName = _get_prefix_and_localName(name)
    # NOTE: Does it make sense to set the namespace if it is empty?
    if ns is not None:
        new_ns = _find_namespace(node, ns, prefix)
        if new_ns is None:
            new_ns = _make_namespace(node, ns, prefix, set_default=0)
    else:
        new_ns = None
    new_node = libxml2mod.xmlNewNsProp(node, new_ns, localName, None)
    return new_node

def Node_createAttribute(node, name):
    name = from_unicode(name)

    # NOTE: xmlNewProp does not seem to work.
    return Node_createAttributeNS(node, None, name)

def Node_createTextNode(node, value):
    value = from_unicode(value)

    return libxml2mod.xmlNewText(value)

def Node_createComment(node, value):
    value = from_unicode(value)

    return libxml2mod.xmlNewComment(value)

def Node_createCDATASection(node, value):
    value = from_unicode(value)

    return libxml2mod.xmlNewCDataBlock(Node_ownerDocument(node), value, len(value))

def Node_insertBefore(node, tmp, oldNode):
    return libxml2mod.xmlAddPrevSibling(oldNode, tmp)

def Node_replaceChild(node, tmp, oldNode):
    return libxml2mod.xmlReplaceNode(oldNode, tmp)

def Node_appendChild(node, tmp):
    return libxml2mod.xmlAddChild(node, tmp)

def Node_removeChild(node, child):
    libxml2mod.xmlUnlinkNode(child)

def Node_importNode(node, other, deep):
    if Node_nodeType(other) == xml.dom.Node.ELEMENT_NODE:
        imported_element = Node_createElementNS(node, Node_namespaceURI(other), Node_tagName(other))
        for attr in Node_attributes(other).values():
            Node_setAttributeNS(imported_element, Node_namespaceURI(attr), Node_nodeName(attr), Node_nodeValue(attr))

        if deep:
            for child in Node_childNodes(other):
                imported_child = Node_importNode(node, child, deep)
                if imported_child:
                    Node_appendChild(imported_element, imported_child)

        return imported_element

    elif Node_nodeType(other) == xml.dom.Node.TEXT_NODE:
        return Node_createTextNode(node, Node_nodeValue(other))

    elif Node_nodeType(other) == xml.dom.Node.COMMENT_NODE:
        return Node_createComment(node, Node_data(other))

    elif Node_nodeType(other) == xml.dom.Node.CDATA_SECTION_NODE:
        return Node_createCDATASection(node, Node_data(other))

    raise xml.dom.NotSupportedErr("Node type '%s' (%d) not supported." % (other, Node_nodeType(other)))

def Node_importNode_DOM(node, other, deep):
    if other.nodeType == xml.dom.Node.ELEMENT_NODE:
        imported_element = Node_createElementNS(node, other.namespaceURI, other.tagName)
        for attr in other.attributes.values():
            Node_setAttributeNS(imported_element, attr.namespaceURI, attr.nodeName, attr.nodeValue)

        if deep:
            for child in other.childNodes:
                imported_child = Node_importNode_DOM(node, child, deep)
                if imported_child:
                    Node_appendChild(imported_element, imported_child)

        return imported_element

    elif other.nodeType == xml.dom.Node.TEXT_NODE:
        return Node_createTextNode(node, other.nodeValue)

    elif other.nodeType == xml.dom.Node.COMMENT_NODE:
        return Node_createComment(node, other.data)

    elif other.nodeType == xml.dom.Node.CDATA_SECTION_NODE:
        return Node_createCDATASection(node, other.data)

    raise xml.dom.NotSupportedErr(
        "Node type '%s' (%d) not supported." % (_reverseNodeTypes[other.nodeType], other.nodeType)
        )

def Node_getElementById(doc, identifier):
    node = libxml2mod.xmlGetID(doc, identifier)
    if node is None:
        return None
    else:
        return Node_parentNode(node)

def Node_xpath(node, expr, variables=None, namespaces=None):
    expr = from_unicode(expr)

    context = libxml2mod.xmlXPathNewContext(Node_ownerDocument(node) or node)
    libxml2mod.xmlXPathSetContextNode(context, node)
    # NOTE: Discover namespaces from the node.
    # NOTE: Work out how to specify paths without having to use prefixes on
    # NOTE: names all the time.
    for prefix, ns in (namespaces or {}).items():
        libxml2mod.xmlXPathRegisterNs(context, prefix, ns)
    # NOTE: No such functions are exposed in current versions of libxml2.
    #for (prefix, ns), value in (variables or {}).items():
    #    value = from_unicode(value)
    #    libxml2mod.xmlXPathRegisterVariableNS(context, prefix, ns, value)
    result = libxml2mod.xmlXPathEval(expr, context)
    libxml2mod.xmlXPathFreeContext(context)
    return result

def Node_xinclude(node):
    result = libxml2mod.xmlXIncludeProcessFlags(node, XML_PARSE_NOERROR | XML_PARSE_NOWARNING | XML_PARSE_NONET)
    if result == -1:
        raise XIncludeException()
    else:
        return result

# Exceptions.

class LSException(Exception):

    "DOM Level 3 Load/Save exception."

    PARSE_ERR = 81
    SERIALIZE_ERR = 82

    def __repr__(self):
        exctype, excdata = self.args[0:2]
        return "LSException(%d, %r)" % (exctype, excdata)

    def __str__(self):
        exctype, excdata = self.args[0:2]
        if exctype == self.PARSE_ERR:
            return "Parse error: %s" % excdata
        elif exctype == self.SERIALIZE_ERR:
            return "Serialize error: %s" % excdata
        else:
            return repr(self)

class XIncludeException(Exception):

    "Unstandardised XInclude exception."

    pass

# Utility functions.

def createDocument(namespaceURI, localName, doctype):
    # NOTE: Fixed to use version 1.0 only.
    d = libxml2mod.xmlNewDoc("1.0")
    if localName is not None:
        # NOTE: Verify that this is always what should occur.
        root = Node_createElementNS(d, namespaceURI, localName)
        Node_appendChild(d, root)
    if doctype is not None:
        libxml2mod.xmlCreateIntSubset(d, doctype.localName, doctype.publicId, doctype.systemId)
    return d

def parse(stream_or_string, html=0, htmlencoding=None, unfinished=0):
    if hasattr(stream_or_string, "read"):
        stream = stream_or_string
        return parseString(stream.read(), html=html, htmlencoding=htmlencoding, unfinished=unfinished)
    else:
        return parseFile(stream_or_string, html=html, htmlencoding=htmlencoding, unfinished=unfinished)

def parseFile(s, html=0, htmlencoding=None, unfinished=0, validate=0, remote=0):
    if not html:
        context = libxml2mod.xmlCreateFileParserCtxt(s)
        return _parseXML(context, unfinished, validate, remote)
    else:
        return libxml2mod.htmlReadFile(s, htmlencoding,
            HTML_PARSE_NOERROR | HTML_PARSE_NOWARNING | html_net_flag(remote))

def parseString(s, html=0, htmlencoding=None, unfinished=0, validate=0, remote=0):
    if not html:
        context = libxml2mod.xmlCreateMemoryParserCtxt(s, len(s))
        return _parseXML(context, unfinished, validate, remote)
    else:
        # NOTE: URL given as None.
        html_url = None
        return libxml2mod.htmlReadMemory(s, len(s), html_url, htmlencoding,
            HTML_PARSE_NOERROR | HTML_PARSE_NOWARNING | html_net_flag(remote))

def parseURI(uri, html=0, htmlencoding=None, unfinished=0, validate=0, remote=0):
    if not html:
        context = libxml2mod.xmlCreateURLParserCtxt(uri, 0)
        return _parseXML(context, unfinished, validate, remote)
    else:
        raise NotImplementedError, "parseURI does not yet support HTML"

def _parseXML(context, unfinished, validate, remote):
    if context is None:
        raise LSException(LSException.PARSE_ERR, DOMError(DOMError.SEVERITY_FATAL_ERROR))

    Parser_configure(context, validate, remote)
    Parser_parse(context)
    doc = Parser_document(context)
    error = Parser_error()

    try:
        if validate and not Parser_valid(context):

            # NOTE: May not be the correct exception.

            raise LSException(
                LSException.PARSE_ERR,
                DOMError(
                    DOMError.SEVERITY_FATAL_ERROR,
                    get_parse_error_message() or "Document did not validate"
                    ))

        elif unfinished and (error is None or Parser_errorCode(error) == XML_ERR_TAG_NOT_FINISHED):

            # NOTE: There may be other unfinished conditions.

            return doc

        elif error is not None and Parser_errorLevel(error) == XML_ERR_FATAL:
            raise LSException(
                LSException.PARSE_ERR,
                DOMError(
                    DOMError.SEVERITY_FATAL_ERROR,
                    get_parse_error_message() or "Document caused fatal error"
                    ))

        else:

            # NOTE: Could provide non-fatal errors or warnings.

            return doc

    finally:
        Parser_resetError(error)
        libxml2mod.xmlFreeParserCtxt(context)

def toString(node, encoding=None, prettyprint=0):
    return libxml2mod.serializeNode(node, encoding, prettyprint)

def toStream(node, stream, encoding=None, prettyprint=0):
    stream.write(toString(node, encoding, prettyprint))

def toFile(node, f, encoding=None, prettyprint=0):
    libxml2mod.saveNodeTo(node, f, encoding, prettyprint)

# libxml2mod constants and helper functions.

HTML_PARSE_NOERROR = 32
HTML_PARSE_NOWARNING = 64
HTML_PARSE_NONET = 2048
XML_PARSE_DTDVALID = 16
XML_PARSE_NOERROR = 32
XML_PARSE_NOWARNING = 64
XML_PARSE_NONET = 2048

XML_ERR_NONE = 0
XML_ERR_WARNING = 1
XML_ERR_ERROR = 2
XML_ERR_FATAL = 3

XML_ERR_TAG_NOT_FINISHED = 77

def html_net_flag(remote):
    if remote:
        return 0
    else:
        return HTML_PARSE_NONET

def xml_net_flag(remote):
    if remote:
        return 0
    else:
        return XML_PARSE_NONET

def xml_validate_flag(validate):
    if validate:
        return XML_PARSE_DTDVALID
    else:
        return 0

def get_parse_error_message():
    error = Parser_error()
    if error is not None:
        filename = libxml2mod.xmlErrorGetFile(error)
        if filename is None:
            filename = "<string>"
        else:
            filename = repr(filename)
        line = libxml2mod.xmlErrorGetLine(error)
        error_message = libxml2mod.xmlErrorGetMessage(error).strip()
        return "Filename %s, line %d: %s" % (filename, line, error_message)
    else:
        return None

def Parser_error():
    return libxml2mod.xmlGetLastError()

def Parser_resetError(error):
    if error is None:
        return libxml2mod.xmlResetLastError()
    else:
        return libxml2mod.xmlResetError(error)

def Parser_errorLevel(error):
    return libxml2mod.xmlErrorGetLevel(error)

def Parser_errorCode(error):
    return libxml2mod.xmlErrorGetCode(error)

def Parser_push():
    return libxml2mod.xmlCreatePushParser(None, "", 0, None)

def Parser_configure(context, validate, remote):
    libxml2mod.xmlParserSetPedantic(context, 0)
    #libxml2mod.xmlParserSetValidate(context, validate)
    libxml2mod.xmlCtxtUseOptions(context,
        XML_PARSE_NOERROR | XML_PARSE_NOWARNING | xml_net_flag(remote) | xml_validate_flag(validate))

def Parser_feed(context, s):
    libxml2mod.xmlParseChunk(context, s, len(s), 1)

def Parser_well_formed(context):
    return libxml2mod.xmlParserGetWellFormed(context)

def Parser_valid(context):
    return libxml2mod.xmlParserGetIsValid(context)

def Parser_document(context):
    return libxml2mod.xmlParserGetDoc(context)

def Parser_parse(context):
    libxml2mod.xmlParseDocument(context)

# Schema and validation helper functions and classes.
# NOTE: Should potentially combine these with other definitions.

RELAXNG_NS = "http://relaxng.org/ns/structure/1.0"
SCHEMATRON_NS = "http://purl.oclc.org/dsdl/schematron"
XMLSCHEMA_NS = "http://www.w3.org/2001/XMLSchema"

def Document_schema(doc, namespaceURI):
    if namespaceURI == RELAXNG_NS:
        return Schema_parseRelaxNG(libxml2mod.xmlRelaxNGNewDocParserCtxt(doc))
    elif namespaceURI == SCHEMATRON_NS:
        return Schema_parseSchematron(libxml2mod.xmlSchematronNewDocParserCtxt(doc))
    elif namespaceURI == XMLSCHEMA_NS:
        return Schema_parseSchema(libxml2mod.xmlSchemaNewDocParserCtxt(doc))
    else:
        return None

def Document_schemaFromString(s, namespaceURI):
    if namespaceURI == RELAXNG_NS:
        return Schema_parseRelaxNG(libxml2mod.xmlRelaxNGNewMemParserCtxt(s, len(s)))
    elif namespaceURI == SCHEMATRON_NS:
        return Schema_parseSchematron(libxml2mod.xmlSchematronNewMemParserCtxt(s, len(s)))
    elif namespaceURI == XMLSCHEMA_NS:
        return Schema_parseSchema(libxml2mod.xmlSchemaNewMemParserCtxt(s, len(s)))
    else:
        return None

def Document_validate(schema, doc, error_handler, namespaceURI):
    if namespaceURI == RELAXNG_NS:
        return Document_validateRelaxNG(schema, doc, error_handler)
    elif namespaceURI == SCHEMATRON_NS:
        return Document_validateSchematron(schema, doc, error_handler)
    elif namespaceURI == XMLSCHEMA_NS:
        return Document_validateSchema(schema, doc, error_handler)
    else:
        return 0

def Document_validateRelaxNG(schema, doc, error_handler):
    validator_context = libxml2mod.xmlRelaxNGNewValidCtxt(schema)
    handler = ValidationHandler(error_handler)
    libxml2mod.xmlRelaxNGSetValidErrors(validator_context, handler.error, handler.warning, None)
    try:
        status = libxml2mod.xmlRelaxNGValidateDoc(validator_context, doc)
        return status == 0
    finally:
        libxml2mod.xmlRelaxNGFreeValidCtxt(validator_context)

def Document_validateSchematron(schema, doc, error_handler):
    validator_context = libxml2mod.xmlSchematronNewValidCtxt(schema)
    handler = ValidationHandler(error_handler)
    libxml2mod.xmlSchematronSetValidErrors(validator_context, handler.error, handler.warning, None)
    try:
        status = libxml2mod.xmlSchematronValidateDoc(validator_context, doc)
        return status == 0
    finally:
        libxml2mod.xmlSchematronFreeValidCtxt(validator_context)

def Document_validateSchema(schema, doc, error_handler):
    validator_context = libxml2mod.xmlSchemaNewValidCtxt(schema)
    handler = ValidationHandler(error_handler)
    libxml2mod.xmlSchemaSetValidErrors(validator_context, handler.error, handler.warning, None)
    try:
        status = libxml2mod.xmlSchemaValidateDoc(validator_context, doc)
        return status == 0
    finally:
        libxml2mod.xmlSchemaFreeValidCtxt(validator_context)

def Schema_parseRelaxNG(context):
    try:
        return libxml2mod.xmlRelaxNGParse(context)
    finally:
        libxml2mod.xmlRelaxNGFreeParserCtxt(context)

def Schema_parseSchematron(context):
    try:
        return libxml2mod.xmlSchematronParse(context)
    finally:
        libxml2mod.xmlSchematronFreeParserCtxt(context)

def Schema_parseSchema(context):
    try:
        return libxml2mod.xmlSchemaParse(context)
    finally:
        libxml2mod.xmlSchemaFreeParserCtxt(context)

def Schema_free(schema, namespaceURI):
    if namespaceURI == RELAXNG_NS:
        libxml2mod.xmlRelaxNGFree(schema)
    elif namespaceURI == SCHEMATRON_NS:
        libxml2mod.xmlSchematronFree(schema)
    elif namespaceURI == XMLSCHEMA_NS:
        libxml2mod.xmlSchemaFree(schema)

class ValidationHandler:

    """
    A handler which collects validation errors and warnings and passes them to a
    DOMErrorHandler.
    """

    def __init__(self, error_handler):
        self.error_handler = error_handler

    def error(self, msg, arg):
        self.error_handler.handleError(DOMError(DOMError.SEVERITY_FATAL_ERROR, msg.strip()))

    def warning(self, msg, arg):
        self.error_handler.handleError(DOMError(DOMError.SEVERITY_WARNING, msg.strip()))

# vim: tabstop=4 expandtab shiftwidth=4
