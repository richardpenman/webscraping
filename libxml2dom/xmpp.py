#!/usr/bin/env python

"""
XMPP support using libxml2dom to capture stanzas as documents. The XMPP
specification employs an "open" or unfinished document as the basis for
communications between client and server - this presents problems for
DOM-oriented libraries.

Various Internet standards specifications exist for XMPP.
See: http://www.xmpp.org/rfcs/rfc3920.html
See: http://www.xmpp.org/rfcs/rfc3921.html

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

The process of connecting, authenticating, and so on is quite convoluted:

s = libxml2dom.xmpp.Session(("localhost", 5222))
d = s.connect("host")
auth = s.createAuth()                        # provides access to the stanza
auth.mechanism = "PLAIN"                     # choose a supported mechanism
auth.setCredentials(jid, username, password) # for PLAIN authentication only
d = s.send(auth)                             # hopefully a success response
d = s.connect("host")                        # have to reconnect!
iq = s.createIq()                            # make an 'iq' stanza
iq.makeBind()                                # set up a binding operation
d = s.send(iq)                               # hopefully a success response
iq = s.createIq()                            # make an 'iq' stanza
iq.makeSession()                             # set up a session
d = s.send(iq)                               # hopefully a success response

See tests/xmpp_test.py for more details.
"""

import libxml2dom
from libxml2dom.macrolib import *
from libxml2dom.macrolib import \
    createDocument as Node_createDocument
import socket
import select
import base64 # for auth elements

# XMPP-related namespaces.

XMPP_BIND_NAMESPACE = "urn:ietf:params:xml:ns:xmpp-bind"
XMPP_CLIENT_NAMESPACE = "jabber:client"
XEP_0022_EVENT_NAMESPACE = "jabber:x:event"
XMPP_REGISTER_NAMESPACE = "jabber:iq:register"
XMPP_SASL_NAMESPACE = "urn:ietf:params:xml:ns:xmpp-sasl"
XMPP_SESSION_NAMESPACE = "urn:ietf:params:xml:ns:xmpp-session"
XMPP_STREAMS_NAMESPACE = "http://etherx.jabber.org/streams"

# Default namespace bindings for XPath.

default_ns = {
    "bind" : XMPP_BIND_NAMESPACE,
    "client" : XMPP_CLIENT_NAMESPACE,
    "event": XEP_0022_EVENT_NAMESPACE,
    "register" : XMPP_REGISTER_NAMESPACE,
    "sasl" : XMPP_SASL_NAMESPACE,
    "session" : XMPP_SESSION_NAMESPACE,
    "stream" : XMPP_STREAMS_NAMESPACE
    }

class XMPPImplementation(libxml2dom.Implementation):

    "Contains an XMPP-specific implementation."

    # Wrapping of documents.

    def adoptDocument(self, node):
        return XMPPDocument(node, self)

    # Factory functions.

    def get_node(self, _node, context_node):

        """
        Get a libxml2dom node for the given low-level '_node' and libxml2dom
        'context_node'.
        """

        if Node_nodeType(_node) == context_node.ELEMENT_NODE:

            # Make special binding elements.

            if Node_namespaceURI(_node) == XMPP_BIND_NAMESPACE:
                if Node_localName(_node) == "bind":
                    return XMPPBindElement(_node, self, context_node.ownerDocument)

            # Make special client elements.

            elif Node_namespaceURI(_node) == XMPP_CLIENT_NAMESPACE:
                if Node_localName(_node) == "iq":
                    return XMPPIqElement(_node, self, context_node.ownerDocument)
                elif Node_localName(_node) == "message":
                    return XMPPMessageElement(_node, self, context_node.ownerDocument)
                elif Node_localName(_node) == "presence":
                    return XMPPPresenceElement(_node, self, context_node.ownerDocument)
                else:
                    return XMPPClientElement(_node, self, context_node.ownerDocument)

            # Make special event elements.

            elif Node_namespaceURI(_node) == XEP_0022_EVENT_NAMESPACE:
                return XEP0022EventElement(_node, self, context_node.ownerDocument)

            # Make special registration elements.

            elif Node_namespaceURI(_node) == XMPP_REGISTER_NAMESPACE:
                return XMPPRegisterElement(_node, self, context_node.ownerDocument)

            # Make special authentication elements.

            elif Node_namespaceURI(_node) == XMPP_SASL_NAMESPACE:
                if Node_localName(_node) == "auth":
                    return XMPPAuthElement(_node, self, context_node.ownerDocument)

            # Make special stream elements.

            elif Node_namespaceURI(_node) == XMPP_STREAMS_NAMESPACE:
                if Node_localName(_node) == "stream":
                    return XMPPStreamElement(_node, self, context_node.ownerDocument)

            # Otherwise, make generic XMPP elements.

            return XMPPElement(_node, self, context_node.ownerDocument)

        else:
            return libxml2dom.Implementation.get_node(self, _node, context_node)

    # Convenience functions.

    def createXMPPStanza(self, namespaceURI, localName):

        "Create a new XMPP stanza document (fragment)."

        return XMPPDocument(Node_createDocument(namespaceURI, localName, None), self).documentElement

# Node classes.

class XMPPNode(libxml2dom.Node):

    "Convenience modifications to nodes specific to libxml2dom.xmpp."

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

class XMPPDocument(libxml2dom._Document, XMPPNode):

    "An XMPP document fragment."

    pass

class XMPPElement(XMPPNode):
    pass

class XMPPAuthElement(XMPPNode):

    "An XMPP auth element."

    def _mechanism(self):
        return self.getAttribute("mechanism")

    def _setMechanism(self, value):
        self.setAttribute("mechanism", value)

    def _value(self):
        return self.textContent

    def setCredentials(self, jid, username, password):

        # NOTE: This is what xmpppy does. Beware of the leopard, with respect to
        # NOTE: the specifications.

        b64value = base64.encodestring("%s\x00%s\x00%s" % (jid, username, password))
        text = self.ownerDocument.createTextNode(b64value)
        self.appendChild(text)

    mechanism = property(_mechanism, _setMechanism)
    value = property(_value)

class XMPPBindElement(XMPPNode):

    "An XMPP bind element."

    def _resource(self):
        return "".join(self.xpath("resource/text()"))

    def _setResource(self, value):
        resources = self.xpath("resource")
        for resource in resources:
            self.removeChild(resource)
        resource = self.ownerDocument.createElement("resource")
        self.appendChild(resource)
        text = self.ownerDocument.createTextNode(value)
        resource.appendChild(text)

    resource = property(_resource, _setResource)

class XMPPClientElement(XMPPNode):

    "An XMPP client element."

    def _id(self):
        return self.getAttribute("id")

    def _setId(self, value):
        self.setAttribute("id", value)

    def _delId(self):
        self.removeAttribute("id")

    def _from(self):
        return self.getAttribute("from")

    def _setFrom(self, value):
        self.setAttribute("from", value)

    def _delFrom(self):
        self.removeAttribute("from")

    def _to(self):
        return self.getAttribute("to")

    def _setTo(self, value):
        self.setAttribute("to", value)

    def _delTo(self):
        self.removeAttribute("to")

    def _type(self):
        return self.getAttribute("type")

    def _setType(self, value):
        self.setAttribute("type", value)

    def _delType(self):
        self.removeAttribute("type")

    id = property(_id, _setId, _delId)
    from_ = property(_from, _setFrom, _delFrom)
    to = property(_to, _setTo, _delTo)
    type = property(_type, _setType, _delType)

class XMPPMessageElement(XMPPClientElement):

    "An XMPP message element."

    def _event(self):
        return self.xpath(".//event:*")[0]

    def _body(self):
        return self.xpath("./client:body")[0]

    def _setBody(self, body):
        self.appendChild(body)

    def _delBody(self):
        self.removeChild(self.body)

    def createBody(self):
        return self.ownerDocument.createElementNS(XMPP_CLIENT_NAMESPACE, "body")

    body = property(_body, _setBody, _delBody)
    event = property(_event)

class XEP0022EventElement(XMPPNode):

    "An XEP-0022 event element."

    def _offline(self):
        return bool(self.xpath("./event:offline"))

    def _delivered(self):
        return bool(self.xpath("./event:delivered"))

    def _displayed(self):
        return bool(self.xpath("./event:displayed"))

    def _composing(self):
        return bool(self.xpath("./event:composing"))

    def _id(self):
        ids = self.xpath("./event:id")
        if ids:
            return ids[0].textContent
        else:
            return None

    offline = property(_offline)
    delivered = property(_delivered)
    displayed = property(_displayed)
    composing = property(_composing)
    id = property(_id)

class XMPPPresenceElement(XMPPClientElement):

    "An XMPP presence element."

    pass

class XMPPIqElement(XMPPClientElement):

    """
    An XMPP 'iq' element used in instant messaging and registration.
    See: http://www.xmpp.org/rfcs/rfc3921.html
    See: http://www.xmpp.org/extensions/xep-0077.html
    """

    def _bind(self):
        return (self.xpath("bind:bind") or [None])[0]

    def _query(self):
        return (self.xpath("register:query") or [None])[0]

    def _session(self):
        return (self.xpath("session:session") or [None])[0]

    bind = property(_bind)
    query = property(_query)
    session = property(_session)

    def createBind(self):
        return self.ownerDocument.createElementNS(XMPP_BIND_NAMESPACE, "bind")

    def createQuery(self):
        return self.ownerDocument.createElementNS(XMPP_REGISTER_NAMESPACE, "query")

    def createSession(self):
        return self.ownerDocument.createElementNS(XMPP_SESSION_NAMESPACE, "session")

    def makeBind(self):
        bind = self.createBind()
        self.appendChild(bind)
        self.id = "bind1"
        self.type = "set"

    def makeQuery(self):
        query = self.createQuery()
        self.appendChild(query)
        self.id = "register1"
        self.type = "get"

    def makeRegistration(self):
        self.id = "register2"
        self.type = "set"

    def makeSession(self, host):
        session = self.createSession()
        self.appendChild(session)
        self.id = "session1"
        self.type = "set"
        self.to = host

class XMPPRegisterElement(XMPPNode):

    """
    A registration element.
    See: http://www.xmpp.org/extensions/xep-0077.html
    """

    def __setitem__(self, name, value):
        element = self.ownerDocument.createElement(name)
        text = self.ownerDocument.createTextNode(value)
        element = self.appendChild(element)
        element.appendChild(text)

class XMPPStreamElement(XMPPNode):
    pass

# Classes providing XMPP session support.

class SessionTerminated(Exception):
    pass

class Session:

    "An XMPP session."

    connect_str = """\
<?xml version="1.0"?>
<stream:stream to='%s' xmlns='jabber:client' xmlns:stream='http://etherx.jabber.org/streams' version='1.0'>"""

    disconnect_str = """\
</stream:stream>"""

    def __init__(self, address, timeout=500, bufsize=1024, encoding="utf-8"):

        """
        Initialise an XMPP session using the given 'address': a tuple of the
        form (hostname, port). The optional 'timeout' (in milliseconds) is used
        for polling the connection for new data, and the optional 'encoding'
        specifies the character encoding employed in the communications.
        """

        self.timeout = timeout
        self.bufsize = bufsize
        self.encoding = encoding
        self.poller = select.poll()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setblocking(1)
        self.socket.connect(address)
        self.poller.register(self.socket.fileno(), select.POLLIN | select.POLLHUP | select.POLLNVAL | select.POLLERR)

    def _ready(self, timeout):

        """
        Return whether data can be read from the server, waiting as long as the
        specified 'timeout' (forever if set to None).
        """

        return self.poller.poll(timeout)

    def read(self):

        "Read as much as possible from the server."

        context = Parser_push()
        Parser_configure(context)

        have_read = 0
        fds = self._ready(self.timeout)
        try:
            while fds:
                for fd, status in fds:
                    if fd == self.socket.fileno():
                        if status & (select.POLLHUP | select.POLLNVAL | select.POLLERR):
                            raise SessionTerminated
                        if status & select.POLLIN:
                            have_read = 1
                            c = self.socket.recv(self.bufsize)
                            Parser_feed(context, c)
                            if Parser_well_formed(context):
                                return default_impl.adoptDocument(Parser_document(context))

                fds = self.poller.poll(self.timeout)

        except SessionTerminated:
            pass

        if have_read:
            return default_impl.adoptDocument(Parser_document(context))
        else:
            return None

    def write(self, s):

        "Write the plain string 's' to the server."

        self.socket.send(s)
        
    def send(self, stanza):

        """
        Send the 'stanza' to the server, returning a response stanza if an
        immediate response was provided, or None otherwise.
        """

        stanza.toStream(self, encoding=self.encoding)
        return self._receive()

    def _receive(self):

        "Return a stanza for data read from the server."

        doc = self.read()
        if doc is None:
            return doc
        else:
            return doc.documentElement

    def receive(self, timeout=None):

        """
        Wait for an incoming stanza, or as long as 'timeout' (in milliseconds),
        or forever if 'timeout' is omitted or set to None, returning either a
        stanza document (fragment) or None if nothing was received.
        """

        if self._ready(timeout):
            return self._receive()
        else:
            return None

    # Stanza creation.

    def createAuth(self):
        return self.createStanza(XMPP_SASL_NAMESPACE, "auth")

    def createIq(self):
        return self.createStanza(XMPP_CLIENT_NAMESPACE, "iq")

    def createMessage(self):
        return self.createStanza(XMPP_CLIENT_NAMESPACE, "message")

    def createPresence(self):
        return self.createStanza(XMPP_CLIENT_NAMESPACE, "presence")

    def createStanza(self, namespaceURI, localName):
        return createXMPPStanza(namespaceURI, localName)

    # High-level methods.

    def connect(self, host):

        # NOTE: Nasty sending of the raw text because it involves only a start
        # NOTE: tag.

        self.write(self.connect_str % host)
        return self._receive()

# Utility functions.

createDocument = libxml2dom.createDocument
createDocumentType = libxml2dom.createDocumentType

def createXMPPStanza(namespaceURI, localName):
    return default_impl.createXMPPStanza(namespaceURI, localName)

def parse(stream_or_string, html=0, htmlencoding=None, unfinished=0, impl=None):
    return libxml2dom.parse(stream_or_string, html=html, htmlencoding=htmlencoding, unfinished=unfinished, impl=(impl or default_impl))

def parseFile(filename, html=0, htmlencoding=None, unfinished=0, impl=None):
    return libxml2dom.parseFile(filename, html=html, htmlencoding=htmlencoding, unfinished=unfinished, impl=(impl or default_impl))

def parseString(s, html=0, htmlencoding=None, unfinished=0, impl=None):
    return libxml2dom.parseString(s, html=html, htmlencoding=htmlencoding, unfinished=unfinished, impl=(impl or default_impl))

def parseURI(uri, html=0, htmlencoding=None, unfinished=0, impl=None):
    return libxml2dom.parseURI(uri, html=html, htmlencoding=htmlencoding, unfinished=unfinished, impl=(impl or default_impl))

# Single instance of the implementation.

default_impl = XMPPImplementation()

# vim: tabstop=4 expandtab shiftwidth=4
