#!/usr/bin/env python

"""
SVG-specific document support.
See: http://www.w3.org/TR/SVGMobile12/python-binding.html
See: http://www.w3.org/TR/SVGMobile12/svgudom.html

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
"""

import libxml2dom
from libxml2dom.events import *
from libxml2dom.macrolib import *
from libxml2dom.macrolib import \
    createDocument as Node_createDocument
import xml.dom
import urllib
import math
import re

SVG_NAMESPACE = "http://www.w3.org/2000/svg"

class _Exception(Exception):

    "A generic SVG exception."

    def __init__(self, code):
        Exception.__init__(self, code)
        self.code = code

class SVGException(_Exception):

    "An SVG exception."

    SVG_WRONG_TYPE_ERR = 0
    SVG_INVALID_VALUE_ERR = 1
    SVG_MATRIX_NOT_INVERTABLE = 2

class GlobalException(_Exception):

    "A global exception."

    NOT_CONNECTED_ERR = 1
    ENCODING_ERR = 2
    DENIED_ERR = 3
    UNKNOWN_ERR = 4

class SVGImplementation(libxml2dom.Implementation):

    "Contains an SVG-specific implementation."

    # Wrapping of documents.

    def adoptDocument(self, node):
        return SVGDocument(node, self)

    # Factory functions.

    def get_node(self, _node, context_node):
        if Node_nodeType(_node) == context_node.ELEMENT_NODE and \
            Node_namespaceURI(_node) == SVG_NAMESPACE:

            if Node_localName(_node) == "svg":
                return SVGSVGElement(_node, self, context_node.ownerDocument)
            else:
                return SVGElement(_node, self, context_node.ownerDocument)
        else:
            return libxml2dom.Implementation.get_node(self, _node, context_node)

    def get_global(self, doc):
        return SVGGlobal(doc)

    # Convenience functions.

    def createSVGDocument(self):

        "Create a new SVG document."

        return SVGDocument(Node_createDocument(SVG_NAMESPACE, "svg", None), self)

# Interfaces and helper classes.

class AsyncStatusCallback:

    "An asynchronous callback interface."

    def operationComplete(self, status):
        pass

class AsyncURLStatus:

    "The status of a URL retrieval operation."

    def __init__(self, success, contentType, content):
        self.success, self.contentType, self.content = success, contentType, content

class ElementTraversal:

    "An interface for element traversal."

    def _firstElementChild(self):
        l = self.xpath("*")
        if l:
            return l[0]
        else:
            return None

    def _lastElementChild(self):
        l = self.xpath("*")
        if l:
            return l[-1]
        else:
            return None

    def _nextElementSibling(self):
        l = self.xpath("following-sibling::*")
        if l:
            return l[0]
        else:
            return None

    def _previousElementSibling(self):
        l = self.xpath("preceding-sibling::*")
        if l:
            return l[0]
        else:
            return None

    firstElementChild = property(_firstElementChild)
    lastElementChild = property(_lastElementChild)
    nextElementSibling = property(_nextElementSibling)
    previousElementSibling = property(_previousElementSibling)

class EventListenerInitializer2:

    "An event listener initialisation interface."

    def initializeEventListeners(self, scriptElement):
        pass

    def createEventListener(self, handlerElement):
        pass

class Global:

    "An empty global interface."

    pass

class SVGGlobal(Global, EventListenerInitializer2):

    "An SVG global."

    def __init__(self, document): # parent

        "Initialise the global with the given 'document'."

        self.document = document

        # Listener management.

        self.listeners = {}

    def createConnection(self):
        raise NotImplementedError, "createConnection"

    def createTimer(self, initialInterval, repeatInterval):
        raise NotImplementedError, "createTimer"

    def gotoLocation(self, newIRI):
        raise NotImplementedError, "gotoLocation"

    def binaryToString(self, octets, encoding):
        try:
            return unicode(octets, encoding)
        except UnicodeDecodeError, exc:
            raise GlobalException(GlobalException.ENCODING_ERR)

    def stringToBinary(self, data, encoding):
        try:
            return data.encode(encoding)
        except UnicodeEncodeError, exc:
            raise GlobalException(GlobalException.ENCODING_ERR)

    def getURL(self, iri, callback):

        # NOTE: Not asynchronous.
        # NOTE: The urlopen function may not support IRIs.
        # No exceptions are supposed to be raised, which is a bit nasty.

        f = urllib.urlopen(iri)
        try:
            try:
                content = f.read()
                contentType = f.headers["Content-Type"]
                callback.operationComplete(AsyncURLStatus(1, contentType, content))
            except:
                callback.operationComplete(AsyncURLStatus(0, None, None))
        finally:
            f.close()

    def postURL(self, iri, data, callback, type=None, encoding=None):

        # NOTE: Not asynchronous.
        # NOTE: The urlopen function may not support IRIs.
        # No exceptions are supposed to be raised, which is a bit nasty.

        opener = urllib.URLopener()
        opener.addheader("Content-Type", type or "text/plain")
        if encoding:
            opener.addheader("Content-Encoding", encoding)
        f = opener.open(iri, data)
        try:
            try:
                content = f.read()
                contentType = f.headers["Content-Type"]
                callback.operationComplete(AsyncURLStatus(1, contentType, content))
            except:
                callback.operationComplete(AsyncURLStatus(0, None, None))
        finally:
            f.close()
            opener.close()

    def parseXML(self, data, contextDoc):
        doc = parseString(data)
        return contextDoc.importNode(doc.documentElement, 1)

class SVGLocatable:

    "A locatable interface."

    pass

class SVGMatrix:

    """
    A matrix.
    See: http://www.w3.org/TR/SVGMobile12/svgudom.html#svg__SVGMatrix
    """

    translate_regexp = re.compile("translate\((.*)\)$")
    scale_regexp = re.compile("scale\((.*)\)$")
    rotate_regexp = re.compile("rotate\((.*)\)$")
    skewX_regexp = re.compile("skewX\((.*)\)$")
    skewY_regexp = re.compile("skewY\((.*)\)$")
    matrix_regexp = re.compile("matrix\((.*)\)$")

    def __init__(self, a=0, b=0, c=0, d=0, e=0, f=0):
        self.matrix = a, b, c, d, e, f

    def __repr__(self):
        return "SVGMatrix(%f, %f, %f, %f, %f, %f)" % self.matrix

    def __eq__(self, other):
        return self.matrix == other.matrix

    def __ne__(self, other):
        return not (self == other)

    def _get_params(self, param_string):
        return map(float, map(lambda s: s.strip(), param_string.split(",")))

    def fromNode(self, node, name):

        """
        Initialise this object from the trait on the 'node' having the given
        'name'.
        """

        value = node.getAttribute(name)
        if value is None:
            raise xml.dom.DOMException(xml.dom.NOT_SUPPORTED_ERR)

        value = value.strip()

        # Translation.

        m = self.translate_regexp.match(value)
        if m:
            a, b, c, d = 1, 0, 0, 1
            e, f = self._get_params(m.group(1))
            self.matrix = a, b, c, d, e, f
            return

        # Scaling.

        m = self.scale_regexp.match(value)
        if m:
            b, c, e, f = 0, 0, 0, 0
            a, d = self._get_params(m.group(1))
            self.matrix = a, b, c, d, e, f
            return

        # Rotation.

        m = self.rotate_regexp.match(value)
        if m:
            e, f = 0, 0
            angle = float(m.group(1).strip())
            a = d = math.cos(math.radians(angle))
            b = math.sin(math.radians(angle))
            c = -b
            self.matrix = a, b, c, d, e, f
            return

        # Skew.

        m = self.skewX_regexp.match(value)
        if m:
            a, b, d, e, f = 1, 0, 1, 0, 0
            angle = float(m.group(1).strip())
            c = math.tan(math.radians(angle))
            self.matrix = a, b, c, d, e, f
            return

        m = self.skewY_regexp.match(value)
        if m:
            a, c, d, e, f = 1, 0, 1, 0, 0
            angle = float(m.group(1).strip())
            b = math.tan(math.radians(angle))
            self.matrix = a, b, c, d, e, f
            return

        # Generic.

        m = self.matrix_regexp.match(value)
        if m:
            self.matrix = self._get_params(m.group(1))
            return

        # Otherwise, complain.

        raise xml.dom.DOMException(xml.dom.TYPE_MISMATCH_ERR)

    def toNode(self, node, name):

        """
        Set the trait on the given 'node' using the given 'name' according to
        this object's attributes.
        """

        a, b, c, d, e, f = self.matrix

        # Translation.

        if (a, b, c, d) == (1, 0, 0, 1):
            node.setAttribute(name, "translate(%f, %f)" % (e, f))

        # Scaling.

        elif (b, c, e, f) == (0, 0, 0, 0):
            node.setAttribute(name, "scale(%f, %f)" % (a, d))

        # Rotation.

        elif a == d and b == -c and (e, f) == (0, 0) and math.degrees(math.acos(a)) == math.degrees(math.asin(b)):
            node.setAttribute(name, "rotate(%f)" % math.degrees(math.acos(a)))

        # Skew.

        elif (a, b, d, e, f) == (1, 0, 1, 0, 0) and c != 0:
            node.setAttribute(name, "skewX(%f)" % math.degrees(math.atan(c)))

        elif (a, c, d, e, f) == (1, 0, 1, 0, 0) and b != 0:
            node.setAttribute(name, "skewX(%f)" % math.degrees(math.atan(b)))

        # Generic matrix.

        else:
            node.setAttribute(name, "matrix(%f, %f, %f, %f, %f, %f)" % (a, b, c, d, e, f))

    def getComponent(self, index):

        """
        Return the component with the given 'index' (starting at zero) from the
        sequence a, b, c, d, e, f where each element corresponds to the matrix
        as follows:

        [ a c e ]
        [ b d f ]
        [ 0 0 1 ]
        """

        try:
            return self.matrix[index]
        except IndexError:
            raise xml.dom.DOMException(xml.dom.INDEX_SIZE_ERR)

    def mMultiply(self, secondMatrix):

        """
        Post-multiply this matrix with 'secondMatrix' and update its contents to
        the result of the multiplication operation defined as follows:

        [ A C E ] [ a c e ]
        [ B D F ] [ b d f ]
        [ 0 0 1 ] [ 0 0 1 ]

        Return this object as a result.
        """

        a, b, c, d, e, f = self.matrix
        A, B, C, D, E, F = secondMatrix.matrix
        self.matrix = A*a + C*b, B*a + D*b, A*c + C*d, B*c + D*d, A*e + C*f + E, B*e + D*f + F
        return self

    def inverse(self):

        """
        det = ad - cb

        See (for example): http://mathworld.wolfram.com/MatrixInverse.html
        """

        det = a*d - c*b
        if det != 0:
            m = 1/det
            a, b, c, d, e, f = self.matrix
            self.matrix = m * d, m * -b, m * -c, m * a, m * (c*f - e*d), m * (e*b - a*f)
            return self
        else:
            raise SVGException(SVGException.SVG_MATRIX_NOT_INVERTABLE)

    def mTranslate(self, x, y):

        """
        [ 1 0 x ]
        [ 0 1 y ]
        [ 0 0 1 ]
        """

        return self.mMultiply(SVGMatrix(1, 0, 0, 1, x, y))

    def mScale(self, scaleFactor):

        """
        [ scaleFactor 0           0 ]
        [ 0           scaleFactor 0 ]
        [ 0           0           1 ]
        """

        return self.mMultiply(SVGMatrix(scaleFactor, 0, 0, scaleFactor, 0, 0))

    def mRotate(self, angle):

        """
        [ cos(angle) -sin(angle) 0 ]
        [ sin(angle) cos(angle)  0 ]
        [ 0          0           1 ]
        """

        return self.mMultiply(
            SVGMatrix(
                math.cos(math.radians(angle)),
                math.sin(math.radians(angle)),
                -math.sin(math.radians(angle)),
                math.cos(math.radians(angle)),
                0, 0
                )
            )

class SVGPath:

    """
    A path.
    See: http://www.w3.org/TR/SVGMobile12/svgudom.html#svg__SVGPath
    See: http://www.w3.org/TR/SVGMobile12/paths.html
    """

    MOVE_TO = 77
    LINE_TO = 76
    CURVE_TO = 67
    QUAD_TO = 81
    CLOSE = 90
    _CLOSE = 122 # More baggage (name not standard).

    nparams = {
        MOVE_TO : 2,
        LINE_TO : 2,
        CURVE_TO : 6,
        QUAD_TO : 4,
        CLOSE : 0,
        _CLOSE : 0
        }

    def __init__(self):
        self.segments = []

    def __eq__(self, other):
        return self.segments == other.segments

    def __ne__(self, other):
        return not (self == other)

    def fromNode(self, node, name):

        """
        Initialise this object from the trait on the 'node' having the given
        'name'.
        """

        value = node.getAttribute(name)
        if value is None:
            raise xml.dom.DOMException(xml.dom.NOT_SUPPORTED_ERR)

        # Try and unpack the attribute value.

        data = value.split()
        self.segments = []
        try:
            i = 0
            while i < len(data):
                cmd = ord(data[i])
                if cmd == self._CLOSE:
                    cmd = self.CLOSE
                i += 1
                n = self.nparams[cmd]
                params = map(float, data[i:i+n])
                self.segments.append((cmd, params))
                i += n
        except (IndexError, ValueError):
            raise xml.dom.DOMException(xml.dom.TYPE_MISMATCH_ERR)

    def toNode(self, node, name):

        """
        Set the trait on the given 'node' using the given 'name' according to
        this object's attributes.
        """

        try:
            l = []
            for cmd, params in self.segments:
                l.append(unichr(cmd))
                for param in params:
                    l.append(str(param))
            node.setAttribute(name, " ".join(l))
        except (IndexError, ValueError):
            raise xml.dom.DOMException(xml.dom.TYPE_MISMATCH_ERR)

    # Interface methods.

    def _numberOfSegments(self):
        return len(self.segments)

    numberOfSegments = property(_numberOfSegments)

    def getSegment(self, cmdIndex):
        try:
            return self.segments[cmdIndex][0]
        except IndexError:
            raise xml.dom.DOMException(xml.dom.INDEX_SIZE_ERR)

    def getSegmentParam(self, cmdIndex, paramIndex):
        try:
            return self.segments[cmdIndex][1][paramIndex]
        except IndexError:
            raise xml.dom.DOMException(xml.dom.INDEX_SIZE_ERR)

    def moveTo(self, x, y):
        self.segments.append((self.MOVE_TO, (x, y)))

    def lineTo(self, x, y):
        self.segments.append((self.LINE_TO, (x, y)))

    def quadTo(self, x1, y1, x2, y2):
        self.segments.append((self.QUAD_TO, (x1, y1, x2, y2)))

    def curveTo(self, x1, y1, x2, y2, x3, y3):
        self.segments.append((self.CURVE_TO, (x1, y1, x2, y2, x3, y3)))

    def close(self):
        self.segments.append((self.CLOSE,))

class SVGPoint:

    "A point used to provide currentTranslate."

    def __init__(self, x, y):
        self.x = x
        self.y = y

    def __repr__(self):
        return "SVGPoint(%f, %f)" % (self.x, self.y)

class SVGRect:

    "A rectangle."

    def __init__(self, x=0, y=0, width=0, height=0):
        self.x, self.y, self.width, self.height = x, y, width, height

    def __eq__(self, other):
        return (self.x, self.y, self.width, self.height) == (other.x, other.y, other.width, other.height)

    def __ne__(self, other):
        return not (self == other)

    def fromNode(self, node, name):

        """
        Initialise this object from the trait on the 'node' having the given
        'name'.
        """

        value = node.getAttribute(name)
        if value is None:
            raise xml.dom.DOMException(xml.dom.NOT_SUPPORTED_ERR)
        try:
            values = map(float, value.split())
            self.x, self.y, self.width, self.height = values
        except (IndexError, ValueError):
            raise xml.dom.DOMException(xml.dom.TYPE_MISMATCH_ERR)

    def toNode(self, node, name):

        """
        Set the trait on the given 'node' using the given 'name' according to
        this object's attributes.
        """

        try:
            values = map(str, [self.x, self.y, self.width, self.height])
            node.setAttribute(name, " ".join(values))
        except (IndexError, ValueError):
            raise xml.dom.DOMException(xml.dom.TYPE_MISMATCH_ERR)

    def __repr__(self):
        return "SVGRect(%f, %f, %f, %f)" % (self.x, self.y, self.width, self.height)

class SVGRGBColor:

    """
    A colour.
    See: http://www.w3.org/TR/SVGMobile12/painting.html#colorSyntax
    """

    def __init__(self, red, green, blue):
        self.red, self.green, self.blue = red, green, blue

    def __repr__(self):
        return "SVGRGBColor(%f, %f, %f)" % (self.red, self.green, self.blue)

class TraitAccess:

    """
    Access to traits stored on elements.
    See: http://www.w3.org/TR/SVGMobile12/svgudom.html#svg__TraitAccess
    """

    def getPathTrait(self, name):
        path = SVGPath()
        path.fromNode(self, name)
        return path

    def setPathTrait(self, name, path):
        path.toNode(self, name)

    def getRectTrait(self, name):
        rect = SVGRect()
        rect.fromNode(self, name)
        return rect

    def setRectTrait(self, name, rect):
        rect.toNode(self, name) 

    def getMatrixTrait(self, name):
        if name != "transform":
            raise xml.dom.DOMException(xml.dom.NOT_SUPPORTED_ERR)
        matrix = SVGMatrix()
        matrix.fromNode(self, name)
        return matrix

    def setMatrixTrait(self, name, matrix):
        if name != "transform":
            raise xml.dom.DOMException(xml.dom.NOT_SUPPORTED_ERR)
        matrix.toNode(self, name) 

# Node classes.

class SVGNode(libxml2dom.Node):

    "Convenience modifications to nodes specific to libxml2dom.svg."

    def xpath(self, expr, variables=None, namespaces=None):

        """
        Evaluate the given 'expr' using the optional 'variables' and
        'namespaces'. If not otherwise specified, the "svg" prefix will be bound
        to SVG_NAMESPACE as defined in this module.
        """

        namespaces = namespaces or {}
        if not namespaces.has_key("svg"):
            namespaces["svg"] = SVG_NAMESPACE
        return libxml2dom.Node.xpath(self, expr, variables, namespaces)

# NOTE: DocumentEvent is from DOM Level 3 Events.
# NOTE: EventSystem is a special libxml2dom.events class.

class SVGDocument(libxml2dom._Document, SVGNode, EventTarget, DocumentEvent, EventSystem):

    "An SVG-specific document node."

    def __init__(self, node, impl):

        """
        Initialise the document with the given 'node', implementation 'impl',
        and global (SVGGlobal) details.
        """

        libxml2dom._Document.__init__(self, node, impl)
        self.global_ = self.impl.get_global(self) # parent

class SVGElement(SVGNode, EventTarget, TraitAccess, ElementTraversal): # NOTE: SVGNode instead of Element.

    "An SVG-specific element."

    def __init__(self, *args, **kw):
        SVGNode.__init__(self, *args, **kw)

    def _id(self):
        return self.getAttribute("id")

    def _setId(self, value):
        self.setAttribute("id", value)

    id = property(_id, _setId)

class SVGLocatableElement(SVGElement, SVGLocatable):

    "A locatable element."

    pass

class SVGTimedElement(SVGElement): # smil::ElementTimeControl

    "A timed element."

    def __init__(self, *args):

        "Initialise the element with the underlying 'args'."

        SVGElement.__init__(self, *args)
        self.document_time = 0
        self.paused = 0

    def _isPaused(self):
        return self.paused

    def pauseElement(self):
        self.paused = 1

    def resumeElement(self):
        self.paused = 0

class SVGSVGElement(SVGLocatableElement, SVGTimedElement):

    "An SVG-specific top-level element."

    NAV_AUTO = 1
    NAV_NEXT = 2
    NAV_PREV = 3
    NAV_UP = 4
    NAV_UP_RIGHT = 5
    NAV_RIGHT = 6
    NAV_DOWN_RIGHT = 7
    NAV_DOWN = 8
    NAV_DOWN_LEFT = 9
    NAV_LEFT = 10
    NAV_UP_LEFT = 11

    def __init__(self, *args):

        "Initialise the element with the underlying 'args'."

        SVGTimedElement.__init__(self, *args)
        self.scale = 1
        self.rotate = 0
        self.translate = SVGPoint(0, 0)

    # NOTE: The scale, rotate and translate properties are not persistent, and
    # NOTE: are specific to individual objects.

    def _currentScale(self):
        return self.scale

    def _currentRotate(self):
        return self.rotate

    def _currentTranslate(self):
        return self.translate

    def _setCurrentScale(self, scale):
        if scale == 0:
            raise xml.dom.DOMException(xml.dom.INVALID_ACCESS_ERR)
        self.scale = scale

    def _setCurrentRotate(self, rotate):
        self.rotate = rotate

    def _viewport(self):
        if self.hasAttribute("viewBox"):
            return self.getRectTrait("viewBox")
        else:
            return SVGRect(0, 0, self._convertMeasurement(self.getAttribute("width")),
                self._convertMeasurement(self.getAttribute("height")))

    # Utility methods.

    units = ["in", "cm", "mm", "pt", "pc", "px", "%"]

    def _convertMeasurement(self, value):
        value = value.strip()
        for unit in self.units:
            if value.endswith(unit):
                # NOTE: No conversion yet!
                return float(value[:-len(unit)].strip())

        raise xml.dom.DOMException(xml.dom.TYPE_MISMATCH_ERR)

    # Standard methods.

    def getCurrentTime(self):
        return self.document_time

    def setCurrentTime(self, setCurrentTime):
        self.document_time = setCurrentTime

    def createSVGMatrixComponents(self, a, b, c, d, e, f):
        return SVGMatrix(a, b, c, d, e, f)

    def createSVGRect(self):
        return SVGRect()

    def createSVGPath(self):
        return SVGPath()

    def createSVGRGBColor(self, red, green, blue):
        return SVGRGBColor(red, green, blue)

    def moveFocus(self, motionType):
        raise NotImplementedError, "moveFocus"

    def setFocus(self, object):
        raise NotImplementedError, "setFocus"

    def getCurrentFocusedObject(self):
        raise NotImplementedError, "getCurrentFocusedObject"

    currentScale = property(_currentScale, _setCurrentScale)
    currentRotate = property(_currentRotate, _setCurrentRotate)
    currentTranslate = property(_currentTranslate)
    viewport = property(_viewport)

# Event handler initialisation.

def initialiseEvents(doc):

    """
    See: http://www.w3.org/TR/SVGMobile12/svgudom.html#svg__EventListenerInitializer2
    See: http://www.w3.org/TR/xml-events/#section-listener-element
    """

    # Initialise script element listeners.

    for script in doc.xpath("//svg:script"):
        doc.global_.initializeEventListeners(script)

    # Initialise handler element listeners using XML Events.

    for handler in doc.xpath("//svg:handler"):
        listener = doc.global_.createEventListener(handler)

        # Attempt to parameterise the registration using the XML Events attributes.

        phase = handler.getAttributeNS(libxml2dom.events.XML_EVENTS_NAMESPACE, "phase")

        # Add the listener for the appropriate type and phases.

        handler.parentNode.addEventListener(
            handler.getAttributeNS(libxml2dom.events.XML_EVENTS_NAMESPACE, "event"),
            listener,
            phase == "capture"
            )

# Utility functions.

createDocument = libxml2dom.createDocument
createDocumentType = libxml2dom.createDocumentType

def createSVGDocument():
    return default_impl.createSVGDocument()

def parse(stream_or_string, html=0, htmlencoding=None, unfinished=0, impl=None):
    doc = libxml2dom.parse(stream_or_string, html=html, htmlencoding=htmlencoding, unfinished=unfinished, impl=(impl or default_impl))
    initialiseEvents(doc)
    return doc

def parseFile(filename, html=0, htmlencoding=None, unfinished=0, impl=None):
    doc = libxml2dom.parseFile(filename, html=html, htmlencoding=htmlencoding, unfinished=unfinished, impl=(impl or default_impl))
    initialiseEvents(doc)
    return doc

def parseString(s, html=0, htmlencoding=None, unfinished=0, impl=None):
    doc = libxml2dom.parseString(s, html=html, htmlencoding=htmlencoding, unfinished=unfinished, impl=(impl or default_impl))
    initialiseEvents(doc)
    return doc

def parseURI(uri, html=0, htmlencoding=None, unfinished=0, impl=None):
    doc = libxml2dom.parseURI(uri, html=html, htmlencoding=htmlencoding, unfinished=unfinished, impl=(impl or default_impl))
    initialiseEvents(doc)
    return doc

# Single instance of the implementation.

default_impl = SVGImplementation()

# vim: tabstop=4 expandtab shiftwidth=4
