#!/usr/bin/env python

"""
DOM Level 3 Events support, with SVG Tiny 1.2 implementation additions.
See: http://www.w3.org/TR/DOM-Level-3-Events/events.html
See: http://www.w3.org/TR/xml-events/

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

import xml.dom
import time

XML_EVENTS_NAMESPACE = "http://www.w3.org/2001/xml-events"

class EventException(Exception):

    UNSPECIFIED_EVENT_TYPE_ERR = 0
    DISPATCH_REQUEST_ERR = 1

class DocumentEvent:

    """
    An event interface supportable by documents.
    See: http://www.w3.org/TR/DOM-Level-3-Events/events.html#Events-DocumentEvent
    """

    def canDispatch(self, namespaceURI, type):
        return namespaceURI is None and event_types.has_key(type)

    def createEvent(self, eventType):
        try:
            return event_types[eventType]()
        except KeyError:
            raise xml.dom.DOMException(xml.dom.NOT_SUPPORTED_ERR)

class Event:

    """
    An event class.
    See: http://www.w3.org/TR/SVGMobile12/svgudom.html#events__Event
    See: http://www.w3.org/TR/DOM-Level-3-Events/events.html#Events-Event
    """

    CAPTURING_PHASE = 1
    AT_TARGET = 2
    BUBBLING_PHASE = 3

    def __init__(self):

        "Initialise the event."

        # Initialised later:

        self.target = None
        self.currentTarget = None
        self.defaultPrevented = 0
        self.type = None
        self.namespaceURI = None

        # DOM Level 3 Events:

        self.bubbles = 1
        self.eventPhase = self.AT_TARGET # permits direct invocation of dispatchEvent
        self.timeStamp = time.time()

        # Propagation flags:

        self.stop = 0
        self.stop_now = 0

    def initEvent(self, eventTypeArg, canBubbleArg, cancelableArg):
        self.initEventNS(None, eventTypeArg, canBubbleArg, cancelableArg)

    def initEventNS(self, namespaceURIArg, eventTypeArg, canBubbleArg, cancelableArg):
        self.namespaceURI = namespaceURIArg
        self.type = eventTypeArg
        self.bubbles = canBubbleArg
        self.cancelable = cancelableArg

    def preventDefault(self):
        self.defaultPrevented = 1

    def stopPropagation(self):
        self.stop = 1

    def stopImmediatePropagation(self):
        self.stop = 1
        self.stop_now = 1

class UIEvent(Event):

    "A user interface event."

    def __init__(self):
        Event.__init__(self)
        self.detail = None

class MouseEvent(UIEvent):

    "A mouse-related event."

    def __init__(self):
        Event.__init__(self)
        self.screenX, self.screenY, self.clientX, self.clientY, self.button = None, None, None, None, None

# Event types registry.

event_types = {
    "Event" : Event,
    "UIEvent" : UIEvent,
    "MouseEvent" : MouseEvent
    }

class EventTarget:

    """
    An event target class.
    See: http://www.w3.org/TR/SVGMobile12/svgudom.html#events__EventTarget
    See: http://www.w3.org/TR/DOM-Level-3-Events/events.html#Events-EventTarget

    The listeners for a node are accessed through the global object. This common
    collection is consequently accessed by all nodes in a document, meaning that
    distinct objects representing the same node can still obtain the set of
    listeners registered for that node. In contrast, any attempt to directly
    store listeners on particular objects would result in the specific object
    which registered the listeners holding the record of such objects, whereas
    other objects obtained independently for the same node would hold no such
    record.
    """

    def addEventListener(self, type, listener, useCapture):

        """
        For the given event 'type', register the given 'listener' for events in
        the capture phase if 'useCapture' is a true value, or for events in the
        target and bubble phases otherwise.
        """

        self.addEventListenerNS(None, type, listener, useCapture)

    def addEventListenerNS(self, namespaceURI, type, listener, useCapture, group=None): # NOTE: group ignored

        """
        For the given 'namespaceURI' and event 'type', register the given
        'listener' for events in the capture phase if 'useCapture' is a true
        value, or for events in the target and bubble phases otherwise.
        """

        listeners = self.ownerDocument.global_.listeners
        if not listeners.has_key(self):
            listeners[self] = {}
        if not listeners[self].has_key((namespaceURI, type)):
            listeners[self][(namespaceURI, type)] = []
        listeners[self][(namespaceURI, type)].append((listener, useCapture))

    def dispatchEvent(self, evt):

        "For this node, dispatch event 'evt' to the registered listeners."

        listeners = self.ownerDocument.global_.listeners
        if not evt.type:
            raise EventException(EventException.UNSPECIFIED_EVENT_TYPE_ERR)

        # Determine the phase and define the current target (this node) for the
        # use of listeners.

        capturing = evt.eventPhase == evt.CAPTURING_PHASE
        evt.currentTarget = self

        # Dispatch on namespaceURI, type.

        for listener, useCapture in listeners.get(self, {}).get((evt.namespaceURI, evt.type), []):

            # Detect requests to stop propagation immediately.

            if evt.stop_now:
                break

            # Dispatch the event to the appropriate listeners according to the
            # phase.

            if capturing and useCapture or not capturing and not useCapture:
                listener.handleEvent(evt)

        return evt.defaultPrevented

    def removeEventListener(self, type, listener, useCapture):

        """
        For the given event 'type', deregister the given 'listener' for events
        in the capture phase if 'useCapture' is a true value, or for events in
        the target and bubble phases otherwise.
        """

        self.removeEventListenerNS(None, type, listener, useCapture)

    def removeEventListenerNS(self, namespaceURI, type, listener, useCapture):

        """
        For the given 'namespaceURI' and event 'type', deregister the given
        'listener' for events in the capture phase if 'useCapture' is a true
        value, or for events in the target and bubble phases otherwise.
        """

        listeners = self.ownerDocument.global_.listeners
        if listeners.has_key(self) and listeners[self].has_key((namespaceURI, type)):
            try:
                listeners[self][(namespaceURI, type)].remove((listener, useCapture))
            except ValueError:
                pass

# NOTE: The specification doesn't say much about the event system, but we will
# NOTE: provide a class to manage the different phases. This is mixed into the
# NOTE: SVGDocument class (and potentially other classes in future).

class EventSystem:

    "An event system which manages the different DOM event flow phases."

    def sendEventToTarget(self, evt, target):

        "Send event 'evt' to the specified 'target' element."

        # Determine the path of the event.

        bubble_route = target.xpath("ancestor::*")
        capture_route = bubble_route[:]
        capture_route.reverse()

        # Initialise the target and execute the capture phase.

        evt.target = target
        evt.eventPhase = evt.CAPTURING_PHASE
        for element in capture_route:
            if evt.stop:
                break
            element.dispatchEvent(evt)

        # Execute the target phase.

        evt.eventPhase = evt.AT_TARGET
        if not evt.stop:
            target.dispatchEvent(evt)

        # Execute the bubble phase, if appropriate.

        evt.eventPhase = evt.BUBBLING_PHASE
        if evt.bubbles:
            for element in bubble_route:
                if evt.stop:
                    break
                element.dispatchEvent(evt)

# vim: tabstop=4 expandtab shiftwidth=4
