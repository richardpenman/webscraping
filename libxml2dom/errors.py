#!/usr/bin/env python

"""
Errors for DOM Level 3.

Copyright (C) 2008 Paul Boddie <paul@boddie.org.uk>

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

class DOMError:

    """
    DOM Level 3 Core exception.
    See: http://www.w3.org/TR/DOM-Level-3-Core/core.html#ERROR-Interfaces-DOMError
    """

    SEVERITY_WARNING = 1
    SEVERITY_ERROR = 2
    SEVERITY_FATAL_ERROR = 3

    def __init__(self, severity=None, message=None, type=None, relatedException=None, relatedData=None, location=None):
        self.severity = severity
        self.message = message
        self.type = type
        self.relatedException = relatedException
        self.relatedData = relatedData
        self.location = location

    def __repr__(self):
        return "DOMError(%d, %r, %r)" % (self.severity, self.message, self.type)

    def __str__(self):
        return "DOMError: %s" % self.message

# NOTE: Find a reasonable way of exposing error details.

class DOMErrorHandler:

    """
    DOM Level 3 Core error handler.
    See: http://www.w3.org/TR/DOM-Level-3-Core/core.html#ERROR-Interfaces-DOMErrorHandler
    """

    def __init__(self):
        self.errors = []

    def handleError(self, error):
        self.errors.append(error)

    # Special extension methods.

    def reset(self):
        self.errors = []

    def __repr__(self):
        return "DOMErrorHandler()"

    def __str__(self):
        return "DOMErrorHandler: %r" % self.errors

# vim: tabstop=4 expandtab shiftwidth=4
