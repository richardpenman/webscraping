#
# pdis.xpath.context
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
XPath evaluation context
"""

class Context:
    def __init__(self, element, namespace_mapping = None):
        self.root = (element,)
        if namespace_mapping is None:
            self.namespace_mapping = {}
        else:
            self.namespace_mapping = namespace_mapping

        self.node = self.root
        self.position = 1
        self.size = 1

    def clone(self):
        context = Context(None)
        context.root = self.root
        context.namespace_mapping = self.namespace_mapping

        context.node = self.node
        context.position = self.position
        context.size = self.size

        return context

    def get_root(self):
        return self.root
