#!usr/bin/env python
#
# Public Domain 2014-2015 MongoDB, Inc.
# Public Domain 2008-2014 ArchEngine, Inc.
#
# This is free and unencumbered software released into the public domain.
#
# Anyone is free to copy, modify, publish, use, compile, sell, or
# distribute this software, either in source code form or as a compiled
# binary, for any purpose, commercial or non-commercial, and by any
# means.
#
# In jurisdictions that recognize copyright laws, the author or authors
# of this software dedicate any and all copyright interest in the
# software to the public domain. We make this dedication for the benefit
# of the public at large and to the detriment of our heirs and
# successors. We intend this dedication to be an overt act of
# relinquishment in perpetuity of all present and future rights to this
# software under copyright law.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

import archengine, aetest
from helper import simple_populate, key_populate, value_populate

# test_colgap.py
#    Test variable-length column-store gap performance.
class test_column_store_gap(aetest.ArchEngineTestCase):
    nentries = 13

    # Cursor forward
    def forward(self, cursor, expected):
        cursor.reset()
        i = 0
        while True:
            if cursor.next() != 0:
                break
            self.assertEqual(cursor.get_key(), expected[i])
            i += 1
        self.assertEqual(i, self.nentries)

    # Cursor backward
    def backward(self, cursor, expected):
        cursor.reset()
        i = 0
        while True:
            if cursor.prev() != 0:
                break
            self.assertEqual(cursor.get_key(), expected[i])
            i += 1
        self.assertEqual(i, self.nentries)

    # Create a variable-length column-store table with really big gaps in the
    # namespace. If this runs in less-than-glacial time, it's working.
    def test_column_store_gap(self):
        uri = 'table:gap'
        simple_populate(self, uri, 'key_format=r,value_format=S', 0)
        cursor = self.session.open_cursor(uri, None, None)
        self.nentries = 0

        # Create a column-store table with large gaps in the name-space.
        v = [ 1000, 2000000000000, 30000000000000 ]
        for i in v:
            cursor[key_populate(cursor, i)] = value_populate(cursor, i)
            self.nentries += 1

        # In-memory cursor forward, backward.
        self.forward(cursor, v)
        self.backward(cursor, list(reversed(v)))

        self.reopen_conn()
        cursor = self.session.open_cursor(uri, None, None)

        # Disk page cursor forward, backward.
        self.forward(cursor, v)
        self.backward(cursor, list(reversed(v)))

    def test_column_store_gap_traverse(self):
        uri = 'table:gap'
        simple_populate(self, uri, 'key_format=r,value_format=S', 0)
        cursor = self.session.open_cursor(uri, None, None)
        self.nentries = 0

        # Create a column store with key gaps. The particular values aren't
        # important, we just want some gaps.
        v = [ 1000, 1001, 2000, 2001]
        for i in v:
            cursor[key_populate(cursor, i)] = value_populate(cursor, i)
            self.nentries += 1

        # In-memory cursor forward, backward.
        self.forward(cursor, v)
        self.backward(cursor, list(reversed(v)))

        self.reopen_conn()
        cursor = self.session.open_cursor(uri, None, None)

        # Disk page cursor forward, backward.
        self.forward(cursor, v)
        self.backward(cursor, list(reversed(v)))

        # Insert some new records, so there are in-memory updates and an
        # on disk image. Put them in the middle of the existing values
        # so the traversal walks to them.
        v2 = [ 1500, 1501 ]
        for i in v2:
            cursor[key_populate(cursor, i)] = value_populate(cursor, i)
            self.nentries += 1

        # Tell the validation what to expect.
        v = [ 1000, 1001, 1500, 1501, 2000, 2001 ]
        self.forward(cursor, v)
        self.backward(cursor, list(reversed(v)))


if __name__ == '__main__':
    aetest.run()
