#!/usr/bin/env python
#
# Public Domain 2014-2015 MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
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

import wiredtiger, wttest
from helper import copy_wiredtiger_home, key_populate, simple_populate

# test_bug014.py
#    JIRA WT-2115: fast-delete pages can be incorrectly lost due to a crash.
class test_bug014(wttest.WiredTigerTestCase):
    def test_bug014(self):
        # Populate a table with 1000 keys on small pages.
        uri = 'table:test_bug014'
        simple_populate(self, uri,
            'allocation_size=512,leaf_page_max=512,key_format=S', 1000)

        # Reopen it so we can fast-delete pages.
        self.reopen_conn()

        # Truncate a chunk of the key/value pairs inside a transaction.
        self.session.begin_transaction(None)
        start = self.session.open_cursor(uri, None)
        start.set_key(key_populate(start, 250))
        end = self.session.open_cursor(uri, None)
        end.set_key(key_populate(end, 500))
        self.session.truncate(None, start, end, None)
        start.close()
        end.close()

        # With the truncation uncommitted, checkpoint the database.
        ckpt_session = self.conn.open_session()
        ckpt_session.checkpoint(None)
        ckpt_session.close()

        # Simulate a crash by copying to a new directory.
        copy_wiredtiger_home(".", "RESTART")

        # Open the new directory.
        conn = self.setUpConnectionOpen("RESTART")
        session = self.setUpSessionOpen(conn)
        cursor = session.open_cursor(uri)

        # Confirm all of the records are there.
        for i in range(1, 1001):
            cursor.set_key(key_populate(cursor, i))
            self.assertEqual(cursor.search(), 0)

        conn.close()

if __name__ == '__main__':
    wttest.run()
