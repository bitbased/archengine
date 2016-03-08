#!/usr/bin/env python
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

from archengine import archengine_open, ArchEngineError
import sys, threading, archengine, aetest

class Callback(archengine.AsyncCallback):
    def __init__(self):
        pass

    def notify(self, op, op_ret, flags):
        raise AssertionError('callback should not be called in this test')


# test_async03.py
#    Async operations
# Try to run async code with an incorrect connection config.
# We expect an operation not supported error.
class test_async03(aetest.ArchEngineTestCase):
    """
    Test basic operations
    """
    table_name1 = 'test_async03'

    # Overrides ArchEngineTestCase so that we can configure
    # async operations.
    def setUpConnectionOpen(self, dir):
        self.home = dir
        # Note: this usage is intentionally wrong,
        # it is missing async=(enabled=true,...
        conn_params = \
                'create,error_prefix="%s: ",' % self.shortid() + \
                'async=(ops_max=50,threads=3)'  # missing enabled=true !
        sys.stdout.flush()
        conn = archengine_open(dir, conn_params)
        self.pr(`conn`)
        return conn

    def test_ops(self):
        tablearg = 'table:' + self.table_name1
        self.session.create(tablearg, 'key_format=S,value_format=S')

        # Populate table with async inserts, callback checks
        # to ensure key/value is correct.
        callback = Callback()

        self.assertRaises(archengine.ArchEngineError,
            lambda: self.conn.async_new_op(tablearg, None, callback))

        self.conn.async_flush()

if __name__ == '__main__':
    aetest.run()
