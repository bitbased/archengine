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
#
# test_backup05.py
#   Test that backups can be performed similar to MongoDB's fsyncLock.
#   We assume writes are not being performed, a checkpoint is done and
#   then we open a backup cursor to prevent log archiving and other file
#   manipulations.  Manually copy the directory and verify it.
#

import fnmatch, os, shutil, time
from suite_subprocess import suite_subprocess
from wiredtiger import wiredtiger_open
from wtscenario import multiply_scenarios, number_scenarios, prune_scenarios
from helper import copy_wiredtiger_home
import wttest

class test_backup05(wttest.WiredTigerTestCase, suite_subprocess):
    uri = 'table:test_backup05'
    create_params = 'key_format=i,value_format=i'
    freq = 5

    def copy_windows(self, olddir, newdir):
        os.mkdir(newdir)
        for fname in os.listdir(olddir):
            fullname = os.path.join(olddir, fname)
            # Skip lock file on Windows since it is locked
            if os.path.isfile(fullname) and "WiredTiger.lock" not in fullname:
                shutil.copy(fullname, newdir)

    def check_manual_backup(self, i, olddir, newdir):
        ''' Simulate a manual backup from olddir and restart in newdir. '''
        self.session.checkpoint()
        cbkup = self.session.open_cursor('backup:', None, None)

        # With the connection still open, copy files to new directory.
        # Half the time use an unaligned copy.
        aligned = (i % (self.freq * 2) != 0) or os.name == "nt"
        copy_wiredtiger_home(olddir, newdir, aligned)

        # Now simulate fsyncUnlock by closing the backup cursor.
        cbkup.close()

        # Open the new directory and verify
        conn = self.setUpConnectionOpen(newdir)
        session = self.setUpSessionOpen(conn)
        session.verify(self.uri)
        conn.close()

    def test_backup(self):
        '''Check manual fsyncLock backup strategy'''

        # Here's the strategy:
        #    - update the table
        #    - checkpoint the database
        #    - open a backup cursor
        #    - copy the database directory (live, simulating a crash)
        #      - use copy tree or non-aligned dd
        #    - verify in the copy
        #    - repeat
        #
        # If the metadata isn't flushed, eventually the metadata we copy will
        # be sufficiently out-of-sync with the data file that it won't verify.
        self.session.create(self.uri, self.create_params)
        for i in range(100):
            c = self.session.open_cursor(self.uri)
            c[i] = i
            c.close()
            if i % self.freq == 0:
                self.check_manual_backup(i, ".", "RESTART")
            else:
                self.session.verify(self.uri)

if __name__ == '__main__':
    wttest.run()
