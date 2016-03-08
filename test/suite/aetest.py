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
#
# ArchEngineTestCase
#   parent class for all test cases
#

# If unittest2 is available, use it in preference to (the old) unittest
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from contextlib import contextmanager
import os, re, shutil, sys, time, traceback
import aescenario
import archengine

def shortenWithEllipsis(s, maxlen):
    if len(s) > maxlen:
        s = s[0:maxlen-3] + '...'
    return s

class CapturedFd(object):
    """
    CapturedFd encapsulates a file descriptor (e.g. 1 or 2) that is diverted
    to a file.  We use this to capture and check the C stdout/stderr.
    Meanwhile we reset Python's sys.stdout, sys.stderr, using duped copies
    of the original 1, 2 fds.  The end result is that Python's sys.stdout
    sys.stderr behave normally (e.g. go to the tty), while the C stdout/stderr
    ends up in a file that we can verify.
    """
    def __init__(self, filename, desc):
        self.filename = filename
        self.desc = desc
        self.expectpos = 0
        self.file = None

    def readFileFrom(self, filename, pos, maxchars):
        """
        Read a file starting at a given position,
        returning the beginning of its contents
        """
        with open(filename, 'r') as f:
            f.seek(pos)
            return shortenWithEllipsis(f.read(maxchars+1), maxchars)

    def capture(self):
        """
        Start capturing the file descriptor.
        Note that the original targetFd is closed, we expect
        that the caller has duped it and passed the dup to us
        in the constructor.
        """
        self.file = open(self.filename, 'w')
        return self.file

    def release(self):
        """
        Stop capturing.
        """
        self.file.close()
        self.file = None

    def check(self, testcase):
        """
        Check to see that there is no unexpected output in the captured output
        file.  If there is, raise it as a test failure.
        This is generally called after 'release' is called.
        """
        if self.file != None:
            self.file.flush()
        filesize = os.path.getsize(self.filename)
        if filesize > self.expectpos:
            contents = self.readFileFrom(self.filename, self.expectpos, 10000)
            ArchEngineTestCase.prout('ERROR: ' + self.filename +
                                     ' unexpected ' + self.desc +
                                     ', contains:\n"' + contents + '"')
            testcase.fail('unexpected ' + self.desc + ', contains: "' +
                      shortenWithEllipsis(contents,100) + '"')
        self.expectpos = filesize

    def checkAdditional(self, testcase, expect):
        """
        Check to see that an additional string has been added to the
        output file.  If it has not, raise it as a test failure.
        In any case, reset the expected pos to account for the new output.
        """
        if self.file != None:
            self.file.flush()
        gotstr = self.readFileFrom(self.filename, self.expectpos, 1000)
        testcase.assertEqual(gotstr, expect, 'in ' + self.desc +
                             ', expected "' + expect + '", but got "' +
                             gotstr + '"')
        self.expectpos = os.path.getsize(self.filename)

    def checkAdditionalPattern(self, testcase, pat):
        """
        Check to see that an additional string has been added to the
        output file.  If it has not, raise it as a test failure.
        In any case, reset the expected pos to account for the new output.
        """
        if self.file != None:
            self.file.flush()
        gotstr = self.readFileFrom(self.filename, self.expectpos, 1000)
        if re.search(pat, gotstr) == None:
            testcase.fail('in ' + self.desc +
                          ', expected pattern "' + pat + '", but got "' +
                          gotstr + '"')
        self.expectpos = os.path.getsize(self.filename)


class ArchEngineTestCase(unittest.TestCase):
    _globalSetup = False
    _printOnceSeen = {}
    conn_config = ''

    @staticmethod
    def globalSetup(preserveFiles = False, useTimestamp = False,
                    gdbSub = False, verbose = 1, dirarg = None,
                    longtest = False):
        ArchEngineTestCase._preserveFiles = preserveFiles
        d = 'AE_TEST' if dirarg == None else dirarg
        if useTimestamp:
            d += '.' + time.strftime('%Y%m%d-%H%M%S', time.localtime())
        shutil.rmtree(d, ignore_errors=True)
        os.makedirs(d)
        aescenario.set_long_run(longtest)
        ArchEngineTestCase._parentTestdir = d
        ArchEngineTestCase._origcwd = os.getcwd()
        ArchEngineTestCase._resultfile = open(os.path.join(d, 'results.txt'), "w", 0)  # unbuffered
        ArchEngineTestCase._gdbSubprocess = gdbSub
        ArchEngineTestCase._longtest = longtest
        ArchEngineTestCase._verbose = verbose
        ArchEngineTestCase._dupout = os.dup(sys.stdout.fileno())
        ArchEngineTestCase._stdout = sys.stdout
        ArchEngineTestCase._stderr = sys.stderr
        ArchEngineTestCase._concurrent = False
        ArchEngineTestCase._globalSetup = True
        ArchEngineTestCase._ttyDescriptor = None

    def fdSetUp(self):
        self.captureout = CapturedFd('stdout.txt', 'standard output')
        self.captureerr = CapturedFd('stderr.txt', 'error output')
        sys.stdout = self.captureout.capture()
        sys.stderr = self.captureerr.capture()

    def fdTearDown(self):
        # restore stderr/stdout
        self.captureout.release()
        self.captureerr.release()
        sys.stdout = ArchEngineTestCase._stdout
        sys.stderr = ArchEngineTestCase._stderr

    def __init__(self, *args, **kwargs):
        if hasattr(self, 'scenarios'):
            assert(len(self.scenarios) == len(dict(self.scenarios)))
        unittest.TestCase.__init__(self, *args, **kwargs)
        if not self._globalSetup:
            ArchEngineTestCase.globalSetup()

    def __str__(self):
        # when running with scenarios, if the number_scenarios() method
        # is used, then each scenario is given a number, which can
        # help distinguish tests.
        scen = ''
        if hasattr(self, 'scenario_number') and hasattr(self, 'scenario_name'):
            scen = '(scenario ' + str(self.scenario_number) + \
                   ': ' + self.scenario_name + ')'
        return self.simpleName() + scen

    def simpleName(self):
        return "%s.%s.%s" %  (self.__module__,
                              self.className(), self._testMethodName)

    # Can be overridden
    def setUpConnectionOpen(self, dir):
        conn = archengine.archengine_open(dir,
            'create,error_prefix="%s",%s' % (self.shortid(), self.conn_config))
        self.pr(`conn`)
        return conn

    # Can be overridden
    def setUpSessionOpen(self, conn):
        return conn.open_session(None)

    # Can be overridden
    def close_conn(self):
        """
        Close the connection if already open.
        """
        if self.conn != None:
            self.conn.close()
            self.conn = None

    def open_conn(self):
        """
        Open the connection if already closed.
        """
        if self.conn == None:
            self.conn = self.setUpConnectionOpen(".")
            self.session = self.setUpSessionOpen(self.conn)

    def reopen_conn(self):
        """
        Reopen the connection.
        """
        self.close_conn()
        self.open_conn()

    def setUp(self):
        if not hasattr(self.__class__, 'ae_ntests'):
            self.__class__.ae_ntests = 0
        if ArchEngineTestCase._concurrent:
            self.testsubdir = self.shortid() + '.' + str(self.__class__.ae_ntests)
        else:
            self.testsubdir = self.className() + '.' + str(self.__class__.ae_ntests)
        self.testdir = os.path.join(ArchEngineTestCase._parentTestdir, self.testsubdir)
        self.__class__.ae_ntests += 1
        if ArchEngineTestCase._verbose > 2:
            self.prhead('started in ' + self.testdir, True)
        self.origcwd = os.getcwd()
        shutil.rmtree(self.testdir, ignore_errors=True)
        if os.path.exists(self.testdir):
            raise Exception(self.testdir + ": cannot remove directory")
        os.makedirs(self.testdir)
        os.chdir(self.testdir)
        self.fdSetUp()
        # tearDown needs a conn field, set it here in case the open fails.
        self.conn = None
        try:
            self.conn = self.setUpConnectionOpen(".")
            self.session = self.setUpSessionOpen(self.conn)
        except:
            self.tearDown()
            raise

    def tearDown(self):
        excinfo = sys.exc_info()
        passed = (excinfo == (None, None, None))
        if passed:
            skipped = False
        else:
            skipped = (excinfo[0] == unittest.SkipTest)
        self.pr('finishing')

        try:
            self.close_conn()
        except:
            pass

        try:
            self.fdTearDown()
            # Only check for unexpected output if the test passed
            if passed:
                self.captureout.check(self)
                self.captureerr.check(self)
        finally:
            # always get back to original directory
            os.chdir(self.origcwd)

        # Clean up unless there's a failure
        if (passed or skipped) and not ArchEngineTestCase._preserveFiles:
            shutil.rmtree(self.testdir, ignore_errors=True)
        else:
            self.pr('preserving directory ' + self.testdir)

        if not passed and not skipped:
            print "ERROR in " + str(self)
            self.pr('FAIL')
            self.prexception(excinfo)
            self.pr('preserving directory ' + self.testdir)
        if ArchEngineTestCase._verbose > 2:
            self.prhead('TEST COMPLETED')

    def backup(self, backup_dir, session=None):
        if session is None:
            session = self.session
        shutil.rmtree(backup_dir, ignore_errors=True)
        os.mkdir(backup_dir)
        bkp_cursor = session.open_cursor('backup:', None, None)
        while True:
            ret = bkp_cursor.next()
            if ret != 0:
                break
            shutil.copy(bkp_cursor.get_key(), backup_dir)
        self.assertEqual(ret, archengine.AE_NOTFOUND)
        bkp_cursor.close()

    @contextmanager
    def expectedStdout(self, expect):
        self.captureout.check(self)
        yield
        self.captureout.checkAdditional(self, expect)

    @contextmanager
    def expectedStderr(self, expect):
        self.captureerr.check(self)
        yield
        self.captureerr.checkAdditional(self, expect)

    @contextmanager
    def expectedStdoutPattern(self, pat):
        self.captureout.check(self)
        yield
        self.captureout.checkAdditionalPattern(self, pat)

    @contextmanager
    def expectedStderrPattern(self, pat):
        self.captureerr.check(self)
        yield
        self.captureerr.checkAdditionalPattern(self, pat)

    def assertRaisesWithMessage(self, exceptionType, expr, message):
        """
        Like TestCase.assertRaises(), but also checks to see
        that a message is printed on stderr.  If message starts
        and ends with a slash, it is considered a pattern that
        must appear in stderr (it need not encompass the entire
        error output).  Otherwise, the message must match verbatim,
        including any trailing newlines.
        """
        if len(message) > 2 and message[0] == '/' and message[-1] == '/':
            with self.expectedStderrPattern(message[1:-1]):
                self.assertRaises(exceptionType, expr)
        else:
            with self.expectedStderr(message):
                self.assertRaises(exceptionType, expr)

    def exceptionToStderr(self, expr):
        """
        Used by assertRaisesHavingMessage to convert an expression
        that throws an error to an expression that throws the
        same error but also has the exception string on stderr.
        """
        try:
            expr()
        except BaseException, err:
            sys.stderr.write('Exception: ' + str(err))
            raise

    def assertRaisesHavingMessage(self, exceptionType, expr, message):
        """
        Like TestCase.assertRaises(), but also checks to see
        that the assert exception, when string-ified, includes a message.
        If message starts and ends with a slash, it is considered a pattern that
        must appear (it need not encompass the entire message).
        Otherwise, the message must match verbatim.
        """
        self.assertRaisesWithMessage(
            exceptionType, lambda: self.exceptionToStderr(expr), message)

    @staticmethod
    def printOnce(msg):
        # There's a race condition with multiple threads,
        # but we won't worry about it.  We err on the side
        # of printing the message too many times.
        if not msg in ArchEngineTestCase._printOnceSeen:
            ArchEngineTestCase._printOnceSeen[msg] = msg
            ArchEngineTestCase.prout(msg)

    def KNOWN_FAILURE(self, name):
        myname = self.simpleName()
        msg = '**** ' + myname + ' HAS A KNOWN FAILURE: ' + name + ' ****'
        self.printOnce(msg)
        self.skipTest('KNOWN FAILURE: ' + name)

    def KNOWN_LIMITATION(self, name):
        myname = self.simpleName()
        msg = '**** ' + myname + ' HAS A KNOWN LIMITATION: ' + name + ' ****'
        self.printOnce(msg)

    @staticmethod
    def printVerbose(level, message):
        if level <= ArchEngineTestCase._verbose:
            ArchEngineTestCase.prout(message)

    def verbose(self, level, message):
        ArchEngineTestCase.printVerbose(level, message)

    def prout(self, s):
        ArchEngineTestCase.prout(s)

    @staticmethod
    def prout(s):
        os.write(ArchEngineTestCase._dupout, s + '\n')

    def pr(self, s):
        """
        print a progress line for testing
        """
        msg = '    ' + self.shortid() + ': ' + s
        ArchEngineTestCase._resultfile.write(msg + '\n')

    def prhead(self, s, *beginning):
        """
        print a header line for testing, something important
        """
        msg = ''
        if len(beginning) > 0:
            msg += '\n'
        msg += '  ' + self.shortid() + ': ' + s
        self.prout(msg)
        ArchEngineTestCase._resultfile.write(msg + '\n')

    def prexception(self, excinfo):
        ArchEngineTestCase._resultfile.write('\n')
        traceback.print_exception(excinfo[0], excinfo[1], excinfo[2], None, ArchEngineTestCase._resultfile)
        ArchEngineTestCase._resultfile.write('\n')

    # print directly to tty, useful for debugging
    def tty(self, message):
        ArchEngineTestCase.tty(message)

    @staticmethod
    def tty(message):
        if ArchEngineTestCase._ttyDescriptor == None:
            ArchEngineTestCase._ttyDescriptor = open('/dev/tty', 'w')
        ArchEngineTestCase._ttyDescriptor.write(message + '\n')

    def ttyVerbose(self, level, message):
        ArchEngineTestCase.ttyVerbose(level, message)

    @staticmethod
    def ttyVerbose(level, message):
        if level <= ArchEngineTestCase._verbose:
            ArchEngineTestCase.tty(message)

    def shortid(self):
        return self.id().replace("__main__.","")

    def className(self):
        return self.__class__.__name__


def longtest(description):
    """
    Used as a function decorator, for example, @aetest.longtest("description").
    The decorator indicates that this test function should only be included
    when running the test suite with the --long option.
    """
    def runit_decorator(func):
        return func
    if not ArchEngineTestCase._longtest:
        return unittest.skip(description + ' (enable with --long)')
    else:
        return runit_decorator

def islongtest():
    return ArchEngineTestCase._longtest

def runsuite(suite, parallel):
    suite_to_run = suite
    if parallel > 1:
        from concurrencytest import ConcurrentTestSuite, fork_for_tests
        if not ArchEngineTestCase._globalSetup:
            ArchEngineTestCase.globalSetup()
        ArchEngineTestCase._concurrent = True
        suite_to_run = ConcurrentTestSuite(suite, fork_for_tests(parallel))
    try:
        return unittest.TextTestRunner(
            verbosity=ArchEngineTestCase._verbose).run(suite_to_run)
    except BaseException as e:
        # This should not happen for regular test errors, unittest should catch everything
        print('ERROR: running test: ', e)
        raise e

def run(name='__main__'):
    result = runsuite(unittest.TestLoader().loadTestsFromName(name), False)
    sys.exit(not result.wasSuccessful())