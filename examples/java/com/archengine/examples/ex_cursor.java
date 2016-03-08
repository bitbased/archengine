/*-
 * Public Domain 2014-2015 MongoDB, Inc.
 * Public Domain 2008-2014 ArchEngine, Inc.
 *
 * This is free and unencumbered software released into the public domain.
 *
 * Anyone is free to copy, modify, publish, use, compile, sell, or
 * distribute this software, either in source code form or as a compiled
 * binary, for any purpose, commercial or non-commercial, and by any
 * means.
 *
 * In jurisdictions that recognize copyright laws, the author or authors
 * of this software dedicate any and all copyright interest in the
 * software to the public domain. We make this dedication for the benefit
 * of the public at large and to the detriment of our heirs and
 * successors. We intend this dedication to be an overt act of
 * relinquishment in perpetuity of all present and future rights to this
 * software under copyright law.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 *
 * ex_cursor.java
 *	This is an example demonstrating some cursor types and operations.
 */

package com.archengine.examples;
import com.archengine.db.*;
import java.io.*;
import java.util.*;

public class ex_cursor {

    public static String home;

    /*! [cursor next] */
    public static int
    cursor_forward_scan(Cursor cursor)
        throws ArchEngineException
    {
        String key, value;
        int ret;

        while ((ret = cursor.next()) == 0) {
            key = cursor.getKeyString();
            value = cursor.getValueString();
        }
        return (ret);
    }
    /*! [cursor next] */

    /*! [cursor prev] */
    public static int
    cursor_reverse_scan(Cursor cursor)
        throws ArchEngineException
    {
        String key, value;
        int ret;

        while ((ret = cursor.prev()) == 0) {
            key = cursor.getKeyString();
            value = cursor.getValueString();
        }
        return (ret);
    }
    /*! [cursor prev] */

    /*! [cursor reset] */
    public static int
    cursor_reset(Cursor cursor)
        throws ArchEngineException
    {
        return (cursor.reset());
    }
    /*! [cursor reset] */

    /*! [cursor search] */
    public static int
    cursor_search(Cursor cursor)
        throws ArchEngineException
    {
        String value;
        int ret;

        cursor.putKeyString("foo");

        if ((ret = cursor.search()) != 0)
            value = cursor.getValueString();

        return (ret);
    }
    /*! [cursor search] */

    /*! [cursor search near] */
    public static int
    cursor_search_near(Cursor cursor)
        throws ArchEngineException
    {
        String key, value;
        SearchStatus exact;

        key = "foo";
        cursor.putKeyString(key);

        exact = cursor.search_near();
        if (exact == SearchStatus.SMALLER)
            /* Returned key smaller than search key */
            key = cursor.getKeyString();
        else if (exact == SearchStatus.LARGER)
            /* Returned key larger than search key */
            key = cursor.getKeyString();
        /* Else exact match found, and key already set */

        value = cursor.getValueString();

        return (0);
    }
    /*! [cursor search near] */

    /*! [cursor insert] */
    public static int
    cursor_insert(Cursor cursor)
        throws ArchEngineException
    {
        cursor.putKeyString("foo");
        cursor.putValueString("bar");

        return (cursor.insert());
    }
    /*! [cursor insert] */

    /*! [cursor update] */
    public static int
    cursor_update(Cursor cursor)
        throws ArchEngineException
    {
        cursor.putKeyString("foo");
        cursor.putValueString("newbar");

        return (cursor.update());
    }
    /*! [cursor update] */

    /*! [cursor remove] */
    public static int
    cursor_remove(Cursor cursor)
        throws ArchEngineException
    {
        cursor.putKeyString("foo");
        return (cursor.remove());
    }
    /*! [cursor remove] */

    public static int
    cursorExample()
        throws ArchEngineException
    {
        Connection conn;
        Cursor cursor;
        Session session;
        int ret;

        /*
         * Create a clean test directory for this run of the test program if the
         * environment variable isn't already set (as is done by make check).
         */
        if (System.getenv("ARCHENGINE_HOME") == null) {
            home = "AE_HOME";
            try {
                Process proc = Runtime.getRuntime().exec("/bin/rm -rf AE_HOME");
                BufferedReader br = new BufferedReader(
                    new InputStreamReader(proc.getInputStream()));
                while(br.ready())
                    System.out.println(br.readLine());
                br.close();
                proc.waitFor();
                new File("AE_HOME").mkdir();
            } catch (Exception ex) {
                System.err.println("Exception: " + ex);
                return (1);
            }
        } else
            home = null;

        conn = archengine.open(home, "create,statistics=(fast)");
        session = conn.open_session(null);

        ret = session.create("table:world",
            "key_format=r,value_format=5sii," +
            "columns=(id,country,population,area)");

        /*! [open cursor #1] */
        cursor = session.open_cursor("table:world", null, null);
        /*! [open cursor #1] */

        /*! [open cursor #2] */
        cursor = session.open_cursor("table:world(country,population)", null, null);
        /*! [open cursor #2] */

        /*! [open cursor #3] */
        cursor = session.open_cursor("statistics:", null, null);
        /*! [open cursor #3] */

        /* Create a simple string table to illustrate basic operations. */
        ret = session.create("table:map", "key_format=S,value_format=S");
        cursor = session.open_cursor("table:map", null, null);
        ret = cursor_insert(cursor);
        ret = cursor_reset(cursor);
        ret = cursor_forward_scan(cursor);
        ret = cursor_reset(cursor);
        ret = cursor_reverse_scan(cursor);
        ret = cursor_search_near(cursor);
        ret = cursor_update(cursor);
        ret = cursor_remove(cursor);
        ret = cursor.close();

        /* Note: closing the connection implicitly closes open session(s). */
        if ((ret = conn.close(null)) != 0)
            System.err.println("Error connecting to " + home + ": " +
                               archengine.archengine_strerror(ret));

        return (ret);
    }

    public static void
    main(String[] argv)
    {
        try {
            System.exit(cursorExample());
        }
        catch (ArchEngineException aee) {
            System.err.println("Exception: " + aee);
            aee.printStackTrace();
            System.exit(1);
        }
    }
}
