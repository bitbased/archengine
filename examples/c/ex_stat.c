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
 * ex_stat.c
 *	This is an example demonstrating how to query database statistics.
 */

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <archengine.h>

int print_cursor(AE_CURSOR *);
int print_database_stats(AE_SESSION *);
int print_file_stats(AE_SESSION *);
int print_overflow_pages(AE_SESSION *);
int get_stat(AE_CURSOR *cursor, int stat_field, uint64_t *valuep);
int print_derived_stats(AE_SESSION *);

static const char *home;

/*! [statistics display function] */
int
print_cursor(AE_CURSOR *cursor)
{
	const char *desc, *pvalue;
	uint64_t value;
	int ret;

	while ((ret = cursor->next(cursor)) == 0 &&
	    (ret = cursor->get_value(cursor, &desc, &pvalue, &value)) == 0)
		if (value != 0)
			printf("%s=%s\n", desc, pvalue);

	return (ret == AE_NOTFOUND ? 0 : ret);
}
/*! [statistics display function] */

int 
print_database_stats(AE_SESSION *session)
{
	AE_CURSOR *cursor;
	int ret;

	/*! [statistics database function] */
	if ((ret = session->open_cursor(session,
	    "statistics:", NULL, NULL, &cursor)) != 0)
		return (ret);

	ret = print_cursor(cursor);
	ret = cursor->close(cursor);
	/*! [statistics database function] */

	return (ret);
}

int 
print_file_stats(AE_SESSION *session)
{
	AE_CURSOR *cursor;
	int ret;

	/*! [statistics table function] */
	if ((ret = session->open_cursor(session,
	    "statistics:table:access", NULL, NULL, &cursor)) != 0)
		return (ret);

	ret = print_cursor(cursor);
	ret = cursor->close(cursor);
	/*! [statistics table function] */

	return (ret);
}

int 
print_overflow_pages(AE_SESSION *session)
{
	/*! [statistics retrieve by key] */
	AE_CURSOR *cursor;
	const char *desc, *pvalue;
	uint64_t value;
	int ret;

	if ((ret = session->open_cursor(session,
	    "statistics:table:access", NULL, NULL, &cursor)) != 0)
		return (ret);

	cursor->set_key(cursor, AE_STAT_DSRC_BTREE_OVERFLOW);
	ret = cursor->search(cursor);
	ret = cursor->get_value(cursor, &desc, &pvalue, &value);
	printf("%s=%s\n", desc, pvalue);

	ret = cursor->close(cursor);
	/*! [statistics retrieve by key] */

	return (ret);
}

/*! [statistics calculation helper function] */
int
get_stat(AE_CURSOR *cursor, int stat_field, uint64_t *valuep)
{
	const char *desc, *pvalue;
	int ret;

	cursor->set_key(cursor, stat_field);
	if ((ret = cursor->search(cursor)) != 0)
		return (ret);

	return (cursor->get_value(cursor, &desc, &pvalue, valuep));
}
/*! [statistics calculation helper function] */

int
print_derived_stats(AE_SESSION *session)
{
	AE_CURSOR *cursor;
	int ret;

	/*! [statistics calculate open table stats] */
	if ((ret = session->open_cursor(session,
	    "statistics:table:access", NULL, NULL, &cursor)) != 0)
		return (ret);
	/*! [statistics calculate open table stats] */

	{
	/*! [statistics calculate table fragmentation] */
	uint64_t ckpt_size, file_size, percent;
	ret = get_stat(cursor, AE_STAT_DSRC_BLOCK_CHECKPOINT_SIZE, &ckpt_size);
	ret = get_stat(cursor, AE_STAT_DSRC_BLOCK_SIZE, &file_size);

	percent = 0;
	if (file_size != 0)
		percent = 100 * ((file_size - ckpt_size) / file_size);
	printf("Table is %" PRIu64 "%% fragmented\n", percent);
	/*! [statistics calculate table fragmentation] */
	}

	{
	/*! [statistics calculate write amplification] */
	uint64_t app_insert, app_remove, app_update, fs_writes;

	ret = get_stat(cursor, AE_STAT_DSRC_CURSOR_INSERT_BYTES, &app_insert);
	ret = get_stat(cursor, AE_STAT_DSRC_CURSOR_REMOVE_BYTES, &app_remove);
	ret = get_stat(cursor, AE_STAT_DSRC_CURSOR_UPDATE_BYTES, &app_update);

	ret = get_stat(cursor, AE_STAT_DSRC_CACHE_BYTES_WRITE, &fs_writes);

	if (app_insert + app_remove + app_update != 0)
		printf("Write amplification is %.2lf\n",
		    (double)fs_writes / (app_insert + app_remove + app_update));
	/*! [statistics calculate write amplification] */
	}

	ret = cursor->close(cursor);

	return (ret);
}

int
main(void)
{
	AE_CONNECTION *conn;
	AE_CURSOR *cursor;
	AE_SESSION *session;
	int ret;

	/*
	 * Create a clean test directory for this run of the test program if the
	 * environment variable isn't already set (as is done by make check).
	 */
	if (getenv("ARCHENGINE_HOME") == NULL) {
		home = "AE_HOME";
		ret = system("rm -rf AE_HOME && mkdir AE_HOME");
	} else
		home = NULL;

	ret = archengine_open(home, NULL, "create,statistics=(all)", &conn);
	ret = conn->open_session(conn, NULL, NULL, &session);
	ret = session->create(
	    session, "table:access", "key_format=S,value_format=S");

	ret = session->open_cursor(
	    session, "table:access", NULL, NULL, &cursor);
	cursor->set_key(cursor, "key");
	cursor->set_value(cursor, "value");
	ret = cursor->insert(cursor);
	ret = cursor->close(cursor);

	ret = session->checkpoint(session, NULL);

	ret = print_database_stats(session);

	ret = print_file_stats(session);

	ret = print_overflow_pages(session);

	ret = print_derived_stats(session);

	return (conn->close(conn, NULL) == 0 ? ret : EXIT_FAILURE);
}
