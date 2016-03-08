/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __truncate_file --
 *	AE_SESSION::truncate for a file.
 */
static int
__truncate_file(AE_SESSION_IMPL *session, const char *uri)
{
	AE_DECL_RET;
	const char *filename;
	uint32_t allocsize;

	filename = uri;
	if (!AE_PREFIX_SKIP(filename, "file:"))
		return (EINVAL);

	/* Open and lock the file. */
	AE_RET(__ae_session_get_btree(
	    session, uri, NULL, NULL, AE_DHANDLE_EXCLUSIVE));
	AE_STAT_FAST_DATA_INCR(session, cursor_truncate);

	/* Get the allocation size. */
	allocsize = S2BT(session)->allocsize;

	AE_RET(__ae_session_release_btree(session));

	/* Close any btree handles in the file. */
	AE_WITH_HANDLE_LIST_LOCK(session,
	    ret = __ae_conn_dhandle_close_all(session, uri, false));
	AE_RET(ret);

	/* Delete the root address and truncate the file. */
	AE_RET(__ae_meta_checkpoint_clear(session, uri));
	AE_RET(__ae_block_manager_truncate(session, filename, allocsize));

	return (0);
}

/*
 * __truncate_table --
 *	AE_SESSION::truncate for a table.
 */
static int
__truncate_table(AE_SESSION_IMPL *session, const char *uri, const char *cfg[])
{
	AE_DECL_RET;
	AE_TABLE *table;
	u_int i;

	AE_RET(__ae_schema_get_table(session, uri, strlen(uri), false, &table));
	AE_STAT_FAST_DATA_INCR(session, cursor_truncate);

	/* Truncate the column groups. */
	for (i = 0; i < AE_COLGROUPS(table); i++)
		AE_ERR(__ae_schema_truncate(
		    session, table->cgroups[i]->source, cfg));

	/* Truncate the indices. */
	AE_ERR(__ae_schema_open_indices(session, table));
	for (i = 0; i < table->nindices; i++)
		AE_ERR(__ae_schema_truncate(
		    session, table->indices[i]->source, cfg));

err:	__ae_schema_release_table(session, table);
	return (ret);
}

/*
 * __truncate_dsrc --
 *	AE_SESSION::truncate for a data-source without a truncate operation.
 */
static int
__truncate_dsrc(AE_SESSION_IMPL *session, const char *uri)
{
	AE_CURSOR *cursor;
	AE_DECL_RET;
	const char *cfg[2];

	/* Open a cursor and traverse the object, removing every entry. */
	cfg[0] = AE_CONFIG_BASE(session, AE_SESSION_open_cursor);
	cfg[1] = NULL;
	AE_RET(__ae_open_cursor(session, uri, NULL, cfg, &cursor));
	while ((ret = cursor->next(cursor)) == 0)
		AE_ERR(cursor->remove(cursor));
	AE_ERR_NOTFOUND_OK(ret);
	AE_STAT_FAST_DATA_INCR(session, cursor_truncate);

err:	AE_TRET(cursor->close(cursor));
	return (ret);
}

/*
 * __ae_schema_truncate --
 *	AE_SESSION::truncate without a range.
 */
int
__ae_schema_truncate(
    AE_SESSION_IMPL *session, const char *uri, const char *cfg[])
{
	AE_DATA_SOURCE *dsrc;
	AE_DECL_RET;
	const char *tablename;

	tablename = uri;

	if (AE_PREFIX_MATCH(uri, "file:")) {
		ret = __truncate_file(session, uri);
	} else if (AE_PREFIX_MATCH(uri, "lsm:"))
		ret = __ae_lsm_tree_truncate(session, uri, cfg);
	else if (AE_PREFIX_SKIP(tablename, "table:"))
		ret = __truncate_table(session, tablename, cfg);
	else if ((dsrc = __ae_schema_get_source(session, uri)) != NULL)
		ret = dsrc->truncate == NULL ?
		    __truncate_dsrc(session, uri) :
		    dsrc->truncate(
		    dsrc, &session->iface, uri, (AE_CONFIG_ARG *)cfg);
	else
		ret = __ae_bad_object_type(session, uri);

	/* If we didn't find a metadata entry, map that error to ENOENT. */
	return (ret == AE_NOTFOUND ? ENOENT : ret);
}

/*
 * __ae_range_truncate --
 *	Truncate of a cursor range, default implementation.
 */
int
__ae_range_truncate(AE_CURSOR *start, AE_CURSOR *stop)
{
	AE_DECL_RET;
	int cmp;

	if (start == NULL) {
		do {
			AE_RET(stop->remove(stop));
		} while ((ret = stop->prev(stop)) == 0);
		AE_RET_NOTFOUND_OK(ret);
	} else {
		cmp = -1;
		do {
			if (stop != NULL)
				AE_RET(start->compare(start, stop, &cmp));
			AE_RET(start->remove(start));
		} while (cmp < 0 && (ret = start->next(start)) == 0);
		AE_RET_NOTFOUND_OK(ret);
	}
	return (0);
}

/*
 * __ae_schema_range_truncate --
 *	AE_SESSION::truncate with a range.
 */
int
__ae_schema_range_truncate(
    AE_SESSION_IMPL *session, AE_CURSOR *start, AE_CURSOR *stop)
{
	AE_CURSOR *cursor;
	AE_DATA_SOURCE *dsrc;
	AE_DECL_RET;
	const char *uri;

	cursor = (start != NULL) ? start : stop;
	uri = cursor->internal_uri;

	if (AE_PREFIX_MATCH(uri, "file:")) {
		if (start != NULL)
			AE_CURSOR_NEEDKEY(start);
		if (stop != NULL)
			AE_CURSOR_NEEDKEY(stop);
		AE_WITH_BTREE(session, ((AE_CURSOR_BTREE *)cursor)->btree,
		    ret = __ae_btcur_range_truncate(
			(AE_CURSOR_BTREE *)start, (AE_CURSOR_BTREE *)stop));
	} else if (AE_PREFIX_MATCH(uri, "table:"))
		ret = __ae_table_range_truncate(
		    (AE_CURSOR_TABLE *)start, (AE_CURSOR_TABLE *)stop);
	else if ((dsrc = __ae_schema_get_source(session, uri)) != NULL &&
	    dsrc->range_truncate != NULL)
		ret = dsrc->range_truncate(dsrc, &session->iface, start, stop);
	else
		ret = __ae_range_truncate(start, stop);
err:
	return (ret);
}
