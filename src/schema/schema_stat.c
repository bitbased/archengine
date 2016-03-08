/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_curstat_colgroup_init --
 *	Initialize the statistics for a column group.
 */
int
__ae_curstat_colgroup_init(AE_SESSION_IMPL *session,
    const char *uri, const char *cfg[], AE_CURSOR_STAT *cst)
{
	AE_COLGROUP *colgroup;
	AE_DECL_ITEM(buf);
	AE_DECL_RET;

	AE_RET(__ae_schema_get_colgroup(session, uri, false, NULL, &colgroup));

	AE_RET(__ae_scr_alloc(session, 0, &buf));
	AE_ERR(__ae_buf_fmt(session, buf, "statistics:%s", colgroup->source));
	ret = __ae_curstat_init(session, buf->data, NULL, cfg, cst);

err:	__ae_scr_free(session, &buf);
	return (ret);
}

/*
 * __ae_curstat_index_init --
 *	Initialize the statistics for an index.
 */
int
__ae_curstat_index_init(AE_SESSION_IMPL *session,
    const char *uri, const char *cfg[], AE_CURSOR_STAT *cst)
{
	AE_DECL_ITEM(buf);
	AE_DECL_RET;
	AE_INDEX *idx;

	AE_RET(__ae_schema_get_index(session, uri, false, NULL, &idx));

	AE_RET(__ae_scr_alloc(session, 0, &buf));
	AE_ERR(__ae_buf_fmt(session, buf, "statistics:%s", idx->source));
	ret = __ae_curstat_init(session, buf->data, NULL, cfg, cst);

err:	__ae_scr_free(session, &buf);
	return (ret);
}

/*
 * __curstat_size_only --
 *	 For very simple tables we can avoid getting table handles if
 *	 configured to only retrieve the size. It's worthwhile because
 *	 workloads that create and drop a lot of tables can put a lot of
 *	 pressure on the table list lock.
 */
static int
__curstat_size_only(AE_SESSION_IMPL *session,
    const char *uri, bool *was_fast,AE_CURSOR_STAT *cst)
{
	AE_CONFIG cparser;
	AE_CONFIG_ITEM ckey, colconf, cval;
	AE_DECL_RET;
	AE_ITEM namebuf;
	ae_off_t filesize;
	char *tableconf;

	AE_CLEAR(namebuf);
	*was_fast = false;

	/* Retrieve the metadata for this table. */
	AE_RET(__ae_metadata_search(session, uri, &tableconf));

	/*
	 * The fast path only works if the table consists of a single file
	 * and does not have any indexes. The absence of named columns is how
	 * we determine that neither of those conditions can be satisfied.
	 */
	AE_ERR(__ae_config_getones(session, tableconf, "columns", &colconf));
	AE_ERR(__ae_config_subinit(session, &cparser, &colconf));
	if ((ret = __ae_config_next(&cparser, &ckey, &cval)) == 0)
		goto err;

	/* Build up the file name from the table URI. */
	AE_ERR(__ae_buf_fmt(
	    session, &namebuf, "%s.ae", uri + strlen("table:")));

	/*
	 * Get the size of the underlying file. This will fail for anything
	 * other than simple tables (LSM for example) and will fail if there
	 * are concurrent schema level operations (for example drop). That is
	 * fine - failing here results in falling back to the slow path of
	 * opening the handle.
	 * !!! Deliberately discard the return code from a failed call - the
	 * error is flagged by not setting fast to true.
	 */
	if (__ae_filesize_name(session, namebuf.data, true, &filesize) == 0) {
		/* Setup and populate the statistics structure */
		__ae_stat_dsrc_init_single(&cst->u.dsrc_stats);
		cst->u.dsrc_stats.block_size = filesize;
		__ae_curstat_dsrc_final(cst);

		*was_fast = true;
	}

err:	__ae_free(session, tableconf);
	__ae_buf_free(session, &namebuf);

	return (ret);
}

/*
 * __ae_curstat_table_init --
 *	Initialize the statistics for a table.
 */
int
__ae_curstat_table_init(AE_SESSION_IMPL *session,
    const char *uri, const char *cfg[], AE_CURSOR_STAT *cst)
{
	AE_CURSOR *stat_cursor;
	AE_DECL_ITEM(buf);
	AE_DECL_RET;
	AE_DSRC_STATS *new, *stats;
	AE_TABLE *table;
	u_int i;
	const char *name;
	bool was_fast;

	/*
	 * If only gathering table size statistics, try a fast path that
	 * avoids the schema and table list locks.
	 */
	if (F_ISSET(cst, AE_CONN_STAT_SIZE)) {
		AE_RET(__curstat_size_only(session, uri, &was_fast, cst));
		if (was_fast)
			return (0);
	}

	name = uri + strlen("table:");
	AE_RET(__ae_schema_get_table(
	    session, name, strlen(name), false, &table));

	AE_ERR(__ae_scr_alloc(session, 0, &buf));

	/*
	 * Process the column groups.
	 *
	 * Set the cursor to reference the data source statistics; we don't
	 * initialize it, instead we copy (rather than aggregate), the first
	 * column's statistics, which has the same effect.
	 */
	stats = &cst->u.dsrc_stats;
	for (i = 0; i < AE_COLGROUPS(table); i++) {
		AE_ERR(__ae_buf_fmt(
		    session, buf, "statistics:%s", table->cgroups[i]->name));
		AE_ERR(__ae_curstat_open(
		    session, buf->data, NULL, cfg, &stat_cursor));
		new = (AE_DSRC_STATS *)AE_CURSOR_STATS(stat_cursor);
		if (i == 0)
			*stats = *new;
		else
			__ae_stat_dsrc_aggregate_single(new, stats);
		AE_ERR(stat_cursor->close(stat_cursor));
	}

	/* Process the indices. */
	AE_ERR(__ae_schema_open_indices(session, table));
	for (i = 0; i < table->nindices; i++) {
		AE_ERR(__ae_buf_fmt(
		    session, buf, "statistics:%s", table->indices[i]->name));
		AE_ERR(__ae_curstat_open(
		    session, buf->data, NULL, cfg, &stat_cursor));
		new = (AE_DSRC_STATS *)AE_CURSOR_STATS(stat_cursor);
		__ae_stat_dsrc_aggregate_single(new, stats);
		AE_ERR(stat_cursor->close(stat_cursor));
	}

	__ae_curstat_dsrc_final(cst);

err:	__ae_schema_release_table(session, table);

	__ae_scr_free(session, &buf);
	return (ret);
}
