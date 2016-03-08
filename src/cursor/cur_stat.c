/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * The statistics identifier is an offset from a base to ensure the integer ID
 * values don't overlap (the idea is if they overlap it's easy for application
 * writers to confuse them).
 */
#define	AE_STAT_KEY_MAX(cst)	(((cst)->stats_base + (cst)->stats_count) - 1)
#define	AE_STAT_KEY_MIN(cst)	((cst)->stats_base)
#define	AE_STAT_KEY_OFFSET(cst)	((cst)->key - (cst)->stats_base)

/*
 * __curstat_print_value --
 *	Convert statistics cursor value to printable format.
 */
static int
__curstat_print_value(AE_SESSION_IMPL *session, uint64_t v, AE_ITEM *buf)
{
	if (v >= AE_BILLION)
		AE_RET(__ae_buf_fmt(session, buf,
		    "%" PRIu64 "B (%" PRIu64 ")", v / AE_BILLION, v));
	else if (v >= AE_MILLION)
		AE_RET(__ae_buf_fmt(session, buf,
		    "%" PRIu64 "M (%" PRIu64 ")", v / AE_MILLION, v));
	else
		AE_RET(__ae_buf_fmt(session, buf, "%" PRIu64, v));

	return (0);
}

/*
 * __curstat_free_config --
 *	Free the saved configuration string stack
 */
static void
__curstat_free_config(AE_SESSION_IMPL *session, AE_CURSOR_STAT *cst)
{
	size_t i;

	if (cst->cfg != NULL) {
		for (i = 0; cst->cfg[i] != NULL; ++i)
			__ae_free(session, cst->cfg[i]);
		__ae_free(session, cst->cfg);
	}
}

/*
 * __curstat_get_key --
 *	AE_CURSOR->get_key for statistics cursors.
 */
static int
__curstat_get_key(AE_CURSOR *cursor, ...)
{
	AE_CURSOR_STAT *cst;
	AE_DECL_RET;
	AE_ITEM *item;
	AE_SESSION_IMPL *session;
	size_t size;
	va_list ap;

	cst = (AE_CURSOR_STAT *)cursor;
	va_start(ap, cursor);
	CURSOR_API_CALL(cursor, session, get_key, NULL);

	AE_CURSOR_NEEDKEY(cursor);

	if (F_ISSET(cursor, AE_CURSTD_RAW)) {
		AE_ERR(__ae_struct_size(
		    session, &size, cursor->key_format, cst->key));
		AE_ERR(__ae_buf_initsize(session, &cursor->key, size));
		AE_ERR(__ae_struct_pack(session, cursor->key.mem, size,
		    cursor->key_format, cst->key));

		item = va_arg(ap, AE_ITEM *);
		item->data = cursor->key.data;
		item->size = cursor->key.size;
	} else
		*va_arg(ap, int *) = cst->key;

err:	va_end(ap);
	API_END_RET(session, ret);
}

/*
 * __curstat_get_value --
 *	AE_CURSOR->get_value for statistics cursors.
 */
static int
__curstat_get_value(AE_CURSOR *cursor, ...)
{
	AE_CURSOR_STAT *cst;
	AE_DECL_RET;
	AE_ITEM *item;
	AE_SESSION_IMPL *session;
	va_list ap;
	size_t size;
	uint64_t *v;
	const char *desc, **p;

	cst = (AE_CURSOR_STAT *)cursor;
	va_start(ap, cursor);
	CURSOR_API_CALL(cursor, session, get_value, NULL);

	AE_CURSOR_NEEDVALUE(cursor);

	AE_ERR(cst->stats_desc(cst, AE_STAT_KEY_OFFSET(cst), &desc));
	if (F_ISSET(cursor, AE_CURSTD_RAW)) {
		AE_ERR(__ae_struct_size(session, &size, cursor->value_format,
		    desc, cst->pv.data, cst->v));
		AE_ERR(__ae_buf_initsize(session, &cursor->value, size));
		AE_ERR(__ae_struct_pack(session, cursor->value.mem, size,
		    cursor->value_format, desc, cst->pv.data, cst->v));

		item = va_arg(ap, AE_ITEM *);
		item->data = cursor->value.data;
		item->size = cursor->value.size;
	} else {
		/*
		 * Don't drop core if the statistics value isn't requested; NULL
		 * pointer support isn't documented, but it's a cheap test.
		 */
		if ((p = va_arg(ap, const char **)) != NULL)
			*p = desc;
		if ((p = va_arg(ap, const char **)) != NULL)
			*p = cst->pv.data;
		if ((v = va_arg(ap, uint64_t *)) != NULL)
			*v = cst->v;
	}

err:	va_end(ap);
	API_END_RET(session, ret);
}

/*
 * __curstat_set_key --
 *	AE_CURSOR->set_key for statistics cursors.
 */
static void
__curstat_set_key(AE_CURSOR *cursor, ...)
{
	AE_CURSOR_STAT *cst;
	AE_DECL_RET;
	AE_ITEM *item;
	AE_SESSION_IMPL *session;
	va_list ap;

	cst = (AE_CURSOR_STAT *)cursor;
	CURSOR_API_CALL(cursor, session, set_key, NULL);
	F_CLR(cursor, AE_CURSTD_KEY_SET);

	va_start(ap, cursor);
	if (F_ISSET(cursor, AE_CURSTD_RAW)) {
		item = va_arg(ap, AE_ITEM *);
		ret = __ae_struct_unpack(session, item->data, item->size,
		    cursor->key_format, &cst->key);
	} else
		cst->key = va_arg(ap, int);
	va_end(ap);

	if ((cursor->saved_err = ret) == 0)
		F_SET(cursor, AE_CURSTD_KEY_EXT);

err:	API_END(session, ret);
}

/*
 * __curstat_set_value --
 *	AE_CURSOR->set_value for statistics cursors.
 */
static void
__curstat_set_value(AE_CURSOR *cursor, ...)
{
	AE_UNUSED(cursor);
	return;
}

/*
 * __curstat_next --
 *	AE_CURSOR->next method for the statistics cursor type.
 */
static int
__curstat_next(AE_CURSOR *cursor)
{
	AE_CURSOR_STAT *cst;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	cst = (AE_CURSOR_STAT *)cursor;
	CURSOR_API_CALL(cursor, session, next, NULL);

	/* Initialize on demand. */
	if (cst->notinitialized) {
		AE_ERR(__ae_curstat_init(
		    session, cursor->internal_uri, NULL, cst->cfg, cst));
		if (cst->next_set != NULL)
			AE_ERR((*cst->next_set)(session, cst, true, true));
		cst->notinitialized = false;
	}

	/* Move to the next item. */
	if (cst->notpositioned) {
		cst->notpositioned = false;
		cst->key = AE_STAT_KEY_MIN(cst);
	} else if (cst->key < AE_STAT_KEY_MAX(cst))
		++cst->key;
	else if (cst->next_set != NULL)
		AE_ERR((*cst->next_set)(session, cst, true, false));
	else
		AE_ERR(AE_NOTFOUND);

	cst->v = (uint64_t)cst->stats[AE_STAT_KEY_OFFSET(cst)];
	AE_ERR(__curstat_print_value(session, cst->v, &cst->pv));
	F_SET(cursor, AE_CURSTD_KEY_INT | AE_CURSTD_VALUE_INT);

	if (0) {
err:		F_CLR(cursor, AE_CURSTD_KEY_SET | AE_CURSTD_VALUE_SET);
	}
	API_END_RET(session, ret);
}

/*
 * __curstat_prev --
 *	AE_CURSOR->prev method for the statistics cursor type.
 */
static int
__curstat_prev(AE_CURSOR *cursor)
{
	AE_CURSOR_STAT *cst;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	cst = (AE_CURSOR_STAT *)cursor;
	CURSOR_API_CALL(cursor, session, prev, NULL);

	/* Initialize on demand. */
	if (cst->notinitialized) {
		AE_ERR(__ae_curstat_init(
		    session, cursor->internal_uri, NULL, cst->cfg, cst));
		if (cst->next_set != NULL)
			AE_ERR((*cst->next_set)(session, cst, false, true));
		cst->notinitialized = false;
	}

	/* Move to the previous item. */
	if (cst->notpositioned) {
		cst->notpositioned = false;
		cst->key = AE_STAT_KEY_MAX(cst);
	} else if (cst->key > AE_STAT_KEY_MIN(cst))
		--cst->key;
	else if (cst->next_set != NULL)
		AE_ERR((*cst->next_set)(session, cst, false, false));
	else
		AE_ERR(AE_NOTFOUND);

	cst->v = (uint64_t)cst->stats[AE_STAT_KEY_OFFSET(cst)];
	AE_ERR(__curstat_print_value(session, cst->v, &cst->pv));
	F_SET(cursor, AE_CURSTD_KEY_INT | AE_CURSTD_VALUE_INT);

	if (0) {
err:		F_CLR(cursor, AE_CURSTD_KEY_SET | AE_CURSTD_VALUE_SET);
	}
	API_END_RET(session, ret);
}

/*
 * __curstat_reset --
 *	AE_CURSOR->reset method for the statistics cursor type.
 */
static int
__curstat_reset(AE_CURSOR *cursor)
{
	AE_CURSOR_STAT *cst;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	cst = (AE_CURSOR_STAT *)cursor;
	CURSOR_API_CALL(cursor, session, reset, NULL);

	cst->notinitialized = cst->notpositioned = true;
	F_CLR(cursor, AE_CURSTD_KEY_SET | AE_CURSTD_VALUE_SET);

err:	API_END_RET(session, ret);
}

/*
 * __curstat_search --
 *	AE_CURSOR->search method for the statistics cursor type.
 */
static int
__curstat_search(AE_CURSOR *cursor)
{
	AE_CURSOR_STAT *cst;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	cst = (AE_CURSOR_STAT *)cursor;
	CURSOR_API_CALL(cursor, session, search, NULL);

	AE_CURSOR_NEEDKEY(cursor);
	F_CLR(cursor, AE_CURSTD_VALUE_SET | AE_CURSTD_VALUE_SET);

	/* Initialize on demand. */
	if (cst->notinitialized) {
		AE_ERR(__ae_curstat_init(
		    session, cursor->internal_uri, NULL, cst->cfg, cst));
		cst->notinitialized = false;
	}

	if (cst->key < AE_STAT_KEY_MIN(cst) || cst->key > AE_STAT_KEY_MAX(cst))
		AE_ERR(AE_NOTFOUND);

	cst->v = (uint64_t)cst->stats[AE_STAT_KEY_OFFSET(cst)];
	AE_ERR(__curstat_print_value(session, cst->v, &cst->pv));
	F_SET(cursor, AE_CURSTD_KEY_INT | AE_CURSTD_VALUE_INT);

err:	API_END_RET(session, ret);
}

/*
 * __curstat_close --
 *	AE_CURSOR->close method for the statistics cursor type.
 */
static int
__curstat_close(AE_CURSOR *cursor)
{
	AE_CURSOR_STAT *cst;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	cst = (AE_CURSOR_STAT *)cursor;
	CURSOR_API_CALL(cursor, session, close, NULL);

	__curstat_free_config(session, cst);

	__ae_buf_free(session, &cst->pv);
	__ae_free(session, cst->desc_buf);

	AE_ERR(__ae_cursor_close(cursor));

err:	API_END_RET(session, ret);
}

/*
 * __curstat_conn_init --
 *	Initialize the statistics for a connection.
 */
static void
__curstat_conn_init(AE_SESSION_IMPL *session, AE_CURSOR_STAT *cst)
{
	AE_CONNECTION_IMPL *conn;

	conn = S2C(session);

	/*
	 * Fill in the connection statistics, and copy them to the cursor.
	 * Optionally clear the connection statistics.
	 */
	__ae_conn_stat_init(session);
	__ae_stat_connection_aggregate(conn->stats, &cst->u.conn_stats);
	if (F_ISSET(cst, AE_CONN_STAT_CLEAR))
		__ae_stat_connection_clear_all(conn->stats);

	cst->stats = (int64_t *)&cst->u.conn_stats;
	cst->stats_base = AE_CONNECTION_STATS_BASE;
	cst->stats_count = sizeof(AE_CONNECTION_STATS) / sizeof(int64_t);
	cst->stats_desc = __ae_stat_connection_desc;
}

/*
 * __curstat_file_init --
 *	Initialize the statistics for a file.
 */
static int
__curstat_file_init(AE_SESSION_IMPL *session,
    const char *uri, const char *cfg[], AE_CURSOR_STAT *cst)
{
	AE_DATA_HANDLE *dhandle;
	AE_DECL_RET;
	const char *filename;

	/*
	 * If we are only getting the size of the file, we don't need to open
	 * the tree.
	 */
	if (F_ISSET(cst, AE_CONN_STAT_SIZE)) {
		filename = uri;
		if (!AE_PREFIX_SKIP(filename, "file:"))
			return (EINVAL);
		__ae_stat_dsrc_init_single(&cst->u.dsrc_stats);
		AE_RET(__ae_block_manager_size(
		    session, filename, &cst->u.dsrc_stats));
		__ae_curstat_dsrc_final(cst);
		return (0);
	}

	AE_RET(__ae_session_get_btree_ckpt(session, uri, cfg, 0));
	dhandle = session->dhandle;

	/*
	 * Fill in the data source statistics, and copy them to the cursor.
	 * Optionally clear the data source statistics.
	 */
	if ((ret = __ae_btree_stat_init(session, cst)) == 0) {
		__ae_stat_dsrc_init_single(&cst->u.dsrc_stats);
		__ae_stat_dsrc_aggregate(dhandle->stats, &cst->u.dsrc_stats);
		if (F_ISSET(cst, AE_CONN_STAT_CLEAR))
			__ae_stat_dsrc_clear_all(dhandle->stats);
		__ae_curstat_dsrc_final(cst);
	}

	/* Release the handle, we're done with it. */
	AE_TRET(__ae_session_release_btree(session));

	return (ret);
}

/*
 * __ae_curstat_dsrc_final --
 *	Finalize a data-source statistics cursor.
 */
void
__ae_curstat_dsrc_final(AE_CURSOR_STAT *cst)
{
	cst->stats = (int64_t *)&cst->u.dsrc_stats;
	cst->stats_base = AE_DSRC_STATS_BASE;
	cst->stats_count = sizeof(AE_DSRC_STATS) / sizeof(int64_t);
	cst->stats_desc = __ae_stat_dsrc_desc;
}

/*
 * __curstat_join_next_set --
 *	Advance to another index used in a join to give another set of
 *	statistics.
 */
static int
__curstat_join_next_set(AE_SESSION_IMPL *session, AE_CURSOR_STAT *cst,
    bool forw, bool init)
{
	AE_CURSOR_JOIN *cjoin;
	AE_JOIN_STATS_GROUP *join_group;
	ssize_t pos;

	AE_ASSERT(session, AE_STREQ(cst->iface.uri, "statistics:join"));
	join_group = &cst->u.join_stats_group;
	cjoin = join_group->join_cursor;
	if (init)
		pos = forw ? 0 : (ssize_t)cjoin->entries_next - 1;
	else
		pos = join_group->join_cursor_entry + (forw ? 1 : -1);
	if (pos < 0 || (size_t)pos >= cjoin->entries_next)
		return (AE_NOTFOUND);

	join_group->join_cursor_entry = pos;
	if (cjoin->entries[pos].index == NULL) {
		AE_ASSERT(session, AE_PREFIX_MATCH(cjoin->iface.uri, "join:"));
		join_group->desc_prefix = cjoin->iface.uri + 5;
	} else
		join_group->desc_prefix = cjoin->entries[pos].index->name;
	join_group->join_stats = cjoin->entries[pos].stats;
	if (!init)
		cst->key = forw ? AE_STAT_KEY_MIN(cst) : AE_STAT_KEY_MAX(cst);
	return (0);
}

/*
 * __curstat_join_desc --
 *	Assemble the description field based on current index and statistic.
 */
static int
__curstat_join_desc(AE_CURSOR_STAT *cst, int slot, const char **resultp)
{
	size_t len;
	const char *static_desc;
	AE_JOIN_STATS_GROUP *sgrp;
	AE_SESSION_IMPL *session;

	sgrp = &cst->u.join_stats_group;
	session = (AE_SESSION_IMPL *)sgrp->join_cursor->iface.session;
	AE_RET(__ae_stat_join_desc(cst, slot, &static_desc));
	len = strlen("join: ") + strlen(sgrp->desc_prefix) +
	    strlen(static_desc) + 1;
	AE_RET(__ae_realloc(session, NULL, len, &cst->desc_buf));
	snprintf(cst->desc_buf, len, "join: %s%s", sgrp->desc_prefix,
	    static_desc);
	*resultp = cst->desc_buf;
	return (0);
}

/*
 * __curstat_join_init --
 *	Initialize the statistics for a joined cursor.
 */
static int
__curstat_join_init(AE_SESSION_IMPL *session,
    AE_CURSOR *curjoin, const char *cfg[], AE_CURSOR_STAT *cst)
{
	AE_CURSOR_JOIN *cjoin;
	AE_DECL_RET;

	AE_UNUSED(cfg);

	if (curjoin == NULL && cst->u.join_stats_group.join_cursor != NULL)
		curjoin = &cst->u.join_stats_group.join_cursor->iface;
	if (curjoin == NULL || !AE_PREFIX_MATCH(curjoin->uri, "join:"))
		AE_ERR_MSG(session, EINVAL,
		    "join cursor must be used with statistics:join");
	cjoin = (AE_CURSOR_JOIN *)curjoin;
	memset(&cst->u.join_stats_group, 0, sizeof(AE_JOIN_STATS_GROUP));
	cst->u.join_stats_group.join_cursor = cjoin;

	cst->stats = (int64_t *)&cst->u.join_stats_group.join_stats;
	cst->stats_base = AE_JOIN_STATS_BASE;
	cst->stats_count = sizeof(AE_JOIN_STATS) / sizeof(int64_t);
	cst->stats_desc = __curstat_join_desc;
	cst->next_set = __curstat_join_next_set;

err:	return (ret);
}

/*
 * __ae_curstat_init --
 *	Initialize a statistics cursor.
 */
int
__ae_curstat_init(AE_SESSION_IMPL *session,
    const char *uri, AE_CURSOR *curjoin, const char *cfg[], AE_CURSOR_STAT *cst)
{
	const char *dsrc_uri;

	if (strcmp(uri, "statistics:") == 0) {
		__curstat_conn_init(session, cst);
		return (0);
	}

	dsrc_uri = uri + strlen("statistics:");

	if (AE_STREQ(dsrc_uri, "join"))
		return (__curstat_join_init(session, curjoin, cfg, cst));

	if (AE_PREFIX_MATCH(dsrc_uri, "colgroup:"))
		return (
		    __ae_curstat_colgroup_init(session, dsrc_uri, cfg, cst));

	if (AE_PREFIX_MATCH(dsrc_uri, "file:"))
		return (__curstat_file_init(session, dsrc_uri, cfg, cst));

	if (AE_PREFIX_MATCH(dsrc_uri, "index:"))
		return (__ae_curstat_index_init(session, dsrc_uri, cfg, cst));

	if (AE_PREFIX_MATCH(dsrc_uri, "lsm:"))
		return (__ae_curstat_lsm_init(session, dsrc_uri, cst));

	if (AE_PREFIX_MATCH(dsrc_uri, "table:"))
		return (__ae_curstat_table_init(session, dsrc_uri, cfg, cst));

	return (__ae_bad_object_type(session, uri));
}

/*
 * __ae_curstat_open --
 *	AE_SESSION->open_cursor method for the statistics cursor type.
 */
int
__ae_curstat_open(AE_SESSION_IMPL *session,
    const char *uri, AE_CURSOR *other, const char *cfg[], AE_CURSOR **cursorp)
{
	AE_CONNECTION_IMPL *conn;
	AE_CURSOR_STATIC_INIT(iface,
	    __curstat_get_key,		/* get-key */
	    __curstat_get_value,	/* get-value */
	    __curstat_set_key,		/* set-key */
	    __curstat_set_value,	/* set-value */
	    __ae_cursor_notsup,		/* compare */
	    __ae_cursor_notsup,		/* equals */
	    __curstat_next,		/* next */
	    __curstat_prev,		/* prev */
	    __curstat_reset,		/* reset */
	    __curstat_search,		/* search */
	    __ae_cursor_notsup,		/* search-near */
	    __ae_cursor_notsup,		/* insert */
	    __ae_cursor_notsup,		/* update */
	    __ae_cursor_notsup,		/* remove */
	    __ae_cursor_notsup,		/* reconfigure */
	    __curstat_close);		/* close */
	AE_CONFIG_ITEM cval, sval;
	AE_CURSOR *cursor;
	AE_CURSOR_STAT *cst;
	AE_DECL_RET;
	size_t i;

	AE_STATIC_ASSERT(offsetof(AE_CURSOR_STAT, iface) == 0);

	conn = S2C(session);

	AE_RET(__ae_calloc_one(session, &cst));
	cursor = &cst->iface;
	*cursor = iface;
	cursor->session = &session->iface;

	/*
	 * Statistics cursor configuration: must match (and defaults to), the
	 * database configuration.
	 */
	if (FLD_ISSET(conn->stat_flags, AE_CONN_STAT_NONE))
		goto config_err;
	if ((ret = __ae_config_gets(session, cfg, "statistics", &cval)) == 0) {
		if ((ret = __ae_config_subgets(
		    session, &cval, "all", &sval)) == 0 && sval.val != 0) {
			if (!FLD_ISSET(conn->stat_flags, AE_CONN_STAT_ALL))
				goto config_err;
			F_SET(cst, AE_CONN_STAT_ALL | AE_CONN_STAT_FAST);
		}
		AE_ERR_NOTFOUND_OK(ret);
		if ((ret = __ae_config_subgets(
		    session, &cval, "fast", &sval)) == 0 && sval.val != 0) {
			if (F_ISSET(cst, AE_CONN_STAT_ALL))
				AE_ERR_MSG(session, EINVAL,
				    "only one statistics configuration value "
				    "may be specified");
			F_SET(cst, AE_CONN_STAT_FAST);
		}
		AE_ERR_NOTFOUND_OK(ret);
		if ((ret = __ae_config_subgets(
		    session, &cval, "size", &sval)) == 0 && sval.val != 0) {
			if (F_ISSET(cst, AE_CONN_STAT_FAST | AE_CONN_STAT_ALL))
				AE_ERR_MSG(session, EINVAL,
				    "only one statistics configuration value "
				    "may be specified");
			F_SET(cst, AE_CONN_STAT_SIZE);
		}
		AE_ERR_NOTFOUND_OK(ret);
		if ((ret = __ae_config_subgets(
		    session, &cval, "clear", &sval)) == 0 && sval.val != 0) {
			if (F_ISSET(cst, AE_CONN_STAT_SIZE))
				AE_ERR_MSG(session, EINVAL,
				    "clear is incompatible with size "
				    "statistics");
			F_SET(cst, AE_CONN_STAT_CLEAR);
		}
		AE_ERR_NOTFOUND_OK(ret);

		/* If no configuration, use the connection's configuration. */
		if (cst->flags == 0) {
			if (FLD_ISSET(conn->stat_flags, AE_CONN_STAT_ALL))
				F_SET(cst, AE_CONN_STAT_ALL);
			if (FLD_ISSET(conn->stat_flags, AE_CONN_STAT_FAST))
				F_SET(cst, AE_CONN_STAT_FAST);
		}

		/* If the connection configures clear, so do we. */
		if (FLD_ISSET(conn->stat_flags, AE_CONN_STAT_CLEAR))
			F_SET(cst, AE_CONN_STAT_CLEAR);
	}

	/*
	 * We return the statistics field's offset as the key, and a string
	 * description, a string value,  and a uint64_t value as the value
	 * columns.
	 */
	cursor->key_format = "i";
	cursor->value_format = "SSq";

	/*
	 * AE_CURSOR.reset on a statistics cursor refreshes the cursor, save
	 * the cursor's configuration for that.
	 */
	for (i = 0; cfg[i] != NULL; ++i)
		;
	AE_ERR(__ae_calloc_def(session, i + 1, &cst->cfg));
	for (i = 0; cfg[i] != NULL; ++i)
		AE_ERR(__ae_strdup(session, cfg[i], &cst->cfg[i]));

	/*
	 * Do the initial statistics snapshot: there won't be cursor operations
	 * to trigger initialization when aggregating statistics for upper-level
	 * objects like tables, we need to a valid set of statistics when before
	 * the open returns.
	 */
	AE_ERR(__ae_curstat_init(session, uri, other, cst->cfg, cst));
	cst->notinitialized = false;

	/* The cursor isn't yet positioned. */
	cst->notpositioned = true;

	/* __ae_cursor_init is last so we don't have to clean up on error. */
	AE_ERR(__ae_cursor_init(cursor, uri, NULL, cfg, cursorp));

	if (0) {
config_err:	AE_ERR_MSG(session, EINVAL,
		    "cursor's statistics configuration doesn't match the "
		    "database statistics configuration");
	}

	if (0) {
err:		__curstat_free_config(session, cst);
		__ae_free(session, cst);
	}

	return (ret);
}
