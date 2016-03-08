/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __curds_txn_enter --
 *	Do transactional initialization when starting an operation.
 */
static int
__curds_txn_enter(AE_SESSION_IMPL *session)
{
	session->ncursors++;				/* XXX */
	__ae_txn_cursor_op(session);

	return (0);
}

/*
 * __curds_txn_leave --
 *	Do transactional cleanup when ending an operation.
 */
static void
__curds_txn_leave(AE_SESSION_IMPL *session)
{
	if (--session->ncursors == 0)			/* XXX */
		__ae_txn_read_last(session);
}

/*
 * __curds_key_set --
 *	Set the key for the data-source.
 */
static int
__curds_key_set(AE_CURSOR *cursor)
{
	AE_CURSOR *source;
	AE_DECL_RET;

	source = ((AE_CURSOR_DATA_SOURCE *)cursor)->source;

	AE_CURSOR_NEEDKEY(cursor);

	source->recno = cursor->recno;
	source->key.data = cursor->key.data;
	source->key.size = cursor->key.size;

err:	return (ret);
}

/*
 * __curds_value_set --
 *	Set the value for the data-source.
 */
static int
__curds_value_set(AE_CURSOR *cursor)
{
	AE_CURSOR *source;
	AE_DECL_RET;

	source = ((AE_CURSOR_DATA_SOURCE *)cursor)->source;

	AE_CURSOR_NEEDVALUE(cursor);

	source->value.data = cursor->value.data;
	source->value.size = cursor->value.size;

err:	return (ret);
}

/*
 * __curds_cursor_resolve --
 *	Resolve cursor operation.
 */
static int
__curds_cursor_resolve(AE_CURSOR *cursor, int ret)
{
	AE_CURSOR *source;

	source = ((AE_CURSOR_DATA_SOURCE *)cursor)->source;

	/*
	 * Update the cursor's key, value and flags.  (We use the _INT flags in
	 * the same way as file objects: there's some chance the underlying data
	 * source is passing us a reference to data only pinned per operation,
	 * might as well be safe.)
	 *
	 * There's also a requirement the underlying data-source never returns
	 * with the cursor/source key referencing application memory: it'd be
	 * great to do a copy as necessary here so the data-source doesn't have
	 * to worry about copying the key, but we don't have enough information
	 * to know if a cursor is pointing at application or data-source memory.
	 */
	if (ret == 0) {
		cursor->key.data = source->key.data;
		cursor->key.size = source->key.size;
		cursor->value.data = source->value.data;
		cursor->value.size = source->value.size;
		cursor->recno = source->recno;

		F_CLR(cursor, AE_CURSTD_KEY_EXT | AE_CURSTD_VALUE_EXT);
		F_SET(cursor, AE_CURSTD_KEY_INT | AE_CURSTD_VALUE_INT);
	} else {
		if (ret == AE_NOTFOUND)
			F_CLR(cursor, AE_CURSTD_KEY_SET | AE_CURSTD_VALUE_SET);
		else
			F_CLR(cursor, AE_CURSTD_KEY_INT | AE_CURSTD_VALUE_INT);

		/*
		 * Cursor operation failure implies a lost cursor position and
		 * a subsequent next/prev starting at the beginning/end of the
		 * table.  We simplify underlying data source implementations
		 * by resetting the cursor explicitly here.
		 */
		AE_TRET(source->reset(source));
	}

	return (ret);
}

/*
 * __curds_compare --
 *	AE_CURSOR.compare method for the data-source cursor type.
 */
static int
__curds_compare(AE_CURSOR *a, AE_CURSOR *b, int *cmpp)
{
	AE_COLLATOR *collator;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	CURSOR_API_CALL(a, session, compare, NULL);

	/*
	 * Confirm both cursors refer to the same source and have keys, then
	 * compare them.
	 */
	if (strcmp(a->internal_uri, b->internal_uri) != 0)
		AE_ERR_MSG(session, EINVAL,
		    "Cursors must reference the same object");

	AE_CURSOR_NEEDKEY(a);
	AE_CURSOR_NEEDKEY(b);

	if (AE_CURSOR_RECNO(a)) {
		if (a->recno < b->recno)
			*cmpp = -1;
		else if (a->recno == b->recno)
			*cmpp = 0;
		else
			*cmpp = 1;
	} else {
		/*
		 * The assumption is data-sources don't provide ArchEngine with
		 * AE_CURSOR.compare methods, instead, we'll copy the key/value
		 * out of the underlying data-source cursor and any comparison
		 * to be done can be done at this level.
		 */
		collator = ((AE_CURSOR_DATA_SOURCE *)a)->collator;
		AE_ERR(__ae_compare(
		    session, collator, &a->key, &b->key, cmpp));
	}

err:	API_END_RET(session, ret);
}

/*
 * __curds_next --
 *	AE_CURSOR.next method for the data-source cursor type.
 */
static int
__curds_next(AE_CURSOR *cursor)
{
	AE_CURSOR *source;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	source = ((AE_CURSOR_DATA_SOURCE *)cursor)->source;

	CURSOR_API_CALL(cursor, session, next, NULL);

	AE_STAT_FAST_CONN_INCR(session, cursor_next); 
	AE_STAT_FAST_DATA_INCR(session, cursor_next);

	AE_ERR(__curds_txn_enter(session));

	F_CLR(cursor, AE_CURSTD_KEY_SET | AE_CURSTD_VALUE_SET);         
	ret = __curds_cursor_resolve(cursor, source->next(source));

err:	__curds_txn_leave(session);

	API_END_RET(session, ret);
}

/*
 * __curds_prev --
 *	AE_CURSOR.prev method for the data-source cursor type.
 */
static int
__curds_prev(AE_CURSOR *cursor)
{
	AE_CURSOR *source;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	source = ((AE_CURSOR_DATA_SOURCE *)cursor)->source;

	CURSOR_API_CALL(cursor, session, prev, NULL);

	AE_STAT_FAST_CONN_INCR(session, cursor_prev);
	AE_STAT_FAST_DATA_INCR(session, cursor_prev);

	AE_ERR(__curds_txn_enter(session));

	F_CLR(cursor, AE_CURSTD_KEY_SET | AE_CURSTD_VALUE_SET);         
	ret = __curds_cursor_resolve(cursor, source->prev(source));

err:	__curds_txn_leave(session);
	API_END_RET(session, ret);
}

/*
 * __curds_reset --
 *	AE_CURSOR.reset method for the data-source cursor type.
 */
static int
__curds_reset(AE_CURSOR *cursor)
{
	AE_CURSOR *source;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	source = ((AE_CURSOR_DATA_SOURCE *)cursor)->source;

	CURSOR_API_CALL(cursor, session, reset, NULL);

	AE_STAT_FAST_CONN_INCR(session, cursor_reset);      
	AE_STAT_FAST_DATA_INCR(session, cursor_reset);

	AE_ERR(source->reset(source));

	F_CLR(cursor, AE_CURSTD_KEY_SET | AE_CURSTD_VALUE_SET);

err:	API_END_RET(session, ret);
}

/*
 * __curds_search --
 *	AE_CURSOR.search method for the data-source cursor type.
 */
static int
__curds_search(AE_CURSOR *cursor)
{
	AE_CURSOR *source;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	source = ((AE_CURSOR_DATA_SOURCE *)cursor)->source;

	CURSOR_API_CALL(cursor, session, search, NULL);

	AE_STAT_FAST_CONN_INCR(session, cursor_search);
	AE_STAT_FAST_DATA_INCR(session, cursor_search);

	AE_ERR(__curds_txn_enter(session));

	AE_ERR(__curds_key_set(cursor));
	ret = __curds_cursor_resolve(cursor, source->search(source));

err:	__curds_txn_leave(session);

	API_END_RET(session, ret);
}

/*
 * __curds_search_near --
 *	AE_CURSOR.search_near method for the data-source cursor type.
 */
static int
__curds_search_near(AE_CURSOR *cursor, int *exact)
{
	AE_CURSOR *source;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	source = ((AE_CURSOR_DATA_SOURCE *)cursor)->source;

	CURSOR_API_CALL(cursor, session, search_near, NULL);

	AE_STAT_FAST_CONN_INCR(session, cursor_search_near);
	AE_STAT_FAST_DATA_INCR(session, cursor_search_near);

	AE_ERR(__curds_txn_enter(session));

	AE_ERR(__curds_key_set(cursor));
	ret =
	    __curds_cursor_resolve(cursor, source->search_near(source, exact));

err:	__curds_txn_leave(session);

	API_END_RET(session, ret);
}

/*
 * __curds_insert --
 *	AE_CURSOR.insert method for the data-source cursor type.
 */
static int
__curds_insert(AE_CURSOR *cursor)
{
	AE_CURSOR *source;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	source = ((AE_CURSOR_DATA_SOURCE *)cursor)->source;

	CURSOR_UPDATE_API_CALL(cursor, session, insert, NULL);

	AE_ERR(__curds_txn_enter(session));

	AE_STAT_FAST_CONN_INCR(session, cursor_insert);     
	AE_STAT_FAST_DATA_INCR(session, cursor_insert);
	AE_STAT_FAST_DATA_INCRV(session,
	    cursor_insert_bytes, cursor->key.size + cursor->value.size);

	if (!F_ISSET(cursor, AE_CURSTD_APPEND))
		AE_ERR(__curds_key_set(cursor));
	AE_ERR(__curds_value_set(cursor));
	ret = __curds_cursor_resolve(cursor, source->insert(source));

err:	__curds_txn_leave(session);

	CURSOR_UPDATE_API_END(session, ret);
	return (ret);
}

/*
 * __curds_update --
 *	AE_CURSOR.update method for the data-source cursor type.
 */
static int
__curds_update(AE_CURSOR *cursor)
{
	AE_CURSOR *source;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	source = ((AE_CURSOR_DATA_SOURCE *)cursor)->source;

	CURSOR_UPDATE_API_CALL(cursor, session, update, NULL);

	AE_STAT_FAST_CONN_INCR(session, cursor_update);     
	AE_STAT_FAST_DATA_INCR(session, cursor_update);
	AE_STAT_FAST_DATA_INCRV(
	    session, cursor_update_bytes, cursor->value.size);

	AE_ERR(__curds_txn_enter(session));

	AE_ERR(__curds_key_set(cursor));
	AE_ERR(__curds_value_set(cursor));
	ret = __curds_cursor_resolve(cursor, source->update(source));

err:	__curds_txn_leave(session);

	CURSOR_UPDATE_API_END(session, ret);
	return (ret);
}

/*
 * __curds_remove --
 *	AE_CURSOR.remove method for the data-source cursor type.
 */
static int
__curds_remove(AE_CURSOR *cursor)
{
	AE_CURSOR *source;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	source = ((AE_CURSOR_DATA_SOURCE *)cursor)->source;

	CURSOR_REMOVE_API_CALL(cursor, session, NULL);

	AE_STAT_FAST_CONN_INCR(session, cursor_remove);     
	AE_STAT_FAST_DATA_INCR(session, cursor_remove);
	AE_STAT_FAST_DATA_INCRV(session, cursor_remove_bytes, cursor->key.size);

	AE_ERR(__curds_txn_enter(session));

	AE_ERR(__curds_key_set(cursor));
	ret = __curds_cursor_resolve(cursor, source->remove(source));

err:	__curds_txn_leave(session);

	CURSOR_UPDATE_API_END(session, ret);
	return (ret);
}

/*
 * __curds_close --
 *	AE_CURSOR.close method for the data-source cursor type.
 */
static int
__curds_close(AE_CURSOR *cursor)
{
	AE_CURSOR_DATA_SOURCE *cds;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	cds = (AE_CURSOR_DATA_SOURCE *)cursor;

	CURSOR_API_CALL(cursor, session, close, NULL);

	if (cds->source != NULL)
		ret = cds->source->close(cds->source);

	if (cds->collator_owned) {
		if (cds->collator->terminate != NULL)
			AE_TRET(cds->collator->terminate(
			    cds->collator, &session->iface));
		cds->collator_owned = 0;
	}
	cds->collator = NULL;

	/*
	 * The key/value formats are in allocated memory, which isn't standard
	 * behavior.
	 */
	__ae_free(session, cursor->key_format);
	__ae_free(session, cursor->value_format);

	AE_TRET(__ae_cursor_close(cursor));

err:	API_END_RET(session, ret);
}

/*
 * __ae_curds_open --
 *	Initialize a data-source cursor.
 */
int
__ae_curds_open(
    AE_SESSION_IMPL *session, const char *uri, AE_CURSOR *owner,
    const char *cfg[], AE_DATA_SOURCE *dsrc, AE_CURSOR **cursorp)
{
	AE_CURSOR_STATIC_INIT(iface,
	    __ae_cursor_get_key,	/* get-key */
	    __ae_cursor_get_value,	/* get-value */
	    __ae_cursor_set_key,	/* set-key */
	    __ae_cursor_set_value,	/* set-value */
	    __curds_compare,		/* compare */
	    __ae_cursor_equals,		/* equals */
	    __curds_next,		/* next */
	    __curds_prev,		/* prev */
	    __curds_reset,		/* reset */
	    __curds_search,		/* search */
	    __curds_search_near,	/* search-near */
	    __curds_insert,		/* insert */
	    __curds_update,		/* update */
	    __curds_remove,		/* remove */
	    __ae_cursor_notsup,		/* reconfigure */
	    __curds_close);		/* close */
	AE_CONFIG_ITEM cval, metadata;
	AE_CURSOR *cursor, *source;
	AE_CURSOR_DATA_SOURCE *data_source;
	AE_DECL_RET;
	char *metaconf;

	AE_STATIC_ASSERT(offsetof(AE_CURSOR_DATA_SOURCE, iface) == 0);

	data_source = NULL;
	metaconf = NULL;

	AE_RET(__ae_calloc_one(session, &data_source));
	cursor = &data_source->iface;
	*cursor = iface;
	cursor->session = &session->iface;

	/*
	 * XXX
	 * The underlying data-source may require the object's key and value
	 * formats.  This isn't a particularly elegant way of getting that
	 * information to the data-source, this feels like a layering problem
	 * to me.
	 */
	AE_ERR(__ae_metadata_search(session, uri, &metaconf));
	AE_ERR(__ae_config_getones(session, metaconf, "key_format", &cval));
	AE_ERR(__ae_strndup(session, cval.str, cval.len, &cursor->key_format));
	AE_ERR(__ae_config_getones(session, metaconf, "value_format", &cval));
	AE_ERR(
	    __ae_strndup(session, cval.str, cval.len, &cursor->value_format));

	AE_ERR(__ae_cursor_init(cursor, uri, owner, cfg, cursorp));

	/* Data-source cursors may have a custom collator. */
	AE_ERR(
	    __ae_config_getones(session, metaconf, "app_metadata", &metadata));
	AE_ERR(__ae_config_gets_none(session, cfg, "collator", &cval));
	if (cval.len != 0)
		AE_ERR(__ae_collator_config(session, uri, &cval, &metadata,
		    &data_source->collator, &data_source->collator_owned));

	AE_ERR(dsrc->open_cursor(dsrc,
	    &session->iface, uri, (AE_CONFIG_ARG *)cfg, &data_source->source));
	source = data_source->source;
	source->session = (AE_SESSION *)session;
	memset(&source->q, 0, sizeof(source->q));
	source->recno = AE_RECNO_OOB;
	memset(source->raw_recno_buf, 0, sizeof(source->raw_recno_buf));
	memset(&source->key, 0, sizeof(source->key));
	memset(&source->value, 0, sizeof(source->value));
	source->saved_err = 0;
	source->flags = 0;

	if (0) {
err:		if (F_ISSET(cursor, AE_CURSTD_OPEN))
			AE_TRET(cursor->close(cursor));
		else
			__ae_free(session, data_source);
		*cursorp = NULL;
	}

	__ae_free(session, metaconf);
	return (ret);
}
