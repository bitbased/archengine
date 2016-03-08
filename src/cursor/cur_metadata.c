/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * Custom NEED macros for metadata cursors - that copy the values into the
 * backing metadata table cursor.
 */
#define	AE_MD_CURSOR_NEEDKEY(cursor) do {				\
	AE_CURSOR_NEEDKEY(cursor);					\
	AE_ERR(__ae_buf_set(session,					\
	    &((AE_CURSOR_METADATA *)(cursor))->file_cursor->key,	\
	    cursor->key.data, cursor->key.size));			\
	F_SET(((AE_CURSOR_METADATA *)(cursor))->file_cursor,		\
	    AE_CURSTD_KEY_EXT);						\
} while (0)

#define	AE_MD_CURSOR_NEEDVALUE(cursor) do {				\
	AE_CURSOR_NEEDVALUE(cursor);					\
	AE_ERR(__ae_buf_set(session,					\
	    &((AE_CURSOR_METADATA *)(cursor))->file_cursor->value,	\
	    cursor->value.data, cursor->value.size));			\
	F_SET(((AE_CURSOR_METADATA *)(cursor))->file_cursor,		\
	    AE_CURSTD_VALUE_EXT);					\
} while (0)

/*
 * __curmetadata_setkv --
 *	Copy key/value into the public cursor, stripping internal metadata for
 *	"create-only" cursors.
 */
static int
__curmetadata_setkv(AE_CURSOR_METADATA *mdc, AE_CURSOR *fc)
{
	AE_CURSOR *c;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;
	char *value;

	c = &mdc->iface;
	session = (AE_SESSION_IMPL *)c->session;

	c->key.data = fc->key.data;
	c->key.size = fc->key.size;
	if (F_ISSET(mdc, AE_MDC_CREATEONLY)) {
		AE_RET(__ae_schema_create_strip(
		    session, fc->value.data, NULL, &value));
		ret = __ae_buf_set(
		    session, &c->value, value, strlen(value) + 1);
		__ae_free(session, value);
		AE_RET(ret);
	} else {
		c->value.data = fc->value.data;
		c->value.size = fc->value.size;
	}

	F_SET(c, AE_CURSTD_KEY_EXT | AE_CURSTD_VALUE_EXT);
	F_CLR(mdc, AE_MDC_ONMETADATA);
	F_SET(mdc, AE_MDC_POSITIONED);

	return (0);
}

/*
 * Check if a key matches the metadata.  The public value is "metadata:",
 * but also check for the internal version of the URI.
 */
#define	AE_KEY_IS_METADATA(key)						\
	(AE_STRING_MATCH(AE_METADATA_URI, (key)->data, (key)->size - 1) ||\
	 AE_STRING_MATCH(AE_METAFILE_URI, (key)->data, (key)->size - 1))

/*
 * __curmetadata_metadata_search --
 *	Retrieve the metadata for the metadata table
 */
static int
__curmetadata_metadata_search(AE_SESSION_IMPL *session, AE_CURSOR *cursor)
{
	AE_CURSOR_METADATA *mdc;
	AE_DECL_RET;
	char *value, *stripped;

	mdc = (AE_CURSOR_METADATA *)cursor;

	/* The metadata search interface allocates a new string in value. */
	AE_RET(__ae_metadata_search(session, AE_METAFILE_URI, &value));

	if (F_ISSET(mdc, AE_MDC_CREATEONLY)) {
		ret = __ae_schema_create_strip(
		    session, value, NULL, &stripped);
		__ae_free(session, value);
		AE_RET(ret);
		value = stripped;
	}

	ret = __ae_buf_setstr(session, &cursor->value, value);
	__ae_free(session, value);
	AE_RET(ret);

	AE_RET(__ae_buf_setstr(session, &cursor->key, AE_METADATA_URI));

	F_SET(mdc, AE_MDC_ONMETADATA | AE_MDC_POSITIONED);
	F_SET(cursor, AE_CURSTD_KEY_EXT | AE_CURSTD_VALUE_EXT);
	return (0);
}

/*
 * __curmetadata_compare --
 *	AE_CURSOR->compare method for the metadata cursor type.
 */
static int
__curmetadata_compare(AE_CURSOR *a, AE_CURSOR *b, int *cmpp)
{
	AE_CURSOR *a_file_cursor, *b_file_cursor;
	AE_CURSOR_METADATA *a_mdc, *b_mdc;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	a_mdc = ((AE_CURSOR_METADATA *)a);
	b_mdc = ((AE_CURSOR_METADATA *)b);
	a_file_cursor = a_mdc->file_cursor;
	b_file_cursor = b_mdc->file_cursor;

	CURSOR_API_CALL(a, session,
	    compare, ((AE_CURSOR_BTREE *)a_file_cursor)->btree);

	if (b->compare != __curmetadata_compare)
		AE_ERR_MSG(session, EINVAL,
		    "Can only compare cursors of the same type");

	AE_MD_CURSOR_NEEDKEY(a);
	AE_MD_CURSOR_NEEDKEY(b);

	if (F_ISSET(a_mdc, AE_MDC_ONMETADATA)) {
		if (F_ISSET(b_mdc, AE_MDC_ONMETADATA))
			*cmpp = 0;
		else
			*cmpp = 1;
	} else if (F_ISSET(b_mdc, AE_MDC_ONMETADATA))
		*cmpp = -1;
	else
		ret = a_file_cursor->compare(
		    a_file_cursor, b_file_cursor, cmpp);

err:	API_END_RET(session, ret);
}

/*
 * __curmetadata_next --
 *	AE_CURSOR->next method for the metadata cursor type.
 */
static int
__curmetadata_next(AE_CURSOR *cursor)
{
	AE_CURSOR *file_cursor;
	AE_CURSOR_METADATA *mdc;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	mdc = (AE_CURSOR_METADATA *)cursor;
	file_cursor = mdc->file_cursor;
	CURSOR_API_CALL(cursor, session,
	    next, ((AE_CURSOR_BTREE *)file_cursor)->btree);

	if (!F_ISSET(mdc, AE_MDC_POSITIONED))
		AE_ERR(__curmetadata_metadata_search(session, cursor));
	else {
		/*
		 * When applications open metadata cursors, they expect to see
		 * all schema-level operations reflected in the results.  Query
		 * at read-uncommitted to avoid confusion caused by the current
		 * transaction state.
		 */
		AE_WITH_TXN_ISOLATION(session, AE_ISO_READ_UNCOMMITTED,
		    ret = file_cursor->next(mdc->file_cursor));
		AE_ERR(ret);
		AE_ERR(__curmetadata_setkv(mdc, file_cursor));
	}

err:	if (ret != 0) {
		F_CLR(mdc, AE_MDC_POSITIONED | AE_MDC_ONMETADATA);
		F_CLR(cursor, AE_CURSTD_KEY_EXT | AE_CURSTD_VALUE_EXT);
	}
	API_END_RET(session, ret);
}

/*
 * __curmetadata_prev --
 *	AE_CURSOR->prev method for the metadata cursor type.
 */
static int
__curmetadata_prev(AE_CURSOR *cursor)
{
	AE_CURSOR *file_cursor;
	AE_CURSOR_METADATA *mdc;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	mdc = (AE_CURSOR_METADATA *)cursor;
	file_cursor = mdc->file_cursor;
	CURSOR_API_CALL(cursor, session,
	    prev, ((AE_CURSOR_BTREE *)file_cursor)->btree);

	if (F_ISSET(mdc, AE_MDC_ONMETADATA)) {
		ret = AE_NOTFOUND;
		goto err;
	}

	AE_WITH_TXN_ISOLATION(session, AE_ISO_READ_UNCOMMITTED,
	    ret = file_cursor->prev(file_cursor));
	if (ret == 0)
		AE_ERR(__curmetadata_setkv(mdc, file_cursor));
	else if (ret == AE_NOTFOUND)
		AE_ERR(__curmetadata_metadata_search(session, cursor));

err:	if (ret != 0) {
		F_CLR(mdc, AE_MDC_POSITIONED | AE_MDC_ONMETADATA);
		F_CLR(cursor, AE_CURSTD_KEY_EXT | AE_CURSTD_VALUE_EXT);
	}
	API_END_RET(session, ret);
}

/*
 * __curmetadata_reset --
 *	AE_CURSOR->reset method for the metadata cursor type.
 */
static int
__curmetadata_reset(AE_CURSOR *cursor)
{
	AE_CURSOR *file_cursor;
	AE_CURSOR_METADATA *mdc;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	mdc = (AE_CURSOR_METADATA *)cursor;
	file_cursor = mdc->file_cursor;
	CURSOR_API_CALL(cursor, session,
	    reset, ((AE_CURSOR_BTREE *)file_cursor)->btree);

	if (F_ISSET(mdc, AE_MDC_POSITIONED) && !F_ISSET(mdc, AE_MDC_ONMETADATA))
		ret = file_cursor->reset(file_cursor);
	F_CLR(mdc, AE_MDC_POSITIONED | AE_MDC_ONMETADATA);
	F_CLR(cursor, AE_CURSTD_KEY_SET | AE_CURSTD_VALUE_SET);

err:	API_END_RET(session, ret);
}

/*
 * __curmetadata_search --
 *	AE_CURSOR->search method for the metadata cursor type.
 */
static int
__curmetadata_search(AE_CURSOR *cursor)
{
	AE_CURSOR *file_cursor;
	AE_CURSOR_METADATA *mdc;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	mdc = (AE_CURSOR_METADATA *)cursor;
	file_cursor = mdc->file_cursor;
	CURSOR_API_CALL(cursor, session,
	    search, ((AE_CURSOR_BTREE *)file_cursor)->btree);

	AE_MD_CURSOR_NEEDKEY(cursor);

	if (AE_KEY_IS_METADATA(&cursor->key))
		AE_ERR(__curmetadata_metadata_search(session, cursor));
	else {
		AE_WITH_TXN_ISOLATION(session, AE_ISO_READ_UNCOMMITTED,
		    ret = file_cursor->search(file_cursor));
		AE_ERR(ret);
		AE_ERR(__curmetadata_setkv(mdc, file_cursor));
	}

err:	if (ret != 0) {
		F_CLR(mdc, AE_MDC_POSITIONED | AE_MDC_ONMETADATA);
		F_CLR(cursor, AE_CURSTD_KEY_EXT | AE_CURSTD_VALUE_EXT);
	}
	API_END_RET(session, ret);
}

/*
 * __curmetadata_search_near --
 *	AE_CURSOR->search_near method for the metadata cursor type.
 */
static int
__curmetadata_search_near(AE_CURSOR *cursor, int *exact)
{
	AE_CURSOR *file_cursor;
	AE_CURSOR_METADATA *mdc;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	mdc = (AE_CURSOR_METADATA *)cursor;
	file_cursor = mdc->file_cursor;
	CURSOR_API_CALL(cursor, session,
	    search_near, ((AE_CURSOR_BTREE *)file_cursor)->btree);

	AE_MD_CURSOR_NEEDKEY(cursor);

	if (AE_KEY_IS_METADATA(&cursor->key)) {
		AE_ERR(__curmetadata_metadata_search(session, cursor));
		*exact = 1;
	} else {
		AE_WITH_TXN_ISOLATION(session, AE_ISO_READ_UNCOMMITTED,
		    ret = file_cursor->search_near(file_cursor, exact));
		AE_ERR(ret);
		AE_ERR(__curmetadata_setkv(mdc, file_cursor));
	}

err:	if (ret != 0) {
		F_CLR(mdc, AE_MDC_POSITIONED | AE_MDC_ONMETADATA);
		F_CLR(cursor, AE_CURSTD_KEY_EXT | AE_CURSTD_VALUE_EXT);
	}
	API_END_RET(session, ret);
}

/*
 * __curmetadata_insert --
 *	AE_CURSOR->insert method for the metadata cursor type.
 */
static int
__curmetadata_insert(AE_CURSOR *cursor)
{
	AE_CURSOR *file_cursor;
	AE_CURSOR_METADATA *mdc;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	mdc = (AE_CURSOR_METADATA *)cursor;
	file_cursor = mdc->file_cursor;
	CURSOR_API_CALL(cursor, session,
	    insert, ((AE_CURSOR_BTREE *)file_cursor)->btree);

	AE_MD_CURSOR_NEEDKEY(cursor);
	AE_MD_CURSOR_NEEDVALUE(cursor);

	/*
	 * Since the key/value formats are 's' the AE_ITEMs must contain a
	 * NULL terminated string.
	 */
	ret =
	    __ae_metadata_insert(session, cursor->key.data, cursor->value.data);

err:	API_END_RET(session, ret);
}

/*
 * __curmetadata_update --
 *	AE_CURSOR->update method for the metadata cursor type.
 */
static int
__curmetadata_update(AE_CURSOR *cursor)
{
	AE_CURSOR *file_cursor;
	AE_CURSOR_METADATA *mdc;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	mdc = (AE_CURSOR_METADATA *)cursor;
	file_cursor = mdc->file_cursor;
	CURSOR_API_CALL(cursor, session,
	    update, ((AE_CURSOR_BTREE *)file_cursor)->btree);

	AE_MD_CURSOR_NEEDKEY(cursor);
	AE_MD_CURSOR_NEEDVALUE(cursor);

	/*
	 * Since the key/value formats are 's' the AE_ITEMs must contain a
	 * NULL terminated string.
	 */
	ret =
	    __ae_metadata_update(session, cursor->key.data, cursor->value.data);

err:	API_END_RET(session, ret);
}

/*
 * __curmetadata_remove --
 *	AE_CURSOR->remove method for the metadata cursor type.
 */
static int
__curmetadata_remove(AE_CURSOR *cursor)
{
	AE_CURSOR *file_cursor;
	AE_CURSOR_METADATA *mdc;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	mdc = (AE_CURSOR_METADATA *)cursor;
	file_cursor = mdc->file_cursor;
	CURSOR_API_CALL(cursor, session,
	    remove, ((AE_CURSOR_BTREE *)file_cursor)->btree);

	AE_MD_CURSOR_NEEDKEY(cursor);

	/*
	 * Since the key format is 's' the AE_ITEM must contain a NULL
	 * terminated string.
	 */
	ret = __ae_metadata_remove(session, cursor->key.data);

err:	API_END_RET(session, ret);
}

/*
 * __curmetadata_close --
 *	AE_CURSOR->close method for the metadata cursor type.
 */
static int
__curmetadata_close(AE_CURSOR *cursor)
{
	AE_CURSOR *file_cursor;
	AE_CURSOR_METADATA *mdc;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	mdc = (AE_CURSOR_METADATA *)cursor;
	file_cursor = mdc->file_cursor;
	CURSOR_API_CALL(cursor, session,
	    close, ((AE_CURSOR_BTREE *)file_cursor)->btree);

	ret = file_cursor->close(file_cursor);
	AE_TRET(__ae_cursor_close(cursor));

err:	API_END_RET(session, ret);
}

/*
 * __ae_curmetadata_open --
 *	AE_SESSION->open_cursor method for metadata cursors.
 *
 * Metadata cursors are a similar to a file cursor on the special metadata
 * table, except that the metadata for the metadata table (which is stored
 * in the turtle file) can also be queried.
 *
 * Metadata cursors are read-only by default.
 */
int
__ae_curmetadata_open(AE_SESSION_IMPL *session,
    const char *uri, AE_CURSOR *owner, const char *cfg[], AE_CURSOR **cursorp)
{
	AE_CURSOR_STATIC_INIT(iface,
	    __ae_cursor_get_key,	/* get-key */
	    __ae_cursor_get_value,	/* get-value */
	    __ae_cursor_set_key,	/* set-key */
	    __ae_cursor_set_value,	/* set-value */
	    __curmetadata_compare,	/* compare */
	    __ae_cursor_equals,		/* equals */
	    __curmetadata_next,		/* next */
	    __curmetadata_prev,		/* prev */
	    __curmetadata_reset,	/* reset */
	    __curmetadata_search,	/* search */
	    __curmetadata_search_near,	/* search-near */
	    __curmetadata_insert,	/* insert */
	    __curmetadata_update,	/* update */
	    __curmetadata_remove,	/* remove */
	    __ae_cursor_notsup,		/* reconfigure */
	    __curmetadata_close);	/* close */
	AE_CURSOR *cursor;
	AE_CURSOR_METADATA *mdc;
	AE_DECL_RET;
	AE_CONFIG_ITEM cval;

	AE_RET(__ae_calloc_one(session, &mdc));

	cursor = &mdc->iface;
	*cursor = iface;
	cursor->session = &session->iface;
	cursor->key_format = "S";
	cursor->value_format = "S";

	/* Open the file cursor for operations on the regular metadata */
	AE_ERR(__ae_metadata_cursor(session, cfg[1], &mdc->file_cursor));

	AE_ERR(__ae_cursor_init(cursor, uri, owner, cfg, cursorp));

	/* If we are only returning create config, strip internal metadata. */
	if (AE_STREQ(uri, "metadata:create"))
		F_SET(mdc, AE_MDC_CREATEONLY);

	/*
	 * Metadata cursors default to readonly; if not set to not-readonly,
	 * they are permanently readonly and cannot be reconfigured.
	 */
	AE_ERR(__ae_config_gets_def(session, cfg, "readonly", 1, &cval));
	if (cval.val != 0) {
		cursor->insert = __ae_cursor_notsup;
		cursor->update = __ae_cursor_notsup;
		cursor->remove = __ae_cursor_notsup;
	}

	if (0) {
err:		if (mdc->file_cursor != NULL)
			AE_TRET(mdc->file_cursor->close(mdc->file_cursor));
		__ae_free(session, mdc);
	}
	return (ret);
}
