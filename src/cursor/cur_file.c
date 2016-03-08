/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * AE_BTREE_CURSOR_SAVE_AND_RESTORE
 *	Save the cursor's key/value data/size fields, call an underlying btree
 *	function, and then consistently handle failure and success.
 */
#define	AE_BTREE_CURSOR_SAVE_AND_RESTORE(cursor, f, ret) do {		\
	AE_ITEM __key_copy = (cursor)->key;				\
	uint64_t __recno = (cursor)->recno;				\
	AE_ITEM __value_copy = (cursor)->value;				\
	if (((ret) = (f)) == 0) {					\
		F_CLR(cursor, AE_CURSTD_KEY_EXT | AE_CURSTD_VALUE_EXT);	\
		F_SET(cursor, AE_CURSTD_KEY_INT | AE_CURSTD_VALUE_INT);	\
	} else {							\
		if (F_ISSET(cursor, AE_CURSTD_KEY_EXT)) {		\
			(cursor)->recno = __recno;			\
			AE_ITEM_SET((cursor)->key, __key_copy);		\
		}							\
		if (F_ISSET(cursor, AE_CURSTD_VALUE_EXT))		\
			AE_ITEM_SET((cursor)->value, __value_copy);	\
		F_CLR(cursor, AE_CURSTD_KEY_INT | AE_CURSTD_VALUE_INT);	\
	}								\
} while (0)

/*
 * __curfile_compare --
 *	AE_CURSOR->compare method for the btree cursor type.
 */
static int
__curfile_compare(AE_CURSOR *a, AE_CURSOR *b, int *cmpp)
{
	AE_CURSOR_BTREE *cbt;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	cbt = (AE_CURSOR_BTREE *)a;
	CURSOR_API_CALL(a, session, compare, cbt->btree);

	/*
	 * Check both cursors are a "file:" type then call the underlying
	 * function, it can handle cursors pointing to different objects.
	 */
	if (!AE_PREFIX_MATCH(a->internal_uri, "file:") ||
	    !AE_PREFIX_MATCH(b->internal_uri, "file:"))
		AE_ERR_MSG(session, EINVAL,
		    "Cursors must reference the same object");

	AE_CURSOR_CHECKKEY(a);
	AE_CURSOR_CHECKKEY(b);

	ret = __ae_btcur_compare(
	    (AE_CURSOR_BTREE *)a, (AE_CURSOR_BTREE *)b, cmpp);

err:	API_END_RET(session, ret);
}

/*
 * __curfile_equals --
 *	AE_CURSOR->equals method for the btree cursor type.
 */
static int
__curfile_equals(AE_CURSOR *a, AE_CURSOR *b, int *equalp)
{
	AE_CURSOR_BTREE *cbt;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	cbt = (AE_CURSOR_BTREE *)a;
	CURSOR_API_CALL(a, session, equals, cbt->btree);

	/*
	 * Check both cursors are a "file:" type then call the underlying
	 * function, it can handle cursors pointing to different objects.
	 */
	if (!AE_PREFIX_MATCH(a->internal_uri, "file:") ||
	    !AE_PREFIX_MATCH(b->internal_uri, "file:"))
		AE_ERR_MSG(session, EINVAL,
		    "Cursors must reference the same object");

	AE_CURSOR_CHECKKEY(a);
	AE_CURSOR_CHECKKEY(b);

	ret = __ae_btcur_equals(
	    (AE_CURSOR_BTREE *)a, (AE_CURSOR_BTREE *)b, equalp);

err:	API_END_RET(session, ret);
}

/*
 * __curfile_next --
 *	AE_CURSOR->next method for the btree cursor type.
 */
static int
__curfile_next(AE_CURSOR *cursor)
{
	AE_CURSOR_BTREE *cbt;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	cbt = (AE_CURSOR_BTREE *)cursor;
	CURSOR_API_CALL(cursor, session, next, cbt->btree);

	F_CLR(cursor, AE_CURSTD_KEY_SET | AE_CURSTD_VALUE_SET);
	if ((ret = __ae_btcur_next(cbt, false)) == 0)
		F_SET(cursor, AE_CURSTD_KEY_INT | AE_CURSTD_VALUE_INT);

err:	API_END_RET(session, ret);
}

/*
 * __curfile_next_random --
 *	AE_CURSOR->next method for the btree cursor type when configured with
 * next_random.
 */
static int
__curfile_next_random(AE_CURSOR *cursor)
{
	AE_CURSOR_BTREE *cbt;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	cbt = (AE_CURSOR_BTREE *)cursor;
	CURSOR_API_CALL(cursor, session, next, cbt->btree);

	F_CLR(cursor, AE_CURSTD_KEY_SET | AE_CURSTD_VALUE_SET);
	if ((ret = __ae_btcur_next_random(cbt)) == 0)
		F_SET(cursor, AE_CURSTD_KEY_INT | AE_CURSTD_VALUE_INT);

err:	API_END_RET(session, ret);
}

/*
 * __curfile_prev --
 *	AE_CURSOR->prev method for the btree cursor type.
 */
static int
__curfile_prev(AE_CURSOR *cursor)
{
	AE_CURSOR_BTREE *cbt;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	cbt = (AE_CURSOR_BTREE *)cursor;
	CURSOR_API_CALL(cursor, session, prev, cbt->btree);

	F_CLR(cursor, AE_CURSTD_KEY_SET | AE_CURSTD_VALUE_SET);
	if ((ret = __ae_btcur_prev(cbt, false)) == 0)
		F_SET(cursor, AE_CURSTD_KEY_INT | AE_CURSTD_VALUE_INT);

err:	API_END_RET(session, ret);
}

/*
 * __curfile_reset --
 *	AE_CURSOR->reset method for the btree cursor type.
 */
static int
__curfile_reset(AE_CURSOR *cursor)
{
	AE_CURSOR_BTREE *cbt;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	cbt = (AE_CURSOR_BTREE *)cursor;
	CURSOR_API_CALL(cursor, session, reset, cbt->btree);

	ret = __ae_btcur_reset(cbt);

	F_CLR(cursor, AE_CURSTD_KEY_SET | AE_CURSTD_VALUE_SET);

err:	API_END_RET(session, ret);
}

/*
 * __curfile_search --
 *	AE_CURSOR->search method for the btree cursor type.
 */
static int
__curfile_search(AE_CURSOR *cursor)
{
	AE_CURSOR_BTREE *cbt;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	cbt = (AE_CURSOR_BTREE *)cursor;
	CURSOR_API_CALL(cursor, session, search, cbt->btree);

	AE_CURSOR_NEEDKEY(cursor);
	AE_CURSOR_NOVALUE(cursor);

	AE_BTREE_CURSOR_SAVE_AND_RESTORE(cursor, __ae_btcur_search(cbt), ret);

err:	API_END_RET(session, ret);
}

/*
 * __curfile_search_near --
 *	AE_CURSOR->search_near method for the btree cursor type.
 */
static int
__curfile_search_near(AE_CURSOR *cursor, int *exact)
{
	AE_CURSOR_BTREE *cbt;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	cbt = (AE_CURSOR_BTREE *)cursor;
	CURSOR_API_CALL(cursor, session, search_near, cbt->btree);

	AE_CURSOR_NEEDKEY(cursor);
	AE_CURSOR_NOVALUE(cursor);

	AE_BTREE_CURSOR_SAVE_AND_RESTORE(
	    cursor, __ae_btcur_search_near(cbt, exact), ret);

err:	API_END_RET(session, ret);
}

/*
 * __curfile_insert --
 *	AE_CURSOR->insert method for the btree cursor type.
 */
static int
__curfile_insert(AE_CURSOR *cursor)
{
	AE_CURSOR_BTREE *cbt;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	cbt = (AE_CURSOR_BTREE *)cursor;
	CURSOR_UPDATE_API_CALL(cursor, session, insert, cbt->btree);
	if (!F_ISSET(cursor, AE_CURSTD_APPEND))
		AE_CURSOR_NEEDKEY(cursor);
	AE_CURSOR_NEEDVALUE(cursor);

	AE_BTREE_CURSOR_SAVE_AND_RESTORE(cursor, __ae_btcur_insert(cbt), ret);

	/*
	 * Insert is the one cursor operation that doesn't end with the cursor
	 * pointing to an on-page item (except for column-store appends, where
	 * we are returning a key). That is, the application's cursor continues
	 * to reference the application's memory after a successful cursor call,
	 * which isn't true anywhere else. We don't want to have to explain that
	 * scoping corner case, so we reset the application's cursor so it can
	 * free the referenced memory and continue on without risking subsequent
	 * core dumps.
	 */
	if (ret == 0) {
		if (!F_ISSET(cursor, AE_CURSTD_APPEND))
			F_CLR(cursor, AE_CURSTD_KEY_INT);
		F_CLR(cursor, AE_CURSTD_VALUE_INT);
	}

err:	CURSOR_UPDATE_API_END(session, ret);
	return (ret);
}

/*
 * __curfile_update --
 *	AE_CURSOR->update method for the btree cursor type.
 */
static int
__curfile_update(AE_CURSOR *cursor)
{
	AE_CURSOR_BTREE *cbt;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	cbt = (AE_CURSOR_BTREE *)cursor;
	CURSOR_UPDATE_API_CALL(cursor, session, update, cbt->btree);

	AE_CURSOR_NEEDKEY(cursor);
	AE_CURSOR_NEEDVALUE(cursor);

	AE_BTREE_CURSOR_SAVE_AND_RESTORE(cursor, __ae_btcur_update(cbt), ret);

err:	CURSOR_UPDATE_API_END(session, ret);
	return (ret);
}

/*
 * __ae_curfile_update_check --
 *	AE_CURSOR->update_check method for the btree cursor type.
 */
int
__ae_curfile_update_check(AE_CURSOR *cursor)
{
	AE_CURSOR_BTREE *cbt;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	cbt = (AE_CURSOR_BTREE *)cursor;
	CURSOR_UPDATE_API_CALL(cursor, session, update, cbt->btree);

	AE_CURSOR_NEEDKEY(cursor);
	AE_CURSOR_NOVALUE(cursor);

	AE_BTREE_CURSOR_SAVE_AND_RESTORE(
	    cursor, __ae_btcur_update_check(cbt), ret);

err:	CURSOR_UPDATE_API_END(session, ret);
	return (ret);
}

/*
 * __curfile_remove --
 *	AE_CURSOR->remove method for the btree cursor type.
 */
static int
__curfile_remove(AE_CURSOR *cursor)
{
	AE_CURSOR_BTREE *cbt;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	cbt = (AE_CURSOR_BTREE *)cursor;
	CURSOR_REMOVE_API_CALL(cursor, session, cbt->btree);

	AE_CURSOR_NEEDKEY(cursor);
	AE_CURSOR_NOVALUE(cursor);

	AE_BTREE_CURSOR_SAVE_AND_RESTORE(cursor, __ae_btcur_remove(cbt), ret);

	/*
	 * After a successful remove, copy the key: the value is not available.
	 */
	if (ret == 0) {
		if (F_ISSET(cursor, AE_CURSTD_KEY_INT) &&
		    !AE_DATA_IN_ITEM(&(cursor)->key)) {
			AE_ERR(__ae_buf_set(session, &cursor->key,
			    cursor->key.data, cursor->key.size));
			F_CLR(cursor, AE_CURSTD_KEY_INT);
			F_SET(cursor, AE_CURSTD_KEY_EXT);
		}
		F_CLR(cursor, AE_CURSTD_VALUE_SET);
	}

err:	CURSOR_UPDATE_API_END(session, ret);
	return (ret);
}

/*
 * __curfile_close --
 *	AE_CURSOR->close method for the btree cursor type.
 */
static int
__curfile_close(AE_CURSOR *cursor)
{
	AE_CURSOR_BTREE *cbt;
	AE_CURSOR_BULK *cbulk;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	cbt = (AE_CURSOR_BTREE *)cursor;
	CURSOR_API_CALL(cursor, session, close, cbt->btree);
	if (F_ISSET(cursor, AE_CURSTD_BULK)) {
		/* Free the bulk-specific resources. */
		cbulk = (AE_CURSOR_BULK *)cbt;
		AE_TRET(__ae_bulk_wrapup(session, cbulk));
		__ae_buf_free(session, &cbulk->last);
	}

	AE_TRET(__ae_btcur_close(cbt, false));
	/* The URI is owned by the btree handle. */
	cursor->internal_uri = NULL;
	AE_TRET(__ae_cursor_close(cursor));

	/*
	 * Note: release the data handle last so that cursor statistics are
	 * updated correctly.
	 */
	if (session->dhandle != NULL) {
		/* Decrement the data-source's in-use counter. */
		__ae_cursor_dhandle_decr_use(session);
		AE_TRET(__ae_session_release_btree(session));
	}

err:	API_END_RET(session, ret);
}

/*
 * __ae_curfile_create --
 *	Open a cursor for a given btree handle.
 */
int
__ae_curfile_create(AE_SESSION_IMPL *session,
    AE_CURSOR *owner, const char *cfg[], bool bulk, bool bitmap,
    AE_CURSOR **cursorp)
{
	AE_CURSOR_STATIC_INIT(iface,
	    __ae_cursor_get_key,	/* get-key */
	    __ae_cursor_get_value,	/* get-value */
	    __ae_cursor_set_key,	/* set-key */
	    __ae_cursor_set_value,	/* set-value */
	    __curfile_compare,		/* compare */
	    __curfile_equals,		/* equals */
	    __curfile_next,		/* next */
	    __curfile_prev,		/* prev */
	    __curfile_reset,		/* reset */
	    __curfile_search,		/* search */
	    __curfile_search_near,	/* search-near */
	    __curfile_insert,		/* insert */
	    __curfile_update,		/* update */
	    __curfile_remove,		/* remove */
	    __ae_cursor_reconfigure,	/* reconfigure */
	    __curfile_close);		/* close */
	AE_BTREE *btree;
	AE_CONFIG_ITEM cval;
	AE_CURSOR *cursor;
	AE_CURSOR_BTREE *cbt;
	AE_CURSOR_BULK *cbulk;
	AE_DECL_RET;
	size_t csize;

	AE_STATIC_ASSERT(offsetof(AE_CURSOR_BTREE, iface) == 0);

	cbt = NULL;

	btree = S2BT(session);
	AE_ASSERT(session, btree != NULL);

	csize = bulk ? sizeof(AE_CURSOR_BULK) : sizeof(AE_CURSOR_BTREE);
	AE_RET(__ae_calloc(session, 1, csize, &cbt));

	cursor = &cbt->iface;
	*cursor = iface;
	cursor->session = &session->iface;
	cursor->internal_uri = btree->dhandle->name;
	cursor->key_format = btree->key_format;
	cursor->value_format = btree->value_format;
	cbt->btree = btree;

	if (session->dhandle->checkpoint != NULL)
		F_SET(cbt, AE_CBT_NO_TXN);

	if (bulk) {
		F_SET(cursor, AE_CURSTD_BULK);

		cbulk = (AE_CURSOR_BULK *)cbt;

		/* Optionally skip the validation of each bulk-loaded key. */
		AE_ERR(__ae_config_gets_def(
		    session, cfg, "skip_sort_check", 0, &cval));
		AE_ERR(__ae_curbulk_init(
		    session, cbulk, bitmap, cval.val == 0 ? 0 : 1));
	}

	/*
	 * random_retrieval
	 * Random retrieval cursors only support next, reset and close.
	 */
	AE_ERR(__ae_config_gets_def(session, cfg, "next_random", 0, &cval));
	if (cval.val != 0) {
		__ae_cursor_set_notsup(cursor);
		cursor->next = __curfile_next_random;
		cursor->reset = __curfile_reset;
	}

	/* Underlying btree initialization. */
	__ae_btcur_open(cbt);

	/* __ae_cursor_init is last so we don't have to clean up on error. */
	AE_ERR(__ae_cursor_init(
	    cursor, cursor->internal_uri, owner, cfg, cursorp));

	AE_STAT_FAST_CONN_INCR(session, cursor_create);
	AE_STAT_FAST_DATA_INCR(session, cursor_create);

	if (0) {
err:		__ae_free(session, cbt);
	}

	return (ret);
}

/*
 * __ae_curfile_open --
 *	AE_SESSION->open_cursor method for the btree cursor type.
 */
int
__ae_curfile_open(AE_SESSION_IMPL *session, const char *uri,
    AE_CURSOR *owner, const char *cfg[], AE_CURSOR **cursorp)
{
	AE_CONFIG_ITEM cval;
	AE_DECL_RET;
	uint32_t flags;
	bool bitmap, bulk;

	bitmap = bulk = false;
	flags = 0;

	/*
	 * Decode the bulk configuration settings. In memory databases
	 * ignore bulk load.
	 */
	if (!F_ISSET(S2C(session), AE_CONN_IN_MEMORY)) {
		AE_RET(__ae_config_gets_def(session, cfg, "bulk", 0, &cval));
		if (cval.type == AE_CONFIG_ITEM_BOOL ||
		    (cval.type == AE_CONFIG_ITEM_NUM &&
		    (cval.val == 0 || cval.val == 1))) {
			bitmap = false;
			bulk = cval.val != 0;
		} else if (AE_STRING_MATCH("bitmap", cval.str, cval.len))
			bitmap = bulk = true;
			/*
			 * Unordered bulk insert is a special case used
			 * internally by index creation on existing tables. It
			 * doesn't enforce any special semantics at the file
			 * level. It primarily exists to avoid some locking
			 * problems between LSM and index creation.
			 */
		else if (!AE_STRING_MATCH("unordered", cval.str, cval.len))
			AE_RET_MSG(session, EINVAL,
			    "Value for 'bulk' must be a boolean or 'bitmap'");
	}

	/* Bulk handles require exclusive access. */
	if (bulk)
		LF_SET(AE_BTREE_BULK | AE_DHANDLE_EXCLUSIVE);

	/* Get the handle and lock it while the cursor is using it. */
	if (AE_PREFIX_MATCH(uri, "file:")) {
		/*
		 * If we are opening exclusive, get the handle while holding
		 * the checkpoint lock.  This prevents a bulk cursor open
		 * failing with EBUSY due to a database-wide checkpoint.
		 */
		if (LF_ISSET(AE_DHANDLE_EXCLUSIVE))
			AE_WITH_CHECKPOINT_LOCK(session, ret =
			    __ae_session_get_btree_ckpt(
			    session, uri, cfg, flags));
		else
			ret = __ae_session_get_btree_ckpt(
			    session, uri, cfg, flags);
		AE_RET(ret);
	} else
		AE_RET(__ae_bad_object_type(session, uri));

	AE_ERR(__ae_curfile_create(session, owner, cfg, bulk, bitmap, cursorp));

	/* Increment the data-source's in-use counter. */
	__ae_cursor_dhandle_incr_use(session);
	return (0);

err:	/* If the cursor could not be opened, release the handle. */
	AE_TRET(__ae_session_release_btree(session));
	return (ret);
}
