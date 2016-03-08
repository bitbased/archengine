/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __curbulk_insert_fix --
 *	Fixed-length column-store bulk cursor insert.
 */
static int
__curbulk_insert_fix(AE_CURSOR *cursor)
{
	AE_BTREE *btree;
	AE_CURSOR_BULK *cbulk;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	cbulk = (AE_CURSOR_BULK *)cursor;
	btree = cbulk->cbt.btree;

	/*
	 * Bulk cursor inserts are updates, but don't need auto-commit
	 * transactions because they are single-threaded and not visible
	 * until the bulk cursor is closed.
	 */
	CURSOR_API_CALL(cursor, session, insert, btree);

	AE_CURSOR_NEEDVALUE(cursor);

	AE_ERR(__ae_bulk_insert_fix(session, cbulk));

	AE_STAT_FAST_DATA_INCR(session, cursor_insert_bulk);

err:	API_END_RET(session, ret);
}

/*
 * __curbulk_insert_var --
 *	Variable-length column-store bulk cursor insert.
 */
static int
__curbulk_insert_var(AE_CURSOR *cursor)
{
	AE_BTREE *btree;
	AE_CURSOR_BULK *cbulk;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;
	bool duplicate;

	cbulk = (AE_CURSOR_BULK *)cursor;
	btree = cbulk->cbt.btree;

	/*
	 * Bulk cursor inserts are updates, but don't need auto-commit
	 * transactions because they are single-threaded and not visible
	 * until the bulk cursor is closed.
	 */
	CURSOR_API_CALL(cursor, session, insert, btree);

	AE_CURSOR_NEEDVALUE(cursor);

	/*
	 * If this isn't the first value inserted, compare it against the last
	 * value and increment the RLE count.
	 *
	 * Instead of a "first time" variable, I'm using the RLE count, because
	 * it is only zero before the first row is inserted.
	 */
	duplicate = false;
	if (cbulk->rle != 0) {
		if (cbulk->last.size == cursor->value.size &&
		    memcmp(cbulk->last.data, cursor->value.data,
		    cursor->value.size) == 0) {
			++cbulk->rle;
			duplicate = true;
		} else
			AE_ERR(__ae_bulk_insert_var(session, cbulk));
	}

	/*
	 * Save a copy of the value for the next comparison and reset the RLE
	 * counter.
	 */
	if (!duplicate) {
		AE_ERR(__ae_buf_set(session,
		    &cbulk->last, cursor->value.data, cursor->value.size));
		cbulk->rle = 1;
	}

	AE_STAT_FAST_DATA_INCR(session, cursor_insert_bulk);

err:	API_END_RET(session, ret);
}

/*
 * __bulk_row_keycmp_err --
 *	Error routine when keys inserted out-of-order.
 */
static int
__bulk_row_keycmp_err(AE_CURSOR_BULK *cbulk)
{
	AE_CURSOR *cursor;
	AE_DECL_ITEM(a);
	AE_DECL_ITEM(b);
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	session = (AE_SESSION_IMPL *)cbulk->cbt.iface.session;
	cursor = &cbulk->cbt.iface;

	AE_ERR(__ae_scr_alloc(session, 512, &a));
	AE_ERR(__ae_scr_alloc(session, 512, &b));

	AE_ERR(__ae_buf_set_printable(
	    session, a, cursor->key.data, cursor->key.size));
	AE_ERR(__ae_buf_set_printable(
	    session, b, cbulk->last.data, cbulk->last.size));

	AE_ERR_MSG(session, EINVAL,
	    "bulk-load presented with out-of-order keys: %.*s compares smaller "
	    "than previously inserted key %.*s",
	    (int)a->size, (const char *)a->data,
	    (int)b->size, (const char *)b->data);

err:	__ae_scr_free(session, &a);
	__ae_scr_free(session, &b);
	return (ret);
}

/*
 * __curbulk_insert_row --
 *	Row-store bulk cursor insert, with key-sort checks.
 */
static int
__curbulk_insert_row(AE_CURSOR *cursor)
{
	AE_BTREE *btree;
	AE_CURSOR_BULK *cbulk;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;
	int cmp;

	cbulk = (AE_CURSOR_BULK *)cursor;
	btree = cbulk->cbt.btree;

	/*
	 * Bulk cursor inserts are updates, but don't need auto-commit
	 * transactions because they are single-threaded and not visible
	 * until the bulk cursor is closed.
	 */
	CURSOR_API_CALL(cursor, session, insert, btree);

	AE_CURSOR_CHECKKEY(cursor);
	AE_CURSOR_CHECKVALUE(cursor);

	/*
	 * If this isn't the first key inserted, compare it against the last key
	 * to ensure the application doesn't accidentally corrupt the table.
	 *
	 * Instead of a "first time" variable, I'm using the RLE count, because
	 * it is only zero before the first row is inserted.
	 */
	if (cbulk->rle != 0) {
		AE_ERR(__ae_compare(session,
		    btree->collator, &cursor->key, &cbulk->last, &cmp));
		if (cmp <= 0)
			AE_ERR(__bulk_row_keycmp_err(cbulk));
	}

	/*
	 * Save a copy of the key for the next comparison and set the RLE
	 * counter.
	 */
	AE_ERR(__ae_buf_set(session,
	    &cbulk->last, cursor->key.data, cursor->key.size));
	cbulk->rle = 1;

	AE_ERR(__ae_bulk_insert_row(session, cbulk));

	AE_STAT_FAST_DATA_INCR(session, cursor_insert_bulk);

err:	API_END_RET(session, ret);
}

/*
 * __curbulk_insert_row_skip_check --
 *	Row-store bulk cursor insert, without key-sort checks.
 */
static int
__curbulk_insert_row_skip_check(AE_CURSOR *cursor)
{
	AE_BTREE *btree;
	AE_CURSOR_BULK *cbulk;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	cbulk = (AE_CURSOR_BULK *)cursor;
	btree = cbulk->cbt.btree;

	/*
	 * Bulk cursor inserts are updates, but don't need auto-commit
	 * transactions because they are single-threaded and not visible
	 * until the bulk cursor is closed.
	 */
	CURSOR_API_CALL(cursor, session, insert, btree);

	AE_CURSOR_NEEDKEY(cursor);
	AE_CURSOR_NEEDVALUE(cursor);

	AE_ERR(__ae_bulk_insert_row(session, cbulk));

	AE_STAT_FAST_DATA_INCR(session, cursor_insert_bulk);

err:	API_END_RET(session, ret);
}

/*
 * __ae_curbulk_init --
 *	Initialize a bulk cursor.
 */
int
__ae_curbulk_init(AE_SESSION_IMPL *session,
    AE_CURSOR_BULK *cbulk, bool bitmap, bool skip_sort_check)
{
	AE_CURSOR *c;
	AE_CURSOR_BTREE *cbt;

	c = &cbulk->cbt.iface;
	cbt = &cbulk->cbt;

	/* Bulk cursors only support insert and close (reset is a no-op). */
	__ae_cursor_set_notsup(c);
	switch (cbt->btree->type) {
	case BTREE_COL_FIX:
		c->insert = __curbulk_insert_fix;
		break;
	case BTREE_COL_VAR:
		c->insert = __curbulk_insert_var;
		break;
	case BTREE_ROW:
		c->insert = skip_sort_check ?
		    __curbulk_insert_row_skip_check : __curbulk_insert_row;
		break;
	AE_ILLEGAL_VALUE(session);
	}

	cbulk->bitmap = bitmap;
	if (bitmap)
		F_SET(c, AE_CURSTD_RAW);

	return (__ae_bulk_init(session, cbulk));
}
