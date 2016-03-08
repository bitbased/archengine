/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_las_stats_update --
 *	Update the lookaside table statistics for return to the application.
 */
void
__ae_las_stats_update(AE_SESSION_IMPL *session)
{
	AE_CONNECTION_IMPL *conn;
	AE_CONNECTION_STATS **cstats;
	AE_DSRC_STATS **dstats;

	conn = S2C(session);

	/*
	 * Lookaside table statistics are copied from the underlying lookaside
	 * table data-source statistics. If there's no lookaside table, values
	 * remain 0.
	 */
	if (!F_ISSET(conn, AE_CONN_LAS_OPEN))
		return;

	/*
	 * We have a cursor, and we need the underlying data handle; we can get
	 * to it by way of the underlying btree handle, but it's a little ugly.
	 */
	cstats = conn->stats;
	dstats = ((AE_CURSOR_BTREE *)
	    conn->las_session->las_cursor)->btree->dhandle->stats;

	AE_STAT_SET(session, cstats,
	    cache_lookaside_insert, AE_STAT_READ(dstats, cursor_insert));
	AE_STAT_SET(session, cstats,
	    cache_lookaside_remove, AE_STAT_READ(dstats, cursor_remove));
}

/*
 * __ae_las_create --
 *	Initialize the database's lookaside store.
 */
int
__ae_las_create(AE_SESSION_IMPL *session)
{
	AE_CONNECTION_IMPL *conn;
	uint32_t session_flags;
	const char *drop_cfg[] = {
	    AE_CONFIG_BASE(session, AE_SESSION_drop), "force=true", NULL };

	conn = S2C(session);

	/*
	 * Done at startup: we cannot do it on demand because we require the
	 * schema lock to create and drop the table, and it may not always be
	 * available.
	 *
	 * Discard any previous incarnation of the table.
	 */
	AE_RET(__ae_session_drop(session, AE_LAS_URI, drop_cfg));

	/* Re-create the table. */
	AE_RET(__ae_session_create(session, AE_LAS_URI, AE_LAS_FORMAT));

	/*
	 * Open a shared internal session used to access the lookaside table.
	 * This session should never be tapped for eviction.
	 */
	session_flags = AE_SESSION_LOOKASIDE_CURSOR | AE_SESSION_NO_EVICTION;
	AE_RET(__ae_open_internal_session(
	    conn, "lookaside table", true, session_flags, &conn->las_session));

	/* Flag that the lookaside table has been created. */
	F_SET(conn, AE_CONN_LAS_OPEN);

	return (0);
}

/*
 * __ae_las_destroy --
 *	Destroy the database's lookaside store.
 */
int
__ae_las_destroy(AE_SESSION_IMPL *session)
{
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	AE_SESSION *ae_session;

	conn = S2C(session);

	if (conn->las_session == NULL)
		return (0);

	ae_session = &conn->las_session->iface;
	ret = ae_session->close(ae_session, NULL);

	conn->las_session = NULL;

	return (ret);
}

/*
 * __ae_las_set_written --
 *	Flag that the lookaside table has been written.
 */
void
__ae_las_set_written(AE_SESSION_IMPL *session)
{
	AE_CONNECTION_IMPL *conn;

	conn = S2C(session);
	if (!conn->las_written) {
		conn->las_written = true;

		/*
		 * Push the flag: unnecessary, but from now page reads must deal
		 * with lookaside table records, and we only do the write once.
		 */
		AE_FULL_BARRIER();
	}
}

/*
 * __ae_las_is_written --
 *	Return if the lookaside table has been written.
 */
bool
__ae_las_is_written(AE_SESSION_IMPL *session)
{
	return (S2C(session)->las_written);
}

/*
 * __ae_las_cursor_create --
 *	Open a new lookaside table cursor.
 */
int
__ae_las_cursor_create(AE_SESSION_IMPL *session, AE_CURSOR **cursorp)
{
	AE_BTREE *btree;
	const char *open_cursor_cfg[] = {
	    AE_CONFIG_BASE(session, AE_SESSION_open_cursor), NULL };

	AE_RET(__ae_open_cursor(
	    session, AE_LAS_URI, NULL, open_cursor_cfg, cursorp));

	/*
	 * Set special flags for the lookaside table: the lookaside flag (used,
	 * for example, to avoid writing records during reconciliation), also
	 * turn off checkpoints and logging.
	 *
	 * Test flags before setting them so updates can't race in subsequent
	 * opens (the first update is safe because it's single-threaded from
	 * archengine_open).
	 */
	btree = S2BT(session);
	if (!F_ISSET(btree, AE_BTREE_LOOKASIDE))
		F_SET(btree, AE_BTREE_LOOKASIDE);
	if (!F_ISSET(btree, AE_BTREE_NO_CHECKPOINT))
		F_SET(btree, AE_BTREE_NO_CHECKPOINT);
	if (!F_ISSET(btree, AE_BTREE_NO_LOGGING))
		F_SET(btree, AE_BTREE_NO_LOGGING);

	return (0);
}

/*
 * __ae_las_cursor --
 *	Return a lookaside cursor.
 */
int
__ae_las_cursor(
    AE_SESSION_IMPL *session, AE_CURSOR **cursorp, uint32_t *session_flags)
{
	AE_CONNECTION_IMPL *conn;

	*cursorp = NULL;

	/*
	 * We don't want to get tapped for eviction after we start using the
	 * lookaside cursor; save a copy of the current eviction state, we'll
	 * turn eviction off before we return.
	 *
	 * Don't cache lookaside table pages, we're here because of eviction
	 * problems and there's no reason to believe lookaside pages will be
	 * useful more than once.
	 */
	*session_flags =
	    F_ISSET(session, AE_SESSION_NO_CACHE | AE_SESSION_NO_EVICTION);

	conn = S2C(session);

	/*
	 * Some threads have their own lookaside table cursors, else lock the
	 * shared lookaside cursor.
	 */
	if (F_ISSET(session, AE_SESSION_LOOKASIDE_CURSOR))
		*cursorp = session->las_cursor;
	else {
		__ae_spin_lock(session, &conn->las_lock);
		*cursorp = conn->las_session->las_cursor;
	}

	/* Turn caching and eviction off. */
	F_SET(session, AE_SESSION_NO_CACHE | AE_SESSION_NO_EVICTION);

	return (0);
}

/*
 * __ae_las_cursor_close --
 *	Discard a lookaside cursor.
 */
int
__ae_las_cursor_close(
	AE_SESSION_IMPL *session, AE_CURSOR **cursorp, uint32_t session_flags)
{
	AE_CONNECTION_IMPL *conn;
	AE_CURSOR *cursor;
	AE_DECL_RET;

	conn = S2C(session);

	if ((cursor = *cursorp) == NULL)
		return (0);
	*cursorp = NULL;

	/* Reset the cursor. */
	ret = cursor->reset(cursor);

	/*
	 * We turned off caching and eviction while the lookaside cursor was in
	 * use, restore the session's flags.
	 */
	F_CLR(session, AE_SESSION_NO_CACHE | AE_SESSION_NO_EVICTION);
	F_SET(session, session_flags);

	/*
	 * Some threads have their own lookaside table cursors, else unlock the
	 * shared lookaside cursor.
	 */
	if (!F_ISSET(session, AE_SESSION_LOOKASIDE_CURSOR))
		__ae_spin_unlock(session, &conn->las_lock);

	return (ret);
}

/*
 * __ae_las_sweep --
 *	Sweep the lookaside table.
 */
int
__ae_las_sweep(AE_SESSION_IMPL *session)
{
	AE_CONNECTION_IMPL *conn;
	AE_CURSOR *cursor;
	AE_DECL_ITEM(las_addr);
	AE_DECL_ITEM(las_key);
	AE_DECL_RET;
	AE_ITEM *key;
	uint64_t cnt, las_counter, las_txnid;
	int64_t remove_cnt;
	uint32_t las_id, session_flags;
	int notused;

	conn = S2C(session);
	cursor = NULL;
	key = &conn->las_sweep_key;
	remove_cnt = 0;
	session_flags = 0;		/* [-Werror=maybe-uninitialized] */

	AE_ERR(__ae_scr_alloc(session, 0, &las_addr));
	AE_ERR(__ae_scr_alloc(session, 0, &las_key));

	AE_ERR(__ae_las_cursor(session, &cursor, &session_flags));

	/*
	 * If we're not starting a new sweep, position the cursor using the key
	 * from the last call (we don't care if we're before or after the key,
	 * just roughly in the same spot is fine).
	 */
	if (key->size != 0) {
		__ae_cursor_set_raw_key(cursor, key);
		ret = cursor->search_near(cursor, &notused);

		/*
		 * Don't search for the same key twice; if we don't set a new
		 * key below, it's because we've reached the end of the table
		 * and we want the next pass to start at the beginning of the
		 * table. Searching for the same key could leave us stuck at
		 * the end of the table, repeatedly checking the same rows.
		 */
		key->size = 0;
		if (ret != 0)
			goto srch_notfound;
	}

	/*
	 * The sweep server wakes up every 10 seconds (by default), it's a slow
	 * moving thread. Try to review the entire lookaside table once every 5
	 * minutes, or every 30 calls.
	 *
	 * The reason is because the lookaside table exists because we're seeing
	 * cache/eviction pressure (it allows us to trade performance and disk
	 * space for cache space), and it's likely lookaside blocks are being
	 * evicted, and reading them back in doesn't help things. A trickier,
	 * but possibly better, alternative might be to review all lookaside
	 * blocks in the cache in order to get rid of them, and slowly review
	 * lookaside blocks that have already been evicted.
	 */
	cnt = (uint64_t)AE_MAX(100, conn->las_record_cnt / 30);

	/* Discard pages we read as soon as we're done with them. */
	F_SET(session, AE_SESSION_NO_CACHE);

	/* Walk the file. */
	for (; cnt > 0 && (ret = cursor->next(cursor)) == 0; --cnt) {
		/*
		 * If the loop terminates after completing a work unit, we will
		 * continue the table sweep next time. Get a local copy of the
		 * sweep key, we're going to reset the cursor; do so before
		 * calling cursor.remove, cursor.remove can discard our hazard
		 * pointer and the page could be evicted from underneath us.
		 */
		if (cnt == 1) {
			AE_ERR(__ae_cursor_get_raw_key(cursor, key));
			if (!AE_DATA_IN_ITEM(key))
				AE_ERR(__ae_buf_set(
				    session, key, key->data, key->size));
		}

		AE_ERR(cursor->get_key(cursor,
		    &las_id, las_addr, &las_counter, &las_txnid, las_key));

		/*
		 * If the on-page record transaction ID associated with the
		 * record is globally visible, the record can be discarded.
		 *
		 * Cursor opened overwrite=true: won't return AE_NOTFOUND should
		 * another thread remove the record before we do, and the cursor
		 * remains positioned in that case.
		 */
		if (__ae_txn_visible_all(session, las_txnid)) {
			AE_ERR(cursor->remove(cursor));
			++remove_cnt;
		}
	}

srch_notfound:
	AE_ERR_NOTFOUND_OK(ret);

	if (0) {
err:		__ae_buf_free(session, key);
	}

	AE_TRET(__ae_las_cursor_close(session, &cursor, session_flags));

	/*
	 * If there were races to remove records, we can over-count.  All
	 * arithmetic is signed, so underflow isn't fatal, but check anyway so
	 * we don't skew low over time.
	 */
	if (remove_cnt > S2C(session)->las_record_cnt)
		S2C(session)->las_record_cnt = 0;
	else if (remove_cnt > 0)
		(void)__ae_atomic_subi64(&conn->las_record_cnt, remove_cnt);

	F_CLR(session, AE_SESSION_NO_CACHE);

	__ae_scr_free(session, &las_addr);
	__ae_scr_free(session, &las_key);

	return (ret);
}
