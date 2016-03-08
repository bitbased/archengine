/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

#define	AE_DHANDLE_CAN_DISCARD(dhandle)					\
	(!F_ISSET(dhandle, AE_DHANDLE_EXCLUSIVE | AE_DHANDLE_OPEN) &&	\
	dhandle->session_inuse == 0 && dhandle->session_ref == 0)

/*
 * __sweep_mark --
 *	Mark idle handles with a time of death, and note if we see dead
 *	handles.
 */
static int
__sweep_mark(AE_SESSION_IMPL *session, time_t now)
{
	AE_CONNECTION_IMPL *conn;
	AE_DATA_HANDLE *dhandle;

	conn = S2C(session);

	TAILQ_FOREACH(dhandle, &conn->dhqh, q) {
		if (AE_IS_METADATA(dhandle))
			continue;

		/*
		 * There are some internal increments of the in-use count such
		 * as eviction.  Don't keep handles alive because of those
		 * cases, but if we see multiple cursors open, clear the time
		 * of death.
		 */
		if (dhandle->session_inuse > 1)
			dhandle->timeofdeath = 0;

		/*
		 * If the handle is open exclusive or currently in use, or the
		 * time of death is already set, move on.
		 */
		if (F_ISSET(dhandle, AE_DHANDLE_EXCLUSIVE) ||
		    dhandle->session_inuse > 0 ||
		    dhandle->timeofdeath != 0)
			continue;

		dhandle->timeofdeath = now;
		AE_STAT_FAST_CONN_INCR(session, dh_sweep_tod);
	}

	return (0);
}

/*
 * __sweep_expire_one --
 *	Mark a single handle dead.
 */
static int
__sweep_expire_one(AE_SESSION_IMPL *session)
{
	AE_BTREE *btree;
	AE_DATA_HANDLE *dhandle;
	AE_DECL_RET;
	bool evict_reset;

	btree = S2BT(session);
	dhandle = session->dhandle;
	evict_reset = false;

	/*
	 * Acquire an exclusive lock on the handle and mark it dead.
	 *
	 * The close would require I/O if an update cannot be written
	 * (updates in a no-longer-referenced file might not yet be
	 * globally visible if sessions have disjoint sets of files
	 * open).  In that case, skip it: we'll retry the close the
	 * next time, after the transaction state has progressed.
	 *
	 * We don't set AE_DHANDLE_EXCLUSIVE deliberately, we want
	 * opens to block on us and then retry rather than returning an
	 * EBUSY error to the application.  This is done holding the
	 * handle list lock so that connection-level handle searches
	 * never need to retry.
	 */
	AE_RET(__ae_try_writelock(session, dhandle->rwlock));

	/* Only sweep clean trees where all updates are visible. */
	if (btree->modified ||
	    !__ae_txn_visible_all(session, btree->rec_max_txn))
		goto err;

	/* Ensure that we aren't racing with the eviction server */
	AE_ERR(__ae_evict_file_exclusive_on(session, &evict_reset));

	/*
	 * Mark the handle as dead and close the underlying file
	 * handle. Closing the handle decrements the open file count,
	 * meaning the close loop won't overrun the configured minimum.
	 */
	ret = __ae_conn_btree_sync_and_close(session, false, true);

	if (evict_reset)
		__ae_evict_file_exclusive_off(session);

err:	AE_TRET(__ae_writeunlock(session, dhandle->rwlock));

	return (ret);
}

/*
 * __sweep_expire --
 *	Mark trees dead if they are clean and haven't been accessed recently,
 *	until we have reached the configured minimum number of handles.
 */
static int
__sweep_expire(AE_SESSION_IMPL *session, time_t now)
{
	AE_CONNECTION_IMPL *conn;
	AE_DATA_HANDLE *dhandle;
	AE_DECL_RET;

	conn = S2C(session);

	TAILQ_FOREACH(dhandle, &conn->dhqh, q) {
		/*
		 * Ignore open files once the btree file count is below the
		 * minimum number of handles.
		 */
		if (conn->open_btree_count < conn->sweep_handles_min)
			break;

		if (AE_IS_METADATA(dhandle) ||
		    !F_ISSET(dhandle, AE_DHANDLE_OPEN) ||
		    dhandle->session_inuse != 0 ||
		    dhandle->timeofdeath == 0 ||
		    difftime(now, dhandle->timeofdeath) <=
		    conn->sweep_idle_time)
			continue;

		AE_WITH_DHANDLE(session, dhandle,
		    ret = __sweep_expire_one(session));
		AE_RET_BUSY_OK(ret);
	}

	return (0);
}

/*
 * __sweep_discard_trees --
 *	Discard pages from dead trees.
 */
static int
__sweep_discard_trees(AE_SESSION_IMPL *session, u_int *dead_handlesp)
{
	AE_CONNECTION_IMPL *conn;
	AE_DATA_HANDLE *dhandle;
	AE_DECL_RET;

	conn = S2C(session);

	*dead_handlesp = 0;

	TAILQ_FOREACH(dhandle, &conn->dhqh, q) {
		if (AE_DHANDLE_CAN_DISCARD(dhandle))
			++*dead_handlesp;

		if (!F_ISSET(dhandle, AE_DHANDLE_OPEN) ||
		    !F_ISSET(dhandle, AE_DHANDLE_DEAD))
			continue;

		/* If the handle is marked "dead", flush it from cache. */
		AE_WITH_DHANDLE(session, dhandle, ret =
		    __ae_conn_btree_sync_and_close(session, false, false));

		/* We closed the btree handle. */
		if (ret == 0) {
			AE_STAT_FAST_CONN_INCR(session, dh_sweep_close);
			++*dead_handlesp;
		} else
			AE_STAT_FAST_CONN_INCR(session, dh_sweep_ref);

		AE_RET_BUSY_OK(ret);
	}

	return (0);
}

/*
 * __sweep_remove_one --
 *	Remove a closed handle from the connection list.
 */
static int
__sweep_remove_one(AE_SESSION_IMPL *session, AE_DATA_HANDLE *dhandle)
{
	AE_DECL_RET;

	/* Try to get exclusive access. */
	AE_RET(__ae_try_writelock(session, dhandle->rwlock));

	/*
	 * If there are no longer any references to the handle in any
	 * sessions, attempt to discard it.
	 */
	if (!AE_DHANDLE_CAN_DISCARD(dhandle))
		AE_ERR(EBUSY);

	AE_WITH_DHANDLE(session, dhandle,
	    ret = __ae_conn_dhandle_discard_single(session, false, true));

	/*
	 * If the handle was not successfully discarded, unlock it and
	 * don't retry the discard until it times out again.
	 */
	if (ret != 0) {
err:		AE_TRET(__ae_writeunlock(session, dhandle->rwlock));
	}

	return (ret);
}

/*
 * __sweep_remove_handles --
 *	Remove closed handles from the connection list.
 */
static int
__sweep_remove_handles(AE_SESSION_IMPL *session)
{
	AE_CONNECTION_IMPL *conn;
	AE_DATA_HANDLE *dhandle, *dhandle_next;
	AE_DECL_RET;

	conn = S2C(session);

	for (dhandle = TAILQ_FIRST(&conn->dhqh);
	    dhandle != NULL;
	    dhandle = dhandle_next) {
		dhandle_next = TAILQ_NEXT(dhandle, q);
		if (AE_IS_METADATA(dhandle))
			continue;
		if (!AE_DHANDLE_CAN_DISCARD(dhandle))
			continue;

		AE_WITH_HANDLE_LIST_LOCK(session,
		    ret = __sweep_remove_one(session, dhandle));
		if (ret == 0)
			AE_STAT_FAST_CONN_INCR(session, dh_sweep_remove);
		else
			AE_STAT_FAST_CONN_INCR(session, dh_sweep_ref);
		AE_RET_BUSY_OK(ret);
	}

	return (ret == EBUSY ? 0 : ret);
}

/*
 * __sweep_server --
 *	The handle sweep server thread.
 */
static AE_THREAD_RET
__sweep_server(void *arg)
{
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;
	time_t now;
	u_int dead_handles;

	session = arg;
	conn = S2C(session);

	/*
	 * Sweep for dead and excess handles.
	 */
	while (F_ISSET(conn, AE_CONN_SERVER_RUN) &&
	    F_ISSET(conn, AE_CONN_SERVER_SWEEP)) {
		/* Wait until the next event. */
		AE_ERR(__ae_cond_wait(session,
		    conn->sweep_cond, conn->sweep_interval * AE_MILLION));
		AE_ERR(__ae_seconds(session, &now));

		AE_STAT_FAST_CONN_INCR(session, dh_sweeps);

		/*
		 * Sweep the lookaside table. If the lookaside table hasn't yet
		 * been written, there's no work to do.
		 */
		if (__ae_las_is_written(session))
			AE_ERR(__ae_las_sweep(session));

		/*
		 * Mark handles with a time of death, and report whether any
		 * handles are marked dead.  If sweep_idle_time is 0, handles
		 * never become idle.
		 */
		if (conn->sweep_idle_time != 0)
			AE_ERR(__sweep_mark(session, now));

		/*
		 * Close handles if we have reached the configured limit.
		 * If sweep_idle_time is 0, handles never become idle.
		 */
		if (conn->sweep_idle_time != 0 &&
		    conn->open_btree_count >= conn->sweep_handles_min)
			AE_ERR(__sweep_expire(session, now));

		AE_ERR(__sweep_discard_trees(session, &dead_handles));

		if (dead_handles > 0)
			AE_ERR(__sweep_remove_handles(session));
	}

	if (0) {
err:		AE_PANIC_MSG(session, ret, "handle sweep server error");
	}
	return (AE_THREAD_RET_VALUE);
}

/*
 * __ae_sweep_config --
 *	Pull out sweep configuration settings
 */
int
__ae_sweep_config(AE_SESSION_IMPL *session, const char *cfg[])
{
	AE_CONFIG_ITEM cval;
	AE_CONNECTION_IMPL *conn;

	conn = S2C(session);

	/*
	 * A non-zero idle time is incompatible with in-memory, and the default
	 * is non-zero; set the in-memory configuration idle time to zero.
	 */
	conn->sweep_idle_time = 0;
	AE_RET(__ae_config_gets(session, cfg, "in_memory", &cval));
	if (cval.val == 0) {
		AE_RET(__ae_config_gets(session,
		    cfg, "file_manager.close_idle_time", &cval));
		conn->sweep_idle_time = (uint64_t)cval.val;
	}

	AE_RET(__ae_config_gets(session,
	    cfg, "file_manager.close_scan_interval", &cval));
	conn->sweep_interval = (uint64_t)cval.val;

	AE_RET(__ae_config_gets(session,
	    cfg, "file_manager.close_handle_minimum", &cval));
	conn->sweep_handles_min = (uint64_t)cval.val;

	return (0);
}

/*
 * __ae_sweep_create --
 *	Start the handle sweep thread.
 */
int
__ae_sweep_create(AE_SESSION_IMPL *session)
{
	AE_CONNECTION_IMPL *conn;
	uint32_t session_flags;

	conn = S2C(session);

	/* Set first, the thread might run before we finish up. */
	F_SET(conn, AE_CONN_SERVER_SWEEP);

	/*
	 * Handle sweep does enough I/O it may be called upon to perform slow
	 * operations for the block manager.
	 *
	 * The sweep thread sweeps the lookaside table for outdated records,
	 * it gets its own cursor for that purpose.
	 *
	 * Don't tap the sweep thread for eviction.
	 */
	session_flags = AE_SESSION_CAN_WAIT |
	    AE_SESSION_LOOKASIDE_CURSOR | AE_SESSION_NO_EVICTION;
	AE_RET(__ae_open_internal_session(
	    conn, "sweep-server", true, session_flags, &conn->sweep_session));
	session = conn->sweep_session;

	AE_RET(__ae_cond_alloc(
	    session, "handle sweep server", false, &conn->sweep_cond));

	AE_RET(__ae_thread_create(
	    session, &conn->sweep_tid, __sweep_server, session));
	conn->sweep_tid_set = 1;

	return (0);
}

/*
 * __ae_sweep_destroy --
 *	Destroy the handle-sweep thread.
 */
int
__ae_sweep_destroy(AE_SESSION_IMPL *session)
{
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	AE_SESSION *ae_session;

	conn = S2C(session);

	F_CLR(conn, AE_CONN_SERVER_SWEEP);
	if (conn->sweep_tid_set) {
		AE_TRET(__ae_cond_signal(session, conn->sweep_cond));
		AE_TRET(__ae_thread_join(session, conn->sweep_tid));
		conn->sweep_tid_set = 0;
	}
	AE_TRET(__ae_cond_destroy(session, &conn->sweep_cond));

	if (conn->sweep_session != NULL) {
		ae_session = &conn->sweep_session->iface;
		AE_TRET(ae_session->close(ae_session, NULL));

		conn->sweep_session = NULL;
	}

	/* Discard any saved lookaside key. */
	__ae_buf_free(session, &conn->las_sweep_key);

	return (ret);
}
