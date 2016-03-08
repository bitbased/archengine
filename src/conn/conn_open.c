/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_connection_open --
 *	Open a connection.
 */
int
__ae_connection_open(AE_CONNECTION_IMPL *conn, const char *cfg[])
{
	AE_SESSION_IMPL *session;

	/* Default session. */
	session = conn->default_session;
	AE_ASSERT(session, session->iface.connection == &conn->iface);

	/*
	 * Tell internal server threads to run: this must be set before opening
	 * any sessions.
	 */
	F_SET(conn, AE_CONN_SERVER_RUN | AE_CONN_LOG_SERVER_RUN);

	/* AE_SESSION_IMPL array. */
	AE_RET(__ae_calloc(session,
	    conn->session_size, sizeof(AE_SESSION_IMPL), &conn->sessions));
	AE_CACHE_LINE_ALIGNMENT_VERIFY(session, conn->sessions);

	/*
	 * Open the default session.  We open this before starting service
	 * threads because those may allocate and use session resources that
	 * need to get cleaned up on close.
	 */
	AE_RET(__ae_open_internal_session(
	    conn, "connection", false, 0, &session));

	/*
	 * The connection's default session is originally a static structure,
	 * swap that out for a more fully-functional session.  It's necessary
	 * to have this step: the session allocation code uses the connection's
	 * session, and if we pass a reference to the default session as the
	 * place to store the allocated session, things get confused and error
	 * handling can be corrupted.  So, we allocate into a stack variable
	 * and then assign it on success.
	 */
	conn->default_session = session;

	/*
	 * Publish: there must be a barrier to ensure the connection structure
	 * fields are set before other threads read from the pointer.
	 */
	AE_WRITE_BARRIER();

	/* Create the cache. */
	AE_RET(__ae_cache_create(session, cfg));

	/* Initialize transaction support. */
	AE_RET(__ae_txn_global_init(session, cfg));

	return (0);
}

/*
 * __ae_connection_close --
 *	Close a connection handle.
 */
int
__ae_connection_close(AE_CONNECTION_IMPL *conn)
{
	AE_CONNECTION *ae_conn;
	AE_DECL_RET;
	AE_DLH *dlh;
	AE_FH *fh;
	AE_SESSION_IMPL *s, *session;
	AE_TXN_GLOBAL *txn_global;
	u_int i;

	ae_conn = &conn->iface;
	txn_global = &conn->txn_global;
	session = conn->default_session;

	/*
	 * We're shutting down.  Make sure everything gets freed.
	 *
	 * It's possible that the eviction server is in the middle of a long
	 * operation, with a transaction ID pinned.  In that case, we will loop
	 * here until the transaction ID is released, when the oldest
	 * transaction ID will catch up with the current ID.
	 */
	for (;;) {
		__ae_txn_update_oldest(session, true);
		if (txn_global->oldest_id == txn_global->current)
			break;
		__ae_yield();
	}

	/* Clear any pending async ops. */
	AE_TRET(__ae_async_flush(session));

	/*
	 * Shut down server threads other than the eviction server, which is
	 * needed later to close btree handles.  Some of these threads access
	 * btree handles, so take care in ordering shutdown to make sure they
	 * exit before files are closed.
	 */
	F_CLR(conn, AE_CONN_SERVER_RUN);
	AE_TRET(__ae_async_destroy(session));
	AE_TRET(__ae_lsm_manager_destroy(session));
	AE_TRET(__ae_sweep_destroy(session));

	F_SET(conn, AE_CONN_CLOSING);

	AE_TRET(__ae_checkpoint_server_destroy(session));
	AE_TRET(__ae_statlog_destroy(session, true));
	AE_TRET(__ae_evict_destroy(session));

	/* Shut down the lookaside table, after all eviction is complete. */
	AE_TRET(__ae_las_destroy(session));

	/* Close open data handles. */
	AE_TRET(__ae_conn_dhandle_discard(session));

	/* Shut down metadata tracking, required before creating tables. */
	AE_TRET(__ae_meta_track_destroy(session));

	/*
	 * Now that all data handles are closed, tell logging that a checkpoint
	 * has completed then shut down the log manager (only after closing
	 * data handles).  The call to destroy the log manager is outside the
	 * conditional because we allocate the log path so that printlog can
	 * run without running logging or recovery.
	 */
	if (FLD_ISSET(conn->log_flags, AE_CONN_LOG_ENABLED) &&
	    FLD_ISSET(conn->log_flags, AE_CONN_LOG_RECOVER_DONE))
		AE_TRET(__ae_txn_checkpoint_log(
		    session, true, AE_TXN_LOG_CKPT_STOP, NULL));
	F_CLR(conn, AE_CONN_LOG_SERVER_RUN);
	AE_TRET(__ae_logmgr_destroy(session));

	/* Free memory for collators, compressors, data sources. */
	AE_TRET(__ae_conn_remove_collator(session));
	AE_TRET(__ae_conn_remove_compressor(session));
	AE_TRET(__ae_conn_remove_data_source(session));
	AE_TRET(__ae_conn_remove_encryptor(session));
	AE_TRET(__ae_conn_remove_extractor(session));

	/*
	 * Complain if files weren't closed, ignoring the lock file, we'll
	 * close it in a minute.
	 */
	TAILQ_FOREACH(fh, &conn->fhqh, q) {
		if (fh == conn->lock_fh)
			continue;

		__ae_errx(session,
		    "Connection has open file handles: %s", fh->name);
		AE_TRET(__ae_close(session, &fh));
		fh = TAILQ_FIRST(&conn->fhqh);
	}

	/* Disconnect from shared cache - must be before cache destroy. */
	AE_TRET(__ae_conn_cache_pool_destroy(session));

	/* Discard the cache. */
	AE_TRET(__ae_cache_destroy(session));

	/* Discard transaction state. */
	AE_TRET(__ae_txn_global_destroy(session));

	/* Close extensions, first calling any unload entry point. */
	while ((dlh = TAILQ_FIRST(&conn->dlhqh)) != NULL) {
		TAILQ_REMOVE(&conn->dlhqh, dlh, q);

		if (dlh->terminate != NULL)
			AE_TRET(dlh->terminate(ae_conn));
		AE_TRET(__ae_dlclose(session, dlh));
	}

	/*
	 * Close the internal (default) session, and switch back to the dummy
	 * session in case of any error messages from the remaining operations
	 * while destroying the connection handle.
	 */
	if (session != &conn->dummy_session) {
		AE_TRET(session->iface.close(&session->iface, NULL));
		session = conn->default_session = &conn->dummy_session;
	}

	/*
	 * The session's split stash isn't discarded during normal session close
	 * because it may persist past the life of the session.  Discard it now.
	 */
	if ((s = conn->sessions) != NULL)
		for (i = 0; i < conn->session_size; ++s, ++i)
			__ae_split_stash_discard_all(session, s);

	/*
	 * The session's hazard pointer memory isn't discarded during normal
	 * session close because access to it isn't serialized.  Discard it
	 * now.
	 */
	if ((s = conn->sessions) != NULL)
		for (i = 0; i < conn->session_size; ++s, ++i) {
			/*
			 * If hash arrays were allocated, free them now.
			 */
			if (s->dhhash != NULL)
				__ae_free(session, s->dhhash);
			if (s->tablehash != NULL)
				__ae_free(session, s->tablehash);
			__ae_free(session, s->hazard);
		}

	/* Destroy the handle. */
	AE_TRET(__ae_connection_destroy(conn));

	return (ret);
}

/*
 * __ae_connection_workers --
 *	Start the worker threads.
 */
int
__ae_connection_workers(AE_SESSION_IMPL *session, const char *cfg[])
{
	/*
	 * Start the optional statistics thread.  Start statistics first so that
	 * other optional threads can know if statistics are enabled or not.
	 */
	AE_RET(__ae_statlog_create(session, cfg));
	AE_RET(__ae_logmgr_create(session, cfg));

	/*
	 * Run recovery.
	 * NOTE: This call will start (and stop) eviction if recovery is
	 * required.  Recovery must run before the lookaside table is created
	 * (because recovery will update the metadata), and before eviction is
	 * started for real.
	 */
	AE_RET(__ae_txn_recover(session));

	/*
	 * Start the optional logging/archive threads.
	 * NOTE: The log manager must be started before checkpoints so that the
	 * checkpoint server knows if logging is enabled.  It must also be
	 * started before any operation that can commit, or the commit can
	 * block.
	 */
	AE_RET(__ae_logmgr_open(session));

	/* Initialize metadata tracking, required before creating tables. */
	AE_RET(__ae_meta_track_init(session));

	/* Create the lookaside table. */
	AE_RET(__ae_las_create(session));

	/*
	 * Start eviction threads.
	 * NOTE: Eviction must be started after the lookaside table is created.
	 */
	AE_RET(__ae_evict_create(session));

	/* Start the handle sweep thread. */
	AE_RET(__ae_sweep_create(session));

	/* Start the optional async threads. */
	AE_RET(__ae_async_create(session, cfg));

	/* Start the optional checkpoint thread. */
	AE_RET(__ae_checkpoint_server_create(session, cfg));

	return (0);
}
