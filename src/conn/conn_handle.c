/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_connection_init --
 *	Structure initialization for a just-created AE_CONNECTION_IMPL handle.
 */
int
__ae_connection_init(AE_CONNECTION_IMPL *conn)
{
	AE_SESSION_IMPL *session;
	u_int i;

	session = conn->default_session;

	for (i = 0; i < AE_HASH_ARRAY_SIZE; i++) {
		TAILQ_INIT(&conn->dhhash[i]);	/* Data handle hash lists */
		TAILQ_INIT(&conn->fhhash[i]);	/* File handle hash lists */
	}

	TAILQ_INIT(&conn->dhqh);		/* Data handle list */
	TAILQ_INIT(&conn->dlhqh);		/* Library list */
	TAILQ_INIT(&conn->dsrcqh);		/* Data source list */
	TAILQ_INIT(&conn->fhqh);		/* File list */
	TAILQ_INIT(&conn->collqh);		/* Collator list */
	TAILQ_INIT(&conn->compqh);		/* Compressor list */
	TAILQ_INIT(&conn->encryptqh);		/* Encryptor list */
	TAILQ_INIT(&conn->extractorqh);		/* Extractor list */

	TAILQ_INIT(&conn->lsmqh);		/* AE_LSM_TREE list */

	/* Setup the LSM work queues. */
	TAILQ_INIT(&conn->lsm_manager.switchqh);
	TAILQ_INIT(&conn->lsm_manager.appqh);
	TAILQ_INIT(&conn->lsm_manager.managerqh);

	/* Configuration. */
	AE_RET(__ae_conn_config_init(session));

	/* Statistics. */
	__ae_stat_connection_init(conn);

	/* Locks. */
	AE_RET(__ae_spin_init(session, &conn->api_lock, "api"));
	AE_RET(__ae_spin_init(session, &conn->checkpoint_lock, "checkpoint"));
	AE_RET(__ae_spin_init(session, &conn->dhandle_lock, "data handle"));
	AE_RET(__ae_spin_init(session, &conn->encryptor_lock, "encryptor"));
	AE_RET(__ae_spin_init(session, &conn->fh_lock, "file list"));
	AE_RET(__ae_rwlock_alloc(session,
	    &conn->hot_backup_lock, "hot backup"));
	AE_RET(__ae_spin_init(session, &conn->las_lock, "lookaside table"));
	AE_RET(__ae_spin_init(session, &conn->reconfig_lock, "reconfigure"));
	AE_RET(__ae_spin_init(session, &conn->schema_lock, "schema"));
	AE_RET(__ae_spin_init(session, &conn->table_lock, "table creation"));
	AE_RET(__ae_spin_init(session, &conn->turtle_lock, "turtle file"));

	AE_RET(__ae_calloc_def(session, AE_PAGE_LOCKS, &conn->page_lock));
	AE_CACHE_LINE_ALIGNMENT_VERIFY(session, conn->page_lock);
	for (i = 0; i < AE_PAGE_LOCKS; ++i)
		AE_RET(
		    __ae_spin_init(session, &conn->page_lock[i], "btree page"));

	/* Setup the spin locks for the LSM manager queues. */
	AE_RET(__ae_spin_init(session,
	    &conn->lsm_manager.app_lock, "LSM application queue lock"));
	AE_RET(__ae_spin_init(session,
	    &conn->lsm_manager.manager_lock, "LSM manager queue lock"));
	AE_RET(__ae_spin_init(
	    session, &conn->lsm_manager.switch_lock, "LSM switch queue lock"));
	AE_RET(__ae_cond_alloc(
	    session, "LSM worker cond", false, &conn->lsm_manager.work_cond));

	/*
	 * Generation numbers.
	 *
	 * Start split generations at one.  Threads publish this generation
	 * number before examining tree structures, and zero when they leave.
	 * We need to distinguish between threads that are in a tree before the
	 * first split has happened, and threads that are not in a tree.
	 */
	conn->split_gen = 1;

	/*
	 * Block manager.
	 * XXX
	 * If there's ever a second block manager, we'll want to make this
	 * more opaque, but for now this is simpler.
	 */
	AE_RET(__ae_spin_init(session, &conn->block_lock, "block manager"));
	for (i = 0; i < AE_HASH_ARRAY_SIZE; i++)
		TAILQ_INIT(&conn->blockhash[i]);/* Block handle hash lists */
	TAILQ_INIT(&conn->blockqh);		/* Block manager list */

	return (0);
}

/*
 * __ae_connection_destroy --
 *	Destroy the connection's underlying AE_CONNECTION_IMPL structure.
 */
int
__ae_connection_destroy(AE_CONNECTION_IMPL *conn)
{
	AE_DECL_RET;
	AE_SESSION_IMPL *session;
	u_int i;

	/* Check there's something to destroy. */
	if (conn == NULL)
		return (0);

	session = conn->default_session;

	/*
	 * Close remaining open files (before discarding the mutex, the
	 * underlying file-close code uses the mutex to guard lists of
	 * open files.
	 */
	AE_TRET(__ae_close(session, &conn->lock_fh));

	/* Remove from the list of connections. */
	__ae_spin_lock(session, &__ae_process.spinlock);
	TAILQ_REMOVE(&__ae_process.connqh, conn, q);
	__ae_spin_unlock(session, &__ae_process.spinlock);

	/* Configuration */
	__ae_conn_config_discard(session);		/* configuration */

	__ae_conn_foc_discard(session);			/* free-on-close */

	__ae_spin_destroy(session, &conn->api_lock);
	__ae_spin_destroy(session, &conn->block_lock);
	__ae_spin_destroy(session, &conn->checkpoint_lock);
	__ae_spin_destroy(session, &conn->dhandle_lock);
	__ae_spin_destroy(session, &conn->encryptor_lock);
	__ae_spin_destroy(session, &conn->fh_lock);
	AE_TRET(__ae_rwlock_destroy(session, &conn->hot_backup_lock));
	__ae_spin_destroy(session, &conn->las_lock);
	__ae_spin_destroy(session, &conn->reconfig_lock);
	__ae_spin_destroy(session, &conn->schema_lock);
	__ae_spin_destroy(session, &conn->table_lock);
	__ae_spin_destroy(session, &conn->turtle_lock);
	for (i = 0; i < AE_PAGE_LOCKS; ++i)
		__ae_spin_destroy(session, &conn->page_lock[i]);
	__ae_free(session, conn->page_lock);

	/* Free allocated memory. */
	__ae_free(session, conn->cfg);
	__ae_free(session, conn->home);
	__ae_free(session, conn->error_prefix);
	__ae_free(session, conn->sessions);

	__ae_free(NULL, conn);
	return (ret);
}
