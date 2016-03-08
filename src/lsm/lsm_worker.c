/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

static int __lsm_worker_general_op(
    AE_SESSION_IMPL *, AE_LSM_WORKER_ARGS *, bool *);
static AE_THREAD_RET __lsm_worker(void *);

/*
 * __ae_lsm_worker_start --
 *	A wrapper around the LSM worker thread start.
 */
int
__ae_lsm_worker_start(AE_SESSION_IMPL *session, AE_LSM_WORKER_ARGS *args)
{
	AE_RET(__ae_verbose(session, AE_VERB_LSM_MANAGER,
	    "Start LSM worker %d type 0x%x", args->id, args->type));
	return (__ae_thread_create(session, &args->tid, __lsm_worker, args));
}

/*
 * __lsm_worker_general_op --
 *	Execute a single bloom, drop or flush work unit.
 */
static int
__lsm_worker_general_op(
    AE_SESSION_IMPL *session, AE_LSM_WORKER_ARGS *cookie, bool *completed)
{
	AE_DECL_RET;
	AE_LSM_CHUNK *chunk;
	AE_LSM_WORK_UNIT *entry;
	bool force;

	*completed = false;
	/*
	 * Return if this thread cannot process a bloom, drop or flush.
	 */
	if (!FLD_ISSET(cookie->type,
	    AE_LSM_WORK_BLOOM | AE_LSM_WORK_DROP | AE_LSM_WORK_FLUSH))
		return (AE_NOTFOUND);

	if ((ret = __ae_lsm_manager_pop_entry(session,
	    cookie->type, &entry)) != 0 || entry == NULL)
		return (ret);

	if (entry->type == AE_LSM_WORK_FLUSH) {
		force = F_ISSET(entry, AE_LSM_WORK_FORCE);
		F_CLR(entry, AE_LSM_WORK_FORCE);
		AE_ERR(__ae_lsm_get_chunk_to_flush(session,
		    entry->lsm_tree, force, &chunk));
		/*
		 * If we got a chunk to flush, checkpoint it.
		 */
		if (chunk != NULL) {
			AE_ERR(__ae_verbose(session, AE_VERB_LSM,
			    "Flush%s chunk %d %s",
			    force ? " w/ force" : "",
			    chunk->id, chunk->uri));
			ret = __ae_lsm_checkpoint_chunk(
			    session, entry->lsm_tree, chunk);
			AE_ASSERT(session, chunk->refcnt > 0);
			(void)__ae_atomic_sub32(&chunk->refcnt, 1);
			AE_ERR(ret);
		}
	} else if (entry->type == AE_LSM_WORK_DROP)
		AE_ERR(__ae_lsm_free_chunks(session, entry->lsm_tree));
	else if (entry->type == AE_LSM_WORK_BLOOM)
		AE_ERR(__ae_lsm_work_bloom(session, entry->lsm_tree));
	*completed = true;

err:	__ae_lsm_manager_free_work_unit(session, entry);
	return (ret);
}

/*
 * __lsm_worker --
 *	A thread that executes work units for all open LSM trees.
 */
static AE_THREAD_RET
__lsm_worker(void *arg)
{
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	AE_LSM_WORK_UNIT *entry;
	AE_LSM_WORKER_ARGS *cookie;
	AE_SESSION_IMPL *session;
	bool progress, ran;

	cookie = (AE_LSM_WORKER_ARGS *)arg;
	session = cookie->session;
	conn = S2C(session);

	entry = NULL;
	while (F_ISSET(conn, AE_CONN_SERVER_RUN) &&
	    F_ISSET(cookie, AE_LSM_WORKER_RUN)) {
		progress = false;

		/*
		 * Workers process the different LSM work queues.  Some workers
		 * can handle several or all work unit types.  So the code is
		 * prioritized so important operations happen first.
		 * Switches are the highest priority.
		 */
		while (FLD_ISSET(cookie->type, AE_LSM_WORK_SWITCH) &&
		    (ret = __ae_lsm_manager_pop_entry(
		    session, AE_LSM_WORK_SWITCH, &entry)) == 0 &&
		    entry != NULL)
			AE_ERR(
			    __ae_lsm_work_switch(session, &entry, &progress));
		/* Flag an error if the pop failed. */
		AE_ERR(ret);

		/*
		 * Next the general operations.
		 */
		ret = __lsm_worker_general_op(session, cookie, &ran);
		if (ret == EBUSY || ret == AE_NOTFOUND)
			ret = 0;
		AE_ERR(ret);
		progress = progress || ran;

		/*
		 * Finally see if there is any merge work we can do.  This is
		 * last because the earlier operations may result in adding
		 * merge work to the queue.
		 */
		if (FLD_ISSET(cookie->type, AE_LSM_WORK_MERGE) &&
		    (ret = __ae_lsm_manager_pop_entry(
		    session, AE_LSM_WORK_MERGE, &entry)) == 0 &&
		    entry != NULL) {
			AE_ASSERT(session, entry->type == AE_LSM_WORK_MERGE);
			ret = __ae_lsm_merge(session,
			    entry->lsm_tree, cookie->id);
			if (ret == AE_NOTFOUND) {
				F_CLR(entry->lsm_tree, AE_LSM_TREE_COMPACTING);
				ret = 0;
			} else if (ret == EBUSY)
				ret = 0;

			/* Paranoia: clear session state. */
			session->dhandle = NULL;

			__ae_lsm_manager_free_work_unit(session, entry);
			entry = NULL;
			progress = true;
		}
		/* Flag an error if the pop failed. */
		AE_ERR(ret);

		/* Don't busy wait if there was any work to do. */
		if (!progress) {
			AE_ERR(
			    __ae_cond_wait(session, cookie->work_cond, 10000));
			continue;
		}
	}

	if (ret != 0) {
err:		__ae_lsm_manager_free_work_unit(session, entry);
		AE_PANIC_MSG(session, ret,
		    "Error in LSM worker thread %d", cookie->id);
	}
	return (AE_THREAD_RET_VALUE);
}
