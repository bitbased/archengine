/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

static int __lsm_manager_run_server(AE_SESSION_IMPL *);

static AE_THREAD_RET __lsm_worker_manager(void *);

/*
 * __ae_lsm_manager_config --
 *	Configure the LSM manager.
 */
int
__ae_lsm_manager_config(AE_SESSION_IMPL *session, const char **cfg)
{
	AE_CONNECTION_IMPL *conn;
	AE_CONFIG_ITEM cval;

	conn = S2C(session);

	AE_RET(__ae_config_gets(session, cfg, "lsm_manager.merge", &cval));
	if (cval.val)
		F_SET(conn, AE_CONN_LSM_MERGE);
	AE_RET(__ae_config_gets(
	    session, cfg, "lsm_manager.worker_thread_max", &cval));
	if (cval.val)
		conn->lsm_manager.lsm_workers_max = (uint32_t)cval.val;
	return (0);
}

/*
 * __lsm_general_worker_start --
 *	Start up all of the general LSM worker threads.
 */
static int
__lsm_general_worker_start(AE_SESSION_IMPL *session)
{
	AE_CONNECTION_IMPL *conn;
	AE_LSM_MANAGER *manager;
	AE_LSM_WORKER_ARGS *worker_args;

	conn = S2C(session);
	manager = &conn->lsm_manager;

	/*
	 * Start the worker threads or new worker threads if called via
	 * reconfigure. The LSM manager is worker[0].
	 * This should get more sophisticated in the future - only launching
	 * as many worker threads as are required to keep up with demand.
	 */
	AE_ASSERT(session, manager->lsm_workers > 0);
	for (; manager->lsm_workers < manager->lsm_workers_max;
	    manager->lsm_workers++) {
		worker_args =
		    &manager->lsm_worker_cookies[manager->lsm_workers];
		worker_args->work_cond = manager->work_cond;
		worker_args->id = manager->lsm_workers;
		/*
		 * The first worker only does switch and drop operations as
		 * these are both short operations and it is essential
		 * that switches are responsive to avoid introducing
		 * throttling stalls.
		 */
		if (manager->lsm_workers == 1)
			worker_args->type =
			    AE_LSM_WORK_DROP | AE_LSM_WORK_SWITCH;
		else {
			worker_args->type =
			    AE_LSM_WORK_BLOOM |
			    AE_LSM_WORK_DROP |
			    AE_LSM_WORK_FLUSH |
			    AE_LSM_WORK_SWITCH;
			/*
			 * Only allow half of the threads to run merges to
			 * avoid all all workers getting stuck in long-running
			 * merge operations. Make sure the first worker is
			 * allowed, so that there is at least one thread
			 * capable of running merges.  We know the first
			 * worker is id 2, so set merges on even numbered
			 * workers.
			 */
			if (manager->lsm_workers % 2 == 0)
				FLD_SET(worker_args->type, AE_LSM_WORK_MERGE);
		}
		F_SET(worker_args, AE_LSM_WORKER_RUN);
		AE_RET(__ae_lsm_worker_start(session, worker_args));
	}

	/*
	 * Setup the first worker properly - if there are only a minimal
	 * number of workers allow the first worker to flush. Otherwise a
	 * single merge can lead to switched chunks filling up the cache.
	 * This is separate to the main loop so that it is applied on startup
	 * and reconfigure.
	 */
	if (manager->lsm_workers_max == AE_LSM_MIN_WORKERS)
		FLD_SET(manager->lsm_worker_cookies[1].type, AE_LSM_WORK_FLUSH);
	else
		FLD_CLR(manager->lsm_worker_cookies[1].type, AE_LSM_WORK_FLUSH);

	return (0);
}

/*
 * __lsm_stop_workers --
 *	Stop worker threads until the number reaches the configured amount.
 */
static int
__lsm_stop_workers(AE_SESSION_IMPL *session)
{
	AE_LSM_MANAGER *manager;
	AE_LSM_WORKER_ARGS *worker_args;
	uint32_t i;

	manager = &S2C(session)->lsm_manager;
	/*
	 * Start at the end of the list of threads and stop them until we
	 * have the desired number.  We want to keep all active threads
	 * packed at the front of the worker array.
	 */
	AE_ASSERT(session, manager->lsm_workers != 0);
	for (i = manager->lsm_workers - 1; i >= manager->lsm_workers_max; i--) {
		worker_args = &manager->lsm_worker_cookies[i];
		/*
		 * Clear this worker's flag so it stops.
		 */
		F_CLR(worker_args, AE_LSM_WORKER_RUN);
		AE_ASSERT(session, worker_args->tid != 0);
		AE_RET(__ae_thread_join(session, worker_args->tid));
		worker_args->tid = 0;
		worker_args->type = 0;
		worker_args->flags = 0;
		manager->lsm_workers--;
		/*
		 * We do not clear the session because they are allocated
		 * statically when the connection was opened.
		 */
	}

	/*
	 * Setup the first worker properly - if there are only a minimal
	 * number of workers it should flush. Since the number of threads
	 * is being reduced the field can't already be set.
	 */
	if (manager->lsm_workers_max == AE_LSM_MIN_WORKERS)
		FLD_SET(manager->lsm_worker_cookies[1].type, AE_LSM_WORK_FLUSH);

	return (0);
}

/*
 * __ae_lsm_manager_reconfig --
 *	Re-configure the LSM manager.
 */
int
__ae_lsm_manager_reconfig(AE_SESSION_IMPL *session, const char **cfg)
{
	AE_LSM_MANAGER *manager;
	uint32_t orig_workers;

	manager = &S2C(session)->lsm_manager;
	orig_workers = manager->lsm_workers_max;

	AE_RET(__ae_lsm_manager_config(session, cfg));
	/*
	 * If LSM hasn't started yet, we simply reconfigured the settings
	 * and we'll let the normal code path start the threads.
	 */
	if (manager->lsm_workers_max == 0)
		return (0);
	if (manager->lsm_workers == 0)
		return (0);
	/*
	 * If the number of workers has not changed, we're done.
	 */
	if (orig_workers == manager->lsm_workers_max)
		return (0);
	/*
	 * If we want more threads, start them.
	 */
	if (manager->lsm_workers_max > orig_workers)
		return (__lsm_general_worker_start(session));

	/*
	 * Otherwise we want to reduce the number of workers.
	 */
	AE_ASSERT(session, manager->lsm_workers_max < orig_workers);
	AE_RET(__lsm_stop_workers(session));
	return (0);
}

/*
 * __ae_lsm_manager_start --
 *	Start the LSM management infrastructure. Our queues and locks were
 *	initialized when the connection was initialized.
 */
int
__ae_lsm_manager_start(AE_SESSION_IMPL *session)
{
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	AE_LSM_MANAGER *manager;
	AE_SESSION_IMPL *worker_session;
	uint32_t i;

	conn = S2C(session);
	manager = &conn->lsm_manager;

	/*
	 * We need at least a manager, a switch thread and a generic
	 * worker.
	 */
	AE_ASSERT(session, manager->lsm_workers_max > 2);

	/*
	 * Open sessions for all potential worker threads here - it's not
	 * safe to have worker threads open/close sessions themselves.
	 * All the LSM worker threads do their operations on read-only
	 * files. Use read-uncommitted isolation to avoid keeping
	 * updates in cache unnecessarily.
	 */
	for (i = 0; i < AE_LSM_MAX_WORKERS; i++) {
		AE_ERR(__ae_open_internal_session(
		    conn, "lsm-worker", false, 0, &worker_session));
		worker_session->isolation = AE_ISO_READ_UNCOMMITTED;
		manager->lsm_worker_cookies[i].session = worker_session;
	}

	/* Start the LSM manager thread. */
	AE_ERR(__ae_thread_create(session, &manager->lsm_worker_cookies[0].tid,
	    __lsm_worker_manager, &manager->lsm_worker_cookies[0]));

	F_SET(conn, AE_CONN_SERVER_LSM);

	if (0) {
err:		for (i = 0;
		    (worker_session =
		    manager->lsm_worker_cookies[i].session) != NULL;
		    i++)
			AE_TRET((&worker_session->iface)->close(
			    &worker_session->iface, NULL));
	}
	return (ret);
}

/*
 * __ae_lsm_manager_free_work_unit --
 *	Release an LSM tree work unit.
 */
void
__ae_lsm_manager_free_work_unit(
    AE_SESSION_IMPL *session, AE_LSM_WORK_UNIT *entry)
{
	if (entry != NULL) {
		AE_ASSERT(session, entry->lsm_tree->queue_ref > 0);

		(void)__ae_atomic_sub32(&entry->lsm_tree->queue_ref, 1);
		__ae_free(session, entry);
	}
}

/*
 * __ae_lsm_manager_destroy --
 *	Destroy the LSM manager threads and subsystem.
 */
int
__ae_lsm_manager_destroy(AE_SESSION_IMPL *session)
{
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	AE_LSM_MANAGER *manager;
	AE_LSM_WORK_UNIT *current;
	AE_SESSION *ae_session;
	uint32_t i;
	uint64_t removed;

	conn = S2C(session);
	manager = &conn->lsm_manager;
	removed = 0;

	if (manager->lsm_workers > 0) {
		/*
		 * Stop the main LSM manager thread first.
		 */
		while (F_ISSET(conn, AE_CONN_SERVER_LSM))
			__ae_yield();

		/* Clean up open LSM handles. */
		ret = __ae_lsm_tree_close_all(session);

		AE_TRET(__ae_thread_join(
		    session, manager->lsm_worker_cookies[0].tid));
		manager->lsm_worker_cookies[0].tid = 0;

		/* Release memory from any operations left on the queue. */
		while ((current = TAILQ_FIRST(&manager->switchqh)) != NULL) {
			TAILQ_REMOVE(&manager->switchqh, current, q);
			++removed;
			__ae_lsm_manager_free_work_unit(session, current);
		}
		while ((current = TAILQ_FIRST(&manager->appqh)) != NULL) {
			TAILQ_REMOVE(&manager->appqh, current, q);
			++removed;
			__ae_lsm_manager_free_work_unit(session, current);
		}
		while ((current = TAILQ_FIRST(&manager->managerqh)) != NULL) {
			TAILQ_REMOVE(&manager->managerqh, current, q);
			++removed;
			__ae_lsm_manager_free_work_unit(session, current);
		}

		/* Close all LSM worker sessions. */
		for (i = 0; i < AE_LSM_MAX_WORKERS; i++) {
			ae_session =
			    &manager->lsm_worker_cookies[i].session->iface;
			AE_TRET(ae_session->close(ae_session, NULL));
		}
	}
	AE_STAT_FAST_CONN_INCRV(session, lsm_work_units_discarded, removed);

	/* Free resources that are allocated in connection initialize */
	__ae_spin_destroy(session, &manager->switch_lock);
	__ae_spin_destroy(session, &manager->app_lock);
	__ae_spin_destroy(session, &manager->manager_lock);
	AE_TRET(__ae_cond_destroy(session, &manager->work_cond));

	return (ret);
}

/*
 * __lsm_manager_worker_shutdown --
 *	Shutdown the LSM manager and worker threads.
 */
static int
__lsm_manager_worker_shutdown(AE_SESSION_IMPL *session)
{
	AE_DECL_RET;
	AE_LSM_MANAGER *manager;
	u_int i;

	manager = &S2C(session)->lsm_manager;

	/*
	 * Wait for the rest of the LSM workers to shutdown. Stop at index
	 * one - since we (the manager) are at index 0.
	 */
	for (i = 1; i < manager->lsm_workers; i++) {
		AE_ASSERT(session, manager->lsm_worker_cookies[i].tid != 0);
		AE_TRET(__ae_cond_signal(session, manager->work_cond));
		AE_TRET(__ae_thread_join(
		    session, manager->lsm_worker_cookies[i].tid));
	}
	return (ret);
}

/*
 * __lsm_manager_run_server --
 *	Run manager thread operations.
 */
static int
__lsm_manager_run_server(AE_SESSION_IMPL *session)
{
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	AE_LSM_TREE *lsm_tree;
	struct timespec now;
	uint64_t fillms, pushms;
	bool dhandle_locked;

	conn = S2C(session);
	dhandle_locked = false;

	while (F_ISSET(conn, AE_CONN_SERVER_RUN)) {
		__ae_sleep(0, 10000);
		if (TAILQ_EMPTY(&conn->lsmqh))
			continue;
		__ae_spin_lock(session, &conn->dhandle_lock);
		F_SET(session, AE_SESSION_LOCKED_HANDLE_LIST);
		dhandle_locked = true;
		TAILQ_FOREACH(lsm_tree, &S2C(session)->lsmqh, q) {
			if (!F_ISSET(lsm_tree, AE_LSM_TREE_ACTIVE))
				continue;
			AE_ERR(__ae_epoch(session, &now));
			pushms = lsm_tree->work_push_ts.tv_sec == 0 ? 0 :
			    AE_TIMEDIFF_MS(now, lsm_tree->work_push_ts);
			fillms = 3 * lsm_tree->chunk_fill_ms;
			if (fillms == 0)
				fillms = 10000;
			/*
			 * If the tree appears to not be triggering enough
			 * LSM maintenance, help it out. Additional work units
			 * don't hurt, and can be necessary if some work
			 * units aren't completed for some reason.
			 * If the tree hasn't been modified, and there are
			 * more than 1 chunks - try to get the tree smaller
			 * so queries run faster.
			 * If we are getting aggressive - ensure there are
			 * enough work units that we can get chunks merged.
			 * If we aren't pushing enough work units, compared
			 * to how often new chunks are being created add some
			 * more.
			 */
			if (lsm_tree->queue_ref >= LSM_TREE_MAX_QUEUE)
				AE_STAT_FAST_CONN_INCR(session,
				    lsm_work_queue_max);
			else if ((!lsm_tree->modified &&
			    lsm_tree->nchunks > 1) ||
			    (lsm_tree->queue_ref == 0 &&
			    lsm_tree->nchunks > 1) ||
			    (lsm_tree->merge_aggressiveness >
			    AE_LSM_AGGRESSIVE_THRESHOLD &&
			     !F_ISSET(lsm_tree, AE_LSM_TREE_COMPACTING)) ||
			    pushms > fillms) {
				AE_ERR(__ae_lsm_manager_push_entry(
				    session, AE_LSM_WORK_SWITCH, 0, lsm_tree));
				AE_ERR(__ae_lsm_manager_push_entry(
				    session, AE_LSM_WORK_DROP, 0, lsm_tree));
				AE_ERR(__ae_lsm_manager_push_entry(
				    session, AE_LSM_WORK_FLUSH, 0, lsm_tree));
				AE_ERR(__ae_lsm_manager_push_entry(
				    session, AE_LSM_WORK_BLOOM, 0, lsm_tree));
				AE_ERR(__ae_verbose(session,
				    AE_VERB_LSM_MANAGER,
				    "MGR %s: queue %d mod %d nchunks %d"
				    " flags 0x%x aggressive %d pushms %" PRIu64
				    " fillms %" PRIu64,
				    lsm_tree->name, lsm_tree->queue_ref,
				    lsm_tree->modified, lsm_tree->nchunks,
				    lsm_tree->flags,
				    lsm_tree->merge_aggressiveness,
				    pushms, fillms));
				AE_ERR(__ae_lsm_manager_push_entry(
				    session, AE_LSM_WORK_MERGE, 0, lsm_tree));
			}
		}
		__ae_spin_unlock(session, &conn->dhandle_lock);
		F_CLR(session, AE_SESSION_LOCKED_HANDLE_LIST);
		dhandle_locked = false;
	}

err:	if (dhandle_locked) {
		__ae_spin_unlock(session, &conn->dhandle_lock);
		F_CLR(session, AE_SESSION_LOCKED_HANDLE_LIST);
	}
	return (ret);
}

/*
 * __lsm_worker_manager --
 *	A thread that manages all open LSM trees, and the shared LSM worker
 *	threads.
 */
static AE_THREAD_RET
__lsm_worker_manager(void *arg)
{
	AE_DECL_RET;
	AE_LSM_WORKER_ARGS *cookie;
	AE_SESSION_IMPL *session;

	cookie = (AE_LSM_WORKER_ARGS *)arg;
	session = cookie->session;

	AE_ERR(__lsm_general_worker_start(session));
	AE_ERR(__lsm_manager_run_server(session));
	AE_ERR(__lsm_manager_worker_shutdown(session));

	if (ret != 0) {
err:		AE_PANIC_MSG(session, ret, "LSM worker manager thread error");
	}
	F_CLR(S2C(session), AE_CONN_SERVER_LSM);
	return (AE_THREAD_RET_VALUE);
}

/*
 * __ae_lsm_manager_clear_tree --
 *	Remove all entries for a tree from the LSM manager queues. This
 *	introduces an inefficiency if LSM trees are being opened and closed
 *	regularly.
 */
int
__ae_lsm_manager_clear_tree(
    AE_SESSION_IMPL *session, AE_LSM_TREE *lsm_tree)
{
	AE_LSM_MANAGER *manager;
	AE_LSM_WORK_UNIT *current, *next;
	uint64_t removed;

	manager = &S2C(session)->lsm_manager;
	removed = 0;

	/* Clear out the tree from the switch queue */
	__ae_spin_lock(session, &manager->switch_lock);

	/* Structure the loop so that it's safe to free as we iterate */
	for (current = TAILQ_FIRST(&manager->switchqh);
	    current != NULL; current = next) {
		next = TAILQ_NEXT(current, q);
		if (current->lsm_tree != lsm_tree)
			continue;
		++removed;
		TAILQ_REMOVE(&manager->switchqh, current, q);
		__ae_lsm_manager_free_work_unit(session, current);
	}
	__ae_spin_unlock(session, &manager->switch_lock);
	/* Clear out the tree from the application queue */
	__ae_spin_lock(session, &manager->app_lock);
	for (current = TAILQ_FIRST(&manager->appqh);
	    current != NULL; current = next) {
		next = TAILQ_NEXT(current, q);
		if (current->lsm_tree != lsm_tree)
			continue;
		++removed;
		TAILQ_REMOVE(&manager->appqh, current, q);
		__ae_lsm_manager_free_work_unit(session, current);
	}
	__ae_spin_unlock(session, &manager->app_lock);
	/* Clear out the tree from the manager queue */
	__ae_spin_lock(session, &manager->manager_lock);
	for (current = TAILQ_FIRST(&manager->managerqh);
	    current != NULL; current = next) {
		next = TAILQ_NEXT(current, q);
		if (current->lsm_tree != lsm_tree)
			continue;
		++removed;
		TAILQ_REMOVE(&manager->managerqh, current, q);
		__ae_lsm_manager_free_work_unit(session, current);
	}
	__ae_spin_unlock(session, &manager->manager_lock);
	AE_STAT_FAST_CONN_INCRV(session, lsm_work_units_discarded, removed);
	return (0);
}

/*
 * We assume this is only called from __ae_lsm_manager_pop_entry and we
 * have session, entry and type available to use.  If the queue is empty
 * we may return from the macro.
 */
#define	LSM_POP_ENTRY(qh, qlock, qlen) do {				\
	if (TAILQ_EMPTY(qh))						\
		return (0);						\
	__ae_spin_lock(session, qlock);					\
	TAILQ_FOREACH(entry, (qh), q) {					\
		if (FLD_ISSET(type, entry->type)) {			\
			TAILQ_REMOVE(qh, entry, q);			\
			AE_STAT_FAST_CONN_DECR(session, qlen);		\
			break;						\
		}							\
	}								\
	__ae_spin_unlock(session, (qlock));				\
} while (0)

/*
 * __ae_lsm_manager_pop_entry --
 *	Retrieve the head of the queue, if it matches the requested work
 *	unit type.
 */
int
__ae_lsm_manager_pop_entry(
    AE_SESSION_IMPL *session, uint32_t type, AE_LSM_WORK_UNIT **entryp)
{
	AE_LSM_MANAGER *manager;
	AE_LSM_WORK_UNIT *entry;

	manager = &S2C(session)->lsm_manager;
	*entryp = NULL;
	entry = NULL;

	/*
	 * Pop the entry off the correct queue based on our work type.
	 */
	if (type == AE_LSM_WORK_SWITCH)
		LSM_POP_ENTRY(&manager->switchqh,
		    &manager->switch_lock, lsm_work_queue_switch);
	else if (type == AE_LSM_WORK_MERGE)
		LSM_POP_ENTRY(&manager->managerqh,
		    &manager->manager_lock, lsm_work_queue_manager);
	else
		LSM_POP_ENTRY(&manager->appqh,
		    &manager->app_lock, lsm_work_queue_app);
	if (entry != NULL)
		AE_STAT_FAST_CONN_INCR(session, lsm_work_units_done);
	*entryp = entry;
	return (0);
}

/*
 * Push a work unit onto the appropriate queue.  This macro assumes we are
 * called from __ae_lsm_manager_push_entry and we have session and entry
 * available for use.
 */
#define	LSM_PUSH_ENTRY(qh, qlock, qlen) do {				\
	__ae_spin_lock(session, qlock);					\
	TAILQ_INSERT_TAIL((qh), entry, q);				\
	AE_STAT_FAST_CONN_INCR(session, qlen);				\
	__ae_spin_unlock(session, qlock);				\
} while (0)

/*
 * __ae_lsm_manager_push_entry --
 *	Add an entry to the end of the switch queue.
 */
int
__ae_lsm_manager_push_entry(AE_SESSION_IMPL *session,
    uint32_t type, uint32_t flags, AE_LSM_TREE *lsm_tree)
{
	AE_DECL_RET;
	AE_LSM_MANAGER *manager;
	AE_LSM_WORK_UNIT *entry;
	bool pushed;

	manager = &S2C(session)->lsm_manager;

	/*
	 * Don't add merges or bloom filter creates if merges
	 * or bloom filters are disabled in the tree.
	 */
	switch (type) {
	case AE_LSM_WORK_BLOOM:
		if (FLD_ISSET(lsm_tree->bloom, AE_LSM_BLOOM_OFF))
			return (0);
		break;
	case AE_LSM_WORK_MERGE:
		if (!F_ISSET(lsm_tree, AE_LSM_TREE_MERGES))
			return (0);
		break;
	}

	/*
	 * Don't allow any work units unless a tree is active, this avoids
	 * races on shutdown between clearing out queues and pushing new
	 * work units.
	 *
	 * Increment the queue reference before checking the flag since
	 * on close, the flag is cleared and then the queue reference count
	 * is checked.
	 */
	(void)__ae_atomic_add32(&lsm_tree->queue_ref, 1);
	if (!F_ISSET(lsm_tree, AE_LSM_TREE_ACTIVE)) {
		(void)__ae_atomic_sub32(&lsm_tree->queue_ref, 1);
		return (0);
	}

	pushed = false;
	AE_ERR(__ae_epoch(session, &lsm_tree->work_push_ts));
	AE_ERR(__ae_calloc_one(session, &entry));
	entry->type = type;
	entry->flags = flags;
	entry->lsm_tree = lsm_tree;
	AE_STAT_FAST_CONN_INCR(session, lsm_work_units_created);

	if (type == AE_LSM_WORK_SWITCH)
		LSM_PUSH_ENTRY(&manager->switchqh,
		    &manager->switch_lock, lsm_work_queue_switch);
	else if (type == AE_LSM_WORK_MERGE)
		LSM_PUSH_ENTRY(&manager->managerqh,
		    &manager->manager_lock, lsm_work_queue_manager);
	else
		LSM_PUSH_ENTRY(&manager->appqh,
		    &manager->app_lock, lsm_work_queue_app);
	pushed = true;

	AE_ERR(__ae_cond_signal(session, manager->work_cond));
	return (0);
err:
	if (!pushed)
		(void)__ae_atomic_sub32(&lsm_tree->queue_ref, 1);
	return (ret);
}
