/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_cond_alloc --
 *	Allocate and initialize a condition variable.
 */
int
__ae_cond_alloc(AE_SESSION_IMPL *session,
    const char *name, bool is_signalled, AE_CONDVAR **condp)
{
	AE_CONDVAR *cond;
	AE_DECL_RET;

	/*
	 * !!!
	 * This function MUST handle a NULL session handle.
	 */
	AE_RET(__ae_calloc_one(session, &cond));

	AE_ERR(pthread_mutex_init(&cond->mtx, NULL));

	/* Initialize the condition variable to permit self-blocking. */
	AE_ERR(pthread_cond_init(&cond->cond, NULL));

	cond->name = name;
	cond->waiters = is_signalled ? -1 : 0;

	*condp = cond;
	return (0);

err:	__ae_free(session, cond);
	return (ret);
}

/*
 * __ae_cond_wait_signal --
 *	Wait on a mutex, optionally timing out.  If we get it
 *	before the time out period expires, let the caller know.
 */
int
__ae_cond_wait_signal(
    AE_SESSION_IMPL *session, AE_CONDVAR *cond, uint64_t usecs, bool *signalled)
{
	struct timespec ts;
	AE_DECL_RET;
	bool locked;

	locked = false;

	/* Fast path if already signalled. */
	*signalled = true;
	if (__ae_atomic_addi32(&cond->waiters, 1) == 0)
		return (0);

	/*
	 * !!!
	 * This function MUST handle a NULL session handle.
	 */
	if (session != NULL) {
		AE_RET(__ae_verbose(session, AE_VERB_MUTEX,
		    "wait %s cond (%p)", cond->name, cond));
		AE_STAT_FAST_CONN_INCR(session, cond_wait);
	}

	AE_ERR(pthread_mutex_lock(&cond->mtx));
	locked = true;

	if (usecs > 0) {
		AE_ERR(__ae_epoch(session, &ts));
		ts.tv_sec += (time_t)
		    (((uint64_t)ts.tv_nsec + AE_THOUSAND * usecs) / AE_BILLION);
		ts.tv_nsec = (long)
		    (((uint64_t)ts.tv_nsec + AE_THOUSAND * usecs) % AE_BILLION);
		ret = pthread_cond_timedwait(&cond->cond, &cond->mtx, &ts);
	} else
		ret = pthread_cond_wait(&cond->cond, &cond->mtx);

	/*
	 * Check pthread_cond_wait() return for EINTR, ETIME and
	 * ETIMEDOUT, some systems return these errors.
	 */
	if (ret == EINTR ||
#ifdef ETIME
	    ret == ETIME ||
#endif
	    ret == ETIMEDOUT) {
		*signalled = false;
		ret = 0;
	}

	(void)__ae_atomic_subi32(&cond->waiters, 1);

err:	if (locked)
		AE_TRET(pthread_mutex_unlock(&cond->mtx));
	if (ret == 0)
		return (0);
	AE_RET_MSG(session, ret, "pthread_cond_wait");
}

/*
 * __ae_cond_signal --
 *	Signal a waiting thread.
 */
int
__ae_cond_signal(AE_SESSION_IMPL *session, AE_CONDVAR *cond)
{
	AE_DECL_RET;
	bool locked;

	locked = false;

	/*
	 * !!!
	 * This function MUST handle a NULL session handle.
	 */
	if (session != NULL)
		AE_RET(__ae_verbose(session, AE_VERB_MUTEX,
		    "signal %s cond (%p)", cond->name, cond));

	/* Fast path if already signalled. */
	if (cond->waiters == -1)
		return (0);

	if (cond->waiters > 0 || !__ae_atomic_casi32(&cond->waiters, 0, -1)) {
		AE_ERR(pthread_mutex_lock(&cond->mtx));
		locked = true;
		AE_ERR(pthread_cond_broadcast(&cond->cond));
	}

err:	if (locked)
		AE_TRET(pthread_mutex_unlock(&cond->mtx));
	if (ret == 0)
		return (0);
	AE_RET_MSG(session, ret, "pthread_cond_broadcast");
}

/*
 * __ae_cond_destroy --
 *	Destroy a condition variable.
 */
int
__ae_cond_destroy(AE_SESSION_IMPL *session, AE_CONDVAR **condp)
{
	AE_CONDVAR *cond;
	AE_DECL_RET;

	cond = *condp;
	if (cond == NULL)
		return (0);

	ret = pthread_cond_destroy(&cond->cond);
	AE_TRET(pthread_mutex_destroy(&cond->mtx));
	__ae_free(session, *condp);

	return (ret);
}
