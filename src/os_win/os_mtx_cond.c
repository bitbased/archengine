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

	/*
	 * !!!
	 * This function MUST handle a NULL session handle.
	 */
	AE_RET(__ae_calloc_one(session, &cond));

	InitializeCriticalSection(&cond->mtx);

	/* Initialize the condition variable to permit self-blocking. */
	InitializeConditionVariable(&cond->cond);

	cond->name = name;
	cond->waiters = is_signalled ? -1 : 0;

	*condp = cond;
	return (0);
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
	DWORD err, milliseconds;
	AE_DECL_RET;
	uint64_t milliseconds64;
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

	EnterCriticalSection(&cond->mtx);
	locked = true;

	if (usecs > 0) {
		milliseconds64 = usecs / 1000;

		/*
		 * Check for 32-bit unsigned integer overflow
		 * INFINITE is max unsigned int on Windows
		 */
		if (milliseconds64 >= INFINITE)
			milliseconds64 = INFINITE - 1;
		milliseconds = (DWORD)milliseconds64;

		/*
		 * 0 would mean the CV sleep becomes a TryCV which we do not
		 * want
		 */
		if (milliseconds == 0)
			milliseconds = 1;

		ret = SleepConditionVariableCS(
		    &cond->cond, &cond->mtx, milliseconds);
	} else
		ret = SleepConditionVariableCS(
		    &cond->cond, &cond->mtx, INFINITE);

	/*
	 * SleepConditionVariableCS returns non-zero on success, 0 on timeout
	 * or failure. Check for timeout, else convert to a ArchEngine error
	 * value and fail.
	 */
	if (ret == 0) {
		if ((err = GetLastError()) == ERROR_TIMEOUT)
			*signalled = false;
		else
			ret = __ae_errno();
	} else
		ret = 0;

	(void)__ae_atomic_subi32(&cond->waiters, 1);

	if (locked)
		LeaveCriticalSection(&cond->mtx);

	if (ret == 0)
		return (0);
	AE_RET_MSG(session, ret, "SleepConditionVariableCS");
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
		EnterCriticalSection(&cond->mtx);
		locked = true;
		WakeAllConditionVariable(&cond->cond);
	}

	if (locked)
		LeaveCriticalSection(&cond->mtx);
	if (ret == 0)
		return (0);
	AE_RET_MSG(session, ret, "WakeAllConditionVariable");
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

	/* Do nothing to delete Condition Variable */
	DeleteCriticalSection(&cond->mtx);
	__ae_free(session, *condp);

	return (ret);
}
