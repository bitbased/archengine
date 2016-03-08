/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_thread_create --
 *	Create a new thread of control.
 */
int
__ae_thread_create(AE_SESSION_IMPL *session,
    ae_thread_t *tidret, AE_THREAD_CALLBACK(*func)(void *), void *arg)
{
	/* Spawn a new thread of control. */
	*tidret = (HANDLE)_beginthreadex(NULL, 0, func, arg, 0, NULL);
	if (*tidret != 0)
		return (0);

	AE_RET_MSG(session, errno, "_beginthreadex");
}

/*
 * __ae_thread_join --
 *	Wait for a thread of control to exit.
 */
int
__ae_thread_join(AE_SESSION_IMPL *session, ae_thread_t tid)
{
	AE_DECL_RET;

	if ((ret = WaitForSingleObject(tid, INFINITE)) != WAIT_OBJECT_0)
		/*
		 * If we fail to wait, we will leak handles so do not continue
		 */
		AE_PANIC_RET(session, ret == WAIT_FAILED ? __ae_errno() : ret,
		    "Wait for thread join failed");

	if (CloseHandle(tid) == 0) {
		AE_RET_MSG(session, __ae_errno(),
		    "CloseHandle: thread join");
	}

	return (0);
}

/*
 * __ae_thread_id --
 *	Fill in a printable version of the process and thread IDs.
 */
void
__ae_thread_id(char* buf, size_t buflen)
{
	(void)snprintf(buf, buflen,
	    "%" PRIu64 ":%" PRIu64,
	    (uint64_t)GetCurrentProcessId(), (uint64_t)GetCurrentThreadId);
}
