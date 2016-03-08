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
	AE_DECL_RET;

	/* Spawn a new thread of control. */
	AE_SYSCALL_RETRY(pthread_create(tidret, NULL, func, arg), ret);
	if (ret == 0)
		return (0);
	AE_RET_MSG(session, ret, "pthread_create");
}

/*
 * __ae_thread_join --
 *	Wait for a thread of control to exit.
 */
int
__ae_thread_join(AE_SESSION_IMPL *session, ae_thread_t tid)
{
	AE_DECL_RET;

	AE_SYSCALL_RETRY(pthread_join(tid, NULL), ret);
	if (ret == 0)
		return (0);

	AE_RET_MSG(session, ret, "pthread_join");
}

/*
 * __ae_thread_id --
 *	Fill in a printable version of the process and thread IDs.
 */
void
__ae_thread_id(char *buf, size_t buflen)
{
	pthread_t self;

	/*
	 * POSIX 1003.1 allows pthread_t to be an opaque type; on systems where
	 * it's a pointer, print the pointer to match gdb output.
	 */
	self = pthread_self();
#ifdef __sun
	(void)snprintf(buf, buflen,
	    "%" PRIuMAX ":%u", (uintmax_t)getpid(), self);
#else
	(void)snprintf(buf, buflen,
	    "%" PRIuMAX ":%p", (uintmax_t)getpid(), (void *)self);
#endif
}
