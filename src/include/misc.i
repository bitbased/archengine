/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * __ae_cond_wait --
 *	Wait on a mutex, optionally timing out.
 */
static inline int
__ae_cond_wait(AE_SESSION_IMPL *session, AE_CONDVAR *cond, uint64_t usecs)
{
	bool notused;

	return (__ae_cond_wait_signal(session, cond, usecs, &notused));
}

/*
 * __ae_strdup --
 *	ANSI strdup function.
 */
static inline int
__ae_strdup(AE_SESSION_IMPL *session, const char *str, void *retp)
{
	return (__ae_strndup(
	    session, str, (str == NULL) ? 0 : strlen(str), retp));
}

/*
 * __ae_seconds --
 *	Return the seconds since the Epoch.
 */
static inline int
__ae_seconds(AE_SESSION_IMPL *session, time_t *timep)
{
	struct timespec t;

	AE_RET(__ae_epoch(session, &t));

	*timep = t.tv_sec;

	return (0);
}

/*
 * __ae_verbose --
 * 	Verbose message.
 */
static inline int
__ae_verbose(AE_SESSION_IMPL *session, int flag, const char *fmt, ...)
    AE_GCC_FUNC_ATTRIBUTE((format (printf, 2, 3)))
{
#ifdef HAVE_VERBOSE
	AE_DECL_RET;
	va_list ap;

	if (AE_VERBOSE_ISSET(session, flag)) {
		va_start(ap, fmt);
		ret = __ae_eventv(session, true, 0, NULL, 0, fmt, ap);
		va_end(ap);
	}
	return (ret);
#else
	AE_UNUSED(session);
	AE_UNUSED(fmt);
	AE_UNUSED(flag);
	return (0);
#endif
}
