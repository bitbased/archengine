/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_epoch --
 *	Return the time since the Epoch.
 */
int
__ae_epoch(AE_SESSION_IMPL *session, struct timespec *tsp)
{
	uint64_t ns100;
	FILETIME time;

	AE_UNUSED(session);

	GetSystemTimeAsFileTime(&time);

	ns100 = (((int64_t)time.dwHighDateTime << 32) + time.dwLowDateTime)
	    - 116444736000000000LL;
	tsp->tv_sec = ns100 / 10000000;
	tsp->tv_nsec = (long)((ns100 % 10000000) * 100);

	return (0);
}

/*
 * localtime_r --
 *	Return the current local time.
 */
struct tm *
localtime_r(const time_t *timer, struct tm *result)
{
	errno_t err;

	err = localtime_s(result, timer);
	if (err != 0) {
		__ae_err(NULL, err, "localtime_s");
		return (NULL);
	}

	return (result);
}
