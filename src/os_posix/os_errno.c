/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_errno --
 *	Return errno, or AE_ERROR if errno not set.
 */
int
__ae_errno(void)
{
	/*
	 * Called when we know an error occurred, and we want the system
	 * error code, but there's some chance it's not set.
	 */
	return (errno == 0 ? AE_ERROR : errno);
}

/*
 * __ae_strerror --
 *	POSIX implementation of AE_SESSION.strerror and archengine_strerror.
 */
const char *
__ae_strerror(AE_SESSION_IMPL *session, int error, char *errbuf, size_t errlen)
{
	const char *p;

	/*
	 * Check for a ArchEngine or POSIX constant string, no buffer needed.
	 */
	if ((p = __ae_archengine_error(error)) != NULL)
		return (p);

	/*
	 * When called from archengine_strerror, write a passed-in buffer.
	 * When called from AE_SESSION.strerror, write the session's buffer.
	 *
	 * Fallback to a generic message.
	 */
	if (session == NULL &&
	    snprintf(errbuf, errlen, "error return: %d", error) > 0)
		return (errbuf);
	if (session != NULL && __ae_buf_fmt(
	    session, &session->err, "error return: %d", error) == 0)
		return (session->err.data);

	/* Defeated. */
	return ("Unable to return error string");
}
