/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

static const int windows_error_offset = -29000;

/*
 * __ae_map_error_to_windows_error --
 *	Return a negative integer, an encoded Windows error
 * Standard C errors are positive integers from 0 - ~200
 * Windows errors are from 0 - 15999 according to the documentation
 */
static DWORD
__ae_map_error_to_windows_error(int error) {
	/* Ensure we do not exceed the error range
	   Also validate he do not get any COM errors
	   (which are negative integers)
	*/
	AE_ASSERT(NULL, error < 0);

	return (error + -(windows_error_offset));
}

/*
 * __ae_map_windows_error_to_error --
 *	Return a positive integer, a decoded Windows error
 */
static int
__ae_map_windows_error_to_error(DWORD winerr) {
	return (winerr + windows_error_offset);
}

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
	DWORD err = GetLastError();

	/* GetLastError should only be called if we hit an actual error */
	AE_ASSERT(NULL, err != ERROR_SUCCESS);

	return (err == ERROR_SUCCESS ?
	    AE_ERROR : __ae_map_windows_error_to_error(err));
}

/*
 * __ae_strerror --
 *	Windows implementation of AE_SESSION.strerror and archengine_strerror.
 */
const char *
__ae_strerror(AE_SESSION_IMPL *session, int error, char *errbuf, size_t errlen)
{
	DWORD lasterror;
	const char *p;
	char buf[512];

	/*
	 * Check for a ArchEngine or POSIX constant string, no buffer needed.
	 */
	if ((p = __ae_archengine_error(error)) != NULL)
		return (p);

	/*
	 * When called from archengine_strerror, write a passed-in buffer.
	 * When called from AE_SESSION.strerror, write the session's buffer.
	 *
	 * Check for Windows errors.
	 */
	if (error < 0) {
		error = __ae_map_error_to_windows_error(error);

		lasterror = FormatMessageA(
			FORMAT_MESSAGE_FROM_SYSTEM |
			    FORMAT_MESSAGE_IGNORE_INSERTS,
			NULL,
			error,
			0, /* let system choose the correct LANGID */
			buf,
			sizeof(buf),
			NULL);

		if (lasterror != 0 && session == NULL &&
		    snprintf(errbuf, errlen, "%s", buf) > 0)
			return (errbuf);
		if (lasterror != 0 && session != NULL &&
		    __ae_buf_fmt(session, &session->err, "%s", buf) == 0)
			return (session->err.data);
	}

	/* Fallback to a generic message. */
	if (session == NULL &&
	    snprintf(errbuf, errlen, "error return: %d", error) > 0)
		return (errbuf);
	if (session != NULL && __ae_buf_fmt(
	    session, &session->err, "error return: %d", error) == 0)
		return (session->err.data);

	/* Defeated. */
	return ("Unable to return error string");
}
