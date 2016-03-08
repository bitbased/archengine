/* DO NOT EDIT: automatically built by dist/api_err.py. */

#include "ae_internal.h"

/*
 * Historically, there was only the archengine_strerror call because the POSIX
 * port didn't need anything more complex; Windows requires memory allocation
 * of error strings, so we added the AE_SESSION.strerror method. Because we
 * want archengine_strerror to continue to be as thread-safe as possible, errors
 * are split into two categories: ArchEngine's or the system's constant strings
 * and Everything Else, and we check constant strings before Everything Else.
 */

/*
 * __ae_archengine_error --
 *	Return a constant string for POSIX-standard and ArchEngine errors.
 */
const char *
__ae_archengine_error(int error)
{
	const char *p;

	/*
	 * Check for ArchEngine specific errors.
	 */
	switch (error) {
	case AE_ROLLBACK:
		return ("AE_ROLLBACK: conflict between concurrent operations");
	case AE_DUPLICATE_KEY:
		return ("AE_DUPLICATE_KEY: attempt to insert an existing key");
	case AE_ERROR:
		return ("AE_ERROR: non-specific ArchEngine error");
	case AE_NOTFOUND:
		return ("AE_NOTFOUND: item not found");
	case AE_PANIC:
		return ("AE_PANIC: ArchEngine library panic");
	case AE_RESTART:
		return ("AE_RESTART: restart the operation (internal)");
	case AE_RUN_RECOVERY:
		return ("AE_RUN_RECOVERY: recovery must be run to continue");
	case AE_CACHE_FULL:
		return ("AE_CACHE_FULL: operation would overflow cache");
	}

	/*
	 * POSIX errors are non-negative integers; check for 0 explicitly incase
	 * the underlying strerror doesn't handle 0, some historically didn't.
	 */
	if (error == 0)
		return ("Successful return: 0");
	if (error > 0 && (p = strerror(error)) != NULL)
		return (p);

	return (NULL);
}

/*
 * archengine_strerror --
 *	Return a string for any error value, non-thread-safe version.
 */
const char *
archengine_strerror(int error)
{
	static char buf[128];

	return (__ae_strerror(NULL, error, buf, sizeof(buf)));
}
