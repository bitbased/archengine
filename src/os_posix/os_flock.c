/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_bytelock --
 *	Lock/unlock a byte in a file.
 */
int
__ae_bytelock(AE_FH *fhp, ae_off_t byte, bool lock)
{
	struct flock fl;
	AE_DECL_RET;

	/*
	 * ArchEngine requires this function be able to acquire locks past
	 * the end of file.
	 *
	 * Note we're using fcntl(2) locking: all fcntl locks associated with a
	 * file for a given process are removed when any file descriptor for the
	 * file is closed by the process, even if a lock was never requested for
	 * that file descriptor.
	 */
	fl.l_start = byte;
	fl.l_len = 1;
	fl.l_type = lock ? F_WRLCK : F_UNLCK;
	fl.l_whence = SEEK_SET;

	AE_SYSCALL_RETRY(fcntl(fhp->fd, F_SETLK, &fl), ret);

	return (ret);
}
