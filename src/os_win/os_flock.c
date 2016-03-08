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
	AE_DECL_RET;

	/*
	 * ArchEngine requires this function be able to acquire locks past
	 * the end of file.
	 *
	 * Note we're using fcntl(2) locking: all fcntl locks associated with a
	 * file for a given process are removed when any file descriptor for the
	 * file is closed by the process, even if a lock was never requested for
	 * that file descriptor.
	 *
	 * http://msdn.microsoft.com/
	 *    en-us/library/windows/desktop/aa365202%28v=vs.85%29.aspx
	 *
	 * You can lock bytes that are beyond the end of the current file.
	 * This is useful to coordinate adding records to the end of a file.
	 */
	if (lock) {
		ret = LockFile(fhp->filehandle, UINT32_MAX & byte,
		    UINT32_MAX & (byte >> 32), 1, 0);
	} else {
		ret = UnlockFile(fhp->filehandle, UINT32_MAX & byte,
		    UINT32_MAX & (byte >> 32), 1, 0);
	}

	if (ret == FALSE)
		AE_RET_MSG(NULL, __ae_errno(), "%s: LockFile", fhp->name);

	return (0);
}
