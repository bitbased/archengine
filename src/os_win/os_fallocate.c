/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_fallocate_config --
 *	Configure fallocate behavior for a file handle.
 */
void
__ae_fallocate_config(AE_SESSION_IMPL *session, AE_FH *fh)
{
	AE_UNUSED(session);

	/*
	 * fallocate on Windows would be implemented using SetEndOfFile, which
	 * can also truncate the file. ArchEngine expects fallocate to ignore
	 * requests to truncate the file which Windows does not do, so we don't
	 * support the call.
	 */
	fh->fallocate_available = AE_FALLOCATE_NOT_AVAILABLE;
	fh->fallocate_requires_locking = false;
}

/*
 * __ae_fallocate --
 *	Allocate space for a file handle.
 */
int
__ae_fallocate(
    AE_SESSION_IMPL *session, AE_FH *fh, ae_off_t offset, ae_off_t len)
{
	AE_UNUSED(session);
	AE_UNUSED(fh);
	AE_UNUSED(offset);
	AE_UNUSED(len);

	return (ENOTSUP);
}
