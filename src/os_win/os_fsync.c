/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_directory_sync_fh --
 *	Flush a directory file handle.
 */
int
__ae_directory_sync_fh(AE_SESSION_IMPL *session, AE_FH *fh)
{
	AE_UNUSED(session);
	AE_UNUSED(fh);
	return (0);
}

/*
 * __ae_directory_sync --
 *	Flush a directory to ensure a file creation is durable.
 */
int
__ae_directory_sync(AE_SESSION_IMPL *session, char *path)
{
	AE_UNUSED(session);
	AE_UNUSED(path);
	return (0);
}

/*
 * __ae_fsync --
 *	Flush a file handle.
 */
int
__ae_fsync(AE_SESSION_IMPL *session, AE_FH *fh)
{
	AE_DECL_RET;

	AE_RET(__ae_verbose(session, AE_VERB_FILEOPS, "%s: FlushFileBuffers",
	    fh->name));

	if ((ret = FlushFileBuffers(fh->filehandle)) == FALSE)
		AE_RET_MSG(session,
		    __ae_errno(), "%s FlushFileBuffers error", fh->name);

	return (0);
}

/*
 * __ae_fsync_async --
 *	Flush a file handle and don't wait for the result.
 */
int
__ae_fsync_async(AE_SESSION_IMPL *session, AE_FH *fh)
{
	AE_UNUSED(session);
	AE_UNUSED(fh);

	return (0);
}
