/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_ftruncate --
 *	Truncate a file.
 */
int
__ae_ftruncate(AE_SESSION_IMPL *session, AE_FH *fh, ae_off_t len)
{
	AE_DECL_RET;
	LARGE_INTEGER largeint;

	largeint.QuadPart = len;

	if ((ret = SetFilePointerEx(
	    fh->filehandle_secondary, largeint, NULL, FILE_BEGIN)) == FALSE)
		AE_RET_MSG(session, __ae_errno(), "%s SetFilePointerEx error",
		    fh->name);

	ret = SetEndOfFile(fh->filehandle_secondary);
	if (ret != FALSE)
		return (0);

	if (GetLastError() == ERROR_USER_MAPPED_FILE)
		return (EBUSY);

	AE_RET_MSG(session, __ae_errno(), "%s SetEndOfFile error", fh->name);
}
