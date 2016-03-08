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

	AE_SYSCALL_RETRY(ftruncate(fh->fd, len), ret);
	if (ret == 0)
		return (0);

	AE_RET_MSG(session, ret, "%s ftruncate error", fh->name);
}
