/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_exist --
 *	Return if the file exists.
 */
int
__ae_exist(AE_SESSION_IMPL *session, const char *filename, bool *existp)
{
	struct stat sb;
	AE_DECL_RET;
	char *path;

	*existp = false;

	AE_RET(__ae_filename(session, filename, &path));

	AE_SYSCALL_RETRY(stat(path, &sb), ret);

	__ae_free(session, path);

	if (ret == 0) {
		*existp = true;
		return (0);
	}
	if (ret == ENOENT)
		return (0);

	AE_RET_MSG(session, ret, "%s: fstat", filename);
}
