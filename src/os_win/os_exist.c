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
	AE_DECL_RET;
	char *path;

	AE_RET(__ae_filename(session, filename, &path));

	ret = GetFileAttributesA(path);

	__ae_free(session, path);

	if (ret != INVALID_FILE_ATTRIBUTES)
		*existp = true;
	else
		*existp = false;

	return (0);
}
