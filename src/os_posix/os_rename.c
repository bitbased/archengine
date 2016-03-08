/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_rename --
 *	Rename a file.
 */
int
__ae_rename(AE_SESSION_IMPL *session, const char *from, const char *to)
{
	AE_DECL_RET;
	char *from_path, *to_path;

	AE_RET(__ae_verbose(
	    session, AE_VERB_FILEOPS, "rename %s to %s", from, to));

	from_path = to_path = NULL;

	AE_RET(__ae_filename(session, from, &from_path));
	AE_TRET(__ae_filename(session, to, &to_path));

	if (ret == 0)
		AE_SYSCALL_RETRY(rename(from_path, to_path), ret);

	__ae_free(session, from_path);
	__ae_free(session, to_path);

	if (ret == 0)
		return (0);

	AE_RET_MSG(session, ret, "rename %s to %s", from, to);
}
