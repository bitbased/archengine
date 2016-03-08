/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_filesize --
 *	Get the size of a file in bytes.
 */
int
__ae_filesize(AE_SESSION_IMPL *session, AE_FH *fh, ae_off_t *sizep)
{
	struct stat sb;
	AE_DECL_RET;

	AE_RET(__ae_verbose(session, AE_VERB_FILEOPS, "%s: fstat", fh->name));

	AE_SYSCALL_RETRY(fstat(fh->fd, &sb), ret);
	if (ret == 0) {
		*sizep = sb.st_size;
		return (0);
	}

	AE_RET_MSG(session, ret, "%s: fstat", fh->name);
}

/*
 * __ae_filesize_name --
 *	Return the size of a file in bytes, given a file name.
 */
int
__ae_filesize_name(AE_SESSION_IMPL *session,
    const char *filename, bool silent, ae_off_t *sizep)
{
	struct stat sb;
	AE_DECL_RET;
	char *path;

	AE_RET(__ae_filename(session, filename, &path));

	AE_SYSCALL_RETRY(stat(path, &sb), ret);

	__ae_free(session, path);

	if (ret == 0) {
		*sizep = sb.st_size;
		return (0);
	}

	/*
	 * Some callers of this function expect failure if the file doesn't
	 * exist, and don't want an error message logged.
	 */
	if (!silent)
		AE_RET_MSG(session, ret, "%s: fstat", filename);
	return (ret);
}
