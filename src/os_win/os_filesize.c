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
	LARGE_INTEGER size;
	AE_DECL_RET;

	AE_RET(__ae_verbose(
	    session, AE_VERB_FILEOPS, "%s: GetFileSizeEx", fh->name));

	if ((ret = GetFileSizeEx(fh->filehandle, &size)) != 0) {
		*sizep = size.QuadPart;
		return (0);
	}

	AE_RET_MSG(session, __ae_errno(), "%s: GetFileSizeEx", fh->name);
}

/*
 * __ae_filesize_name --
 *	Return the size of a file in bytes, given a file name.
 */
int
__ae_filesize_name(AE_SESSION_IMPL *session,
    const char *filename, bool silent, ae_off_t *sizep)
{
	WIN32_FILE_ATTRIBUTE_DATA data;
	AE_DECL_RET;
	char *path;

	AE_RET(__ae_filename(session, filename, &path));

	ret = GetFileAttributesExA(path, GetFileExInfoStandard, &data);

	__ae_free(session, path);

	if (ret != 0) {
		*sizep =
		    ((int64_t)data.nFileSizeHigh << 32) | data.nFileSizeLow;
		return (0);
	}

	/*
	 * Some callers of this function expect failure if the file doesn't
	 * exist, and don't want an error message logged.
	 */
	ret = __ae_errno();
	if (!silent)
		AE_RET_MSG(session, ret, "%s: GetFileAttributesEx", filename);
	return (ret);
}
