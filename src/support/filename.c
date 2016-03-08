/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_filename --
 *	Build a file name in a scratch buffer, automatically calculate the
 *	length of the file name.
 */
int
__ae_filename(AE_SESSION_IMPL *session, const char *name, char **path)
{
	return (__ae_nfilename(session, name, strlen(name), path));
}

/*
 * __ae_nfilename --
 *	Build a file name in a scratch buffer.  If the name is already an
 *	absolute path duplicate it, otherwise generate a path relative to the
 *	connection home directory.
  *     Needs to work with a NULL session handle - since this is called via
  *     the exists API which is used by the test utilities.
 */
int
__ae_nfilename(
    AE_SESSION_IMPL *session, const char *name, size_t namelen, char **path)
{
	size_t len;
	char *buf;

	*path = NULL;

	if (session == NULL || __ae_absolute_path(name))
		AE_RET(__ae_strndup(session, name, namelen, path));
	else {
		len = strlen(S2C(session)->home) + 1 + namelen + 1;
		AE_RET(__ae_calloc(session, 1, len, &buf));
		snprintf(buf, len, "%s%s%.*s", S2C(session)->home,
		    __ae_path_separator(), (int)namelen, name);
		*path = buf;
	}

	return (0);
}

/*
 * __ae_remove_if_exists --
 *	Remove a file if it exists.
 */
int
__ae_remove_if_exists(AE_SESSION_IMPL *session, const char *name)
{
	bool exist;

	AE_RET(__ae_exist(session, name, &exist));
	if (exist)
		AE_RET(__ae_remove(session, name));
	return (0);
}

/*
 * __ae_sync_and_rename_fh --
 *	Sync and close a file, and swap it into place.
 */
int
__ae_sync_and_rename_fh(
    AE_SESSION_IMPL *session, AE_FH **fhp, const char *from, const char *to)
{
	AE_DECL_RET;
	AE_FH *fh;

	fh = *fhp;
	*fhp = NULL;

	/* Flush to disk and close the handle. */
	ret = __ae_fsync(session, fh);
	AE_TRET(__ae_close(session, &fh));
	AE_RET(ret);

	/* Rename the source file to the target. */
	AE_RET(__ae_rename(session, from, to));

	/* Flush the backing directory to guarantee the rename. */
	return (__ae_directory_sync(session, NULL));
}

/*
 * __ae_sync_and_rename_fp --
 *	Sync and close a file, and swap it into place.
 */
int
__ae_sync_and_rename_fp(
    AE_SESSION_IMPL *session, FILE **fpp, const char *from, const char *to)
{
	FILE *fp;

	fp = *fpp;
	*fpp = NULL;

	/* Flush to disk and close the handle. */
	AE_RET(__ae_fclose(&fp, AE_FHANDLE_WRITE));

	/* Rename the source file to the target. */
	AE_RET(__ae_rename(session, from, to));

	/* Flush the backing directory to guarantee the rename. */
	return (__ae_directory_sync(session, NULL));
}
