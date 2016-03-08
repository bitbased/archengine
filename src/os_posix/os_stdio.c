/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_fopen --
 *	Open a FILE handle.
 */
int
__ae_fopen(AE_SESSION_IMPL *session,
    const char *name, AE_FHANDLE_MODE mode_flag, u_int flags, FILE **fpp)
{
	AE_DECL_RET;
	const char *mode, *path;
	char *pathbuf;

	AE_RET(__ae_verbose(session, AE_VERB_FILEOPS, "%s: fopen", name));

	pathbuf = NULL;
	if (LF_ISSET(AE_FOPEN_FIXED))
		path = name;
	else {
		AE_RET(__ae_filename(session, name, &pathbuf));
		path = pathbuf;
	}

	mode = NULL;
	switch (mode_flag) {
	case AE_FHANDLE_APPEND:
		mode = AE_FOPEN_APPEND;
		break;
	case AE_FHANDLE_READ:
		mode = AE_FOPEN_READ;
		break;
	case AE_FHANDLE_WRITE:
		mode = AE_FOPEN_WRITE;
		break;
	}
	*fpp = fopen(path, mode);
	if (*fpp == NULL)
		ret = __ae_errno();

	if (pathbuf != NULL)
		__ae_free(session, pathbuf);

	if (ret == 0)
		return (0);
	AE_RET_MSG(session, ret, "%s: fopen", name);
}

/*
 * __ae_vfprintf --
 *	Vfprintf for a FILE handle.
 */
int
__ae_vfprintf(FILE *fp, const char *fmt, va_list ap)
{
	return (vfprintf(fp, fmt, ap) < 0 ? __ae_errno() : 0);
}

/*
 * __ae_fprintf --
 *	Fprintf for a FILE handle.
 */
int
__ae_fprintf(FILE *fp, const char *fmt, ...)
    AE_GCC_FUNC_ATTRIBUTE((format (printf, 2, 3)))
{
	AE_DECL_RET;
	va_list ap;

	va_start(ap, fmt);
	ret = __ae_vfprintf(fp, fmt, ap);
	va_end(ap);

	return (ret);
}

/*
 * __ae_fflush --
 *	Flush a FILE handle.
 */
int
__ae_fflush(FILE *fp)
{
	/* Flush the handle. */
	return (fflush(fp) == 0 ? 0 : __ae_errno());
}

/*
 * __ae_fclose --
 *	Close a FILE handle.
 */
int
__ae_fclose(FILE **fpp, AE_FHANDLE_MODE mode_flag)
{
	FILE *fp;
	AE_DECL_RET;

	if (*fpp == NULL)
		return (0);

	fp = *fpp;
	*fpp = NULL;

	/*
	 * If the handle was opened for writing, flush the file to the backing
	 * OS buffers, then flush the OS buffers to the backing disk.
	 */
	if (mode_flag == AE_FHANDLE_APPEND || mode_flag == AE_FHANDLE_WRITE) {
		ret = __ae_fflush(fp);
		if (fsync(fileno(fp)) != 0)
			AE_TRET(__ae_errno());
	}

	/* Close the handle. */
	if (fclose(fp) != 0)
		AE_TRET(__ae_errno());

	return (ret);
}
