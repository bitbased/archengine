/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_handle_sync --
 *	Flush a file handle.
 */
static int
__ae_handle_sync(int fd)
{
	AE_DECL_RET;

#if defined(F_FULLFSYNC)
	/*
	 * OS X fsync documentation:
	 * "Note that while fsync() will flush all data from the host to the
	 * drive (i.e. the "permanent storage device"), the drive itself may
	 * not physically write the data to the platters for quite some time
	 * and it may be written in an out-of-order sequence. For applications
	 * that require tighter guarantees about the integrity of their data,
	 * Mac OS X provides the F_FULLFSYNC fcntl. The F_FULLFSYNC fcntl asks
	 * the drive to flush all buffered data to permanent storage."
	 *
	 * OS X F_FULLFSYNC fcntl documentation:
	 * "This is currently implemented on HFS, MS-DOS (FAT), and Universal
	 * Disk Format (UDF) file systems."
	 */
	AE_SYSCALL_RETRY(fcntl(fd, F_FULLFSYNC, 0), ret);
	if (ret == 0)
		return (0);
	/*
	 * Assume F_FULLFSYNC failed because the file system doesn't support it
	 * and fallback to fsync.
	 */
#endif
#if defined(HAVE_FDATASYNC)
	AE_SYSCALL_RETRY(fdatasync(fd), ret);
#else
	AE_SYSCALL_RETRY(fsync(fd), ret);
#endif
	return (ret);
}

/*
 * __ae_directory_sync_fh --
 *	Flush a directory file handle.  We don't use __ae_fsync because
 *	most file systems don't require this step and we don't want to
 *	penalize them by calling fsync.
 */
int
__ae_directory_sync_fh(AE_SESSION_IMPL *session, AE_FH *fh)
{
#ifdef __linux__
	AE_DECL_RET;

	if ((ret = __ae_handle_sync(fh->fd)) == 0)
		return (0);
	AE_RET_MSG(session, ret, "%s: fsync", fh->name);
#else
	AE_UNUSED(session);
	AE_UNUSED(fh);
	return (0);
#endif
}

/*
 * __ae_directory_sync --
 *	Flush a directory to ensure a file creation is durable.
 */
int
__ae_directory_sync(AE_SESSION_IMPL *session, char *path)
{
#ifdef __linux__
	AE_DECL_RET;
	int fd, tret;
	char *dir;

	/*
	 * POSIX 1003.1 does not require that fsync of a file handle ensures the
	 * entry in the directory containing the file has also reached disk (and
	 * there are historic Linux filesystems requiring this), do an explicit
	 * fsync on a file descriptor for the directory to be sure.
	 */
	if (path == NULL || (dir = strrchr(path, '/')) == NULL) {
		dir = NULL;
		path = (char *)S2C(session)->home;
	} else
		*dir = '\0';
	AE_SYSCALL_RETRY(((fd =
	    open(path, O_RDONLY, 0444)) == -1 ? 1 : 0), ret);
	if (dir != NULL)
		*dir = '/';
	if (ret != 0)
		AE_RET_MSG(session, ret, "%s: open", path);

	if ((ret = __ae_handle_sync(fd)) != 0)
		AE_ERR_MSG(session, ret, "%s: fsync", path);

err:	AE_SYSCALL_RETRY(close(fd), tret);
	if (tret != 0)
		__ae_err(session, tret, "%s: close", path);
	AE_TRET(tret);
	return (ret);
#else
	AE_UNUSED(session);
	AE_UNUSED(path);
	return (0);
#endif
}

/*
 * __ae_fsync --
 *	Flush a file handle.
 */
int
__ae_fsync(AE_SESSION_IMPL *session, AE_FH *fh)
{
	AE_DECL_RET;

	AE_RET(__ae_verbose(session, AE_VERB_FILEOPS, "%s: fsync", fh->name));

	if ((ret = __ae_handle_sync(fh->fd)) == 0)
		return (0);
	AE_RET_MSG(session, ret, "%s fsync error", fh->name);
}

/*
 * __ae_fsync_async --
 *	Flush a file handle and don't wait for the result.
 */
int
__ae_fsync_async(AE_SESSION_IMPL *session, AE_FH *fh)
{
#ifdef	HAVE_SYNC_FILE_RANGE
	AE_DECL_RET;

	AE_RET(__ae_verbose(
	    session, AE_VERB_FILEOPS, "%s: sync_file_range", fh->name));

	AE_SYSCALL_RETRY(sync_file_range(fh->fd,
	    (off64_t)0, (off64_t)0, SYNC_FILE_RANGE_WRITE), ret);
	if (ret == 0)
		return (0);
	AE_RET_MSG(session, ret, "%s: sync_file_range", fh->name);
#else
	AE_UNUSED(session);
	AE_UNUSED(fh);
	return (0);
#endif
}
