/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __open_directory --
 *	Open up a file handle to a directory.
 */
static int
__open_directory(AE_SESSION_IMPL *session, char *path, int *fd)
{
	AE_DECL_RET;

	AE_SYSCALL_RETRY(((*fd =
	    open(path, O_RDONLY, 0444)) == -1 ? 1 : 0), ret);
	if (ret != 0)
		AE_RET_MSG(session, ret, "%s: open_directory", path);
	return (ret);
}

/*
 * __ae_open --
 *	Open a file handle.
 */
int
__ae_open(AE_SESSION_IMPL *session,
    const char *name, bool ok_create, bool exclusive, int dio_type, AE_FH **fhp)
{
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	AE_FH *fh, *tfh;
	mode_t mode;
	uint64_t bucket, hash;
	int f, fd;
	bool direct_io, matched;
	char *path;

	conn = S2C(session);
	direct_io = false;
	fh = NULL;
	fd = -1;
	path = NULL;

	AE_RET(__ae_verbose(session, AE_VERB_FILEOPS, "%s: open", name));

	/* Increment the reference count if we already have the file open. */
	matched = false;
	hash = __ae_hash_city64(name, strlen(name));
	bucket = hash % AE_HASH_ARRAY_SIZE;
	__ae_spin_lock(session, &conn->fh_lock);
	TAILQ_FOREACH(tfh, &conn->fhhash[bucket], hashq) {
		if (strcmp(name, tfh->name) == 0) {
			++tfh->ref;
			*fhp = tfh;
			matched = true;
			break;
		}
	}
	__ae_spin_unlock(session, &conn->fh_lock);
	if (matched)
		return (0);

	AE_RET(__ae_filename(session, name, &path));

	if (dio_type == AE_FILE_TYPE_DIRECTORY) {
		AE_ERR(__open_directory(session, path, &fd));
		goto setupfh;
	}

	f = O_RDWR;
#ifdef O_BINARY
	/* Windows clones: we always want to treat the file as a binary. */
	f |= O_BINARY;
#endif
#ifdef O_CLOEXEC
	/*
	 * Security:
	 * The application may spawn a new process, and we don't want another
	 * process to have access to our file handles.
	 */
	f |= O_CLOEXEC;
#endif
#ifdef O_NOATIME
	/* Avoid updating metadata for read-only workloads. */
	if (dio_type == AE_FILE_TYPE_DATA ||
	    dio_type == AE_FILE_TYPE_CHECKPOINT)
		f |= O_NOATIME;
#endif

	if (ok_create) {
		f |= O_CREAT;
		if (exclusive)
			f |= O_EXCL;
		mode = 0666;
	} else
		mode = 0;

#ifdef O_DIRECT
	if (dio_type && FLD_ISSET(conn->direct_io, dio_type)) {
		f |= O_DIRECT;
		direct_io = true;
	}
#endif
	if (dio_type == AE_FILE_TYPE_LOG &&
	    FLD_ISSET(conn->txn_logsync, AE_LOG_DSYNC))
#ifdef O_DSYNC
		f |= O_DSYNC;
#elif defined(O_SYNC)
		f |= O_SYNC;
#else
		AE_ERR_MSG(session, ENOTSUP,
		    "Unsupported log sync mode requested");
#endif
	AE_SYSCALL_RETRY(((fd = open(path, f, mode)) == -1 ? 1 : 0), ret);
	if (ret != 0)
		AE_ERR_MSG(session, ret,
		    direct_io ?
		    "%s: open failed with direct I/O configured, some "
		    "filesystem types do not support direct I/O" : "%s", path);

setupfh:
#if defined(HAVE_FCNTL) && defined(FD_CLOEXEC) && !defined(O_CLOEXEC)
	/*
	 * Security:
	 * The application may spawn a new process, and we don't want another
	 * process to have access to our file handles.  There's an obvious
	 * race here, so we prefer the flag to open if available.
	 */
	if ((f = fcntl(fd, F_GETFD)) == -1 ||
	    fcntl(fd, F_SETFD, f | FD_CLOEXEC) == -1)
		AE_ERR_MSG(session, __ae_errno(), "%s: fcntl", name);
#endif

#if defined(HAVE_POSIX_FADVISE)
	/* Disable read-ahead on trees: it slows down random read workloads. */
	if (dio_type == AE_FILE_TYPE_DATA ||
	    dio_type == AE_FILE_TYPE_CHECKPOINT)
		AE_ERR(posix_fadvise(fd, 0, 0, POSIX_FADV_RANDOM));
#endif

	AE_ERR(__ae_calloc_one(session, &fh));
	AE_ERR(__ae_strdup(session, name, &fh->name));
	fh->name_hash = hash;
	fh->fd = fd;
	fh->ref = 1;
	fh->direct_io = direct_io;

	/* Set the file's size. */
	AE_ERR(__ae_filesize(session, fh, &fh->size));

	/* Configure file extension. */
	if (dio_type == AE_FILE_TYPE_DATA ||
	    dio_type == AE_FILE_TYPE_CHECKPOINT)
		fh->extend_len = conn->data_extend_len;

	/* Configure fallocate/posix_fallocate calls. */
	__ae_fallocate_config(session, fh);

	/*
	 * Repeat the check for a match, but then link onto the database's list
	 * of files.
	 */
	matched = false;
	__ae_spin_lock(session, &conn->fh_lock);
	TAILQ_FOREACH(tfh, &conn->fhhash[bucket], hashq) {
		if (strcmp(name, tfh->name) == 0) {
			++tfh->ref;
			*fhp = tfh;
			matched = true;
			break;
		}
	}
	if (!matched) {
		AE_CONN_FILE_INSERT(conn, fh, bucket);
		(void)__ae_atomic_add32(&conn->open_file_count, 1);
		*fhp = fh;
	}
	__ae_spin_unlock(session, &conn->fh_lock);
	if (matched) {
err:		if (fh != NULL) {
			__ae_free(session, fh->name);
			__ae_free(session, fh);
		}
		if (fd != -1)
			(void)close(fd);
	}

	__ae_free(session, path);
	return (ret);
}

/*
 * __ae_close --
 *	Close a file handle.
 */
int
__ae_close(AE_SESSION_IMPL *session, AE_FH **fhp)
{
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	AE_FH *fh;
	uint64_t bucket;

	conn = S2C(session);

	if (*fhp == NULL)
		return (0);
	fh = *fhp;
	*fhp = NULL;

	AE_RET(__ae_verbose(session, AE_VERB_FILEOPS, "%s: close", fh->name));

	__ae_spin_lock(session, &conn->fh_lock);
	if (fh == NULL || fh->ref == 0 || --fh->ref > 0) {
		__ae_spin_unlock(session, &conn->fh_lock);
		return (0);
	}

	/* Remove from the list. */
	bucket = fh->name_hash % AE_HASH_ARRAY_SIZE;
	AE_CONN_FILE_REMOVE(conn, fh, bucket);
	(void)__ae_atomic_sub32(&conn->open_file_count, 1);

	__ae_spin_unlock(session, &conn->fh_lock);

	/* Discard the memory. */
	if (close(fh->fd) != 0) {
		ret = __ae_errno();
		__ae_err(session, ret, "close: %s", fh->name);
	}

	__ae_free(session, fh->name);
	__ae_free(session, fh);
	return (ret);
}
