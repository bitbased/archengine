/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_open --
 *	Open a file handle.
 */
int
__ae_open(AE_SESSION_IMPL *session,
    const char *name, bool ok_create, bool exclusive, int dio_type, AE_FH **fhp)
{
	DWORD dwCreationDisposition;
	HANDLE filehandle, filehandle_secondary;
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	AE_FH *fh, *tfh;
	uint64_t bucket, hash;
	int f, share_mode;
	bool direct_io, matched;
	char *path;

	conn = S2C(session);
	fh = NULL;
	path = NULL;
	filehandle = INVALID_HANDLE_VALUE;
	filehandle_secondary = INVALID_HANDLE_VALUE;
	direct_io = false;
	hash = __ae_hash_city64(name, strlen(name));
	bucket = hash % AE_HASH_ARRAY_SIZE;

	AE_RET(__ae_verbose(session, AE_VERB_FILEOPS, "%s: open", name));

	/* Increment the reference count if we already have the file open. */
	matched = false;
	__ae_spin_lock(session, &conn->fh_lock);
	TAILQ_FOREACH(tfh, &conn->fhhash[bucket], hashq)
		if (strcmp(name, tfh->name) == 0) {
			++tfh->ref;
			*fhp = tfh;
			matched = true;
			break;
		}
	__ae_spin_unlock(session, &conn->fh_lock);
	if (matched)
		return (0);

	/* For directories, create empty file handles with invalid handles */
	if (dio_type == AE_FILE_TYPE_DIRECTORY) {
		goto setupfh;
	}

	AE_RET(__ae_filename(session, name, &path));

	share_mode = FILE_SHARE_READ | FILE_SHARE_WRITE;
	/*
	 * Security:
	 * The application may spawn a new process, and we don't want another
	 * process to have access to our file handles.
	 *
	 * TODO: Set tighter file permissions but set bInheritHandle to false
	 * to prevent inheritance
	 */

	f = FILE_ATTRIBUTE_NORMAL;

	dwCreationDisposition = 0;
	if (ok_create) {
		dwCreationDisposition = CREATE_NEW;
		if (exclusive)
			dwCreationDisposition = CREATE_ALWAYS;
	} else
		dwCreationDisposition = OPEN_EXISTING;

	/*
	 * direct_io means no OS file caching. This requires aligned buffer
	 * allocations like O_DIRECT.
	 */
	if (dio_type && FLD_ISSET(conn->direct_io, dio_type)) {
		f |= FILE_FLAG_NO_BUFFERING;
		direct_io = true;
	}

	/* FILE_FLAG_WRITE_THROUGH does not require aligned buffers */
	if (dio_type && FLD_ISSET(conn->write_through, dio_type)) {
		f |= FILE_FLAG_WRITE_THROUGH;
	}

	if (dio_type == AE_FILE_TYPE_LOG &&
	    FLD_ISSET(conn->txn_logsync, AE_LOG_DSYNC)) {
		f |= FILE_FLAG_WRITE_THROUGH;
	}

	/* Disable read-ahead on trees: it slows down random read workloads. */
	if (dio_type == AE_FILE_TYPE_DATA ||
	    dio_type == AE_FILE_TYPE_CHECKPOINT)
		f |= FILE_FLAG_RANDOM_ACCESS;

	filehandle = CreateFileA(path,
				(GENERIC_READ | GENERIC_WRITE),
				share_mode,
				NULL,
				dwCreationDisposition,
				f,
				NULL);
	if (filehandle == INVALID_HANDLE_VALUE) {
		if (GetLastError() == ERROR_FILE_EXISTS && ok_create)
			filehandle = CreateFileA(path,
						(GENERIC_READ | GENERIC_WRITE),
						share_mode,
						NULL,
						OPEN_EXISTING,
						f,
						NULL);

		if (filehandle == INVALID_HANDLE_VALUE)
			AE_ERR_MSG(session, __ae_errno(),
			    direct_io ?
			    "%s: open failed with direct I/O configured, some "
			    "filesystem types do not support direct I/O" :
			    "%s", path);
	}

	/*
	 * Open a second handle to file to support allocation/truncation
	 * concurrently with reads on the file. Writes would also move the file
	 * pointer.
	 */
	filehandle_secondary = CreateFileA(path,
	    (GENERIC_READ | GENERIC_WRITE),
	    share_mode,
	    NULL,
	    OPEN_EXISTING,
	    f,
	    NULL);
	if (filehandle == INVALID_HANDLE_VALUE)
		AE_ERR_MSG(session, __ae_errno(),
		    "open failed for secondary handle: %s", path);

setupfh:
	AE_ERR(__ae_calloc_one(session, &fh));
	AE_ERR(__ae_strdup(session, name, &fh->name));
	fh->name_hash = hash;
	fh->filehandle = filehandle;
	fh->filehandle_secondary = filehandle_secondary;
	fh->ref = 1;
	fh->direct_io = direct_io;

	/* Set the file's size. */
	if (dio_type != AE_FILE_TYPE_DIRECTORY)
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
	TAILQ_FOREACH(tfh, &conn->fhhash[bucket], hashq)
		if (strcmp(name, tfh->name) == 0) {
			++tfh->ref;
			*fhp = tfh;
			matched = true;
			break;
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
		if (filehandle != INVALID_HANDLE_VALUE)
			(void)CloseHandle(filehandle);
		if (filehandle_secondary != INVALID_HANDLE_VALUE)
			(void)CloseHandle(filehandle_secondary);
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

	/* Discard the memory.
	 * Note: For directories, we do not open valid directory handles on
	 * windows since it is not possible to sync a directory
	 */
	if (fh->filehandle != INVALID_HANDLE_VALUE &&
	    CloseHandle(fh->filehandle) == 0) {
		ret = __ae_errno();
		__ae_err(session, ret, "CloseHandle: %s", fh->name);
	}

	if (fh->filehandle_secondary != INVALID_HANDLE_VALUE &&
	    CloseHandle(fh->filehandle_secondary) == 0) {
		ret = __ae_errno();
		__ae_err(session, ret, "CloseHandle: secondary: %s", fh->name);
	}

	__ae_free(session, fh->name);
	__ae_free(session, fh);
	return (ret);
}
