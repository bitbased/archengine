/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

static int __backup_all(AE_SESSION_IMPL *, AE_CURSOR_BACKUP *);
static int __backup_cleanup_handles(AE_SESSION_IMPL *, AE_CURSOR_BACKUP *);
static int __backup_file_create(AE_SESSION_IMPL *, AE_CURSOR_BACKUP *, bool);
static int __backup_list_all_append(AE_SESSION_IMPL *, const char *[]);
static int __backup_list_append(
    AE_SESSION_IMPL *, AE_CURSOR_BACKUP *, const char *);
static int __backup_start(
    AE_SESSION_IMPL *, AE_CURSOR_BACKUP *, const char *[]);
static int __backup_stop(AE_SESSION_IMPL *);
static int __backup_uri(AE_SESSION_IMPL *, const char *[], bool *, bool *);

/*
 * __curbackup_next --
 *	AE_CURSOR->next method for the backup cursor type.
 */
static int
__curbackup_next(AE_CURSOR *cursor)
{
	AE_CURSOR_BACKUP *cb;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	cb = (AE_CURSOR_BACKUP *)cursor;
	CURSOR_API_CALL(cursor, session, next, NULL);

	if (cb->list == NULL || cb->list[cb->next].name == NULL) {
		F_CLR(cursor, AE_CURSTD_KEY_SET);
		AE_ERR(AE_NOTFOUND);
	}

	cb->iface.key.data = cb->list[cb->next].name;
	cb->iface.key.size = strlen(cb->list[cb->next].name) + 1;
	++cb->next;

	F_SET(cursor, AE_CURSTD_KEY_INT);

err:	API_END_RET(session, ret);
}

/*
 * __curbackup_reset --
 *	AE_CURSOR->reset method for the backup cursor type.
 */
static int
__curbackup_reset(AE_CURSOR *cursor)
{
	AE_CURSOR_BACKUP *cb;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	cb = (AE_CURSOR_BACKUP *)cursor;
	CURSOR_API_CALL(cursor, session, reset, NULL);

	cb->next = 0;
	F_CLR(cursor, AE_CURSTD_KEY_SET | AE_CURSTD_VALUE_SET);

err:	API_END_RET(session, ret);
}

/*
 * __curbackup_close --
 *	AE_CURSOR->close method for the backup cursor type.
 */
static int
__curbackup_close(AE_CURSOR *cursor)
{
	AE_CURSOR_BACKUP *cb;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;
	int tret;

	cb = (AE_CURSOR_BACKUP *)cursor;
	CURSOR_API_CALL(cursor, session, close, NULL);

	AE_TRET(__backup_cleanup_handles(session, cb));
	AE_TRET(__ae_cursor_close(cursor));
	session->bkp_cursor = NULL;

	AE_WITH_SCHEMA_LOCK(session,
	    tret = __backup_stop(session));		/* Stop the backup. */
	AE_TRET(tret);

err:	API_END_RET(session, ret);
}

/*
 * __ae_curbackup_open --
 *	AE_SESSION->open_cursor method for the backup cursor type.
 */
int
__ae_curbackup_open(AE_SESSION_IMPL *session,
    const char *uri, const char *cfg[], AE_CURSOR **cursorp)
{
	AE_CURSOR_STATIC_INIT(iface,
	    __ae_cursor_get_key,	/* get-key */
	    __ae_cursor_notsup,		/* get-value */
	    __ae_cursor_notsup,		/* set-key */
	    __ae_cursor_notsup,		/* set-value */
	    __ae_cursor_notsup,		/* compare */
	    __ae_cursor_notsup,		/* equals */
	    __curbackup_next,		/* next */
	    __ae_cursor_notsup,		/* prev */
	    __curbackup_reset,		/* reset */
	    __ae_cursor_notsup,		/* search */
	    __ae_cursor_notsup,		/* search-near */
	    __ae_cursor_notsup,		/* insert */
	    __ae_cursor_notsup,		/* update */
	    __ae_cursor_notsup,		/* remove */
	    __ae_cursor_notsup,		/* reconfigure */
	    __curbackup_close);		/* close */
	AE_CURSOR *cursor;
	AE_CURSOR_BACKUP *cb;
	AE_DECL_RET;

	AE_STATIC_ASSERT(offsetof(AE_CURSOR_BACKUP, iface) == 0);

	cb = NULL;

	AE_RET(__ae_calloc_one(session, &cb));
	cursor = &cb->iface;
	*cursor = iface;
	cursor->session = &session->iface;
	session->bkp_cursor = cb;

	cursor->key_format = "S";	/* Return the file names as the key. */
	cursor->value_format = "";	/* No value. */

	/*
	 * Start the backup and fill in the cursor's list.  Acquire the schema
	 * lock, we need a consistent view when creating a copy.
	 */
	AE_WITH_SCHEMA_LOCK(session, ret = __backup_start(session, cb, cfg));
	AE_ERR(ret);

	/* __ae_cursor_init is last so we don't have to clean up on error. */
	AE_ERR(__ae_cursor_init(cursor, uri, NULL, cfg, cursorp));

	if (0) {
err:		__ae_free(session, cb);
	}

	return (ret);
}

/*
 * __backup_log_append --
 *	Append log files needed for backup.
 */
static int
__backup_log_append(AE_SESSION_IMPL *session, AE_CURSOR_BACKUP *cb, bool active)
{
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	u_int i, logcount;
	char **logfiles;

	conn = S2C(session);
	logfiles = NULL;
	logcount = 0;
	ret = 0;

	if (conn->log) {
		AE_ERR(__ae_log_get_all_files(
		    session, &logfiles, &logcount, &cb->maxid, active));
		for (i = 0; i < logcount; i++)
			AE_ERR(__backup_list_append(session, cb, logfiles[i]));
	}
err:	if (logfiles != NULL)
		__ae_log_files_free(session, logfiles, logcount);
	return (ret);
}

/*
 * __backup_start --
 *	Start a backup.
 */
static int
__backup_start(
    AE_SESSION_IMPL *session, AE_CURSOR_BACKUP *cb, const char *cfg[])
{
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	bool exist, log_only, target_list;

	conn = S2C(session);

	cb->next = 0;
	cb->list = NULL;
	cb->list_next = 0;

	/*
	 * Single thread hot backups: we're holding the schema lock, so we
	 * know we'll serialize with other attempts to start a hot backup.
	 */
	if (conn->hot_backup)
		AE_RET_MSG(
		    session, EINVAL, "there is already a backup cursor open");

	/*
	 * The hot backup copy is done outside of ArchEngine, which means file
	 * blocks can't be freed and re-allocated until the backup completes.
	 * The checkpoint code checks the backup flag, and if a backup cursor
	 * is open checkpoints aren't discarded. We release the lock as soon
	 * as we've set the flag, we don't want to block checkpoints, we just
	 * want to make sure no checkpoints are deleted.  The checkpoint code
	 * holds the lock until it's finished the checkpoint, otherwise we
	 * could start a hot backup that would race with an already-started
	 * checkpoint.
	 */
	AE_RET(__ae_writelock(session, conn->hot_backup_lock));
	conn->hot_backup = true;
	AE_ERR(__ae_writeunlock(session, conn->hot_backup_lock));

	/* Create the hot backup file. */
	AE_ERR(__backup_file_create(session, cb, false));

	/* Add log files if logging is enabled. */

	/*
	 * If a list of targets was specified, work our way through them.
	 * Else, generate a list of all database objects.
	 *
	 * Include log files if doing a full backup, and copy them before
	 * copying data files to avoid rolling the metadata forward across
	 * a checkpoint that completes during the backup.
	 */
	target_list = false;
	AE_ERR(__backup_uri(session, cfg, &target_list, &log_only));

	if (!target_list) {
		AE_ERR(__backup_log_append(session, cb, true));
		AE_ERR(__backup_all(session, cb));
	}

	/* Add the hot backup and standard ArchEngine files to the list. */
	if (log_only) {
		/*
		 * Close any hot backup file.
		 * We're about to open the incremental backup file.
		 */
		AE_TRET(__ae_fclose(&cb->bfp, AE_FHANDLE_WRITE));
		AE_ERR(__backup_file_create(session, cb, log_only));
		AE_ERR(__backup_list_append(
		    session, cb, AE_INCREMENTAL_BACKUP));
	} else {
		AE_ERR(__backup_list_append(session, cb, AE_METADATA_BACKUP));
		AE_ERR(__ae_exist(session, AE_BASECONFIG, &exist));
		if (exist)
			AE_ERR(__backup_list_append(
			    session, cb, AE_BASECONFIG));
		AE_ERR(__ae_exist(session, AE_USERCONFIG, &exist));
		if (exist)
			AE_ERR(__backup_list_append(
			    session, cb, AE_USERCONFIG));
		AE_ERR(__backup_list_append(session, cb, AE_ARCHENGINE));
	}

err:	/* Close the hot backup file. */
	AE_TRET(__ae_fclose(&cb->bfp, AE_FHANDLE_WRITE));
	if (ret != 0) {
		AE_TRET(__backup_cleanup_handles(session, cb));
		AE_TRET(__backup_stop(session));
	}

	return (ret);
}

/*
 * __backup_cleanup_handles --
 *	Release and free all btree handles held by the backup. This is kept
 *	separate from __backup_stop because it can be called without the
 *	schema lock held.
 */
static int
__backup_cleanup_handles(AE_SESSION_IMPL *session, AE_CURSOR_BACKUP *cb)
{
	AE_CURSOR_BACKUP_ENTRY *p;
	AE_DECL_RET;

	if (cb->list == NULL)
		return (0);

	/* Release the handles, free the file names, free the list itself. */
	for (p = cb->list; p->name != NULL; ++p) {
		if (p->handle != NULL)
			AE_WITH_DHANDLE(session, p->handle,
			    AE_TRET(__ae_session_release_btree(session)));
		__ae_free(session, p->name);
	}

	__ae_free(session, cb->list);
	return (ret);
}

/*
 * __backup_stop --
 *	Stop a backup.
 */
static int
__backup_stop(AE_SESSION_IMPL *session)
{
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;

	conn = S2C(session);

	/* Remove any backup specific file. */
	ret = __ae_backup_file_remove(session);

	/* Checkpoint deletion can proceed, as can the next hot backup. */
	AE_TRET(__ae_writelock(session, conn->hot_backup_lock));
	conn->hot_backup = false;
	AE_TRET(__ae_writeunlock(session, conn->hot_backup_lock));

	return (ret);
}

/*
 * __backup_all --
 *	Backup all objects in the database.
 */
static int
__backup_all(AE_SESSION_IMPL *session, AE_CURSOR_BACKUP *cb)
{
	AE_CONFIG_ITEM cval;
	AE_CURSOR *cursor;
	AE_DECL_RET;
	const char *key, *value;

	cursor = NULL;

	/*
	 * Open a cursor on the metadata file and copy all of the entries to
	 * the hot backup file.
	 */
	AE_ERR(__ae_metadata_cursor(session, NULL, &cursor));
	while ((ret = cursor->next(cursor)) == 0) {
		AE_ERR(cursor->get_key(cursor, &key));
		AE_ERR(cursor->get_value(cursor, &value));
		AE_ERR(__ae_fprintf(cb->bfp, "%s\n%s\n", key, value));

		/*
		 * While reading the metadata file, check there are no "sources"
		 * or "types" which can't support hot backup.  This checks for
		 * a data source that's non-standard, which can't be backed up,
		 * but is also sanity checking: if there's an entry backed by
		 * anything other than a file or lsm entry, we're confused.
		 */
		if ((ret = __ae_config_getones(
		    session, value, "type", &cval)) == 0 &&
		    !AE_PREFIX_MATCH_LEN(cval.str, cval.len, "file") &&
		    !AE_PREFIX_MATCH_LEN(cval.str, cval.len, "lsm"))
			AE_ERR_MSG(session, ENOTSUP,
			    "hot backup is not supported for objects of "
			    "type %.*s", (int)cval.len, cval.str);
		AE_ERR_NOTFOUND_OK(ret);
		if ((ret =__ae_config_getones(
		    session, value, "source", &cval)) == 0 &&
		    !AE_PREFIX_MATCH_LEN(cval.str, cval.len, "file:") &&
		    !AE_PREFIX_MATCH_LEN(cval.str, cval.len, "lsm:"))
			AE_ERR_MSG(session, ENOTSUP,
			    "hot backup is not supported for objects of "
			    "source %.*s", (int)cval.len, cval.str);
		AE_ERR_NOTFOUND_OK(ret);
	}
	AE_ERR_NOTFOUND_OK(ret);

	/* Build a list of the file objects that need to be copied. */
	AE_WITH_HANDLE_LIST_LOCK(session,
	    ret = __ae_meta_btree_apply(
	    session, __backup_list_all_append, NULL));

err:	if (cursor != NULL)
		AE_TRET(cursor->close(cursor));
	return (ret);
}

/*
 * __backup_uri --
 *	Backup a list of objects.
 */
static int
__backup_uri(AE_SESSION_IMPL *session,
    const char *cfg[], bool *foundp, bool *log_only)
{
	AE_CONFIG targetconf;
	AE_CONFIG_ITEM cval, k, v;
	AE_DECL_ITEM(tmp);
	AE_DECL_RET;
	bool target_list;
	const char *uri;

	*foundp = *log_only = false;

	/*
	 * If we find a non-empty target configuration string, we have a job,
	 * otherwise it's not our problem.
	 */
	AE_RET(__ae_config_gets(session, cfg, "target", &cval));
	AE_RET(__ae_config_subinit(session, &targetconf, &cval));
	for (target_list = false;
	    (ret = __ae_config_next(&targetconf, &k, &v)) == 0;
	    target_list = true) {
		/* If it is our first time through, allocate. */
		if (!target_list) {
			*foundp = true;
			AE_ERR(__ae_scr_alloc(session, 512, &tmp));
		}

		AE_ERR(__ae_buf_fmt(session, tmp, "%.*s", (int)k.len, k.str));
		uri = tmp->data;
		if (v.len != 0)
			AE_ERR_MSG(session, EINVAL,
			    "%s: invalid backup target: URIs may need quoting",
			    uri);

		/*
		 * Handle log targets.  We do not need to go through the
		 * schema worker, just call the function to append them.
		 * Set log_only only if it is our only URI target.
		 */
		if (AE_PREFIX_MATCH(uri, "log:")) {
			*log_only = !target_list;
			AE_ERR(__ae_backup_list_uri_append(session, uri, NULL));
		} else {
			*log_only = false;
			AE_ERR(__ae_schema_worker(session,
			    uri, NULL, __ae_backup_list_uri_append, cfg, 0));
		}
	}
	AE_ERR_NOTFOUND_OK(ret);

err:	__ae_scr_free(session, &tmp);
	return (ret);
}

/*
 * __backup_file_create --
 *	Create the meta-data backup file.
 */
static int
__backup_file_create(
    AE_SESSION_IMPL *session, AE_CURSOR_BACKUP *cb, bool incremental)
{
	return (__ae_fopen(session,
	    incremental ? AE_INCREMENTAL_BACKUP : AE_METADATA_BACKUP,
	    AE_FHANDLE_WRITE, 0, &cb->bfp));
}

/*
 * __ae_backup_file_remove --
 *	Remove the incremental and meta-data backup files.
 */
int
__ae_backup_file_remove(AE_SESSION_IMPL *session)
{
	AE_DECL_RET;

	AE_TRET(__ae_remove_if_exists(session, AE_INCREMENTAL_BACKUP));
	AE_TRET(__ae_remove_if_exists(session, AE_METADATA_BACKUP));
	return (ret);
}

/*
 * __ae_backup_list_uri_append --
 *	Append a new file name to the list, allocate space as necessary.
 *	Called via the schema_worker function.
 */
int
__ae_backup_list_uri_append(
    AE_SESSION_IMPL *session, const char *name, bool *skip)
{
	AE_CURSOR_BACKUP *cb;
	char *value;

	cb = session->bkp_cursor;
	AE_UNUSED(skip);

	if (AE_PREFIX_MATCH(name, "log:")) {
		AE_RET(__backup_log_append(session, cb, false));
		return (0);
	}

	/* Add the metadata entry to the backup file. */
	AE_RET(__ae_metadata_search(session, name, &value));
	AE_RET(__ae_fprintf(cb->bfp, "%s\n%s\n", name, value));
	__ae_free(session, value);

	/* Add file type objects to the list of files to be copied. */
	if (AE_PREFIX_MATCH(name, "file:"))
		AE_RET(__backup_list_append(session, cb, name));

	return (0);
}

/*
 * __backup_list_all_append --
 *	Append a new file name to the list, allocate space as necessary.
 *	Called via the __ae_meta_btree_apply function.
 */
static int
__backup_list_all_append(AE_SESSION_IMPL *session, const char *cfg[])
{
	AE_CURSOR_BACKUP *cb;
	const char *name;

	AE_UNUSED(cfg);

	cb = session->bkp_cursor;
	name = session->dhandle->name;

	/* Ignore files in the process of being bulk-loaded. */
	if (F_ISSET(S2BT(session), AE_BTREE_BULK))
		return (0);

	/* Ignore the lookaside table. */
	if (strcmp(name, AE_LAS_URI) == 0)
		return (0);

	/* Add the file to the list of files to be copied. */
	return (__backup_list_append(session, cb, name));
}

/*
 * __backup_list_append --
 *	Append a new file name to the list, allocate space as necessary.
 */
static int
__backup_list_append(
    AE_SESSION_IMPL *session, AE_CURSOR_BACKUP *cb, const char *uri)
{
	AE_CURSOR_BACKUP_ENTRY *p;
	AE_DATA_HANDLE *old_dhandle;
	AE_DECL_RET;
	bool need_handle;
	const char *name;

	/* Leave a NULL at the end to mark the end of the list. */
	AE_RET(__ae_realloc_def(session, &cb->list_allocated,
	    cb->list_next + 2, &cb->list));
	p = &cb->list[cb->list_next];
	p[0].name = p[1].name = NULL;
	p[0].handle = p[1].handle = NULL;

	need_handle = false;
	name = uri;
	if (AE_PREFIX_MATCH(uri, "file:")) {
		need_handle = true;
		name += strlen("file:");
	}

	/*
	 * !!!
	 * Assumes metadata file entries map one-to-one to physical files.
	 * To support a block manager where that's not the case, we'd need
	 * to call into the block manager and get a list of physical files
	 * that map to this logical "file".  I'm not going to worry about
	 * that for now, that block manager might not even support physical
	 * copying of files by applications.
	 */
	AE_RET(__ae_strdup(session, name, &p->name));

	/*
	 * If it's a file in the database, get a handle for the underlying
	 * object (this handle blocks schema level operations, for example
	 * AE_SESSION.drop or an LSM file discard after level merging).
	 */
	if (need_handle) {
		old_dhandle = session->dhandle;
		if ((ret =
		    __ae_session_get_btree(session, uri, NULL, NULL, 0)) == 0)
			p->handle = session->dhandle;
		session->dhandle = old_dhandle;
		AE_RET(ret);
	}

	++cb->list_next;
	return (0);
}
