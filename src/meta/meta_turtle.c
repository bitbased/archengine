/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __metadata_config --
 *	Return the default configuration information for the metadata file.
 */
static int
__metadata_config(AE_SESSION_IMPL *session, char **metaconfp)
{
	AE_DECL_ITEM(buf);
	AE_DECL_RET;
	const char *cfg[] = { AE_CONFIG_BASE(session, file_meta), NULL, NULL };
	char *metaconf;

	*metaconfp = NULL;

	metaconf = NULL;

	/* Create a turtle file with default values. */
	AE_RET(__ae_scr_alloc(session, 0, &buf));
	AE_ERR(__ae_buf_fmt(session, buf,
	    "key_format=S,value_format=S,id=%d,version=(major=%d,minor=%d)",
	    AE_METAFILE_ID,
	    AE_BTREE_MAJOR_VERSION_MAX, AE_BTREE_MINOR_VERSION_MAX));
	cfg[1] = buf->data;
	AE_ERR(__ae_config_collapse(session, cfg, &metaconf));

	*metaconfp = metaconf;

	if (0) {
err:		__ae_free(session, metaconf);
	}
	__ae_scr_free(session, &buf);
	return (ret);
}

/*
 * __metadata_init --
 *	Create the metadata file.
 */
static int
__metadata_init(AE_SESSION_IMPL *session)
{
	AE_DECL_RET;

	/*
	 * We're single-threaded, but acquire the schema lock regardless: the
	 * lower level code checks that it is appropriately synchronized.
	 */
	AE_WITH_SCHEMA_LOCK(session,
	    ret = __ae_schema_create(session, AE_METAFILE_URI, NULL));

	return (ret);
}

/*
 * __metadata_load_hot_backup --
 *	Load the contents of any hot backup file.
 */
static int
__metadata_load_hot_backup(AE_SESSION_IMPL *session)
{
	FILE *fp;
	AE_DECL_ITEM(key);
	AE_DECL_ITEM(value);
	AE_DECL_RET;
	bool exist;

	/* Look for a hot backup file: if we find it, load it. */
	AE_RET(__ae_exist(session, AE_METADATA_BACKUP, &exist));
	if (!exist)
		return (0);
	AE_RET(__ae_fopen(session,
	    AE_METADATA_BACKUP, AE_FHANDLE_READ, 0, &fp));

	/* Read line pairs and load them into the metadata file. */
	AE_ERR(__ae_scr_alloc(session, 512, &key));
	AE_ERR(__ae_scr_alloc(session, 512, &value));
	for (;;) {
		AE_ERR(__ae_getline(session, key, fp));
		if (key->size == 0)
			break;
		AE_ERR(__ae_getline(session, value, fp));
		if (value->size == 0)
			AE_ERR(__ae_illegal_value(session, AE_METADATA_BACKUP));
		AE_ERR(__ae_metadata_update(session, key->data, value->data));
	}

	F_SET(S2C(session), AE_CONN_WAS_BACKUP);

err:	AE_TRET(__ae_fclose(&fp, AE_FHANDLE_READ));
	__ae_scr_free(session, &key);
	__ae_scr_free(session, &value);
	return (ret);
}

/*
 * __metadata_load_bulk --
 *	Create any bulk-loaded file stubs.
 */
static int
__metadata_load_bulk(AE_SESSION_IMPL *session)
{
	AE_CURSOR *cursor;
	AE_DECL_RET;
	uint32_t allocsize;
	bool exist;
	const char *filecfg[] = { AE_CONFIG_BASE(session, file_meta), NULL };
	const char *key;

	/*
	 * If a file was being bulk-loaded during the hot backup, it will appear
	 * in the metadata file, but the file won't exist.  Create on demand.
	 */
	AE_ERR(__ae_metadata_cursor(session, NULL, &cursor));
	while ((ret = cursor->next(cursor)) == 0) {
		AE_ERR(cursor->get_key(cursor, &key));
		if (!AE_PREFIX_SKIP(key, "file:"))
			continue;

		/* If the file exists, it's all good. */
		AE_ERR(__ae_exist(session, key, &exist));
		if (exist)
			continue;

		/*
		 * If the file doesn't exist, assume it's a bulk-loaded file;
		 * retrieve the allocation size and re-create the file.
		 */
		AE_ERR(__ae_direct_io_size_check(
		    session, filecfg, "allocation_size", &allocsize));
		AE_ERR(__ae_block_manager_create(session, key, allocsize));
	}
	AE_ERR_NOTFOUND_OK(ret);

err:	if (cursor != NULL)
		AE_TRET(cursor->close(cursor));

	return (ret);
}

/*
 * __ae_turtle_init --
 *	Check the turtle file and create if necessary.
 */
int
__ae_turtle_init(AE_SESSION_IMPL *session)
{
	AE_DECL_RET;
	bool exist, exist_incr;
	char *metaconf;

	metaconf = NULL;

	/*
	 * Discard any turtle setup file left-over from previous runs.  This
	 * doesn't matter for correctness, it's just cleaning up random files.
	 */
	AE_RET(__ae_remove_if_exists(session, AE_METADATA_TURTLE_SET));

	/*
	 * We could die after creating the turtle file and before creating the
	 * metadata file, or worse, the metadata file might be in some random
	 * state.  Make sure that doesn't happen: if we don't find the turtle
	 * file, first create the metadata file, load any hot backup, and then
	 * create the turtle file.  No matter what happens, if metadata file
	 * creation doesn't fully complete, we won't have a turtle file and we
	 * will repeat the process until we succeed.
	 *
	 * Incremental backups can occur only if recovery is run and it becomes
	 * live. So, if there is a turtle file and an incremental backup file,
	 * that is an error.  Otherwise, if there's already a turtle file, we're
	 * done.
	 */
	AE_RET(__ae_exist(session, AE_INCREMENTAL_BACKUP, &exist_incr));
	AE_RET(__ae_exist(session, AE_METADATA_TURTLE, &exist));
	if (exist) {
		if (exist_incr)
			AE_RET_MSG(session, EINVAL,
			    "Incremental backup after running recovery "
			    "is not allowed.");
	} else {
		if (exist_incr)
			F_SET(S2C(session), AE_CONN_WAS_BACKUP);

		/* Create the metadata file. */
		AE_RET(__metadata_init(session));

		/* Load any hot-backup information. */
		AE_RET(__metadata_load_hot_backup(session));

		/* Create any bulk-loaded file stubs. */
		AE_RET(__metadata_load_bulk(session));

		/* Create the turtle file. */
		AE_RET(__metadata_config(session, &metaconf));
		AE_WITH_TURTLE_LOCK(session, ret = __ae_turtle_update(
		    session, AE_METAFILE_URI, metaconf));
		AE_ERR(ret);
	}

	/* Remove the backup files, we'll never read them again. */
	AE_ERR(__ae_backup_file_remove(session));

err:	__ae_free(session, metaconf);
	return (ret);
}

/*
 * __ae_turtle_read --
 *	Read the turtle file.
 */
int
__ae_turtle_read(AE_SESSION_IMPL *session, const char *key, char **valuep)
{
	FILE *fp;
	AE_DECL_ITEM(buf);
	AE_DECL_RET;
	bool exist, match;

	*valuep = NULL;

	/*
	 * Open the turtle file; there's one case where we won't find the turtle
	 * file, yet still succeed.  We create the metadata file before creating
	 * the turtle file, and that means returning the default configuration
	 * string for the metadata file.
	 */
	AE_RET(__ae_exist(session, AE_METADATA_TURTLE, &exist));
	if (!exist)
		return (strcmp(key, AE_METAFILE_URI) == 0 ?
		    __metadata_config(session, valuep) : AE_NOTFOUND);
	AE_RET(__ae_fopen(session,
	    AE_METADATA_TURTLE, AE_FHANDLE_READ, 0, &fp));

	/* Search for the key. */
	AE_ERR(__ae_scr_alloc(session, 512, &buf));
	for (match = false;;) {
		AE_ERR(__ae_getline(session, buf, fp));
		if (buf->size == 0)
			AE_ERR(AE_NOTFOUND);
		if (strcmp(key, buf->data) == 0)
			match = true;

		/* Key matched: read the subsequent line for the value. */
		AE_ERR(__ae_getline(session, buf, fp));
		if (buf->size == 0)
			AE_ERR(__ae_illegal_value(session, AE_METADATA_TURTLE));
		if (match)
			break;
	}

	/* Copy the value for the caller. */
	AE_ERR(__ae_strdup(session, buf->data, valuep));

err:	AE_TRET(__ae_fclose(&fp, AE_FHANDLE_READ));
	__ae_scr_free(session, &buf);
	return (ret);
}

/*
 * __ae_turtle_update --
 *	Update the turtle file.
 */
int
__ae_turtle_update(
    AE_SESSION_IMPL *session, const char *key,  const char *value)
{
	AE_FH *fh;
	AE_DECL_ITEM(buf);
	AE_DECL_RET;
	int vmajor, vminor, vpatch;
	const char *version;

	fh = NULL;

	/*
	 * Create the turtle setup file: we currently re-write it from scratch
	 * every time.
	 */
	AE_RET(__ae_open(session,
	    AE_METADATA_TURTLE_SET, true, true, AE_FILE_TYPE_TURTLE, &fh));

	version = archengine_version(&vmajor, &vminor, &vpatch);
	AE_ERR(__ae_scr_alloc(session, 2 * 1024, &buf));
	AE_ERR(__ae_buf_fmt(session, buf,
	    "%s\n%s\n%s\n" "major=%d,minor=%d,patch=%d\n%s\n%s\n",
	    AE_METADATA_VERSION_STR, version,
	    AE_METADATA_VERSION, vmajor, vminor, vpatch,
	    key, value));
	AE_ERR(__ae_write(session, fh, 0, buf->size, buf->data));

	/* Flush the handle and rename the file into place. */
	ret = __ae_sync_and_rename_fh(
	    session, &fh, AE_METADATA_TURTLE_SET, AE_METADATA_TURTLE);

	/* Close any file handle left open, remove any temporary file. */
err:	AE_TRET(__ae_close(session, &fh));
	AE_TRET(__ae_remove_if_exists(session, AE_METADATA_TURTLE_SET));

	__ae_scr_free(session, &buf);
	return (ret);
}
