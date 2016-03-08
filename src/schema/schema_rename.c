/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __rename_file --
 *	AE_SESSION::rename for a file.
 */
static int
__rename_file(
    AE_SESSION_IMPL *session, const char *uri, const char *newuri)
{
	AE_DECL_RET;
	bool exist;
	const char *filename, *newfile;
	char *newvalue, *oldvalue;

	newvalue = oldvalue = NULL;

	filename = uri;
	newfile = newuri;
	if (!AE_PREFIX_SKIP(filename, "file:") ||
	    !AE_PREFIX_SKIP(newfile, "file:"))
		return (EINVAL);

	/* Close any btree handles in the file. */
	AE_WITH_HANDLE_LIST_LOCK(session,
	    ret = __ae_conn_dhandle_close_all(session, uri, false));
	AE_ERR(ret);

	/*
	 * First, check if the file being renamed exists in the system.  Doing
	 * this check first matches the table rename behavior because we return
	 * AE_NOTFOUND when the renamed file doesn't exist (subsequently mapped
	 * to ENOENT by the session layer).
	 */
	AE_ERR(__ae_metadata_search(session, uri, &oldvalue));

	/*
	 * Check to see if the proposed name is already in use, in either the
	 * metadata or the filesystem.
	 */
	switch (ret = __ae_metadata_search(session, newuri, &newvalue)) {
	case 0:
		AE_ERR_MSG(session, EEXIST, "%s", newuri);
		/* NOTREACHED */
	case AE_NOTFOUND:
		break;
	default:
		AE_ERR(ret);
	}
	AE_ERR(__ae_exist(session, newfile, &exist));
	if (exist)
		AE_ERR_MSG(session, EEXIST, "%s", newfile);

	/* Replace the old file entries with new file entries. */
	AE_ERR(__ae_metadata_remove(session, uri));
	AE_ERR(__ae_metadata_insert(session, newuri, oldvalue));

	/* Rename the underlying file. */
	AE_ERR(__ae_rename(session, filename, newfile));
	if (AE_META_TRACKING(session))
		AE_ERR(__ae_meta_track_fileop(session, uri, newuri));

err:	__ae_free(session, newvalue);
	__ae_free(session, oldvalue);
	return (ret);
}

/*
 * __rename_tree --
 *	Rename an index or colgroup reference.
 */
static int
__rename_tree(AE_SESSION_IMPL *session,
    AE_TABLE *table, const char *newuri, const char *name, const char *cfg[])
{
	AE_CONFIG_ITEM cval;
	AE_DECL_ITEM(nn);
	AE_DECL_ITEM(ns);
	AE_DECL_ITEM(nv);
	AE_DECL_ITEM(os);
	AE_DECL_RET;
	bool is_colgroup;
	const char *newname, *olduri, *suffix;
	char *value;

	olduri = table->name;
	value = NULL;

	newname = newuri;
	(void)AE_PREFIX_SKIP(newname, "table:");

	/*
	 * Create the new data source URI and update the schema value.
	 *
	 * 'name' has the format (colgroup|index):<tablename>[:<suffix>];
	 * we need the suffix.
	 */
	is_colgroup = AE_PREFIX_MATCH(name, "colgroup:");
	if (!is_colgroup && !AE_PREFIX_MATCH(name, "index:"))
		AE_ERR_MSG(session, EINVAL,
		    "expected a 'colgroup:' or 'index:' source: '%s'", name);

	suffix = strchr(name, ':');
	/* An existing table should have a well formed name. */
	AE_ASSERT(session, suffix != NULL);
	suffix = strchr(suffix + 1, ':');

	AE_ERR(__ae_scr_alloc(session, 0, &nn));
	AE_ERR(__ae_buf_fmt(session, nn, "%s%s%s",
	    is_colgroup ? "colgroup:" : "index:",
	    newname,
	    (suffix == NULL) ? "" : suffix));

	/* Skip the colon, if any. */
	if (suffix != NULL)
		++suffix;

	/* Read the old schema value. */
	AE_ERR(__ae_metadata_search(session, name, &value));

	/*
	 * Calculate the new data source URI.  Use the existing table structure
	 * and substitute the new name temporarily.
	 */
	AE_ERR(__ae_scr_alloc(session, 0, &ns));
	table->name = newuri;
	if (is_colgroup)
		AE_ERR(__ae_schema_colgroup_source(
		    session, table, suffix, value, ns));
	else
		AE_ERR(__ae_schema_index_source(
		    session, table, suffix, value, ns));

	if ((ret = __ae_config_getones(session, value, "source", &cval)) != 0)
		AE_ERR_MSG(session, EINVAL,
		    "index or column group has no data source: %s", value);

	/* Take a copy of the old data source. */
	AE_ERR(__ae_scr_alloc(session, 0, &os));
	AE_ERR(__ae_buf_fmt(session, os, "%.*s", (int)cval.len, cval.str));

	/* Overwrite it with the new data source. */
	AE_ERR(__ae_scr_alloc(session, 0, &nv));
	AE_ERR(__ae_buf_fmt(session, nv, "%.*s%s%s",
	    (int)AE_PTRDIFF(cval.str, value), value,
	    (const char *)ns->data, cval.str + cval.len));

	/*
	 * Do the rename before updating the metadata to avoid leaving the
	 * metadata inconsistent if the rename fails.
	 */
	AE_ERR(__ae_schema_rename(session, os->data, ns->data, cfg));

	/*
	 * Remove the old metadata entry.
	 * Insert the new metadata entry.
	 */
	AE_ERR(__ae_metadata_remove(session, name));
	AE_ERR(__ae_metadata_insert(session, nn->data, nv->data));

err:	__ae_scr_free(session, &nn);
	__ae_scr_free(session, &ns);
	__ae_scr_free(session, &nv);
	__ae_scr_free(session, &os);
	__ae_free(session, value);
	table->name = olduri;
	return (ret);
}

/*
 * __metadata_rename --
 *	Rename an entry in the metadata table.
 */
static int
__metadata_rename(AE_SESSION_IMPL *session, const char *uri, const char *newuri)
{
	AE_DECL_RET;
	char *value;

	AE_RET(__ae_metadata_search(session, uri, &value));
	AE_ERR(__ae_metadata_remove(session, uri));
	AE_ERR(__ae_metadata_insert(session, newuri, value));

err:	__ae_free(session, value);
	return (ret);
}

/*
 * __rename_table --
 *	AE_SESSION::rename for a table.
 */
static int
__rename_table(AE_SESSION_IMPL *session,
    const char *uri, const char *newuri, const char *cfg[])
{
	AE_DECL_RET;
	AE_TABLE *table;
	u_int i;
	const char *oldname;

	oldname = uri;
	(void)AE_PREFIX_SKIP(oldname, "table:");

	AE_RET(__ae_schema_get_table(
	    session, oldname, strlen(oldname), false, &table));

	/* Rename the column groups. */
	for (i = 0; i < AE_COLGROUPS(table); i++)
		AE_ERR(__rename_tree(session, table, newuri,
		    table->cgroups[i]->name, cfg));

	/* Rename the indices. */
	AE_ERR(__ae_schema_open_indices(session, table));
	for (i = 0; i < table->nindices; i++)
		AE_ERR(__rename_tree(session, table, newuri,
		    table->indices[i]->name, cfg));

	AE_ERR(__ae_schema_remove_table(session, table));
	table = NULL;

	/* Rename the table. */
	AE_ERR(__metadata_rename(session, uri, newuri));

err:	if (table != NULL)
		__ae_schema_release_table(session, table);
	return (ret);
}

/*
 * __ae_schema_rename --
 *	AE_SESSION::rename.
 */
int
__ae_schema_rename(AE_SESSION_IMPL *session,
    const char *uri, const char *newuri, const char *cfg[])
{
	AE_DATA_SOURCE *dsrc;
	AE_DECL_RET;
	const char *p, *t;

	/* The target type must match the source type. */
	for (p = uri, t = newuri; *p == *t && *p != ':'; ++p, ++t)
		;
	if (*p != ':' || *t != ':')
		AE_RET_MSG(session, EINVAL,
		    "rename target type must match URI: %s to %s", uri, newuri);

	/*
	 * We track rename operations, if we fail in the middle, we want to
	 * back it all out.
	 */
	AE_RET(__ae_meta_track_on(session));

	if (AE_PREFIX_MATCH(uri, "file:"))
		ret = __rename_file(session, uri, newuri);
	else if (AE_PREFIX_MATCH(uri, "lsm:"))
		ret = __ae_lsm_tree_rename(session, uri, newuri, cfg);
	else if (AE_PREFIX_MATCH(uri, "table:"))
		ret = __rename_table(session, uri, newuri, cfg);
	else if ((dsrc = __ae_schema_get_source(session, uri)) != NULL)
		ret = dsrc->rename == NULL ?
		    __ae_object_unsupported(session, uri) :
		    dsrc->rename(dsrc,
		    &session->iface, uri, newuri, (AE_CONFIG_ARG *)cfg);
	else
		ret = __ae_bad_object_type(session, uri);

	/* Bump the schema generation so that stale data is ignored. */
	++S2C(session)->schema_gen;

	AE_TRET(__ae_meta_track_off(session, true, ret != 0));

	/* If we didn't find a metadata entry, map that error to ENOENT. */
	return (ret == AE_NOTFOUND ? ENOENT : ret);
}
