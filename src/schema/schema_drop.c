/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __drop_file --
 *	Drop a file.
 */
static int
__drop_file(
    AE_SESSION_IMPL *session, const char *uri, bool force, const char *cfg[])
{
	AE_CONFIG_ITEM cval;
	AE_DECL_RET;
	bool remove_files;
	const char *filename;

	AE_RET(__ae_config_gets(session, cfg, "remove_files", &cval));
	remove_files = cval.val != 0;

	filename = uri;
	if (!AE_PREFIX_SKIP(filename, "file:"))
		return (EINVAL);

	/* Close all btree handles associated with this file. */
	AE_WITH_HANDLE_LIST_LOCK(session,
	    ret = __ae_conn_dhandle_close_all(session, uri, force));
	AE_RET(ret);

	/* Remove the metadata entry (ignore missing items). */
	AE_TRET(__ae_metadata_remove(session, uri));
	if (!remove_files)
		return (ret);

	/*
	 * Schedule the remove of the underlying physical file when the drop
	 * completes.
	 */
	AE_TRET(__ae_meta_track_drop(session, filename));

	return (ret);
}

/*
 * __drop_colgroup --
 *	AE_SESSION::drop for a colgroup.
 */
static int
__drop_colgroup(
    AE_SESSION_IMPL *session, const char *uri, bool force, const char *cfg[])
{
	AE_COLGROUP *colgroup;
	AE_DECL_RET;
	AE_TABLE *table;

	AE_ASSERT(session, F_ISSET(session, AE_SESSION_LOCKED_TABLE));

	/* If we can get the colgroup, detach it from the table. */
	if ((ret = __ae_schema_get_colgroup(
	    session, uri, force, &table, &colgroup)) == 0) {
		table->cg_complete = false;
		AE_TRET(__ae_schema_drop(session, colgroup->source, cfg));
	}

	AE_TRET(__ae_metadata_remove(session, uri));
	return (ret);
}

/*
 * __drop_index --
 *	AE_SESSION::drop for a colgroup.
 */
static int
__drop_index(
    AE_SESSION_IMPL *session, const char *uri, bool force, const char *cfg[])
{
	AE_INDEX *idx;
	AE_DECL_RET;
	AE_TABLE *table;

	/* If we can get the colgroup, detach it from the table. */
	if ((ret = __ae_schema_get_index(
	    session, uri, force, &table, &idx)) == 0) {
		table->idx_complete = false;
		AE_TRET(__ae_schema_drop(session, idx->source, cfg));
	}

	AE_TRET(__ae_metadata_remove(session, uri));
	return (ret);
}

/*
 * __drop_table --
 *	AE_SESSION::drop for a table.
 */
static int
__drop_table(AE_SESSION_IMPL *session, const char *uri, const char *cfg[])
{
	AE_COLGROUP *colgroup;
	AE_DECL_RET;
	AE_INDEX *idx;
	AE_TABLE *table;
	const char *name;
	u_int i;

	name = uri;
	(void)AE_PREFIX_SKIP(name, "table:");

	table = NULL;
	AE_ERR(__ae_schema_get_table(
	    session, name, strlen(name), true, &table));

	/* Drop the column groups. */
	for (i = 0; i < AE_COLGROUPS(table); i++) {
		if ((colgroup = table->cgroups[i]) == NULL)
			continue;
		/*
		 * Drop the column group before updating the metadata to avoid
		 * the metadata for the table becoming inconsistent if we can't
		 * get exclusive access.
		 */
		AE_ERR(__ae_schema_drop(session, colgroup->source, cfg));
		AE_ERR(__ae_metadata_remove(session, colgroup->name));
	}

	/* Drop the indices. */
	AE_ERR(__ae_schema_open_indices(session, table));
	for (i = 0; i < table->nindices; i++) {
		if ((idx = table->indices[i]) == NULL)
			continue;
		/*
		 * Drop the column group before updating the metadata to avoid
		 * the metadata for the table becoming inconsistent if we can't
		 * get exclusive access.
		 */
		AE_ERR(__ae_schema_drop(session, idx->source, cfg));
		AE_ERR(__ae_metadata_remove(session, idx->name));
	}

	AE_ERR(__ae_schema_remove_table(session, table));
	table = NULL;

	/* Remove the metadata entry (ignore missing items). */
	AE_ERR(__ae_metadata_remove(session, uri));

err:	if (table != NULL)
		__ae_schema_release_table(session, table);
	return (ret);
}

/*
 * __ae_schema_drop --
 *	Process a AE_SESSION::drop operation for all supported types.
 */
int
__ae_schema_drop(AE_SESSION_IMPL *session, const char *uri, const char *cfg[])
{
	AE_CONFIG_ITEM cval;
	AE_DATA_SOURCE *dsrc;
	AE_DECL_RET;
	bool force;

	AE_RET(__ae_config_gets_def(session, cfg, "force", 0, &cval));
	force = cval.val != 0;

	AE_RET(__ae_meta_track_on(session));

	/* Paranoia: clear any handle from our caller. */
	session->dhandle = NULL;

	if (AE_PREFIX_MATCH(uri, "colgroup:"))
		ret = __drop_colgroup(session, uri, force, cfg);
	else if (AE_PREFIX_MATCH(uri, "file:"))
		ret = __drop_file(session, uri, force, cfg);
	else if (AE_PREFIX_MATCH(uri, "index:"))
		ret = __drop_index(session, uri, force, cfg);
	else if (AE_PREFIX_MATCH(uri, "lsm:"))
		ret = __ae_lsm_tree_drop(session, uri, cfg);
	else if (AE_PREFIX_MATCH(uri, "table:"))
		ret = __drop_table(session, uri, cfg);
	else if ((dsrc = __ae_schema_get_source(session, uri)) != NULL)
		ret = dsrc->drop == NULL ?
		    __ae_object_unsupported(session, uri) :
		    dsrc->drop(
		    dsrc, &session->iface, uri, (AE_CONFIG_ARG *)cfg);
	else
		ret = __ae_bad_object_type(session, uri);

	/*
	 * Map AE_NOTFOUND to ENOENT, based on the assumption AE_NOTFOUND means
	 * there was no metadata entry.  Map ENOENT to zero if force is set.
	 */
	if (ret == AE_NOTFOUND || ret == ENOENT)
		ret = force ? 0 : ENOENT;

	/* Bump the schema generation so that stale data is ignored. */
	++S2C(session)->schema_gen;

	AE_TRET(__ae_meta_track_off(session, true, ret != 0));

	return (ret);
}
