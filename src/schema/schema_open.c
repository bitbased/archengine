/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_schema_colgroup_name --
 *	Get the URI for a column group.  This is used for metadata lookups.
 *	The only complexity here is that simple tables (with a single column
 *	group) use a simpler naming scheme.
 */
int
__ae_schema_colgroup_name(AE_SESSION_IMPL *session,
    AE_TABLE *table, const char *cgname, size_t len, AE_ITEM *buf)
{
	const char *tablename;

	tablename = table->name;
	(void)AE_PREFIX_SKIP(tablename, "table:");

	return ((table->ncolgroups == 0) ?
	    __ae_buf_fmt(session, buf, "colgroup:%s", tablename) :
	    __ae_buf_fmt(session, buf, "colgroup:%s:%.*s",
	    tablename, (int)len, cgname));
}

/*
 * __ae_schema_open_colgroups --
 *	Open the column groups for a table.
 */
int
__ae_schema_open_colgroups(AE_SESSION_IMPL *session, AE_TABLE *table)
{
	AE_COLGROUP *colgroup;
	AE_CONFIG cparser;
	AE_CONFIG_ITEM ckey, cval;
	AE_DECL_RET;
	AE_DECL_ITEM(buf);
	char *cgconfig;
	u_int i;

	AE_ASSERT(session, F_ISSET(session, AE_SESSION_LOCKED_TABLE));

	if (table->cg_complete)
		return (0);

	colgroup = NULL;
	cgconfig = NULL;

	AE_RET(__ae_scr_alloc(session, 0, &buf));

	AE_ERR(__ae_config_subinit(session, &cparser, &table->cgconf));

	/* Open each column group. */
	for (i = 0; i < AE_COLGROUPS(table); i++) {
		if (table->ncolgroups > 0)
			AE_ERR(__ae_config_next(&cparser, &ckey, &cval));
		else
			AE_CLEAR(ckey);

		/*
		 * Always open from scratch: we may have failed part of the way
		 * through opening a table, or column groups may have changed.
		 */
		__ae_schema_destroy_colgroup(session, &table->cgroups[i]);

		AE_ERR(__ae_buf_init(session, buf, 0));
		AE_ERR(__ae_schema_colgroup_name(session, table,
		    ckey.str, ckey.len, buf));
		if ((ret = __ae_metadata_search(
		    session, buf->data, &cgconfig)) != 0) {
			/* It is okay if the table is incomplete. */
			if (ret == AE_NOTFOUND)
				ret = 0;
			goto err;
		}

		AE_ERR(__ae_calloc_one(session, &colgroup));
		AE_ERR(__ae_strndup(
		    session, buf->data, buf->size, &colgroup->name));
		colgroup->config = cgconfig;
		cgconfig = NULL;
		AE_ERR(__ae_config_getones(session,
		    colgroup->config, "columns", &colgroup->colconf));
		AE_ERR(__ae_config_getones(
		    session, colgroup->config, "source", &cval));
		AE_ERR(__ae_strndup(
		    session, cval.str, cval.len, &colgroup->source));
		table->cgroups[i] = colgroup;
		colgroup = NULL;
	}

	if (!table->is_simple) {
		AE_ERR(__ae_table_check(session, table));

		AE_ERR(__ae_buf_init(session, buf, 0));
		AE_ERR(__ae_struct_plan(session,
		    table, table->colconf.str, table->colconf.len, true, buf));
		AE_ERR(__ae_strndup(
		    session, buf->data, buf->size, &table->plan));
	}

	table->cg_complete = true;

err:	__ae_scr_free(session, &buf);
	__ae_schema_destroy_colgroup(session, &colgroup);
	if (cgconfig != NULL)
		__ae_free(session, cgconfig);
	return (ret);
}

/*
 * __open_index --
 *	Open an index.
 */
static int
__open_index(AE_SESSION_IMPL *session, AE_TABLE *table, AE_INDEX *idx)
{
	AE_CONFIG colconf;
	AE_CONFIG_ITEM ckey, cval, metadata;
	AE_DECL_ITEM(buf);
	AE_DECL_ITEM(plan);
	AE_DECL_RET;
	u_int npublic_cols, i;

	AE_ERR(__ae_scr_alloc(session, 0, &buf));

	/* Get the data source from the index config. */
	AE_ERR(__ae_config_getones(session, idx->config, "source", &cval));
	AE_ERR(__ae_strndup(session, cval.str, cval.len, &idx->source));

	AE_ERR(__ae_config_getones(session, idx->config, "immutable", &cval));
	if (cval.val)
		F_SET(idx, AE_INDEX_IMMUTABLE);

	/*
	 * Compatibility: we didn't always maintain collator information in
	 * index metadata, cope when it isn't found.
	 */
	AE_CLEAR(cval);
	AE_ERR_NOTFOUND_OK(__ae_config_getones(
	    session, idx->config, "collator", &cval));
	if (cval.len != 0) {
		AE_CLEAR(metadata);
		AE_ERR_NOTFOUND_OK(__ae_config_getones(
		    session, idx->config, "app_metadata", &metadata));
		AE_ERR(__ae_collator_config(
		    session, idx->name, &cval, &metadata,
		    &idx->collator, &idx->collator_owned));
	}

	AE_ERR(__ae_extractor_config(
	    session, idx->name, idx->config, &idx->extractor,
	    &idx->extractor_owned));

	AE_ERR(__ae_config_getones(session, idx->config, "key_format", &cval));
	AE_ERR(__ae_strndup(session, cval.str, cval.len, &idx->key_format));

	/*
	 * The key format for an index is somewhat subtle: the application
	 * specifies a set of columns that it will use for the key, but the
	 * engine usually adds some hidden columns in order to derive the
	 * primary key.  These hidden columns are part of the file's key.
	 *
	 * The file's key_format is stored persistently, we need to calculate
	 * the index cursor key format (which will usually omit some of those
	 * keys).
	 */
	AE_ERR(__ae_buf_init(session, buf, 0));
	AE_ERR(__ae_config_getones(
	    session, idx->config, "columns", &idx->colconf));

	/* Start with the declared index columns. */
	AE_ERR(__ae_config_subinit(session, &colconf, &idx->colconf));
	for (npublic_cols = 0;
	    (ret = __ae_config_next(&colconf, &ckey, &cval)) == 0;
	    ++npublic_cols)
		AE_ERR(__ae_buf_catfmt(
		    session, buf, "%.*s,", (int)ckey.len, ckey.str));
	if (ret != AE_NOTFOUND)
		goto err;

	/*
	 * If we didn't find any columns, the index must have an extractor.
	 * We don't rely on this unconditionally because it was only added to
	 * the metadata after version 2.3.1.
	 */
	if (npublic_cols == 0) {
		AE_ERR(__ae_config_getones(
		    session, idx->config, "index_key_columns", &cval));
		npublic_cols = (u_int)cval.val;
		AE_ASSERT(session, npublic_cols != 0);
		for (i = 0; i < npublic_cols; i++)
			AE_ERR(__ae_buf_catfmt(session, buf, "\"bad col\","));
	}

	/*
	 * Now add any primary key columns from the table that are not
	 * already part of the index key.
	 */
	AE_ERR(__ae_config_subinit(session, &colconf, &table->colconf));
	for (i = 0; i < table->nkey_columns &&
	    (ret = __ae_config_next(&colconf, &ckey, &cval)) == 0;
	    i++) {
		/*
		 * If the primary key column is already in the secondary key,
		 * don't add it again.
		 */
		if (__ae_config_subgetraw(
		    session, &idx->colconf, &ckey, &cval) == 0)
			continue;
		AE_ERR(__ae_buf_catfmt(
		    session, buf, "%.*s,", (int)ckey.len, ckey.str));
	}
	AE_ERR_NOTFOUND_OK(ret);

	/*
	 * If the table doesn't yet have its column groups, don't try to
	 * calculate a plan: we are just checking that the index creation is
	 * sane.
	 */
	if (!table->cg_complete)
		goto err;

	AE_ERR(__ae_scr_alloc(session, 0, &plan));
	AE_ERR(__ae_struct_plan(
	    session, table, buf->data, buf->size, false, plan));
	AE_ERR(__ae_strndup(session, plan->data, plan->size, &idx->key_plan));

	/* Set up the cursor key format (the visible columns). */
	AE_ERR(__ae_buf_init(session, buf, 0));
	AE_ERR(__ae_struct_truncate(session,
	    idx->key_format, npublic_cols, buf));
	AE_ERR(__ae_strndup(
	    session, buf->data, buf->size, &idx->idxkey_format));

	/*
	 * Add a trailing padding byte to the format.  This ensures that there
	 * will be no special optimization of the last column, so the primary
	 * key columns can be simply appended.
	 */
	AE_ERR(__ae_buf_catfmt(session, buf, "x"));
	AE_ERR(__ae_strndup(session, buf->data, buf->size, &idx->exkey_format));

	/* By default, index cursor values are the table value columns. */
	/* TODO Optimize to use index columns in preference to table lookups. */
	AE_ERR(__ae_buf_init(session, plan, 0));
	AE_ERR(__ae_struct_plan(session,
	    table, table->colconf.str, table->colconf.len, true, plan));
	AE_ERR(__ae_strndup(session, plan->data, plan->size, &idx->value_plan));

err:	__ae_scr_free(session, &buf);
	__ae_scr_free(session, &plan);
	return (ret);
}

/*
 * __schema_open_index --
 *	Open one or more indices for a table (internal version).
 */
static int
__schema_open_index(AE_SESSION_IMPL *session,
    AE_TABLE *table, const char *idxname, size_t len, AE_INDEX **indexp)
{
	AE_CURSOR *cursor;
	AE_DECL_ITEM(tmp);
	AE_DECL_RET;
	AE_INDEX *idx;
	u_int i;
	int cmp;
	bool match;
	const char *idxconf, *name, *tablename, *uri;

	/* Check if we've already done the work. */
	if (idxname == NULL && table->idx_complete)
		return (0);

	cursor = NULL;
	idx = NULL;
	match = false;

	/* Build a search key. */
	tablename = table->name;
	(void)AE_PREFIX_SKIP(tablename, "table:");
	AE_ERR(__ae_scr_alloc(session, 512, &tmp));
	AE_ERR(__ae_buf_fmt(session, tmp, "index:%s:", tablename));

	/* Find matching indices. */
	AE_ERR(__ae_metadata_cursor(session, NULL, &cursor));
	cursor->set_key(cursor, tmp->data);
	if ((ret = cursor->search_near(cursor, &cmp)) == 0 && cmp < 0)
		ret = cursor->next(cursor);
	for (i = 0; ret == 0; i++, ret = cursor->next(cursor)) {
		AE_ERR(cursor->get_key(cursor, &uri));
		name = uri;
		if (!AE_PREFIX_SKIP(name, tmp->data))
			break;

		/* Is this the index we are looking for? */
		match = idxname == NULL || AE_STRING_MATCH(name, idxname, len);

		/*
		 * Ensure there is space, including if we have to make room for
		 * a new entry in the middle of the list.
		 */
		AE_ERR(__ae_realloc_def(session, &table->idx_alloc,
		    AE_MAX(i, table->nindices) + 1, &table->indices));

		/* Keep the in-memory list in sync with the metadata. */
		cmp = 0;
		while (table->indices[i] != NULL &&
		    (cmp = strcmp(uri, table->indices[i]->name)) > 0) {
			/* Index no longer exists, remove it. */
			__ae_free(session, table->indices[i]);
			memmove(&table->indices[i], &table->indices[i + 1],
			    (table->nindices - i) * sizeof(AE_INDEX *));
			table->indices[--table->nindices] = NULL;
		}
		if (cmp < 0) {
			/* Make room for a new index. */
			memmove(&table->indices[i + 1], &table->indices[i],
			    (table->nindices - i) * sizeof(AE_INDEX *));
			table->indices[i] = NULL;
			++table->nindices;
		}

		if (!match)
			continue;

		if (table->indices[i] == NULL) {
			AE_ERR(cursor->get_value(cursor, &idxconf));
			AE_ERR(__ae_calloc_one(session, &idx));
			AE_ERR(__ae_strdup(session, uri, &idx->name));
			AE_ERR(__ae_strdup(session, idxconf, &idx->config));
			AE_ERR(__open_index(session, table, idx));

			/*
			 * If we're checking the creation of an index before a
			 * table is fully created, don't save the index: it
			 * will need to be reopened once the table is complete.
			 */
			if (!table->cg_complete) {
				AE_ERR(
				    __ae_schema_destroy_index(session, &idx));
				if (idxname != NULL)
					break;
				continue;
			}

			table->indices[i] = idx;
			idx = NULL;

			/*
			 * If the slot is bigger than anything else we've seen,
			 * bump the number of indices.
			 */
			if (i >= table->nindices)
				table->nindices = i + 1;
		}

		/* If we were looking for a single index, we're done. */
		if (indexp != NULL)
			*indexp = table->indices[i];
		if (idxname != NULL)
			break;
	}
	AE_ERR_NOTFOUND_OK(ret);
	if (idxname != NULL && !match)
		ret = AE_NOTFOUND;

	/* If we did a full pass, we won't need to do it again. */
	if (idxname == NULL) {
		table->nindices = i;
		table->idx_complete = true;
	}

err:	__ae_scr_free(session, &tmp);
	AE_TRET(__ae_schema_destroy_index(session, &idx));
	if (cursor != NULL)
		AE_TRET(cursor->close(cursor));
	return (ret);
}

/*
 * __ae_schema_open_index --
 *	Open one or more indices for a table.
 */
int
__ae_schema_open_index(AE_SESSION_IMPL *session,
    AE_TABLE *table, const char *idxname, size_t len, AE_INDEX **indexp)
{
	AE_DECL_RET;

	AE_WITH_TXN_ISOLATION(session, AE_ISO_READ_UNCOMMITTED,
	    ret = __schema_open_index(session, table, idxname, len, indexp));
	return (ret);
}

/*
 * __ae_schema_open_indices --
 *	Open the indices for a table.
 */
int
__ae_schema_open_indices(AE_SESSION_IMPL *session, AE_TABLE *table)
{
	return (__ae_schema_open_index(session, table, NULL, 0, NULL));
}

/*
 * __schema_open_table --
 *	Open a named table (internal version).
 */
static int
__schema_open_table(AE_SESSION_IMPL *session,
    const char *name, size_t namelen, bool ok_incomplete, AE_TABLE **tablep)
{
	AE_CONFIG cparser;
	AE_CONFIG_ITEM ckey, cval;
	AE_CURSOR *cursor;
	AE_DECL_ITEM(buf);
	AE_DECL_RET;
	AE_TABLE *table;
	const char *tconfig;
	char *tablename;

	cursor = NULL;
	table = NULL;
	tablename = NULL;

	AE_ASSERT(session, F_ISSET(session, AE_SESSION_LOCKED_TABLE));

	AE_ERR(__ae_scr_alloc(session, 0, &buf));
	AE_ERR(__ae_buf_fmt(session, buf, "table:%.*s", (int)namelen, name));
	AE_ERR(__ae_strndup(session, buf->data, buf->size, &tablename));

	AE_ERR(__ae_metadata_cursor(session, NULL, &cursor));
	cursor->set_key(cursor, tablename);
	AE_ERR(cursor->search(cursor));
	AE_ERR(cursor->get_value(cursor, &tconfig));

	AE_ERR(__ae_calloc_one(session, &table));
	table->name = tablename;
	tablename = NULL;
	table->name_hash = __ae_hash_city64(name, namelen);

	AE_ERR(__ae_config_getones(session, tconfig, "columns", &cval));

	AE_ERR(__ae_config_getones(session, tconfig, "key_format", &cval));
	AE_ERR(__ae_strndup(session, cval.str, cval.len, &table->key_format));
	AE_ERR(__ae_config_getones(session, tconfig, "value_format", &cval));
	AE_ERR(__ae_strndup(session, cval.str, cval.len, &table->value_format));
	AE_ERR(__ae_strdup(session, tconfig, &table->config));

	/* Point to some items in the copy to save re-parsing. */
	AE_ERR(__ae_config_getones(session, table->config,
	    "columns", &table->colconf));

	/*
	 * Count the number of columns: tables are "simple" if the columns
	 * are not named.
	 */
	AE_ERR(__ae_config_subinit(session, &cparser, &table->colconf));
	table->is_simple = true;
	while ((ret = __ae_config_next(&cparser, &ckey, &cval)) == 0)
		table->is_simple = false;
	if (ret != AE_NOTFOUND)
		goto err;

	/* Check that the columns match the key and value formats. */
	if (!table->is_simple)
		AE_ERR(__ae_schema_colcheck(session,
		    table->key_format, table->value_format, &table->colconf,
		    &table->nkey_columns, NULL));

	AE_ERR(__ae_config_getones(session, table->config,
	    "colgroups", &table->cgconf));

	/* Count the number of column groups. */
	AE_ERR(__ae_config_subinit(session, &cparser, &table->cgconf));
	table->ncolgroups = 0;
	while ((ret = __ae_config_next(&cparser, &ckey, &cval)) == 0)
		++table->ncolgroups;
	if (ret != AE_NOTFOUND)
		goto err;

	if (table->ncolgroups > 0 && table->is_simple)
		AE_ERR_MSG(session, EINVAL,
		    "%s requires a table with named columns", tablename);

	AE_ERR(__ae_calloc_def(session, AE_COLGROUPS(table), &table->cgroups));
	AE_ERR(__ae_schema_open_colgroups(session, table));

	if (!ok_incomplete && !table->cg_complete)
		AE_ERR_MSG(session, EINVAL, "'%s' cannot be used "
		    "until all column groups are created",
		    table->name);

	/* Copy the schema generation into the new table. */
	table->schema_gen = S2C(session)->schema_gen;

	*tablep = table;

	if (0) {
err:		AE_TRET(__ae_schema_destroy_table(session, &table));
	}
	if (cursor != NULL)
		AE_TRET(cursor->close(cursor));

	__ae_free(session, tablename);
	__ae_scr_free(session, &buf);
	return (ret);
}

/*
 * __ae_schema_get_colgroup --
 *	Find a column group by URI.
 */
int
__ae_schema_get_colgroup(AE_SESSION_IMPL *session,
    const char *uri, bool quiet, AE_TABLE **tablep, AE_COLGROUP **colgroupp)
{
	AE_COLGROUP *colgroup;
	AE_TABLE *table;
	const char *tablename, *tend;
	u_int i;

	*colgroupp = NULL;

	tablename = uri;
	if (!AE_PREFIX_SKIP(tablename, "colgroup:"))
		return (__ae_bad_object_type(session, uri));

	if ((tend = strchr(tablename, ':')) == NULL)
		tend = tablename + strlen(tablename);

	AE_RET(__ae_schema_get_table(session,
	    tablename, AE_PTRDIFF(tend, tablename), false, &table));

	for (i = 0; i < AE_COLGROUPS(table); i++) {
		colgroup = table->cgroups[i];
		if (strcmp(colgroup->name, uri) == 0) {
			*colgroupp = colgroup;
			if (tablep != NULL)
				*tablep = table;
			else
				__ae_schema_release_table(session, table);
			return (0);
		}
	}

	__ae_schema_release_table(session, table);
	if (quiet)
		AE_RET(ENOENT);
	AE_RET_MSG(session, ENOENT, "%s not found in table", uri);
}

/*
 * __ae_schema_get_index --
 *	Find an index by URI.
 */
int
__ae_schema_get_index(AE_SESSION_IMPL *session,
    const char *uri, bool quiet, AE_TABLE **tablep, AE_INDEX **indexp)
{
	AE_DECL_RET;
	AE_INDEX *idx;
	AE_TABLE *table;
	const char *tablename, *tend;
	u_int i;

	*indexp = NULL;

	tablename = uri;
	if (!AE_PREFIX_SKIP(tablename, "index:") ||
	    (tend = strchr(tablename, ':')) == NULL)
		return (__ae_bad_object_type(session, uri));

	AE_RET(__ae_schema_get_table(session,
	    tablename, AE_PTRDIFF(tend, tablename), false, &table));

	/* Try to find the index in the table. */
	for (i = 0; i < table->nindices; i++) {
		idx = table->indices[i];
		if (idx != NULL && strcmp(idx->name, uri) == 0) {
			if (tablep != NULL)
				*tablep = table;
			else
				__ae_schema_release_table(session, table);
			*indexp = idx;
			return (0);
		}
	}

	/* Otherwise, open it. */
	AE_ERR(__ae_schema_open_index(
	    session, table, tend + 1, strlen(tend + 1), indexp));
	if (tablep != NULL)
		*tablep = table;

err:	__ae_schema_release_table(session, table);
	AE_RET(ret);

	if (*indexp != NULL)
		return (0);

	if (quiet)
		AE_RET(ENOENT);
	AE_RET_MSG(session, ENOENT, "%s not found in table", uri);
}

/*
 * __ae_schema_open_table --
 *	Open a named table.
 */
int
__ae_schema_open_table(AE_SESSION_IMPL *session,
    const char *name, size_t namelen, bool ok_incomplete, AE_TABLE **tablep)
{
	AE_DECL_RET;

	AE_WITH_TXN_ISOLATION(session, AE_ISO_READ_UNCOMMITTED,
	    ret = __schema_open_table(
	    session, name, namelen, ok_incomplete, tablep));
	return (ret);
}
