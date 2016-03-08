/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_schema_create_strip --
 *	Discard any configuration information from a schema entry that is not
 * applicable to an session.create call, here for the ae dump command utility,
 * which only wants to dump the schema information needed for load.
 */
int
__ae_schema_create_strip(AE_SESSION_IMPL *session,
    const char *v1, const char *v2, char **value_ret)
{
	const char *cfg[] =
	    { AE_CONFIG_BASE(session, AE_SESSION_create), v1, v2, NULL };

	return (__ae_config_collapse(session, cfg, value_ret));
}

/*
 * __ae_direct_io_size_check --
 *	Return a size from the configuration, complaining if it's insufficient
 * for direct I/O.
 */
int
__ae_direct_io_size_check(AE_SESSION_IMPL *session,
    const char **cfg, const char *config_name, uint32_t *allocsizep)
{
	AE_CONFIG_ITEM cval;
	AE_CONNECTION_IMPL *conn;
	int64_t align;

	*allocsizep = 0;

	conn = S2C(session);

	AE_RET(__ae_config_gets(session, cfg, config_name, &cval));

	/*
	 * This function exists as a place to hang this comment: if direct I/O
	 * is configured, page sizes must be at least as large as any buffer
	 * alignment as well as a multiple of the alignment.  Linux gets unhappy
	 * if you configure direct I/O and then don't do I/O in alignments and
	 * units of its happy place.
	 */
	if (FLD_ISSET(conn->direct_io,
	   AE_FILE_TYPE_CHECKPOINT | AE_FILE_TYPE_DATA)) {
		align = (int64_t)conn->buffer_alignment;
		if (align != 0 && (cval.val < align || cval.val % align != 0))
			AE_RET_MSG(session, EINVAL,
			    "when direct I/O is configured, the %s size must "
			    "be at least as large as the buffer alignment as "
			    "well as a multiple of the buffer alignment",
			    config_name);
	}
	*allocsizep = (uint32_t)cval.val;
	return (0);
}

/*
 * __create_file --
 *	Create a new 'file:' object.
 */
static int
__create_file(AE_SESSION_IMPL *session,
    const char *uri, bool exclusive, const char *config)
{
	AE_DECL_ITEM(val);
	AE_DECL_RET;
	uint32_t allocsize;
	bool is_metadata;
	const char *filename, **p, *filecfg[] =
	    { AE_CONFIG_BASE(session, file_meta), config, NULL, NULL };
	char *fileconf;

	fileconf = NULL;

	is_metadata = strcmp(uri, AE_METAFILE_URI) == 0;

	filename = uri;
	if (!AE_PREFIX_SKIP(filename, "file:"))
		AE_RET_MSG(session, EINVAL, "Expected a 'file:' URI: %s", uri);

	/* Check if the file already exists. */
	if (!is_metadata && (ret =
	    __ae_metadata_search(session, uri, &fileconf)) != AE_NOTFOUND) {
		if (exclusive)
			AE_TRET(EEXIST);
		goto err;
	}

	/* Sanity check the allocation size. */
	AE_RET(__ae_direct_io_size_check(
	    session, filecfg, "allocation_size", &allocsize));

	/* Create the file. */
	AE_ERR(__ae_block_manager_create(session, filename, allocsize));
	if (AE_META_TRACKING(session))
		AE_ERR(__ae_meta_track_fileop(session, NULL, uri));

	/*
	 * If creating an ordinary file, append the file ID and current version
	 * numbers to the passed-in configuration and insert the resulting
	 * configuration into the metadata.
	 */
	if (!is_metadata) {
		AE_ERR(__ae_scr_alloc(session, 0, &val));
		AE_ERR(__ae_buf_fmt(session, val,
		    "id=%" PRIu32 ",version=(major=%d,minor=%d)",
		    ++S2C(session)->next_file_id,
		    AE_BTREE_MAJOR_VERSION_MAX, AE_BTREE_MINOR_VERSION_MAX));
		for (p = filecfg; *p != NULL; ++p)
			;
		*p = val->data;
		AE_ERR(__ae_config_collapse(session, filecfg, &fileconf));
		AE_ERR(__ae_metadata_insert(session, uri, fileconf));
	}

	/*
	 * Open the file to check that it was setup correctly. We don't need to
	 * pass the configuration, we just wrote the collapsed configuration
	 * into the metadata file, and it's going to be read/used by underlying
	 * functions.
	 *
	 * Keep the handle exclusive until it is released at the end of the
	 * call, otherwise we could race with a drop.
	 */
	AE_ERR(__ae_session_get_btree(
	    session, uri, NULL, NULL, AE_DHANDLE_EXCLUSIVE));
	if (AE_META_TRACKING(session))
		AE_ERR(__ae_meta_track_handle_lock(session, true));
	else
		AE_ERR(__ae_session_release_btree(session));

err:	__ae_scr_free(session, &val);
	__ae_free(session, fileconf);
	return (ret);
}

/*
 * __ae_schema_colgroup_source --
 *	Get the URI of the data source for a column group.
 */
int
__ae_schema_colgroup_source(AE_SESSION_IMPL *session,
    AE_TABLE *table, const char *cgname, const char *config, AE_ITEM *buf)
{
	AE_CONFIG_ITEM cval;
	AE_DECL_RET;
	size_t len;
	const char *prefix, *suffix, *tablename;

	tablename = table->name + strlen("table:");
	if ((ret = __ae_config_getones(session, config, "type", &cval)) == 0 &&
	    !AE_STRING_MATCH("file", cval.str, cval.len)) {
		prefix = cval.str;
		len = cval.len;
		suffix = "";
	} else {
		prefix = "file";
		len = strlen(prefix);
		suffix = ".ae";
	}
	AE_RET_NOTFOUND_OK(ret);

	if (cgname == NULL)
		AE_RET(__ae_buf_fmt(session, buf, "%.*s:%s%s",
		    (int)len, prefix, tablename, suffix));
	else
		AE_RET(__ae_buf_fmt(session, buf, "%.*s:%s_%s%s",
		    (int)len, prefix, tablename, cgname, suffix));

	return (0);
}

/*
 * __create_colgroup --
 *	Create a column group.
 */
static int
__create_colgroup(AE_SESSION_IMPL *session,
    const char *name, bool exclusive, const char *config)
{
	AE_CONFIG_ITEM cval;
	AE_DECL_RET;
	AE_ITEM confbuf, fmt, namebuf;
	AE_TABLE *table;
	size_t tlen;
	const char **cfgp, *cfg[4] =
	    { AE_CONFIG_BASE(session, colgroup_meta), config, NULL, NULL };
	const char *sourcecfg[] = { config, NULL, NULL };
	const char *cgname, *source, *sourceconf, *tablename;
	char *cgconf, *oldconf;

	sourceconf = NULL;
	cgconf = oldconf = NULL;
	AE_CLEAR(fmt);
	AE_CLEAR(confbuf);
	AE_CLEAR(namebuf);

	tablename = name;
	if (!AE_PREFIX_SKIP(tablename, "colgroup:"))
		return (EINVAL);
	cgname = strchr(tablename, ':');
	if (cgname != NULL) {
		tlen = (size_t)(cgname - tablename);
		++cgname;
	} else
		tlen = strlen(tablename);

	if ((ret =
	    __ae_schema_get_table(session, tablename, tlen, true, &table)) != 0)
		AE_RET_MSG(session, (ret == AE_NOTFOUND) ? ENOENT : ret,
		    "Can't create '%s' for non-existent table '%.*s'",
		    name, (int)tlen, tablename);

	/* Make sure the column group is referenced from the table. */
	if (cgname != NULL && (ret =
	    __ae_config_subgets(session, &table->cgconf, cgname, &cval)) != 0)
		AE_ERR_MSG(session, EINVAL,
		    "Column group '%s' not found in table '%.*s'",
		    cgname, (int)tlen, tablename);

	/* Find the first NULL entry in the cfg stack. */
	for (cfgp = &cfg[1]; *cfgp; cfgp++)
		;

	/* Add the source to the colgroup config before collapsing. */
	if (__ae_config_getones(
	    session, config, "source", &cval) == 0 && cval.len != 0) {
		AE_ERR(__ae_buf_fmt(
		    session, &namebuf, "%.*s", (int)cval.len, cval.str));
		source = namebuf.data;
	} else {
		AE_ERR(__ae_schema_colgroup_source(
		    session, table, cgname, config, &namebuf));
		source = namebuf.data;
		AE_ERR(__ae_buf_fmt(
		    session, &confbuf, "source=\"%s\"", source));
		*cfgp++ = confbuf.data;
	}

	/* Calculate the key/value formats: these go into the source config. */
	AE_ERR(__ae_buf_fmt(session, &fmt, "key_format=%s", table->key_format));
	if (cgname == NULL)
		AE_ERR(__ae_buf_catfmt
		    (session, &fmt, ",value_format=%s", table->value_format));
	else {
		if (__ae_config_getones(session, config, "columns", &cval) != 0)
			AE_ERR_MSG(session, EINVAL,
			    "No 'columns' configuration for '%s'", name);
		AE_ERR(__ae_buf_catfmt(session, &fmt, ",value_format="));
		AE_ERR(__ae_struct_reformat(session,
		    table, cval.str, cval.len, NULL, true, &fmt));
	}
	sourcecfg[1] = fmt.data;
	AE_ERR(__ae_config_merge(session, sourcecfg, NULL, &sourceconf));

	AE_ERR(__ae_schema_create(session, source, sourceconf));

	AE_ERR(__ae_config_collapse(session, cfg, &cgconf));
	if ((ret = __ae_metadata_insert(session, name, cgconf)) != 0) {
		/*
		 * If the entry already exists in the metadata, we're done.
		 * This is an error for exclusive creates but okay otherwise.
		 */
		if (ret == AE_DUPLICATE_KEY)
			ret = exclusive ? EEXIST : 0;
		goto err;
	}

	AE_ERR(__ae_schema_open_colgroups(session, table));

err:	__ae_free(session, cgconf);
	__ae_free(session, sourceconf);
	__ae_free(session, oldconf);
	__ae_buf_free(session, &confbuf);
	__ae_buf_free(session, &fmt);
	__ae_buf_free(session, &namebuf);

	__ae_schema_release_table(session, table);
	return (ret);
}

/*
 * __ae_schema_index_source --
 *	Get the URI of the data source for an index.
 */
int
__ae_schema_index_source(AE_SESSION_IMPL *session,
    AE_TABLE *table, const char *idxname, const char *config, AE_ITEM *buf)
{
	AE_CONFIG_ITEM cval;
	AE_DECL_RET;
	size_t len;
	const char *prefix, *suffix, *tablename;

	tablename = table->name + strlen("table:");
	if ((ret = __ae_config_getones(session, config, "type", &cval)) == 0 &&
	    !AE_STRING_MATCH("file", cval.str, cval.len)) {
		prefix = cval.str;
		len = cval.len;
		suffix = "_idx";
	} else {
		prefix = "file";
		len = strlen(prefix);
		suffix = ".aei";
	}
	AE_RET_NOTFOUND_OK(ret);

	AE_RET(__ae_buf_fmt(session, buf, "%.*s:%s_%s%s",
	    (int)len, prefix, tablename, idxname, suffix));

	return (0);
}

/*
 * __fill_index --
 *	Fill the index from the current contents of the table.
 */
static int
__fill_index(AE_SESSION_IMPL *session, AE_TABLE *table, AE_INDEX *idx)
{
	AE_DECL_RET;
	AE_CURSOR *tcur, *icur;
	AE_SESSION *ae_session;

	ae_session = &session->iface;
	tcur = NULL;
	icur = NULL;
	AE_RET(__ae_schema_open_colgroups(session, table));

	/*
	 * If the column groups have not been completely created,
	 * there cannot be data inserted yet, and we're done.
	 */
	if (!table->cg_complete)
		return (0);

	AE_ERR(ae_session->open_cursor(ae_session,
	    idx->source, NULL, "bulk=unordered", &icur));
	AE_ERR(ae_session->open_cursor(ae_session,
	    table->name, NULL, "readonly", &tcur));

	while ((ret = tcur->next(tcur)) == 0)
		AE_ERR(__ae_apply_single_idx(session, idx,
		    icur, (AE_CURSOR_TABLE *)tcur, icur->insert));

	AE_ERR_NOTFOUND_OK(ret);
err:
	if (icur)
		AE_TRET(icur->close(icur));
	if (tcur)
		AE_TRET(tcur->close(tcur));
	return (ret);
}

/*
 * __create_index --
 *	Create an index.
 */
static int
__create_index(AE_SESSION_IMPL *session,
    const char *name, bool exclusive, const char *config)
{
	AE_CONFIG kcols, pkcols;
	AE_CONFIG_ITEM ckey, cval, icols, kval;
	AE_DECL_PACK_VALUE(pv);
	AE_DECL_RET;
	AE_INDEX *idx;
	AE_ITEM confbuf, extra_cols, fmt, namebuf;
	AE_PACK pack;
	AE_TABLE *table;
	const char *cfg[4] =
	    { AE_CONFIG_BASE(session, index_meta), NULL, NULL, NULL };
	const char *sourcecfg[] = { config, NULL, NULL };
	const char *source, *sourceconf, *idxname, *tablename;
	char *idxconf;
	size_t tlen;
	bool have_extractor;
	u_int i, npublic_cols;

	sourceconf = NULL;
	idxconf = NULL;
	AE_CLEAR(confbuf);
	AE_CLEAR(fmt);
	AE_CLEAR(extra_cols);
	AE_CLEAR(namebuf);
	have_extractor = false;

	tablename = name;
	if (!AE_PREFIX_SKIP(tablename, "index:"))
		return (EINVAL);
	idxname = strchr(tablename, ':');
	if (idxname == NULL)
		AE_RET_MSG(session, EINVAL, "Invalid index name, "
		    "should be <table name>:<index name>: %s", name);

	tlen = (size_t)(idxname++ - tablename);
	if ((ret =
	    __ae_schema_get_table(session, tablename, tlen, true, &table)) != 0)
		AE_RET_MSG(session, ret,
		    "Can't create an index for a non-existent table: %.*s",
		    (int)tlen, tablename);

	if (table->is_simple)
		AE_RET_MSG(session, EINVAL,
		    "%s requires a table with named columns", name);

	if (__ae_config_getones(session, config, "source", &cval) == 0) {
		AE_ERR(__ae_buf_fmt(session, &namebuf,
		    "%.*s", (int)cval.len, cval.str));
		source = namebuf.data;
	} else {
		AE_ERR(__ae_schema_index_source(
		    session, table, idxname, config, &namebuf));
		source = namebuf.data;

		/* Add the source name to the index config before collapsing. */
		AE_ERR(__ae_buf_catfmt(session, &confbuf,
		    ",source=\"%s\"", source));
	}

	if (__ae_config_getones_none(
	    session, config, "extractor", &cval) == 0 && cval.len != 0) {
		have_extractor = true;
		/* Custom extractors must supply a key format. */
		if ((ret = __ae_config_getones(
		    session, config, "key_format", &kval)) != 0)
			AE_ERR_MSG(session, EINVAL,
			    "%s: custom extractors require a key_format", name);
	}

	/* Calculate the key/value formats. */
	AE_CLEAR(icols);
	if (__ae_config_getones(session, config, "columns", &icols) != 0 &&
	    !have_extractor)
		AE_ERR_MSG(session, EINVAL,
		    "%s: requires 'columns' configuration", name);

	/*
	 * Count the public columns using the declared columns for normal
	 * indices or the key format for custom extractors.
	 */
	npublic_cols = 0;
	if (!have_extractor) {
		AE_ERR(__ae_config_subinit(session, &kcols, &icols));
		while ((ret = __ae_config_next(&kcols, &ckey, &cval)) == 0)
			++npublic_cols;
		AE_ERR_NOTFOUND_OK(ret);
	} else {
		AE_ERR(__pack_initn(session, &pack, kval.str, kval.len));
		while ((ret = __pack_next(&pack, &pv)) == 0)
			++npublic_cols;
		AE_ERR_NOTFOUND_OK(ret);
	}

	/*
	 * The key format for an index is somewhat subtle: the application
	 * specifies a set of columns that it will use for the key, but the
	 * engine usually adds some hidden columns in order to derive the
	 * primary key.  These hidden columns are part of the source's
	 * key_format, which we are calculating now, but not part of an index
	 * cursor's key_format.
	 */
	AE_ERR(__ae_config_subinit(session, &pkcols, &table->colconf));
	for (i = 0; i < table->nkey_columns &&
	    (ret = __ae_config_next(&pkcols, &ckey, &cval)) == 0;
	    i++) {
		/*
		 * If the primary key column is already in the secondary key,
		 * don't add it again.
		 */
		if (__ae_config_subgetraw(session, &icols, &ckey, &cval) == 0) {
			if (have_extractor)
				AE_ERR_MSG(session, EINVAL,
				    "an index with a custom extractor may not "
				    "include primary key columns");
			continue;
		}
		AE_ERR(__ae_buf_catfmt(
		    session, &extra_cols, "%.*s,", (int)ckey.len, ckey.str));
	}
	if (ret != 0 && ret != AE_NOTFOUND)
		goto err;

	/* Index values are empty: all columns are packed into the index key. */
	AE_ERR(__ae_buf_fmt(session, &fmt, "value_format=,key_format="));

	if (have_extractor) {
		AE_ERR(__ae_buf_catfmt(session, &fmt, "%.*s",
		    (int)kval.len, kval.str));
		AE_CLEAR(icols);
	}

	/*
	 * Construct the index key format, or append the primary key columns
	 * for custom extractors.
	 */
	AE_ERR(__ae_struct_reformat(session, table,
	    icols.str, icols.len, (const char *)extra_cols.data, false, &fmt));

	/* Check for a record number index key, which makes no sense. */
	AE_ERR(__ae_config_getones(session, fmt.data, "key_format", &cval));
	if (cval.len == 1 && cval.str[0] == 'r')
		AE_ERR_MSG(session, EINVAL,
		    "column-store index may not use the record number as its "
		    "index key");

	AE_ERR(__ae_buf_catfmt(
	    session, &fmt, ",index_key_columns=%u", npublic_cols));

	sourcecfg[1] = fmt.data;
	AE_ERR(__ae_config_merge(session, sourcecfg, NULL, &sourceconf));

	AE_ERR(__ae_schema_create(session, source, sourceconf));

	cfg[1] = sourceconf;
	cfg[2] = confbuf.data;
	AE_ERR(__ae_config_collapse(session, cfg, &idxconf));
	if ((ret = __ae_metadata_insert(session, name, idxconf)) != 0) {
		/*
		 * If the entry already exists in the metadata, we're done.
		 * This is an error for exclusive creates but okay otherwise.
		 */
		if (ret == AE_DUPLICATE_KEY)
			ret = exclusive ? EEXIST : 0;
		goto err;
	}

	/* Make sure that the configuration is valid. */
	AE_ERR(__ae_schema_open_index(
	    session, table, idxname, strlen(idxname), &idx));

	AE_ERR(__fill_index(session, table, idx));

err:	__ae_free(session, idxconf);
	__ae_free(session, sourceconf);
	__ae_buf_free(session, &confbuf);
	__ae_buf_free(session, &extra_cols);
	__ae_buf_free(session, &fmt);
	__ae_buf_free(session, &namebuf);

	__ae_schema_release_table(session, table);
	return (ret);
}

/*
 * __create_table --
 *	Create a table.
 */
static int
__create_table(AE_SESSION_IMPL *session,
    const char *name, bool exclusive, const char *config)
{
	AE_CONFIG conf;
	AE_CONFIG_ITEM cgkey, cgval, cval;
	AE_DECL_RET;
	AE_TABLE *table;
	const char *cfg[4] =
	    { AE_CONFIG_BASE(session, table_meta), config, NULL, NULL };
	const char *tablename;
	char *tableconf, *cgname;
	size_t cgsize;
	int ncolgroups;

	cgname = NULL;
	table = NULL;
	tableconf = NULL;

	tablename = name;
	if (!AE_PREFIX_SKIP(tablename, "table:"))
		return (EINVAL);

	if ((ret = __ae_schema_get_table(session,
	    tablename, strlen(tablename), false, &table)) == 0) {
		__ae_schema_release_table(session, table);
		return (exclusive ? EEXIST : 0);
	}
	AE_RET_NOTFOUND_OK(ret);

	AE_ERR(__ae_config_gets(session, cfg, "colgroups", &cval));
	AE_ERR(__ae_config_subinit(session, &conf, &cval));
	for (ncolgroups = 0;
	    (ret = __ae_config_next(&conf, &cgkey, &cgval)) == 0;
	    ncolgroups++)
		;
	AE_ERR_NOTFOUND_OK(ret);

	AE_ERR(__ae_config_collapse(session, cfg, &tableconf));
	if ((ret = __ae_metadata_insert(session, name, tableconf)) != 0) {
		/*
		 * If the entry already exists in the metadata, we're done.
		 * This is an error for exclusive creates but okay otherwise.
		 */
		if (ret == AE_DUPLICATE_KEY)
			ret = exclusive ? EEXIST : 0;
		goto err;
	}

	/* Attempt to open the table now to catch any errors. */
	AE_ERR(__ae_schema_get_table(
	    session, tablename, strlen(tablename), true, &table));

	if (ncolgroups == 0) {
		cgsize = strlen("colgroup:") + strlen(tablename) + 1;
		AE_ERR(__ae_calloc_def(session, cgsize, &cgname));
		snprintf(cgname, cgsize, "colgroup:%s", tablename);
		AE_ERR(__create_colgroup(session, cgname, exclusive, config));
	}

	if (0) {
err:		if (table != NULL) {
			AE_TRET(__ae_schema_remove_table(session, table));
			table = NULL;
		}
	}
	if (table != NULL)
		__ae_schema_release_table(session, table);
	__ae_free(session, cgname);
	__ae_free(session, tableconf);
	return (ret);
}

/*
 * __create_data_source --
 *	Create a custom data source.
 */
static int
__create_data_source(AE_SESSION_IMPL *session,
    const char *uri, const char *config, AE_DATA_SOURCE *dsrc)
{
	AE_CONFIG_ITEM cval;
	const char *cfg[] = {
	    AE_CONFIG_BASE(session, AE_SESSION_create), config, NULL };

	/*
	 * Check to be sure the key/value formats are legal: the underlying
	 * data source doesn't have access to the functions that check.
	 */
	AE_RET(__ae_config_gets(session, cfg, "key_format", &cval));
	AE_RET(__ae_struct_confchk(session, &cval));
	AE_RET(__ae_config_gets(session, cfg, "value_format", &cval));
	AE_RET(__ae_struct_confchk(session, &cval));

	/*
	 * User-specified collators aren't supported for data-source objects.
	 */
	if (__ae_config_getones_none(
	    session, config, "collator", &cval) != AE_NOTFOUND && cval.len != 0)
		AE_RET_MSG(session, EINVAL,
		    "AE_DATA_SOURCE objects do not support AE_COLLATOR "
		    "ordering");

	return (dsrc->create(dsrc, &session->iface, uri, (AE_CONFIG_ARG *)cfg));
}

/*
 * __ae_schema_create --
 *	Process a AE_SESSION::create operation for all supported types.
 */
int
__ae_schema_create(
    AE_SESSION_IMPL *session, const char *uri, const char *config)
{
	AE_CONFIG_ITEM cval;
	AE_DATA_SOURCE *dsrc;
	AE_DECL_RET;
	bool exclusive;

	exclusive =
	    __ae_config_getones(session, config, "exclusive", &cval) == 0 &&
	    cval.val != 0;

	/*
	 * We track create operations: if we fail in the middle of creating a
	 * complex object, we want to back it all out.
	 */
	AE_RET(__ae_meta_track_on(session));

	if (AE_PREFIX_MATCH(uri, "colgroup:"))
		ret = __create_colgroup(session, uri, exclusive, config);
	else if (AE_PREFIX_MATCH(uri, "file:"))
		ret = __create_file(session, uri, exclusive, config);
	else if (AE_PREFIX_MATCH(uri, "lsm:"))
		ret = __ae_lsm_tree_create(session, uri, exclusive, config);
	else if (AE_PREFIX_MATCH(uri, "index:"))
		ret = __create_index(session, uri, exclusive, config);
	else if (AE_PREFIX_MATCH(uri, "table:"))
		ret = __create_table(session, uri, exclusive, config);
	else if ((dsrc = __ae_schema_get_source(session, uri)) != NULL)
		ret = dsrc->create == NULL ?
		    __ae_object_unsupported(session, uri) :
		    __create_data_source(session, uri, config, dsrc);
	else
		ret = __ae_bad_object_type(session, uri);

	session->dhandle = NULL;
	AE_TRET(__ae_meta_track_off(session, true, ret != 0));

	return (ret);
}
