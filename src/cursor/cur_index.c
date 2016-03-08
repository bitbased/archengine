/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

 /*
 * __ae_curindex_joined --
 *	Produce an error that this cursor is being used in a join call.
 */
int
__ae_curindex_joined(AE_CURSOR *cursor)
{
	AE_SESSION_IMPL *session;

	session = (AE_SESSION_IMPL *)cursor->session;
	__ae_errx(session, "index cursor is being used in a join");
	return (ENOTSUP);
}

/*
 * __curindex_get_value --
 *	AE_CURSOR->get_value implementation for index cursors.
 */
static int
__curindex_get_value(AE_CURSOR *cursor, ...)
{
	AE_DECL_RET;
	AE_SESSION_IMPL *session;
	va_list ap;

	va_start(ap, cursor);
	JOINABLE_CURSOR_API_CALL(cursor, session, get_value, NULL);
	AE_ERR(__ae_curindex_get_valuev(cursor, ap));

err:	va_end(ap);
	API_END_RET(session, ret);
}

/*
 * __curindex_set_value --
 *	AE_CURSOR->set_value implementation for index cursors.
 */
static void
__curindex_set_value(AE_CURSOR *cursor, ...)
{
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	JOINABLE_CURSOR_API_CALL(cursor, session, set_value, NULL);
	ret = ENOTSUP;
err:	cursor->saved_err = ret;
	F_CLR(cursor, AE_CURSTD_VALUE_SET);
	API_END(session, ret);
}

/*
 * __curindex_compare --
 *	AE_CURSOR->compare method for the index cursor type.
 */
static int
__curindex_compare(AE_CURSOR *a, AE_CURSOR *b, int *cmpp)
{
	AE_CURSOR_INDEX *cindex;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	cindex = (AE_CURSOR_INDEX *)a;
	JOINABLE_CURSOR_API_CALL(a, session, compare, NULL);

	/* Check both cursors are "index:" type. */
	if (!AE_PREFIX_MATCH(a->uri, "index:") ||
	    strcmp(a->uri, b->uri) != 0)
		AE_ERR_MSG(session, EINVAL,
		    "Cursors must reference the same object");

	AE_CURSOR_CHECKKEY(a);
	AE_CURSOR_CHECKKEY(b);

	ret = __ae_compare(
	    session, cindex->index->collator, &a->key, &b->key, cmpp);

err:	API_END_RET(session, ret);
}

/*
 * __curindex_move --
 *	When an index cursor changes position, set the primary key in the
 *	associated column groups and update their positions to match.
 */
static int
__curindex_move(AE_CURSOR_INDEX *cindex)
{
	AE_CURSOR **cp, *first;
	AE_SESSION_IMPL *session;
	u_int i;

	session = (AE_SESSION_IMPL *)cindex->iface.session;
	first = NULL;

	/* Point the public cursor to the key in the child. */
	__ae_cursor_set_raw_key(&cindex->iface, &cindex->child->key);
	F_CLR(&cindex->iface, AE_CURSTD_KEY_SET | AE_CURSTD_VALUE_SET);

	for (i = 0, cp = cindex->cg_cursors;
	    i < AE_COLGROUPS(cindex->table);
	    i++, cp++) {
		if (*cp == NULL)
			continue;
		if (first == NULL) {
			/*
			 * Set the primary key -- note that we need the primary
			 * key columns, so we have to use the full key format,
			 * not just the public columns.
			 */
			AE_RET(__ae_schema_project_slice(session,
			    cp, cindex->index->key_plan,
			    1, cindex->index->key_format,
			    &cindex->iface.key));
			first = *cp;
		} else {
			(*cp)->key.data = first->key.data;
			(*cp)->key.size = first->key.size;
			(*cp)->recno = first->recno;
		}
		F_SET(*cp, AE_CURSTD_KEY_EXT);
		if (cindex->cg_needvalue[i])
			AE_RET((*cp)->search(*cp));
	}

	F_SET(&cindex->iface, AE_CURSTD_KEY_INT | AE_CURSTD_VALUE_INT);
	return (0);
}

/*
 * __curindex_next --
 *	AE_CURSOR->next method for index cursors.
 */
static int
__curindex_next(AE_CURSOR *cursor)
{
	AE_CURSOR_INDEX *cindex;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	cindex = (AE_CURSOR_INDEX *)cursor;
	JOINABLE_CURSOR_API_CALL(cursor, session, next, NULL);
	F_CLR(cursor, AE_CURSTD_KEY_SET | AE_CURSTD_VALUE_SET);

	if ((ret = cindex->child->next(cindex->child)) == 0)
		ret = __curindex_move(cindex);

err:	API_END_RET(session, ret);
}

/*
 * __curindex_prev --
 *	AE_CURSOR->prev method for index cursors.
 */
static int
__curindex_prev(AE_CURSOR *cursor)
{
	AE_CURSOR_INDEX *cindex;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	cindex = (AE_CURSOR_INDEX *)cursor;
	JOINABLE_CURSOR_API_CALL(cursor, session, prev, NULL);
	F_CLR(cursor, AE_CURSTD_KEY_SET | AE_CURSTD_VALUE_SET);

	if ((ret = cindex->child->prev(cindex->child)) == 0)
		ret = __curindex_move(cindex);

err:	API_END_RET(session, ret);
}

/*
 * __curindex_reset --
 *	AE_CURSOR->reset method for index cursors.
 */
static int
__curindex_reset(AE_CURSOR *cursor)
{
	AE_CURSOR **cp;
	AE_CURSOR_INDEX *cindex;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;
	u_int i;

	cindex = (AE_CURSOR_INDEX *)cursor;
	JOINABLE_CURSOR_API_CALL(cursor, session, reset, NULL);
	F_CLR(cursor, AE_CURSTD_KEY_SET | AE_CURSTD_VALUE_SET);

	AE_TRET(cindex->child->reset(cindex->child));
	for (i = 0, cp = cindex->cg_cursors;
	    i < AE_COLGROUPS(cindex->table);
	    i++, cp++) {
		if (*cp == NULL)
			continue;
		AE_TRET((*cp)->reset(*cp));
	}

err:	API_END_RET(session, ret);
}

/*
 * __curindex_search --
 *	AE_CURSOR->search method for index cursors.
 */
static int
__curindex_search(AE_CURSOR *cursor)
{
	AE_CURSOR *child;
	AE_CURSOR_INDEX *cindex;
	AE_DECL_RET;
	AE_ITEM found_key;
	AE_SESSION_IMPL *session;
	int cmp;

	cindex = (AE_CURSOR_INDEX *)cursor;
	child = cindex->child;
	JOINABLE_CURSOR_API_CALL(cursor, session, search, NULL);

	/*
	 * We are searching using the application-specified key, which
	 * (usually) doesn't contain the primary key, so it is just a prefix of
	 * any matching index key.  Do a search_near, step to the next entry if
	 * we land on one that is too small, then check that the prefix
	 * matches.
	 */
	__ae_cursor_set_raw_key(child, &cursor->key);
	AE_ERR(child->search_near(child, &cmp));

	if (cmp < 0)
		AE_ERR(child->next(child));

	/*
	 * We expect partial matches, and want the smallest record with a key
	 * greater than or equal to the search key.
	 *
	 * If the key we find is shorter than the search key, it can't possibly
	 * match.
	 *
	 * The only way for the key to be exactly equal is if there is an index
	 * on the primary key, because otherwise the primary key columns will
	 * be appended to the index key, but we don't disallow that (odd) case.
	 */
	found_key = child->key;
	if (found_key.size < cursor->key.size)
		AE_ERR(AE_NOTFOUND);
	found_key.size = cursor->key.size;

	AE_ERR(__ae_compare(
	    session, cindex->index->collator, &cursor->key, &found_key, &cmp));
	if (cmp != 0) {
		ret = AE_NOTFOUND;
		goto err;
	}

	AE_ERR(__curindex_move(cindex));

	if (0) {
err:		F_CLR(cursor, AE_CURSTD_KEY_INT | AE_CURSTD_VALUE_INT);
	}

	API_END_RET(session, ret);
}

/*
 * __curindex_search_near --
 *	AE_CURSOR->search_near method for index cursors.
 */
static int
__curindex_search_near(AE_CURSOR *cursor, int *exact)
{
	AE_CURSOR_INDEX *cindex;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	cindex = (AE_CURSOR_INDEX *)cursor;
	JOINABLE_CURSOR_API_CALL(cursor, session, search_near, NULL);
	__ae_cursor_set_raw_key(cindex->child, &cursor->key);
	if ((ret = cindex->child->search_near(cindex->child, exact)) == 0)
		ret = __curindex_move(cindex);
	else
		F_CLR(cursor, AE_CURSTD_KEY_INT | AE_CURSTD_VALUE_INT);

err:	API_END_RET(session, ret);
}

/*
 * __curindex_close --
 *	AE_CURSOR->close method for index cursors.
 */
static int
__curindex_close(AE_CURSOR *cursor)
{
	AE_CURSOR_INDEX *cindex;
	AE_CURSOR **cp;
	AE_DECL_RET;
	AE_INDEX *idx;
	AE_SESSION_IMPL *session;
	u_int i;

	cindex = (AE_CURSOR_INDEX *)cursor;
	idx = cindex->index;

	JOINABLE_CURSOR_API_CALL(cursor, session, close, NULL);

	if ((cp = cindex->cg_cursors) != NULL)
		for (i = 0, cp = cindex->cg_cursors;
		    i < AE_COLGROUPS(cindex->table); i++, cp++)
			if (*cp != NULL) {
				AE_TRET((*cp)->close(*cp));
				*cp = NULL;
			}

	__ae_free(session, cindex->cg_needvalue);
	__ae_free(session, cindex->cg_cursors);
	if (cindex->key_plan != idx->key_plan)
		__ae_free(session, cindex->key_plan);
	if (cursor->value_format != cindex->table->value_format)
		__ae_free(session, cursor->value_format);
	if (cindex->value_plan != idx->value_plan)
		__ae_free(session, cindex->value_plan);

	if (cindex->child != NULL)
		AE_TRET(cindex->child->close(cindex->child));

	__ae_schema_release_table(session, cindex->table);
	/* The URI is owned by the index. */
	cursor->internal_uri = NULL;
	AE_TRET(__ae_cursor_close(cursor));

err:	API_END_RET(session, ret);
}

/*
 * __curindex_open_colgroups --
 *	Open cursors on the column groups required for an index cursor.
 */
static int
__curindex_open_colgroups(
    AE_SESSION_IMPL *session, AE_CURSOR_INDEX *cindex, const char *cfg_arg[])
{
	AE_TABLE *table;
	AE_CURSOR **cp;
	u_long arg;
	/* Child cursors are opened with dump disabled. */
	const char *cfg[] = { cfg_arg[0], cfg_arg[1], "dump=\"\"", NULL };
	char *proj;
	size_t cgcnt;

	table = cindex->table;
	cgcnt = AE_COLGROUPS(table);
	AE_RET(__ae_calloc_def(session, cgcnt, &cindex->cg_needvalue));
	AE_RET(__ae_calloc_def(session, cgcnt, &cp));
	cindex->cg_cursors = cp;

	/* Work out which column groups we need. */
	for (proj = (char *)cindex->value_plan; *proj != '\0'; proj++) {
		arg = strtoul(proj, &proj, 10);
		if (*proj == AE_PROJ_VALUE)
			cindex->cg_needvalue[arg] = 1;
		if ((*proj != AE_PROJ_KEY && *proj != AE_PROJ_VALUE) ||
		    cp[arg] != NULL)
			continue;
		AE_RET(__ae_open_cursor(session,
		    table->cgroups[arg]->source,
		    &cindex->iface, cfg, &cp[arg]));
	}

	return (0);
}

/*
 * __ae_curindex_open --
 *	AE_SESSION->open_cursor method for index cursors.
 */
int
__ae_curindex_open(AE_SESSION_IMPL *session,
    const char *uri, AE_CURSOR *owner, const char *cfg[], AE_CURSOR **cursorp)
{
	AE_CURSOR_STATIC_INIT(iface,
	    __ae_cursor_get_key,	/* get-key */
	    __curindex_get_value,	/* get-value */
	    __ae_cursor_set_key,	/* set-key */
	    __curindex_set_value,	/* set-value */
	    __curindex_compare,		/* compare */
	    __ae_cursor_equals,		/* equals */
	    __curindex_next,		/* next */
	    __curindex_prev,		/* prev */
	    __curindex_reset,		/* reset */
	    __curindex_search,		/* search */
	    __curindex_search_near,	/* search-near */
	    __ae_cursor_notsup,		/* insert */
	    __ae_cursor_notsup,		/* update */
	    __ae_cursor_notsup,		/* remove */
	    __ae_cursor_notsup,		/* reconfigure */
	    __curindex_close);		/* close */
	AE_CURSOR_INDEX *cindex;
	AE_CURSOR *cursor;
	AE_DECL_ITEM(tmp);
	AE_DECL_RET;
	AE_INDEX *idx;
	AE_TABLE *table;
	const char *columns, *idxname, *tablename;
	size_t namesize;

	tablename = uri;
	if (!AE_PREFIX_SKIP(tablename, "index:") ||
	    (idxname = strchr(tablename, ':')) == NULL)
		AE_RET_MSG(session, EINVAL, "Invalid cursor URI: '%s'", uri);
	namesize = (size_t)(idxname - tablename);
	++idxname;

	if ((ret = __ae_schema_get_table(session,
	    tablename, namesize, false, &table)) != 0) {
		if (ret == AE_NOTFOUND)
			AE_RET_MSG(session, EINVAL,
			    "Cannot open cursor '%s' on unknown table", uri);
		return (ret);
	}

	columns = strchr(idxname, '(');
	if (columns == NULL)
		namesize = strlen(idxname);
	else
		namesize = (size_t)(columns - idxname);

	if ((ret = __ae_schema_open_index(
	    session, table, idxname, namesize, &idx)) != 0) {
		__ae_schema_release_table(session, table);
		return (ret);
	}
	AE_RET(__ae_calloc_one(session, &cindex));

	cursor = &cindex->iface;
	*cursor = iface;
	cursor->session = &session->iface;

	cindex->table = table;
	cindex->index = idx;
	cindex->key_plan = idx->key_plan;
	cindex->value_plan = idx->value_plan;

	cursor->internal_uri = idx->name;
	cursor->key_format = idx->idxkey_format;
	cursor->value_format = table->value_format;

	/*
	 * XXX
	 * A very odd corner case is an index with a recno key.
	 * The only way to get here is by creating an index on a column store
	 * using only the primary's recno as the index key.  Disallow that for
	 * now.
	 */
	if (AE_CURSOR_RECNO(cursor))
		AE_ERR_MSG(session, AE_ERROR,
		    "Column store indexes based on a record number primary "
		    "key are not supported.");

	/* Handle projections. */
	if (columns != NULL) {
		AE_ERR(__ae_scr_alloc(session, 0, &tmp));
		AE_ERR(__ae_struct_reformat(session, table,
		    columns, strlen(columns), NULL, false, tmp));
		AE_ERR(__ae_strndup(
		    session, tmp->data, tmp->size, &cursor->value_format));

		AE_ERR(__ae_buf_init(session, tmp, 0));
		AE_ERR(__ae_struct_plan(session, table,
		    columns, strlen(columns), false, tmp));
		AE_ERR(__ae_strndup(
		    session, tmp->data, tmp->size, &cindex->value_plan));
	}

	AE_ERR(__ae_cursor_init(
	    cursor, cursor->internal_uri, owner, cfg, cursorp));

	AE_ERR(__ae_open_cursor(
	    session, idx->source, cursor, cfg, &cindex->child));

	/* Open the column groups needed for this index cursor. */
	AE_ERR(__curindex_open_colgroups(session, cindex, cfg));

	if (F_ISSET(cursor, AE_CURSTD_DUMP_JSON))
		AE_ERR(__ae_json_column_init(cursor, table->key_format,
			&idx->colconf, &table->colconf));

	if (0) {
err:		AE_TRET(__curindex_close(cursor));
		*cursorp = NULL;
	}

	__ae_scr_free(session, &tmp);
	return (ret);
}
