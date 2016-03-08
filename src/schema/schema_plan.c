/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __find_next_col --
 *	Find the next column to use for a plan.
 */
static int
__find_next_col(AE_SESSION_IMPL *session, AE_TABLE *table,
    AE_CONFIG_ITEM *colname, u_int *cgnump, u_int *colnump, char *coltype)
{
	AE_COLGROUP *colgroup;
	AE_CONFIG conf;
	AE_CONFIG_ITEM cval, k, v;
	AE_DECL_RET;
	u_int cg, col, foundcg, foundcol, matchcg, matchcol;
	bool getnext;

	foundcg = foundcol = UINT_MAX;
	matchcg = *cgnump;
	matchcol = (*coltype == AE_PROJ_KEY) ?
	    *colnump : *colnump + table->nkey_columns;

	getnext = true;
	for (colgroup = NULL, cg = 0; cg < AE_COLGROUPS(table); cg++) {
		colgroup = table->cgroups[cg];

		/*
		 * If there is only one column group, we just scan through all
		 * of the columns.  For tables with multiple column groups, we
		 * look at the key columns once, then go through the value
		 * columns for each group.
		 */
		if (cg == 0) {
			cval = table->colconf;
			col = 0;
		} else {
cgcols:			cval = colgroup->colconf;
			col = table->nkey_columns;
		}
		AE_RET(__ae_config_subinit(session, &conf, &cval));
		for (; (ret = __ae_config_next(&conf, &k, &v)) == 0; col++) {
			if (k.len == colname->len &&
			    strncmp(colname->str, k.str, k.len) == 0) {
				if (getnext) {
					foundcg = cg;
					foundcol = col;
				}
				getnext = cg == matchcg && col == matchcol;
			}
			if (cg == 0 && table->ncolgroups > 0 &&
			    col == table->nkey_columns - 1)
				goto cgcols;
		}
		AE_RET_TEST(ret != AE_NOTFOUND, ret);

		colgroup = NULL;
	}

	if (foundcg == UINT_MAX)
		return (AE_NOTFOUND);

	*cgnump = foundcg;
	if (foundcol < table->nkey_columns) {
		*coltype = AE_PROJ_KEY;
		*colnump = foundcol;
	} else {
		*coltype = AE_PROJ_VALUE;
		*colnump = foundcol - table->nkey_columns;
	}
	return (0);
}

/*
 * __ae_schema_colcheck --
 *	Check that a list of columns matches a (key,value) format pair.
 */
int
__ae_schema_colcheck(AE_SESSION_IMPL *session,
    const char *key_format, const char *value_format, AE_CONFIG_ITEM *colconf,
    u_int *kcolsp, u_int *vcolsp)
{
	AE_CONFIG conf;
	AE_CONFIG_ITEM k, v;
	AE_DECL_PACK_VALUE(pv);
	AE_DECL_RET;
	AE_PACK pack;
	u_int kcols, ncols, vcols;

	AE_RET(__pack_init(session, &pack, key_format));
	for (kcols = 0; (ret = __pack_next(&pack, &pv)) == 0; kcols++)
		;
	AE_RET_TEST(ret != AE_NOTFOUND, ret);

	AE_RET(__pack_init(session, &pack, value_format));
	for (vcols = 0; (ret = __pack_next(&pack, &pv)) == 0; vcols++)
		;
	AE_RET_TEST(ret != AE_NOTFOUND, ret);

	/* Walk through the named columns. */
	AE_RET(__ae_config_subinit(session, &conf, colconf));
	for (ncols = 0; (ret = __ae_config_next(&conf, &k, &v)) == 0; ncols++)
		;
	AE_RET_TEST(ret != AE_NOTFOUND, ret);

	if (ncols != 0 && ncols != kcols + vcols)
		AE_RET_MSG(session, EINVAL, "Number of columns in '%.*s' "
		    "does not match key format '%s' plus value format '%s'",
		    (int)colconf->len, colconf->str, key_format, value_format);

	if (kcolsp != NULL)
		*kcolsp = kcols;
	if (vcolsp != NULL)
		*vcolsp = vcols;

	return (0);
}

/*
 * __ae_table_check --
 *	Make sure all columns appear in a column group.
 */
int
__ae_table_check(AE_SESSION_IMPL *session, AE_TABLE *table)
{
	AE_CONFIG conf;
	AE_CONFIG_ITEM k, v;
	AE_DECL_RET;
	u_int cg, col, i;
	char coltype;

	if (table->is_simple)
		return (0);

	/* Walk through the columns. */
	AE_RET(__ae_config_subinit(session, &conf, &table->colconf));

	/* Skip over the key columns. */
	for (i = 0; i < table->nkey_columns; i++)
		AE_RET(__ae_config_next(&conf, &k, &v));
	cg = col = 0;
	coltype = 0;
	while ((ret = __ae_config_next(&conf, &k, &v)) == 0) {
		if (__find_next_col(
		    session, table, &k, &cg, &col, &coltype) != 0)
			AE_RET_MSG(session, EINVAL,
			    "Column '%.*s' in '%s' does not appear in a "
			    "column group",
			    (int)k.len, k.str, table->name);
		/*
		 * Column groups can't store key columns in their value:
		 * __ae_struct_reformat should have already detected this case.
		 */
		AE_ASSERT(session, coltype == AE_PROJ_VALUE);

	}
	AE_RET_TEST(ret != AE_NOTFOUND, ret);

	return (0);
}

/*
 * __ae_struct_plan --
 *	Given a table cursor containing a complete table, build the "projection
 *	plan" to distribute the columns to dependent stores.  A string
 *	representing the plan will be appended to the plan buffer.
 */
int
__ae_struct_plan(AE_SESSION_IMPL *session, AE_TABLE *table,
    const char *columns, size_t len, bool value_only, AE_ITEM *plan)
{
	AE_CONFIG conf;
	AE_CONFIG_ITEM k, v;
	AE_DECL_RET;
	u_int cg, col, current_cg, current_col, i, start_cg, start_col;
	bool have_it;
	char coltype, current_coltype;

	start_cg = start_col = UINT_MAX;	/* -Wuninitialized */

	/* Work through the value columns by skipping over the key columns. */
	AE_RET(__ae_config_initn(session, &conf, columns, len));
	if (value_only)
		for (i = 0; i < table->nkey_columns; i++)
			AE_RET(__ae_config_next(&conf, &k, &v));

	current_cg = cg = 0;
	current_col = col = INT_MAX;
	current_coltype = coltype = AE_PROJ_KEY; /* Keep lint quiet. */
	for (i = 0; (ret = __ae_config_next(&conf, &k, &v)) == 0; i++) {
		have_it = false;

		while ((ret = __find_next_col(session, table,
		    &k, &cg, &col, &coltype)) == 0 &&
		    (!have_it || cg != start_cg || col != start_col)) {
			/*
			 * First we move to the column.  If that is in a
			 * different column group to the last column we
			 * accessed, or before the last column in the same
			 * column group, or moving from the key to the value,
			 * we need to switch column groups or rewind.
			 */
			if (current_cg != cg || current_col > col ||
			    current_coltype != coltype) {
				AE_ASSERT(session, !value_only ||
				    coltype == AE_PROJ_VALUE);
				AE_RET(__ae_buf_catfmt(
				    session, plan, "%d%c", cg, coltype));

				/*
				 * Set the current column group and column
				 * within the table.
				 */
				current_cg = cg;
				current_col = 0;
				current_coltype = coltype;
			}
			/* Now move to the column we want. */
			if (current_col < col) {
				if (col - current_col > 1)
					AE_RET(__ae_buf_catfmt(session,
					    plan, "%d", col - current_col));
				AE_RET(__ae_buf_catfmt(session,
				    plan, "%c", AE_PROJ_SKIP));
			}
			/*
			 * Now copy the value in / out.  In the common case,
			 * where each value is used in one column, we do a
			 * "next" operation.  If the value is used again, we do
			 * a "reuse" operation to avoid making another copy.
			 */
			if (!have_it) {
				AE_RET(__ae_buf_catfmt(session,
				    plan, "%c", AE_PROJ_NEXT));

				start_cg = cg;
				start_col = col;
				have_it = true;
			} else
				AE_RET(__ae_buf_catfmt(session,
				    plan, "%c", AE_PROJ_REUSE));
			current_col = col + 1;
		}
		/*
		 * We may fail to find a column if it is a custom extractor.
		 * In that case, treat it as the first value column: we only
		 * ever use such plans to extract the primary key from the
		 * index.
		 */
		if (ret == AE_NOTFOUND)
			AE_RET(__ae_buf_catfmt(session, plan,
			    "0%c%c", AE_PROJ_VALUE, AE_PROJ_NEXT));
	}
	AE_RET_TEST(ret != AE_NOTFOUND, ret);

	/* Special case empty plans. */
	if (i == 0 && plan->size == 0)
		AE_RET(__ae_buf_set(session, plan, "", 1));

	return (0);
}

/*
 * __find_column_format --
 *	Find the format of the named column.
 */
static int
__find_column_format(AE_SESSION_IMPL *session, AE_TABLE *table,
    AE_CONFIG_ITEM *colname, bool value_only, AE_PACK_VALUE *pv)
{
	AE_CONFIG conf;
	AE_CONFIG_ITEM k, v;
	AE_DECL_RET;
	AE_PACK pack;
	bool inkey;

	AE_RET(__ae_config_subinit(session, &conf, &table->colconf));
	AE_RET(__pack_init(session, &pack, table->key_format));
	inkey = true;

	while ((ret = __ae_config_next(&conf, &k, &v)) == 0) {
		if ((ret = __pack_next(&pack, pv)) == AE_NOTFOUND && inkey) {
			ret = __pack_init(session, &pack, table->value_format);
			if (ret == 0)
				ret = __pack_next(&pack, pv);
			inkey = false;
		}
		if (ret != 0)
			return (ret);

		if (k.len == colname->len &&
		    strncmp(colname->str, k.str, k.len) == 0) {
			if (value_only && inkey)
				return (EINVAL);
			return (0);
		}
	}

	return (ret);
}

/*
 * __ae_struct_reformat --
 *	Given a table and a list of columns (which could be values in a column
 *	group or index keys), calculate the resulting new format string.
 *	The result will be appended to the format buffer.
 */
int
__ae_struct_reformat(AE_SESSION_IMPL *session, AE_TABLE *table,
    const char *columns, size_t len, const char *extra_cols, bool value_only,
    AE_ITEM *format)
{
	AE_CONFIG config;
	AE_CONFIG_ITEM k, next_k, next_v;
	AE_DECL_PACK_VALUE(pv);
	AE_DECL_RET;
	bool have_next;

	AE_RET(__ae_config_initn(session, &config, columns, len));
	/*
	 * If an empty column list is specified, this will fail with
	 * AE_NOTFOUND, that's okay.
	 */
	AE_RET_NOTFOUND_OK(ret = __ae_config_next(&config, &next_k, &next_v));
	if (ret == AE_NOTFOUND) {
		if (extra_cols != NULL) {
			AE_RET(__ae_config_init(session, &config, extra_cols));
			AE_RET(__ae_config_next(&config, &next_k, &next_v));
			extra_cols = NULL;
		} else if (format->size == 0) {
			AE_RET(__ae_buf_set(session, format, "", 1));
			return (0);
		}
	}
	do {
		k = next_k;
		ret = __ae_config_next(&config, &next_k, &next_v);
		if (ret != 0 && ret != AE_NOTFOUND)
			return (ret);
		have_next = ret == 0;

		if (!have_next && extra_cols != NULL) {
			AE_RET(__ae_config_init(session, &config, extra_cols));
			AE_RET(__ae_config_next(&config, &next_k, &next_v));
			have_next = true;
			extra_cols = NULL;
		}

		if ((ret = __find_column_format(session,
		    table, &k, value_only, &pv)) != 0) {
			if (value_only && ret == EINVAL)
				AE_RET_MSG(session, EINVAL,
				    "A column group cannot store key column "
				    "'%.*s' in its value", (int)k.len, k.str);
			AE_RET_MSG(session, EINVAL,
			    "Column '%.*s' not found", (int)k.len, k.str);
		}

		/*
		 * Check whether we're moving an unsized AE_ITEM from the end
		 * to the middle, or vice-versa.  This determines whether the
		 * size needs to be prepended.  This is the only case where the
		 * destination size can be larger than the source size.
		 */
		if (pv.type == 'u' && !pv.havesize && have_next)
			pv.type = 'U';
		else if (pv.type == 'U' && !have_next)
			pv.type = 'u';

		if (pv.havesize)
			AE_RET(__ae_buf_catfmt(
			    session, format, "%d%c", (int)pv.size, pv.type));
		else
			AE_RET(__ae_buf_catfmt(session, format, "%c", pv.type));
	} while (have_next);

	return (0);
}

/*
 * __ae_struct_truncate --
 *	Return a packing string for the first N columns in a value.
 */
int
__ae_struct_truncate(AE_SESSION_IMPL *session,
    const char *input_fmt, u_int ncols, AE_ITEM *format)
{
	AE_DECL_PACK_VALUE(pv);
	AE_PACK pack;

	AE_RET(__pack_init(session, &pack, input_fmt));
	while (ncols-- > 0) {
		AE_RET(__pack_next(&pack, &pv));
		if (pv.havesize)
			AE_RET(__ae_buf_catfmt(
			    session, format, "%d%c", (int)pv.size, pv.type));
		else
			AE_RET(__ae_buf_catfmt(session, format, "%c", pv.type));
	}

	return (0);
}
