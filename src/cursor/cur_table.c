/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

static int __curtable_open_indices(AE_CURSOR_TABLE *ctable);
static int __curtable_update(AE_CURSOR *cursor);

#define	APPLY_CG(ctable, f) do {					\
	AE_CURSOR **__cp;						\
	u_int __i;							\
	for (__i = 0, __cp = ctable->cg_cursors;			\
	    __i < AE_COLGROUPS(ctable->table);				\
	    __i++, __cp++)						\
		AE_TRET((*__cp)->f(*__cp));				\
} while (0)

/* Cursor type for custom extractor callback. */
typedef struct {
	AE_CURSOR iface;
	AE_CURSOR_TABLE *ctable;
	AE_CURSOR *idxc;
	int (*f)(AE_CURSOR *);
} AE_CURSOR_EXTRACTOR;

/*
 * __curextract_insert --
 *	Handle a key produced by a custom extractor.
 */
static int
__curextract_insert(AE_CURSOR *cursor) {
	AE_CURSOR_EXTRACTOR *cextract;
	AE_ITEM *key, ikey, pkey;
	AE_SESSION_IMPL *session;

	cextract = (AE_CURSOR_EXTRACTOR *)cursor;
	session = (AE_SESSION_IMPL *)cursor->session;

	AE_ITEM_SET(ikey, cursor->key);
	/*
	 * We appended a padding byte to the key to avoid rewriting the last
	 * column.  Strip that away here.
	 */
	AE_ASSERT(session, ikey.size > 0);
	--ikey.size;
	AE_RET(__ae_cursor_get_raw_key(cextract->ctable->cg_cursors[0], &pkey));

	/*
	 * We have the index key in the format we need, and all of the primary
	 * key columns are required: just append them.
	 */
	key = &cextract->idxc->key;
	AE_RET(__ae_buf_grow(session, key, ikey.size + pkey.size));
	memcpy((uint8_t *)key->mem, ikey.data, ikey.size);
	memcpy((uint8_t *)key->mem + ikey.size, pkey.data, pkey.size);
	key->size = ikey.size + pkey.size;

	/*
	 * The index key is now set and the value is empty (it starts clear and
	 * is never set).
	 */
	F_SET(cextract->idxc, AE_CURSTD_KEY_EXT | AE_CURSTD_VALUE_EXT);

	/* Call the underlying cursor function to update the index. */
	return (cextract->f(cextract->idxc));
}

/*
 * __ae_apply_single_idx --
 *	Apply an operation to a single index of a table.
 */
int
__ae_apply_single_idx(AE_SESSION_IMPL *session, AE_INDEX *idx,
    AE_CURSOR *cur, AE_CURSOR_TABLE *ctable, int (*f)(AE_CURSOR *))
{
	AE_CURSOR_STATIC_INIT(iface,
	    __ae_cursor_get_key,	/* get-key */
	    __ae_cursor_get_value,	/* get-value */
	    __ae_cursor_set_key,	/* set-key */
	    __ae_cursor_set_value,	/* set-value */
	    __ae_cursor_notsup,		/* compare */
	    __ae_cursor_notsup,		/* equals */
	    __ae_cursor_notsup,		/* next */
	    __ae_cursor_notsup,		/* prev */
	    __ae_cursor_notsup,		/* reset */
	    __ae_cursor_notsup,		/* search */
	    __ae_cursor_notsup,		/* search-near */
	    __curextract_insert,	/* insert */
	    __ae_cursor_notsup,		/* update */
	    __ae_cursor_notsup,		/* reconfigure */
	    __ae_cursor_notsup,		/* remove */
	    __ae_cursor_notsup);	/* close */
	AE_CURSOR_EXTRACTOR extract_cursor;
	AE_DECL_RET;
	AE_ITEM key, value;

	if (idx->extractor) {
		extract_cursor.iface = iface;
		extract_cursor.iface.session = &session->iface;
		extract_cursor.iface.key_format = idx->exkey_format;
		extract_cursor.ctable = ctable;
		extract_cursor.idxc = cur;
		extract_cursor.f = f;

		AE_RET(__ae_cursor_get_raw_key(&ctable->iface, &key));
		AE_RET(__ae_cursor_get_raw_value(&ctable->iface, &value));
		ret = idx->extractor->extract(idx->extractor,
		    &session->iface, &key, &value,
		    &extract_cursor.iface);

		__ae_buf_free(session, &extract_cursor.iface.key);
		AE_RET(ret);
	} else {
		AE_RET(__ae_schema_project_merge(session,
		    ctable->cg_cursors,
		    idx->key_plan, idx->key_format, &cur->key));
		/*
		 * The index key is now set and the value is empty
		 * (it starts clear and is never set).
		 */
		F_SET(cur, AE_CURSTD_KEY_EXT | AE_CURSTD_VALUE_EXT);
		AE_RET(f(cur));
	}
	return (0);
}

/*
 * __apply_idx --
 *	Apply an operation to all indices of a table.
 */
static int
__apply_idx(AE_CURSOR_TABLE *ctable, size_t func_off, bool skip_immutable) {
	AE_CURSOR **cp;
	AE_INDEX *idx;
	AE_SESSION_IMPL *session;
	int (*f)(AE_CURSOR *);
	u_int i;

	cp = ctable->idx_cursors;
	session = (AE_SESSION_IMPL *)ctable->iface.session;

	for (i = 0; i < ctable->table->nindices; i++, cp++) {
		idx = ctable->table->indices[i];
		if (skip_immutable && F_ISSET(idx, AE_INDEX_IMMUTABLE))
			continue;

		f = *(int (**)(AE_CURSOR *))((uint8_t *)*cp + func_off);
		AE_RET(__ae_apply_single_idx(session, idx, *cp, ctable, f));
		AE_RET((*cp)->reset(*cp));
	}

	return (0);
}

/*
 * __ae_curtable_get_key --
 *	AE_CURSOR->get_key implementation for tables.
 */
int
__ae_curtable_get_key(AE_CURSOR *cursor, ...)
{
	AE_CURSOR *primary;
	AE_CURSOR_TABLE *ctable;
	AE_DECL_RET;
	va_list ap;

	ctable = (AE_CURSOR_TABLE *)cursor;
	primary = *ctable->cg_cursors;

	va_start(ap, cursor);
	ret = __ae_cursor_get_keyv(primary, cursor->flags, ap);
	va_end(ap);

	return (ret);
}

/*
 * __ae_curtable_get_value --
 *	AE_CURSOR->get_value implementation for tables.
 */
int
__ae_curtable_get_value(AE_CURSOR *cursor, ...)
{
	AE_DECL_RET;
	AE_SESSION_IMPL *session;
	va_list ap;

	va_start(ap, cursor);
	JOINABLE_CURSOR_API_CALL(cursor, session, get_value, NULL);
	AE_ERR(__ae_curtable_get_valuev(cursor, ap));

err:	va_end(ap);
	API_END_RET(session, ret);
}

/*
 * __ae_curtable_set_key --
 *	AE_CURSOR->set_key implementation for tables.
 */
void
__ae_curtable_set_key(AE_CURSOR *cursor, ...)
{
	AE_CURSOR **cp, *primary;
	AE_CURSOR_TABLE *ctable;
	va_list ap;
	u_int i;

	ctable = (AE_CURSOR_TABLE *)cursor;
	cp = ctable->cg_cursors;
	primary = *cp++;

	va_start(ap, cursor);
	__ae_cursor_set_keyv(primary, cursor->flags, ap);
	va_end(ap);

	if (!F_ISSET(primary, AE_CURSTD_KEY_SET))
		return;

	/* Copy the primary key to the other cursors. */
	for (i = 1; i < AE_COLGROUPS(ctable->table); i++, cp++) {
		(*cp)->recno = primary->recno;
		(*cp)->key.data = primary->key.data;
		(*cp)->key.size = primary->key.size;
		F_SET(*cp, AE_CURSTD_KEY_EXT);
	}
}

/*
 * __ae_curtable_set_value --
 *	AE_CURSOR->set_value implementation for tables.
 */
void
__ae_curtable_set_value(AE_CURSOR *cursor, ...)
{
	AE_CURSOR **cp;
	AE_CURSOR_TABLE *ctable;
	AE_DECL_RET;
	AE_ITEM *item, *tmp;
	AE_SESSION_IMPL *session;
	va_list ap;
	u_int i;

	ctable = (AE_CURSOR_TABLE *)cursor;
	JOINABLE_CURSOR_API_CALL(cursor, session, set_value, NULL);

	va_start(ap, cursor);
	if (F_ISSET(cursor, AE_CURSOR_RAW_OK | AE_CURSTD_DUMP_JSON)) {
		item = va_arg(ap, AE_ITEM *);
		cursor->value.data = item->data;
		cursor->value.size = item->size;
		ret = __ae_schema_project_slice(session,
		    ctable->cg_cursors, ctable->plan, 0,
		    cursor->value_format, &cursor->value);
	} else {
		/*
		 * The user may be passing us pointers returned by get_value
		 * that point into the buffers we are about to update.
		 * Move them aside first.
		 */
		for (i = 0, cp = ctable->cg_cursors;
		    i < AE_COLGROUPS(ctable->table); i++, cp++) {
			item = &(*cp)->value;
			if (F_ISSET(*cp, AE_CURSTD_VALUE_SET) &&
			    AE_DATA_IN_ITEM(item)) {
				ctable->cg_valcopy[i] = *item;
				item->mem = NULL;
				item->memsize = 0;
			}
		}

		ret = __ae_schema_project_in(session,
		    ctable->cg_cursors, ctable->plan, ap);

		for (i = 0, cp = ctable->cg_cursors;
		    i < AE_COLGROUPS(ctable->table); i++, cp++) {
			tmp = &ctable->cg_valcopy[i];
			if (tmp->mem != NULL) {
				item = &(*cp)->value;
				if (item->mem == NULL) {
					item->mem = tmp->mem;
					item->memsize = tmp->memsize;
				} else
					__ae_free(session, tmp->mem);
			}
		}

	}
	va_end(ap);

	for (i = 0, cp = ctable->cg_cursors;
	    i < AE_COLGROUPS(ctable->table); i++, cp++)
		if (ret == 0)
			F_SET(*cp, AE_CURSTD_VALUE_EXT);
		else {
			(*cp)->saved_err = ret;
			F_CLR(*cp, AE_CURSTD_VALUE_SET);
		}

err:	API_END(session, ret);
}

/*
 * __curtable_compare --
 *	AE_CURSOR->compare implementation for tables.
 */
static int
__curtable_compare(AE_CURSOR *a, AE_CURSOR *b, int *cmpp)
{
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	JOINABLE_CURSOR_API_CALL(a, session, compare, NULL);

	/*
	 * Confirm both cursors refer to the same source and have keys, then
	 * call the underlying object's comparison routine.
	 */
	if (strcmp(a->internal_uri, b->internal_uri) != 0)
		AE_ERR_MSG(session, EINVAL,
		    "comparison method cursors must reference the same object");
	AE_CURSOR_CHECKKEY(AE_CURSOR_PRIMARY(a));
	AE_CURSOR_CHECKKEY(AE_CURSOR_PRIMARY(b));

	ret = AE_CURSOR_PRIMARY(a)->compare(
	    AE_CURSOR_PRIMARY(a), AE_CURSOR_PRIMARY(b), cmpp);

err:	API_END_RET(session, ret);
}

/*
 * __curtable_next --
 *	AE_CURSOR->next method for the table cursor type.
 */
static int
__curtable_next(AE_CURSOR *cursor)
{
	AE_CURSOR_TABLE *ctable;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	ctable = (AE_CURSOR_TABLE *)cursor;
	JOINABLE_CURSOR_API_CALL(cursor, session, next, NULL);
	APPLY_CG(ctable, next);

err:	API_END_RET(session, ret);
}

/*
 * __curtable_next_random --
 *	AE_CURSOR->next method for the table cursor type when configured with
 *	next_random.
 */
static int
__curtable_next_random(AE_CURSOR *cursor)
{
	AE_CURSOR *primary, **cp;
	AE_CURSOR_TABLE *ctable;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;
	u_int i;

	ctable = (AE_CURSOR_TABLE *)cursor;
	JOINABLE_CURSOR_API_CALL(cursor, session, next, NULL);
	cp = ctable->cg_cursors;

	/* Split out the first next, it retrieves the random record. */
	primary = *cp++;
	AE_ERR(primary->next(primary));

	/* Fill in the rest of the columns. */
	for (i = 1; i < AE_COLGROUPS(ctable->table); i++, cp++) {
		(*cp)->key.data = primary->key.data;
		(*cp)->key.size = primary->key.size;
		(*cp)->recno = primary->recno;
		F_SET(*cp, AE_CURSTD_KEY_EXT);
		AE_ERR((*cp)->search(*cp));
	}

err:	API_END_RET(session, ret);
}

/*
 * __curtable_prev --
 *	AE_CURSOR->prev method for the table cursor type.
 */
static int
__curtable_prev(AE_CURSOR *cursor)
{
	AE_CURSOR_TABLE *ctable;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	ctable = (AE_CURSOR_TABLE *)cursor;
	JOINABLE_CURSOR_API_CALL(cursor, session, prev, NULL);
	APPLY_CG(ctable, prev);

err:	API_END_RET(session, ret);
}

/*
 * __curtable_reset --
 *	AE_CURSOR->reset method for the table cursor type.
 */
static int
__curtable_reset(AE_CURSOR *cursor)
{
	AE_CURSOR_TABLE *ctable;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	ctable = (AE_CURSOR_TABLE *)cursor;
	JOINABLE_CURSOR_API_CALL(cursor, session, reset, NULL);
	APPLY_CG(ctable, reset);

err:	API_END_RET(session, ret);
}

/*
 * __curtable_search --
 *	AE_CURSOR->search method for the table cursor type.
 */
static int
__curtable_search(AE_CURSOR *cursor)
{
	AE_CURSOR_TABLE *ctable;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	ctable = (AE_CURSOR_TABLE *)cursor;
	JOINABLE_CURSOR_API_CALL(cursor, session, search, NULL);
	APPLY_CG(ctable, search);

err:	API_END_RET(session, ret);
}

/*
 * __curtable_search_near --
 *	AE_CURSOR->search_near method for the table cursor type.
 */
static int
__curtable_search_near(AE_CURSOR *cursor, int *exact)
{
	AE_CURSOR_TABLE *ctable;
	AE_CURSOR *primary, **cp;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;
	u_int i;

	ctable = (AE_CURSOR_TABLE *)cursor;
	JOINABLE_CURSOR_API_CALL(cursor, session, search_near, NULL);
	cp = ctable->cg_cursors;
	primary = *cp;
	AE_ERR(primary->search_near(primary, exact));

	for (i = 1, ++cp; i < AE_COLGROUPS(ctable->table); i++) {
		(*cp)->key.data = primary->key.data;
		(*cp)->key.size = primary->key.size;
		(*cp)->recno = primary->recno;
		F_SET(*cp, AE_CURSTD_KEY_EXT);
		AE_ERR((*cp)->search(*cp));
	}

err:	API_END_RET(session, ret);
}

/*
 * __curtable_insert --
 *	AE_CURSOR->insert method for the table cursor type.
 */
static int
__curtable_insert(AE_CURSOR *cursor)
{
	AE_CURSOR *primary, **cp;
	AE_CURSOR_TABLE *ctable;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;
	uint32_t flag_orig;
	u_int i;

	ctable = (AE_CURSOR_TABLE *)cursor;
	JOINABLE_CURSOR_UPDATE_API_CALL(cursor, session, insert, NULL);
	AE_ERR(__curtable_open_indices(ctable));

	/*
	 * Split out the first insert, it may be allocating a recno.
	 *
	 * If the table has indices, we also need to know whether this record
	 * is replacing an existing record so that the existing index entries
	 * can be removed.  We discover if this is an overwrite by configuring
	 * the primary cursor for no-overwrite, and checking if the insert
	 * detects a duplicate key.
	 */
	cp = ctable->cg_cursors;
	primary = *cp++;

	flag_orig = F_MASK(primary, AE_CURSTD_OVERWRITE);
	if (ctable->table->nindices > 0)
		F_CLR(primary, AE_CURSTD_OVERWRITE);
	ret = primary->insert(primary);

	/*
	 * !!!
	 * AE_CURSOR.insert clears the set internally/externally flags
	 * but doesn't touch the items. We could make a copy each time
	 * for overwrite cursors, but for now we just reset the flags.
	 */
	F_SET(primary, flag_orig | AE_CURSTD_KEY_EXT | AE_CURSTD_VALUE_EXT);

	if (ret == AE_DUPLICATE_KEY && F_ISSET(cursor, AE_CURSTD_OVERWRITE))
		AE_ERR(__curtable_update(cursor));
	else {
		AE_ERR(ret);

		for (i = 1; i < AE_COLGROUPS(ctable->table); i++, cp++) {
			(*cp)->recno = primary->recno;
			AE_ERR((*cp)->insert(*cp));
		}

		AE_ERR(__apply_idx(ctable, offsetof(AE_CURSOR, insert), false));
	}

	/*
	 * AE_CURSOR.insert doesn't leave the cursor positioned, and the
	 * application may want to free the memory used to configure the
	 * insert; don't read that memory again (matching the underlying
	 * file object cursor insert semantics).
	 */
	F_CLR(primary, AE_CURSTD_KEY_SET | AE_CURSTD_VALUE_SET);

err:	CURSOR_UPDATE_API_END(session, ret);

	return (ret);
}

/*
 * __curtable_update --
 *	AE_CURSOR->update method for the table cursor type.
 */
static int
__curtable_update(AE_CURSOR *cursor)
{
	AE_CURSOR_TABLE *ctable;
	AE_DECL_ITEM(value_copy);
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	ctable = (AE_CURSOR_TABLE *)cursor;
	JOINABLE_CURSOR_UPDATE_API_CALL(cursor, session, update, NULL);
	AE_ERR(__curtable_open_indices(ctable));

	/*
	 * If the table has indices, first delete any old index keys, then
	 * update the primary, then insert the new index keys.  This is
	 * complicated by the fact that we need the old value to generate the
	 * old index keys, so we make a temporary copy of the new value.
	 */
	if (ctable->table->nindices > 0) {
		AE_ERR(__ae_scr_alloc(
		    session, ctable->cg_cursors[0]->value.size, &value_copy));
		AE_ERR(__ae_schema_project_merge(session,
		    ctable->cg_cursors, ctable->plan,
		    cursor->value_format, value_copy));
		APPLY_CG(ctable, search);

		/* Remove only if the key exists. */
		if (ret == 0) {
			AE_ERR(__apply_idx(ctable,
			    offsetof(AE_CURSOR, remove), true));
			AE_ERR(__ae_schema_project_slice(session,
			    ctable->cg_cursors, ctable->plan, 0,
			    cursor->value_format, value_copy));
		} else
			AE_ERR_NOTFOUND_OK(ret);
	}

	APPLY_CG(ctable, update);
	AE_ERR(ret);

	if (ctable->table->nindices > 0)
		AE_ERR(__apply_idx(ctable, offsetof(AE_CURSOR, insert), true));

err:	CURSOR_UPDATE_API_END(session, ret);
	__ae_scr_free(session, &value_copy);
	return (ret);
}

/*
 * __curtable_remove --
 *	AE_CURSOR->remove method for the table cursor type.
 */
static int
__curtable_remove(AE_CURSOR *cursor)
{
	AE_CURSOR_TABLE *ctable;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	ctable = (AE_CURSOR_TABLE *)cursor;
	JOINABLE_CURSOR_REMOVE_API_CALL(cursor, session, NULL);
	AE_ERR(__curtable_open_indices(ctable));

	/* Find the old record so it can be removed from indices */
	if (ctable->table->nindices > 0) {
		APPLY_CG(ctable, search);
		AE_ERR(ret);
		AE_ERR(__apply_idx(ctable, offsetof(AE_CURSOR, remove), false));
	}

	APPLY_CG(ctable, remove);

err:	CURSOR_UPDATE_API_END(session, ret);
	return (ret);
}

/*
 * __ae_table_range_truncate --
 *	Truncate of a cursor range, table implementation.
 */
int
__ae_table_range_truncate(AE_CURSOR_TABLE *start, AE_CURSOR_TABLE *stop)
{
	AE_CURSOR *ae_start, *ae_stop;
	AE_CURSOR_TABLE *ctable;
	AE_DECL_ITEM(key);
	AE_DECL_RET;
	AE_ITEM raw;
	AE_SESSION_IMPL *session;
	u_int i;
	int cmp;

	ctable = (start != NULL) ? start : stop;
	session = (AE_SESSION_IMPL *)ctable->iface.session;
	ae_start = &start->iface;
	ae_stop = &stop->iface;

	/* Open any indices. */
	AE_RET(__curtable_open_indices(ctable));
	AE_RET(__ae_scr_alloc(session, 128, &key));
	AE_STAT_FAST_DATA_INCR(session, cursor_truncate);

	/*
	 * Step through the cursor range, removing the index entries.
	 *
	 * If there are indices, copy the key we're using to step through the
	 * cursor range (so we can reset the cursor to its original position),
	 * then remove all of the index records in the truncated range.  Copy
	 * the raw key because the memory is only valid until the cursor moves.
	 */
	if (ctable->table->nindices > 0) {
		if (start == NULL) {
			AE_ERR(__ae_cursor_get_raw_key(ae_stop, &raw));
			AE_ERR(__ae_buf_set(session, key, raw.data, raw.size));

			do {
				APPLY_CG(stop, search);
				AE_ERR(ret);
				AE_ERR(__apply_idx(
				    stop, offsetof(AE_CURSOR, remove), false));
			} while ((ret = ae_stop->prev(ae_stop)) == 0);
			AE_ERR_NOTFOUND_OK(ret);

			__ae_cursor_set_raw_key(ae_stop, key);
			APPLY_CG(stop, search);
		} else {
			AE_ERR(__ae_cursor_get_raw_key(ae_start, &raw));
			AE_ERR(__ae_buf_set(session, key, raw.data, raw.size));

			cmp = -1;
			do {
				APPLY_CG(start, search);
				AE_ERR(ret);
				AE_ERR(__apply_idx(
				    start, offsetof(AE_CURSOR, remove), false));
				if (stop != NULL)
					AE_ERR(ae_start->compare(
					    ae_start, ae_stop,
					    &cmp));
			} while (cmp < 0 &&
			    (ret = ae_start->next(ae_start)) == 0);
			AE_ERR_NOTFOUND_OK(ret);

			__ae_cursor_set_raw_key(ae_start, key);
			APPLY_CG(start, search);
		}
	}

	/* Truncate the column groups. */
	for (i = 0; i < AE_COLGROUPS(ctable->table); i++)
		AE_ERR(__ae_range_truncate(
		    (start == NULL) ? NULL : start->cg_cursors[i],
		    (stop == NULL) ? NULL : stop->cg_cursors[i]));

err:	__ae_scr_free(session, &key);
	return (ret);
}

/*
 * __curtable_close --
 *	AE_CURSOR->close method for the table cursor type.
 */
static int
__curtable_close(AE_CURSOR *cursor)
{
	AE_CURSOR_TABLE *ctable;
	AE_CURSOR **cp;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;
	u_int i;

	ctable = (AE_CURSOR_TABLE *)cursor;
	JOINABLE_CURSOR_API_CALL(cursor, session, close, NULL);

	if (ctable->cg_cursors != NULL)
		for (i = 0, cp = ctable->cg_cursors;
		     i < AE_COLGROUPS(ctable->table); i++, cp++)
			if (*cp != NULL) {
				AE_TRET((*cp)->close(*cp));
				*cp = NULL;
			}

	if (ctable->idx_cursors != NULL)
		for (i = 0, cp = ctable->idx_cursors;
		    i < ctable->table->nindices; i++, cp++)
			if (*cp != NULL) {
				AE_TRET((*cp)->close(*cp));
				*cp = NULL;
			}

	if (ctable->plan != ctable->table->plan)
		__ae_free(session, ctable->plan);
	if (ctable->cfg != NULL) {
		for (i = 0; ctable->cfg[i] != NULL; ++i)
			__ae_free(session, ctable->cfg[i]);
		__ae_free(session, ctable->cfg);
	}
	if (cursor->value_format != ctable->table->value_format)
		__ae_free(session, cursor->value_format);
	__ae_free(session, ctable->cg_cursors);
	__ae_free(session, ctable->cg_valcopy);
	__ae_free(session, ctable->idx_cursors);
	__ae_schema_release_table(session, ctable->table);
	/* The URI is owned by the table. */
	cursor->internal_uri = NULL;
	AE_TRET(__ae_cursor_close(cursor));

err:	API_END_RET(session, ret);
}

/*
 * __curtable_open_colgroups --
 *	Open cursors on column groups for a table cursor.
 */
static int
__curtable_open_colgroups(AE_CURSOR_TABLE *ctable, const char *cfg_arg[])
{
	AE_SESSION_IMPL *session;
	AE_TABLE *table;
	AE_CURSOR **cp;
	/*
	 * Underlying column groups are always opened without dump or readonly,
	 * and only the primary is opened with next_random.
	 */
	const char *cfg[] = {
		cfg_arg[0], cfg_arg[1], "dump=\"\",readonly=0", NULL, NULL
	};
	u_int i;
	bool complete;

	session = (AE_SESSION_IMPL *)ctable->iface.session;
	table = ctable->table;

	/* If the table is incomplete, wait on the table lock and recheck. */
	complete = table->cg_complete;
	if (!complete)
		AE_WITH_TABLE_LOCK(session, complete = table->cg_complete);
	if (!complete)
		AE_RET_MSG(session, EINVAL,
		    "Can't use '%s' until all column groups are created",
		    table->name);

	AE_RET(__ae_calloc_def(session,
	    AE_COLGROUPS(table), &ctable->cg_cursors));
	AE_RET(__ae_calloc_def(session,
	    AE_COLGROUPS(table), &ctable->cg_valcopy));

	for (i = 0, cp = ctable->cg_cursors;
	    i < AE_COLGROUPS(table);
	    i++, cp++) {
		AE_RET(__ae_open_cursor(session, table->cgroups[i]->source,
		    &ctable->iface, cfg, cp));
		cfg[3] = "next_random=false";
	}
	return (0);
}

/*
 * __curtable_open_indices --
 *	Open cursors on indices for a table cursor.
 */
static int
__curtable_open_indices(AE_CURSOR_TABLE *ctable)
{
	AE_CURSOR **cp, *primary;
	AE_SESSION_IMPL *session;
	AE_TABLE *table;
	u_int i;

	session = (AE_SESSION_IMPL *)ctable->iface.session;
	table = ctable->table;

	AE_RET(__ae_schema_open_indices(session, table));
	if (table->nindices == 0 || ctable->idx_cursors != NULL)
		return (0);

	/* Check for bulk cursors. */
	primary = *ctable->cg_cursors;
	if (F_ISSET(primary, AE_CURSTD_BULK))
		AE_RET_MSG(session, ENOTSUP,
		    "Bulk load is not supported for tables with indices");

	AE_RET(__ae_calloc_def(session, table->nindices, &ctable->idx_cursors));
	for (i = 0, cp = ctable->idx_cursors; i < table->nindices; i++, cp++)
		AE_RET(__ae_open_cursor(session, table->indices[i]->source,
		    &ctable->iface, ctable->cfg, cp));
	return (0);
}

/*
 * __ae_curtable_open --
 *	AE_SESSION->open_cursor method for table cursors.
 */
int
__ae_curtable_open(AE_SESSION_IMPL *session,
    const char *uri, AE_CURSOR *owner, const char *cfg[], AE_CURSOR **cursorp)
{
	AE_CURSOR_STATIC_INIT(iface,
	    __ae_curtable_get_key,	/* get-key */
	    __ae_curtable_get_value,	/* get-value */
	    __ae_curtable_set_key,	/* set-key */
	    __ae_curtable_set_value,	/* set-value */
	    __curtable_compare,		/* compare */
	    __ae_cursor_equals,		/* equals */
	    __curtable_next,		/* next */
	    __curtable_prev,		/* prev */
	    __curtable_reset,		/* reset */
	    __curtable_search,		/* search */
	    __curtable_search_near,	/* search-near */
	    __curtable_insert,		/* insert */
	    __curtable_update,		/* update */
	    __curtable_remove,		/* remove */
	    __ae_cursor_reconfigure,	/* reconfigure */
	    __curtable_close);		/* close */
	AE_CONFIG_ITEM cval;
	AE_CURSOR *cursor;
	AE_CURSOR_TABLE *ctable;
	AE_DECL_ITEM(tmp);
	AE_DECL_RET;
	AE_TABLE *table;
	size_t size;
	int cfg_cnt;
	const char *tablename, *columns;

	AE_STATIC_ASSERT(offsetof(AE_CURSOR_TABLE, iface) == 0);

	ctable = NULL;

	tablename = uri;
	if (!AE_PREFIX_SKIP(tablename, "table:"))
		return (EINVAL);
	columns = strchr(tablename, '(');
	if (columns == NULL)
		size = strlen(tablename);
	else
		size = AE_PTRDIFF(columns, tablename);
	AE_RET(__ae_schema_get_table(session, tablename, size, false, &table));

	if (table->is_simple) {
		/* Just return a cursor on the underlying data source. */
		ret = __ae_open_cursor(session,
		    table->cgroups[0]->source, NULL, cfg, cursorp);

		__ae_schema_release_table(session, table);
		return (ret);
	}

	AE_RET(__ae_calloc_one(session, &ctable));

	cursor = &ctable->iface;
	*cursor = iface;
	cursor->session = &session->iface;
	cursor->internal_uri = table->name;
	cursor->key_format = table->key_format;
	cursor->value_format = table->value_format;

	ctable->table = table;
	ctable->plan = table->plan;

	/* Handle projections. */
	AE_ERR(__ae_scr_alloc(session, 0, &tmp));
	if (columns != NULL) {
		AE_ERR(__ae_struct_reformat(session, table,
		    columns, strlen(columns), NULL, true, tmp));
		AE_ERR(__ae_strndup(
		    session, tmp->data, tmp->size, &cursor->value_format));

		AE_ERR(__ae_buf_init(session, tmp, 0));
		AE_ERR(__ae_struct_plan(session, table,
		    columns, strlen(columns), false, tmp));
		AE_ERR(__ae_strndup(
		    session, tmp->data, tmp->size, &ctable->plan));
	}

	/*
	 * random_retrieval
	 * Random retrieval cursors only support next, reset and close.
	 */
	AE_ERR(__ae_config_gets_def(session, cfg, "next_random", 0, &cval));
	if (cval.val != 0) {
		__ae_cursor_set_notsup(cursor);
		cursor->next = __curtable_next_random;
		cursor->reset = __curtable_reset;
	}

	AE_ERR(__ae_cursor_init(
	    cursor, cursor->internal_uri, owner, cfg, cursorp));

	if (F_ISSET(cursor, AE_CURSTD_DUMP_JSON))
		AE_ERR(__ae_json_column_init(cursor, table->key_format,
		    NULL, &table->colconf));

	/*
	 * Open the colgroup cursors immediately: we're going to need them for
	 * any operation.  We defer opening index cursors until we need them
	 * for an update.  Note that this must come after the call to
	 * __ae_cursor_init: the table cursor must already be on the list of
	 * session cursors or we can't work out where to put the colgroup
	 * cursor(s).
	 */
	AE_ERR(__curtable_open_colgroups(ctable, cfg));

	/*
	 * We'll need to squirrel away a copy of the cursor configuration for
	 * if/when we open indices.
	 *
	 * cfg[0] is the baseline configuration for the cursor open and we can
	 * acquire another copy from the configuration structures, so it would
	 * be reasonable not to copy it here: but I'd rather be safe than sorry.
	 *
	 * cfg[1] is the application configuration.
	 *
	 * Underlying indices are always opened without dump or readonly; that
	 * information is appended to cfg[1] so later "fast" configuration calls
	 * (checking only cfg[0] and cfg[1]) work. I don't expect to see more
	 * than two configuration strings here, but it's written to compact into
	 * two configuration strings, a copy of cfg[0] and the rest in cfg[1].
	 */
	AE_ERR(__ae_calloc_def(session, 3, &ctable->cfg));
	AE_ERR(__ae_strdup(session, cfg[0], &ctable->cfg[0]));
	AE_ERR(__ae_buf_set(session, tmp, "", 0));
	for (cfg_cnt = 1; cfg[cfg_cnt] != NULL; ++cfg_cnt)
		AE_ERR(__ae_buf_catfmt(session, tmp, "%s,", cfg[cfg_cnt]));
	AE_ERR(__ae_buf_catfmt(session, tmp, "dump=\"\",readonly=0"));
	AE_ERR(__ae_strdup(session, tmp->data, &ctable->cfg[1]));

	if (0) {
err:		AE_TRET(__curtable_close(cursor));
		*cursorp = NULL;
	}

	__ae_scr_free(session, &tmp);
	return (ret);
}
