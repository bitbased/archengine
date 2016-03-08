/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_cursor_notsup --
 *	Unsupported cursor actions.
 */
int
__ae_cursor_notsup(AE_CURSOR *cursor)
{
	AE_UNUSED(cursor);

	return (ENOTSUP);
}

/*
 * __ae_cursor_noop --
 *	Cursor noop.
 */
int
__ae_cursor_noop(AE_CURSOR *cursor)
{
	AE_UNUSED(cursor);

	return (0);
}

/*
 * __ae_cursor_set_notsup --
 *	Reset the cursor methods to not-supported.
 */
void
__ae_cursor_set_notsup(AE_CURSOR *cursor)
{
	/*
	 * Set all of the cursor methods (except for close and reset), to fail.
	 * Close is unchanged so the cursor can be discarded, reset defaults to
	 * a no-op because session transactional operations reset all of the
	 * cursors in a session, and random cursors shouldn't block transactions
	 * or checkpoints.
	 */
	cursor->compare =
	    (int (*)(AE_CURSOR *, AE_CURSOR *, int *))__ae_cursor_notsup;
	cursor->next = __ae_cursor_notsup;
	cursor->prev = __ae_cursor_notsup;
	cursor->reset = __ae_cursor_noop;
	cursor->search = __ae_cursor_notsup;
	cursor->search_near = (int (*)(AE_CURSOR *, int *))__ae_cursor_notsup;
	cursor->insert = __ae_cursor_notsup;
	cursor->update = __ae_cursor_notsup;
	cursor->remove = __ae_cursor_notsup;
}

/*
 * __ae_cursor_kv_not_set --
 *	Standard error message for key/values not set.
 */
int
__ae_cursor_kv_not_set(AE_CURSOR *cursor, bool key)
{
	AE_SESSION_IMPL *session;

	session = (AE_SESSION_IMPL *)cursor->session;

	AE_RET_MSG(session,
	    cursor->saved_err == 0 ? EINVAL : cursor->saved_err,
	    "requires %s be set", key ? "key" : "value");
}

/*
 * __ae_cursor_get_key --
 *	AE_CURSOR->get_key default implementation.
 */
int
__ae_cursor_get_key(AE_CURSOR *cursor, ...)
{
	AE_DECL_RET;
	va_list ap;

	va_start(ap, cursor);
	ret = __ae_cursor_get_keyv(cursor, cursor->flags, ap);
	va_end(ap);
	return (ret);
}

/*
 * __ae_cursor_set_key --
 *	AE_CURSOR->set_key default implementation.
 */
void
__ae_cursor_set_key(AE_CURSOR *cursor, ...)
{
	va_list ap;

	va_start(ap, cursor);
	__ae_cursor_set_keyv(cursor, cursor->flags, ap);
	va_end(ap);
}

/*
 * __ae_cursor_get_raw_key --
 *	Temporarily force raw mode in a cursor to get a canonical copy of
 * the key.
 */
int
__ae_cursor_get_raw_key(AE_CURSOR *cursor, AE_ITEM *key)
{
	AE_DECL_RET;
	bool raw_set;

	raw_set = F_ISSET(cursor, AE_CURSTD_RAW);
	if (!raw_set)
		F_SET(cursor, AE_CURSTD_RAW);
	ret = cursor->get_key(cursor, key);
	if (!raw_set)
		F_CLR(cursor, AE_CURSTD_RAW);
	return (ret);
}

/*
 * __ae_cursor_set_raw_key --
 *	Temporarily force raw mode in a cursor to set a canonical copy of
 * the key.
 */
void
__ae_cursor_set_raw_key(AE_CURSOR *cursor, AE_ITEM *key)
{
	bool raw_set;

	raw_set = F_ISSET(cursor, AE_CURSTD_RAW);
	if (!raw_set)
		F_SET(cursor, AE_CURSTD_RAW);
	cursor->set_key(cursor, key);
	if (!raw_set)
		F_CLR(cursor, AE_CURSTD_RAW);
}

/*
 * __ae_cursor_get_raw_value --
 *	Temporarily force raw mode in a cursor to get a canonical copy of
 * the value.
 */
int
__ae_cursor_get_raw_value(AE_CURSOR *cursor, AE_ITEM *value)
{
	AE_DECL_RET;
	bool raw_set;

	raw_set = F_ISSET(cursor, AE_CURSTD_RAW);
	if (!raw_set)
		F_SET(cursor, AE_CURSTD_RAW);
	ret = cursor->get_value(cursor, value);
	if (!raw_set)
		F_CLR(cursor, AE_CURSTD_RAW);
	return (ret);
}

/*
 * __ae_cursor_set_raw_value --
 *	Temporarily force raw mode in a cursor to set a canonical copy of
 * the value.
 */
void
__ae_cursor_set_raw_value(AE_CURSOR *cursor, AE_ITEM *value)
{
	bool raw_set;

	raw_set = F_ISSET(cursor, AE_CURSTD_RAW);
	if (!raw_set)
		F_SET(cursor, AE_CURSTD_RAW);
	cursor->set_value(cursor, value);
	if (!raw_set)
		F_CLR(cursor, AE_CURSTD_RAW);
}

/*
 * __ae_cursor_get_keyv --
 *	AE_CURSOR->get_key worker function.
 */
int
__ae_cursor_get_keyv(AE_CURSOR *cursor, uint32_t flags, va_list ap)
{
	AE_DECL_RET;
	AE_ITEM *key;
	AE_SESSION_IMPL *session;
	size_t size;
	const char *fmt;

	CURSOR_API_CALL(cursor, session, get_key, NULL);
	if (!F_ISSET(cursor, AE_CURSTD_KEY_EXT | AE_CURSTD_KEY_INT))
		AE_ERR(__ae_cursor_kv_not_set(cursor, true));

	if (AE_CURSOR_RECNO(cursor)) {
		if (LF_ISSET(AE_CURSTD_RAW)) {
			key = va_arg(ap, AE_ITEM *);
			key->data = cursor->raw_recno_buf;
			AE_ERR(__ae_struct_size(
			    session, &size, "q", cursor->recno));
			key->size = size;
			ret = __ae_struct_pack(session, cursor->raw_recno_buf,
			    sizeof(cursor->raw_recno_buf), "q", cursor->recno);
		} else
			*va_arg(ap, uint64_t *) = cursor->recno;
	} else {
		/* Fast path some common cases. */
		fmt = cursor->key_format;
		if (LF_ISSET(AE_CURSOR_RAW_OK) || AE_STREQ(fmt, "u")) {
			key = va_arg(ap, AE_ITEM *);
			key->data = cursor->key.data;
			key->size = cursor->key.size;
		} else if (AE_STREQ(fmt, "S"))
			*va_arg(ap, const char **) = cursor->key.data;
		else
			ret = __ae_struct_unpackv(session,
			    cursor->key.data, cursor->key.size, fmt, ap);
	}

err:	API_END_RET(session, ret);
}

/*
 * __ae_cursor_set_keyv --
 *	AE_CURSOR->set_key default implementation.
 */
void
__ae_cursor_set_keyv(AE_CURSOR *cursor, uint32_t flags, va_list ap)
{
	AE_DECL_RET;
	AE_SESSION_IMPL *session;
	AE_ITEM *buf, *item, tmp;
	size_t sz;
	va_list ap_copy;
	const char *fmt, *str;

	buf = &cursor->key;
	tmp.mem = NULL;

	CURSOR_API_CALL(cursor, session, set_key, NULL);
	if (F_ISSET(cursor, AE_CURSTD_KEY_SET) && AE_DATA_IN_ITEM(buf)) {
		tmp = *buf;
		buf->mem = NULL;
		buf->memsize = 0;
	}

	F_CLR(cursor, AE_CURSTD_KEY_SET);

	if (AE_CURSOR_RECNO(cursor)) {
		if (LF_ISSET(AE_CURSTD_RAW)) {
			item = va_arg(ap, AE_ITEM *);
			AE_ERR(__ae_struct_unpack(session,
			    item->data, item->size, "q", &cursor->recno));
		} else
			cursor->recno = va_arg(ap, uint64_t);
		if (cursor->recno == AE_RECNO_OOB)
			AE_ERR_MSG(session, EINVAL,
			    "%d is an invalid record number", AE_RECNO_OOB);
		buf->data = &cursor->recno;
		sz = sizeof(cursor->recno);
	} else {
		/* Fast path some common cases and special case AE_ITEMs. */
		fmt = cursor->key_format;
		if (LF_ISSET(AE_CURSOR_RAW_OK | AE_CURSTD_DUMP_JSON) ||
		    AE_STREQ(fmt, "u")) {
			item = va_arg(ap, AE_ITEM *);
			sz = item->size;
			buf->data = item->data;
		} else if (AE_STREQ(fmt, "S")) {
			str = va_arg(ap, const char *);
			sz = strlen(str) + 1;
			buf->data = (void *)str;
		} else {
			va_copy(ap_copy, ap);
			ret = __ae_struct_sizev(
			    session, &sz, cursor->key_format, ap_copy);
			va_end(ap_copy);
			AE_ERR(ret);

			AE_ERR(__ae_buf_initsize(session, buf, sz));
			AE_ERR(__ae_struct_packv(
			    session, buf->mem, sz, cursor->key_format, ap));
		}
	}
	if (sz == 0)
		AE_ERR_MSG(session, EINVAL, "Empty keys not permitted");
	else if ((uint32_t)sz != sz)
		AE_ERR_MSG(session, EINVAL,
		    "Key size (%" PRIu64 ") out of range", (uint64_t)sz);
	cursor->saved_err = 0;
	buf->size = sz;
	F_SET(cursor, AE_CURSTD_KEY_EXT);
	if (0) {
err:		cursor->saved_err = ret;
	}

	/*
	 * If we copied the key, either put the memory back into the cursor,
	 * or if we allocated some memory in the meantime, free it.
	 */
	if (tmp.mem != NULL) {
		if (buf->mem == NULL) {
			buf->mem = tmp.mem;
			buf->memsize = tmp.memsize;
		} else
			__ae_free(session, tmp.mem);
	}
	API_END(session, ret);
}

/*
 * __ae_cursor_get_value --
 *	AE_CURSOR->get_value default implementation.
 */
int
__ae_cursor_get_value(AE_CURSOR *cursor, ...)
{
	AE_DECL_RET;
	va_list ap;

	va_start(ap, cursor);
	ret = __ae_cursor_get_valuev(cursor, ap);
	va_end(ap);
	return (ret);
}

/*
 * __ae_cursor_get_valuev --
 *	AE_CURSOR->get_value worker implementation.
 */
int
__ae_cursor_get_valuev(AE_CURSOR *cursor, va_list ap)
{
	AE_DECL_RET;
	AE_ITEM *value;
	AE_SESSION_IMPL *session;
	const char *fmt;

	CURSOR_API_CALL(cursor, session, get_value, NULL);

	if (!F_ISSET(cursor, AE_CURSTD_VALUE_EXT | AE_CURSTD_VALUE_INT))
		AE_ERR(__ae_cursor_kv_not_set(cursor, false));

	/* Fast path some common cases. */
	fmt = cursor->value_format;
	if (F_ISSET(cursor, AE_CURSOR_RAW_OK) || AE_STREQ(fmt, "u")) {
		value = va_arg(ap, AE_ITEM *);
		value->data = cursor->value.data;
		value->size = cursor->value.size;
	} else if (AE_STREQ(fmt, "S"))
		*va_arg(ap, const char **) = cursor->value.data;
	else if (AE_STREQ(fmt, "t") ||
	    (isdigit(fmt[0]) && AE_STREQ(fmt + 1, "t")))
		*va_arg(ap, uint8_t *) = *(uint8_t *)cursor->value.data;
	else
		ret = __ae_struct_unpackv(session,
		    cursor->value.data, cursor->value.size, fmt, ap);

err:	API_END_RET(session, ret);
}

/*
 * __ae_cursor_set_value --
 *	AE_CURSOR->set_value default implementation.
 */
void
__ae_cursor_set_value(AE_CURSOR *cursor, ...)
{
	va_list ap;

	va_start(ap, cursor);
	__ae_cursor_set_valuev(cursor, ap);
	va_end(ap);
}

/*
 * __ae_cursor_set_valuev --
 *	AE_CURSOR->set_value worker implementation.
 */
void
__ae_cursor_set_valuev(AE_CURSOR *cursor, va_list ap)
{
	AE_DECL_RET;
	AE_ITEM *buf, *item, tmp;
	AE_SESSION_IMPL *session;
	const char *fmt, *str;
	va_list ap_copy;
	size_t sz;

	buf = &cursor->value;
	tmp.mem = NULL;

	CURSOR_API_CALL(cursor, session, set_value, NULL);
	if (F_ISSET(cursor, AE_CURSTD_VALUE_SET) && AE_DATA_IN_ITEM(buf)) {
		tmp = *buf;
		buf->mem = NULL;
		buf->memsize = 0;
	}

	F_CLR(cursor, AE_CURSTD_VALUE_SET);

	/* Fast path some common cases. */
	fmt = cursor->value_format;
	if (F_ISSET(cursor, AE_CURSOR_RAW_OK | AE_CURSTD_DUMP_JSON) ||
	    AE_STREQ(fmt, "u")) {
		item = va_arg(ap, AE_ITEM *);
		sz = item->size;
		buf->data = item->data;
	} else if (AE_STREQ(fmt, "S")) {
		str = va_arg(ap, const char *);
		sz = strlen(str) + 1;
		buf->data = str;
	} else if (AE_STREQ(fmt, "t") ||
	    (isdigit(fmt[0]) && AE_STREQ(fmt + 1, "t"))) {
		sz = 1;
		AE_ERR(__ae_buf_initsize(session, buf, sz));
		*(uint8_t *)buf->mem = (uint8_t)va_arg(ap, int);
	} else {
		va_copy(ap_copy, ap);
		ret = __ae_struct_sizev(session,
		    &sz, cursor->value_format, ap_copy);
		va_end(ap_copy);
		AE_ERR(ret);
		AE_ERR(__ae_buf_initsize(session, buf, sz));
		AE_ERR(__ae_struct_packv(session, buf->mem, sz,
		    cursor->value_format, ap));
	}
	F_SET(cursor, AE_CURSTD_VALUE_EXT);
	buf->size = sz;

	if (0) {
err:		cursor->saved_err = ret;
	}

	/*
	 * If we copied the value, either put the memory back into the cursor,
	 * or if we allocated some memory in the meantime, free it.
	 */
	if (tmp.mem != NULL) {
		if (buf->mem == NULL) {
			buf->mem = tmp.mem;
			buf->memsize = tmp.memsize;
		} else
			__ae_free(session, tmp.mem);
	}

	API_END(session, ret);
}

/*
 * __ae_cursor_close --
 *	AE_CURSOR->close default implementation.
 */
int
__ae_cursor_close(AE_CURSOR *cursor)
{
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	session = (AE_SESSION_IMPL *)cursor->session;

	if (F_ISSET(cursor, AE_CURSTD_OPEN)) {
		TAILQ_REMOVE(&session->cursors, cursor, q);

		(void)__ae_atomic_sub32(&S2C(session)->open_cursor_count, 1);
		AE_STAT_FAST_DATA_DECR(session, session_cursor_open);
	}

	__ae_buf_free(session, &cursor->key);
	__ae_buf_free(session, &cursor->value);

	__ae_free(session, cursor->internal_uri);
	__ae_free(session, cursor->uri);
	__ae_overwrite_and_free(session, cursor);
	return (ret);
}

/*
 * __ae_cursor_equals --
 *	AE_CURSOR->equals default implementation.
 */
int
__ae_cursor_equals(AE_CURSOR *cursor, AE_CURSOR *other, int *equalp)
{
	AE_DECL_RET;
	AE_SESSION_IMPL *session;
	int cmp;

	session = (AE_SESSION_IMPL *)cursor->session;
	CURSOR_API_CALL(cursor, session, equals, NULL);

	AE_ERR(cursor->compare(cursor, other, &cmp));
	*equalp = (cmp == 0) ? 1 : 0;

err:	API_END(session, ret);
	return (ret);
}

/*
 * __ae_cursor_reconfigure --
 *	Set runtime-configurable settings.
 */
int
__ae_cursor_reconfigure(AE_CURSOR *cursor, const char *config)
{
	AE_CONFIG_ITEM cval;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	session = (AE_SESSION_IMPL *)cursor->session;

	/* Reconfiguration resets the cursor. */
	AE_RET(cursor->reset(cursor));

	/*
	 * append
	 * Only relevant to column stores.
	 */
	if (AE_CURSOR_RECNO(cursor)) {
		if ((ret = __ae_config_getones(
		    session, config, "append", &cval)) == 0) {
			if (cval.val)
				F_SET(cursor, AE_CURSTD_APPEND);
			else
				F_CLR(cursor, AE_CURSTD_APPEND);
		} else
			AE_RET_NOTFOUND_OK(ret);
	}

	/*
	 * overwrite
	 */
	if ((ret = __ae_config_getones(
	    session, config, "overwrite", &cval)) == 0) {
		if (cval.val)
			F_SET(cursor, AE_CURSTD_OVERWRITE);
		else
			F_CLR(cursor, AE_CURSTD_OVERWRITE);
	} else
		AE_RET_NOTFOUND_OK(ret);

	return (0);
}

/*
 * __ae_cursor_dup_position --
 *	Set a cursor to another cursor's position.
 */
int
__ae_cursor_dup_position(AE_CURSOR *to_dup, AE_CURSOR *cursor)
{
	AE_ITEM key;

	/*
	 * Get a copy of the cursor's raw key, and set it in the new cursor,
	 * then search for that key to position the cursor.
	 *
	 * We don't clear the AE_ITEM structure: all that happens when getting
	 * and setting the key is the data/size fields are reset to reference
	 * the original cursor's key.
	 *
	 * That said, we're playing games with the cursor flags: setting the key
	 * sets the key/value application-set flags in the new cursor, which may
	 * or may not be correct, but there's nothing simple that fixes it.  We
	 * depend on the subsequent cursor search to clean things up, as search
	 * is required to copy and/or reference private memory after success.
	 */
	AE_RET(__ae_cursor_get_raw_key(to_dup, &key));
	__ae_cursor_set_raw_key(cursor, &key);

	/*
	 * We now have a reference to the raw key, but we don't know anything
	 * about the memory in which it's stored, it could be btree/file page
	 * memory in the cache, application memory or the original cursor's
	 * key/value AE_ITEMs.  Memory allocated in support of another cursor
	 * could be discarded when that cursor is closed, so it's a problem.
	 * However, doing a search to position the cursor will fix the problem:
	 * cursors cannot reference application memory after cursor operations
	 * and that requirement will save the day.
	 */
	AE_RET(cursor->search(cursor));

	return (0);
}

/*
 * __ae_cursor_init --
 *	Default cursor initialization.
 */
int
__ae_cursor_init(AE_CURSOR *cursor,
    const char *uri, AE_CURSOR *owner, const char *cfg[], AE_CURSOR **cursorp)
{
	AE_CONFIG_ITEM cval;
	AE_CURSOR *cdump;
	AE_SESSION_IMPL *session;

	session = (AE_SESSION_IMPL *)cursor->session;

	if (cursor->internal_uri == NULL)
		AE_RET(__ae_strdup(session, uri, &cursor->internal_uri));

	/*
	 * append
	 * The append flag is only relevant to column stores.
	 */
	if (AE_CURSOR_RECNO(cursor)) {
		AE_RET(__ae_config_gets_def(session, cfg, "append", 0, &cval));
		if (cval.val != 0)
			F_SET(cursor, AE_CURSTD_APPEND);
	}

	/*
	 * checkpoint, readonly
	 * Checkpoint cursors are permanently read-only, avoid the extra work
	 * of two configuration string checks.
	 */
	AE_RET(__ae_config_gets_def(session, cfg, "checkpoint", 0, &cval));
	if (cval.len != 0) {
		cursor->insert = __ae_cursor_notsup;
		cursor->update = __ae_cursor_notsup;
		cursor->remove = __ae_cursor_notsup;
	} else {
		AE_RET(
		    __ae_config_gets_def(session, cfg, "readonly", 0, &cval));
		if (cval.val != 0) {
			cursor->insert = __ae_cursor_notsup;
			cursor->update = __ae_cursor_notsup;
			cursor->remove = __ae_cursor_notsup;
		}
	}

	/*
	 * dump
	 * If an index cursor is opened with dump, then this
	 * function is called on the index files, with the dump
	 * config string, and with the index cursor as an owner.
	 * We don't want to create a dump cursor in that case, because
	 * we'll create the dump cursor on the index cursor itself.
	 */
	AE_RET(__ae_config_gets_def(session, cfg, "dump", 0, &cval));
	if (cval.len != 0 && owner == NULL) {
		F_SET(cursor,
		    AE_STRING_MATCH("json", cval.str, cval.len) ?
		    AE_CURSTD_DUMP_JSON :
		    (AE_STRING_MATCH("print", cval.str, cval.len) ?
		    AE_CURSTD_DUMP_PRINT : AE_CURSTD_DUMP_HEX));
		/*
		 * Dump cursors should not have owners: only the
		 * top-level cursor should be wrapped in a dump cursor.
		 */
		AE_RET(__ae_curdump_create(cursor, owner, &cdump));
		owner = cdump;
	} else
		cdump = NULL;

	/* overwrite */
	AE_RET(__ae_config_gets_def(session, cfg, "overwrite", 1, &cval));
	if (cval.val)
		F_SET(cursor, AE_CURSTD_OVERWRITE);
	else
		F_CLR(cursor, AE_CURSTD_OVERWRITE);

	/* raw */
	AE_RET(__ae_config_gets_def(session, cfg, "raw", 0, &cval));
	if (cval.val != 0)
		F_SET(cursor, AE_CURSTD_RAW);

	/*
	 * Cursors that are internal to some other cursor (such as file cursors
	 * inside a table cursor) should be closed after the containing cursor.
	 * Arrange for that to happen by putting internal cursors after their
	 * owners on the queue.
	 */
	if (owner != NULL) {
		AE_ASSERT(session, F_ISSET(owner, AE_CURSTD_OPEN));
		TAILQ_INSERT_AFTER(&session->cursors, owner, cursor, q);
	} else
		TAILQ_INSERT_HEAD(&session->cursors, cursor, q);

	F_SET(cursor, AE_CURSTD_OPEN);
	(void)__ae_atomic_add32(&S2C(session)->open_cursor_count, 1);
	AE_STAT_FAST_DATA_INCR(session, session_cursor_open);

	*cursorp = (cdump != NULL) ? cdump : cursor;
	return (0);
}
