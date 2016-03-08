/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __raw_to_dump --
 *	We have a buffer where the data item contains a raw value,
 *	convert it to a printable string.
 */
static int
__raw_to_dump(
    AE_SESSION_IMPL *session, AE_ITEM *from, AE_ITEM *to, bool hexonly)
{
	if (hexonly)
		AE_RET(__ae_raw_to_hex(session, from->data, from->size, to));
	else
		AE_RET(
		    __ae_raw_to_esc_hex(session, from->data, from->size, to));

	return (0);
}

/*
 * __dump_to_raw --
 *	We have a buffer containing a dump string,
 *	convert it to a raw value.
 */
static int
__dump_to_raw(
    AE_SESSION_IMPL *session, const char *src_arg, AE_ITEM *item, bool hexonly)
{
	if (hexonly)
		AE_RET(__ae_hex_to_raw(session, src_arg, item));
	else
		AE_RET(__ae_esc_hex_to_raw(session, src_arg, item));

	return (0);
}

/*
 * __curdump_get_key --
 *	AE_CURSOR->get_key for dump cursors.
 */
static int
__curdump_get_key(AE_CURSOR *cursor, ...)
{
	AE_CURSOR *child;
	AE_CURSOR_DUMP *cdump;
	AE_CURSOR_JSON *json;
	AE_DECL_RET;
	AE_ITEM item, *itemp;
	AE_SESSION_IMPL *session;
	size_t size;
	uint64_t recno;
	const char *fmt;
	const void *buffer;
	va_list ap;

	cdump = (AE_CURSOR_DUMP *)cursor;
	child = cdump->child;

	va_start(ap, cursor);
	CURSOR_API_CALL(cursor, session, get_key, NULL);

	if (F_ISSET(cursor, AE_CURSTD_DUMP_JSON)) {
		json = (AE_CURSOR_JSON *)cursor->json_private;
		AE_ASSERT(session, json != NULL);
		if (AE_CURSOR_RECNO(cursor)) {
			AE_ERR(child->get_key(child, &recno));
			buffer = &recno;
			size = sizeof(recno);
			fmt = "R";
		} else {
			AE_ERR(__ae_cursor_get_raw_key(child, &item));
			buffer = item.data;
			size = item.size;
			if (F_ISSET(cursor, AE_CURSTD_RAW))
				fmt = "u";
			else
				fmt = cursor->key_format;
		}
		ret = __ae_json_alloc_unpack(
		    session, buffer, size, fmt, json, true, ap);
	} else {
		if (AE_CURSOR_RECNO(cursor) &&
		    !F_ISSET(cursor, AE_CURSTD_RAW)) {
			AE_ERR(child->get_key(child, &recno));

			AE_ERR(__ae_buf_fmt(session, &cursor->key, "%"
			    PRIu64, recno));
		} else {
			AE_ERR(child->get_key(child, &item));

			AE_ERR(__raw_to_dump(session, &item, &cursor->key,
			    F_ISSET(cursor, AE_CURSTD_DUMP_HEX)));
		}

		if (F_ISSET(cursor, AE_CURSTD_RAW)) {
			itemp = va_arg(ap, AE_ITEM *);
			itemp->data = cursor->key.data;
			itemp->size = cursor->key.size;
		} else
			*va_arg(ap, const char **) = cursor->key.data;
	}

err:	va_end(ap);
	API_END_RET(session, ret);
}

/*
 * str2recno --
 *	Convert a string to a record number.
 */
static int
str2recno(AE_SESSION_IMPL *session, const char *p, uint64_t *recnop)
{
	uint64_t recno;
	char *endptr;

	/*
	 * strtouq takes lots of things like hex values, signs and so on and so
	 * forth -- none of them are OK with us.  Check the string starts with
	 * digit, that turns off the special processing.
	 */
	if (!isdigit(p[0]))
		goto format;

	errno = 0;
	recno = __ae_strtouq(p, &endptr, 0);
	if (recno == ULLONG_MAX && errno == ERANGE)
		AE_RET_MSG(session, ERANGE, "%s: invalid record number", p);
	if (endptr[0] != '\0')
format:		AE_RET_MSG(session, EINVAL, "%s: invalid record number", p);

	*recnop = recno;
	return (0);
}

/*
 * __curdump_set_key --
 *	AE_CURSOR->set_key for dump cursors.
 */
static void
__curdump_set_key(AE_CURSOR *cursor, ...)
{
	AE_CURSOR_DUMP *cdump;
	AE_CURSOR *child;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;
	uint64_t recno;
	va_list ap;
	const char *p;

	cdump = (AE_CURSOR_DUMP *)cursor;
	child = cdump->child;
	CURSOR_API_CALL(cursor, session, set_key, NULL);

	va_start(ap, cursor);
	if (F_ISSET(cursor, AE_CURSTD_RAW))
		p = va_arg(ap, AE_ITEM *)->data;
	else
		p = va_arg(ap, const char *);
	va_end(ap);

	if (AE_CURSOR_RECNO(cursor) && !F_ISSET(cursor, AE_CURSTD_RAW)) {
		AE_ERR(str2recno(session, p, &recno));

		child->set_key(child, recno);
	} else {
		if (F_ISSET(cursor, AE_CURSTD_DUMP_JSON))
			AE_ERR(__ae_json_to_item(session, p, cursor->key_format,
			    (AE_CURSOR_JSON *)cursor->json_private, true,
			    &cursor->key));
		else
			AE_ERR(__dump_to_raw(session, p, &cursor->key,
			    F_ISSET(cursor, AE_CURSTD_DUMP_HEX)));

		child->set_key(child, &cursor->key);
	}

	if (0) {
err:		cursor->saved_err = ret;
		F_CLR(cursor, AE_CURSTD_KEY_SET);
	}
	API_END(session, ret);
}

/*
 * __curdump_get_value --
 *	AE_CURSOR->get_value for dump cursors.
 */
static int
__curdump_get_value(AE_CURSOR *cursor, ...)
{
	AE_CURSOR_DUMP *cdump;
	AE_CURSOR_JSON *json;
	AE_CURSOR *child;
	AE_DECL_RET;
	AE_ITEM item, *itemp;
	AE_SESSION_IMPL *session;
	va_list ap;
	const char *fmt;

	cdump = (AE_CURSOR_DUMP *)cursor;
	child = cdump->child;

	va_start(ap, cursor);
	CURSOR_API_CALL(cursor, session, get_value, NULL);

	if (F_ISSET(cursor, AE_CURSTD_DUMP_JSON)) {
		json = (AE_CURSOR_JSON *)cursor->json_private;
		AE_ASSERT(session, json != NULL);
		AE_ERR(__ae_cursor_get_raw_value(child, &item));
		fmt = F_ISSET(cursor, AE_CURSTD_RAW) ?
		    "u" : cursor->value_format;
		ret = __ae_json_alloc_unpack(
		    session, item.data, item.size, fmt, json, false, ap);
	} else {
		AE_ERR(child->get_value(child, &item));

		AE_ERR(__raw_to_dump(session, &item, &cursor->value,
		    F_ISSET(cursor, AE_CURSTD_DUMP_HEX)));

		if (F_ISSET(cursor, AE_CURSTD_RAW)) {
			itemp = va_arg(ap, AE_ITEM *);
			itemp->data = cursor->value.data;
			itemp->size = cursor->value.size;
		} else
			*va_arg(ap, const char **) = cursor->value.data;
	}

err:	va_end(ap);
	API_END_RET(session, ret);
}

/*
 * __curdump_set_value --
 *	AE_CURSOR->set_value for dump cursors.
 */
static void
__curdump_set_value(AE_CURSOR *cursor, ...)
{
	AE_CURSOR_DUMP *cdump;
	AE_CURSOR *child;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;
	va_list ap;
	const char *p;

	cdump = (AE_CURSOR_DUMP *)cursor;
	child = cdump->child;
	CURSOR_API_CALL(cursor, session, set_value, NULL);

	va_start(ap, cursor);
	if (F_ISSET(cursor, AE_CURSTD_RAW))
		p = va_arg(ap, AE_ITEM *)->data;
	else
		p = va_arg(ap, const char *);
	va_end(ap);

	if (F_ISSET(cursor, AE_CURSTD_DUMP_JSON))
		AE_ERR(__ae_json_to_item(session, p, cursor->value_format,
		    (AE_CURSOR_JSON *)cursor->json_private, false,
		    &cursor->value));
	else
		AE_ERR(__dump_to_raw(session, p, &cursor->value,
		    F_ISSET(cursor, AE_CURSTD_DUMP_HEX)));

	child->set_value(child, &cursor->value);

	if (0) {
err:		cursor->saved_err = ret;
		F_CLR(cursor, AE_CURSTD_VALUE_SET);
	}
	API_END(session, ret);
}

/* Pass through a call to the underlying cursor. */
#define	AE_CURDUMP_PASS(op)						\
static int								\
__curdump_##op(AE_CURSOR *cursor)					\
{									\
	AE_CURSOR *child;						\
									\
	child = ((AE_CURSOR_DUMP *)cursor)->child;			\
	return (child->op(child));					\
}

AE_CURDUMP_PASS(next)
AE_CURDUMP_PASS(prev)
AE_CURDUMP_PASS(reset)
AE_CURDUMP_PASS(search)

/*
 * __curdump_search_near --
 *	AE_CURSOR::search_near for dump cursors.
 */
static int
__curdump_search_near(AE_CURSOR *cursor, int *exact)
{
	AE_CURSOR_DUMP *cdump;

	cdump = (AE_CURSOR_DUMP *)cursor;
	return (cdump->child->search_near(cdump->child, exact));
}

AE_CURDUMP_PASS(insert)
AE_CURDUMP_PASS(update)
AE_CURDUMP_PASS(remove)

/*
 * __curdump_close --
 *	AE_CURSOR::close for dump cursors.
 */
static int
__curdump_close(AE_CURSOR *cursor)
{
	AE_CURSOR_DUMP *cdump;
	AE_CURSOR *child;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	cdump = (AE_CURSOR_DUMP *)cursor;
	child = cdump->child;

	CURSOR_API_CALL(cursor, session, close, NULL);
	if (child != NULL)
		AE_TRET(child->close(child));
	/* We shared the child's URI. */
	cursor->internal_uri = NULL;
	__ae_json_close(session, cursor);
	AE_TRET(__ae_cursor_close(cursor));

err:	API_END_RET(session, ret);
}

/*
 * __ae_curdump_create --
 *	initialize a dump cursor.
 */
int
__ae_curdump_create(AE_CURSOR *child, AE_CURSOR *owner, AE_CURSOR **cursorp)
{
	AE_CURSOR_STATIC_INIT(iface,
	    __curdump_get_key,		/* get-key */
	    __curdump_get_value,	/* get-value */
	    __curdump_set_key,		/* set-key */
	    __curdump_set_value,	/* set-value */
	    __ae_cursor_notsup,		/* compare */
	    __ae_cursor_notsup,		/* equals */
	    __curdump_next,		/* next */
	    __curdump_prev,		/* prev */
	    __curdump_reset,		/* reset */
	    __curdump_search,		/* search */
	    __curdump_search_near,	/* search-near */
	    __curdump_insert,		/* insert */
	    __curdump_update,		/* update */
	    __curdump_remove,		/* remove */
	    __ae_cursor_notsup,		/* reconfigure */
	    __curdump_close);		/* close */
	AE_CURSOR *cursor;
	AE_CURSOR_DUMP *cdump;
	AE_CURSOR_JSON *json;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;
	const char *cfg[2];

	AE_STATIC_ASSERT(offsetof(AE_CURSOR_DUMP, iface) == 0);

	session = (AE_SESSION_IMPL *)child->session;

	AE_RET(__ae_calloc_one(session, &cdump));
	cursor = &cdump->iface;
	*cursor = iface;
	cursor->session = child->session;
	cursor->internal_uri = child->internal_uri;
	cursor->key_format = child->key_format;
	cursor->value_format = child->value_format;
	cdump->child = child;

	/* Copy the dump flags from the child cursor. */
	F_SET(cursor, F_MASK(child,
	    AE_CURSTD_DUMP_HEX | AE_CURSTD_DUMP_JSON | AE_CURSTD_DUMP_PRINT));
	if (F_ISSET(cursor, AE_CURSTD_DUMP_JSON)) {
		AE_ERR(__ae_calloc_one(session, &json));
		cursor->json_private = child->json_private = json;
	}

	/* __ae_cursor_init is last so we don't have to clean up on error. */
	cfg[0] = AE_CONFIG_BASE(session, AE_SESSION_open_cursor);
	cfg[1] = NULL;
	AE_ERR(__ae_cursor_init(cursor, NULL, owner, cfg, cursorp));

	if (0) {
err:		__ae_free(session, cursor);
	}
	return (ret);
}
