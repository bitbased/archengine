/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __curconfig_close --
 *	AE_CURSOR->close method for the config cursor type.
 */
static int
__curconfig_close(AE_CURSOR *cursor)
{
	return (__ae_cursor_close(cursor));
}

/*
 * __ae_curconfig_open --
 *	AE_SESSION->open_cursor method for config cursors.
 */
int
__ae_curconfig_open(AE_SESSION_IMPL *session,
    const char *uri, const char *cfg[], AE_CURSOR **cursorp)
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
	    __ae_cursor_noop,		/* reset */
	    __ae_cursor_notsup,		/* search */
	    __ae_cursor_notsup,		/* search-near */
	    __ae_cursor_notsup,		/* insert */
	    __ae_cursor_notsup,		/* update */
	    __ae_cursor_notsup,		/* remove */
	    __ae_cursor_notsup,		/* reconfigure */
	    __curconfig_close);
	AE_CURSOR_CONFIG *cconfig;
	AE_CURSOR *cursor;
	AE_DECL_RET;

	AE_STATIC_ASSERT(offsetof(AE_CURSOR_CONFIG, iface) == 0);

	AE_UNUSED(uri);

	AE_RET(__ae_calloc_one(session, &cconfig));

	cursor = &cconfig->iface;
	*cursor = iface;
	cursor->session = &session->iface;
	cursor->key_format = cursor->value_format = "S";

	/* __ae_cursor_init is last so we don't have to clean up on error. */
	AE_ERR(__ae_cursor_init(cursor, uri, NULL, cfg, cursorp));

	if (0) {
err:		__ae_free(session, cconfig);
	}
	return (ret);
}
