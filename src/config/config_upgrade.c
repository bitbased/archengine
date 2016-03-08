/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_config_upgrade --
 *	Upgrade a configuration string by appended the replacement version.
 */
int
__ae_config_upgrade(AE_SESSION_IMPL *session, AE_ITEM *buf)
{
	AE_CONFIG_ITEM v;
	const char *config;

	config = buf->data;

	/*
	 * archengine_open:
	 *	lsm_merge=boolean -> lsm_manager=(merge=boolean)
	 */
	if (__ae_config_getones(
	    session, config, "lsm_merge", &v) != AE_NOTFOUND)
		AE_RET(__ae_buf_catfmt(session, buf,
		    ",lsm_manager=(merge=%s)", v.val ? "true" : "false"));

	return (0);
}
