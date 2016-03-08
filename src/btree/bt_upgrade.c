/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_upgrade --
 *	Upgrade a file.
 */
int
__ae_upgrade(AE_SESSION_IMPL *session, const char *cfg[])
{
	AE_UNUSED(cfg);

	/* There's nothing to upgrade, yet. */
	AE_RET(__ae_progress(session, NULL, 1));
	return (0);
}
