/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_has_priv --
 *	Return if the process has special privileges, defined as having
 *	different effective and read UIDs or GIDs.
 */
bool
__ae_has_priv(void)
{
	return (getuid() != geteuid() || getgid() != getegid());
}
