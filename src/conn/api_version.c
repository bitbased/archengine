/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * archengine_version --
 *	Return library version information.
 */
const char *
archengine_version(int *majorp, int *minorp, int *patchp)
{
	if (majorp != NULL)
		*majorp = ARCHENGINE_VERSION_MAJOR;
	if (minorp != NULL)
		*minorp = ARCHENGINE_VERSION_MINOR;
	if (patchp != NULL)
		*patchp = ARCHENGINE_VERSION_PATCH;
	return (ARCHENGINE_VERSION_STRING);
}
