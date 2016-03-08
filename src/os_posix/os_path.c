/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_absolute_path --
 *	Return if a filename is an absolute path.
 */
bool
__ae_absolute_path(const char *path)
{
	return (path[0] == '/');
}

/*
 * __ae_path_separator --
 *	Return the path separator string.
 */
const char *
__ae_path_separator(void)
{
	return ("/");
}
