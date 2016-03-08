/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_getenv --
 * 	Get a non-NULL, greater than zero-length environment variable.
 */
int
__ae_getenv(AE_SESSION_IMPL *session, const char *variable, const char **envp)
{
	const char *temp;

	*envp = NULL;

	if (((temp = getenv(variable)) != NULL) && strlen(temp) > 0)
		return (__ae_strdup(session, temp, envp));

	return (AE_NOTFOUND);
}
