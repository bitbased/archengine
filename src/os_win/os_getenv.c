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
	AE_DECL_RET;
	DWORD size;

	*envp = NULL;

	size = GetEnvironmentVariableA(variable, NULL, 0);
	if (size <= 1)
		return (AE_NOTFOUND);

	AE_RET(__ae_calloc(session, 1, size, envp));

	ret = GetEnvironmentVariableA(variable, *envp, size);
	/* We expect the number of bytes not including nul terminator. */
	if ((ret + 1) != size)
		AE_RET_MSG(session, __ae_errno(),
		    "GetEnvironmentVariableA failed: %s", variable);

	return (0);
}
