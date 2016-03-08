/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_dlopen --
 *	Open a dynamic library.
 */
int
__ae_dlopen(AE_SESSION_IMPL *session, const char *path, AE_DLH **dlhp)
{
	AE_DECL_RET;
	AE_DLH *dlh;

	AE_RET(__ae_calloc_one(session, &dlh));
	AE_ERR(__ae_strdup(session, path, &dlh->name));

	/* NULL means load from the current binary */
	if (path == NULL) {
		ret = GetModuleHandleExA(0, NULL, (HMODULE *)&dlh->handle);
		if (ret == FALSE)
			AE_ERR_MSG(session,
			    __ae_errno(), "GetModuleHandleEx(%s): %s", path, 0);
	} else {
		// TODO: load dll here
		DebugBreak();
	}

	/* Windows returns 0 on failure, AE expects 0 on success */
	ret = !ret;

	*dlhp = dlh;
	if (0) {
err:		__ae_free(session, dlh->name);
		__ae_free(session, dlh);
	}
	return (ret);
}

/*
 * __ae_dlsym --
 *	Lookup a symbol in a dynamic library.
 */
int
__ae_dlsym(AE_SESSION_IMPL *session,
    AE_DLH *dlh, const char *name, bool fail, void *sym_ret)
{
	void *sym;

	*(void **)sym_ret = NULL;

	sym = GetProcAddress(dlh->handle, name);
	if (sym == NULL && fail) {
		AE_RET_MSG(session, __ae_errno(),
		    "GetProcAddress(%s in %s)", name, dlh->name);
	}

	*(void **)sym_ret = sym;
	return (0);
}

/*
 * __ae_dlclose --
 *	Close a dynamic library
 */
int
__ae_dlclose(AE_SESSION_IMPL *session, AE_DLH *dlh)
{
	AE_DECL_RET;

	if ((ret = FreeLibrary(dlh->handle)) == FALSE) {
		__ae_err(session, __ae_errno(), "FreeLibrary");
	}

	/* Windows returns 0 on failure, AE expects 0 on success */
	ret = !ret;

	__ae_free(session, dlh->name);
	__ae_free(session, dlh);
	return (ret);
}
