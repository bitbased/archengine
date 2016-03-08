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

	if ((dlh->handle = dlopen(path, RTLD_LAZY)) == NULL)
		AE_ERR_MSG(
		    session, __ae_errno(), "dlopen(%s): %s", path, dlerror());

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
	if ((sym = dlsym(dlh->handle, name)) == NULL) {
		if (fail)
			AE_RET_MSG(session, __ae_errno(),
			    "dlsym(%s in %s): %s", name, dlh->name, dlerror());
		return (0);
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

	/*
	 * FreeBSD dies inside __cxa_finalize when closing handles.
	 *
	 * For now, just skip the dlclose: this may leak some resources until
	 * the process exits, but that is preferable to hard-to-debug crashes
	 * during exit.
	 */
#ifndef __FreeBSD__
	if (dlclose(dlh->handle) != 0) {
		ret = __ae_errno();
		__ae_err(session, ret, "dlclose: %s", dlerror());
	}
#endif

	__ae_free(session, dlh->name);
	__ae_free(session, dlh);
	return (ret);
}
