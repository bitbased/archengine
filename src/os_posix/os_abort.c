/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_abort --
 *	Abort the process, dropping core.
 */
void
__ae_abort(AE_SESSION_IMPL *session)
    AE_GCC_FUNC_ATTRIBUTE((noreturn))
{
	__ae_errx(session, "aborting ArchEngine library");

#ifdef HAVE_DIAGNOSTIC
	__ae_attach(session);
#endif

	abort();
	/* NOTREACHED */
}
