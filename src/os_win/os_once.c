/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_init_once_callback --
 *	Global initialization, run once.
 */
BOOL CALLBACK _ae_init_once_callback(
    _Inout_      PINIT_ONCE InitOnce,
    _Inout_opt_  PVOID Parameter,
    _Out_opt_    PVOID *Context
    )
{
	void(*init_routine)(void) = Parameter;
	AE_UNUSED(InitOnce);
	AE_UNUSED(Context);

	init_routine();

	return (TRUE);
}

/*
 * __ae_once --
 *	One-time initialization per process.
 */
int
__ae_once(void(*init_routine)(void))
{
	INIT_ONCE once_control = INIT_ONCE_STATIC_INIT;
	PVOID lpContext = NULL;

	return !InitOnceExecuteOnce(&once_control, &_ae_init_once_callback,
	    init_routine, lpContext);
}
