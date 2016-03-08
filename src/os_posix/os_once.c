/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_once --
 *	One-time initialization per process.
 */
int
__ae_once(void (*init_routine)(void))
{
	static pthread_once_t once_control = PTHREAD_ONCE_INIT;

	return (pthread_once(&once_control, init_routine));
}
