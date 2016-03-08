/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_sleep --
 *	Pause the thread of control.
 */
void
__ae_sleep(uint64_t seconds, uint64_t micro_seconds)
{
	struct timeval t;

	t.tv_sec = (time_t)(seconds + micro_seconds / AE_MILLION);
	t.tv_usec = (suseconds_t)(micro_seconds % AE_MILLION);

	(void)select(0, NULL, NULL, NULL, &t);
}
