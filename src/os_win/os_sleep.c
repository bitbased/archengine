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
	/*
	 * If the caller wants a small pause, set to our
	 * smallest granularity.
	 */
	if (seconds == 0 && micro_seconds < AE_THOUSAND)
		micro_seconds = AE_THOUSAND;
	Sleep(seconds * AE_THOUSAND + micro_seconds / AE_THOUSAND);
}
