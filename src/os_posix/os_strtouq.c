/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_strtouq --
 *	Convert a string to an unsigned quad integer.
 */
uint64_t
__ae_strtouq(const char *nptr, char **endptr, int base)
{
#if defined(HAVE_STRTOUQ)
	return (strtouq(nptr, endptr, base));
#else
	AE_STATIC_ASSERT(sizeof(uint64_t) == sizeof(unsigned long long));

	return (strtoull(nptr, endptr, base));
#endif
}
