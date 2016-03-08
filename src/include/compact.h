/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

struct __ae_compact {
	uint32_t	lsm_count;	/* Number of LSM trees seen */
	uint32_t	file_count;	/* Number of files seen */
	uint64_t	max_time;	/* Configured timeout */
};
